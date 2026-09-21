package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"collaborating.tuhh.de/e-19/teaching/bd26_project_f1_c/internal/kafka"
	"collaborating.tuhh.de/e-19/teaching/bd26_project_f1_c/internal/utility"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/websocket"
)

const (
	broadcastQueueSize           = 1024
	clientQueueSize              = 256
	defaultNotificationThreshold = int64(200)
	minimumNotificationThreshold = int64(200)
	maximumNotificationThreshold = int64(10000)
)

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	Broadcast  chan []byte
}

type Client struct {
	conn                  *websocket.Conn
	send                  chan []byte
	done                  chan struct{}
	closeOnce             sync.Once
	notificationThreshold int64
}

type WebSocketRequest struct {
	Type string `json:"type"`
}

type ViewportRequest struct {
	Type   string                 `json:"type"`
	Zoom   int                    `json:"zoom"`
	Bounds utility.ViewportBounds `json:"bounds"`
}

type NotificationSettingsRequest struct {
	Type        string  `json:"type"`
	CriticalCPM float64 `json:"critical_cpm"`
}

type NotificationSettingsResponse struct {
	Type        string `json:"type"`
	CriticalCPM int64  `json:"critical_cpm"`
}

type H3ViewportResponse struct {
	Type       string `json:"type"`
	Zoom       int    `json:"zoom"`
	Resolution int    `json:"resolution"`
}

type WSErrorResponse struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type NotificationResponse struct {
	Type      string          `json:"type"`
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Key       string          `json:"key,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// NewHub creates a websocket hub with bounded broadcast queues.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		Broadcast:  make(chan []byte, broadcastQueueSize),
	}
}

// Run registers clients and fans out Kafka broadcasts without writing directly to sockets.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			h.removeClient(client)

		case msg := <-h.Broadcast:
			h.broadcast(msg)
		}
	}
}

// StartNotifications consumes new hotspot messages and broadcasts them over the existing websocket connection.
func (h *Hub) StartNotifications(ctx context.Context, brokers string, topic string, groupID string) {
	go func() {
		log.Printf("Kafka notification consumer configured with brokers=%q topic=%q group_id=%q", brokers, topic, groupID)
		err := kafka.ConsumeNotifications(ctx, brokers, topic, groupID, func(msg *confluentKafka.Message) error {
			payload, err := json.Marshal(notificationResponse(msg))
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case h.Broadcast <- payload:
				return nil
			default:
				log.Printf("dropping notification because websocket broadcast queue is full")
				return nil
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("notification consumer stopped: %v", err)
		}
	}()
}

// HandleWebSocket upgrades an HTTP request and starts the client read/write loops.
func (h *Hub) HandleWebSocket(brokers string, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade websocket: %v", err)
		return
	}

	client := &Client{
		conn:                  conn,
		send:                  make(chan []byte, clientQueueSize),
		done:                  make(chan struct{}),
		notificationThreshold: defaultNotificationThreshold,
	}

	h.register <- client
	go client.writePump()
	h.readPump(client, brokers)
}

// readPump reads viewport requests and starts a cancellable fetch for the latest request.
func (h *Hub) readPump(client *Client, brokers string) {
	var searchCancel context.CancelFunc

	defer func() {
		if searchCancel != nil {
			searchCancel()
		}
		h.unregister <- client
	}()

	for {
		_, payload, err := client.conn.ReadMessage()
		if err != nil {
			log.Printf("websocket read error: %v", err)
			return
		}

		var request WebSocketRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			sendWSError(client, "invalid websocket request JSON")
			continue
		}

		switch request.Type {
		case "notification_settings":
			handleNotificationSettings(client, payload)

		case "viewport":
			var req ViewportRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				sendWSError(client, "invalid viewport request JSON")
				continue
			}

			if searchCancel != nil {
				searchCancel()
			}

			ctx, cancel := context.WithCancel(context.Background())
			searchCancel = cancel
			go handleViewportRequest(ctx, client, brokers, req)

		default:
			sendWSError(client, "unsupported websocket request type")
		}
	}
}

// writePump serializes every write to a client websocket connection.
func (c *Client) writePump() {
	defer c.conn.Close()

	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("websocket write error: %v", err)
				c.close()
				return
			}
		}
	}
}

// broadcast queues a Kafka notification for clients whose personal CPM threshold is met.
func (h *Hub) broadcast(msg []byte) {
	cpm, ok := notificationCPM(msg)
	if !ok {
		log.Printf("dropping notification without a numeric cpm value")
		return
	}

	for client := range h.clients {
		threshold := atomic.LoadInt64(&client.notificationThreshold)
		if cpm < float64(threshold) {
			continue
		}

		select {
		case <-client.done:
			h.removeClient(client)
		case client.send <- msg:
		default:
			log.Printf("dropping slow websocket client")
			h.removeClient(client)
		}
	}
}

func handleNotificationSettings(client *Client, payload []byte) {
	var req NotificationSettingsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		sendWSError(client, "invalid notification settings JSON")
		return
	}

	threshold := int64(req.CriticalCPM)
	if req.CriticalCPM != float64(threshold) || threshold < minimumNotificationThreshold || threshold > maximumNotificationThreshold {
		sendWSError(client, "notification threshold must be a whole number between 200 and 10000 CPM")
		return
	}

	atomic.StoreInt64(&client.notificationThreshold, threshold)
	if err := sendJSON(client, NotificationSettingsResponse{
		Type:        "notification_settings",
		CriticalCPM: threshold,
	}); err != nil {
		log.Printf("websocket notification settings response failed: %v", err)
	}
}

func notificationCPM(message []byte) (float64, bool) {
	var envelope struct {
		Type    string `json:"type"`
		Payload struct {
			CPM json.RawMessage `json:"cpm"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(message, &envelope); err != nil || envelope.Type != "notification" {
		return 0, false
	}

	var numeric float64
	if err := json.Unmarshal(envelope.Payload.CPM, &numeric); err == nil {
		return numeric, true
	}

	var text string
	if err := json.Unmarshal(envelope.Payload.CPM, &text); err != nil {
		return 0, false
	}

	parsed, err := strconv.ParseFloat(text, 64)
	return parsed, err == nil
}

// removeClient unregisters a client and closes its outbound queue.
func (h *Hub) removeClient(client *Client) {
	if _, ok := h.clients[client]; !ok {
		return
	}

	delete(h.clients, client)
	client.close()
}

// close signals the client writer to stop and closes the websocket once.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

// handleViewportRequest fetches the current viewport snapshot first, then streams live updates for the same viewport until cancelled.
func handleViewportRequest(ctx context.Context, client *Client, brokers string, req ViewportRequest) {
	resolution := utility.ZoomToH3Resolution(req.Zoom)
	topic := topicForResolution(resolution)

	cells, err := utility.CellsForViewport(
		req.Bounds.North,
		req.Bounds.East,
		req.Bounds.South,
		req.Bounds.West,
		req.Zoom,
	)
	if err != nil {
		sendWSError(client, "failed to calculate viewport cells: "+err.Error())
		return
	}

	wanted := makeCellSet(cells)
	liveWanted := makeCellSet(cells)

	err = kafka.FetchViewport(ctx, brokers, topic, wanted, func(msg *confluentKafka.Message) error {
		return sendJSON(client, snapshotPointResponse(msg))
	})
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		sendWSError(client, "failed to fetch viewport points: "+err.Error())
		return
	}

	sendViewportComplete(client, req.Zoom, resolution)

	err = kafka.LiveViewport(ctx, brokers, topic, liveWanted, func(msg *confluentKafka.Message) error {
		return sendJSON(client, livePointResponse(msg))
	})
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		sendWSError(client, "failed to stream live viewport points: "+err.Error())
	}
}

// makeCellSet converts viewport cells into a lookup set.
func makeCellSet(cells []string) map[string]struct{} {
	wanted := make(map[string]struct{}, len(cells))
	for _, cell := range cells {
		wanted[cell] = struct{}{}
	}
	return wanted
}

// snapshotPointResponse converts a Kafka message into a websocket payload.
func snapshotPointResponse(msg *confluentKafka.Message) map[string]any {
	if msg == nil {
		return map[string]any{"type": "snapshot_point"}
	}

	return map[string]any{
		"type":      "snapshot_point",
		"topic":     safeTopic(msg),
		"partition": msg.TopicPartition.Partition,
		"offset":    msg.TopicPartition.Offset,
		"cell":      string(msg.Key),
		"payload":   json.RawMessage(msg.Value),
	}
}

// livePointResponse converts a Kafka message into a live websocket payload.
func livePointResponse(msg *confluentKafka.Message) map[string]any {
	if msg == nil {
		return map[string]any{"type": "live_point"}
	}

	return map[string]any{
		"type":      "update_point",
		"topic":     safeTopic(msg),
		"partition": msg.TopicPartition.Partition,
		"offset":    msg.TopicPartition.Offset,
		"cell":      string(msg.Key),
		"payload":   json.RawMessage(msg.Value),
	}
}

// notificationResponse converts a Kafka hotspot message into a websocket notification payload.
func notificationResponse(msg *confluentKafka.Message) NotificationResponse {
	if msg == nil {
		return NotificationResponse{Type: "notification"}
	}

	return NotificationResponse{
		Type:      "notification",
		Topic:     safeTopic(msg),
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       string(msg.Key),
		Payload:   json.RawMessage(msg.Value),
	}
}

// sendViewportComplete sends the viewport completion response.
func sendViewportComplete(client *Client, zoom int, resolution int) {
	if err := sendJSON(client, H3ViewportResponse{Type: "viewport_h3", Zoom: zoom, Resolution: resolution}); err != nil {
		log.Printf("websocket viewport response failed: %v", err)
	}
}

// sendWSError sends a websocket error response.
func sendWSError(client *Client, message string) {
	if err := sendJSON(client, WSErrorResponse{Type: "error", Message: message}); err != nil {
		log.Printf("websocket error response failed: %v", err)
	}
}

// sendJSON marshals a value and queues it for the client's single websocket writer.
func sendJSON(client *Client, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}

	select {
	case <-client.done:
		return fmt.Errorf("websocket client closed")
	case client.send <- payload:
		return nil
	default:
		return fmt.Errorf("websocket client send queue full")
	}
}

// safeTopic returns the message topic without dereferencing a nil pointer.
func safeTopic(msg *confluentKafka.Message) string {
	if msg == nil || msg.TopicPartition.Topic == nil {
		return ""
	}
	return *msg.TopicPartition.Topic
}

// topicForResolution returns the Kafka topic name for an H3 resolution.
func topicForResolution(resolution int) string {
	return fmt.Sprintf("safecast-cells-r%d", resolution)
}
