package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	ws "collaborating.tuhh.de/e-19/teaching/bd26_project_f1_c/internal/websocket"
)

const (
	defaultKafkaBrokers             = "localhost:29092"
	defaultKafkaTopic               = "safecast-cells-r2,safecast-cells-r4,safecast-cells-r6,safecast-cells-r8,safecast-cells-r12"
	defaultKafkaGroupID             = "group1"
	defaultKafkaNotificationTopic   = "safecast-hotspots"
	defaultKafkaNotificationGroupID = "middleware-notifications"
	defaultPort                     = "8080"
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", defaultKafkaBrokers)
	topics := parseTopics(getEnv("KAFKA_TOPICS", defaultKafkaTopic))
	groupID := getEnv("KAFKA_GROUP_ID", defaultKafkaGroupID)
	notificationTopic := getEnv("KAFKA_NOTIFICATION_TOPIC", defaultKafkaNotificationTopic)
	notificationGroupID := getEnv("KAFKA_NOTIFICATION_GROUP_ID", defaultKafkaNotificationGroupID)
	port := getEnv("PORT", defaultPort)

	hub := ws.NewHub()

	go hub.Run()
	hub.StartNotifications(context.Background(), brokers, notificationTopic, notificationGroupID)

	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		hub.HandleWebSocket(brokers, w, r)
	})
	//http.HandleFunc("/testing", ws.HandleTestingWebSocket)
	//http.HandleFunc("/h3-test", utility.HandleViewportRequest)

	addr := listenAddr(port)
	log.Printf("API server listening on %s", addr)
	log.Printf("Kafka consumer configured with brokers=%q topics=%v group_id=%q", brokers, topics, groupID)
	log.Printf("Kafka notification topic configured as %q with group_id=%q", notificationTopic, notificationGroupID)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write([]byte("bd26 middleware is running\n"))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

// Splits comma-separated Kafka topic names.
func parseTopics(value string) []string {
	rawTopics := strings.Split(value, ",")
	topics := make([]string, 0, len(rawTopics))
	for _, rawTopic := range rawTopics {
		if topic := strings.TrimSpace(rawTopic); topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

func listenAddr(port string) string {
	if strings.HasPrefix(port, ":") {
		return port
	}
	return ":" + port
}
