package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

const notificationPartitions = int32(3)

// ConsumeNotifications reads the notification topic from the current end of all
// known hotspot partitions. It intentionally does not replay old hotspot messages;
// only messages produced after the middleware starts are forwarded to the frontend.
func ConsumeNotifications(
	ctx context.Context,
	brokers string,
	topic string,
	groupID string,
	send func(*confluentKafka.Message) error,
) error {
	consumer, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupID,
		"enable.auto.commit":       false,
		"auto.offset.reset":        "latest",
		"go.events.channel.enable": false,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	assignments := make([]confluentKafka.TopicPartition, 0, notificationPartitions)
	for partition := int32(0); partition < notificationPartitions; partition++ {
		_, high, err := consumer.QueryWatermarkOffsets(topic, partition, 5000)
		if err != nil {
			return fmt.Errorf("query notification watermarks for %s[%d]: %w", topic, partition, err)
		}

		assignments = append(assignments, confluentKafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    confluentKafka.Offset(high),
		})
	}

	if err := consumer.Assign(assignments); err != nil {
		return fmt.Errorf("assign notification consumer to latest offsets for %s: %w", topic, err)
	}

	const pollMs = 250

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		ev := consumer.Poll(pollMs)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *confluentKafka.Message:
			if !isCriticalHotspot(e) {
				continue
			}
			if err := send(e); err != nil {
				return err
			}

		case confluentKafka.Error:
			if e.IsRetriable() {
				log.Printf("retriable notification consumer error: %v", e)
				continue
			}
			return e
		}
	}
}

func isCriticalHotspot(msg *confluentKafka.Message) bool {
	if msg == nil || len(msg.Value) == 0 {
		return false
	}

	decoder := json.NewDecoder(bytes.NewReader(msg.Value))
	decoder.UseNumber()

	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		log.Printf("dropping invalid hotspot JSON: %v", err)
		return false
	}

	if strings.EqualFold(fmt.Sprint(payload["status"]), "critical") {
		return true
	}

	cpm, ok := numericField(payload, "cpm")
	if !ok {
		cpm, ok = numericField(payload, "radiation")
	}
	return ok && cpm > 200
}

func numericField(payload map[string]any, key string) (float64, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return 0, false
	}

	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case float64:
		return typed, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(typed)), 64)
		return parsed, err == nil
	}
}
