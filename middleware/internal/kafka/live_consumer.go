package kafka

import (
	"context"
	"fmt"
	"log"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type LiveConsumer struct {
	consumer *confluentKafka.Consumer
}

func NewLiveConsumer(brokers string, groupID string) (*LiveConsumer, error) {
	c, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 "live_consumer",
		"enable.auto.commit":       true,
		"auto.offset.reset":        "latest",
		"go.events.channel.enable": false,
	})
	if err != nil {
		return nil, err
	}

	return &LiveConsumer{consumer: c}, nil
}

func (l *LiveConsumer) Close() error {
	return l.consumer.Close()
}

// LiveViewport consumes only new Kafka messages from the selected topic and forwards
// messages whose Kafka key/H3 cell is inside the currently requested viewport.
func LiveViewport(
	ctx context.Context,
	brokers string,
	topic string,
	wanted map[string]struct{},
	send func(*confluentKafka.Message) error,
) error {
	consumer, err := NewLiveConsumer(brokers, "")
	if err != nil {
		return err
	}
	defer consumer.Close()

	if len(wanted) == 0 {
		return nil
	}

	if err := consumer.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("subscribe live consumer to %s: %w", topic, err)
	}

	const pollMs = 100

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		ev := consumer.consumer.Poll(pollMs)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *confluentKafka.Message:
			key, err := extractH3Key(e.Key)
			if err != nil {
				continue
			}

			if _, ok := wanted[key]; !ok {
				continue
			}

			if err := send(e); err != nil {
				return err
			}

		case confluentKafka.Error:
			if e.IsRetriable() {
				log.Printf("retriable live consumer error: %v", e)
				continue
			}
			return e
		}
	}
}
