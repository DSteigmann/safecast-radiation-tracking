package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type FetchConsumer struct {
	consumer *confluentKafka.Consumer
}

func NewFetchConsumer(brokers string) (*FetchConsumer, error) {
	c, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 "fetch-consumer",
		"enable.auto.commit":       false,
		"auto.offset.reset":        "latest",
		"enable.partition.eof":     true,
		"go.events.channel.enable": false,
	})
	if err != nil {
		return nil, err
	}

	return &FetchConsumer{consumer: c}, nil
}

func (f *FetchConsumer) Close() error {
	return f.consumer.Close()
}

func FetchViewport(
	ctx context.Context,
	brokers string,
	topic string,
	wanted map[string]struct{},
	send func(*confluentKafka.Message) error,
) error {
	var (
		wg       sync.WaitGroup
		wantedMu sync.Mutex
		sendMu   sync.Mutex
		errMu    sync.Mutex
		firstErr error
	)

	for partition := int32(0); partition < 3; partition++ {
		wg.Add(1)

		go func(partition int32) {
			defer wg.Done()

			consumer, err := NewFetchConsumer(brokers)
			if err != nil {
				setFirstErr(&errMu, &firstErr, err)
				return
			}
			defer consumer.Close()

			err = consumer.FetchPartition(
				ctx,
				topic,
				partition,
				wanted,
				&wantedMu,
				send,
				&sendMu,
			)
			if err != nil && err != context.Canceled {
				setFirstErr(&errMu, &firstErr, err)
			}
		}(partition)
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return firstErr
}

func (f *FetchConsumer) FetchPartition(
	ctx context.Context,
	topic string,
	partition int32,
	wanted map[string]struct{},
	wantedMu *sync.Mutex,
	send func(*confluentKafka.Message) error,
	sendMu *sync.Mutex,
) error {
	const (
		// Adjust value cautiosly, as too high might show old values, while too low might result in a laggy experience.
		chunkSize = int64(50000)
		timeoutMs = 1000
		pollMs    = 25
	)

	low, high, err := f.consumer.QueryWatermarkOffsets(topic, partition, timeoutMs)
	if err != nil {
		return fmt.Errorf("query watermarks for %s[%d]: %w", topic, partition, err)
	}

	end := high

	for end > low {
		if err := ctx.Err(); err != nil {
			return err
		}

		if wantedEmpty(wanted, wantedMu) {
			return nil
		}

		start := end - chunkSize
		if start < low {
			start = low
		}

		err := f.consumer.Assign([]confluentKafka.TopicPartition{
			{
				Topic:     &topic,
				Partition: partition,
				Offset:    confluentKafka.Offset(start),
			},
		})
		if err != nil {
			return fmt.Errorf("assign %s[%d] at offset %d: %w", topic, partition, start, err)
		}

		for {
			if err := ctx.Err(); err != nil {
				return err
			}

			if wantedEmpty(wanted, wantedMu) {
				return nil
			}

			ev := f.consumer.Poll(pollMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *confluentKafka.Message:
				offset := int64(e.TopicPartition.Offset)
				if offset >= end {
					goto previousChunk
				}

				key, err := extractH3Key(e.Key)
				if err != nil {
					continue
				}

				wantedMu.Lock()
				_, ok := wanted[key]
				if ok {
					delete(wanted, key)
				}
				wantedMu.Unlock()

				if !ok {
					continue
				}

				sendMu.Lock()
				err = send(e)
				sendMu.Unlock()

				if err != nil {
					return err
				}

			case confluentKafka.PartitionEOF:
				goto previousChunk

			case confluentKafka.Error:
				return e
			}
		}

	previousChunk:
		end = start
	}

	return nil
}

func wantedEmpty(wanted map[string]struct{}, mu *sync.Mutex) bool {
	mu.Lock()
	defer mu.Unlock()
	return len(wanted) == 0
}

func setFirstErr(mu *sync.Mutex, target *error, err error) {
	mu.Lock()
	defer mu.Unlock()

	if *target == nil {
		*target = err
	}
}

func extractH3Key(rawKey []byte) (string, error) {
	if len(rawKey) == 0 {
		return "", fmt.Errorf("empty kafka key")
	}

	var parsed struct {
		H3 string `json:"h3"`
	}

	if err := json.Unmarshal(rawKey, &parsed); err == nil && parsed.H3 != "" {
		return parsed.H3, nil
	}

	return string(rawKey), nil
}
