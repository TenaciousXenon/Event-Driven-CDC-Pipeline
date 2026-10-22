
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type CDCEvent struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Source struct {
		LSN int64 `json:"lsn"`
	} `json:"source"`
	Op string `json:"op"`
}

type CDCEnvelope struct {
	Payload *CDCEvent `json:"payload"`
}

type CDCKey struct {
	Payload struct {
		ID int64 `json:"id"`
	} `json:"payload"`
}

const maxAttempts = 5

type DLQRecord struct {
    OriginalTopic     string    `json:"original_topic"`
    OriginalPartition int       `json:"original_partition"`
    OriginalOffset    int64     `json:"original_offset"`
    KeyBase64         string    `json:"key_base64"`
    ValueBase64       string    `json:"value_base64"`
    Error             string    `json:"error"`
    Attempts          int       `json:"attempts"`
    FailedAt          time.Time `json:"failed_at"`
}

// need to count for poision pill in future
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

var redisClient *redis.Client

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Apply a versioned Elasticsearch operation.
// Returns stale=true when Elasticsearch rejects an older version.
func writeElastic(
	ctx context.Context,
	id string,
	version int64,
	op string,
	data map[string]interface{},
) (bool, error) {
	base := env(
		"ELASTICSEARCH_URL",
		"http://elasticsearch:9200",
	)

	endpoint := fmt.Sprintf(
		"%s/events/_doc/%s?version=%d&version_type=external_gte",
		base,
		url.PathEscape(id),
		version,
	)

	method := http.MethodPut
	var body io.Reader

	if op == "d" {
		method = http.MethodDelete
	} else {
		raw, err := json.Marshal(data)
		if err != nil {
			return false, err
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(
		ctx, method, endpoint, body,
	)
	if err != nil {
		return false, err
	}

	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return true, nil
	}

	// A DELETE of an absent document can return 404.
	if op == "d" && resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf(
			"Elasticsearch returned %s",
			resp.Status,
		)
	}

	return false, nil
}

// Atomically apply Redis changes only when the incoming
// version is not older than the stored version.
var cacheScript = redis.NewScript(`
local previous = redis.call("GET", KEYS[2])
local incoming = ARGV[1]

if previous then
    local older = (#incoming < #previous) or
        (#incoming == #previous and incoming < previous)

    if older then
        return 0
    end
end

if ARGV[2] == "d" then
    redis.call("DEL", KEYS[1])
else
    redis.call("SET", KEYS[1], ARGV[3])
end

redis.call("SET", KEYS[2], incoming)
return 1
`)

func writeRedis(
	ctx context.Context,
	id string,
	version int64,
	op string,
	data map[string]interface{},
) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return cacheScript.Run(
		ctx,
		redisClient,
		[]string{
			"event:" + id,
			"event-version:" + id,
		},
		strconv.FormatInt(version, 10),
		op,
		string(raw),
	).Err()
}

func processMessage(ctx context.Context, msg kafka.Message) error {
	// Debezium delete tombstones have null values.
	if msg.Value == nil {
		return nil
	}

	var envelope CDCEnvelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}

	if envelope.Payload == nil {
		return fmt.Errorf("missing CDC payload")
	}

	event := envelope.Payload

	var key CDCKey
	if err := json.Unmarshal(msg.Key, &key); err != nil {
		return fmt.Errorf("decode key: %w", err)
	}

	id := strconv.FormatInt(key.Payload.ID, 10)
	version := event.Source.LSN

	if version <= 0 {
		return fmt.Errorf("missing valid LSN for id %s", id)
	}

	switch event.Op {
	case "c", "u", "r":
		if event.After == nil {
			return fmt.Errorf("missing after image for id %s", id)
		}

	case "d":
		// Delete uses the same document ID.

	default:
		return fmt.Errorf("unsupported CDC operation: %q", event.Op)
	}

	stale, err := writeElastic(
		ctx, id, version, event.Op, event.After,
	)
	if err != nil {
		return err
	}

	if stale {
		log.Printf("Skipped stale event id=%s version=%d", id, version)
		return nil
	}

	if err := writeRedis(
		ctx, id, version, event.Op, event.After,
	); err != nil {
		return err
	}

	log.Printf(
		"Processed id=%s op=%s lsn=%d",
		id, event.Op, version,
	)
	return nil
}


// Try processing the same message up to five times.
// Delay doubles after each failure.
func processWithRetry(
    ctx context.Context,
    msg kafka.Message,
) error {
    delay := time.Second

    for attempt := 1; attempt <= maxAttempts; attempt++ {
        err := processMessage(ctx, msg)

        if err == nil {
            return nil
        }

        // Don't send interrupted shutdowns to the DLQ.
        if ctx.Err() != nil {
            return ctx.Err()
        }

        log.Printf(
            "Processing failed: partition=%d offset=%d attempt=%d/%d error=%v",
            msg.Partition,
            msg.Offset,
            attempt,
            maxAttempts,
            err,
        )

        if attempt == maxAttempts {
            return fmt.Errorf(
                "processing failed after %d attempts: %w",
                maxAttempts,
                err,
            )
        }

        timer := time.NewTimer(delay)

        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()

        case <-timer.C:
        }

        delay *= 2
    }

    return nil
}

// Publish the failed message and diagnostic information
// to the dead-letter topic.
func sendToDLQ(
    ctx context.Context,
    writer *kafka.Writer,
    msg kafka.Message,
    processingErr error,
) error {
    record := DLQRecord{
        OriginalTopic:     msg.Topic,
        OriginalPartition: msg.Partition,
        OriginalOffset:    msg.Offset,
        KeyBase64:         base64.StdEncoding.EncodeToString(msg.Key),
        ValueBase64:       base64.StdEncoding.EncodeToString(msg.Value),
        Error:             processingErr.Error(),
        Attempts:          maxAttempts,
        FailedAt:          time.Now().UTC(),
    }

    payload, err := json.Marshal(record)
    if err != nil {
        return fmt.Errorf("marshal DLQ record: %w", err)
    }

    err = writer.WriteMessages(
        ctx,
        kafka.Message{
            Key:   msg.Key,
            Value: payload,
        },
    )
    if err != nil {
        return fmt.Errorf("publish DLQ record: %w", err)
    }

    return nil
}


func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	redisClient = redis.NewClient(&redis.Options{
		Addr: env("REDIS_ADDR", "redis:6379"),
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis unavailable: ", err)
	}

	dlqWriter := &kafka.Writer{
		Addr: kafka.TCP("redpanda:9092"),
		Topic: "app.public.events.dlq",
		RequiredAcks: kafka.RequireAll,
		Async: false,
	}
	defer dlqWriter.Close()	

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{
			env("REDPANDA_BROKERS", "redpanda:9092"),
		},
		Topic:          "app.public.events",
		GroupID:        env("CONSUMER_GROUP", "cdc-workers"),
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
	})
	defer reader.Close()

	log.Println("Go CDC worker started")

	
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Fetch error: %v", err)
			return
		}

		// Process with exponential backoff.
		err = processWithRetry(ctx, msg)

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			// Processing failed after all attempts.
			// Preserve the message in the DLQ.
			log.Printf(
				"Sending failed event to DLQ: partition=%d offset=%d",
				msg.Partition,
				msg.Offset,
			)

			if dlqErr := sendToDLQ(
				ctx, dlqWriter, msg, err,
			); dlqErr != nil {
				// Important: do not commit the source offset.
				log.Printf("DLQ publish failed: %v", dlqErr)
				return
			}
		}

		// Processing succeeded OR the DLQ acknowledged
		// the failed event. We can now commit.
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Offset commit failed: %v", err)
			return
		}
	}

}
