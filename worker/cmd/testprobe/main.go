// testprobe is a test-only network client. It is built only by Dockerfile.test.
// It has no listening ports and communicates through one JSON request on stdin.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"distributed-cdc/worker/elastic"
	"distributed-cdc/worker/processing"
	"distributed-cdc/worker/sinks"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type request struct {
	Action  string           `json:"action"`
	Topic   string           `json:"topic"`
	Group   string           `json:"group"`
	Key     string           `json:"key"`
	Value   *string          `json:"value"`
	Offset  int64            `json:"offset"`
	Limit   int              `json:"limit"`
	Service string           `json:"service"`
	Method  string           `json:"method"`
	Path    string           `json:"path"`
	Body    json.RawMessage  `json:"body"`
	Event   processing.Event `json:"event"`
}

func run(ctx context.Context, q request) (any, error) {
	client := &kafka.Client{Addr: kafka.TCP("redpanda:9092"), Timeout: 8 * time.Second}
	switch q.Action {
	case "produce":
		var value kafka.Bytes
		if q.Value != nil {
			value = kafka.NewBytes([]byte(*q.Value))
		}
		res, err := client.Produce(ctx, &kafka.ProduceRequest{Topic: q.Topic, Partition: 0, RequiredAcks: kafka.RequireAll, Records: kafka.NewRecordReader(kafka.Record{Key: kafka.NewBytes([]byte(q.Key)), Value: value})})
		if err != nil {
			return nil, err
		}
		if res.Error != nil {
			return nil, res.Error
		}
		if len(res.RecordErrors) != 0 {
			return nil, fmt.Errorf("record errors: %v", res.RecordErrors)
		}
		return map[string]any{"offset": res.BaseOffset}, nil
	case "offset":
		res, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: q.Group, Topics: map[string][]int{q.Topic: {0}}})
		if err != nil {
			return nil, err
		}
		if errors.Is(res.Error, kafka.GroupIdNotFound) {
			return map[string]any{"offset": int64(-1)}, nil
		}
		if res.Error != nil {
			return nil, res.Error
		}
		parts := res.Topics[q.Topic]
		if len(parts) != 1 {
			return nil, fmt.Errorf("missing partition")
		}
		if parts[0].Error != nil {
			return nil, parts[0].Error
		}
		return map[string]any{"offset": parts[0].CommittedOffset}, nil
	case "bounds":
		conn, err := kafka.DialLeader(ctx, "tcp", "redpanda:9092", q.Topic, 0)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(8 * time.Second))
		first, last, err := conn.ReadOffsets()
		return map[string]any{"first": first, "last": last}, err
	case "read":
		if q.Limit < 1 || q.Limit > 500 {
			return nil, fmt.Errorf("limit must be 1..500")
		}
		r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"redpanda:9092"}, Topic: q.Topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second})
		defer r.Close()
		if err := r.SetOffset(q.Offset); err != nil {
			return nil, err
		}
		records := []map[string]any{}
		for i := 0; i < q.Limit; i++ {
			m, err := r.FetchMessage(ctx)
			if err != nil {
				return nil, err
			}
			records = append(records, map[string]any{"offset": m.Offset, "key": m.Key, "value": m.Value})
		}
		return records, nil
	case "redis":
		c := redis.NewClient(&redis.Options{Addr: "redis:6379", ContextTimeoutEnabled: true})
		defer c.Close()
		return c.HGetAll(ctx, q.Key).Result()
	case "apply":
		if q.Service == "redis" {
			c := redis.NewClient(&redis.Options{Addr: "redis:6379", ContextTimeoutEnabled: true})
			defer c.Close()
			s := sinks.Redis{Client: c, Prefix: "event:"}
			return map[string]any{"ok": true}, s.Apply(ctx, q.Event)
		}
		if q.Service != "elasticsearch" {
			return nil, fmt.Errorf("unsupported sink")
		}
		s := sinks.Elasticsearch{Client: elastic.New("http://elasticsearch:9200"), Alias: "events-write"}
		return map[string]any{"ok": true}, s.Apply(ctx, q.Event)
	case "http":
		base := ""
		switch q.Service {
		case "elasticsearch":
			base = "http://elasticsearch:9200"
		case "debezium":
			base = "http://debezium:8083"
		default:
			return nil, fmt.Errorf("unsupported HTTP service")
		}
		return requestHTTP(ctx, base, q)
	default:
		return nil, fmt.Errorf("unknown action %q", q.Action)
	}
}
func requestHTTP(ctx context.Context, base string, q request) (any, error) {
	if q.Path == "" || q.Path[0] != '/' {
		return nil, fmt.Errorf("absolute request path required")
	}
	var requestBody io.Reader
	if len(q.Body) > 0 && !bytes.Equal(bytes.TrimSpace(q.Body), []byte("null")) {
		requestBody = bytes.NewReader(q.Body)
	}
	req, err := http.NewRequestWithContext(ctx, q.Method, base+q.Path, requestBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	h := http.Client{Timeout: 8 * time.Second}
	res, err := h.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var body any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&body) != nil {
		body = string(raw)
	}
	return map[string]any{"status": res.StatusCode, "body": body}, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	var q request
	decoder := json.NewDecoder(os.Stdin)
	decoder.UseNumber()
	if err := decoder.Decode(&q); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	res, err := run(ctx, q)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
