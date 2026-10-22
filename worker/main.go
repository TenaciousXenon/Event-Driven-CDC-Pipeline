package main

import (
	"context"
	"distributed-cdc/worker/consumer"
	"distributed-cdc/worker/elastic"
	"distributed-cdc/worker/sinks"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func run(ctx context.Context) error {
	kind := env("SINK", "elasticsearch")
	var sink sinks.Sink
	switch kind {
	case "elasticsearch":
		sink = &sinks.Elasticsearch{Client: elastic.New(env("ELASTICSEARCH_URL", "http://elasticsearch:9200")), Alias: env("ES_WRITE_ALIAS", "events-write")}
	case "redis":
		c := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "redis:6379"), ContextTimeoutEnabled: true})
		defer c.Close()
		sink = &sinks.Redis{Client: c, Prefix: env("REDIS_PREFIX", "event:")}
	default:
		return fmt.Errorf("unsupported SINK %q", kind)
	}
	brokers := strings.Split(env("REDPANDA_BROKERS", "redpanda:9092"), ",")
	group := env("CONSUMER_GROUP", "cdc-"+kind)
	topic := env("CDC_TOPIC", "app.public.events")
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, CommitInterval: 0, MaxBytes: 10e6})
	defer reader.Close()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: env("DLQ_TOPIC", topic+"."+kind+".dlq"), RequiredAcks: kafka.RequireAll, Async: false, WriteTimeout: 10 * time.Second, AllowAutoTopicCreation: false}
	defer writer.Close()
	worker := consumer.Worker{Reader: reader, DLQ: writer, Sink: sink, SinkName: kind, Group: group, Attempts: 5, Backoff: time.Second}
	slog.Info("worker started", "sink", kind, "group", group)
	return worker.Run(ctx)
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	canceled := ctx.Err() != nil
	stop()
	if err != nil && !canceled {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
