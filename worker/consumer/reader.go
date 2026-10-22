package consumer

import (
	"context"
	"distributed-cdc/worker/processing"
	"distributed-cdc/worker/sinks"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"time"
)

type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}
type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}
type Worker struct {
	Reader          Reader
	DLQ             Writer
	Sink            sinks.Sink
	SinkName, Group string
	Attempts        int
	Backoff         time.Duration
}
type DLQRecord struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       []byte    `json:"key_base64"`
	Value     []byte    `json:"value_base64"`
	Sink      string    `json:"sink"`
	Group     string    `json:"group"`
	Error     string    `json:"error"`
	Attempts  int       `json:"attempts"`
	FailedAt  time.Time `json:"failed_at"`
}

func (w *Worker) Handle(ctx context.Context, m kafka.Message) error {
	e, decodeErr := processing.Decode(m.Key, m.Value, m.Offset)
	attempts, err := processing.Retry(ctx, w.Attempts, w.Backoff, func() error {
		if decodeErr != nil {
			return processing.Permanent(decodeErr)
		}
		if e == nil {
			return nil
		}
		return w.Sink.Apply(ctx, *e)
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var permanent *processing.PermanentError
		if !errors.As(err, &permanent) {
			return fmt.Errorf("sink unavailable, offset not committed: %w", err)
		}
		raw, marshalErr := json.Marshal(DLQRecord{m.Topic, m.Partition, m.Offset, m.Key, m.Value, w.SinkName, w.Group, err.Error(), attempts, time.Now().UTC()})
		if marshalErr != nil {
			return marshalErr
		}
		// Stable identity permits downstream deduplication after ambiguous acknowledgements.
		key := fmt.Sprintf("%s/%s/%d/%d", w.SinkName, m.Topic, m.Partition, m.Offset)
		if err := w.DLQ.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: raw}); err != nil {
			return fmt.Errorf("DLQ failed, offset not committed: %w", err)
		}
		slog.Error("event quarantined", "sink", w.SinkName, "partition", m.Partition, "offset", m.Offset, "error", permanent)
	}
	if err := w.Reader.CommitMessages(ctx, m); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	slog.Info("offset committed", "sink", w.SinkName, "partition", m.Partition, "offset", m.Offset)
	return nil
}
func (w *Worker) Run(ctx context.Context) error {
	// Strictly sequential: committing a later partition offset also commits earlier ones.
	for {
		m, err := w.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := w.Handle(ctx, m); err != nil {
			return err
		}
	}
}
