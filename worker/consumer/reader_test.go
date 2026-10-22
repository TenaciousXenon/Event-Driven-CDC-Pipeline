package consumer

import (
	"context"
	"distributed-cdc/worker/processing"
	"errors"
	"github.com/segmentio/kafka-go"
	"testing"
)

type fakeReader struct {
	commits int
	err     error
}

func (r *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, errors.New("unused")
}
func (r *fakeReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	return r.err
}

type fakeWriter struct {
	writes int
	err    error
}

func (w *fakeWriter) WriteMessages(context.Context, ...kafka.Message) error { w.writes++; return w.err }

type fakeSink struct {
	calls int
	err   error
}

func (s *fakeSink) Apply(context.Context, processing.Event) error { s.calls++; return s.err }
func TestCommitBarrier(t *testing.T) {
	bad := errors.New("failure")
	for _, tt := range []struct {
		name                       string
		value                      []byte
		sinkErr, dlqErr, commitErr error
		commits, writes            int
		wantErr                    bool
	}{
		{"success", []byte(`{"payload":{"op":"c","after":{}}}`), nil, nil, nil, 1, 0, false},
		{"tombstone", nil, nil, nil, nil, 1, 0, false},
		{"decode poison", []byte(`bad`), nil, nil, nil, 1, 1, false},
		{"DLQ unavailable", []byte(`bad`), nil, bad, nil, 0, 1, true},
		{"sink unavailable", []byte(`{"payload":{"op":"d"}}`), bad, nil, nil, 0, 0, true},
		{"mapping poison", []byte(`{"payload":{"op":"d"}}`), processing.Permanent(bad), nil, nil, 1, 1, false},
		{"commit failure", nil, nil, nil, bad, 1, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &fakeReader{err: tt.commitErr}
			d := &fakeWriter{err: tt.dlqErr}
			s := &fakeSink{err: tt.sinkErr}
			w := Worker{Reader: r, DLQ: d, Sink: s, Attempts: 2}
			err := w.Handle(context.Background(), kafka.Message{Key: []byte(`{"payload":{"id":1}}`), Value: tt.value})
			if (err != nil) != tt.wantErr || r.commits != tt.commits || d.writes != tt.writes {
				t.Fatalf("err=%v commits=%d writes=%d", err, r.commits, d.writes)
			}
		})
	}
}
func TestCancelledNotQuarantined(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &fakeReader{}
	d := &fakeWriter{}
	w := Worker{Reader: r, DLQ: d, Sink: &fakeSink{}, Attempts: 2}
	if err := w.Handle(ctx, kafka.Message{Value: []byte(`bad`)}); err == nil || r.commits != 0 || d.writes != 0 {
		t.Fatal("cancel committed or quarantined")
	}
}
