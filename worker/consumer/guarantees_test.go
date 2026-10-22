package consumer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"distributed-cdc/worker/processing"
	"github.com/segmentio/kafka-go"
)

type journal struct {
	steps                      []string
	sinkErr, dlqErr, commitErr error
	message                    kafka.Message
}

func (j *journal) Apply(context.Context, processing.Event) error {
	j.steps = append(j.steps, "apply")
	return j.sinkErr
}
func (j *journal) WriteMessages(_ context.Context, m ...kafka.Message) error {
	j.steps = append(j.steps, "dlq")
	j.message = m[0]
	return j.dlqErr
}
func (j *journal) CommitMessages(context.Context, ...kafka.Message) error {
	j.steps = append(j.steps, "commit")
	return j.commitErr
}
func (j *journal) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, errors.New("fetch failed")
}
func TestDeliveryOrderingAndDLQEvidence(t *testing.T) {
	msg := kafka.Message{Topic: "source", Partition: 2, Offset: 91, Key: []byte(`{"payload":{"id":17}}`), Value: []byte(`{"payload":{"op":"d"}}`)}
	for _, poison := range []bool{false, true} {
		j := &journal{}
		if poison {
			j.sinkErr = processing.Permanent(errors.New("bad mapping"))
		}
		w := Worker{Reader: j, DLQ: j, Sink: j, SinkName: "redis", Group: "cache", Attempts: 5}
		if err := w.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
		want := []string{"apply", "commit"}
		if poison {
			want = []string{"apply", "dlq", "commit"}
		}
		if !reflect.DeepEqual(j.steps, want) {
			t.Fatalf("order=%v", j.steps)
		}
		if poison {
			var raw map[string]any
			if err := json.Unmarshal(j.message.Value, &raw); err != nil {
				t.Fatal(err)
			}
			if string(j.message.Key) != "redis/source/2/91" || raw["sink"] != "redis" || raw["group"] != "cache" || raw["offset"] != float64(91) || raw["attempts"] != float64(1) {
				t.Fatalf("missing diagnostic evidence: %s", j.message.Value)
			}
			if raw["key_base64"] != base64.StdEncoding.EncodeToString(msg.Key) || raw["value_base64"] != base64.StdEncoding.EncodeToString(msg.Value) {
				t.Fatal("original bytes not retained")
			}
		}
	}
}
func TestWriteSucceededCommitFailedThenRedelivery(t *testing.T) {
	j := &journal{commitErr: errors.New("lost commit acknowledgement")}
	w := Worker{Reader: j, DLQ: j, Sink: j, Attempts: 2}
	msg := kafka.Message{Key: []byte(`{"payload":{"id":1}}`), Value: []byte(`{"payload":{"op":"d"}}`)}
	if err := w.Handle(context.Background(), msg); err == nil {
		t.Fatal("commit failure hidden")
	}
	j.commitErr = nil
	if err := w.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(j.steps, []string{"apply", "commit", "apply", "commit"}) {
		t.Fatal(j.steps)
	}
}
func TestDLQAcknowledgedCommitFailedMayDuplicate(t *testing.T) {
	j := &journal{commitErr: errors.New("commit unavailable")}
	w := Worker{Reader: j, DLQ: j, Sink: j, SinkName: "elasticsearch", Attempts: 2}
	msg := kafka.Message{Topic: "source", Offset: 7, Value: []byte("not JSON")}
	if err := w.Handle(context.Background(), msg); err == nil {
		t.Fatal("commit failure hidden")
	}
	firstKey := string(j.message.Key)
	j.commitErr = nil
	if err := w.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if string(j.message.Key) != firstKey || !reflect.DeepEqual(j.steps, []string{"dlq", "commit", "dlq", "commit"}) {
		t.Fatal("DLQ dedup identity changed")
	}
}
func TestRunStopsBeforeLaterCommit(t *testing.T) {
	r := &queuedReader{messages: []kafka.Message{
		{Offset: 10, Key: []byte(`{"payload":{"id":1}}`), Value: []byte(`{"payload":{"op":"d"}}`)},
		{Offset: 11, Value: nil},
	}}
	w := Worker{Reader: r, DLQ: &fakeWriter{}, Sink: &fakeSink{err: errors.New("outage")}, Attempts: 3}
	if err := w.Run(context.Background()); err == nil || r.fetches != 1 || r.commits != 0 {
		t.Fatalf("err=%v fetches=%d commits=%d", err, r.fetches, r.commits)
	}
}

type queuedReader struct {
	messages         []kafka.Message
	fetches, commits int
}

func (r *queuedReader) FetchMessage(context.Context) (kafka.Message, error) {
	if r.fetches >= len(r.messages) {
		return kafka.Message{}, errors.New("done")
	}
	m := r.messages[r.fetches]
	r.fetches++
	return m, nil
}
func (r *queuedReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	return nil
}
func TestRunPreservesPartitionOrder(t *testing.T) {
	r := &queuedReader{messages: []kafka.Message{{Partition: 0, Offset: 1}, {Partition: 1, Offset: 9}, {Partition: 0, Offset: 2}}}
	w := Worker{Reader: r, DLQ: &fakeWriter{}, Sink: &fakeSink{}, Attempts: 1}
	if err := w.Run(context.Background()); err == nil || r.commits != 3 {
		t.Fatal("did not handle all tombstones in order")
	}
}
