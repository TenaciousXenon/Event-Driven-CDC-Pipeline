package processing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
)

// Event uses the immutable Kafka position as its per-key ordering token.
// Keys MUST stay in the same partition for the lifetime of a sink namespace.
type Event struct {
	ID      string
	Version int64
	Deleted bool
	Data    map[string]any
}

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func Permanent(err error) error         { return &PermanentError{Err: err} }

func decode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

// Decode accepts schema-enabled Debezium envelopes with signed bigint IDs.
// Kafka tombstones have zero-length values; malformed envelopes are not tombstones.
func Decode(key, value []byte, offset int64) (*Event, error) {
	if len(value) == 0 {
		return nil, nil
	}
	if offset < 0 || offset == math.MaxInt64 {
		return nil, fmt.Errorf("invalid offset")
	}
	var k struct {
		Payload *struct {
			ID json.Number `json:"id"`
		} `json:"payload"`
	}
	if err := decode(key, &k); err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	if k.Payload == nil {
		return nil, fmt.Errorf("missing key payload")
	}
	id, err := strconv.ParseInt(string(k.Payload.ID), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("key id must be a signed bigint: %w", err)
	}
	var v struct {
		Payload *struct {
			Op    string         `json:"op"`
			After map[string]any `json:"after"`
		} `json:"payload"`
	}
	if err := decode(value, &v); err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}
	if v.Payload == nil {
		return nil, fmt.Errorf("missing event payload")
	}
	e := &Event{ID: strconv.FormatInt(id, 10), Version: offset + 1}
	switch v.Payload.Op {
	case "c", "u", "r":
		if v.Payload.After == nil {
			return nil, fmt.Errorf("missing after image")
		}
		e.Data = v.Payload.After
	case "d":
		e.Deleted = true
	default:
		return nil, fmt.Errorf("unsupported operation %q", v.Payload.Op)
	}
	return e, nil
}
