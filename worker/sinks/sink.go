package sinks

import (
	"context"
	"distributed-cdc/worker/processing"
)

type Sink interface {
	Apply(context.Context, processing.Event) error
}
