package sinks

import (
	"context"
	"distributed-cdc/worker/elastic"
	"distributed-cdc/worker/processing"
	"errors"
	"fmt"
	"net/url"
)

type Elasticsearch struct {
	Client *elastic.Client
	Alias  string
}

func (s *Elasticsearch) Apply(ctx context.Context, e processing.Event) error {
	// Persist soft tombstones indefinitely: ES hard-delete version metadata expires.
	body := map[string]any{"deleted": e.Deleted, "data": e.Data}
	path := fmt.Sprintf("/%s/_doc/%s?require_alias=true&version=%d&version_type=external", url.PathEscape(s.Alias), url.PathEscape(e.ID), e.Version)
	err := s.Client.Do(ctx, "PUT", path, body, nil)
	var h *elastic.HTTPError
	if errors.As(err, &h) {
		if h.Status == 409 {
			return nil
		} // Duplicate or stale delivery.
		if h.Status == 400 {
			return processing.Permanent(err)
		}
	}
	return err
}
