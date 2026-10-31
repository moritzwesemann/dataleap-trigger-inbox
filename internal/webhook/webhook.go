package webhook

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const maxBody = 1 << 20

var (
	ErrNotJSON   = errors.New("body must be JSON")
	ErrNoEventID = errors.New("missing event id: send X-Event-ID or an id field")
)

type Delivery struct {
	AgentID string
	EventID string
	Payload []byte
}

func Read(w http.ResponseWriter, r *http.Request) (Delivery, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		return Delivery{}, err
	}
	if !json.Valid(body) {
		return Delivery{}, ErrNotJSON
	}
	id := eventID(r.Header, body)
	if id == "" {
		return Delivery{}, ErrNoEventID
	}
	return Delivery{AgentID: r.PathValue("agent_id"), EventID: id, Payload: body}, nil
}

func eventID(h http.Header, body []byte) string {
	if id := h.Get("X-Event-ID"); id != "" {
		return id
	}
	var b struct {
		ID      string `json:"id"`
		EventID string `json:"event_id"`
	}
	if json.Unmarshal(body, &b) != nil {
		return ""
	}
	if b.EventID != "" {
		return b.EventID
	}
	return b.ID
}
