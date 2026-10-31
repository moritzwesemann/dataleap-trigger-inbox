package inbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"triggerinbox/internal/store"
	"triggerinbox/internal/webhook"
)

type Inbox struct {
	store       *store.Store
	log         *slog.Logger
	SaveTimeout time.Duration
	queue       chan pending
}

func New(s *store.Store, log *slog.Logger) *Inbox {
	return &Inbox{store: s, log: log, SaveTimeout: 2 * time.Second, queue: make(chan pending, 10_000)}
}

func (in *Inbox) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /triggers/{agent_id}", in.receive)
	mux.HandleFunc("GET /agents/{agent_id}/events", in.list)
	mux.HandleFunc("GET /stats", in.stats)
	return mux
}

func (in *Inbox) receive(w http.ResponseWriter, r *http.Request) {
	d, err := webhook.Read(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), in.SaveTimeout)
	defer cancel()
	isNew, err := in.save(ctx, store.NewEvent{AgentID: d.AgentID, EventID: d.EventID, Payload: d.Payload})
	if err != nil {
		in.log.Error("save failed", "agent_id", d.AgentID, "event_id", d.EventID, "err", err)
		http.Error(w, "could not save event, retry later", http.StatusServiceUnavailable)
		return
	}
	result := "stored"
	if !isNew {
		result = "duplicate"
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": result})
}

func (in *Inbox) list(w http.ResponseWriter, r *http.Request) {
	status := store.Status(r.URL.Query().Get("status"))
	switch status {
	case "":
		status = store.Failed
	case store.Waiting, store.Running, store.Done, store.Failed:
	default:
		http.Error(w, "status must be waiting, running, done or failed", http.StatusBadRequest)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			http.Error(w, "limit must be between 1 and 1000", http.StatusBadRequest)
			return
		}
		limit = n
	}
	events, err := in.store.List(r.Context(), r.PathValue("agent_id"), status, limit)
	if err != nil {
		in.log.Error("list failed", "err", err)
		http.Error(w, "could not list events", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (in *Inbox) stats(w http.ResponseWriter, r *http.Request) {
	st, err := in.store.Stats(r.Context())
	if err != nil {
		in.log.Error("stats failed", "err", err)
		http.Error(w, "could not read stats", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
