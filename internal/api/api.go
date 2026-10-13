// Package api serves the job queue over HTTP.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/abuhamdeh002/taskforge/internal/job"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

// NewHandler returns the HTTP handler for the queue.
// log may be nil, in which case requests are not logged.
func NewHandler(st store.Store, log *slog.Logger) http.Handler {
	s := &server{store: st, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.cancelJob)
	mux.HandleFunc("GET /healthz", s.healthz)
	return logging(log, mux)
}

type server struct {
	store store.Store
	log   *slog.Logger
}

type createRequest struct {
	Type       string     `json:"type"`
	Payload    string     `json:"payload"`
	MaxRetries int        `json:"max_retries"`
	RunAt      *time.Time `json:"run_at"`
}

type jobResponse struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Payload    string    `json:"payload"`
	State      job.State `json:"state"`
	Attempts   int       `json:"attempts"`
	MaxRetries int       `json:"max_retries"`
	LastError  string    `json:"last_error,omitempty"`
	RunAt      time.Time `json:"run_at"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	j := job.Job{
		Type:       req.Type,
		Payload:    []byte(req.Payload),
		MaxRetries: req.MaxRetries,
	}
	if req.RunAt != nil {
		j.RunAt = *req.RunAt
	}
	created, err := s.store.Create(r.Context(), j)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("Location", "/jobs/"+created.ID)
	writeJSON(w, http.StatusCreated, toResponse(created))
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(j))
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.ListFilter{State: job.State(q.Get("status"))}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid limit %q", raw))
			return
		}
		filter.Limit = n
	}
	jobs, err := s.store.List(r.Context(), filter)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := make([]jobResponse, len(jobs))
	for i, j := range jobs {
		out[i] = toResponse(j)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Cancel(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func toResponse(j job.Job) jobResponse {
	return jobResponse{
		ID:         j.ID,
		Type:       j.Type,
		Payload:    string(j.Payload),
		State:      j.State,
		Attempts:   j.Attempts,
		MaxRetries: j.MaxRetries,
		LastError:  j.LastError,
		RunAt:      j.RunAt,
		CreatedAt:  j.CreatedAt,
		UpdatedAt:  j.UpdatedAt,
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, job.MaxPayload+4096)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON: extra data")
	}
	return nil
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, store.ErrNotCancellable):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, store.ErrInvalidID),
		errors.Is(err, job.ErrInvalidType),
		errors.Is(err, job.ErrInvalidState),
		errors.Is(err, job.ErrInvalidMaxRetries),
		errors.Is(err, job.ErrInvalidAttempts),
		errors.Is(err, job.ErrPayloadTooLarge):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, err)
	default:
		writeError(w, http.StatusInternalServerError, errors.New("internal error"))
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && status < 500 {
		// The status is already on the wire. There is nowhere better to put this.
		return
	}
}
