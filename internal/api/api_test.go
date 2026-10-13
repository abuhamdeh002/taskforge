package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/abuhamdeh002/taskforge/internal/api"
	"github.com/abuhamdeh002/taskforge/internal/store"
)

func TestStatusCodes(t *testing.T) {
	h := api.NewHandler(store.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	created, loc := postJSON(t, h, "/jobs", `{"type":"echo","payload":"hello","max_retries":2}`, http.StatusCreated)
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatal("missing id")
	}
	if loc != "/jobs/"+id {
		t.Fatalf("location = %q", loc)
	}

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "health", method: http.MethodGet, path: "/healthz", want: http.StatusOK},
		{name: "get job", method: http.MethodGet, path: "/jobs/" + id, want: http.StatusOK},
		{name: "get missing", method: http.MethodGet, path: "/jobs/missing", want: http.StatusNotFound},
		{name: "list", method: http.MethodGet, path: "/jobs?status=pending&limit=10", want: http.StatusOK},
		{name: "list bad status", method: http.MethodGet, path: "/jobs?status=nope", want: http.StatusBadRequest},
		{name: "list bad limit", method: http.MethodGet, path: "/jobs?limit=abc", want: http.StatusBadRequest},
		{name: "create bad json", method: http.MethodPost, path: "/jobs", body: "{", want: http.StatusBadRequest},
		{name: "create bad type", method: http.MethodPost, path: "/jobs", body: `{"type":""}`, want: http.StatusBadRequest},
		{name: "create unknown field", method: http.MethodPost, path: "/jobs", body: `{"type":"echo","nope":1}`, want: http.StatusBadRequest},
		{name: "cancel", method: http.MethodDelete, path: "/jobs/" + id, want: http.StatusNoContent},
		{name: "cancel again", method: http.MethodDelete, path: "/jobs/" + id, want: http.StatusNoContent},
		{name: "cancel missing", method: http.MethodDelete, path: "/jobs/missing", want: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := do(t, h, tt.method, tt.path, tt.body)
			if status != tt.want {
				t.Fatalf("status = %d, want %d", status, tt.want)
			}
		})
	}
}

func TestCancelFinishedJobConflicts(t *testing.T) {
	st := store.NewMemory()
	h := api.NewHandler(st, nil)
	res, _ := postJSON(t, h, "/jobs", `{"type":"echo","payload":"x"}`, http.StatusCreated)
	id := res["id"].(string)

	if _, err := st.ClaimNext(t.Context()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	j, err := st.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	j.State = "succeeded"
	if err := st.Update(t.Context(), j); err != nil {
		t.Fatalf("update: %v", err)
	}
	if status := do(t, h, http.MethodDelete, "/jobs/"+id, ""); status != http.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
}

func TestListFiltersByStatus(t *testing.T) {
	h := api.NewHandler(store.NewMemory(), nil)
	postJSON(t, h, "/jobs", `{"type":"echo","payload":"a"}`, http.StatusCreated)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/jobs?status=pending", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var jobs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(jobs) != 1 || jobs[0]["state"] != "pending" || jobs[0]["payload"] != "a" {
		t.Fatalf("jobs = %#v", jobs)
	}
}

func postJSON(t *testing.T, h http.Handler, path, body string, want int) (map[string]any, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	h.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("POST %s = %d, want %d, body %s", path, rec.Code, want, rec.Body.String())
	}
	if want == http.StatusNoContent {
		return nil, ""
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	return out, rec.Header().Get("Location")
}

func do(t *testing.T, h http.Handler, method, path, body string) int {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewBufferString(body)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, rdr)
	h.ServeHTTP(rec, req)
	return rec.Code
}
