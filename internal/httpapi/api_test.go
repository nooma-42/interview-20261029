package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"rockets/internal/domain"
	"rockets/internal/store"
)

type stub struct{ err error }

func (s stub) Receive(context.Context, domain.Event) (store.Result, error) {
	return store.Result{}, s.err
}
func (s stub) Get(context.Context, string) (domain.Rocket, error) { return domain.Rocket{}, s.err }
func (s stub) List(context.Context, store.ListOptions) ([]domain.Rocket, error) {
	return []domain.Rocket{}, s.err
}
func (s stub) Ready(context.Context) error                 { return s.err }
func (s stub) PendingCount(context.Context) (int64, error) { return 3, s.err }

const launch = `{"metadata":{"channel":"a","messageNumber":1,"messageTime":"2022-01-01T00:00:00Z","messageType":"RocketLaunched"},"message":{"type":"x","launchSpeed":0,"mission":"m"}}`

func TestHTTPBoundary(t *testing.T) {
	h := New(stub{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, c := range []struct {
		method, path, body string
		want               int
	}{
		{"GET", "/health/live", "", 200}, {"GET", "/health/ready", "", 200}, {"PUT", "/messages", "", 405}, {"GET", "/unknown", "", 404},
		{"POST", "/messages", "{", 400}, {"POST", "/messages", strings.Repeat(" ", MaxBodySize+1), 413},
		{"GET", "/rockets?limit=0", "", 400}, {"GET", "/rockets?limit=1001", "", 400}, {"GET", "/rockets?offset=-1", "", 400},
		{"GET", "/rockets?sort=speed;DROP", "", 400}, {"GET", "/rockets?order=random", "", 400}, {"GET", "/rockets?limit=1&limit=2", "", 400},
		{"GET", "/rockets?foo=x", "", 400}, {"GET", "/rockets?offset=x", "", 400}, {"GET", "/rockets?sort=speed&order=desc&limit=2&offset=1", "", 200},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, strings.NewReader(c.body)))
		if w.Code != c.want {
			t.Errorf("%s %s: got %d want %d: %s", c.method, c.path, w.Code, c.want, w.Body.String())
		}
		if !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
			t.Errorf("non-JSON response: %s", c.path)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, metric := range []string{"rocket_pending_events 3", "rocket_pending_scrape_success 1", "rocket_http_request_duration_seconds"} {
		if !strings.Contains(w.Body.String(), metric) {
			t.Error("missing metric", metric)
		}
	}
	if strings.Contains(w.Body.String(), `channel=`) {
		t.Fatal("unbounded channel label")
	}
}
func TestRepositoryErrors(t *testing.T) {
	for _, c := range []struct {
		err                error
		path, method, body string
		want               int
	}{
		{store.ErrConflict, "/messages", "POST", launch, 409}, {domain.ErrOverflow, "/messages", "POST", launch, 422},
		{errors.New("db down"), "/messages", "POST", launch, 503}, {store.ErrNotFound, "/rockets/a", "GET", "", 404},
		{errors.New("db down"), "/health/ready", "GET", "", 503}, {errors.New("db down"), "/health/live", "GET", "", 200},
	} {
		h := New(stub{c.err}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, strings.NewReader(c.body)))
		if w.Code != c.want {
			t.Errorf("got %d want %d", w.Code, c.want)
		}
		if strings.Contains(w.Body.String(), "db down") {
			t.Fatal("internal error leaked")
		}
	}
}
