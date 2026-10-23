package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"rockets/internal/domain"
	"rockets/internal/store"
)

const MaxBodySize = 1 << 20

type Repository interface {
	Receive(context.Context, domain.Event) (store.Result, error)
	Get(context.Context, string) (domain.Rocket, error)
	List(context.Context, store.ListOptions) ([]domain.Rocket, error)
	Ready(context.Context) error
}

type API struct {
	repo    Repository
	logger  *slog.Logger
	events  *prometheus.CounterVec
	applied prometheus.Counter
	latency *prometheus.HistogramVec
	mux     *http.ServeMux
}

// New owns its registry, allowing independent API instances in tests and one process.
func New(repo Repository, logger *slog.Logger) *API {
	a := &API{repo: repo, logger: logger, mux: http.NewServeMux()}
	reg := prometheus.NewRegistry()
	a.events = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rocket_events_received_total", Help: "Ingestion requests by durable outcome."}, []string{"outcome"})
	a.applied = prometheus.NewCounter(prometheus.CounterOpts{Name: "rocket_events_applied_total", Help: "Events applied by this instance after successful commit."})
	a.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "rocket_http_request_duration_seconds", Help: "HTTP latency by route, method and status.", Buckets: prometheus.DefBuckets}, []string{"route", "method", "status"})
	reg.MustRegister(a.events, a.applied, a.latency, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	a.handle("/messages", http.MethodPost, a.receive)
	a.handle("/rockets", http.MethodGet, a.list)
	a.handle("/rockets/{channel}", http.MethodGet, a.get)
	a.handle("/health/live", http.MethodGet, func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "alive"}) })
	a.handle("/health/ready", http.MethodGet, a.ready)
	metrics := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	a.handle("/metrics", http.MethodGet, metrics.ServeHTTP)
	a.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fail(w, 404, "not_found", "endpoint not found") })
	return a
}

// EventsApplied records only batches whose commit was acknowledged.
func (a *API) EventsApplied(n int64) { a.applied.Add(float64(n)) }

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &recorder{ResponseWriter: w, status: 200}
	a.mux.ServeHTTP(rec, r)
	route := r.Pattern
	if route == "" {
		route = "/"
	}
	method := r.Method
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		method = "OTHER"
	}
	a.latency.WithLabelValues(route, method, strconv.Itoa(rec.status)).Observe(time.Since(start).Seconds())
}

type recorder struct {
	http.ResponseWriter
	status int
}

func (w *recorder) WriteHeader(status int) { w.status = status; w.ResponseWriter.WriteHeader(status) }

func (a *API) handle(path, method string, h http.HandlerFunc) {
	a.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			fail(w, 405, "method_not_allowed", "method not allowed")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		h(w, r.WithContext(ctx))
	})
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, code, message string) {
	write(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func (a *API) unavailable(w http.ResponseWriter, err error) {
	a.logger.Error("database operation failed", "error", err)
	w.Header().Set("Retry-After", "1")
	fail(w, 503, "unavailable", "database unavailable; retry the request")
}
func (a *API) receive(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodySize)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		a.events.WithLabelValues("invalid").Inc()
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			fail(w, 413, "body_too_large", "request body exceeds 1 MiB")
		} else {
			fail(w, 400, "invalid_message", "cannot read request body")
		}
		return
	}
	event, err := domain.Parse(data)
	if err != nil {
		a.events.WithLabelValues("invalid").Inc()
		fail(w, 400, "invalid_message", err.Error())
		return
	}
	result, err := a.repo.Receive(r.Context(), event)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrConflict):
			a.events.WithLabelValues("conflict").Inc()
			fail(w, 409, "message_conflict", err.Error())
		default:
			a.events.WithLabelValues("unavailable").Inc()
			a.unavailable(w, err)
		}
		return
	}
	outcome := "stored"
	if result.Duplicate {
		outcome = "duplicate"
	}
	a.events.WithLabelValues(outcome).Inc()
	write(w, http.StatusAccepted, result)
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	result, err := a.repo.Get(r.Context(), r.PathValue("channel"))
	if errors.Is(err, store.ErrNotFound) {
		fail(w, 404, "not_found", "rocket not found")
		return
	}
	if err != nil {
		a.unavailable(w, err)
		return
	}
	write(w, 200, result)
}
func (a *API) list(w http.ResponseWriter, r *http.Request) {
	q, parseErr := url.ParseQuery(r.URL.RawQuery)
	if parseErr != nil {
		fail(w, 400, "invalid_query", "malformed query string")
		return
	}
	o := store.ListOptions{Sort: "channel", Order: "asc", Limit: 100}
	for key, values := range q {
		if len(values) != 1 {
			fail(w, 400, "invalid_query", "query parameters must appear once")
			return
		}
		value := values[0]
		switch key {
		case "sort":
			o.Sort = value
		case "order":
			o.Order = value
		case "limit", "offset":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				fail(w, 400, "invalid_query", "limit and offset must be non-negative integers")
				return
			}
			if key == "limit" {
				o.Limit = n
			} else {
				o.Offset = n
			}
		default:
			fail(w, 400, "invalid_query", "unsupported query parameter")
			return
		}
	}
	if (o.Sort != "channel" && o.Sort != "speed" && o.Sort != "mission" && o.Sort != "lastMessageTime") || (o.Order != "asc" && o.Order != "desc") || o.Limit < 1 || o.Limit > 1000 {
		fail(w, 400, "invalid_query", "invalid sort, order or limit (1-1000)")
		return
	}
	result, err := a.repo.List(r.Context(), o)
	if err != nil {
		a.unavailable(w, err)
		return
	}
	write(w, 200, result)
}
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.repo.Ready(ctx); err != nil {
		a.unavailable(w, err)
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}
