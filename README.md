# Rockets

A Go HTTP service that stores rocket events in PostgreSQL and exposes the latest complete state. It accepts out-of-order delivery and retries, commits events and snapshots together, and can run multiple API instances against one database.

## Run

Requirements: Docker with Compose v2. For local development and tests, Go 1.25 or later.

```sh
docker compose up -d --build --wait
curl http://localhost:8088/health/ready
```

Compose starts a healthy PostgreSQL 17 database, runs the embedded SQL migrations once, then starts the API at **http://localhost:8088**. Both IPv4 and IPv6 loopback are published so `localhost` works with either address family. The API runs as a non-root user in a distroless image. Database data lives in the `postgres_data` volume. `docker compose down` preserves it; `docker compose down -v` deletes it. The credentials in Compose are for local demonstration.

To run the binary against your own database:

```sh
export DATABASE_URL='postgres://rockets:rockets@localhost:5432/rockets?sslmode=disable'
go run ./cmd/rockets migrate
HTTP_ADDR=:8088 go run ./cmd/rockets serve
```

`DATABASE_URL` is required; `HTTP_ADDR` defaults to `:8088`. Each instance uses at most 20 database connections by default; set `pool_max_conns` in the connection URL to change this. SIGINT/SIGTERM drains HTTP requests for up to 15 seconds before closing the pool. Request bodies, headers, database operations and shutdown have bounded sizes or timeouts.

## API

```sh
curl -i http://localhost:8088/messages \
  -H 'Content-Type: application/json' \
  -d '{"metadata":{"channel":"demo","messageNumber":1,"messageTime":"2022-02-02T19:39:05.86337+01:00","messageType":"RocketLaunched"},"message":{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}}'

curl http://localhost:8088/rockets/demo
curl 'http://localhost:8088/rockets?sort=speed&order=desc&limit=10&offset=0'
```

| Endpoint | Behavior |
| --- | --- |
| `POST /messages` | Accept the challenge's original event envelope. `200` if this event is applied, `202` if durably pending. |
| `GET /rockets/{channel}` | Snapshot, or `404` for an unknown channel. |
| `GET /rockets` | JSON array, including rockets awaiting launch; empty results return `[]`. |
| `GET /health/live` | Process liveness, independent of the database. |
| `GET /health/ready` | `200` when the database is reachable and the schema is migrated; otherwise `503`. |
| `GET /metrics` | Prometheus exposition, including Go/process metrics. |

A received event returns:

```json
{"channel":"demo","lastAppliedMessageNumber":1,"pendingMessageCount":0,"duplicate":false}
```

A rocket snapshot returns:

```json
{
  "channel":"demo",
  "type":"Falcon-9",
  "speed":500,
  "mission":"ARTEMIS",
  "status":"launched",
  "explosionReason":null,
  "lastMessageTime":"2022-02-02T18:39:05.86337Z",
  "lastAppliedMessageNumber":1,
  "maxReceivedMessageNumber":1,
  "pendingMessageCount":0
}
```

List parameters: `sort=channel|speed|mission|lastMessageTime`, `order=asc|desc`, `limit=1..1000` (default 100), `offset>=0` (default 0). Default sort is channel ascending. Null values always come last; ties use channel ascending. Text ordering follows the database collation (Compose initializes it as `C`). Each query sees a committed snapshot. Offset pages are separate queries and can shift during concurrent updates.

Errors have one format:

```json
{"error":{"code":"invalid_message","message":"unsupported messageType"}}
```

| Status | Meaning |
| --- | --- |
| `400` | Invalid JSON/UTF-8, missing/invalid required fields, unknown fields or message types, invalid list query. |
| `404` / `405` | Unknown rocket/endpoint or unsupported method. |
| `409` | Same channel and sequence number with different validated content. |
| `413` | Body exceeds 1 MiB. |
| `422` | Applying the contiguous sequence would overflow an int64 speed; the whole transaction rolls back. |
| `503` | Database/readiness/write/commit failure, including an operation timeout. Retry after one second. |

## Ordering and domain decisions

- `messageNumber`, starting at 1, defines order. `messageTime` is data, even if it moves backwards; it must be representable as a UTC RFC3339 timestamp. `RocketLaunched` must be #1; all other event types must have numbers greater than 1. Numbers and speeds use signed 64-bit integers.
- Required text fields must be nonblank and must not contain NUL. Channel IDs are arbitrary nonblank strings up to 255 bytes; UUIDs are accepted without requiring UUID syntax. `launchSpeed` and `by` are required nonnegative integers, including zero. The four remaining payloads are `{"by":n}` (increase/decrease), `{"newMission":"..."}`, and `{"reason":"..."}`.
- Decreases subtract the supplied amount, including into negative speeds. Arithmetic overflow rejects the transaction. There is no silent clamp or wraparound.
- A channel first seen with #2 or later is persisted as `awaiting_launch`. Type, speed, mission, explosion reason and last applied event time are `null` until known.
- After an explosion, retain the last speed, mission and first explosion reason. Later events still advance sequence/time but cannot change the terminal state.
- `lastMessageTime` belongs to the last **applied** sequence number, not the newest arrival or largest timestamp. PostgreSQL stores snapshot timestamps to microsecond precision. The canonical event envelope retains its timestamp precision for conflict detection.
- Equivalent validated content is a duplicate: JSON whitespace/key order and timezone offsets denoting the same instant do not matter. Changes to a timestamp instant, payload or type return `409`. A duplicate returns `200` or `202` according to whether its own sequence number is currently applied, even if other events remain pending.

Missing numbers wait indefinitely and never keep the HTTP request open:

| Arrival | HTTP | Last applied | Pending count |
| --- | --- | --- | --- |
| #1 | 200 | 1 | 0 |
| #3 | 202 | 1 | 1 |
| #4 | 202 | 1 | 2 |
| #2 | 200 | 4 | 0 |

A `202` acknowledges durable storage; the sender need not retry that event. Subsequent gap-filling events replay every now-contiguous event in the same transaction. Pending events survive process/container restarts. Without missing-event redelivery, the snapshot stays at the last contiguous number forever.

## Implementation and consistency

`internal/domain` validates typed events and applies pure state transitions. `internal/store` owns pgx, embedded versioned migrations and transactions. `internal/httpapi` owns HTTP validation, status mapping and metrics. `cmd/rockets` handles configuration and shutdown.

For each ingestion request:

1. Begin a transaction, create the rocket if absent and take `SELECT ... FOR UPDATE` on that channel's row.
2. Insert into `rocket_events`, keyed by `(channel, message_number)`. Existing content is compared as canonical JSONB; a conflict rolls back.
3. Replay the contiguous sequence after `last_applied`, fetching at most 256 events per query and stopping at the first gap.
4. Update the snapshot, received count and maximum received sequence, then commit **before** returning a success response.

`pendingMessageCount = received_count - last_applied`: contiguous application means exactly `last_applied` unique events have been applied. Events and these counters share the same transaction. Reads cannot see half of a write. Different rockets can progress concurrently; the database serializes writers for the same rocket across all API instances. PostgreSQL's default READ COMMITTED isolation is sufficient with this row lock. An ambiguous commit failure returns `503`; retrying safely resolves whether the commit happened.

There is no queue or worker: a successful HTTP response is already durable, and gap repair happens on ingestion. Migrations use a transaction-scoped advisory lock and version records, so concurrent migration jobs serialize. Apply migrations before serving; migrations are forward-only. Existing migration files must not be edited after deployment. The initial schema keeps only primary-key indexes; sorting uses `ORDER BY` without extra indexes until query volume justifies them.

## Verification

```sh
go vet -tags=integration ./...

# Separate temporary database; never use the application database for tests.
docker compose -f compose.test.yaml -p rockets-test up -d --wait test-db
TEST_DATABASE_URL='postgres://rockets:rockets@localhost:55432/rockets_test?sslmode=disable' \
  go test -race -tags=integration ./... -count=1 -v
```

Integration tests require a database name ending in `_test`, create an isolated schema for each test, and remove only those schemas afterwards. They cover sequence gaps, launch arriving late, duplicates/conflicts, persisted pending state after reopening the service, concurrent first inserts, two API instances, atomic reads, update and deferred-commit failures, overflow rollback, sorting and pagination. Unit tests cover all five transitions, terminal behavior, validation and int64 boundaries.

GitHub Actions runs formatting checks, dependency verification, `go vet`, unit tests, PostgreSQL integration tests with the race detector, and a Docker image build.

### Official test program

Use the executable from the challenge bundle for your OS/architecture. No test binary is redistributed here. With the API running:

```sh
# Run from the challenge bundle directory; no flags means the assessor's defaults.
./darwin_arm64/rockets launch http://localhost:8088/messages
# Linux alternative:
./linux_arm64/rockets launch http://localhost:8088/messages

# Slower demonstration from the challenge instructions:
./darwin_arm64/rockets launch http://localhost:8088/messages \
  --message-delay=500ms --concurrency-level=1
```

`launch --help` reports defaults of 100,000 messages, concurrency 3, seed 444, and no delay. The program checks reception compatibility; the automated tests assert event processing and final snapshots.

See [recorded verification](docs/verification.md), [AI use and ownership](docs/ai-use.md), and the [ten-minute interview outline](docs/demo.md).

## Observability and limits

- `rocket_http_request_duration_seconds`: histogram by fixed route, bounded method and status.
- `rocket_events_received_total`: requests by `applied`, `pending`, `duplicate`, `conflict`, `invalid`, `overflow`, or `unavailable` outcome.
- `rocket_events_applied_total`: events applied by this instance after a successful commit, including replayed events. It resets with the process; rare ambiguous commits can undercount this operational metric. Database state remains authoritative.
- `rocket_pending_events`: total committed pending events in the shared database, queried at scrape time. It survives instance restarts. **Do not sum it across replicas**; use `max`. If collection fails, the value is omitted and `rocket_pending_scrape_success` is 0.

Metrics never use channel IDs as labels. The pending scrape scans rocket counters, so frequent scraping of a very large fleet would need a cheaper aggregate. HTTP metrics and database transaction atomicity are local guarantees, not a promise of exactly-once delivery by the sender.

This submission intentionally has no authentication, frontend, queue, event cleanup or deployment platform. Missing events and retained logs can grow without bound; production needs retransmission, retention, backups and operational limits. A large contiguous backlog can exceed the ten-second request timeout; the transaction rolls back and the sender retries. A future design could cap replay work and use durable workers. Overflow in an already-pending event blocks further progress until an explicit operator policy resolves it; retrying the same gap-filling request cannot fix invalid arithmetic. Conflicting events likewise require sender/operator resolution. These cases are exposed rather than silently skipping history.
