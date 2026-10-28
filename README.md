# Rockets

Go + PostgreSQL service for out-of-order rocket events. Events are stored before acknowledgment; a background loop updates rocket snapshots in sequence.

## Run

Requires Docker Compose v2. Local development requires Go 1.25+.

```sh
cp .env.example .env
# Set database credentials in .env.
docker compose up -d --build --wait
curl http://localhost:8088/health/ready
```

Compose starts PostgreSQL 17, applies migrations and serves the API on port 8088. Data survives `docker compose down`; adding `-v` deletes it.

To use PostgreSQL outside Compose:

```sh
export DATABASE_URL='postgres://rockets:rockets@localhost:5432/rockets?sslmode=disable'
go run ./cmd/rockets migrate
go run ./cmd/rockets serve
```

`HTTP_ADDR` defaults to `:8088`. Run `rockets migrate` before starting the API.

## API

| Endpoint | Behavior |
| --- | --- |
| `POST /messages` | Durably store an event: `202` for new events and identical retries. The response includes `channel` and `duplicate`. |
| `GET /rockets/{channel}` | Current snapshot and replay progress; `404` if unknown. |
| `GET /rockets` | Sorted, paginated JSON array. |
| `GET /health/live` | Process liveness. |
| `GET /health/ready` | Database and migration readiness. |
| `GET /metrics` | Prometheus metrics. |

```sh
curl -i http://localhost:8088/messages \
  -H 'Content-Type: application/json' \
  -d '{"metadata":{"channel":"demo","messageNumber":1,"messageTime":"2022-02-02T18:39:05Z","messageType":"RocketLaunched"},"message":{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}}'

curl http://localhost:8088/rockets/demo
curl 'http://localhost:8088/rockets?sort=speed&order=desc&limit=10&offset=0'
```

List options: `sort=channel|speed|mission|lastMessageTime`, `order=asc|desc`, `limit=1..1000`, `offset>=0`. Defaults: channel ascending, limit 100, offset 0. Nulls sort last; ties use channel ascending.

Errors: `400` invalid input, `409` conflicting event content, `413` body over 1 MiB, `503` database failure (safe to retry). Responses use `{"error":{"code":"...","message":"..."}}`.

## Design

- **Ordering:** `messageNumber` starts at 1 with `RocketLaunched`; timestamps do not determine order. Missing numbers block later events. Until launch is applied, the snapshot is `awaiting_launch`.
- **Deduplication:** `(channel, message_number)` identifies an event. Equivalent validated content is a duplicate; different content returns `409`.
- **Receive:** Events for an existing channel are stored independently by message number. Identical retries succeed; conflicting payloads return `409`. Creating a new channel may briefly serialize its first arrivals.
- **Replay:** Each API process runs one background loop. It selects rockets with the next event present using `FOR NO KEY UPDATE SKIP LOCKED` and commits at most 256 consecutive events per transaction, with a five-second timeout. Snapshot and checkpoint commit together.
- **Recovery:** Work is discovered from database state, so restarts resume without new requests. Idle polling is 100 ms; database failures back off one second. Shutdown cancels the loop and waits for it before closing the database pool.
- **Blocked events:** Overflow or invalid stored events preserve the valid prefix, set `replayError` and delay that rocket for 30 seconds. Other rockets continue; no event is silently skipped.
- **Domain rules:** Speed may become negative. Explosion freezes speed, mission and the first explosion reason; later events only advance sequence and time.

`202` confirms durable storage but does not guarantee application. GET may briefly show an older snapshot. `lastAppliedMessageNumber` is the checkpoint. Permanent gaps and overflow require sender/operator resolution.

The code is split into `internal/domain` (validation/transitions), `internal/store` (transactions/replay), `internal/httpapi` (HTTP/metrics) and `cmd/rockets` (startup/shutdown).

## Tests

```sh
go test ./...
go vet -tags=integration ./...
docker compose -f compose.test.yaml -p rockets-test up -d --wait test-db
make integration
```

Integration tests use an isolated database ending in `_test` and cover ordering, duplicates, concurrent Receive and Replay, restart recovery, idempotent migration, batch limits, rollback, blocked events and pagination. `make integration` sources `.env` as shell assignments; quote values containing shell metacharacters. CI runs tests with the race detector and builds the Docker image.

Run the supplied generator from the challenge bundle using the binary for your platform:

```sh
./darwin_arm64/rockets launch http://localhost:8088/messages
```

Verified with `go test -race ./...`, `go vet -tags=integration ./...`, and `make integration` against the isolated PostgreSQL test database. An official generator run with `--max-messages=1000` produced 1,000 unique events across 20 channels; independent ordered replay matched every GET snapshot and speed-sorted list. The full 100,000-message run and Docker image build have not been run.

## Limits

No authentication or event retention policy is included. Missing events can block progress indefinitely. Replay uses `(replay_after, channel)` to scan in scheduling order and the events primary key to look up the next message; many channels without their next event can still require scanning many candidates. List sorting may need additional indexes at larger scale. Offset pagination can shift during updates.

Metrics include HTTP latency and received/applied event counters. Applied counters are process-local; database state is authoritative.
