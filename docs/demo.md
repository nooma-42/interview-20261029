# Ten-minute interview demonstration

| Time | Demonstration | Point to explain |
| --- | --- | --- |
| 0–1 min | Describe the original envelope and show a rocket snapshot. | Out-of-order delivery and retries are the core problem. |
| 1–3 min | Draw HTTP → transaction → row lock → event log and snapshot. Open `internal/store/store.go`. | Acknowledge only after commit; one serialization point per rocket works across replicas. |
| 3–5 min | Send #1, #3, #4, then #2 using the integration test or curl. Query between requests. | Durable pending, precise `200`/`202`, and replay by sequence rather than timestamp. |
| 5–6 min | Repeat a request, change its payload, then show the `409`. | Idempotency has a defined content identity. |
| 6–7 min | Run the two-instance and rollback integration tests. | Concurrency, atomic reads, deferred commit failure and safe retry. |
| 7–8 min | Show the official default-run record and the state assertions in integration tests. | The official sender checks compatibility; automated assertions check snapshot correctness. |
| 8–9 min | Show `/metrics`, health checks, Compose and CI. | Pending is shared database state; latency and counters are per instance. |
| 9–10 min | Discuss AI use, permanent gaps, backlog/retention, overflow and offset pagination. | Explain accepted limits and what to change for production. |

Preparation commands:

```sh
docker compose up -d --build --wait
docker compose -f compose.test.yaml -p rockets-test up -d --wait test-db
TEST_DATABASE_URL='postgres://rockets:rockets@localhost:55432/rockets_test?sslmode=disable' \
  go test -race -tags=integration ./internal/store -run 'TestSequenceHTTPAndDeduplication|TestTwoInstancesConcurrentAndAtomicReads|TestRollbackOnUpdateAndCommitFailure' -count=1 -v
curl http://localhost:8088/metrics
```

Use a new channel for each curl demonstration. Keep the recorded official run available; a full default run is too long for the ten-minute presentation. Be ready to discuss the details rather than reading every file.
