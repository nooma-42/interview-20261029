# Recorded verification

Executed by Codex on 2026-10-23 on macOS arm64, Go 1.25.5, Docker through OrbStack, PostgreSQL 17 Alpine. These are local results, not a production capacity claim. Human review is recorded separately in [ai-use.md](ai-use.md).

## Automated checks

The maintained verification is `go vet -tags=integration ./...` and `go test -race -tags=integration ./... -count=1` against an isolated PostgreSQL test database. Commands are in the [README](../README.md#verification).

Tests cover all five events, validation, ordering gaps, duplicates/conflicts, late launch, persisted pending state after reopening, concurrent first messages, two API instances, atomic reads, update/deferred-commit failure and retry, overflow rollback, sorting and pagination. Replay across 520 contiguous events exercises multiple 256-event fetches. Migration checks cover fresh installs and removal of optional sort indexes from an existing database without changing its snapshot.

After simplification, vet and all race integration tests passed, including the new fresh-install and existing-data migration checks. The rebuilt Compose service applied migration 002 successfully and became healthy. The original implementation also passed vet, unit and race integration tests. Docker multistage build and startup from a clean volume also passed: PostgreSQL became healthy, migrations exited successfully and the API became healthy. Later starts preserved data. The macOS linker emitted `LC_DYSYMTAB` warnings for race-instrumented binaries; the tests executed successfully. Hosted GitHub Actions has not been run in this session.

## Official generator

The supplied macOS arm64 executable ran without modification. The command was:

```sh
../lunar-backend-engineer-challenge/darwin_arm64/rockets launch http://localhost:8088/messages
```

Defaults: 100,000 messages, concurrency 3, seed 444, no delay. The first run eventually delivered every message, but log review exposed IPv6 connection failures because Compose published only IPv4 loopback. Compose was corrected to publish both address families.

The final run used a clean `rockets-official` Compose database. It exited with status 0, logged **100,000 successful sends and zero errors**, and stored **20 rockets, 100,000 unique events and zero pending**. A one-off independent SQL reconciliation found no snapshot differences. IPv4 and IPv6 readiness checks both passed. The temporary project was stopped with its volume preserved, and the default service was restored on port 8088.

## Earlier exploratory checks

Before simplification, a deterministic shuffled workload verified 100 rockets with 10,000 unique events and 2,000 duplicates. It took 3.126 seconds with 16 workers: 3,839 requests/s, p50 3.797 ms, p95 7.238 ms, p99 10.112 ms. The official workload was also running. These measurements apply to that earlier version and are not a capacity guarantee.

A container restart preserved pending #3; sending #2 afterwards applied both exactly once. Stopping PostgreSQL left live at `200` and returned `503` for ready and writes. After recovery, the failed write was absent and its retry succeeded. Reconciliation found no differences.

The custom load, SQL reconciliation and smoke tools were removed during simplification. They are historical verification evidence, not maintained commands. The retained checks are the automated tests and official generator; current API behavior is unchanged by the tool removal. Raw logs remain in ignored `artifacts/`.
