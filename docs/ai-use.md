# AI use and ownership

This record distinguishes agent work from human verification. It does not claim the candidate performed checks that have only been run by the agent.

## Work delegated to AI

The user supplied an implementation plan produced by an earlier agent. In this implementation session, Codex read the original challenge DOCX and bundle README, then authored the Go domain layer, PostgreSQL transactions and migrations, HTTP API, Docker/Compose setup, metrics, tests, CI configuration, load tool and documentation. The initial implementation used no subagents; a later user-requested review used three subagents for storage, API and delivery concerns. The original event envelope and challenge binary were treated as external compatibility requirements.

The plan specified durable pending events, no gap timeout, a row lock per rocket, and database-backed snapshots. Codex retained these decisions. Routine implementation choices included strict payload validation, canonical UTC timestamps for duplicate comparison, a counter-based pending count, bounded replay fetches and a read-only SQL reconciliation independent of the Go reducer.

## Agent verification and corrections

Codex ran unit tests, real PostgreSQL integration tests with the race detector, vet, container builds, Compose startup, the official generator with default parameters, and a deterministic shuffled load test. Exact results and limitations are recorded in [verification.md](verification.md).

During implementation, the first test build exposed an unused test import, which was removed. Review of query validation identified that `URL.Query()` discards malformed query errors; it was replaced with `url.ParseQuery` and malformed queries return `400`. The first complete official run delivered all 100,000 events, but log review exposed IPv6 loopback connection failures followed by retries. Compose was updated to publish both loopback address families and the official defaults were rerun against a fresh database. Transaction rollback cleanup was also bounded independently of a canceled request, and readiness checks all embedded migration versions. Final boundary review added rejection of invalid UTF-8 and timestamps whose UTC conversion cannot be serialized as RFC3339, with regression tests. The tests include failure injection at both update and deferred commit, because a successful insert alone does not establish transaction atomicity. The initial load verifier used arithmetic expectations and the one-off SQL reconciliation checked snapshots from the persisted log.

Docker was initially unavailable because OrbStack was stopped. It was started with user authorization. Dependency downloads and local-network tests needed sandbox escalation. The supplied macOS binary, previously reported as being killed, executed successfully in this session without changing or re-signing it.

## User-directed simplification

The user asked to stop expanding oversized-string edge cases and simplify the optional tooling. Codex removed the custom load tool, SQL audit and smoke script, their Make/Compose entrypoints, and the duplicate CI unit-test step. A new forward-only migration removes the three optional sort indexes while preserving existing data. Core event, concurrency, persistence and rollback tests remain. Small input guards remain in place; no new boundary scenarios were added.

## Human review before submission

No candidate-performed manual code review or verification is evidenced in this session. Before presenting, the candidate should run the documented commands and be able to explain:

- Why the lock covers event insertion, replay and snapshot update, including two concurrent first messages for a new channel.
- Why acknowledging pending events with `202` is safe and why permanent gaps stop progress.
- How a lost commit acknowledgment differs from a rolled-back transaction, and why retries remain safe.
- How terminal state, negative speeds, integer overflow and duplicate identity are defined.
- Which tests establish these claims, and the limits of workload numbers and process-local metrics.

In a larger system, keep small reviewable changes, independently check invariants and migrations, review dependencies and operational failure modes, and record decisions requiring product input. AI-generated tests can share the same misunderstanding as AI-generated code; the original specification, official generator and retained integration tests provide separate checks, but do not replace candidate ownership.
