# dd-kafka
# KafkaDD — Deterministic Ingestion & Replay for Kafka

[![Docker Pulls](https://img.shields.io/docker/pulls/braineousai/dd-kafka-dd-pack.svg)](https://hub.docker.com/r/braineousai/dd-kafka-dd-pack)
[![Docker Image Version](https://img.shields.io/docker/v/braineousai/dd-kafka-dd-pack?sort=semver)](https://hub.docker.com/r/braineousai/dd-kafka-dd-pack)


KafkaDD is a deterministic ingestion system designed for event-driven platforms where **correctness, replayability, and failure transparency** matter more than raw throughput.

This project is **not** a Kafka wrapper, a generic DLQ library, or an “AI-powered” framework.

It provides a **contracted ingestion pipeline** with the following guarantees:

- **Anchored ingestion** — events are accepted into a deterministic processing spine
- **Canonical identity** — every accepted event is represented by a stable `ingestionId`
- **Frozen snapshots** — system state is captured as immutable snapshots of facts
- **Explicit failure semantics** — failures are classified, explained, and observable
- **DLQ as observability** — not blind retries, not silent drops

KafkaDD is built for systems where *“it usually works”* is not good enough.

---

## Mental Model

Think in terms of **contracts**, not handlers.

1. **Ingestion**
    - An HTTP request is accepted into the system.
    - Acceptance means the event has entered a deterministic pipeline.
    - Acceptance does **not** mean the event is guaranteed to be persisted forever.

2. **Identity**
    - Every accepted request resolves to a single `ingestionId`.
    - `ingestionId` is the only public identifier exposed by the system.
    - All downstream observability, replay, and debugging is anchored to this ID.

3. **Snapshots**
    - The system represents state as a snapshot of frozen facts.
    - Given the same facts and the same rules, the snapshot is always identical.
    - Snapshots never change after creation.

4. **Views**
    - Views are interpretations derived from snapshots.
    - Views may evolve over time.
    - Snapshots do not.

5. **Failures & DLQ**
    - Failures are explicit and categorized.
    - Domain failures and system failures are treated differently.
    - DLQs exist to explain *why* something failed—not to hide it.

If you need ingestion that is **auditable, replayable, and deterministic**, KafkaDD is designed for that job.

## API & Contract Semantics

KafkaDD exposes a deliberately minimal public contract.

The ingestion API does **one thing**:  
it accepts an event into a deterministic processing pipeline and returns an `ingestionId`.

### What a successful response means

A successful HTTP response indicates that:

- the request passed fail-fast validation
- the request entered the deterministic ingestion spine
- a canonical `ingestionId` was resolved

It does **not** guarantee that:
- the event has already been produced to Kafka
- downstream processing has completed
- the event will never fail

KafkaDD separates **acceptance** from **outcome** by design.

### The `ingestionId` axis

`ingestionId` is the only public identifier exposed by the system.

All downstream concerns — including replay, DLQ inspection, debugging, and audit — are anchored to this ID.

This allows KafkaDD to evolve internally (storage, topics, processors) without breaking the public contract.

If you have an `ingestionId`, you can always ask:
- *What happened?*
- *Why did it happen?*
- *Can it be replayed?*

### Deterministic resolution

Given the same inputs and the same frozen rules:
- the same request will always resolve to the same `ingestionId`
- the same request will always produce the same outcome

This property is fundamental to replay safety and auditability.

### Ingestion phases and latency

KafkaDD ingestion occurs in two closely coupled phases:

1. **Acceptance (synchronous)**
    - Fail-fast validation is applied.
    - The request is resolved to a deterministic `ingestionId`.
    - The event is committed to the ingestion pipeline.
    - An HTTP response is returned.

2. **Commit (asynchronous, near-immediate)**
    - The event is produced to Kafka.
    - Internal state and observability records are persisted.
    - Any failure during this phase is surfaced via explicit DLQ semantics.

This design ensures that ingestion is **not fire-and-forget**, while still avoiding long-running synchronous requests.

A successful response means the event has entered the system deterministically.  
It does not imply long delays or deferred batch processing.

If the commit phase fails, the failure is observable and traceable via the same `ingestionId`.


## Snapshots and Views

KafkaDD separates **facts**, **views**, and **snapshots** to prevent reality drift while allowing external systems to evolve safely.

### Facts

Facts are immutable assertions that have been accepted and anchored by the system.

- Facts are append-only.
- Facts never change once anchored.
- A fact represents what was true at the time it entered the pipeline.

### Views

Views are derived interpretations built over anchored facts.

- Views do not mutate facts.
- Views are a way to observe and reason about anchored history.
- Views are always derived, never authoritative.

### Snapshots

A snapshot is a **frozen materialization of a view over a fixed set of anchored facts**.

- Snapshots are immutable once created.
- Snapshots represent system state at a specific point in time.
- Given the same anchored facts, the snapshot is always identical.

Snapshots are the system’s anchor against reality drift.

### Reality change and re-introduction

External reality may change after a fact has been anchored.

KafkaDD does not mutate or rewrite anchored facts to reflect those changes.

Instead:
- updated reality is reintroduced into the pipeline as a **new fact**
- the new fact is anchored independently
- a new snapshot is derived

This ensures that:
- history remains frozen and auditable
- change is explicit, not silent
- downstream systems can reconcile state deterministically

### Preventing reality drift

KafkaDD prevents reality drift by enforcing a simple rule:

> Anchored facts never change.  
> Changes in reality enter the system as new facts.

Snapshots freeze history.  
Re-introduction moves the system forward.

## DLQ Taxonomy

KafkaDD treats Dead Letter Queues as **observability boundaries**, not retry buckets.

Failures are classified based on *why* they occurred and *where* the fix belongs.  
This classification is explicit and intentional.

### DLQ-D — Domain DLQ (Non-Replayable)

DLQ-D captures **domain-level failures**.

These occur when the system is behaving correctly, but the input is invalid or semantically incorrect.

Examples include:
- schema or contract violations
- failed validation
- semantic conflicts
- deterministic poison-pill payloads

Key properties:
- DLQ-D events are **not replayable**
- Replaying the same input will deterministically fail again
- The fix lives **outside** the ingestion pipeline

Corrected reality must be reintroduced as a **new fact**, producing a new snapshot.  
KafkaDD does not attempt to heal bad data automatically.

### DLQ-S — System DLQ (Conditionally Replayable)

DLQ-S captures **system-level failures**.

These occur when the input is valid, but the system cannot complete processing due to environmental conditions.

Examples include:
- Kafka or broker unavailability
- network timeouts
- transient infrastructure failures
- downstream system instability

Key properties:
- DLQ-S events may be replayable
- Replay safety depends on whether the underlying fact was anchored before the failure occurred
- Replay never rewrites anchored history

Replay eligibility is a policy decision, not an assumption.

### DLQ as observability

KafkaDD uses DLQ to answer a single question:

> *Why did this fail?*

Each DLQ entry is classified, explainable, and traceable via its `ingestionId`.

DLQs exist to make failures **visible, attributable, and actionable** —  
not to hide them behind automatic retries.

## Next Steps

KafkaDD is designed to be **understood before it is extended**.  
Before adding features or integrating it into a larger system, ensure the core contracts are clear and stable.

### 1. Read the Core Documentation (in order)

Start with the four core documents below. They define the non-negotiable semantics of the system.

1. **Getting Started — Ingestion & Contracts**  
   [`GETTING_STARTED.md`](GETTING_STARTED.md)  
   Learn what ingestion acceptance means, what it does *not* mean, and how the public contract is intentionally minimal.

2. **Configuration — Determinism & Identity**  
   [`CONFIGURATION.md`](CONFIGURATION.md)  
   Understand how deterministic resolution, identity (`ingestionId`), and frozen rules are configured and enforced.

3. **Operations — Snapshots, DLQ, and Observability**  
   [`OPERATIONS.md`](OPERATIONS.md)  
   Learn how to observe outcomes, interpret DLQ classifications, and reason about system state without relying on storage internals.

4. **Docker — Running KafkaDD Locally**  
   [`DOCKER.md`](DOCKER.md)  
   Run KafkaDD in a controlled local environment to observe happy-path ingestion and failure behavior.

---

### 2. Run the System Before Extending It

Before modifying or extending KafkaDD, ensure you can:

- submit a valid ingestion request
- receive a stable `ingestionId`
- observe the outcome via DLQ or system logs
- explain *why* the outcome occurred

Start with **happy-path ingestion only**.  
Replay, failure injection, and concurrency come later.

---

### 3. Observe Before You Change Anything

For a single `ingestionId`, you should be able to answer:

- Was the request accepted?
- Did it succeed or fail?
- If it failed, was it classified as domain or system failure?
- Is replay meaningful or pointless?

If these questions cannot be answered confidently, do not extend the system yet.

---

### 4. Extend Along Explicit Axes Only

KafkaDD is intentionally narrow.  
Safe extensions include:

- additional ingestion validations
- new snapshot-derived views
- stricter DLQ classification
- improved WHY codes and diagnostics
- tooling for observability and inspection

Avoid extensions that:
- mutate anchored facts
- hide failures behind retries
- introduce non-deterministic identifiers
- couple consumers to internal storage

If determinism weakens, the extension is incorrect.

---

### 5. Treat Replay as a Contract, Not a Convenience

Replay exists to:
- reapply already-anchored facts
- complete interrupted system work
- preserve auditability

Replay is **not**:
- a retry button
- a recovery hack
- a way to fix bad history

If replay changes history, something is broken.

---

### 6. Read the Troubleshooting Appendix Last

The troubleshooting appendix is for **production reality**, not onboarding.

1. **TroubleShooting Guide**
   [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md)
2. **Troubleshooting Index**
   [`TROUBLESHOOTINGINDEX.md`](TROUBLESHOOTINGINDEX.md)
   


Read it when:
- an outcome is surprising
- determinism appears violated
- failures seem unclear or inconsistent

Most issues are misunderstandings of the contract, not bugs.


---

## Docker Images (v1.0.0)

KafkaDD services are published to Docker Hub with immutable version tags.

### Core Runtime

- **dd-pack (API entrypoint)**  
  https://hub.docker.com/r/braineousai/dd-kafka-dd-pack

- **Kafka Producer**  
  https://hub.docker.com/r/braineousai/dd-kafka-dd-module-kafka-producer

- **Kafka Processor**  
  https://hub.docker.com/r/braineousai/dd-kafka-dd-module-kafka-processor

- **DLQ Service**  
  https://hub.docker.com/r/braineousai/dd-kafka-dd-module-dlq-service

### Recommended Usage

Use explicit release tags for evaluation and integration:

```bash
braineousai/dd-kafka-dd-pack:1.0.0
```

`latest` is provided for convenience, but `1.0.0` is the canonical release reference.

### Commercial support

KafkaDD is open-source and free to use.

For teams that want hands-on help (ingestion diagnostics, DLQ/replay hardening, or production incident review), commercial support is available here:
👉 https://www.upwork.com/services/product/development-it-spring-boot-microservice-with-kafka-starter-api-consumer-1984337692019194130

Same model. Applied to your system.







