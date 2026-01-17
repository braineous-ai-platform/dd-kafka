# Operations

This document describes how to **observe and operate** KafkaDD in a running environment.

KafkaDD is designed so that **Kafka is the primary truth surface** for outcomes:
- success / failure
- DLQ classification
- replay evidence

Internal stores are implementation details. Operators should reason from Kafka topics and message payloads.

---

## Operating model

KafkaDD separates:

1. **Acceptance**
    - The HTTP request is accepted into the deterministic pipeline.
    - A successful response returns an `ingestionId`.

2. **Outcome**
    - What happened next is observable via Kafka topics (and WHY codes inside messages).
    - Failures are explicitly classified and emitted to DLQ topics.

A successful HTTP response does **not** mean “everything completed”.
It means “this request entered the deterministic spine and has a canonical `ingestionId`”.

---

## What to observe

KafkaDD observability is organized into a small number of channels.

### 1) Main processing topics

These represent the normal processing lane.

**Question answered:**  
Did the system process events successfully?

**Operator action:**  
Inspect the primary topic(s) for expected outputs and progression.

### 2) DLQ-D (Domain DLQ)

Domain failures: invalid input, validation failures, semantic poison pills.

**Question answered:**  
What failed because the *data* is bad?

**Operator action:**
- Fix upstream data
- Reintroduce corrected reality as a **new fact** (new request/event)
- Do not replay the same poison pill expecting a different result

DLQ-D events are **not intended** to be replayed.

Replaying a DLQ-D event is safe, but ineffective:
- the same input will deterministically fail again
- the event will be reclassified back into DLQ-D
- no history is rewritten
- no pipeline corruption occurs

DLQ-D replay does not act as a poison pill.
It exists to make domain failures observable, not to heal invalid data.


### 3) DLQ-S (System DLQ)

System failures: infrastructure or transient environment faults.

**Question answered:**  
What failed because the *system/environment* could not complete processing?

**Operator action:**
- Investigate infra (Kafka/network/downstream availability)
- Decide replay policy (manual or automated)
- Replay only when replay safety conditions are met (see below)

DLQ-S is **conditionally replayable**.

### 4) Replay lane (if enabled)

Replay is an explicit operational action that reintroduces events into processing.

**Question answered:**  
What was replayed, and what was the outcome?

**Operator action:**
- Track replays via replay-trigger topic(s) or replay outcome topic(s) (if present)
- Confirm outcomes through the same observability surfaces (main + DLQs)

---

## Replay safety

Replay is safe **only if** the underlying fact was anchored before the failure occurred.

Operationally:

- If the event is already anchored and the failure was DLQ-S, replay can be safe.
- If the failure happened before anchoring, replay safety is not guaranteed by default.

KafkaDD does not rewrite anchored history. Replays must not silently mutate past facts.

---

## Tools (CLI-first)

KafkaDD is operable with standard Kafka CLI tools.

Use whatever is already standard in your environment, for example:
- `kafka-console-consumer`
- `kafka-console-producer`
- `kcat` (if you prefer)

Kafka-native tooling is sufficient for:
- inspecting DLQ traffic
- validating classification
- auditing outcomes
- verifying replay effects

No custom UI is required for core operation.

---

## Operator playbooks

### Playbook: “My request returned ingestionId — did it succeed?”

1. Confirm the `ingestionId` from the HTTP response.
2. Inspect main processing topic(s) for corresponding downstream evidence.
3. If not found, inspect:
    - DLQ-D for domain failure classification
    - DLQ-S for system failure classification

### Playbook: DLQ-D entry observed

1. Treat this as a deterministic input problem.
2. Fix upstream data or schema.
3. Reintroduce corrected reality as a **new** request/event.
4. Do not replay the same invalid payload.

### Playbook: DLQ-S entry observed

1. Treat this as a system/environment issue.
2. Investigate infra (Kafka connectivity, timeouts, downstream dependencies).
3. Decide replay action:
    - replay if the underlying fact was anchored before failure
    - otherwise treat as a non-replayable incident until verified

### Playbook: DLQ-D entry observed

1. Treat this as a deterministic input problem.
2. Fix upstream data or schema.
3. Reintroduce corrected reality as a **new** request/event.

Replaying the original DLQ-D event is safe, but will
deterministically result in the same DLQ-D outcome.
---

## What KafkaDD will not do

- It will not silently “heal” bad data.
- It will not hide failures behind automatic retries.
- It will not rewrite anchored facts.
- It will not make Mongo/UI stores the source of truth for outcomes.

KafkaDD is designed so that failures are **visible, attributable, and actionable**.

---

## Next steps

- See [GETTING_STARTED.md](GETTING_STARTED.md) to generate real traffic
- See [CONFIGURATION.md](CONFIGURATION.md) for file-based configuration
- See [DOCKER.md](DOCKER.md) for Docker topology and service addressing
