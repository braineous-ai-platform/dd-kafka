# Troubleshooting: Ingestion

This document helps diagnose **common ingestion confusions**.  
It is intentionally practical and **contract-focused**.

---

## 1. “Ingestion returned 200 OK, but I don’t see data yet”

**This is expected behavior.**

The ingestion HTTP endpoint confirms:
- the request was accepted
- the payload is valid
- an `ingestionId` was assigned

It does **not** guarantee that:
- data has been persisted
- downstream processing has completed
- storage systems have finished work

Ingestion is an **asynchronous pipeline**.  
Persistence and side effects happen **after** HTTP acceptance.

Importantly, **“later” does not mean “too late.”**  
Events are processed deterministically once accepted, even if downstream systems are temporarily unavailable.

### What to check
- **DLQ-D (Domain DLQ)**  
  If the event later fails domain validation or business rules
- **DLQ-S (System DLQ)**  
  If downstream systems (Kafka, Mongo, consumers) are unavailable or error

### Important boundary
- Failures during **pre-sync / handoff validation** return an **HTTP error**
- In such cases, **no `ingestionId` is issued**
- If you received an `ingestionId`, the event passed pre-sync validation

---

## 2. “Why is the ingestionId the same when I retry?”

This is **by design**.

If you send the **same logical event** again:
- the system returns the **same `ingestionId`**
- retries are idempotent
- no duplicate logical ingestion is created

This allows safe retries without fear of duplication.

---

## 3. “Why did I get a new ingestionId for a similar request?”

A new `ingestionId` means the system considered the request **logically different**.

Common reasons include:
- payload content changed
- facts or derived view changed
- required fields differ

Even small differences can result in a new identity.

---

## 4. “Mongo is empty, is ingestion broken?”

**Not necessarily.**

Mongo persistence is **out of the HTTP contract**.  
It depends on:
- Kafka availability
- consumer health
- downstream validation

If downstream components are unavailable, ingestion can still succeed at HTTP level while persistence happens later (or fails into DLQ).

---

## 5. “Should I assert Mongo in my tests?”

Only if the test is **explicitly end-to-end** and synchronous.

For **contract tests**:
- assert HTTP response
- assert `ingestionId` stability
- do **not** assert Mongo directly

Asserting Mongo immediately after HTTP acceptance can be flaky due to asynchronous processing.

---

## 6. “Where is the source of truth?”

Each layer has a clear responsibility:

- **HTTP ingestion**  
  Acceptance contract
- **`ingestionId`**  
  Stable identity
- **DLQ-D / DLQ-S**  
  Explanation when something fails
- **Query / inspection tools**  
  Downstream state

---

## Mental model

> **If you received an `ingestionId`, the system accepted responsibility.**  
> Processing may happen later, but it will not be lost.
