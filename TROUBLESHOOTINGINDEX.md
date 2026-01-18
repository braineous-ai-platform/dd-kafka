# Troubleshooting Index — Ingestion

This index provides a **fast lookup** for common ingestion symptoms.  
Use it alongside `TROUBLESHOOTING.md` for deeper context.

---

| Symptom | Likely Cause | What to Check | Next Action |
|-------|-------------|---------------|-------------|
| HTTP 200 OK but data not visible yet | Asynchronous processing | System health, processing progress | Wait; ingestion is accepted and will be processed |
| Same ingestionId returned on retry | Idempotent retry | Compare request payloads | Safe to retry; behavior is correct |
| New ingestionId for similar request | Logical difference detected | Payload, facts, derived view | Treat as a new logical ingestion |
| No ingestionId returned, HTTP error | Pre-sync / handoff validation failed | HTTP error response | Fix request; retry after validation passes |
| Mongo has no records | Persistence is async or downstream unavailable | Kafka, consumers, DLQ-S | Investigate downstream systems |
| Event rejected after acceptance | Domain rule violation | DLQ-D | Fix domain issue and re-submit |
| Tests flaky when asserting Mongo | Async pipeline timing | Test intent | Remove Mongo assertions from contract tests |

---

## Guidance

- **Contract tests** should assert HTTP response and ingestionId behavior only.
- **Persistence checks** belong to end-to-end or downstream-specific tests.
- **DLQ-D vs DLQ-S** cleanly separates domain failures from system failures.

---

If you know the ingestionId, the system accepted responsibility.
