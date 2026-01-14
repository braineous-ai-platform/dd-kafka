package io.braineous.dd.processor;

import ai.braineous.rag.prompt.models.cgo.graph.GraphBuilder;
import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.consumer.service.DDEventOrchestrator;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.support.InMemoryIngestionStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ProcessorOrchestratorTest {

    @Inject
    private ProcessorOrchestrator orch; // <-- your class under test

    @Inject
    private DDEventOrchestrator eventOrch;

    @BeforeEach
    void setup() {
        InMemoryIngestionStore store = new InMemoryIngestionStore();
        eventOrch.setStore(store);
        store.reset();
        GraphBuilder.getInstance().clear();
    }

    @org.junit.jupiter.api.Test
    void orchestrate_ok_returns_processorResult_and_consumer_persists_once() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);
        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-001"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        // -------- Act 1: producer side (anchoring) --------
        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();
        ProcessorResult pr = orch.orchestrate(ddEvent);

        // -------- Assert 1: ProcessorResult contract --------
        assertNotNull(pr);
        assertTrue(pr.isOk());

        assertNotNull(pr.getId());
        assertTrue(pr.getId().trim().length() > 0);

        assertNotNull(pr.getIngestionId());
        assertTrue(pr.getIngestionId().trim().length() > 0);

        assertNull(pr.getWhy());

        // We expect orchestrator to return the anchored ddEventJson
        assertNotNull(pr.getDdEventJson());

        // -------- Act 2: call consumer directly (no Kafka) --------
        IngestionReceipt receipt = eventOrch.orchestrate(pr.getDdEventJson().toString());
        assertNotNull(receipt);

        // -------- Assert 2: store seam --------
        assertEquals(1, store.storeCalls());

        String persisted = store.lastStoredPayload();
        assertNotNull(persisted);
        assertTrue(persisted.trim().length() > 0);

        assertTrue(persisted.contains("\"ingestionId\""));
        assertTrue(persisted.contains(pr.getIngestionId()));

        assertTrue(persisted.contains("\"kafka\""));
        assertTrue(persisted.contains("\"topic\":\"requests\""));
        assertTrue(persisted.contains("\"payload\""));
        assertTrue(persisted.contains("\"encoding\":\"base64\""));
    }

    @org.junit.jupiter.api.Test
    void orchestrate_non2xx_returns_fail_why_and_does_not_persist() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(500);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-002"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // -------- Act: producer side --------
        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.result", pr == null ? null : pr.toJsonString());

        // -------- Assert: failure contract --------
        assertNotNull(pr);
        assertFalse(pr.isOk());

        assertNotNull(pr.getId());
        assertTrue(pr.getId().trim().length() > 0);

        // In fail path, ingestionId may or may not be set — do NOT require it.
        assertNotNull(pr.getWhy());
        assertNotNull(pr.getWhy().reason());
        assertTrue(pr.getWhy().reason().trim().length() > 0);

        // Should not call consumer in this test; ensure no persistence happened.
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Test
    void orchestrate_httpPoster_exception_returns_fail_and_does_not_persist() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        // Fake poster that throws
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String jsonBody) throws Exception {
                throw new RuntimeException("boom-http");
            }
        };
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-003"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.exception.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // -------- Act --------
        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.exception.result",
                pr == null ? null : pr.toJsonString());

        // -------- Assert: failure contract --------
        assertNotNull(pr);
        assertFalse(pr.isOk());

        assertNotNull(pr.getWhy());
        assertNotNull(pr.getWhy().reason());
        assertTrue(pr.getWhy().reason().trim().length() > 0);

        // No consumer call, no persistence
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.exception.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Test
    void orchestrate_null_ddEventJson_returns_fail_and_does_not_persist() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        Console.log("test.processor.orch.null.in", null);

        // -------- Act --------
        ProcessorResult pr = orch.orchestrate((JsonObject) null);

        Console.log("test.processor.orch.null.result",
                pr == null ? null : pr.toJsonString());

        // -------- Assert --------
        assertNotNull(pr);
        assertFalse(pr.isOk());

        assertNotNull(pr.getWhy());
        assertNotNull(pr.getWhy().reason());
        assertTrue(pr.getWhy().reason().trim().length() > 0);

        // No consumer call, no persistence
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.null.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Test
    void orchestrate_missing_kafka_returns_fail_and_does_not_persist() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.missingKafka.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // -------- Act --------
        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.missingKafka.result",
                pr == null ? null : pr.toJsonString());

        // -------- Assert --------
        assertNotNull(pr);
        assertFalse(pr.isOk());

        assertNotNull(pr.getWhy());
        assertNotNull(pr.getWhy().reason());
        assertTrue(pr.getWhy().reason().trim().length() > 0);

        // No consumer call, no persistence
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.missingKafka.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Test
    void orchestrate_missing_payload_returns_fail_and_does_not_persist() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-006"
      }
    }
    """;

        Console.log("test.processor.orch.missingPayload.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // -------- Act --------
        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.missingPayload.result",
                pr == null ? null : pr.toJsonString());

        // -------- Assert --------
        assertNotNull(pr);
        assertFalse(pr.isOk());

        assertNotNull(pr.getWhy());
        assertNotNull(pr.getWhy().reason());
        assertTrue(pr.getWhy().reason().trim().length() > 0);

        // No consumer call, no persistence
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.missingPayload.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Disabled("PARKED: ingestionId determinism across retries requires DLQ-aware resolveIngestionId; enable after DLQ integration")
    @org.junit.jupiter.api.Test
    void orchestrate_same_input_twice_returns_same_ingestionId() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-007"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.determinism.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        ProcessorResult r1 = orch.orchestrate(ddEvent);
        ProcessorResult r2 = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.determinism.r1", r1 == null ? null : r1.toJsonString());
        Console.log("test.processor.orch.determinism.r2", r2 == null ? null : r2.toJsonString());

        assertNotNull(r1);
        assertNotNull(r2);
        assertTrue(r1.isOk());
        assertTrue(r2.isOk());

        assertNotNull(r1.getIngestionId());
        assertNotNull(r2.getIngestionId());

        assertEquals(r1.getIngestionId(), r2.getIngestionId());

        // Producer test only: we do NOT call consumer here.
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.determinism.store.calls", store.storeCalls());
    }

    @org.junit.jupiter.api.Disabled("PARKED: ingestionId determinism across retries requires DLQ-aware resolveIngestionId; enable after DLQ integration")
    @org.junit.jupiter.api.Test
    void orchestrate_seals_ingestionId_and_is_deterministic_for_same_input() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        // Producer is isolated: no real HTTP
        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-007"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.seal.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // -------- Act: producer twice on same input --------
        ProcessorResult r1 = orch.orchestrate(ddEvent);
        ProcessorResult r2 = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.seal.r1", r1 == null ? null : r1.toJsonString());
        Console.log("test.processor.orch.seal.r2", r2 == null ? null : r2.toJsonString());

        // -------- Assert: sealed identity (no drift) --------
        assertNotNull(r1);
        assertNotNull(r2);

        assertTrue(r1.isOk());
        assertTrue(r2.isOk());

        assertNotNull(r1.getIngestionId());
        assertNotNull(r2.getIngestionId());
        assertTrue(r1.getIngestionId().trim().length() > 0);
        assertTrue(r2.getIngestionId().trim().length() > 0);

        // Canonical axis: same input -> same sealed ingestionId
        assertEquals(r1.getIngestionId(), r2.getIngestionId());

        // Sealed event should carry the id inside ddEventJson too
        assertNotNull(r1.getDdEventJson());
        assertNotNull(r2.getDdEventJson());
        assertTrue(r1.getDdEventJson().has("ingestionId"));
        assertTrue(r2.getDdEventJson().has("ingestionId"));
        assertEquals(r1.getIngestionId(), r1.getDdEventJson().get("ingestionId").getAsString());
        assertEquals(r2.getIngestionId(), r2.getDdEventJson().get("ingestionId").getAsString());

        // This test is about identity sealing, not consumer/store liveness
        assertEquals(0, store.storeCalls());

        Console.log("test.processor.orch.seal.store.calls", store.storeCalls());
    }


    @org.junit.jupiter.api.Disabled("PARKED: ingestionId determinism across retries requires DLQ-aware resolveIngestionId; enable after DLQ integration")
    @org.junit.jupiter.api.Test
    void consumer_same_anchored_event_twice_persists_once() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-008"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.consumer.idempotency.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();
        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.consumer.idempotency.pr", pr == null ? null : pr.toJsonString());

        assertNotNull(pr);
        assertTrue(pr.isOk());
        assertNotNull(pr.getDdEventJson());
        assertNotNull(pr.getIngestionId());

        String anchored = pr.getDdEventJson().toString();

        // Act: consumer twice with SAME anchored payload (simulate duplicate delivery)
        IngestionReceipt r1 = eventOrch.orchestrate(anchored);
        IngestionReceipt r2 = eventOrch.orchestrate(anchored);

        Console.log("test.processor.consumer.idempotency.r1", r1 == null ? null : r1.toJson().toString());
        Console.log("test.processor.consumer.idempotency.r2", r2 == null ? null : r2.toJson().toString());

        assertNotNull(r1);
        assertNotNull(r2);

        // Assert: persisted once
        assertEquals(1, store.storeCalls());

        String persisted = store.lastStoredPayload();
        Console.log("test.processor.consumer.idempotency.persisted", persisted);

        assertNotNull(persisted);
        assertTrue(persisted.contains("\"ingestionId\""));
        assertTrue(persisted.contains(pr.getIngestionId()));
    }

    @org.junit.jupiter.api.Test
    void orchestrate_sets_same_ingestionId_on_processorResult_and_ddEventJson() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-7c"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        Console.log("test.processor.orch.idAxis.in", ddEventJson);

        JsonObject ddEvent = JsonParser.parseString(ddEventJson).getAsJsonObject();

        ProcessorResult pr = orch.orchestrate(ddEvent);

        Console.log("test.processor.orch.idAxis.result", pr == null ? null : pr.toJsonString());

        assertNotNull(pr);
        assertTrue(pr.isOk());

        assertNotNull(pr.getIngestionId());
        assertTrue(pr.getIngestionId().trim().length() > 0);

        assertNotNull(pr.getDdEventJson());
        assertTrue(pr.getDdEventJson().has("ingestionId"));

        String anchoredId = pr.getDdEventJson().get("ingestionId").getAsString();
        assertNotNull(anchoredId);
        assertTrue(anchoredId.trim().length() > 0);

        // Canonical axis invariant: API id == anchored event id
        assertEquals(pr.getIngestionId(), anchoredId);

        // Producer-only test
        assertEquals(0, store.storeCalls());
    }


    @org.junit.jupiter.api.Test
    void orchestrate_adds_only_anchor_fields_and_preserves_kafka_and_payload() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) eventOrch.getStore();
        assertNotNull(store);

        HttpPoster httpPoster = new FakeHttpPoster(200);
        this.orch.setHttpPoster(httpPoster);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 9,
        "offset": 123,
        "timestamp": 1767114000999,
        "key": "fact-009"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        JsonObject input = JsonParser.parseString(ddEventJson).getAsJsonObject();

        // snapshot original kafka/payload for later comparison
        JsonObject kafkaBefore = input.getAsJsonObject("kafka").deepCopy();
        JsonObject payloadBefore = input.getAsJsonObject("payload").deepCopy();

        Console.log("test.processor.orch.mutation.in", input.toString());

        ProcessorResult pr = orch.orchestrate(input);

        Console.log("test.processor.orch.mutation.result", pr == null ? null : pr.toJsonString());
        Console.log("test.processor.orch.mutation.input.after", input.toString());

        assertNotNull(pr);
        assertTrue(pr.isOk());

        // Anchors exist
        assertTrue(input.has("ingestionId"));
        assertTrue(input.has("view"));

        assertNotNull(pr.getIngestionId());
        assertEquals(pr.getIngestionId(), input.get("ingestionId").getAsString());

        // Preserve kafka/payload contents (no drift)
        assertEquals(kafkaBefore.toString(), input.getAsJsonObject("kafka").toString());
        assertEquals(payloadBefore.toString(), input.getAsJsonObject("payload").toString());

        // This is a producer-only mutation test: no consumer call expected
        assertEquals(0, store.storeCalls());
    }


    //-----------------------------------------------------------------
    static class FakeHttpPoster implements io.braineous.dd.core.processor.HttpPoster {
        private final int status;
        FakeHttpPoster(int status) { this.status = status; }

        @Override
        public int post(String endpoint, String jsonBody) {
            return status;
        }
    }
}
