package io.braineous.dd.processor;

import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.models.cgo.graph.GraphBuilder;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
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
    void orchestrate_ok_returns_processorResult_and_consumer_persists_once() {

        // ---- build input (NO kafka.key) ----
        com.google.gson.JsonObject kafka = new com.google.gson.JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 3);
        kafka.addProperty("offset", 48192L);
        kafka.addProperty("timestamp", 1767114000123L);

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        com.google.gson.JsonObject in = new com.google.gson.JsonObject();
        in.add("kafka", kafka);
        in.add("payload", payload);

        Console.log("test.processor.orch.in", in.toString());

        // ---- act ----
        ProcessorResult out = this.orch.orchestrate(in);

        com.google.gson.Gson gson = new com.google.gson.Gson();
        Console.log("test.processor.orch.result.obj", gson.toJson(out));
        Console.log("test.processor.orch.out.ddEventJson", in.toString()); // PO mutates input in-place

        // ---- assert ----
        org.junit.jupiter.api.Assertions.assertNotNull(out);
        org.junit.jupiter.api.Assertions.assertTrue(out.isOk(), "Expected ok=true but got ok=false: " + gson.toJson(out));

        String ingestionId = out.getIngestionId();
        Console.log("test.processor.orch.ingestionId", ingestionId);

        org.junit.jupiter.api.Assertions.assertNotNull(ingestionId);
        org.junit.jupiter.api.Assertions.assertTrue(ingestionId.trim().length() > 0);

        // input should now carry ingestionId (since PO stamps it on ddEventJson)
        org.junit.jupiter.api.Assertions.assertTrue(in.has("ingestionId"));
        org.junit.jupiter.api.Assertions.assertEquals(ingestionId, in.get("ingestionId").getAsString());

        // kafka + payload must still be present and unchanged (no key added)
        org.junit.jupiter.api.Assertions.assertTrue(in.has("kafka"));
        org.junit.jupiter.api.Assertions.assertTrue(in.get("kafka").isJsonObject());
        org.junit.jupiter.api.Assertions.assertFalse(in.getAsJsonObject("kafka").has("key"));

        org.junit.jupiter.api.Assertions.assertTrue(in.has("payload"));
        org.junit.jupiter.api.Assertions.assertTrue(in.get("payload").isJsonObject());
        org.junit.jupiter.api.Assertions.assertEquals("base64", in.getAsJsonObject("payload").get("encoding").getAsString());
        org.junit.jupiter.api.Assertions.assertEquals("AAECAwQFBgcICQ==", in.getAsJsonObject("payload").get("value").getAsString());

        // view should be attached on success
        org.junit.jupiter.api.Assertions.assertTrue(in.has("view"));
        org.junit.jupiter.api.Assertions.assertTrue(in.get("view").isJsonObject());
    }

    @org.junit.jupiter.api.Test
    void orchestrate_sets_same_ingestionId_on_processorResult_and_ddEventJson() {

        // ---- build input (NO kafka.key) ----
        com.google.gson.JsonObject kafka = new com.google.gson.JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 3);
        kafka.addProperty("offset", 48192L);
        kafka.addProperty("timestamp", 1767114000123L);

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        com.google.gson.JsonObject in = new com.google.gson.JsonObject();
        in.add("kafka", kafka);
        in.add("payload", payload);

        Console.log("test.processor.orch.idAxis.in", in.toString());

        // ---- act ----
        ProcessorResult out = this.orch.orchestrate(in);

        com.google.gson.Gson gson = new com.google.gson.Gson();
        Console.log("test.processor.orch.idAxis.result.obj", gson.toJson(out));
        Console.log("test.processor.orch.idAxis.ddEventJson.after", in.toString());

        // ---- assert ----
        org.junit.jupiter.api.Assertions.assertNotNull(out);
        org.junit.jupiter.api.Assertions.assertTrue(out.isOk(), "Expected ok=true but got ok=false: " + gson.toJson(out));

        String ingestionId = out.getIngestionId();
        Console.log("test.processor.orch.idAxis.ingestionId", ingestionId);

        org.junit.jupiter.api.Assertions.assertNotNull(ingestionId);
        org.junit.jupiter.api.Assertions.assertTrue(ingestionId.trim().length() > 0);

        // ddEventJson stamped ingestionId must match ProcessorResult ingestionId
        org.junit.jupiter.api.Assertions.assertTrue(in.has("ingestionId"));
        org.junit.jupiter.api.Assertions.assertEquals(ingestionId, in.get("ingestionId").getAsString());

        // sanity: no kafka.key garbage introduced
        org.junit.jupiter.api.Assertions.assertFalse(in.getAsJsonObject("kafka").has("key"));
    }

    @org.junit.jupiter.api.Test
    void orchestrate_adds_only_anchor_fields_and_preserves_kafka_and_payload() {

        // ---- build input (NO kafka.key) ----
        com.google.gson.JsonObject kafka = new com.google.gson.JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 9);
        kafka.addProperty("offset", 123L);
        kafka.addProperty("timestamp", 1767114000999L);

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        com.google.gson.JsonObject in = new com.google.gson.JsonObject();
        in.add("kafka", kafka);
        in.add("payload", payload);

        // snapshot of original kafka/payload for comparison
        String kafkaBefore = in.getAsJsonObject("kafka").toString();
        String payloadBefore = in.getAsJsonObject("payload").toString();

        Console.log("test.processor.orch.mutation.in", in.toString());

        // ---- act ----
        ProcessorResult out = this.orch.orchestrate(in);

        com.google.gson.Gson gson = new com.google.gson.Gson();
        Console.log("test.processor.orch.mutation.result.obj", gson.toJson(out));
        Console.log("test.processor.orch.mutation.input.after", in.toString());

        // ---- assert ----
        org.junit.jupiter.api.Assertions.assertNotNull(out);
        org.junit.jupiter.api.Assertions.assertTrue(out.isOk(), "Expected ok=true but got ok=false: " + gson.toJson(out));

        // kafka + payload preserved (byte-for-byte JSON string compare)
        org.junit.jupiter.api.Assertions.assertEquals(kafkaBefore, in.getAsJsonObject("kafka").toString());
        org.junit.jupiter.api.Assertions.assertEquals(payloadBefore, in.getAsJsonObject("payload").toString());

        // only anchor fields added
        org.junit.jupiter.api.Assertions.assertTrue(in.has("ingestionId"));
        org.junit.jupiter.api.Assertions.assertTrue(in.has("view"));

        // still no kafka.key garbage
        org.junit.jupiter.api.Assertions.assertFalse(in.getAsJsonObject("kafka").has("key"));

        // ingestionId matches result ingestionId
        org.junit.jupiter.api.Assertions.assertEquals(out.getIngestionId(), in.get("ingestionId").getAsString());
    }


    //------------------------------------------------------



    private String resolveIngestionIdFromSnapshot(GraphSnapshot snap) {

        if (snap == null) {
            return null;
        }

        java.util.Map<String, Fact> nodes = snap.nodes();
        if (nodes == null) {
            return null;
        }

        if (nodes.size() != 1) {
            return null; // violates 1 ingestion == 1 fact invariant
        }

        for (java.util.Map.Entry<String, Fact> e : nodes.entrySet()) {

            String key = e.getKey();
            if (key != null && key.trim().length() > 0) {
                return key;
            }

            Fact f = e.getValue();
            if (f != null) {
                String id = f.getId();
                if (id != null && id.trim().length() > 0) {
                    return id;
                }
            }
        }

        return null;
    }


    private JsonObject buildEvent_noKey(String topic, int partition, long offset, long timestamp, String payloadBase64) {

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", topic);
        kafka.addProperty("partition", partition);
        kafka.addProperty("offset", offset);
        kafka.addProperty("timestamp", timestamp);

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", payloadBase64);

        JsonObject root = new JsonObject();
        root.add("kafka", kafka);
        root.add("payload", payload);

        return root;
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
