package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.braineous.dd.replay.services.ReplayService;

@io.quarkus.test.junit.QuarkusTest
public class ReplayServiceMongoParityIT {

    @jakarta.inject.Inject
    MongoClient mongoClient;

    @jakarta.inject.Inject
    MongoReplayStore store;

    private CapturingHttpPoster poster = new CapturingHttpPoster();

    @org.junit.jupiter.api.BeforeEach
    void setup() {
        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL)
                .drop();
        forceHttpPoster(poster);
        poster.calls.clear();
    }

    @org.junit.jupiter.api.Test
    void parity_domain_vs_system_dlq_same_results_and_orchestration() {

        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        java.time.Instant t1 = java.time.Instant.parse("2026-01-07T20:00:01Z");
        java.time.Instant t2 = java.time.Instant.parse("2026-01-07T20:00:02Z");
        java.time.Instant t3 = java.time.Instant.parse("2026-01-07T20:00:03Z");

        col.insertOne(doc("ID-0", payload(0), t1, "DLQ-A"));
        col.insertOne(doc("ID-1", payload(1), t2, "DLQ-A"));
        col.insertOne(doc("ID-2", payload(2), t3, "DLQ-A"));

        ReplayService svc = new ReplayService();
        svc.setStore(store);

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        // act
        ReplayResult a = svc.replayByDomainDlqId(req);
        java.util.List<com.google.gson.JsonObject> callsA =
                new java.util.ArrayList<>(poster.calls);

        poster.calls.clear();

        ReplayResult b = svc.replayBySystemDlqId(req);
        java.util.List<com.google.gson.JsonObject> callsB =
                new java.util.ArrayList<>(poster.calls);

        // assert results
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());
        org.junit.jupiter.api.Assertions.assertEquals(a.matchedCount(), b.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), b.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, a.replayedCount());

        // assert orchestration parity (same payloads, same order)
        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
    }

    // ---------------- helpers ----------------

    private static org.bson.Document doc(
            String id, String payload, java.time.Instant ts, String dlqId) {
        return new org.bson.Document()
                .append("ingestionId", id)
                .append("payload", payload)
                .append("createdAt", java.util.Date.from(ts))
                .append("dlqId", dlqId);
    }

    private static String payload(int n) {
        return """
        {
          "payload": { "n": %d }
        }
        """.formatted(n);
    }

    private static void set(Object target, String field, Object value) {
        try {
            var f = target.getClass().getDeclaredField(field);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void forceHttpPoster(HttpPoster poster) {
        try {
            var f = ProcessorOrchestrator.class.getDeclaredField("httpPoster");
            f.setAccessible(true);
            f.set(ProcessorOrchestrator.getInstance(), poster);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class CapturingHttpPoster implements HttpPoster {
        final java.util.List<com.google.gson.JsonObject> calls = new java.util.ArrayList<>();
        @Override
        public int post(String endpoint, String payload) {
            calls.add(com.google.gson.JsonParser.parseString(payload).getAsJsonObject());
            return 200;
        }
    }
}
