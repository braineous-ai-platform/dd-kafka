package io.braineous.dd.replay;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.braineous.dd.replay.persistence.ReplayStore;
import io.braineous.dd.replay.services.ReplayService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ReplayServiceIT {

    private CapturingHttpPoster poster = new CapturingHttpPoster();

    @org.junit.jupiter.api.BeforeEach
    void clear() {
        poster.calls.clear();
        forceHttpPoster(poster);
    }

    @org.junit.jupiter.api.Test
    public void it_replay_triggers_ingestion_via_orchestrator_for_all_events() {

        // -------- arrange --------
        java.util.List<ReplayEvent> events = java.util.List.of(
                event("E-1", validPayload(1), java.time.Instant.parse("2026-01-05T10:00:01Z")),
                event("E-2", validPayload(2), java.time.Instant.parse("2026-01-05T10:00:02Z")),
                event("E-3", validPayload(3), java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);

        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(10); // limit removed; kept for call-site compatibility

        // -------- act --------
        ReplayResult result = service.replayByDomainDlqId(req);

        // -------- assert --------
        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.replayedCount());

        org.junit.jupiter.api.Assertions.assertEquals(3, poster.calls.size());
        org.junit.jupiter.api.Assertions.assertEquals(1, poster.calls.get(0).getAsJsonObject("payload").get("n").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(2, poster.calls.get(1).getAsJsonObject("payload").get("n").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(3, poster.calls.get(2).getAsJsonObject("payload").get("n").getAsInt());
    }

    @org.junit.jupiter.api.Test
    public void it_replay_skips_bad_json_payload_and_still_posts_valid_ones() {

        // arrange: 2 valid + 1 invalid JSON payload
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", validPayload(1), java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-bad", "{not-json}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-2", validPayload(2), java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);

        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(10); // limit removed

        // act
        ReplayResult result = service.replayByDomainDlqId(req);

        // assert: matched includes all events; replayed excludes bad JSON
        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.replayedCount());

        // assert: only 2 posts happened, and they correspond to n=1 then n=2
        org.junit.jupiter.api.Assertions.assertEquals(2, poster.calls.size());
        org.junit.jupiter.api.Assertions.assertEquals(1, poster.calls.get(0).getAsJsonObject("payload").get("n").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(2, poster.calls.get(1).getAsJsonObject("payload").get("n").getAsInt());
    }

    @org.junit.jupiter.api.Test
    public void it_replay_replays_all_valid_events_no_limit_behavior() {

        // arrange: 3 valid events (old test was limit=1; limit removed now)
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", validPayload(1), java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-2", validPayload(2), java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-3", validPayload(3), java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);

        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(1); // kept for call-site compatibility

        // act
        ReplayResult result = service.replayByDomainDlqId(req);

        // assert: matched = all, replayed = all (no limit)
        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.replayedCount());

        // assert: all 3 events posted, earliest first
        org.junit.jupiter.api.Assertions.assertEquals(3, poster.calls.size());
        org.junit.jupiter.api.Assertions.assertEquals(1, poster.calls.get(0).getAsJsonObject("payload").get("n").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(2, poster.calls.get(1).getAsJsonObject("payload").get("n").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(3, poster.calls.get(2).getAsJsonObject("payload").get("n").getAsInt());
    }

    @io.quarkus.test.junit.QuarkusTest
    public class ReplayServiceTimeParityIT {

        @jakarta.inject.Inject
        com.mongodb.client.MongoClient mongoClient;

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
        void parity_timeWindow_vs_timeObjectKey_same_results_and_orchestration() {

            var col = mongoClient.getDatabase(MongoReplayStore.DB)
                    .getCollection(MongoReplayStore.INGESTION_COL);

            java.time.Instant t1 = java.time.Instant.parse("2026-01-07T21:00:01Z");
            java.time.Instant t2 = java.time.Instant.parse("2026-01-07T21:00:02Z");
            java.time.Instant t3 = java.time.Instant.parse("2026-01-07T21:00:03Z");
            java.time.Instant t4 = java.time.Instant.parse("2026-01-07T21:00:04Z");

            // Insert 4 docs, all share the same objectKey
            col.insertOne(doc("ID-0", payload(0), t1, "OBJ-A"));
            col.insertOne(doc("ID-1", payload(1), t2, "OBJ-A"));
            col.insertOne(doc("ID-2", payload(2), t3, "OBJ-A"));
            col.insertOne(doc("ID-3", payload(3), t4, "OBJ-A"));

            ReplayService svc = new ReplayService();
            svc.setStore(store);

            // -------- act: time window [t1, t4)
            ReplayRequest byWindow = new ReplayRequest();
            set(byWindow, "fromTime", t1.toString());
            set(byWindow, "toTime",   t4.toString());
            set(byWindow, "stream",   "ingestion");
            set(byWindow, "reason",   "it-test");

            ReplayResult a = svc.replayByTimeWindow(byWindow);
            java.util.List<com.google.gson.JsonObject> callsA =
                    new java.util.ArrayList<>(poster.calls);

            poster.calls.clear();

            // -------- act: object key (same rows)
            ReplayRequest byKey = new ReplayRequest();
            set(byKey, "objectKey", "OBJ-A");
            set(byKey, "stream",   "ingestion");
            set(byKey, "reason",   "it-test");

            ReplayResult b = svc.replayByTimeObjectKey(byKey);
            java.util.List<com.google.gson.JsonObject> callsB =
                    new java.util.ArrayList<>(poster.calls);

            // -------- assert parity
            org.junit.jupiter.api.Assertions.assertTrue(a.ok());
            org.junit.jupiter.api.Assertions.assertTrue(b.ok());
            org.junit.jupiter.api.Assertions.assertEquals(a.matchedCount(), b.matchedCount());
            org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), b.replayedCount());
            org.junit.jupiter.api.Assertions.assertEquals(3, a.replayedCount()); // t1,t2,t3

            // same payloads, same order
            org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
        }

        // ---------------- helpers ----------------

        private static org.bson.Document doc(
                String id, String payload, java.time.Instant ts, String objectKey) {
            return new org.bson.Document()
                    .append("ingestionId", id)
                    .append("payload", payload)
                    .append("createdAt", java.util.Date.from(ts))
                    .append("objectKey", objectKey);
        }

        private static String payload(int n) {
            return """
        { "payload": { "n": %d } }
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

    //-------helpers---------------------------------
    class InMemoryReplayStore implements ReplayStore {

        private final java.util.List<ReplayEvent> events;

        InMemoryReplayStore(java.util.List<ReplayEvent> events) {
            this.events = new java.util.ArrayList<>(events);
        }

        @Override public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {
            return new java.util.ArrayList<>(events);
        }

        @Override public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
            return new java.util.ArrayList<>(events);
        }

        @Override public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
            return new java.util.ArrayList<>(events);
        }

        @Override public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {
            return new java.util.ArrayList<>(events);
        }
    }

    class CapturingHttpPoster implements HttpPoster {

        final java.util.List<com.google.gson.JsonObject> calls = new java.util.ArrayList<>();

        @Override
        public int post(String endpoint, String payload) {
            calls.add(com.google.gson.JsonParser.parseString(payload).getAsJsonObject());
            return 200;
        }
    }

    private static ReplayEvent event(String id, String payload, java.time.Instant ts) {
        return new ReplayEvent(id, payload, ts);
    }

    private static String validPayload(int n) {
        return """
        {
          "kafka": {
            "topic": "t",
            "partition": 0,
            "offset": %d,
            "timestamp": %d
          },
          "payload": {
            "encoding": "base64",
            "value": "dGVzdA==",
            "n": %d
          }
        }
        """.formatted(n, System.currentTimeMillis(), n);
    }

    private static ReplayRequest fakeRequest(int limit) {
        // limit removed; argument kept to avoid touching call sites
        return new ReplayRequest();
    }

    private static void forceHttpPoster(HttpPoster poster) {
        try {
            java.lang.reflect.Field f = ProcessorOrchestrator.class.getDeclaredField("httpPoster");
            f.setAccessible(true);
            f.set(ProcessorOrchestrator.getInstance(), poster);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

