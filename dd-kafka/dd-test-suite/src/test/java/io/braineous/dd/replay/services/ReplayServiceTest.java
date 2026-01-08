package io.braineous.dd.replay.services;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.ReplayStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ReplayServiceTest {

    @org.junit.jupiter.api.Test
    void replayEvents_sortsByTimestamp_thenId() {
        // arrange
        ReplayService svc = new ReplayService() {
            int calls = 0;
            final java.util.List<String> seenIds = new java.util.ArrayList<>();

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                seenIds.add(payloadJson.get("id").getAsString());
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        java.util.List<ReplayEvent> events = new java.util.ArrayList<>();
        // out of order on purpose; same timestamp tie-breaks by id
        events.add(new ReplayEvent("b", "{\"id\":\"b\"}", java.time.Instant.parse("2026-01-05T10:00:02Z")));
        events.add(new ReplayEvent("a", "{\"id\":\"a\"}", java.time.Instant.parse("2026-01-05T10:00:01Z")));
        events.add(new ReplayEvent("c", "{\"id\":\"c\"}", java.time.Instant.parse("2026-01-05T10:00:02Z")));

        ReplayRequest req = new ReplayRequest(); // limit removed

        // act
        ReplayResult result = svc.replayEvents(events, req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.replayedCount());      // all valid replayed
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());       // matched is full list size
        // deterministic order: timestamp asc => "a" first, then tie-break on id between "b" and "c" => "b"
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("a", "b", "c"),
                ((java.util.List<String>) getSeenIds(svc)));
    }

    @org.junit.jupiter.api.Test
    void replayEvents_nullOrEmpty_returnsEmpty_andDoesNotOrchestrate() {
        ReplayService svc = new ReplayService() {
            int calls = 0;

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        ReplayRequest req = new ReplayRequest();
        setLimit(req, 500); // no-op now

        // null list
        ReplayResult r1 = svc.replayEvents(null, req);
        org.junit.jupiter.api.Assertions.assertTrue(r1.ok());
        org.junit.jupiter.api.Assertions.assertEquals(0, r1.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(0, r1.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(0, getCalls(svc));

        // empty list
        ReplayResult r2 = svc.replayEvents(new java.util.ArrayList<>(), req);
        org.junit.jupiter.api.Assertions.assertTrue(r2.ok());
        org.junit.jupiter.api.Assertions.assertEquals(0, r2.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(0, r2.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(0, getCalls(svc));
    }

    @org.junit.jupiter.api.Test
    void replayEvents_badPayload_isSkipped_andReplayContinues() {
        ReplayService svc = new ReplayService() {
            int calls = 0;

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        java.util.List<ReplayEvent> events = new java.util.ArrayList<>();
        events.add(new ReplayEvent("a", "{\"id\":\"a\"}", java.time.Instant.parse("2026-01-05T10:00:01Z")));
        events.add(new ReplayEvent("b", "NOT_JSON", java.time.Instant.parse("2026-01-05T10:00:02Z"))); // bad
        events.add(new ReplayEvent("c", "{\"id\":\"c\"}", java.time.Instant.parse("2026-01-05T10:00:03Z")));

        ReplayRequest req = new ReplayRequest();
        setLimit(req, 500); // no-op now

        ReplayResult result = svc.replayEvents(events, req);

        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.replayedCount(), "Should replay only valid payloads");
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount(), "Matched is still total selected by store");
        org.junit.jupiter.api.Assertions.assertEquals(2, getCalls(svc));
    }

    @org.junit.jupiter.api.Test
    void replayByTimeWindow_storeNull_returnsBadRequest() {
        ReplayService svc = new ReplayService(); // store not injected
        ReplayRequest req = new ReplayRequest();

        ReplayResult result = svc.replayByTimeWindow(req);

        org.junit.jupiter.api.Assertions.assertFalse(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals("store_null", result.reason());
    }

    @org.junit.jupiter.api.Test
    void replayByTimeWindow_usesStore_andReplaysAllSelected() {
        ReplayService svc = new ReplayService() {
            int calls = 0;

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        ReplayStore mem = new ReplayStore() {
            @Override
            public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
                java.util.List<ReplayEvent> events = new java.util.ArrayList<>();
                events.add(new ReplayEvent("a", "{\"id\":\"a\"}", java.time.Instant.parse("2026-01-05T10:00:01Z")));
                events.add(new ReplayEvent("b", "{\"id\":\"b\"}", java.time.Instant.parse("2026-01-05T10:00:02Z")));
                return events;
            }

            @Override
            public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {
                return java.util.List.of();
            }

            @Override
            public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {
                return java.util.List.of();
            }

            @Override
            public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
                return java.util.List.of();
            }
        };

        svc.setStore(mem);

        ReplayRequest req = new ReplayRequest();
        setLimit(req, 500); // no-op now

        ReplayResult result = svc.replayByTimeWindow(req);

        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, getCalls(svc));
    }

    @org.junit.jupiter.api.Test
    void replayByTimeWindow_replaysAllSelected_whenAllValid() {
        ReplayService svc = new ReplayService() {
            int calls = 0;

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        ReplayStore mem = new ReplayStore() {
            @Override
            public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
                java.util.List<ReplayEvent> events = new java.util.ArrayList<>();
                events.add(new ReplayEvent("a", "{\"id\":\"a\"}", java.time.Instant.parse("2026-01-05T10:00:01Z")));
                events.add(new ReplayEvent("b", "{\"id\":\"b\"}", java.time.Instant.parse("2026-01-05T10:00:02Z")));
                events.add(new ReplayEvent("c", "{\"id\":\"c\"}", java.time.Instant.parse("2026-01-05T10:00:03Z")));
                return events;
            }

            @Override public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) { return java.util.List.of(); }
            @Override public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) { return java.util.List.of(); }
            @Override public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) { return java.util.List.of(); }
        };

        svc.setStore(mem);

        ReplayRequest req = new ReplayRequest();
        setLimit(req, 2); // no-op now

        ReplayResult result = svc.replayByTimeWindow(req);

        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, getCalls(svc));
    }

    @org.junit.jupiter.api.Test
    void replayByTimeWindow_skipsBadPayload_andContinues() {
        ReplayService svc = new ReplayService() {
            int calls = 0;

            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls++;
                Console.log("replay_orchestrate", payloadJson);
            }
        };

        ReplayStore mem = new ReplayStore() {
            @Override
            public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
                java.util.List<ReplayEvent> events = new java.util.ArrayList<>();
                events.add(new ReplayEvent("a", "{\"id\":\"a\"}", java.time.Instant.parse("2026-01-05T10:00:01Z")));
                events.add(new ReplayEvent("b", "NOT_JSON", java.time.Instant.parse("2026-01-05T10:00:02Z"))); // bad
                events.add(new ReplayEvent("c", "{\"id\":\"c\"}", java.time.Instant.parse("2026-01-05T10:00:03Z")));
                return events;
            }

            @Override public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) { return java.util.List.of(); }
            @Override public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) { return java.util.List.of(); }
            @Override public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) { return java.util.List.of(); }
        };

        svc.setStore(mem);

        ReplayRequest req = new ReplayRequest();
        setLimit(req, 500); // no-op now

        ReplayResult result = svc.replayByTimeWindow(req);

        org.junit.jupiter.api.Assertions.assertTrue(result.ok());
        org.junit.jupiter.api.Assertions.assertEquals(3, result.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, getCalls(svc));
    }

    @org.junit.jupiter.api.Test
    public void parity_domainDlq_vs_systemDlq_same_events_same_result_and_same_orchestration() {

        // arrange: same events returned for both entrypoints
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-3", "{\"a\":3}", java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        ReplayStore store = storeReturning(events, events, java.util.List.of(), java.util.List.of());

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByDomainDlqId(req);
        ReplayResult b = svcB.replayBySystemDlqId(req);

        // assert: same ReplayResult semantics
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());
        org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), b.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(a.matchedCount(), b.matchedCount());

        // assert: same orchestration count and deterministic order (timestamp then id)
        org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), callsA.size());
        org.junit.jupiter.api.Assertions.assertEquals(b.replayedCount(), callsB.size());
        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);

        // expected order: E-1, E-2, E-3 (sorted by timestamp then id)
        org.junit.jupiter.api.Assertions.assertEquals(1, callsA.get(0).get("a").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(2, callsA.get(1).get("a").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(3, callsA.get(2).get("a").getAsInt());
    }

    @org.junit.jupiter.api.Test
    public void parity_domainDlq_vs_systemDlq_bad_payloads_skipped_consistently() {

        // arrange: include one bad JSON payload that will be skipped
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-bad", "{not-json}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        ReplayStore store = storeReturning(events, events, java.util.List.of(), java.util.List.of());

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByDomainDlqId(req);
        ReplayResult b = svcB.replayBySystemDlqId(req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());

        // matchedCount includes all events returned by store; replayedCount excludes bad payload
        org.junit.jupiter.api.Assertions.assertEquals(3, a.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, b.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, a.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, b.replayedCount());

        // assert orchestrate only called for valid JSON payloads, and in same order
        org.junit.jupiter.api.Assertions.assertEquals(2, callsA.size());
        org.junit.jupiter.api.Assertions.assertEquals(2, callsB.size());
        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
    }

    @org.junit.jupiter.api.Test
    public void parity_domainDlq_vs_systemDlq_replays_all_identically() {

        // arrange: 5 valid events
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-3", "{\"a\":3}", java.time.Instant.parse("2026-01-05T10:00:03Z")),
                new ReplayEvent("E-4", "{\"a\":4}", java.time.Instant.parse("2026-01-05T10:00:04Z")),
                new ReplayEvent("E-5", "{\"a\":5}", java.time.Instant.parse("2026-01-05T10:00:05Z"))
        );

        ReplayStore store = storeReturning(events, events, java.util.List.of(), java.util.List.of());

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByDomainDlqId(req);
        ReplayResult b = svcB.replayBySystemDlqId(req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());
        org.junit.jupiter.api.Assertions.assertEquals(5, a.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(5, b.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(5, a.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(5, b.matchedCount());

        // exact same calls, same order
        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
    }

    @org.junit.jupiter.api.Test
    public void parity_timeWindow_vs_timeObjectKey_same_events_same_result_and_same_orchestration() {

        // arrange
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-3", "{\"a\":3}", java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        ReplayStore store = storeReturning(java.util.List.of(), java.util.List.of(), events, events);

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByTimeWindow(req);
        ReplayResult b = svcB.replayByTimeObjectKey(req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());
        org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), b.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(a.matchedCount(), b.matchedCount());

        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
        org.junit.jupiter.api.Assertions.assertEquals(a.replayedCount(), callsA.size());
        org.junit.jupiter.api.Assertions.assertEquals(b.replayedCount(), callsB.size());
    }

    @org.junit.jupiter.api.Test
    public void parity_timeWindow_vs_timeObjectKey_bad_payloads_skipped_consistently() {

        // arrange
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-bad", "{not-json}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:03Z"))
        );

        ReplayStore store = storeReturning(java.util.List.of(), java.util.List.of(), events, events);

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByTimeWindow(req);
        ReplayResult b = svcB.replayByTimeObjectKey(req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());

        org.junit.jupiter.api.Assertions.assertEquals(3, a.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(3, b.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, a.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(2, b.replayedCount());

        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
    }

    @org.junit.jupiter.api.Test
    public void parity_timeWindow_vs_timeObjectKey_replays_all_identically() {

        // arrange
        java.util.List<ReplayEvent> events = java.util.List.of(
                new ReplayEvent("E-1", "{\"a\":1}", java.time.Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-2", "{\"a\":2}", java.time.Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-3", "{\"a\":3}", java.time.Instant.parse("2026-01-05T10:00:03Z")),
                new ReplayEvent("E-4", "{\"a\":4}", java.time.Instant.parse("2026-01-05T10:00:04Z"))
        );

        ReplayStore store = storeReturning(java.util.List.of(), java.util.List.of(), events, events);

        java.util.List<com.google.gson.JsonObject> callsA = new java.util.ArrayList<>();
        java.util.List<com.google.gson.JsonObject> callsB = new java.util.ArrayList<>();

        ReplayService svcA = serviceSpy(store, callsA);
        ReplayService svcB = serviceSpy(store, callsB);

        ReplayRequest req = org.mockito.Mockito.mock(ReplayRequest.class);

        // act
        ReplayResult a = svcA.replayByTimeWindow(req);
        ReplayResult b = svcB.replayByTimeObjectKey(req);

        // assert
        org.junit.jupiter.api.Assertions.assertTrue(a.ok());
        org.junit.jupiter.api.Assertions.assertTrue(b.ok());
        org.junit.jupiter.api.Assertions.assertEquals(4, a.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(4, b.matchedCount());
        org.junit.jupiter.api.Assertions.assertEquals(4, a.replayedCount());
        org.junit.jupiter.api.Assertions.assertEquals(4, b.replayedCount());

        org.junit.jupiter.api.Assertions.assertEquals(callsA, callsB);
    }

    //--------------------------------------------------------------
    private static Object getSeenIds(ReplayService svc) {
        try {
            java.lang.reflect.Field f = svc.getClass().getDeclaredField("seenIds");
            f.setAccessible(true);
            return f.get(svc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int getCalls(ReplayService svc) {
        try {
            java.lang.reflect.Field f = svc.getClass().getDeclaredField("calls");
            f.setAccessible(true);
            return (int) f.get(svc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setLimit(ReplayRequest req, int limit) {
        // limit removed from ReplayRequest; keep helper so old tests compile and don't blow up
        // no-op by design
    }

    private ReplayService serviceSpy(ReplayStore store, java.util.List<com.google.gson.JsonObject> calls) {
        ReplayService svc = new ReplayService() {
            @Override
            void orchestrate(com.google.gson.JsonObject payloadJson) {
                calls.add(payloadJson);
            }
        };
        svc.setStore(store);
        return svc;
    }

    private ReplayStore storeReturning(
            java.util.List<ReplayEvent> byDomainDlq,
            java.util.List<ReplayEvent> bySystemDlq,
            java.util.List<ReplayEvent> byTimeWindow,
            java.util.List<ReplayEvent> byTimeObjectKey
    ) {
        return new ReplayStore() {
            @Override public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {
                return mutable(byDomainDlq);
            }
            @Override public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
                return mutable(bySystemDlq);
            }
            @Override public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
                return mutable(byTimeWindow);
            }
            @Override public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {
                return mutable(byTimeObjectKey);
            }
        };
    }

    private static java.util.List<ReplayEvent> mutable(java.util.List<ReplayEvent> in) {
        return (in == null) ? null : new java.util.ArrayList<>(in);
    }
}

