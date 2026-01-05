package io.braineous.dd.replay;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
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

    @BeforeEach
    void clear() {
        poster.calls.clear();
        forceHttpPoster(poster);
    }


    @Test
    public void it_replay_triggers_ingestion_via_orchestrator_for_all_events() {

        // -------- arrange --------
        List<ReplayEvent> events = List.of(
                event("E-1", validPayload(1), Instant.parse("2026-01-05T10:00:01Z")),
                event("E-2", validPayload(2), Instant.parse("2026-01-05T10:00:02Z")),
                event("E-3", validPayload(3), Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);


        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(10);

        // -------- act --------
        ReplayResult result = service.replayByDomainDlqId(req);

        // -------- assert --------
        assertTrue(result.ok());
        assertEquals(3, result.matchedCount());
        assertEquals(3, result.replayedCount());

        assertEquals(3, poster.calls.size());
        assertEquals(1, poster.calls.get(0).get("payload").getAsJsonObject().get("n").getAsInt());
        assertEquals(2, poster.calls.get(1).get("payload").getAsJsonObject().get("n").getAsInt());
        assertEquals(3, poster.calls.get(2).get("payload").getAsJsonObject().get("n").getAsInt());
    }

    @Test
    public void it_replay_skips_bad_json_payload_and_still_posts_valid_ones() {

        // arrange: 2 valid + 1 invalid JSON payload
        List<ReplayEvent> events = List.of(
                new ReplayEvent("E-1", validPayload(1), Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-bad", "{not-json}", Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-2", validPayload(2), Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);

        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(10);

        // act
        ReplayResult result = service.replayByDomainDlqId(req);

        // assert: matched includes all events; replayed excludes bad JSON
        assertTrue(result.ok());
        assertEquals(3, result.matchedCount());
        assertEquals(2, result.replayedCount());

        // assert: only 2 posts happened, and they correspond to n=1 then n=2
        assertEquals(2, poster.calls.size());
        assertEquals(1, poster.calls.get(0).getAsJsonObject("payload").get("n").getAsInt());
        assertEquals(2, poster.calls.get(1).getAsJsonObject("payload").get("n").getAsInt());
    }

    @Test
    public void it_replay_respects_limit_and_stops_posting_beyond_limit() {

        // arrange: 3 valid events, limit = 1
        List<ReplayEvent> events = List.of(
                new ReplayEvent("E-1", validPayload(1), Instant.parse("2026-01-05T10:00:01Z")),
                new ReplayEvent("E-2", validPayload(2), Instant.parse("2026-01-05T10:00:02Z")),
                new ReplayEvent("E-3", validPayload(3), Instant.parse("2026-01-05T10:00:03Z"))
        );

        InMemoryReplayStore store = new InMemoryReplayStore(events);

        ReplayService service = new ReplayService();
        service.setStore(store);

        ReplayRequest req = fakeRequest(1); // hard limit

        // act
        ReplayResult result = service.replayByDomainDlqId(req);

        // assert: matched = all, replayed = limit
        assertTrue(result.ok());
        assertEquals(3, result.matchedCount());
        assertEquals(1, result.replayedCount());

        // assert: only first (earliest) event posted
        assertEquals(1, poster.calls.size());
        assertEquals(1, poster.calls.get(0)
                .getAsJsonObject("payload")
                .get("n").getAsInt());
    }


    //-------helpers---------------------------------
    class InMemoryReplayStore implements ReplayStore {

        private final List<ReplayEvent> events;

        InMemoryReplayStore(List<ReplayEvent> events) {
            this.events = new ArrayList<>(events);
        }

        @Override public List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {
            return new ArrayList<>(events);
        }

        @Override public List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
            return new ArrayList<>(events);
        }

        @Override public List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
            return new ArrayList<>(events);
        }

        @Override public List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {
            return new ArrayList<>(events);
        }
    }

    class CapturingHttpPoster implements HttpPoster {

        final List<JsonObject> calls = new ArrayList<>();

        @Override
        public int post(String endpoint, String payload) {
            calls.add(JsonParser.parseString(payload).getAsJsonObject());
            return 200;
        }
    }

    private static ReplayEvent event(String id, String payload, Instant ts) {
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
        return new ReplayRequest() {
            @Override public int limitOrDefault(int def) { return limit; }
        };
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

}
