package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MongoReplayStoreIT {

    @Inject
    MongoReplayStore store;

    @Inject
    MongoClient mongoClient;

    @BeforeEach
    void reset() {
        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL)
                .drop();
    }

    @Test
    void findByTimeWindow_returnsEvents_inCreatedAtOrder_andWithinWindow() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        // Arrange: 3 docs (only 2 inside window)
        Instant t0 = Instant.parse("2026-01-07T00:00:00Z");
        Instant t1 = Instant.parse("2026-01-07T00:10:00Z"); // inside
        Instant t2 = Instant.parse("2026-01-07T00:20:00Z"); // inside
        Instant t3 = Instant.parse("2026-01-07T00:30:00Z"); // window end (exclusive)

        col.insertOne(doc("ID-0", "P0", t0));
        col.insertOne(doc("ID-1", "P1", t1));
        col.insertOne(doc("ID-2", "P2", t2));

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", t1.toString());
        set(req, "toTime",   t3.toString());
        set(req, "stream",   "ingestion");
        set(req, "reason",   "it-test");

        // Act
        List<ReplayEvent> events = store.findByTimeWindow(req);

        // Assert: only ID-1, ID-2 in correct order
        assertNotNull(events);
        assertEquals(2, events.size());

        assertEquals("ID-1", events.get(0).id());
        assertEquals("P1",   events.get(0).payload());
        assertEquals(t1,     events.get(0).timestamp());

        assertEquals("ID-2", events.get(1).id());
        assertEquals("P2",   events.get(1).payload());
        assertEquals(t2,     events.get(1).timestamp());
    }

    @Test
    void findByTimeWindow_fromInclusive_toExclusive_edges() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant from = Instant.parse("2026-01-07T01:00:00Z");
        Instant mid  = Instant.parse("2026-01-07T01:10:00Z");
        Instant to   = Instant.parse("2026-01-07T01:20:00Z");

        // exactly at from => included
        col.insertOne(doc("ID-FROM", "P-FROM", from));
        // inside => included
        col.insertOne(doc("ID-MID",  "P-MID",  mid));
        // exactly at to => excluded (because [from, to))
        col.insertOne(doc("ID-TO",   "P-TO",   to));

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", from.toString());
        set(req, "toTime",   to.toString());

        List<ReplayEvent> events = store.findByTimeWindow(req);

        assertNotNull(events);
        assertEquals(2, events.size());

        assertEquals("ID-FROM", events.get(0).id());
        assertEquals("ID-MID",  events.get(1).id());
    }

    @Test
    void findByTimeWindow_nullRequest_returnsEmpty() {
        List<ReplayEvent> events = store.findByTimeWindow(null);
        assertNotNull(events);
        assertTrue(events.isEmpty());
    }

    @Test
    void findByTimeWindow_blankTimes_returnsEmpty() {
        ReplayRequest req = new ReplayRequest();

        // blank from
        set(req, "fromTime", "   ");
        set(req, "toTime",   Instant.parse("2026-01-07T02:00:00Z").toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());

        // blank to
        set(req, "fromTime", Instant.parse("2026-01-07T01:00:00Z").toString());
        set(req, "toTime",   " ");
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_invalidTimes_returnsEmpty() {
        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", "NOT_INSTANT");
        set(req, "toTime",   "2026-01-07T03:00:00Z");
        assertTrue(store.findByTimeWindow(req).isEmpty());

        set(req, "fromTime", "2026-01-07T01:00:00Z");
        set(req, "toTime",   "NOT_INSTANT");
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_fromNotBeforeTo_returnsEmpty() {
        ReplayRequest req = new ReplayRequest();
        Instant t = Instant.parse("2026-01-07T04:00:00Z");

        // from == to
        set(req, "fromTime", t.toString());
        set(req, "toTime",   t.toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());

        // from after to
        set(req, "fromTime", Instant.parse("2026-01-07T05:00:00Z").toString());
        set(req, "toTime",   Instant.parse("2026-01-07T04:00:00Z").toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant from = Instant.parse("2026-01-07T06:00:00Z");
        Instant to   = Instant.parse("2026-01-07T07:00:00Z");

        // doc missing ingestionId/payload; createdAt inside window
        col.insertOne(new Document().append("createdAt", Date.from(Instant.parse("2026-01-07T06:10:00Z"))));

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", from.toString());
        set(req, "toTime",   to.toString());

        List<ReplayEvent> events = store.findByTimeWindow(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.id());
        assertNull(e.payload());
        assertNotNull(e.timestamp());
    }

    // ---------------- helpers ----------------

    private static Document doc(String ingestionId, String payload, Instant createdAt) {
        return new Document()
                .append("ingestionId", ingestionId)
                .append("payload", payload)
                .append("createdAt", Date.from(createdAt));
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
}


