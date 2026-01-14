package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
    @AfterEach
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


    @Test
    void findByTimeObjectKey_returnsEvents_matchingKey_sortedByCreatedAt() {
        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T10:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T10:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T10:00:03Z");

        // non-matching key
        col.insertOne(new Document()
                .append("ingestionId", "KEY-B")
                .append("payload", "Px")
                .append("createdAt", Date.from(t2)));

        // matching key = ingestionId (current contract)
        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t2)));

        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P0")
                .append("createdAt", Date.from(t1)));

        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P2")
                .append("createdAt", Date.from(t3)));

        ReplayRequest req = new ReplayRequest();
        set(req, "objectKey", "KEY-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeObjectKey(req);

        assertNotNull(events);
        assertEquals(3, events.size());

        assertEquals("KEY-A", events.get(0).id());
        assertEquals("P0", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("KEY-A", events.get(1).id());
        assertEquals("P1", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());

        assertEquals("KEY-A", events.get(2).id());
        assertEquals("P2", events.get(2).payload());
        assertEquals(t3, events.get(2).timestamp());
    }


    @Test
    void findByTimeObjectKey_nullOrBlankKey_returnsEmpty() {

        // null request => empty
        assertNotNull(store.findByTimeObjectKey(null));
        assertTrue(store.findByTimeObjectKey(null).isEmpty());

        // null key => empty
        ReplayRequest r1 = new ReplayRequest();
        set(r1, "objectKey", null);
        set(r1, "stream", "ingestion");
        set(r1, "reason", "it-test");
        assertTrue(store.findByTimeObjectKey(r1).isEmpty());

        // blank key => empty
        ReplayRequest r2 = new ReplayRequest();
        set(r2, "objectKey", "   ");
        set(r2, "stream", "ingestion");
        set(r2, "reason", "it-test");
        assertTrue(store.findByTimeObjectKey(r2).isEmpty());
    }

    @Test
    void findByTimeObjectKey_trimsKey_andMatches() {
        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T11:00:01Z");

        // current contract: objectKey == ingestionId
        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "objectKey", "   KEY-A   "); // trimming must happen
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeObjectKey(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertEquals("KEY-A", e.id());
        assertEquals("P1", e.payload());
        assertEquals(t1, e.timestamp());
    }


    @Test
    void findByTimeObjectKey_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T12:00:01Z");

        // missing payload; still matches objectKey (objectKey == ingestionId contract)
        col.insertOne(new Document()
                .append("createdAt", Date.from(t1))
                .append("ingestionId", "KEY-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "objectKey", "KEY-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeObjectKey(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNotNull(e.id());
        assertEquals("KEY-A", e.id());
        assertNull(e.payload());
        assertNotNull(e.timestamp());
    }


    @Test
    void findByDomainDlqId_returnsEvents_matchingDlqId_sortedByCreatedAt() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T13:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T13:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T13:00:03Z");

        // only DLQ-A should match
        col.insertOne(new Document()
                .append("ingestionId", "ID-x")
                .append("payload", "Px")
                .append("createdAt", Date.from(t2))
                .append("dlqId", "DLQ-B"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t2))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-0")
                .append("payload", "P0")
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "P2")
                .append("createdAt", Date.from(t3))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(3, events.size());

        assertEquals("ID-0", events.get(0).id());
        assertEquals("P0", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals(t2, events.get(1).timestamp());
        assertEquals(t3, events.get(2).timestamp());
    }

    @Test
    void findByDomainDlqId_nullOrBlankDlqId_returnsEmpty() {

        assertNotNull(store.findByDomainDlqId(null));
        assertTrue(store.findByDomainDlqId(null).isEmpty());

        ReplayRequest r1 = new ReplayRequest();
        set(r1, "dlqId", null);
        set(r1, "stream", "ingestion");
        set(r1, "reason", "it-test");
        assertTrue(store.findByDomainDlqId(r1).isEmpty());

        ReplayRequest r2 = new ReplayRequest();
        set(r2, "dlqId", "   ");
        set(r2, "stream", "ingestion");
        set(r2, "reason", "it-test");
        assertTrue(store.findByDomainDlqId(r2).isEmpty());
    }

    @Test
    void findByDomainDlqId_trimsDlqId_andMatches() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T14:00:01Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-A   ");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals("ID-1", events.get(0).id());
    }

    @Test
    void findByDomainDlqId_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T15:00:01Z");

        // missing ingestionId/payload; still matches dlqId
        col.insertOne(new Document()
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.id());
        assertNull(e.payload());
        assertNotNull(e.timestamp());
    }

    @Test
    void findBySystemDlqId_returnsEvents_matchingDlqId_sortedByCreatedAt() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T16:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T16:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T16:00:03Z");

        // only DLQ-A should match
        col.insertOne(new Document()
                .append("ingestionId", "ID-x")
                .append("payload", "Px")
                .append("createdAt", Date.from(t2))
                .append("dlqId", "DLQ-B"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t2))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-0")
                .append("payload", "P0")
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "P2")
                .append("createdAt", Date.from(t3))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(3, events.size());

        assertEquals("ID-0", events.get(0).id());
        assertEquals("P0", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals(t2, events.get(1).timestamp());
        assertEquals(t3, events.get(2).timestamp());
    }

    @Test
    void findBySystemDlqId_nullOrBlankDlqId_returnsEmpty() {

        assertNotNull(store.findBySystemDlqId(null));
        assertTrue(store.findBySystemDlqId(null).isEmpty());

        ReplayRequest r1 = new ReplayRequest();
        set(r1, "dlqId", null);
        set(r1, "stream", "ingestion");
        set(r1, "reason", "it-test");
        assertTrue(store.findBySystemDlqId(r1).isEmpty());

        ReplayRequest r2 = new ReplayRequest();
        set(r2, "dlqId", "   ");
        set(r2, "stream", "ingestion");
        set(r2, "reason", "it-test");
        assertTrue(store.findBySystemDlqId(r2).isEmpty());
    }

    @Test
    void findBySystemDlqId_trimsDlqId_andMatches() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T17:00:01Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-A   ");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals("ID-1", events.get(0).id());
    }

    @Test
    void findBySystemDlqId_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T18:00:01Z");

        // missing ingestionId/payload; still matches dlqId
        col.insertOne(new Document()
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.id());
        assertNull(e.payload());
        assertNotNull(e.timestamp());
    }

    @Test
    void parity_domainDlq_vs_systemDlq_same_results_same_order() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T19:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T19:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T19:00:03Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-0")
                .append("payload", "P0")
                .append("createdAt", Date.from(t1))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t2))
                .append("dlqId", "DLQ-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "P2")
                .append("createdAt", Date.from(t3))
                .append("dlqId", "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> a = store.findByDomainDlqId(req);
        List<ReplayEvent> b = store.findBySystemDlqId(req);

        assertNotNull(a);
        assertNotNull(b);
        assertEquals(a.size(), b.size());
        assertEquals(3, a.size());

        // strict equality on mapped fields in order
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).id(),        b.get(i).id());
            assertEquals(a.get(i).payload(),   b.get(i).payload());
            assertEquals(a.get(i).timestamp(), b.get(i).timestamp());
        }
    }



    //-----------------------------------------------------------------


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


