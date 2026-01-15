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

        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection("dlq_domain")
                .drop();

        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection("dlq_system")
                .drop();
    }

    // -------------------------------------------------------------------------
    // TIME WINDOW (ingestion col)
    // -------------------------------------------------------------------------

    @Test
    void findByTimeWindow_returnsEvents_inCreatedAtOrder_andWithinWindow() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t0 = Instant.parse("2026-01-07T00:00:00Z");
        Instant t1 = Instant.parse("2026-01-07T00:10:00Z"); // inside
        Instant t2 = Instant.parse("2026-01-07T00:20:00Z"); // inside
        Instant t3 = Instant.parse("2026-01-07T00:30:00Z"); // to exclusive

        col.insertOne(doc("ID-0", "P0", t0));
        col.insertOne(doc("ID-1", "P1", t1));
        col.insertOne(doc("ID-2", "P2", t2));
        col.insertOne(doc("ID-3", "P3", t3)); // excluded by to-exclusive

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", t1.toString());
        set(req, "toTime", t3.toString());
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeWindow(req);

        assertNotNull(events);
        assertEquals(2, events.size());

        // assert stable contract: payload + timestamp + order (NO ingestionId)
        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("P2", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());

        assertTrue(events.get(0).timestamp().isBefore(events.get(1).timestamp()));
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

        set(req, "fromTime", "   ");
        set(req, "toTime", Instant.parse("2026-01-07T02:00:00Z").toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());

        set(req, "fromTime", Instant.parse("2026-01-07T01:00:00Z").toString());
        set(req, "toTime", " ");
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_invalidTimes_returnsEmpty() {
        ReplayRequest req = new ReplayRequest();

        set(req, "fromTime", "NOT_INSTANT");
        set(req, "toTime", "2026-01-07T03:00:00Z");
        assertTrue(store.findByTimeWindow(req).isEmpty());

        set(req, "fromTime", "2026-01-07T01:00:00Z");
        set(req, "toTime", "NOT_INSTANT");
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_fromNotBeforeTo_returnsEmpty() {
        ReplayRequest req = new ReplayRequest();
        Instant t = Instant.parse("2026-01-07T04:00:00Z");

        set(req, "fromTime", t.toString());
        set(req, "toTime", t.toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());

        set(req, "fromTime", Instant.parse("2026-01-07T05:00:00Z").toString());
        set(req, "toTime", Instant.parse("2026-01-07T04:00:00Z").toString());
        assertTrue(store.findByTimeWindow(req).isEmpty());
    }

    @Test
    void findByTimeWindow_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T12:00:01Z");

        // payload missing => ReplayEvent.payload() should be null; timestamp maps
        col.insertOne(new Document()
                .append("createdAt", Date.from(t1))
                .append("ingestionId", "ID-1"));

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", "2026-01-07T12:00:00Z");
        set(req, "toTime", "2026-01-07T12:10:00Z");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeWindow(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.payload());
        assertNotNull(e.timestamp());

        // id should exist (either _id or ingestionId fallback)
        assertNotNull(e.id());
    }

    @Test
    void findByTimeWindow_fromInclusive_toExclusive_edges() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant from = Instant.parse("2026-01-07T01:00:00Z");
        Instant justBeforeTo = Instant.parse("2026-01-07T01:19:59Z");
        Instant to = Instant.parse("2026-01-07T01:20:00Z");

        col.insertOne(doc("ID-FROM", "P-FROM", from));
        col.insertOne(doc("ID-IN", "P-IN", justBeforeTo));
        col.insertOne(doc("ID-TO", "P-TO", to)); // excluded

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", from.toString());
        set(req, "toTime", to.toString());
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeWindow(req);

        assertNotNull(events);
        assertEquals(2, events.size());

        assertEquals("P-FROM", events.get(0).payload());
        assertEquals(from, events.get(0).timestamp());

        assertEquals("P-IN", events.get(1).payload());
        assertEquals(justBeforeTo, events.get(1).timestamp());
    }

    // -------------------------------------------------------------------------
    // TIME + OBJECT KEY (ingestion col; selector is ingestionId == objectKey)
    // -------------------------------------------------------------------------

    @Test
    void findByTimeObjectKey_nullOrBlankKey_returnsEmpty() {
        assertNotNull(store.findByTimeObjectKey(null));
        assertTrue(store.findByTimeObjectKey(null).isEmpty());

        ReplayRequest r1 = new ReplayRequest();
        set(r1, "objectKey", null);
        set(r1, "stream", "ingestion");
        set(r1, "reason", "it-test");
        assertTrue(store.findByTimeObjectKey(r1).isEmpty());

        ReplayRequest r2 = new ReplayRequest();
        set(r2, "objectKey", "   ");
        set(r2, "stream", "ingestion");
        set(r2, "reason", "it-test");
        assertTrue(store.findByTimeObjectKey(r2).isEmpty());
    }

    @Test
    void findByTimeObjectKey_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T12:00:01Z");

        // missing payload; still matches ingestionId == objectKey
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
        assertNull(e.payload());
        assertNotNull(e.timestamp());
        assertNotNull(e.id()); // _id or ingestionId fallback
    }

    @Test
    void findByTimeObjectKey_returnsEvents_matchingKey_sortedByCreatedAt() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T01:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T01:00:02Z");

        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P2")
                .append("createdAt", Date.from(t2)));

        ReplayRequest req = new ReplayRequest();
        set(req, "objectKey", "KEY-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeObjectKey(req);

        assertNotNull(events);
        assertEquals(2, events.size());

        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("P2", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());
    }

    @Test
    void findByTimeObjectKey_trimsKey_andMatches() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T11:00:01Z");

        col.insertOne(new Document()
                .append("ingestionId", "KEY-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "objectKey", "   KEY-A   ");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByTimeObjectKey(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());
    }

    // -------------------------------------------------------------------------
    // DLQ DOMAIN (dlq_domain col)
    // -------------------------------------------------------------------------

    @Test
    void findByDomainDlqId_returnsEvents_matchingDlqId_sortedByCreatedAt() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_domain");

        Instant t1 = Instant.parse("2026-01-07T13:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T13:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T13:00:03Z");

        // control: other dlqId
        col.insertOne(new Document()
                .append("dlqId", "DLQ-B")
                .append("payload", "PX")
                .append("createdAt", Date.from(t2)));

        // match: DLQ-A (3)
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P0").append("createdAt", Date.from(t1)));
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P1").append("createdAt", Date.from(t2)));
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P2").append("createdAt", Date.from(t3)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(3, events.size());

        assertEquals("P0", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("P1", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());

        assertEquals("P2", events.get(2).payload());
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
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_domain");

        Instant t1 = Instant.parse("2026-01-07T14:00:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-A   ");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());
    }

    @Test
    void findByDomainDlqId_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_domain");

        Instant t1 = Instant.parse("2026-01-07T15:00:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-A")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findByDomainDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.payload());
        assertNotNull(e.timestamp());
        assertNotNull(e.id()); // should be _id string
    }

    // -------------------------------------------------------------------------
    // DLQ SYSTEM (dlq_system col)
    // -------------------------------------------------------------------------

    @Test
    void findBySystemDlqId_returnsEvents_matchingDlqId_sortedByCreatedAt() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_system");

        Instant t1 = Instant.parse("2026-01-07T16:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T16:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T16:00:03Z");

        // control: other dlqId
        col.insertOne(new Document()
                .append("dlqId", "DLQ-B")
                .append("payload", "PX")
                .append("createdAt", Date.from(t2)));

        // match: DLQ-A (3)
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P0").append("createdAt", Date.from(t1)));
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P1").append("createdAt", Date.from(t2)));
        col.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P2").append("createdAt", Date.from(t3)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(3, events.size());

        assertEquals("P0", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("P1", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());

        assertEquals("P2", events.get(2).payload());
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
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_system");

        Instant t1 = Instant.parse("2026-01-07T17:00:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-A")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-A   ");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());
    }

    @Test
    void findBySystemDlqId_missingFields_doesNotCrash_andMapsNulls() {
        var col = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_system");

        Instant t1 = Instant.parse("2026-01-07T18:00:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-A")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> events = store.findBySystemDlqId(req);

        assertNotNull(events);
        assertEquals(1, events.size());

        ReplayEvent e = events.get(0);
        assertNull(e.payload());
        assertNotNull(e.timestamp());
        assertNotNull(e.id());
    }

    @Test
    void parity_domainDlq_vs_systemDlq_same_results_same_order() {
        var domain = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_domain");
        var system = mongoClient.getDatabase(MongoReplayStore.DB).getCollection("dlq_system");

        Instant t1 = Instant.parse("2026-01-07T19:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T19:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T19:00:03Z");

        domain.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P0").append("createdAt", Date.from(t1)));
        domain.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P1").append("createdAt", Date.from(t2)));
        domain.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P2").append("createdAt", Date.from(t3)));

        system.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P0").append("createdAt", Date.from(t1)));
        system.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P1").append("createdAt", Date.from(t2)));
        system.insertOne(new Document().append("dlqId", "DLQ-A").append("payload", "P2").append("createdAt", Date.from(t3)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "DLQ-A");
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");

        List<ReplayEvent> a = store.findByDomainDlqId(req);
        List<ReplayEvent> b = store.findBySystemDlqId(req);

        assertNotNull(a);
        assertNotNull(b);
        assertEquals(3, a.size());
        assertEquals(3, b.size());

        // parity on replay-surface fields (payload+timestamp). id can differ across collections.
        for (int i = 0; i < 3; i++) {
            assertEquals(a.get(i).payload(), b.get(i).payload());
            assertEquals(a.get(i).timestamp(), b.get(i).timestamp());
        }
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

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




