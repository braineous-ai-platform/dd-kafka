package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
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
        MongoDatabase db = mongoClient.getDatabase(MongoReplayStore.DB);

        db.getCollection(MongoReplayStore.INGESTION_COL).drop();
        db.getCollection("dlq_domain").drop();
        db.getCollection("dlq_system").drop();
    }

    // ---------------------------------------------------------------------------------
    @Test
    void findByTimeWindow_happyPath_returnsSortedAndMapped() {

        MongoCollection<Document> col =
                mongoClient.getDatabase(MongoReplayStore.DB)
                        .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-15T17:00:01Z");
        Instant t2 = Instant.parse("2026-01-15T17:00:02Z");
        Instant t3 = Instant.parse("2026-01-15T17:00:03Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t2)));

        col.insertOne(new Document()
                .append("ingestionId", "ID-0")
                .append("payload", "P0")
                .append("createdAt", Date.from(t1)));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "P2")
                .append("createdAt", Date.from(t3)));

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", "2026-01-15T17:00:00Z");
        set(req, "toTime", "2026-01-15T17:00:10Z");

        List<ReplayEvent> out = store.findByTimeWindow(req);

        assertNotNull(out);
        assertEquals(3, out.size());

        // sorted by createdAt asc (then _id)
        assertEquals("P0", out.get(0).payload());
        assertEquals("P1", out.get(1).payload());
        assertEquals("P2", out.get(2).payload());

        assertNotNull(out.get(0).id());
        assertNotNull(out.get(0).timestamp());
    }

    @Test
    void findByTimeWindow_fromNotBeforeTo_returnsEmpty() {

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", "2026-01-15T17:00:10Z");
        set(req, "toTime", "2026-01-15T17:00:10Z");

        List<ReplayEvent> out = store.findByTimeWindow(req);

        assertNotNull(out);
        assertEquals(0, out.size());
    }

    @Test
    void findByTimeWindow_badInstant_returnsEmpty() {

        ReplayRequest req = new ReplayRequest();
        set(req, "fromTime", "not-an-instant");
        set(req, "toTime", "2026-01-15T17:00:10Z");

        List<ReplayEvent> out = store.findByTimeWindow(req);

        assertNotNull(out);
        assertEquals(0, out.size());
    }

    // ---------------------------------------------------------------------------------
    @Test
    void findByTimeObjectKey_trimsIngestionId_andMatches() {

        MongoCollection<Document> col =
                mongoClient.getDatabase(MongoReplayStore.DB)
                        .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-15T17:05:01Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();

        // IMPORTANT: your impl uses request.ingestionId() (not objectKey)
        set(req, "ingestionId", "   ID-1   ");

        List<ReplayEvent> out = store.findByTimeObjectKey(req);

        assertNotNull(out);
        assertEquals(1, out.size());
        assertEquals("P1", out.get(0).payload());
    }

    @Test
    void findByTimeObjectKey_blankIngestionId_returnsEmpty() {

        ReplayRequest req = new ReplayRequest();
        set(req, "ingestionId", "   ");

        List<ReplayEvent> out = store.findByTimeObjectKey(req);

        assertNotNull(out);
        assertEquals(0, out.size());
    }

    // ---------------------------------------------------------------------------------
    @Test
    void findByDomainDlqId_trimsDlqId_andMatches() {

        MongoCollection<Document> col =
                mongoClient.getDatabase(MongoReplayStore.DB)
                        .getCollection("dlq_domain");

        Instant t1 = Instant.parse("2026-01-15T17:10:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-1")
                .append("payload", "P-DOMAIN-1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-1   ");

        List<ReplayEvent> out = store.findByDomainDlqId(req);

        assertNotNull(out);
        assertEquals(1, out.size());
        assertEquals("P-DOMAIN-1", out.get(0).payload());
    }

    @Test
    void findBySystemDlqId_trimsDlqId_andMatches() {

        MongoCollection<Document> col =
                mongoClient.getDatabase(MongoReplayStore.DB)
                        .getCollection("dlq_system");

        Instant t1 = Instant.parse("2026-01-15T17:11:01Z");

        col.insertOne(new Document()
                .append("dlqId", "DLQ-SYS-1")
                .append("payload", "P-SYS-1")
                .append("createdAt", Date.from(t1)));

        ReplayRequest req = new ReplayRequest();
        set(req, "dlqId", "   DLQ-SYS-1   ");

        List<ReplayEvent> out = store.findBySystemDlqId(req);

        assertNotNull(out);
        assertEquals(1, out.size());
        assertEquals("P-SYS-1", out.get(0).payload());
    }

    // ---------------------------------------------------------------------------------
    private static void set(Object target, String fieldName, Object value) {
        if (target == null) {
            throw new IllegalArgumentException("target cannot be null");
        }
        if (fieldName == null || fieldName.trim().length() == 0) {
            throw new IllegalArgumentException("fieldName cannot be blank");
        }

        Class<?> c = target.getClass();
        Field f = null;

        while (c != null && f == null) {
            try {
                f = c.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }

        if (f == null) {
            throw new IllegalArgumentException("No field '" + fieldName + "' on " + target.getClass().getName());
        }

        try {
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed setting field '" + fieldName + "': " + String.valueOf(e), e);
        }
    }
}






