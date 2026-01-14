package io.braineous.dd.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Date;

import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.braineous.dd.replay.services.ReplayService;
import jakarta.inject.Inject;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ReplayServiceIT {

    @Inject
    ReplayService replayService;

    @Inject
    com.mongodb.client.MongoClient mongoClient;

    @BeforeEach
    void setUp() {
        dropCollections();
    }

    @AfterEach
    void tearDown() {
        dropCollections();
    }

    @Test
    public void it_replay_triggers_ingestion_via_orchestrator_for_all_events() {

        seedReplayDocs();

        ReplayRequest req = new ReplayRequest();
        set(req, "stream", "ingestion");
        set(req, "reason", "it-test");
        set(req, "fromTime", "2026-01-12T00:00:00Z");
        set(req, "toTime",   "2026-01-12T00:00:10Z");

        ReplayResult result = replayService.replayByTimeWindow(req);

        assertNotNull(result);
        assertTrue(result.ok());
        assertEquals(2, result.replayedCount());

        var ingestionCol = mongoClient
                .getDatabase(MongoReplayStore.DB) // replace with MongoIngestionStore.DB if different
                .getCollection(MongoReplayStore.INGESTION_COL); // replace with MongoIngestionStore.INGESTION_COL if different

        assertEquals(1, ingestionCol.countDocuments(new Document("payload", "{\"payload\":\"P1\"}")));
        assertEquals(1, ingestionCol.countDocuments(new Document("payload", "{\"payload\":\"P2\"}")));
    }

    // ---------------- helpers ----------------

    private void seedReplayDocs() {
        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-12T00:00:01Z");
        Instant t2 = Instant.parse("2026-01-12T00:00:02Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "{\"payload\":\"P1\"}")
                .append("createdAt", Date.from(t1))
                .append("objectKey", "KEY-A"));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "{\"payload\":\"P2\"}")
                .append("createdAt", Date.from(t2))
                .append("objectKey", "KEY-A"));
    }

    private void dropCollections() {
        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL)
                .drop();

        // also drop ingestion store collection here if it is different
        // mongoClient.getDatabase(MongoIngestionStore.DB)
        //        .getCollection(MongoIngestionStore.INGESTION_COL)
        //        .drop();
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



