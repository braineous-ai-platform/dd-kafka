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

        seedReplayDocs(); // ensure payload contains view.snapshotHash so ingestion store can resolve

        ReplayRequest req = new ReplayRequest();
        set(req, "reason", "it-test");
        set(req, "fromTime", "2026-01-12T00:00:00Z");
        set(req, "toTime",   "2026-01-12T00:00:10Z");

        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        // --- capture createdAt BEFORE (select by snapshotHash; never by ingestionId) ---
        Document p1Before = col.find(new Document("snapshotHash", "SNAP-1")).first();
        Document p2Before = col.find(new Document("snapshotHash", "SNAP-2")).first();

        assertNotNull(p1Before);
        assertNotNull(p2Before);

        Date c1Before = p1Before.getDate("createdAt");
        Date c2Before = p2Before.getDate("createdAt");

        assertNotNull(c1Before);
        assertNotNull(c2Before);

        // --- Act ---
        ReplayResult result = replayService.replayByTimeWindow(req);

        // --- Assert contract ---
        assertNotNull(result);
        assertTrue(result.ok());
        assertEquals(2, result.replayedCount());

        // --- capture createdAt AFTER ---
        Document p1After = col.find(new Document("snapshotHash", "SNAP-1")).first();
        Document p2After = col.find(new Document("snapshotHash", "SNAP-2")).first();

        assertNotNull(p1After);
        assertNotNull(p2After);

        Date c1After = p1After.getDate("createdAt");
        Date c2After = p2After.getDate("createdAt");

        assertNotNull(c1After);
        assertNotNull(c2After);

        // --- THE KEY ASSERTION: replay = touch createdAt forward ---
        assertTrue(c1After.getTime() >= c1Before.getTime());
        assertTrue(c2After.getTime() >= c2Before.getTime());
    }


    // ---------------- helpers ----------------



    private void seedReplayDocs() {
        var col = mongoClient
                .getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-12T00:00:01Z");
        Instant t2 = Instant.parse("2026-01-12T00:00:02Z");

        String payload1 = "{\"ingestionId\":\"ID-1\",\"view\":{\"snapshotHash\":\"SNAP-1\"},\"payload\":\"P1\"}";
        String payload2 = "{\"ingestionId\":\"ID-2\",\"view\":{\"snapshotHash\":\"SNAP-2\"},\"payload\":\"P2\"}";

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("snapshotHash", "SNAP-1")
                .append("payload", payload1)
                .append("createdAt", Date.from(t1)));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("snapshotHash", "SNAP-2")
                .append("payload", payload2)
                .append("createdAt", Date.from(t2)));
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



