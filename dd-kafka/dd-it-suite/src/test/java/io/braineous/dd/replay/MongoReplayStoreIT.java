package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import io.braineous.dd.consumer.service.persistence.MongoIngestionStore;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
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

    @org.junit.jupiter.api.BeforeEach
    void reset() {
        mongoClient.getDatabase("dd").getCollection(MongoIngestionStore.COL).drop();
    }

    @Test
    void findByTimeWindow_returnsEvents_inCreatedAtOrder_andWithinWindow() {
        var col = mongoClient.getDatabase("dd").getCollection(MongoIngestionStore.COL);

        // Arrange: 3 docs (only 2 inside window)
        Instant t0 = Instant.parse("2026-01-07T00:00:00Z");
        Instant t1 = Instant.parse("2026-01-07T00:10:00Z"); // inside
        Instant t2 = Instant.parse("2026-01-07T00:20:00Z"); // inside
        Instant t3 = Instant.parse("2026-01-07T00:30:00Z");

        col.insertOne(new Document()
                .append("ingestionId", "ID-0")
                .append("payload", "P0")
                .append("createdAt", Date.from(t0)));

        col.insertOne(new Document()
                .append("ingestionId", "ID-1")
                .append("payload", "P1")
                .append("createdAt", Date.from(t1)));

        col.insertOne(new Document()
                .append("ingestionId", "ID-2")
                .append("payload", "P2")
                .append("createdAt", Date.from(t2)));

        // window: [t1, t3)
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
        assertEquals("P1", events.get(0).payload());
        assertEquals(t1, events.get(0).timestamp());

        assertEquals("ID-2", events.get(1).id());
        assertEquals("P2", events.get(1).payload());
        assertEquals(t2, events.get(1).timestamp());
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

