package io.braineous.dd.replay;

import com.mongodb.client.MongoClient;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.MongoReplayStore;
import io.braineous.dd.replay.services.ReplayService;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@org.junit.jupiter.api.Disabled("PARKED: ingestionId determinism across retries requires DLQ-aware resolveIngestionId; enable after DLQ integration")
@QuarkusTest
public class ReplayServiceMongoParityIT {

    @Inject
    MongoClient mongoClient;

    @Inject
    MongoReplayStore store;

    @Inject
    ProcessorOrchestrator orch;

    @Inject
    ReplayService replayService;

    @BeforeEach
    void setup() {
        mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL)
                .drop();
    }

    @Test
    void parity_domain_vs_system_dlq_same_results_and_mongo_effects() {

        var col = mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL);

        Instant t1 = Instant.parse("2026-01-07T20:00:01Z");
        Instant t2 = Instant.parse("2026-01-07T20:00:02Z");
        Instant t3 = Instant.parse("2026-01-07T20:00:03Z");

        col.insertOne(doc("ID-0", payload(0), t1, "DLQ-A"));
        col.insertOne(doc("ID-1", payload(1), t2, "DLQ-A"));
        col.insertOne(doc("ID-2", payload(2), t3, "DLQ-A"));

        ReplayRequest req = new ReplayRequest();
        req.setDlqId("DLQ-A");
        req.setReason("ingestion");

        // -------- act --------
        ReplayResult a = replayService.replayByDomainDlqId(req);

        List<Document> afterDomainReplay = snapshotMongo();

        ReplayResult b = replayService.replayBySystemDlqId(req);

        List<Document> afterSystemReplay = snapshotMongo();

        // -------- assert: API parity --------
        Assertions.assertTrue(a.ok());
        Assertions.assertTrue(b.ok());
        Assertions.assertEquals(a.matchedCount(), b.matchedCount());
        Assertions.assertEquals(a.replayedCount(), b.replayedCount());
        Assertions.assertEquals(3, a.replayedCount());

        // -------- assert: Mongo parity --------
        Assertions.assertEquals(afterDomainReplay.size(), afterSystemReplay.size());

        List<Document> normA = normalize(afterDomainReplay);
        List<Document> normB = normalize(afterSystemReplay);

        Assertions.assertEquals(normA, normB);
    }

    // ---------------- helpers ----------------

    private List<Document> snapshotMongo() {
        return mongoClient.getDatabase(MongoReplayStore.DB)
                .getCollection(MongoReplayStore.INGESTION_COL)
                .find()
                .into(new java.util.ArrayList<>());
    }

    private static List<Document> normalize(List<Document> docs) {
        return docs.stream()
                .map(d -> {
                    Document c = new Document(d);
                    c.remove("_id");
                    c.remove("ingestionId"); // nondeterministic
                    return c;
                })
                .sorted((a, b) -> a.toJson().compareTo(b.toJson()))
                .collect(Collectors.toList());
    }

    private static Document doc(
            String id, String payload, Instant ts, String dlqId) {
        return new Document()
                .append("ingestionId", id)
                .append("payload", payload)
                .append("createdAt", Date.from(ts))
                .append("dlqId", dlqId);
    }

    private static String payload(int n) {
        String base64 = java.util.Base64.getEncoder()
                .encodeToString(("{\"n\":" + n + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8));

        return """
        {
          "kafka": {
            "topic": "requests",
            "partition": 1,
            "offset": %d,
            "timestamp": 1767114000123,
            "key": "fact-%d"
          },
          "payload": {
            "encoding": "base64",
            "value": "%s"
          }
        }
        """.formatted(100 + n, n, base64);
    }
}
