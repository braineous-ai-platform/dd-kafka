package io.braineous.dd.consumer.service.persistence;

import ai.braineous.rag.prompt.cgo.api.Edge;
import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MongoIngestionStoreIT {

    @Inject
    MongoIngestionStore store;

    @Inject
    MongoClient mongoClient;

    @org.junit.jupiter.api.BeforeEach
    void start() {
        var db = mongoClient.getDatabase(MongoIngestionStore.DB);
        db.getCollection(MongoIngestionStore.COL).drop();
    }


    @org.junit.jupiter.api.AfterEach
    void cleanup() {
        this.start();
    }

    @Test
    void storeIngestion_ok_persistsDoc() {
        // Arrange
        String payload = "{\"hello\":\"world\"}";

        java.util.Map<String, Fact> nodes = new java.util.HashMap<>();
        java.util.Map<String, Edge> edges = new java.util.HashMap<>();

        Fact f1 = new Fact("F-1", "atomic"); // adapt to your Fact ctor
        nodes.put(f1.getId(), f1);

        GraphSnapshot view = new GraphSnapshot(nodes, edges);

        // Act
        IngestionReceipt r = store.storeIngestion(payload, view);

        // Assert receipt
        assertNotNull(r);
        assertTrue(r.ok());
        assertNull(r.why());
        assertNotNull(r.ingestionId());
        assertNotNull(r.payloadHash());
        assertNotNull(r.snapshotHash());
        assertNotNull(r.snapshotHash().getValue());
        assertEquals(nodes.size(), r.nodeCount());
        assertEquals(edges.size(), r.edgeCount());
        assertEquals("mongo", r.storeType());

        // Assert persisted doc exists (by hashes)
        var col = mongoClient.getDatabase("dd").getCollection("ingestion");

        String snap = view.snapshotHash().getValue();

        Document found = col.find(new Document()
                        .append("snapshotHash", snap)
                        .append("payloadHash", IngestionReceipt.sha256Hex(payload)))
                .first();

        assertNotNull(found);
        assertEquals(payload, found.getString("payload"));
        assertEquals(nodes.size(), found.getInteger("nodeCount"));
        assertEquals(edges.size(), found.getInteger("edgeCount"));
        assertNotNull(found.getDate("createdAt"));
        assertNotNull(found.getString("ingestionId"));
    }

    @Test
    void storeIngestion_payloadBlank_returnsFailReceipt_andNoInsert() {
        // Arrange
        String payload = "   ";

        java.util.Map<String, Fact> nodes = new java.util.HashMap<>();
        java.util.Map<String, Edge> edges = new java.util.HashMap<>();
        nodes.put("F-1", new Fact("F-1", "atomic"));

        GraphSnapshot view = new GraphSnapshot(nodes, edges);

        // Act
        IngestionReceipt r = store.storeIngestion(payload, view);

        // Assert receipt (DLQ-domain style)
        assertNotNull(r);
        assertFalse(r.ok());
        assertNotNull(r.why());
        assertEquals("DD-ING-payload_blank", r.why().getReason());   // adjust accessor if different
        assertNull(r.payloadHash());                              // fail-fast before hashing
        assertNull(r.snapshotHash());                             // we intentionally don’t compute on blank payload
        assertEquals("mongo", r.storeType());

        // Assert NO insert happened
        var col = mongoClient.getDatabase(MongoIngestionStore.DB).getCollection(MongoIngestionStore.COL);
        assertEquals(0L, col.countDocuments());
    }
}

