package io.braineous.dd.consumer.service.persistence;

import ai.braineous.rag.prompt.cgo.api.Edge;
import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MongoIngestionStoreIT {

    /*
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
        String ingestionId = ProcessorOrchestrator.nextIngestionId();
        JsonObject ddEventJson = JsonParser.parseString(payload).getAsJsonObject();
        ddEventJson.addProperty("ingestionId", ingestionId);

        java.util.Map<String, Fact> nodes = new java.util.HashMap<>();
        java.util.Map<String, Edge> edges = new java.util.HashMap<>();

        Fact f1 = new Fact("F-1", "atomic"); // adapt to your Fact ctor
        nodes.put(f1.getId(), f1);

        GraphSnapshot view = new GraphSnapshot(nodes, edges);

        // Act

        IngestionReceipt r = store.storeIngestion(ddEventJson.toString(), view);

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

        // --- debug context ---
        Console.log("it.mongo.query.ingestionId", ingestionId);
        Console.log("it.mongo.query.snapshotHash", snap);

        // --- canonical query: ingestionId is the public axis ---
        Document query = new Document("ingestionId", ingestionId);

        Console.log("it.mongo.query.document", query.toJson());

        Document found = col.find(query).first();

        Console.log("it.mongo.query.result", found == null ? null : found.toJson());

        // --- assertions ---
        assertNotNull(found);

        assertEquals(
                ddEventJson.toString(),
                found.getString("payload")
        );

        assertEquals(nodes.size(), found.getInteger("nodeCount"));
        assertEquals(edges.size(), found.getInteger("edgeCount"));

        assertNotNull(found.getDate("createdAt"));
        assertEquals(ingestionId, found.getString("ingestionId"));

    }

    @Test
    void storeIngestion_payloadBlank_throwsIllegalArgumentException_ingestionIdRequired() {

        String payload = "   ";

        java.lang.IllegalArgumentException ex = org.junit.jupiter.api.Assertions.assertThrows(
                java.lang.IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        store.storeIngestion(payload, null);
                    }
                }
        );

        org.junit.jupiter.api.Assertions.assertEquals("ingestionId_required", ex.getMessage());
    }*/

}

