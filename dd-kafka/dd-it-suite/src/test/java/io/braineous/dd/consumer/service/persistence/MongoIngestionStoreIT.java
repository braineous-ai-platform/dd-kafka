package io.braineous.dd.consumer.service.persistence;

import ai.braineous.rag.prompt.cgo.api.Edge;
import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.cgo.DDCGOOrchestrator;
import io.braineous.dd.consumer.service.DDEventOrchestrator;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MongoIngestionStoreIT {


    @Inject
    MongoIngestionStore store;

    @Inject
    private DDCGOOrchestrator cgoOrch;

    @Inject
    private DDEventOrchestrator orch;

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
        JsonObject ddEventJson = JsonParser.parseString(payload).getAsJsonObject();


        // Act
        JsonArray events = new JsonArray();
        events.add(ddEventJson);

        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);
        Map<String, String> map = addCGOata(events, snapshot);
        Console.log("dd_events", events.toString());

        IngestionReceipt r = store.storeIngestion(events.get(0).toString());

        // Assert receipt
        assertNotNull(r);
        assertTrue(r.ok());
        assertNull(r.why());
        assertNotNull(r.ingestionId());
        assertNotNull(r.payloadHash());
        assertNotNull(r.snapshotHash());
        assertNotNull(r.snapshotHash().getValue());
        assertEquals("mongo", r.storeType());

        // Assert persisted doc exists (by hashes)
        var col = mongoClient.getDatabase("dd").getCollection("ingestion");

        String snap = map.get("snap");
        String ingestionId = map.get("ingestionId");

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

        assertEquals(events.get(0).toString(), found.getString("payload"));


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
                        store.storeIngestion(payload);
                    }
                }
        );

        org.junit.jupiter.api.Assertions.assertEquals("ingestionId_required", ex.getMessage());
    }

    //-----------------------------------------------
    private Map<String, String> addCGOata(JsonArray ddEvents, GraphSnapshot snapshot) {

        if (ddEvents == null || snapshot == null) {
            return null;
        }

        String snap = null;
        if (snapshot.snapshotHash() != null) {
            snap = snapshot.snapshotHash().getValue();
        }

        if (snap == null || snap.trim().length() == 0) {
            return null;
        }

        // Cache axis birth per snapshot for this batch so duplicates don't get different ingestionIds
        String cachedIngestionId = null;

        String ingestionId = null;

        for (int i = 0; i < ddEvents.size(); i++) {

            JsonElement el = ddEvents.get(i);
            if (el == null || !el.isJsonObject()) {
                continue;
            }

            JsonObject obj = el.getAsJsonObject();

            // -----------------------------
            // Wrap arbitrary payload into DD envelope if needed
            // -----------------------------
            JsonObject ddEvent = obj;

            boolean hasKafka = ddEvent.has("kafka");
            boolean hasPayload = ddEvent.has("payload");

            if (!hasKafka || !hasPayload) {

                JsonObject env = new JsonObject();

                // deterministic-ish kafka stub (enough for anchoring + ingestionId axis)
                JsonObject kafka = new JsonObject();
                kafka.addProperty("topic", "it");        // stable
                kafka.addProperty("partition", 0);
                kafka.addProperty("offset", 1L);
                kafka.addProperty("timestamp", 1700000000000L);
                kafka.addProperty("key", "it");

                env.add("kafka", kafka);

                // embed original object as payload.value
                JsonObject payload = new JsonObject();
                payload.addProperty("encoding", "json");
                payload.addProperty("value", ddEvent.toString());

                env.add("payload", payload);

                // swap in the wrapped event
                ddEvents.set(i, env);
                ddEvent = env;
            }

            // Resolve ingestionId once per snapshot for this batch (stable across duplicates)
            if (cachedIngestionId == null) {
                cachedIngestionId = this.orch.getStore().resolveIngestionId(ddEvent.toString(), snap);
            }

            ddEvent.addProperty("ingestionId", cachedIngestionId);
            ingestionId = cachedIngestionId;

            JsonObject snapJson = snapshot.toJson();
            snapJson.addProperty(MongoIngestionStore.F_SNAPSHOT_HASH, snap); // "snapshotHash"

            ddEvent.add("view", snapJson);
        }

        Map<String, String> map = new HashMap<>();
        map.put("ingestionId", ingestionId);
        map.put("snap", snap);

        return map;
    }

}

