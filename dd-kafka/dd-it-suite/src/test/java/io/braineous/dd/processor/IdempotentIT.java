package io.braineous.dd.processor;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoClient;
import io.braineous.dd.core.model.DDEvent;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.braineous.dd.processor.client.DDIngestionHttpPoster;
import io.braineous.dd.core.processor.GsonJsonSerializer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class IdempotentIT {

    private static final String CONSUMER_BASE = System.getProperty("dd.consumer.baseUrl", "http://localhost:8082");
    private static final HttpClient http = HttpClient.newHttpClient();

    @Inject
    private ProcessorOrchestrator orch;

    @BeforeEach
    public void setUp() throws Exception {
        resetOrchestratorPoster();

        // 1) Clear capture store (best-effort; ignore if endpoint not up yet)
        try {
            postNoBody(CONSUMER_BASE + "/debug/captures/clear");
        } catch (Exception e) {
            // swallow - tests that rely on captures will fail with clear signal anyway
        }

        // 2) Drop Mongo ingestion collection (hard reset)
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");
        try {
            mongo.getDatabase("dd").getCollection("ingestion").drop();
        } finally {
            mongo.close();
        }
    }

    // DO NOT call setUp() here

    // optional best-effort (not correctness-critical)




    @Test
    void orchestrate_should_hit_producer_and_consumer_should_receive(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson =
                JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // real poster
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        assertNotNull(result);
        assertTrue(result.isOk(), "Orchestrator failed: " +
                (result.getWhy() != null ? result.getWhy() : "null"));

        // ---------- Anchor (producer-side) ----------
        String ingestionId = result.getIngestionId();
        assertNotNull(ingestionId);
        assertTrue(ingestionId.trim().length() > 0);

        // poll for consumer consume (transport confirmation only)
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }
        assertTrue(size > 0, "Expected consumer to consume at least 1 message");

        // ---------- Anchor (store-side via MongoDB) ----------
        // Cleanest: query ingestion collection directly by ingestionId

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd")
                        .getCollection("ingestion");

        org.bson.Document doc = col.find(
                new org.bson.Document("ingestionId", ingestionId)
        ).first();

        mongo.close();

        assertNotNull(doc, "Expected Mongo ingestion record for ingestionId=" + ingestionId);

        // doc schema: kafka/payload envelopes live INSIDE the stored 'payload' string, not as top-level fields
        String storedPayloadJson = doc.getString("payload");
        Console.log("it_anchor_mongo_payload", storedPayloadJson);

        assertNotNull(storedPayloadJson);
        assertTrue(storedPayloadJson.trim().length() > 0);

        assertTrue(storedPayloadJson.contains("\"ingestionId\""));
        assertTrue(storedPayloadJson.contains(ingestionId));

        // minimal envelope markers (anchor spine only)
        assertTrue(storedPayloadJson.contains("\"kafka\""));
        assertTrue(storedPayloadJson.contains("\"payload\""));

    }



    @org.junit.jupiter.api.Disabled("PARKED: deferred for flaky env related cleanup issues. Anchor contract not affected")
    @Test
    void orchestrate_missingKafka_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // sabotage: remove kafka (missing root.kafka)
        ddEventJson.remove("kafka");

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-missing_kafka", result.getWhy().getReason());

        // anchor must NOT exist on fail-fast
        assertNull(result.getIngestionId(), "Expected ingestionId to be null on ok=false fail-fast");

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // assert no store side-effect (Mongo must NOT have a record for this itTest)
        // We query by the itTest header value embedded in stored payload JSON.
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        long count = col.countDocuments(
                new org.bson.Document("payload",
                        new org.bson.Document("$regex", "\"itTest\":\"" + ti.getDisplayName() + "\""))
        );

        mongo.close();

        assertEquals(0L, count, "Expected no Mongo ingestion record for fail-fast request (itTest=" + ti.getDisplayName() + ")");
    }

    @Test
    void orchestrate_headersWrongType_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // sabotage: headers must be object, make it an array
        ddEventJson.getAsJsonObject("kafka")
                .add("headers", JsonParser.parseString("[\"bad\"]"));

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals(
                "DD-ORCH-VALIDATE-kafka_headers_type",
                result.getWhy().getReason()
        );

        // anchor must NOT exist on fail-fast
        assertNull(result.getIngestionId(), "Expected ingestionId to be null on ok=false fail-fast");

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // assert no store side-effect (Mongo must NOT have a record for this itTest)
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        long count = col.countDocuments(
                new org.bson.Document("payload",
                        new org.bson.Document("$regex", "\"itTest\":\"" + ti.getDisplayName() + "\""))
        );

        mongo.close();

        assertEquals(0L, count, "Expected no Mongo ingestion record for fail-fast request (itTest=" + ti.getDisplayName() + ")");
    }

    @Test
    void orchestrate_withExtraFields_shouldStillProduce_and_consume(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson =
                JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // extras ignored by design (forward-compatible at input, not guaranteed to persist)
        ddEventJson.addProperty("extraRootField", "root-ok");

        JsonObject kafka = ddEventJson.getAsJsonObject("kafka");
        kafka.addProperty("extraKafkaField", "kafka-ok");

        JsonObject payload = ddEventJson.getAsJsonObject("payload");
        payload.addProperty("extraPayloadField", "payload-ok");

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert ok
        assertNotNull(result);
        assertTrue(
                result.isOk(),
                "Orchestrator failed: " + (result.getWhy() != null ? result.getWhy() : "null")
        );

        // anchor must exist on ok=true
        String ingestionId = result.getIngestionId();
        assertNotNull(ingestionId);
        assertTrue(ingestionId.trim().length() > 0);

        // poll for consumer consume
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }
        assertTrue(size > 0, "Expected consumer to consume at least 1 message");

        // ---------- Anchor assertion (store-side via MongoDB) ----------
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc =
                col.find(new org.bson.Document("ingestionId", ingestionId)).first();

        mongo.close();

        assertNotNull(
                doc,
                "Expected Mongo ingestion record for ingestionId=" + ingestionId
        );

        String storedPayloadJson = doc.getString("payload");
        Console.log("it_anchor_mongo_payload", storedPayloadJson);

        assertNotNull(storedPayloadJson);
        assertTrue(storedPayloadJson.trim().length() > 0);

        // parse and assert canonical anchor envelope only
        com.google.gson.JsonObject storedObj =
                com.google.gson.JsonParser.parseString(storedPayloadJson).getAsJsonObject();

        assertTrue(storedObj.has("kafka"));
        assertTrue(storedObj.has("payload"));
        assertTrue(storedObj.has("ingestionId"));

        assertEquals(
                ingestionId,
                storedObj.get("ingestionId").getAsString()
        );

        assertEquals(
                "requests",
                storedObj.getAsJsonObject("kafka").get("topic").getAsString()
        );
    }

    @Test
    void orchestrate_blankTopic_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // sabotage: blank topic
        ddEventJson.getAsJsonObject("kafka").addProperty("topic", "  ");

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-kafka_topic", result.getWhy().getReason());

        // anchor must NOT exist on fail-fast
        assertNull(result.getIngestionId(), "Expected ingestionId to be null on ok=false fail-fast");

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // assert no store side-effect (Mongo must NOT have a record for this itTest)
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        long count = col.countDocuments(
                new org.bson.Document(
                        "payload",
                        new org.bson.Document("$regex", "\"itTest\":\"" + ti.getDisplayName() + "\"")
                )
        );

        mongo.close();

        assertEquals(
                0L,
                count,
                "Expected no Mongo ingestion record for fail-fast request (itTest=" + ti.getDisplayName() + ")"
        );
    }


    @Test
    void orchestrate_zeroTimestamp_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // sabotage: timestamp must be > 0
        ddEventJson.getAsJsonObject("kafka").addProperty("timestamp", 0L);

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-kafka_timestamp", result.getWhy().getReason());

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
    }



    @Test
    void orchestrate_negativeOffset_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // sabotage: offset must be >= 0
        ddEventJson.getAsJsonObject("kafka").addProperty("offset", -1L);

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-kafka_offset", result.getWhy().getReason());

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
    }





    @Test
    void orchestrate_transportThrows_shouldFail_and_notProduce(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // --- FORCE RESET poster (external, test-only) ---
        Field f = ProcessorOrchestrator.class.getDeclaredField("httpPoster");
        f.setAccessible(true);
        f.set(orch, null);

        // transport failure poster
        HttpPoster throwingPoster = (url, jsonBody) -> {
            Console.log("TRANSPORT", "HttpPoster invoked, throwing");
            throw new RuntimeException("IT_TRANSPORT_DOWN");
        };

        // orchestrate
        orch.setHttpPoster(throwingPoster);

        ProcessorResult result = orch.orchestrate(ddEventJson);

        // Console.log
        Console.log("RESULT", result);
        Console.log("WHY", result != null ? result.getWhy() : null);

        // assert failure
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());

        // lock exact reason (now observed)
        assertEquals("DD-REST-call_failed", result.getWhy().getReason());

        // ingestionId should still exist (anchor assignment happens before publish attempt)
        String ingestionId = result.getIngestionId();
        assertNotNull(ingestionId);
        assertTrue(ingestionId.trim().length() > 0);

        // assert no consume side-effect (message never produced)
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // IMPORTANT (new spine): persistence is EP-only after Kafka.
        // Since transport failed, EP never ran => Mongo ingestion doc must NOT exist.
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc =
                col.find(new org.bson.Document("ingestionId", ingestionId)).first();

        mongo.close();

        Console.log("it_anchor_mongo_doc", doc);

        assertNull(doc, "Expected NO Mongo ingestion record for ingestionId=" + ingestionId
                + " because transport failed before Kafka; EP persistence did not run");
    }



    @Test
    void orchestrate_replaySameEventLater_shouldConsumeAgain_forNow(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate first time
        orch.setHttpPoster(httpPoster);

        ProcessorResult r1 = orch.orchestrate(ddEventJson);
        Console.log("REPLAY_SEND_1", r1);
        assertNotNull(r1);
        assertTrue(r1.isOk(), "First send failed: " + (r1.getWhy() != null ? r1.getWhy() : "null"));

        // anchor invariant
        assertNotNull(r1.getIngestionId());
        assertTrue(r1.getIngestionId().trim().length() > 0);

        // wait until at least 1 consumed
        awaitSizeAtLeast(1);
        Console.log("AFTER_FIRST_CONSUME_SIZE", getInt(CONSUMER_BASE + "/debug/captures/size"));

        // simulate "later / restart / state reset" on consumer side
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");
        Console.log("CONSUMER_CLEARED", "ok");

        Thread.sleep(250); // small gap to make it feel like replay, not immediate dup

        // replay: same event again
        ProcessorResult r2 = orch.orchestrate(ddEventJson);
        Console.log("REPLAY_SEND_2", r2);
        assertNotNull(r2);
        assertTrue(r2.isOk(), "Replay send failed: " + (r2.getWhy() != null ? r2.getWhy() : "null"));

        // anchor invariant: replay of same event should mint same ingestionId
        assertNotNull(r2.getIngestionId());
        assertEquals(r1.getIngestionId(), r2.getIngestionId(), "Replay must keep same ingestionId for same event");

        // should consume again (size returns to 1 after clear)
        awaitSizeAtLeast(1);
        int size = getInt(CONSUMER_BASE + "/debug/captures/size");
        Console.log("AFTER_REPLAY_CONSUME_SIZE", size);

        assertEquals(1, size, "Expected replay to be consumed after consumer store reset");

        // store assertion: anchor doc exists
        String ingestionId = r1.getIngestionId();

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc =
                col.find(new org.bson.Document("ingestionId", ingestionId)).first();

        mongo.close();

        assertNotNull(doc, "Expected Mongo ingestion record for ingestionId=" + ingestionId);
    }


    //end-to-end processorOrch -> producer ->consumer -> cgo



    //@org.junit.jupiter.api.Disabled("PARKED: ingestionId determinism across retries requires DLQ-aware resolveIngestionId; enable after DLQ integration")
    @Test
    void idempotency_sameEventTwice_shouldNotChangeGraphSnapshot(TestInfo ti) throws Exception {

        // clear consumer capture store (ok even if graph not cleared; we compare after first vs after dup)
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();

        // IMPORTANT: build fresh JSON for each send (orch mutates input by adding ingestionId + view)
        JsonObject ddEventJson1 = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // real poster
        HttpPoster httpPoster = new DDIngestionHttpPoster();
        orch.setHttpPoster(httpPoster);

        // ---- send #1 ----
        ProcessorResult r1 = orch.orchestrate(ddEventJson1);
        Console.log("IDEMPOTENCY_SEND_1", r1);
        assertNotNull(r1);
        assertTrue(r1.isOk(), "First send failed: " + (r1.getWhy() != null ? r1.getWhy() : "null"));

        String id1 = r1.getIngestionId();
        assertNotNull(id1);
        assertTrue(id1.trim().length() > 0);

        // HARDEN: wait until EP has persisted ingestion doc (so resolveIngestionId can find it)
        awaitMongoIngestionDoc(id1);

        // wait for graph to appear and capture hash1
        awaitGraphNonEmpty();
        String hash1 = fetchGraphCanonicalSha256();
        Console.log("IDEMPOTENCY_GRAPH_HASH_1", hash1);

        // ---- send #2 (true retry: fresh JSON) ----
        JsonObject ddEventJson2 = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        ProcessorResult r2 = orch.orchestrate(ddEventJson2);
        Console.log("IDEMPOTENCY_SEND_2", r2);
        assertNotNull(r2);
        assertTrue(r2.isOk(), "Second send failed: " + (r2.getWhy() != null ? r2.getWhy() : "null"));

        String id2 = r2.getIngestionId();
        assertNotNull(id2);
        assertTrue(id2.trim().length() > 0);

        // Now determinism should hold: same snapshotHash => same ingestionId
        assertEquals(id1, id2, "Anchor ingestionId must be stable for same event");

        // allow consumer to consume twice (transport may still deliver dup)
        awaitSizeAtLeast(2);

        // fetch hash2 and assert unchanged
        awaitGraphNonEmpty();
        String hash2 = fetchGraphCanonicalSha256();
        Console.log("IDEMPOTENCY_GRAPH_HASH_2", hash2);

        assertEquals(hash1, hash2, "Duplicate event must not change CGO graph snapshot (business idempotency)");
    }


    //---------------------------------------------------------------------------------------
    private void awaitMongoIngestionDoc(String ingestionId) {
        long deadline = System.currentTimeMillis() + 5000L;

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");
        try {
            com.mongodb.client.MongoCollection<org.bson.Document> col =
                    mongo.getDatabase("dd").getCollection("ingestion");

            while (System.currentTimeMillis() < deadline) {
                org.bson.Document doc =
                        col.find(new org.bson.Document("ingestionId", ingestionId)).first();
                if (doc != null) {
                    Console.log("IDEMPOTENCY_MONGO_DOC_FOUND", ingestionId);
                    return;
                }
                try { Thread.sleep(50L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
            }

            org.junit.jupiter.api.Assertions.fail("timeout waiting for mongo ingestion doc for ingestionId=" + ingestionId);

        } finally {
            mongo.close();
        }
    }

    private void awaitSizeAtLeast(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size >= expected) return;
            Thread.sleep(100);
        }
        Console.log("AWAIT_TIMEOUT_SIZE", size);
        fail("Timed out waiting for consumer size >= " + expected + " (actual=" + size + ")");
    }

    private static void postNoBody(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new AssertionError("POST " + url + " failed: " + resp.statusCode() + " body=" + resp.body());
        }
    }

    private static int getInt(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AssertionError("GET " + url + " failed: " + resp.statusCode() + " body=" + resp.body());
        }
        return Integer.parseInt(resp.body().trim());
    }

    private void resetOrchestratorPoster() throws Exception {
        Field f = ProcessorOrchestrator.class.getDeclaredField("httpPoster");
        f.setAccessible(true);
        f.set(orch, null);
    }

    private String fetchGraphCanonicalSha256() throws Exception {
        String raw = getString(CONSUMER_BASE + "/debug/graph");
        JsonObject obj = JsonParser.parseString(raw).getAsJsonObject();

        // If you later add a server-side snapshotHash, just return it here:
        // if (obj.has("snapshotHash")) return obj.get("snapshotHash").getAsString();

        String canonical = canonicalizeJson(obj);
        return sha256Hex(canonical);
    }


    private static String canonicalizeJson(com.google.gson.JsonElement el) {
        if (el == null || el.isJsonNull()) return "null";

        if (el.isJsonPrimitive()) return el.toString();

        if (el.isJsonArray()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            boolean first = true;
            for (com.google.gson.JsonElement child : el.getAsJsonArray()) {
                if (!first) sb.append(",");
                first = false;
                sb.append(canonicalizeJson(child));
            }
            sb.append("]");
            return sb.toString();
        }

        // object: sort keys for deterministic order
        com.google.gson.JsonObject obj = el.getAsJsonObject();
        java.util.List<String> keys = new java.util.ArrayList<>(obj.keySet());
        java.util.Collections.sort(keys);

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (String k : keys) {
            if (!first) sb.append(",");
            first = false;
            sb.append(com.google.gson.JsonParser.parseString("\"" + k.replace("\"", "\\\"") + "\"").toString());
            sb.append(":");
            sb.append(canonicalizeJson(obj.get(k)));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String sha256Hex(String s) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : bytes) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getString(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AssertionError("GET " + url + " failed: " + resp.statusCode() + " body=" + resp.body());
        }
        return resp.body();
    }

    private void awaitGraphNonEmpty() throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            String raw = getString(CONSUMER_BASE + "/debug/graph");
            if (raw != null && !raw.contains("\"status\":\"EMPTY\"")) return;
            Thread.sleep(100);
        }
        fail("Timed out waiting for /debug/graph to become non-empty");
    }


}
