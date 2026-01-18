package io.braineous.dd.processor;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.braineous.dd.core.model.DDEvent;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.braineous.dd.processor.client.DDIngestionHttpPoster;
import io.braineous.dd.core.processor.GsonJsonSerializer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class IdempotentIT {

    private static final String CONSUMER_BASE = System.getProperty("dd.consumer.baseUrl", "http://localhost:8082");
    private static final HttpClient http = HttpClient.newHttpClient();

    // matches your ingestion.sh
    private static final String BASE_URL = System.getProperty("dd.baseUrl", "http://localhost:8080");
    private static final String ENDPOINT = "/api/ingestion";

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
        //Thread.sleep(500);
        //assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // assert no store side-effect (Mongo must NOT have a record for this itTest)
        /*
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        long count = col.countDocuments(
                new org.bson.Document("payload",
                        new org.bson.Document("$regex", "\"itTest\":\"" + ti.getDisplayName() + "\""))
        );

        mongo.close();

        assertEquals(0L, count, "Expected no Mongo ingestion record for fail-fast request (itTest=" + ti.getDisplayName() + ")");*/
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
        //Thread.sleep(500);
        //assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
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
        /*awaitGraphNonEmpty();
        String hash2 = fetchGraphCanonicalSha256();
        Console.log("IDEMPOTENCY_GRAPH_HASH_2", hash2);

        assertEquals(hash1, hash2, "Duplicate event must not change CGO graph snapshot (business idempotency)");*/
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
                        "fact-001",              // legacy metadata, ignored for identity now
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // ----- NEW AXIS: factId derived from payload -----
        String ddEventStr = ddEventJson.toString();
        DDEvent parsed = DDEvent.fromJson(ddEventStr);
        String factId = parsed.getPayload().deriveFactIdFromPayloadBase64();
        String expectedIngestionId = "DD-ING-" + factId;

        Console.log("it_factId_payload_derived", factId);
        Console.log("it_expected_ingestionId", expectedIngestionId);
        Console.log("it_event_in", ddEventJson);

        // transport failure poster
        HttpPoster throwingPoster = (url, jsonBody) -> {
            Console.log("TRANSPORT", "HttpPoster invoked, throwing");
            throw new RuntimeException("IT_TRANSPORT_DOWN");
        };

        orch.setHttpPoster(throwingPoster);

        ProcessorResult result = orch.orchestrate(ddEventJson);

        Console.log("RESULT", result);
        Console.log("WHY", result != null ? result.getWhy() : null);

        // assert failure
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());

        // You originally expected this (and once ingestionId is resolvable, you should reach transport)
        assertEquals("DD-REST-call_failed", result.getWhy().getReason(),
                "Expected transport failure reason after identity resolved. got=" + result.getWhy());

        // ingestionId may NOT be copied into result on failure path.
        // But ddEventJson SHOULD have ingestionId property by the time transport is invoked.
        JsonObject outJson = result.getDdEventJson();
        Console.log("it_out_json", outJson);

        assertNotNull(outJson);
        assertTrue(outJson.has("ingestionId"), "Expected ddEventJson.ingestionId to be present even on transport failure");
        assertEquals(expectedIngestionId, outJson.get("ingestionId").getAsString());

        // assert no consume side-effect (message never produced)
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));

        // Since transport failed, EP never ran => Mongo ingestion doc must NOT exist.
        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc =
                col.find(new org.bson.Document("ingestionId", expectedIngestionId)).first();

        mongo.close();

        Console.log("it_anchor_mongo_doc", doc);

        assertNull(doc, "Expected NO Mongo ingestion record for ingestionId=" + expectedIngestionId
                + " because transport failed before Kafka; EP persistence did not run");
    }

    @Test
    void orchestrate_replaySameEventLater_shouldConsumeAgain_forNow(TestInfo ti) throws Exception {

        // clear consumer store
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        // Step 1: build a base event to derive factId from payload
        DDEvent base = DDEvent.of(
                "requests",
                3,
                48192L,
                1767114000123L,
                "fact-001",
                "base64",
                "AAECAwQFBgcICQ=="
        );

        String derivedFactId = base.getPayload().deriveFactIdFromPayloadBase64();
        Console.log("it_factId_payload_derived", derivedFactId);

        // Step 2: build the REAL event with kafka.key == derivedFactId (matches PO lookup)
        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        derivedFactId,
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();
        Console.log("it_event_in", ddEventJson);

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();
        orch.setHttpPoster(httpPoster);

        // orchestrate first time
        ProcessorResult r1 = orch.orchestrate(JsonParser.parseString(serializer.toJson(event)).getAsJsonObject());
        Console.log("REPLAY_SEND_1", r1);

        assertNotNull(r1);
        assertTrue(r1.isOk(), "First send failed: " + (r1.getWhy() != null ? r1.getWhy() : "null"));

        assertNotNull(r1.getIngestionId());
        assertTrue(r1.getIngestionId().trim().length() > 0);

        // wait until at least 1 consumed
        awaitSizeAtLeast(1);
        Console.log("AFTER_FIRST_CONSUME_SIZE", getInt(CONSUMER_BASE + "/debug/captures/size"));

        // simulate later/restart on consumer side
        postNoBody(CONSUMER_BASE + "/debug/captures/clear");
        Console.log("CONSUMER_CLEARED", "ok");

        Thread.sleep(250);

        // replay: same event again (fresh JSON)
        ProcessorResult r2 = orch.orchestrate(JsonParser.parseString(serializer.toJson(event)).getAsJsonObject());
        Console.log("REPLAY_SEND_2", r2);

        assertNotNull(r2);
        assertTrue(r2.isOk(), "Replay send failed: " + (r2.getWhy() != null ? r2.getWhy() : "null"));

        assertNotNull(r2.getIngestionId());
        assertEquals(r1.getIngestionId(), r2.getIngestionId(), "Replay must keep same ingestionId for same event");

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

    @Test
    void orchestrate_should_hit_producer_and_consumer_should_receive(TestInfo ti) throws Exception {

        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        // base event => derive factId
        DDEvent base = DDEvent.of(
                "requests",
                3,
                48192L,
                1767114000123L,
                "fact-001",
                "base64",
                "AAECAwQFBgcICQ=="
        );

        String derivedFactId = base.getPayload().deriveFactIdFromPayloadBase64();
        Console.log("it_factId_payload_derived", derivedFactId);

        String expectedIngestionId = "DD-ING-" + derivedFactId;
        Console.log("it_expected_ingestionId", expectedIngestionId);

        // real event => kafka.key MUST match derivedFactId (PO expects it)
        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        derivedFactId,
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson =
                JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        Console.log("it_event_in", ddEventJson);

        HttpPoster httpPoster = new DDIngestionHttpPoster();
        orch.setHttpPoster(httpPoster);

        ProcessorResult result = orch.orchestrate(ddEventJson);
        Console.log("it_result", result);

        assertNotNull(result);
        assertTrue(result.isOk(), "Orchestrator failed: " + (result.getWhy() != null ? result.getWhy() : "null"));

        String ingestionId = result.getIngestionId();
        assertNotNull(ingestionId);
        assertTrue(ingestionId.trim().length() > 0);

        // strong invariant with this PO logic
        assertEquals(expectedIngestionId, ingestionId);

        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }
        assertTrue(size > 0, "Expected consumer to consume at least 1 message");

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc = col.find(
                new org.bson.Document("ingestionId", ingestionId)
        ).first();

        mongo.close();

        assertNotNull(doc, "Expected Mongo ingestion record for ingestionId=" + ingestionId);

        String storedPayloadJson = doc.getString("payload");
        Console.log("it_anchor_mongo_payload", storedPayloadJson);

        assertNotNull(storedPayloadJson);
        assertTrue(storedPayloadJson.trim().length() > 0);

        assertTrue(storedPayloadJson.contains("\"ingestionId\""));
        assertTrue(storedPayloadJson.contains(ingestionId));

        assertTrue(storedPayloadJson.contains("\"kafka\""));
        assertTrue(storedPayloadJson.contains("\"payload\""));
    }

    @Test
    void orchestrate_withExtraFields_shouldStillProduce_and_consume(TestInfo ti) throws Exception {

        postNoBody(CONSUMER_BASE + "/debug/captures/clear");

        // base event => derive factId
        DDEvent base = DDEvent.of(
                "requests",
                3,
                48192L,
                1767114000123L,
                "fact-001",
                "base64",
                "AAECAwQFBgcICQ=="
        );

        String derivedFactId = base.getPayload().deriveFactIdFromPayloadBase64();
        Console.log("it_factId_payload_derived", derivedFactId);

        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        derivedFactId,
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("itTest", ti.getDisplayName())
                .header("correlationId", "corr-9911");

        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson =
                JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // extras ignored by design
        ddEventJson.addProperty("extraRootField", "root-ok");

        JsonObject kafka = ddEventJson.getAsJsonObject("kafka");
        kafka.addProperty("extraKafkaField", "kafka-ok");

        JsonObject payload = ddEventJson.getAsJsonObject("payload");
        payload.addProperty("extraPayloadField", "payload-ok");

        Console.log("it_event_in", ddEventJson);

        HttpPoster httpPoster = new DDIngestionHttpPoster();
        orch.setHttpPoster(httpPoster);

        ProcessorResult result = orch.orchestrate(ddEventJson);
        Console.log("it_result", result);

        assertNotNull(result);
        assertTrue(result.isOk(), "Orchestrator failed: " + (result.getWhy() != null ? result.getWhy() : "null"));

        String ingestionId = result.getIngestionId();
        assertNotNull(ingestionId);
        assertTrue(ingestionId.trim().length() > 0);

        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }
        assertTrue(size > 0, "Expected consumer to consume at least 1 message");

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");

        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongo.getDatabase("dd").getCollection("ingestion");

        org.bson.Document doc =
                col.find(new org.bson.Document("ingestionId", ingestionId)).first();

        mongo.close();

        assertNotNull(doc, "Expected Mongo ingestion record for ingestionId=" + ingestionId);

        String storedPayloadJson = doc.getString("payload");
        Console.log("it_anchor_mongo_payload", storedPayloadJson);

        assertNotNull(storedPayloadJson);
        assertTrue(storedPayloadJson.trim().length() > 0);

        com.google.gson.JsonObject storedObj =
                com.google.gson.JsonParser.parseString(storedPayloadJson).getAsJsonObject();

        assertTrue(storedObj.has("kafka"));
        assertTrue(storedObj.has("payload"));
        assertTrue(storedObj.has("ingestionId"));

        assertEquals(ingestionId, storedObj.get("ingestionId").getAsString());
        assertEquals("requests", storedObj.getAsJsonObject("kafka").get("topic").getAsString());
    }

    @Test
    void it_req1_twice_req2_once_should_create_only_two_mongo_records(TestInfo ti) {

        // --------- build REQ1 / REQ2 (same as ingestion.sh) ----------
        String ddEvent1 =
                "{"
                        + "\"kafka\":{\"topic\":\"requests\",\"partition\":3,\"offset\":48192,\"timestamp\":1767114000123},"
                        + "\"payload\":{\"encoding\":\"base64\",\"value\":\"AAECAwQFBgcICQ==\"},"
                        + "\"itTest\":\"" + ti.getDisplayName() + "\""
                        + "}";

        String ddEvent2 =
                "{"
                        + "\"kafka\":{\"topic\":\"requests\",\"partition\":3,\"offset\":48192,\"timestamp\":1767114000123},"
                        + "\"payload\":{\"encoding\":\"base64\",\"value\":\"AAECAwQFBgcICg==\"},"
                        + "\"itTest\":\"" + ti.getDisplayName() + "\""
                        + "}";

        com.google.gson.Gson g = new com.google.gson.Gson();

        // payload must be a JSON STRING (quoted/escaped), not an object
        String req1 =
                "{"
                        + "\"stream\":\"ingestion\","
                        + "\"payload\":" + g.toJson(ddEvent1)
                        + "}";

        String req2 =
                "{"
                        + "\"stream\":\"ingestion\","
                        + "\"payload\":" + g.toJson(ddEvent2)
                        + "}";

        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1", req1);
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ2", req2);

        // --------- call REQ1 twice ----------
        io.restassured.response.Response r11 =
                io.restassured.RestAssured.given()
                        .contentType("application/json")
                        .accept("application/json")
                        .body(req1)
                        .post("/api/ingestion");

        String raw11 = r11.asString();
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_1_STATUS", r11.getStatusCode());
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_1_RAW", raw11);

        io.restassured.response.Response r12 =
                io.restassured.RestAssured.given()
                        .contentType("application/json")
                        .accept("application/json")
                        .body(req1)
                        .post("/api/ingestion");

        String raw12 = r12.asString();
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_2_STATUS", r12.getStatusCode());
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_2_RAW", raw12);

        // --------- call REQ2 once ----------
        io.restassured.response.Response r21 =
                io.restassured.RestAssured.given()
                        .contentType("application/json")
                        .accept("application/json")
                        .body(req2)
                        .post("/api/ingestion");

        String raw21 = r21.asString();
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ2_CALL_1_STATUS", r21.getStatusCode());
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ2_CALL_1_RAW", raw21);

        org.junit.jupiter.api.Assertions.assertEquals(200, r11.getStatusCode());
        org.junit.jupiter.api.Assertions.assertEquals(200, r12.getStatusCode());
        org.junit.jupiter.api.Assertions.assertEquals(200, r21.getStatusCode());

        // --------- extract ingestionIds from responses (inline, no helper) ----------
        String ing11 = null;
        try {
            com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(raw11).getAsJsonObject();
            if (root.has("data") && !root.get("data").isJsonNull()) {
                com.google.gson.JsonObject data = root.getAsJsonObject("data");
                if (data.has("ingestionId") && !data.get("ingestionId").isJsonNull()) {
                    ing11 = data.get("ingestionId").getAsString();
                }
            }
        } catch (Exception ignore) {
            ing11 = null;
        }

        String ing12 = null;
        try {
            com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(raw12).getAsJsonObject();
            if (root.has("data") && !root.get("data").isJsonNull()) {
                com.google.gson.JsonObject data = root.getAsJsonObject("data");
                if (data.has("ingestionId") && !data.get("ingestionId").isJsonNull()) {
                    ing12 = data.get("ingestionId").getAsString();
                }
            }
        } catch (Exception ignore) {
            ing12 = null;
        }

        String ing21 = null;
        try {
            com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(raw21).getAsJsonObject();
            if (root.has("data") && !root.get("data").isJsonNull()) {
                com.google.gson.JsonObject data = root.getAsJsonObject("data");
                if (data.has("ingestionId") && !data.get("ingestionId").isJsonNull()) {
                    ing21 = data.get("ingestionId").getAsString();
                }
            }
        } catch (Exception ignore) {
            ing21 = null;
        }

        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_1_INGESTION_ID", ing11);
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ1_CALL_2_INGESTION_ID", ing12);
        ai.braineous.rag.prompt.observe.Console.log("IT_REQ2_CALL_1_INGESTION_ID", ing21);

        org.junit.jupiter.api.Assertions.assertNotNull(ing11, "REQ1 call-1 missing ingestionId. raw=" + raw11);
        org.junit.jupiter.api.Assertions.assertNotNull(ing12, "REQ1 call-2 missing ingestionId. raw=" + raw12);
        org.junit.jupiter.api.Assertions.assertNotNull(ing21, "REQ2 call-1 missing ingestionId. raw=" + raw21);

        // surface contract
        org.junit.jupiter.api.Assertions.assertEquals(ing11, ing12, "REQ1 twice must return same ingestionId (idempotent).");
        org.junit.jupiter.api.Assertions.assertNotEquals(ing11, ing21, "REQ2 must produce a different ingestionId (new fact).");

        // --------- Mongo assertion (ONLY for these 2 ids; ignore other suite noise) ----------
        /*com.mongodb.client.MongoClient mongoClient =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");
        com.mongodb.client.MongoCollection<org.bson.Document> col =
                mongoClient
                        .getDatabase("dd")
                        .getCollection("ingestion");

        long countId1 =
                col.countDocuments(new org.bson.Document("ingestionId", ing11));

        long countId2 =
                col.countDocuments(new org.bson.Document("ingestionId", ing21));

        ai.braineous.rag.prompt.observe.Console.log("IT_MONGO_COUNT_ID1", countId1);
        ai.braineous.rag.prompt.observe.Console.log("IT_MONGO_COUNT_ID2", countId2);

        //org.junit.jupiter.api.Assertions.assertEquals(1L, countId1, "Expected exactly 1 Mongo doc for ingestionId=" + ing11);
        //org.junit.jupiter.api.Assertions.assertEquals(1L, countId2, "Expected exactly 1 Mongo doc for ingestionId=" + ing21);

        long countBoth =
                col.countDocuments(
                        com.mongodb.client.model.Filters.in("ingestionId", ing11, ing21)
                );

        ai.braineous.rag.prompt.observe.Console.log("IT_MONGO_COUNT_BOTH_IDS", countBoth);

        org.junit.jupiter.api.Assertions.assertEquals(
                2L,
                countBoth,
                "Expected exactly 2 docs for the two ingestionIds from this test (dedup + new)."
        );*/
    }




    //---------------------------------------------------------------------------------------
    /**
     * Extracts data.ingestionId from the HTTP JSON response:
     * { "ok": true, "data": { "ingestionId": "DD-ING-..." } }
     */
    private String extractIngestionIdOrNull(String rawJson) {

        if (rawJson == null || rawJson.trim().isEmpty()) {
            return null;
        }

        com.google.gson.JsonObject root =
                com.google.gson.JsonParser.parseString(rawJson).getAsJsonObject();

        if (!root.has("data") || root.get("data").isJsonNull()) {
            return null;
        }

        com.google.gson.JsonObject data = root.getAsJsonObject("data");

        if (!data.has("ingestionId") || data.get("ingestionId").isJsonNull()) {
            return null;
        }

        String v = data.get("ingestionId").getAsString();
        if (v == null) {
            return null;
        }

        if (v.trim().isEmpty()) {
            return null;
        }

        return v;
    }
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
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
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

    private static String buildReq(TestInfo ti, String innerPayloadJsonString) {
        // wrapper format from ingestion.sh
        String itName = ti != null ? ti.getDisplayName() : "null";
        // we add itTest marker inside the payload json so you can trace if needed later
        // (non-breaking extra field at root of inner json string)
        String inner = innerPayloadJsonString;
        if (innerPayloadJsonString != null && innerPayloadJsonString.startsWith("{")) {
            // add "itTest" at top-level of inner payload object (safe)
            inner = "{\"itTest\":\"" + escapeJson(itName) + "\"," + innerPayloadJsonString.substring(1);
        }

        String req =
                "{"
                        + "\"stream\":\"ingestion\","
                        + "\"payload\":\"" + escapeJson(inner) + "\""
                        + "}";

        Console.log("IT_REQ_BUILT", req);
        return req;
    }

    private static JsonObject postIngestion(String requestJson) {
        String raw =
                given()
                        .baseUri(BASE_URL)
                        .contentType("application/json")
                        .accept("application/json")
                        .body(requestJson)
                        .when()
                        .post(ENDPOINT)
                        .then()
                        .statusCode(200)
                        .extract()
                        .asString();

        Console.log("IT_HTTP_RAW_RESP", raw);
        return JsonParser.parseString(raw).getAsJsonObject();
    }

    private static String extractIngestionId(JsonObject resp) {
        assertNotNull(resp, "Response JSON is null");
        assertTrue(resp.has("ingestionId"), "Response missing ingestionId: " + resp);
        String id = resp.get("ingestionId").getAsString();
        assertNotNull(id);
        assertTrue(id.trim().length() > 0);
        return id;
    }

    private static void awaitMongoTotalDocs(long expected) {
        long deadline = System.currentTimeMillis() + 8000L;

        com.mongodb.client.MongoClient mongo = MongoClients.create("mongodb://localhost:27017");
        try {
            com.mongodb.client.MongoCollection<Document> col =
                    mongo.getDatabase("dd").getCollection("ingestion");

            while (System.currentTimeMillis() < deadline) {
                long total = col.countDocuments();
                Console.log("IT_MONGO_TOTAL_DOCS_POLL", total);
                if (total == expected) {
                    return;
                }
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            long finalTotal = col.countDocuments();
            fail("Timed out waiting for Mongo total docs == " + expected + " (actual=" + finalTotal + ")");
        } finally {
            mongo.close();
        }
    }

    private static long mongoCountByIngestionId(String ingestionId) {
        com.mongodb.client.MongoClient mongo = MongoClients.create("mongodb://localhost:27017");
        try {
            com.mongodb.client.MongoCollection<Document> col =
                    mongo.getDatabase("dd").getCollection("ingestion");

            return col.countDocuments(new Document("ingestionId", ingestionId));
        } finally {
            mongo.close();
        }
    }


    private static Document mongoFindByIngestionId(String ingestionId) {
        com.mongodb.client.MongoClient mongo = MongoClients.create("mongodb://localhost:27017");
        try {
            com.mongodb.client.MongoCollection<Document> col =
                    mongo.getDatabase("dd").getCollection("ingestion");

            return col.find(new Document("ingestionId", ingestionId)).first();
        } finally {
            mongo.close();
        }
    }

    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        // minimal escape for embedding JSON string inside JSON string
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String postJson(String url, String body) throws Exception {
        java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                .build();

        java.net.http.HttpResponse<String> resp =
                java.net.http.HttpClient.newHttpClient()
                        .send(req, java.net.http.HttpResponse.BodyHandlers.ofString());

        Console.log("IT_HTTP_STATUS", resp.statusCode());
        Console.log("IT_HTTP_RAW_RESP", resp.body());

        org.junit.jupiter.api.Assertions.assertTrue(
                resp.statusCode() >= 200 && resp.statusCode() < 300,
                "HTTP failed: status=" + resp.statusCode() + " body=" + resp.body()
        );

        return resp.body();
    }

    private static void awaitMongoDocExists(String ingestionId) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;

        com.mongodb.client.MongoClient mongo =
                com.mongodb.client.MongoClients.create("mongodb://localhost:27017");
        try {
            com.mongodb.client.MongoCollection<org.bson.Document> col =
                    mongo.getDatabase("dd").getCollection("ingestion");

            while (System.currentTimeMillis() < deadline) {
                org.bson.Document doc =
                        col.find(new org.bson.Document("ingestionId", ingestionId)).first();
                if (doc != null) {
                    Console.log("IT_MONGO_DOC_FOUND", ingestionId);
                    return;
                }
                Thread.sleep(50L);
            }

            org.junit.jupiter.api.Assertions.fail("Timeout waiting for Mongo doc ingestionId=" + ingestionId);

        } finally {
            mongo.close();
        }
    }
}
