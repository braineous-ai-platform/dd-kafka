package io.braineous.dd.resources;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.restassured.response.Response;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
public class IngestionResourceIT {

    @Inject
    com.mongodb.client.MongoClient mongoClient;

    private final Gson gson = new Gson();

    @BeforeEach
    void beforeEach() {
        resetMongo();
    }

    @AfterEach
    void afterEach() {
        resetMongo();
    }

    @Test
    void it_ingestion_validEvent_returns200_andPersistsByIngestionIdAxis() {

        // ANCHOR DOCTRINE: only 200 is "anchored". anything else => fail.
        String ddEventEnvelope = ddEventEnvelopeHelloBase64();
        String wrappedRequest = wrapPayload(ddEventEnvelope);

        Response resp = postIngestion(wrappedRequest);

        int code = resp.statusCode();
        Console.log("it_ingestion_status", Integer.valueOf(code));
        Console.log("it_ingestion_body", resp.asString());

        // HARD ASSERT: anchored means 200
        Assertions.assertEquals(
                200,
                code,
                "Anchor doctrine: only 200 is guaranteed. Anything else means NOT anchored."
        );

        // contract: envelope A
        resp.then()
                .body("ok", equalTo(true))
                .body("why", equalTo(null))
                .body("data", notNullValue())
                .body("data.ingestionId", notNullValue())
                .body("data.ok", equalTo(true));

        String ingestionId = resp.jsonPath().getString("data.ingestionId");
        Assertions.assertNotNull(ingestionId, "ingestionId must be present on 200");
        Assertions.assertTrue(ingestionId.trim().length() > 0, "ingestionId must be non-blank on 200");

        // anchor side-effect: ingestionId axis must exist in Mongo
        Document doc = assertMongoHasIngestionId(ingestionId);

        // minimal anchor invariants only
        Assertions.assertNotNull(doc.getDate("createdAt"), "Mongo doc must have createdAt");
        Assertions.assertNotNull(doc.getString("payload"), "Mongo doc must have payload (string JSON)");

        // sanity payload carries kafka envelope (string contains)
        String payloadText = doc.getString("payload");
        Assertions.assertTrue(payloadText.contains("\"kafka\""), "Stored payload must contain kafka envelope");
        Assertions.assertTrue(
                payloadText.contains("\"topic\":\"ingestion\"") || payloadText.contains("\"topic\": \"ingestion\""),
                "Stored payload must contain topic=ingestion"
        );
    }

    @Test
    void it_ingestion_payloadBlank_returns400_andNoPersist() {

        given()
                .contentType("application/json")
                .accept("application/json")
                .body("{ \"payload\": \"   \" }")
                .when()
                .post("/api/ingestion")
                .then()
                .statusCode(400)
                .body("ok", equalTo(false))
                .body("data", equalTo(null))
                .body("why.code", equalTo("DD-INGEST-payload_blank"));

        // anchor doctrine: 400 means no anchoring attempt should persist
        assertMongoCountEquals(0L);
    }

    @Test
    void it_events_timeWindow_validWindow_returns200_andMatchesMongoByCreatedAtAxis() {

        // --- seed deterministic mongo docs ---
        insertIngestionDoc("ID-TW-1", "2026-01-15T00:10:00Z", ddEventEnvelopeHelloBase64());
        insertIngestionDoc("ID-TW-2", "2026-01-15T00:20:00Z", ddEventEnvelopeHelloBase64());
        insertIngestionDoc("ID-TW-3", "2026-01-15T02:00:00Z", ddEventEnvelopeHelloBase64()); // outside window

        // --- call endpoint ---
        io.restassured.response.Response resp = getEventsByTimeWindow(
                "2026-01-15T00:00:00Z",
                "2026-01-15T01:00:00Z"
        );

        int code = resp.statusCode();
        Console.log("it_events_timeWindow_status", Integer.valueOf(code));
        Console.log("it_events_timeWindow_body", resp.asString());

        // HARD ASSERT: anchored means 200
        org.junit.jupiter.api.Assertions.assertEquals(
                200,
                code,
                "Anchor doctrine: only 200 is guaranteed. Anything else means NOT anchored."
        );

        // contract: envelope A
        resp.then()
                .body("ok", org.hamcrest.Matchers.equalTo(true))
                .body("why", org.hamcrest.Matchers.equalTo(null))
                .body("data", org.hamcrest.Matchers.notNullValue())
                .body("data.fromTime", org.hamcrest.Matchers.equalTo("2026-01-15T00:00:00Z"))
                .body("data.toTime", org.hamcrest.Matchers.equalTo("2026-01-15T01:00:00Z"))
                .body("data.count", org.hamcrest.Matchers.equalTo(2))
                .body("data.events", org.hamcrest.Matchers.notNullValue());

        // E2E sanity: events array contains the seeded ingestionIds (order not assumed)
        java.util.List<String> ids = resp.jsonPath().getList("data.events.ingestionId");
        Console.log("it_events_timeWindow_ids", ids);

        org.junit.jupiter.api.Assertions.assertNotNull(ids, "events.ingestionId list must not be null");
        org.junit.jupiter.api.Assertions.assertEquals(2, ids.size(), "Expected exactly 2 ingestionIds in window");

        org.junit.jupiter.api.Assertions.assertTrue(ids.contains("ID-TW-1"), "Must contain ID-TW-1");
        org.junit.jupiter.api.Assertions.assertTrue(ids.contains("ID-TW-2"), "Must contain ID-TW-2");
        org.junit.jupiter.api.Assertions.assertFalse(ids.contains("ID-TW-3"), "Must NOT contain ID-TW-3 (outside window)");
    }

    @Test
    void it_events_timeWindow_validWindow_noMatches_returns200_count0() {

        // seed only outside window
        insertIngestionDoc("ID-TW-9", "2026-01-15T05:00:00Z", ddEventEnvelopeHelloBase64());

        io.restassured.response.Response resp = getEventsByTimeWindow(
                "2026-01-15T00:00:00Z",
                "2026-01-15T01:00:00Z"
        );

        int code = resp.statusCode();
        Console.log("it_events_timeWindow_status", Integer.valueOf(code));
        Console.log("it_events_timeWindow_body", resp.asString());

        org.junit.jupiter.api.Assertions.assertEquals(
                200,
                code,
                "Anchor doctrine: only 200 is guaranteed. Anything else means NOT anchored."
        );

        resp.then()
                .body("ok", org.hamcrest.Matchers.equalTo(true))
                .body("why", org.hamcrest.Matchers.equalTo(null))
                .body("data", org.hamcrest.Matchers.notNullValue())
                .body("data.count", org.hamcrest.Matchers.equalTo(0))
                .body("data.events.size()", org.hamcrest.Matchers.equalTo(0));
    }

    @Test
    void it_events_byIngestionId_found_returns200_andMatchesMongoAxis() {

        insertIngestionDoc("ID-LOOK-1", "2026-01-15T00:10:00Z", ddEventEnvelopeHelloBase64());

        io.restassured.response.Response resp =
                io.restassured.RestAssured
                        .given()
                        .accept("application/json")
                        .when()
                        .get("/api/ingestion/events/{ingestionId}", "ID-LOOK-1")
                        .andReturn();

        int code = resp.statusCode();
        Console.log("it_events_byIngestionId_status", Integer.valueOf(code));
        Console.log("it_events_byIngestionId_body", resp.asString());

        org.junit.jupiter.api.Assertions.assertEquals(
                200,
                code,
                "Anchor doctrine: only 200 is guaranteed. Anything else means NOT anchored."
        );

        resp.then()
                .body("ok", org.hamcrest.Matchers.equalTo(true))
                .body("why", org.hamcrest.Matchers.equalTo(null))
                .body("data", org.hamcrest.Matchers.notNullValue())
                .body("data.ingestionId", org.hamcrest.Matchers.equalTo("ID-LOOK-1"));
    }


    // ---------------- helpers ----------------

    private Response postIngestion(String wrappedRequest) {
        return given()
                .contentType("application/json")
                .accept("application/json")
                .body(wrappedRequest)
                .when()
                .post("/api/ingestion")
                .andReturn();
    }

    private String ddEventEnvelopeHelloBase64() {
        return "{\n" +
                "  \"kafka\": {\n" +
                "    \"topic\": \"ingestion\",\n" +
                "    \"partition\": 0,\n" +
                "    \"offset\": 1,\n" +
                "    \"timestamp\": 1700000000000\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"encoding\": \"base64\",\n" +
                "    \"value\": \"SGVsbG8=\"\n" +
                "  }\n" +
                "}";
    }

    private String wrapPayload(String ddEventEnvelope) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("payload", ddEventEnvelope);
        return gson.toJson(m);
    }

    private void resetMongo() {
        mongoClient.getDatabase(MongoIngestionStore.DB)
                .getCollection(MongoIngestionStore.COL)
                .drop();
    }

    private MongoCollection<Document> ingestionColl() {
        MongoDatabase db = mongoClient.getDatabase(MongoIngestionStore.DB);
        return db.getCollection(MongoIngestionStore.COL);
    }

    private void assertMongoCountEquals(long expected) {
        MongoCollection<Document> col = ingestionColl();

        long deadline = System.currentTimeMillis() + 3000;
        long count = -1L;

        while (System.currentTimeMillis() < deadline) {
            count = col.countDocuments();
            if (count == expected) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // deterministic: ignore
            }
        }

        Assertions.assertEquals(
                expected,
                count,
                "Expected Mongo ingestion count == " + expected + " within 3000ms, but was " + count
        );
    }

    private Document assertMongoHasIngestionId(String ingestionId) {

        MongoCollection<Document> col = ingestionColl();

        Document doc = null;
        long deadline = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < deadline) {
            doc = col.find(new Document("ingestionId", ingestionId)).first();
            if (doc != null) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // deterministic: ignore
            }
        }

        Console.log("it_mongo_query_ingestionId", ingestionId);

        if (doc == null) {
            Console.log("it_mongo_doc", null);
        } else {
            Console.log("it_mongo_doc", doc.toJson());
        }

        Assertions.assertNotNull(
                doc,
                "Mongo never stored ingestionId=" + ingestionId
        );

        Assertions.assertEquals(
                ingestionId,
                doc.getString("ingestionId"),
                "Mongo doc ingestionId mismatch"
        );

        return doc;
    }

    private io.restassured.response.Response getEventsByTimeWindow(String fromTime, String toTime) {
        return io.restassured.RestAssured
                .given()
                .accept("application/json")
                .queryParam("fromTime", fromTime)
                .queryParam("toTime", toTime)
                .when()
                .get("/api/ingestion/events/time-window")
                .andReturn();
    }

    private void insertIngestionDoc(String ingestionId, String createdAtIso, String ddEventEnvelope) {

        java.util.Date createdAt = java.util.Date.from(java.time.Instant.parse(createdAtIso));

        org.bson.Document doc = new org.bson.Document()
                .append("ingestionId", ingestionId)
                .append("createdAt", createdAt)
                // store shape: payload is string JSON (same as your ingestion test asserts)
                .append("payload", ddEventEnvelope);

        Console.log("it_seed_ingestionId", ingestionId);
        Console.log("it_seed_createdAt", createdAtIso);

        ingestionColl().insertOne(doc);

        // deterministic: ensure seed is visible immediately
        org.junit.jupiter.api.Assertions.assertNotNull(
                ingestionColl().find(new org.bson.Document("ingestionId", ingestionId)).first(),
                "Seed insert failed for ingestionId=" + ingestionId
        );
    }

}





