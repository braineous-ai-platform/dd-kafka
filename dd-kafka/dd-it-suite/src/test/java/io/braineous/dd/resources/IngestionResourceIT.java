package io.braineous.dd.resources;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.Gson;
import io.braineous.dd.consumer.service.persistence.MongoIngestionStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.core.IsEqual.equalTo;

//@QuarkusTest
public class IngestionResourceIT {

    /*@Inject
    com.mongodb.client.MongoClient mongoClient;

    @BeforeEach
    void before() {
        resetMongo();
    }

    @AfterEach
    void after() {
        resetMongo();
    }

    @Test
    void it_ingestion_validEvent_returns200_okTrue() {

        String ddEventEnvelope = """
    {
      "kafka": {
        "topic": "ingestion",
        "partition": 0,
        "offset": 1,
        "timestamp": 1700000000000
      },
      "payload": {
        "encoding": "base64",
        "value": "SGVsbG8="
      }
    }
    """;

        String wrappedRequest = new Gson().toJson(
                java.util.Map.of("payload", ddEventEnvelope)
        );

        given()
                .contentType("application/json")
                .accept("application/json")
                .body(wrappedRequest)
                .when()
                .post("/api/ingestion")
                .then()
                .statusCode(200)
                .body("ok", equalTo(true));
    }

    @Test
    void it_ingestion_payloadBlank_returns400() {

        given()
                .contentType("application/json")
                .accept("application/json")
                .body("""
        { "payload": "   " }
        """)
                .when()
                .post("/api/ingestion")
                .then()
                .statusCode(400)
                .body("ok", equalTo(false))
                .body("why.reason", equalTo("DD-INGEST-payload_blank"));
    }

    @Test
    void it_ingestion_validEvent_persistsToMongo() {

        // --- given ---
        String ddEventEnvelope = """
    {
      "kafka": {
        "topic": "ingestion",
        "partition": 0,
        "offset": 1,
        "timestamp": 1700000000000
      },
      "payload": {
        "encoding": "base64",
        "value": "SGVsbG8="
      }
    }
    """;

        String body = new com.google.gson.Gson().toJson(
                java.util.Map.of("payload", ddEventEnvelope)
        );

        // --- when ---
        given()
                .contentType("application/json")
                .accept("application/json")
                .body(body)
                .when()
                .post("/api/ingestion")
                .then()
                .statusCode(200)
                .body("ok", equalTo(true));

        // --- then: mongo side-effect (eventual) ---
        var db = mongoClient.getDatabase(MongoIngestionStore.DB);
        var coll = db.getCollection(MongoIngestionStore.COL);

        long expected = 1L;
        long timeoutMs = 3000L;
        long intervalMs = 100L;

        long start = System.currentTimeMillis();
        long count = 0L;

        while (System.currentTimeMillis() - start < timeoutMs) {
            count = coll.countDocuments();
            Console.log("it_mongo_ingestion_count", count);

            if (count >= expected) {
                break;
            }

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ignored) {
                // keep deterministic behavior
            }
        }

        org.junit.jupiter.api.Assertions.assertTrue(
                count >= expected,
                "Mongo ingestion count expected >= " + expected + " within " + timeoutMs + "ms, but was " + count
        );

        // Optional: light sanity on stored doc (no deep schema coupling)
        var doc = coll.find().first();
        Console.log("it_mongo_ingestion_doc", doc);

        org.junit.jupiter.api.Assertions.assertNotNull(doc);
        org.junit.jupiter.api.Assertions.assertTrue(
                doc.toJson().contains("\"kafka\"")
        );
    }



    //--------------------------------------
    private void resetMongo() {
        mongoClient.getDatabase(MongoIngestionStore.DB)
                .getCollection(MongoIngestionStore.COL)
                .drop();
    }*/

}
