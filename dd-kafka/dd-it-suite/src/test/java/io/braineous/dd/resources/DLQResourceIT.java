package io.braineous.dd.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class DLQResourceIT {

    @Inject
    MongoClient mongoClient;

    @BeforeEach
    @AfterEach
    void reset() {
        mongoClient.getDatabase("dd").getCollection("dlq_domain").drop();
        mongoClient.getDatabase("dd").getCollection("dlq_system").drop();
    }

    @Test
    void it_system_timeWindow_then_byId_roundTrip() {

        // seed via HTTP (store endpoint not exposed; we seed directly in mongo)
        // insert ONE doc in dlq_system
        org.bson.Document doc = new org.bson.Document()
                .append("dlqId", "DD-DLQ-it-1")
                .append("kind", "system")
                .append("createdAt", java.util.Date.from(java.time.Instant.parse("2026-01-01T12:00:00Z")))
                .append("payloadSha256", "x")
                .append("payload", "{\"hello\":\"system\"}");

        mongoClient.getDatabase("dd").getCollection("dlq_system").insertOne(doc);

        String from = "2026-01-01T00:00:00Z";
        String to   = "2026-01-02T00:00:00Z";

        String body =
                given()
                        .queryParam("fromTime", from)
                        .queryParam("toTime", to)
                        .when()
                        .get("/api/dlq/system")
                        .then()
                        .statusCode(200)
                        .extract().asString();

        JsonArray arr = JsonParser.parseString(body).getAsJsonArray();
        assertTrue(arr.size() >= 1);

        JsonObject first = arr.get(0).getAsJsonObject();
        assertEquals("DD-DLQ-it-1", first.get("dlqId").getAsString());

        String byIdBody =
                given()
                        .queryParam("dlqId", "DD-DLQ-it-1")
                        .when()
                        .get("/api/dlq/system/by-id")
                        .then()
                        .statusCode(200)
                        .extract().asString();

        JsonObject byId = JsonParser.parseString(byIdBody).getAsJsonObject();
        assertEquals("DD-DLQ-it-1", byId.get("dlqId").getAsString());
        assertEquals("system", byId.get("kind").getAsString());
    }

    @Test
    void it_domain_byId_notFound_returnsEmptyObject() {

        String body =
                given()
                        .queryParam("dlqId", "DD-DLQ-missing")
                        .when()
                        .get("/api/dlq/domain/by-id")
                        .then()
                        .statusCode(200)
                        .extract().asString();

        JsonObject jo = JsonParser.parseString(body).getAsJsonObject();
        assertEquals(0, jo.size());
    }
}
