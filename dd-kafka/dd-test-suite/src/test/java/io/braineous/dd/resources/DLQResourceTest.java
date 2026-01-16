package io.braineous.dd.resources;


import io.braineous.dd.dlq.persistence.DLQStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class DLQResourceTest {

    @Inject
    DLQStore store;

    // ---------------- SYSTEM ----------------

    @Test
    void system_timeWindow_ok() {
        given()
                .queryParam("fromTime", "2026-01-01T00:00:00Z")
                .queryParam("toTime",   "2026-01-02T00:00:00Z")
                .when()
                .get("/api/dlq/system")
                .then()
                .statusCode(200)
                .body("$", notNullValue());
    }

    @Test
    void system_timeWindow_missingParams_returns400() {
        given()
                .when()
                .get("/api/dlq/system")
                .then()
                .statusCode(400)
                .body("ok", equalTo(false));
    }

    @Test
    void system_byId_ok() {
        given()
                .queryParam("dlqId", "DD-DLQ-test-1")
                .when()
                .get("/api/dlq/system/by-id")
                .then()
                .statusCode(200)
                .body("$", notNullValue());
    }

    // ---------------- DOMAIN ----------------

    @Test
    void domain_timeWindow_ok() {
        given()
                .queryParam("fromTime", "2026-01-01T00:00:00Z")
                .queryParam("toTime",   "2026-01-02T00:00:00Z")
                .when()
                .get("/api/dlq/domain")
                .then()
                .statusCode(200)
                .body("$", notNullValue());
    }

    @Test
    void domain_byId_ok() {
        given()
                .queryParam("dlqId", "DD-DLQ-test-2")
                .when()
                .get("/api/dlq/domain/by-id")
                .then()
                .statusCode(200)
                .body("$", notNullValue());
    }

    // ------------------------------------------------------------------
    // FAKE STORE — WIRED HERE, NO MONGO, NO KAFKA
    // ------------------------------------------------------------------

    @jakarta.enterprise.inject.Alternative
    @jakarta.annotation.Priority(1) // beats MongoDLQStore
    @jakarta.enterprise.context.ApplicationScoped
    public static class FakeDLQStore implements DLQStore {

        @Override
        public void storeDomainFailure(String payload) { }

        @Override
        public void storeSystemFailure(String payload) { }

        @Override
        public com.google.gson.JsonArray findSystemFailureByTimeWindow(String fromTime, String toTime) {
            return new com.google.gson.JsonArray();
        }

        @Override
        public com.google.gson.JsonObject findSystemFailureById(String dlqId) {
            com.google.gson.JsonObject jo = new com.google.gson.JsonObject();
            jo.addProperty("dlqId", dlqId);
            jo.addProperty("kind", "system");
            return jo;
        }

        @Override
        public com.google.gson.JsonArray findDomainFailureByTimeWindow(String fromTime, String toTime) {
            return new com.google.gson.JsonArray();
        }

        @Override
        public com.google.gson.JsonObject findDomainFailureById(String dlqId) {
            com.google.gson.JsonObject jo = new com.google.gson.JsonObject();
            jo.addProperty("dlqId", dlqId);
            jo.addProperty("kind", "domain");
            return jo;
        }
    }
}

