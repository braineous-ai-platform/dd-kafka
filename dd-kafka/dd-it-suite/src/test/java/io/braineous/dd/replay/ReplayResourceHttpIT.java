package io.braineous.dd.replay;

import ai.braineous.cgo.config.ConfigService;
import io.braineous.dd.core.config.DDConfigService;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import org.bson.Document;
import org.hamcrest.Matchers;

@io.quarkus.test.junit.QuarkusTest
public class ReplayResourceHttpIT {

    @jakarta.inject.Inject
    com.mongodb.client.MongoClient mongoClient;

    @org.junit.jupiter.api.BeforeEach
    void before() {
        resetMongo();
    }

    @org.junit.jupiter.api.AfterEach
    void after() {
        resetMongo();
    }

    @org.junit.jupiter.api.Test
    void http_timeWindow_badRequest_when_reason_missing_returns400() {

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        String reqJson = """
        {
          "reason": "   ",
          "fromTime": "2026-01-07T00:00:00Z",
          "toTime":   "2026-01-07T01:00:00Z"
        }
        """;

        io.restassured.RestAssured
                .given()
                .contentType("application/json")
                .accept("application/json")
                .body(reqJson)
                .when()
                .post("/api/replay/time-window")
                .then()
                .statusCode(400)
                .body("ok", Matchers.equalTo(false))
                .body("reason", Matchers.equalTo("DD-REPLAY-bad_request-reason_missing"))
                .body("matchedCount", Matchers.equalTo(0))
                .body("replayedCount", Matchers.equalTo(0));
    }

    @org.junit.jupiter.api.Test
    void http_timeWindow_gateOn_returnsOk_andReportsMatchedEvents() {

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        com.mongodb.client.MongoCollection<Document> col = mongoClient
                .getDatabase(MongoIngestionStore.DB)
                .getCollection(MongoIngestionStore.COL);

        // Seed 1 replayable event inside the requested time window
        java.time.Instant seedAt = java.time.Instant.parse("2026-01-07T00:15:00Z");

        // Must satisfy ProcessorOrchestrator.validate(...) so replay loop doesn't skip it.
        String ddEventJson = """
        {
          "kafka": {
            "topic": "ingestion",
            "partition": 0,
            "offset": 1,
            "timestamp": 1736295600000
          },
          "payload": {
            "encoding": "base64",
            "value": "eyJoZWxsbyI6IndvcmxkIn0="
          }
        }
        """;

        col.insertOne(new Document()
                .append("ingestionId", "SEED-1")
                .append("payload", ddEventJson)
                .append("createdAt", java.util.Date.from(seedAt))
                // schema-ish fields (safe to include; replay ignores)
                .append("snapshotHash", "snap-SEED-1")
                .append("payloadHash", "hash-SEED-1"));

        java.time.Instant t1 = java.time.Instant.parse("2026-01-07T00:10:00Z");
        java.time.Instant t3 = java.time.Instant.parse("2026-01-07T00:30:00Z"); // exclusive end

        String reqJson = """
        {
          "reason": "it-test",
          "fromTime": "%s",
          "toTime":   "%s"
        }
        """.formatted(t1.toString(), t3.toString());

        io.restassured.RestAssured
                .given()
                .contentType("application/json")
                .accept("application/json")
                .body(reqJson)
                .when()
                .post("/api/replay/time-window")
                .then()
                .statusCode(200)
                .body("ok", Matchers.equalTo(true))
                .body("reason", Matchers.nullValue())
                .body("replayId", Matchers.notNullValue())
                .body("matchedCount", Matchers.equalTo(1))
                // transport may or may not be wired in this IT; accept either
                .body("replayedCount", Matchers.anyOf(
                        Matchers.equalTo(0),
                        Matchers.equalTo(1)
                ));
    }

    @org.junit.jupiter.api.Test
    void http_ingestion_badRequest_when_ingestionId_missing_returns400() {

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        String reqJson = """
        {
          "reason": "it-test",
          "ingestionId": "   "
        }
        """;

        io.restassured.RestAssured
                .given()
                .contentType("application/json")
                .accept("application/json")
                .body(reqJson)
                .when()
                .post("/api/replay/ingestion")
                .then()
                .statusCode(400)
                .body("ok", Matchers.equalTo(false))
                .body("reason", Matchers.equalTo("DD-REPLAY-bad_request-ingestionId_missing"))
                .body("matchedCount", Matchers.equalTo(0))
                .body("replayedCount", Matchers.equalTo(0));
    }

    @org.junit.jupiter.api.Test
    void http_dlqDomain_badRequest_when_dlqId_missing_returns400() {

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        String reqJson = """
        {
          "reason": "it-test",
          "dlqId": "   "
        }
        """;

        io.restassured.RestAssured
                .given()
                .contentType("application/json")
                .accept("application/json")
                .body(reqJson)
                .when()
                .post("/api/replay/dlq/domain")
                .then()
                .statusCode(400)
                .body("ok", Matchers.equalTo(false))
                .body("reason", Matchers.equalTo("DD-REPLAY-bad_request-dlqId_missing"))
                .body("matchedCount", Matchers.equalTo(0))
                .body("replayedCount", Matchers.equalTo(0));
    }

    @org.junit.jupiter.api.Test
    void http_dlqSystem_badRequest_when_dlqId_missing_returns400() {

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        String reqJson = """
        {
          "reason": "it-test",
          "dlqId": "   "
        }
        """;

        io.restassured.RestAssured
                .given()
                .contentType("application/json")
                .accept("application/json")
                .body(reqJson)
                .when()
                .post("/api/replay/dlq/system")
                .then()
                .statusCode(400)
                .body("ok", Matchers.equalTo(false))
                .body("reason", Matchers.equalTo("DD-REPLAY-bad_request-dlqId_missing"))
                .body("matchedCount", Matchers.equalTo(0))
                .body("replayedCount", Matchers.equalTo(0));
    }

    @org.junit.jupiter.api.Test
    void http_timeWindow_returns403_when_disabled() {

        // Force-disable at JVM level so the running app's gate sees it
        String key = "dd.feature.replay.enabled";
        String prev = System.getProperty(key);
        System.setProperty(key, "false");

        try {
            String reqJson = """
        {
          "reason": "it-test",
          "fromTime": "2026-01-07T00:00:00Z",
          "toTime":   "2026-01-07T01:00:00Z"
        }
        """;

            io.restassured.RestAssured
                    .given()
                    .contentType("application/json")
                    .accept("application/json")
                    .body(reqJson)
                    .when()
                    .post("/api/replay/time-window")
                    .then()
                    .statusCode(403)
                    .body("ok", org.hamcrest.Matchers.equalTo(false))
                    .body("reason", org.hamcrest.Matchers.equalTo("DD-CONFIG-replay_disabled"))
                    .body("matchedCount", org.hamcrest.Matchers.equalTo(0))
                    .body("replayedCount", org.hamcrest.Matchers.equalTo(0));

        } finally {
            // restore prior value
            if (prev == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, prev);
            }
        }
    }


    // ---------------- helpers ----------------

    private void resetMongo() {
        mongoClient.getDatabase(MongoIngestionStore.DB)
                .getCollection(MongoIngestionStore.COL)
                .drop();
    }
}





