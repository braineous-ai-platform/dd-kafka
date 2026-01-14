package io.braineous.dd.dlq;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;

import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import io.braineous.dd.dlq.service.DLQOrchestrator;

import io.braineous.dd.dlq.service.client.DLQClient;
import io.braineous.dd.dlq.service.client.DLQHttpPoster;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class DLQIT {

    @Inject
    private DLQOrchestrator orch;

    @Inject
    com.mongodb.client.MongoClient mongoClient;

    @BeforeEach
    void setup() {
        CaptureStore.getInstance().clear();
        mongoClient.getDatabase("dd").getCollection("dlq_system").drop();
    }


    @org.junit.jupiter.api.Disabled("DLQ-D not wired yet; system-only scope")
    @Test
    void domainFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();
        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("k", "v");

        orch.setHttpPoster(poster);

        // ---- act ----
        orch.orchestrateDomainFailure(ddEventJson);

        Console.log("dlq_it_domain_act_sent", ddEventJson.toString());

        // ---- assert (async: http -> kafka -> consumer -> mongo) ----
        awaitCount(dlqDomainCol(), 1L);

        org.bson.Document doc = dlqDomainCol().find().first();
        assertNotNull(doc);

        Console.log("dlq_it_domain_doc", String.valueOf(doc));

        assertEquals("domain", doc.getString("kind"));
        assertNotNull(doc.getString("dlqId"));
        assertNotNull(doc.getDate("createdAt"));

        String payload = doc.getString("payload");
        assertNotNull(payload);
        assertTrue(payload.contains("\"k\":\"v\""));

        String payloadSha = doc.getString("payloadSha256");
        assertEquals(sha256(payload), payloadSha);
    }


    @Test
    void systemFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();
        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("sys", "boom");

        orch.setHttpPoster(poster);

        // ---- act ----
        orch.orchestrateSystemFailure(new Exception("dlq_it_test"),
                ddEventJson.toString());

        Console.log("dlq_it_system_act_sent", ddEventJson.toString());

        // ---- assert (async: http -> kafka -> consumer -> mongo) ----
        awaitCount(dlqSystemCol(), 1L);

        org.bson.Document doc = dlqSystemCol().find().first();
        assertNotNull(doc);

        Console.log("dlq_it_system_doc", String.valueOf(doc));

        assertEquals("system", doc.getString("kind"));
        assertNotNull(doc.getString("dlqId"));
        assertNotNull(doc.getDate("createdAt"));

        String payload = doc.getString("payload");
        assertNotNull(payload);

        // evidence: original + system annotations
        assertTrue(payload.contains("\"sys\":\"boom\""));
        assertTrue(payload.contains("\"dlqSystemCode\":\"DD-DLQ-SYSTEM-EXCEPTION\""));
        assertTrue(payload.contains("\"dlqSystemException\":\"dlq_it_test\""));

        String payloadSha = doc.getString("payloadSha256");
        assertEquals(sha256(payload), payloadSha);
    }


    @Test
    void nullInput_does_nothing() {
        HttpPoster poster = new DLQHttpPoster();
        orch.setHttpPoster(poster);

        // ---- act ----
        orch.orchestrateSystemFailure(null, null);

        // ---- assert ----
        assertEquals(0L, dlqSystemCol().countDocuments());
    }


    @Test
    void invoke_ok_2xx_defensive() {
        HttpPoster httpPoster = new DLQHttpPoster();

        JsonSerializer serializer = o -> {
            Console.log("serializer.toJson", o);
            return "{\"ok\":true}";
        };

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("id", "evt-1");

        Object ddEvent = new Object();
        String endpoint = "/domain_failure";

        // ---- act ----
        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEvent);

        // ---- assert ----
        Console.log("dlq.result", result);

        assertTrue(result.isOk(), "expected ok=true");
        assertNull(result.getWhy(), "why must be null on success");
        assertEquals(endpoint, result.getEndpoint());
        assertEquals(204, result.getHttpStatus());
        assertNotNull(result.getDurationMs());
        assertEquals(ddEventJson, result.getDdEventJson());
        assertNotNull(result.getId());
    }

    //------------------------------------------------------
    private com.mongodb.client.MongoCollection<org.bson.Document> dlqDomainCol() {
        return mongoClient.getDatabase("dd").getCollection("dlq_domain");
    }

    private com.mongodb.client.MongoCollection<org.bson.Document> dlqSystemCol() {
        return mongoClient.getDatabase("dd").getCollection("dlq_system");
    }

    private void awaitCount(com.mongodb.client.MongoCollection<org.bson.Document> col, long expected) {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            long c = col.countDocuments();
            if (c == expected) return;
            try {
                Thread.sleep(50L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        org.junit.jupiter.api.Assertions.fail("timeout waiting for count=" + expected + " but was " + col.countDocuments());
    }

    private static String sha256(String s) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (int i = 0; i < h.length; i++) {
                sb.append(String.format("%02x", h[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
