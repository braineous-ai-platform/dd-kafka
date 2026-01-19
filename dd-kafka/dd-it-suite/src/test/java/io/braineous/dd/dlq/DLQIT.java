package io.braineous.dd.dlq;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;

import com.mongodb.client.MongoCollection;
import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import io.braineous.dd.dlq.service.DLQOrchestrator;

import io.braineous.dd.dlq.service.client.DLQClient;
import io.braineous.dd.dlq.service.client.DLQHttpPoster;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
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
        mongoClient.getDatabase("dd").getCollection("dlq_domain").drop(); // <-- add this
        try { Thread.sleep(250L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }



    @Test
    void systemFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("sys", "boom");

        orch.setHttpPoster(poster);

        // ---- act ----
        orch.orchestrateSystemFailure(new Exception("dlq_it_test"), ddEventJson.toString());

        Console.log("dlq_it_system_act_sent", ddEventJson.toString());

        // ---- assert (async: http -> kafka -> consumer -> mongo) ----
        String needle = "dlqSystemCode";
        Console.log("dlq_it_system_await_needle", needle);

        awaitPayloadContains(dlqSystemCol(), needle);

        // pull the doc we actually waited for
        Document doc = dlqSystemCol()
                .find(new Document("payload", new Document("$regex", needle)))
                .first();

        assertNotNull(doc);
        Console.log("dlq_it_system_doc", String.valueOf(doc));

        assertEquals("system", doc.getString("kind"));
        assertNotNull(doc.getString("dlqId"));
        assertNotNull(doc.getDate("createdAt"));

        String payload = doc.getString("payload");
        assertNotNull(payload);

        assertTrue(payload.contains("\"sys\":\"boom\""));
        assertTrue(payload.contains("\"dlqSystemCode\":\"DD-DLQ-SYSTEM-EXCEPTION\""));
        assertTrue(payload.contains("\"dlqSystemException\":\"dlq_it_test\""));

        String payloadSha = doc.getString("payloadSha256");
        assertEquals(sha256(payload), payloadSha);
    }

    @Test
    void domainFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("dom", "boom");

        orch.setHttpPoster(poster);

        // ---- act ----
        orch.orchestrateDomainFailure(new Exception("dlq_it_test"), ddEventJson.toString());

        Console.log("dlq_it_domain_act_sent", ddEventJson.toString());

        // ---- assert (async: http -> kafka -> consumer -> mongo) ----
        String needle = "dlqDomainCode";
        Console.log("dlq_it_domain_await_needle", needle);

        awaitPayloadContains(dlqDomainCol(), needle);

        // pull the doc we actually waited for
        Document doc = dlqDomainCol()
                .find(new Document("payload", new Document("$regex", needle)))
                .first();

        assertNotNull(doc);
        Console.log("dlq_it_domain_doc", String.valueOf(doc));

        assertEquals("domain", doc.getString("kind"));
        assertNotNull(doc.getString("dlqId"));
        assertNotNull(doc.getDate("createdAt"));

        String payload = doc.getString("payload");
        assertNotNull(payload);

        assertTrue(payload.contains("\"dom\":\"boom\""));
        assertTrue(payload.contains("\"dlqDomainCode\":\"DD-DLQ-DOMAIN-EXCEPTION\""));
        assertTrue(payload.contains("\"dlqDomainException\":\"dlq_it_test\""));

        String payloadSha = doc.getString("payloadSha256");
        assertEquals(sha256(payload), payloadSha);
    }

    @Test
    void nullInput_does_nothing() {
        HttpPoster poster = new DLQHttpPoster();
        orch.setHttpPoster(poster);

        // ---- pre-assert: ensure clean slate (drop is async-ish) ----
        awaitAtLeast(dlqSystemCol(), 0L);
        awaitAtLeast(dlqDomainCol(), 0L);

        long sysBefore = dlqSystemCol().countDocuments();
        long domBefore = dlqDomainCol().countDocuments();

        Console.log("dlq_it_null_before_sys", String.valueOf(sysBefore));
        Console.log("dlq_it_null_before_dom", String.valueOf(domBefore));

        // we expect both empty at start for this test
        assertEquals(0L, sysBefore);
        assertEquals(0L, domBefore);

        // ---- act ----
        orch.orchestrateSystemFailure(null, null);
        orch.orchestrateDomainFailure(null, null);

        // ---- assert ----
        long sysAfter = dlqSystemCol().countDocuments();
        long domAfter = dlqDomainCol().countDocuments();

        Console.log("dlq_it_null_after_sys", String.valueOf(sysAfter));
        Console.log("dlq_it_null_after_dom", String.valueOf(domAfter));

        assertEquals(0L, sysAfter);
        assertEquals(0L, domAfter);
    }

    @Test
    void invoke_ok_2xx_defensive() {
        HttpPoster httpPoster = new DLQHttpPoster();

        JsonSerializer serializer = o -> {
            Console.log("serializer.toJson", String.valueOf(o));
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
        Console.log("dlq.result", String.valueOf(result));

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

    private void awaitAtLeast(com.mongodb.client.MongoCollection<org.bson.Document> col, long minExpected) {
        long deadline = System.currentTimeMillis() + 15000L;
        while (System.currentTimeMillis() < deadline) {
            long c = col.countDocuments();
            if (c >= minExpected) return;
            try { Thread.sleep(50L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        org.junit.jupiter.api.Assertions.fail("timeout waiting for count>=" + minExpected + " but was " + col.countDocuments());
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

    private void awaitPayloadContains(MongoCollection<Document> col, String needle) {
        long deadline = System.currentTimeMillis() + 15000L;
        while (System.currentTimeMillis() < deadline) {
            Document doc = col.find(new Document("payload", new Document("$regex", needle))).first();
            if (doc != null) return;
            try { Thread.sleep(50L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        Assertions.fail("timeout waiting for payload containing: " + needle);
    }

}
