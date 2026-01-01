package io.braineous.dd.processor;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.DDEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class ProcessorOrchestratorITTest {

    private static final String CONSUMER_BASE = System.getProperty("dd.consumer.baseUrl", "http://localhost:8081");
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    public void setUp() throws Exception{
        resetOrchestratorPoster();
    }

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

        // build event
        GsonJsonSerializer serializer = new GsonJsonSerializer();
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();


        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance(); // or getInstance()/setter — use your wiring
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        assertNotNull(result);
        assertTrue(result.isOk(), "Orchestrator failed: " + (result.getWhy() != null ? result.getWhy() : "null"));

        // poll for consumer consume
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }

        assertTrue(size > 0, "Expected consumer to consume at least 1 message");
    }

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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-missing_kafka", result.getWhy().getReason());

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
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

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
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
        JsonObject ddEventJson = JsonParser.parseString(serializer.toJson(event)).getAsJsonObject();

        // extras ignored by design (forward-compatible)
        ddEventJson.addProperty("extraRootField", "root-ok");

        JsonObject kafka = ddEventJson.getAsJsonObject("kafka");
        kafka.addProperty("extraKafkaField", "kafka-ok");

        JsonObject payload = ddEventJson.getAsJsonObject("payload");
        payload.addProperty("extraPayloadField", "payload-ok");

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert ok
        assertNotNull(result);
        assertTrue(result.isOk(), "Orchestrator failed: " + (result.getWhy() != null ? result.getWhy() : "null"));

        // poll for consumer consume
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size > 0) break;
            Thread.sleep(100);
        }

        assertTrue(size > 0, "Expected consumer to consume at least 1 message");
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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-kafka_topic", result.getWhy().getReason());

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
    }

    @Test
    void orchestrate_negativePartition_shouldFail_and_notProduce(TestInfo ti) throws Exception {

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

        // sabotage: negative partition
        ddEventJson.getAsJsonObject("kafka").addProperty("partition", -1);

        // real poster (no mock)
        HttpPoster httpPoster = new DDIngestionHttpPoster();

        // orchestrate
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);
        ProcessorResult result = orch.orchestrate(ddEventJson);

        // assert fail-fast
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());
        assertEquals("DD-ORCH-VALIDATE-kafka_partition", result.getWhy().getReason());

        // assert no produce/consume side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
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
        f.set(ProcessorOrchestrator.getInstance(), null);

        // transport failure poster
        HttpPoster throwingPoster = (url, jsonBody) -> {
            Console.log("TRANSPORT", "HttpPoster invoked, throwing");
            throw new RuntimeException("IT_TRANSPORT_DOWN");
        };

        // orchestrate
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(throwingPoster);

        ProcessorResult result = orch.orchestrate(ddEventJson);

        // Console.log
        Console.log("RESULT", result);
        Console.log("WHY", result != null ? result.getWhy() : null);

        // assert failure
        assertNotNull(result);
        assertFalse(result.isOk());
        assertNotNull(result.getWhy());

        // NOTE: lock exact reason after first run
        // assertEquals("DD-PRODUCER-INVOKE-exception", result.getWhy().getReason());

        // assert no side-effect
        Thread.sleep(500);
        assertEquals(0, getInt(CONSUMER_BASE + "/debug/captures/size"));
    }

    @Test
    void orchestrate_sameEventTwice_shouldConsumeTwice_forNow(TestInfo ti) throws Exception {

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

        // orchestrate (send twice)
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);

        ProcessorResult r1 = orch.orchestrate(ddEventJson);
        Console.log("DUP_SEND_1", r1);

        ProcessorResult r2 = orch.orchestrate(ddEventJson);
        Console.log("DUP_SEND_2", r2);

        assertNotNull(r1);
        assertTrue(r1.isOk(), "First send failed: " + (r1.getWhy() != null ? r1.getWhy() : "null"));

        assertNotNull(r2);
        assertTrue(r2.isOk(), "Second send failed: " + (r2.getWhy() != null ? r2.getWhy() : "null"));

        // poll until size == 2 (or timeout)
        long deadline = System.currentTimeMillis() + 10_000;
        int size = 0;
        while (System.currentTimeMillis() < deadline) {
            size = getInt(CONSUMER_BASE + "/debug/captures/size");
            if (size >= 2) break;
            Thread.sleep(100);
        }

        Console.log("CONSUMER_SIZE", size);
        assertEquals(2, size, "Expected two consumed messages for duplicate sends (pre-idempotency)");
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
        ProcessorOrchestrator orch = ProcessorOrchestrator.getInstance();
        orch.setHttpPoster(httpPoster);

        ProcessorResult r1 = orch.orchestrate(ddEventJson);
        Console.log("REPLAY_SEND_1", r1);
        assertNotNull(r1);
        assertTrue(r1.isOk(), "First send failed: " + (r1.getWhy() != null ? r1.getWhy() : "null"));

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

        // should consume again (size returns to 1 after clear)
        awaitSizeAtLeast(1);
        int size = getInt(CONSUMER_BASE + "/debug/captures/size");
        Console.log("AFTER_REPLAY_CONSUME_SIZE", size);

        assertEquals(1, size, "Expected replay to be consumed after consumer store reset");
    }

    //---------------------------------------------------------------------------------------
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

    private static void resetOrchestratorPoster() throws Exception {
        Field f = ProcessorOrchestrator.class.getDeclaredField("httpPoster");
        f.setAccessible(true);
        f.set(ProcessorOrchestrator.getInstance(), null);
    }

}
