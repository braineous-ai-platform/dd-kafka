package io.braineous.dd.resources;

import ai.braineous.rag.prompt.observe.Console;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.processor.ProcessorResult;
import io.braineous.dd.replay.model.IngestionRequest;
import org.junit.jupiter.api.Test;

public class IngestionResourceTest {

    @Test
    void http_ingestion_payloadBlank_returns400() {

        IngestionResource res = new IngestionResource();

        IngestionRequest req = org.mockito.Mockito.mock(IngestionRequest.class);
        org.mockito.Mockito.when(req.getPayload()).thenReturn("   ");

        jakarta.ws.rs.core.Response resp = res.ingestion(req);

        Console.log("ut_resp_status", resp.getStatus());

        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        Object entity = resp.getEntity();
        org.junit.jupiter.api.Assertions.assertNotNull(entity);

        ProcessorResult pr = (ProcessorResult) entity;
        Console.log("ut_resp_entity", pr);

        org.junit.jupiter.api.Assertions.assertFalse(pr.isOk());
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-payload_blank", pr.getWhy().reason());
    }

    @Test
    void http_ingestion_validEvent_returns200_andCallsPoster() {

        IngestionResource res = new IngestionResource();

        FakeHttpPoster fake = new FakeHttpPoster(200);
        res.setHttpPoster(fake);

        String ddEventJson = """
    {
      "kafka": {
        "topic": "ingestion",
        "partition": 0,
        "offset": 1,
        "timestamp": 1700000000000,
        "headers": { "x": "y" }
      },
      "payload": {
        "encoding": "base64",
        "value": "SGVsbG8="
      }
    }
    """;

        IngestionRequest req = org.mockito.Mockito.mock(IngestionRequest.class);
        org.mockito.Mockito.when(req.getPayload()).thenReturn(ddEventJson);

        jakarta.ws.rs.core.Response resp = res.ingestion(req);

        Console.log("ut_resp_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        ProcessorResult pr = (ProcessorResult) resp.getEntity();
        Console.log("ut_resp_entity", pr);

        org.junit.jupiter.api.Assertions.assertNotNull(pr);
        org.junit.jupiter.api.Assertions.assertTrue(pr.isOk());

        // poster was invoked (via DDProducerClient)
        org.junit.jupiter.api.Assertions.assertEquals(1, fake.calls);

        // internal endpoint string used by orchestrator (must stay in sync)
        org.junit.jupiter.api.Assertions.assertEquals("/api/ingestion", fake.lastEndpoint);

        // sanity: body flowed through
        org.junit.jupiter.api.Assertions.assertNotNull(fake.lastJsonBody);
        org.junit.jupiter.api.Assertions.assertTrue(fake.lastJsonBody.contains("\"kafka\""));
        org.junit.jupiter.api.Assertions.assertTrue(fake.lastJsonBody.contains("\"payload\""));
    }

    @Test
    void http_ingestion_missingKafkaTopic_returns200_andOkFalse() {

        IngestionResource res = new IngestionResource();

        FakeHttpPoster fake = new FakeHttpPoster(200);
        res.setHttpPoster(fake);

        String ddEventJson_missingTopic = """
    {
      "kafka": {
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

        IngestionRequest req = org.mockito.Mockito.mock(IngestionRequest.class);
        org.mockito.Mockito.when(req.getPayload()).thenReturn(ddEventJson_missingTopic);

        jakarta.ws.rs.core.Response resp = res.ingestion(req);

        Console.log("ut_resp_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        ProcessorResult pr = (ProcessorResult) resp.getEntity();
        Console.log("ut_resp_entity", pr);

        org.junit.jupiter.api.Assertions.assertNotNull(pr);
        org.junit.jupiter.api.Assertions.assertFalse(pr.isOk());

        // Orchestrator validation reason
        org.junit.jupiter.api.Assertions.assertEquals("DD-ORCH-VALIDATE-kafka_topic", pr.getWhy().reason());

        // Validation should short-circuit; poster should not be called
        org.junit.jupiter.api.Assertions.assertEquals(0, fake.calls);
    }

    @Test
    void http_ingestion_payloadValueNotBase64_returns200_andOkFalse() {

        IngestionResource res = new IngestionResource();

        FakeHttpPoster fake = new FakeHttpPoster(200);
        res.setHttpPoster(fake);

        String ddEventJson_badBase64 = """
    {
      "kafka": {
        "topic": "ingestion",
        "partition": 0,
        "offset": 1,
        "timestamp": 1700000000000
      },
      "payload": {
        "encoding": "base64",
        "value": "NOT_BASE64!!!"
      }
    }
    """;

        IngestionRequest req = org.mockito.Mockito.mock(IngestionRequest.class);
        org.mockito.Mockito.when(req.getPayload()).thenReturn(ddEventJson_badBase64);

        jakarta.ws.rs.core.Response resp = res.ingestion(req);

        Console.log("ut_resp_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        ProcessorResult pr = (ProcessorResult) resp.getEntity();
        Console.log("ut_resp_entity", pr);

        org.junit.jupiter.api.Assertions.assertNotNull(pr);
        org.junit.jupiter.api.Assertions.assertFalse(pr.isOk());
        org.junit.jupiter.api.Assertions.assertEquals(
                "DD-ORCH-VALIDATE-payload_value_base64",
                pr.getWhy().reason()
        );

        // short-circuit before transport
        org.junit.jupiter.api.Assertions.assertEquals(0, fake.calls);
    }

    @Test
    void http_ingestion_payloadNotJsonObject_returns400() {

        IngestionResource res = new IngestionResource();

        FakeHttpPoster fake = new FakeHttpPoster(200);
        res.setHttpPoster(fake);

        // valid JSON, but not an object
        String payloadArray = "[]";

        IngestionRequest req = org.mockito.Mockito.mock(IngestionRequest.class);
        org.mockito.Mockito.when(req.getPayload()).thenReturn(payloadArray);

        jakarta.ws.rs.core.Response resp = res.ingestion(req);

        Console.log("ut_resp_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        ProcessorResult pr = (ProcessorResult) resp.getEntity();
        Console.log("ut_resp_entity", pr);

        org.junit.jupiter.api.Assertions.assertNotNull(pr);
        org.junit.jupiter.api.Assertions.assertFalse(pr.isOk());
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-payload_invalid_json", pr.getWhy().reason());

        // resource rejected before orchestrator/transport
        org.junit.jupiter.api.Assertions.assertEquals(0, fake.calls);
    }


    //--------------------------------------------------
    static class FakeHttpPoster implements HttpPoster {
        String lastEndpoint;
        String lastJsonBody;
        int calls;

        private final int status;

        FakeHttpPoster(int status) {
            this.status = status;
        }

        @Override
        public int post(String endpoint, String jsonBody) {
            this.calls++;
            this.lastEndpoint = endpoint;
            this.lastJsonBody = jsonBody;

            Console.log("fakePoster_post_endpoint", endpoint);
            Console.log("fakePoster_post_body", jsonBody);

            return status;
        }
    }


}
