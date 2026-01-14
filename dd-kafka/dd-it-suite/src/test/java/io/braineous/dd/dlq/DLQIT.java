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

    @BeforeEach
    void setup() {
        CaptureStore.getInstance().clear();
    }

    @Test
    void domainFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();
        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("k", "v");

        orch.setHttpPoster(poster);
        orch.orchestrateDomainFailure(ddEventJson);

        assertEquals(1, CaptureStore.getInstance().sizeDomainFailure());
        assertNotNull(CaptureStore.getInstance().getDlqResult());

    }


    @Test
    void systemFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();
        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("sys", "boom");

        orch.setHttpPoster(poster);
        orch.orchestrateSystemFailure(new Exception("dlq_it_test"),
                ddEventJson.toString());

        assertEquals(1, CaptureStore.getInstance().sizeSystemFailure());
        assertNotNull(CaptureStore.getInstance().getDlqResult());

    }

    @Test
    void nullInput_does_nothing() {
        HttpPoster poster = new DLQHttpPoster();
        orch.setHttpPoster(poster);
        orch.orchestrateDomainFailure(null);

        assertEquals(0, CaptureStore.getInstance().sizeDomainFailure());
        assertNull(CaptureStore.getInstance().getDlqResult());
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

}
