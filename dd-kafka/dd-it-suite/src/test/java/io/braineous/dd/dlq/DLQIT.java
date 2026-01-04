package io.braineous.dd.dlq;

import com.google.gson.JsonObject;

import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.dlq.service.DLQOrchestrator;

import io.braineous.dd.dlq.service.client.DLQHttpPoster;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class DLQIT {

    @BeforeEach
    void setup() {
        CaptureStore.getInstance().clear();
    }




    @Test
    void domainFailure_routes_and_records() {
        HttpPoster poster = new DLQHttpPoster();
        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("k", "v");

        DLQOrchestrator orch = DLQOrchestrator.getInstance();
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

        DLQOrchestrator orch = DLQOrchestrator.getInstance();
        orch.setHttpPoster(poster);
        orch.orchestrateSystemFailure(ddEventJson);

        assertEquals(1, CaptureStore.getInstance().sizeSystemFailure());
        assertNotNull(CaptureStore.getInstance().getDlqResult());

    }

    @Test
    void nullInput_does_nothing() {
        HttpPoster poster = new DLQHttpPoster();
        DLQOrchestrator orch = DLQOrchestrator.getInstance();
        orch.setHttpPoster(poster);
        orch.orchestrateDomainFailure(null);

        assertEquals(0, CaptureStore.getInstance().sizeDomainFailure());
        assertNull(CaptureStore.getInstance().getDlqResult());
    }

}
