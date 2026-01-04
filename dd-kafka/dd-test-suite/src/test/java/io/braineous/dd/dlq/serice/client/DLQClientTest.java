package io.braineous.dd.dlq.serice.client;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import io.braineous.dd.dlq.service.client.DLQClient;
import io.braineous.dd.dlq.service.client.DLQHttpPoster;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DLQClientTest {

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

