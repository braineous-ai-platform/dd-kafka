package io.braineous.dd.dlq.service.client;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DLQClientTest {

    @Test
    void invoke_non_2xx_defensive() {
        HttpPoster httpPoster = (endpoint, body) -> {
            Console.log("httpPoster.post", endpoint + " :: " + body);
            return 500;
        };

        JsonSerializer serializer = o -> {
            Console.log("serializer.toJson", o);
            return "{\"ok\":false}";
        };

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("id", "evt-2");

        Object ddEvent = new Object();
        String endpoint = "/domain_failure";

        // ---- act ----
        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEvent);

        // ---- assert ----
        Console.log("dlq.result", result);

        assertFalse(result.isOk(), "expected ok=false");
        assertNotNull(result.getWhy(), "why must be present on failure");
        assertEquals("DD-REST-non_2xx", result.getWhy().getReason());
        assertEquals(500, result.getHttpStatus());
        assertEquals(endpoint, result.getEndpoint());
        assertEquals(ddEventJson, result.getDdEventJson());
    }

    @Test
    void invoke_exception_defensive() {
        HttpPoster httpPoster = (endpoint, body) -> {
            Console.log("httpPoster.post", "throwing exception");
            throw new RuntimeException("boom");
        };

        JsonSerializer serializer = o -> {
            Console.log("serializer.toJson", o);
            return "{\"ok\":true}";
        };

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("id", "evt-3");

        Object ddEvent = new Object();
        String endpoint = "/domain_failure";

        // ---- act ----
        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEvent);

        // ---- assert ----
        Console.log("dlq.result", result);

        assertFalse(result.isOk(), "expected ok=false");
        assertNotNull(result.getWhy(), "why must be present on exception");
        assertEquals("DD-REST-call_failed", result.getWhy().getReason());
        assertNull(result.getHttpStatus(), "httpStatus must be null on exception");
        assertNotNull(result.getDurationMs(), "duration must be captured");
        assertEquals(endpoint, result.getEndpoint());
        assertEquals(ddEventJson, result.getDdEventJson());
    }

    @Test
    void invoke_null_guards_defensive() {
        HttpPoster httpPoster = (endpoint, body) -> {
            fail("httpPoster.post must NOT be called on guard failures");
            return 200;
        };

        JsonSerializer serializer = o -> {
            fail("serializer.toJson must NOT be called on guard failures");
            return "{}";
        };

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("id", "evt-guard");

        Object ddEvent = new Object();

        // ---- httpPoster null ----
        DLQResult r1 = DLQClient.getInstance()
                .invoke(null, serializer, "/domain_failure", ddEventJson, ddEvent);
        Console.log("guard.httpPoster.null", r1);
        assertFalse(r1.isOk());
        assertEquals("DD-REST-missing_httpPoster", r1.getWhy().getReason());

        // ---- serializer null ----
        DLQResult r2 = DLQClient.getInstance()
                .invoke(httpPoster, null, "/domain_failure", ddEventJson, ddEvent);
        Console.log("guard.serializer.null", r2);
        assertFalse(r2.isOk());
        assertEquals("DD-REST-missing_serializer", r2.getWhy().getReason());

        // ---- endpoint blank ----
        DLQResult r3 = DLQClient.getInstance()
                .invoke(httpPoster, serializer, "  ", ddEventJson, ddEvent);
        Console.log("guard.endpoint.blank", r3);
        assertFalse(r3.isOk());
        assertEquals("DD-REST-missing_endpoint", r3.getWhy().getReason());

        // ---- ddEventJson null ----
        DLQResult r4 = DLQClient.getInstance()
                .invoke(httpPoster, serializer, "/domain_failure", null, ddEvent);
        Console.log("guard.ddEventJson.null", r4);
        assertFalse(r4.isOk());
        assertEquals("DD-REST-null_ddEventJson", r4.getWhy().getReason());

        // ---- ddEvent null ----
        DLQResult r5 = DLQClient.getInstance()
                .invoke(httpPoster, serializer, "/domain_failure", ddEventJson, null);
        Console.log("guard.ddEvent.null", r5);
        assertFalse(r5.isOk());
        assertEquals("DD-REST-null_event", r5.getWhy().getReason());
    }


}

