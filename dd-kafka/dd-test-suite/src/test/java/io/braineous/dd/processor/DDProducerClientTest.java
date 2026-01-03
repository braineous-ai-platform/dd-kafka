package io.braineous.dd.processor;

import com.google.gson.JsonObject;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DDProducerClientTest {

    @Test
    void invoke_should_fail_when_httpPoster_is_null() {

        // --- setup ---
        HttpPoster httpPoster = null;

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-1 result = " + result);
        System.out.println("Ball-1 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-missing_httpPoster", result.getWhy().getReason());
    }

    @Test
    void invoke_should_fail_when_serializer_is_null() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                return 200; // should not be called
            }
        };

        JsonSerializer serializer = null;

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-2 result = " + result);
        System.out.println("Ball-2 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-missing_serializer", result.getWhy().getReason());
    }

    @Test
    void invoke_should_fail_when_ingestionEndpoint_is_blank() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                return 200; // should not be called
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{}";
            }
        };

        String ingestionEndpoint = "   "; // blank

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-3 result = " + result);
        System.out.println("Ball-3 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-missing_endpoint", result.getWhy().getReason());
    }

    @Test
    void invoke_should_fail_when_ddEvent_is_null() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                return 200; // must not be called
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = null; // key condition

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-4 result = " + result);
        System.out.println("Ball-4 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-null_event", result.getWhy().getReason());
    }

    @Test
    void invoke_should_succeed_when_http_returns_200() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                // --- Console.log ---
                System.out.println("Ball-5 httpPoster.post endpoint=" + endpoint);
                System.out.println("Ball-5 httpPoster.post body=" + body);
                return 200;
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{\"hello\":\"world\"}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-5 result = " + result);
        System.out.println("Ball-5 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertTrue(result.isOk());

        // success must not carry why
        assertNull(result.getWhy());
    }

    @Test
    void invoke_should_fail_when_http_returns_500() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                System.out.println("Ball-6 httpPoster.post endpoint=" + endpoint);
                System.out.println("Ball-6 httpPoster.post body=" + body);
                return 500;
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{\"hello\":\"world\"}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-6 result = " + result);
        System.out.println("Ball-6 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-non_2xx", result.getWhy().getReason());
        assertTrue(result.getWhy().getDetails().contains("500"));
    }

    @Test
    void invoke_should_fail_when_serializer_throws_exception() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                return 200; // should not be called because serializer fails first
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                throw new RuntimeException("boom_from_serializer");
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-7 result = " + result);
        System.out.println("Ball-7 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-call_failed", result.getWhy().getReason());
        assertTrue(result.getWhy().getDetails().contains("boom_from_serializer"));
    }

    @Test
    void invoke_should_fail_when_httpPoster_throws_exception() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                throw new RuntimeException("boom_from_httpPoster");
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{\"hello\":\"world\"}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-8 result = " + result);
        System.out.println("Ball-8 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-call_failed", result.getWhy().getReason());
        assertTrue(result.getWhy().getDetails().contains("boom_from_httpPoster"));
    }

    @Test
    void invoke_should_fail_when_http_returns_404() {

        // --- setup ---
        HttpPoster httpPoster = new HttpPoster() {
            @Override
            public int post(String endpoint, String body) {
                return 404;
            }
        };

        JsonSerializer serializer = new JsonSerializer() {
            @Override
            public String toJson(Object o) {
                return "{\"hello\":\"world\"}";
            }
        };

        String ingestionEndpoint = "http://localhost/ingestion";

        JsonObject ddEventJson = new JsonObject();
        ddEventJson.addProperty("test", "value");

        Object ddEvent = ddEventJson;

        // --- execute ---
        ProcessorResult result = DDProducerClient.getInstance()
                .invoke(httpPoster, serializer, ingestionEndpoint, ddEventJson, ddEvent);

        // --- Console.log ---
        System.out.println("Ball-9 result = " + result);
        System.out.println("Ball-9 why    = " + result.getWhy());

        // --- assert ---
        assertNotNull(result);
        assertFalse(result.isOk());

        assertNotNull(result.getWhy());
        assertEquals("DD-REST-non_2xx", result.getWhy().getReason());
        assertTrue(result.getWhy().getDetails().contains("404"));
    }


}
