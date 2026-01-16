package io.braineous.dd.dlq.service.client;

import com.google.gson.JsonObject;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;

public class DLQClient {
    private static final DLQClient client = new DLQClient();

    private DLQClient() {
    }

    public static DLQClient getInstance(){
        return client;
    }

    public DLQResult invoke(
            HttpPoster httpPoster,
            JsonSerializer serializer,
            String dlqEndpoint,
            JsonObject ddEventJson,
            Object ddEvent) {

        final String endpoint = dlqEndpoint; // keep raw; caller can pass "/dlq/publish" or "DLQEndpoints.publish"
        final long t0 = System.nanoTime();

        // ---- null/blank guards (no throw; return fail) ----
        if (httpPoster == null) {
            return DLQResult.fail(ddEventJson, endpoint, null, null,
                    new Why("DD-REST-missing_httpPoster", "httpPoster is null"));
        }
        if (serializer == null) {
            return DLQResult.fail(ddEventJson, endpoint, null, null,
                    new Why("DD-REST-missing_serializer", "serializer is null"));
        }
        if (endpoint == null || endpoint.isBlank()) {
            return DLQResult.fail(ddEventJson, endpoint, null, null,
                    new Why("DD-REST-missing_endpoint", "ingestionEndpoint is null/blank"));
        }
        if (ddEventJson == null) {
            // keep your earlier semantics: fail even before call
            return DLQResult.fail(endpoint, null, null,
                    new Why("DD-REST-null_ddEventJson", "ddEventJson is null"));
        }
        if (ddEvent == null) {
            return DLQResult.fail(ddEventJson, endpoint, null, null,
                    new Why("DD-REST-null_event", "ddEvent is null"));
        }

        try {
            String body = serializer.toJson(ddEvent);

            int status = httpPoster.post(endpoint, body);

            long durationMs = (System.nanoTime() - t0) / 1_000_000L;

            if (status >= 200 && status < 300) {
                return DLQResult.ok(ddEventJson, endpoint, status, durationMs);
            }

            return DLQResult.fail(ddEventJson, endpoint, status, durationMs,
                    new Why("DD-REST-non_2xx", "HTTP status " + status));

        } catch (Exception e) {
            e.printStackTrace();

            long durationMs = (System.nanoTime() - t0) / 1_000_000L;

            String msg = (e.getMessage() != null && !e.getMessage().isBlank())
                    ? e.getMessage()
                    : e.getClass().getSimpleName();

            return DLQResult.fail(ddEventJson, endpoint, null, durationMs,
                    new Why("DD-REST-call_failed", msg));
        }
    }




}
