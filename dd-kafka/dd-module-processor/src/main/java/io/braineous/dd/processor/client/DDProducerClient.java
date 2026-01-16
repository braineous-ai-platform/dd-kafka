package io.braineous.dd.processor.client;

import com.google.gson.JsonObject;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.processor.ProcessorResult;

public class DDProducerClient {
    private static final DDProducerClient client = new DDProducerClient();

    private DDProducerClient() {
    }

    public static DDProducerClient getInstance(){
        return client;
    }

    public ProcessorResult invoke(
            HttpPoster httpPoster,
            JsonSerializer serializer,
            String ingestionEndpoint,
            JsonObject ddEventJson,
            Object ddEvent) {

        if (httpPoster == null) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-REST-missing_httpPoster", "httpPoster is null"));
        }
        if (serializer == null) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-REST-missing_serializer", "serializer is null"));
        }
        if (ingestionEndpoint == null || ingestionEndpoint.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-REST-missing_endpoint", "ingestionEndpoint is null/blank"));
        }
        if (ddEventJson == null) {
            return ProcessorResult.fail(new Why("DD-REST-null_ddEventJson", "ddEventJson is null"));
        }

        if (ddEvent == null) {
            return ProcessorResult.fail(new Why("DD-REST-null_event", "ddEvent is null"));
        }

        try {
            String body = serializer.toJson(ddEvent);
            int status = httpPoster.post(ingestionEndpoint, body);

            if (status >= 200 && status < 300) {
                return ProcessorResult.ok(ddEventJson);
            }

            return ProcessorResult.fail(ddEventJson,
                    new Why("DD-REST-non_2xx", "HTTP status " + status));

        } catch (Exception e) {
            e.printStackTrace();

            String msg = (e.getMessage() != null && !e.getMessage().isBlank())
                    ? e.getMessage()
                    : e.getClass().getSimpleName();

            return ProcessorResult.fail(ddEventJson, new Why("DD-REST-call_failed", msg));
        }
    }



}
