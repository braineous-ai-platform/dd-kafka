package io.braineous.dd.dlq.service;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.core.processor.GsonJsonSerializer;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import io.braineous.dd.dlq.service.client.DLQClient;
import io.braineous.dd.dlq.service.client.DLQHttpPoster;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DLQOrchestrator {
    private static final String DOMAIN_ENDPOINT = "/domain_failure";
    private static final String SYSTEM_ENDPOINT = "/system_failure";


    private HttpPoster httpPoster = new DLQHttpPoster();
    private final JsonSerializer serializer = new GsonJsonSerializer();


    public DLQOrchestrator() {
    }


    public void setHttpPoster(HttpPoster httpPoster) {
        this.httpPoster = httpPoster;
    }

    public void orchestrateDomainFailure(JsonObject ddEventJson) {
        if(ddEventJson == null){
            return;
        }

        if (httpPoster == null || serializer == null) {
            return;
        }


        CaptureStore store = CaptureStore.getInstance();
        String endpoint = DOMAIN_ENDPOINT;

        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEventJson);

        //for IT Test
        store.addDomainFailure(ddEventJson.toString());
        store.setDlqResult(result);
    }

    //----------------------------------------------------------------------------------------------
    public DLQResult orchestrateSystemFailure(Exception exception, String ingestionStr){
        try {
            if (exception == null) {
                return null;
            }

            if (ingestionStr == null || ingestionStr.trim().length() == 0) {
                return null;
            }

            JsonElement dlqEventElement = JsonParser.parseString(ingestionStr);

            JsonObject dlqEvent;
            if (dlqEventElement.isJsonArray()) {
                JsonArray arr = dlqEventElement.getAsJsonArray();
                if (arr == null || arr.size() == 0) {
                    throw new IllegalArgumentException("DLQ-ING-events_empty");
                }
                dlqEvent = arr.get(0).getAsJsonObject();
            } else if (dlqEventElement.isJsonObject()) {
                dlqEvent = dlqEventElement.getAsJsonObject();
            } else {
                throw new IllegalArgumentException("DLQ-ING-events_not_object");
            }

            String message = exception.getMessage();
            if (message == null || message.trim().length() == 0) {
                message = exception.getClass().getName();
            }

            JsonObject out = dlqEvent.deepCopy();
            out.addProperty("dlqSystemCode", "DD-DLQ-SYSTEM-EXCEPTION");
            out.addProperty("dlqSystemException", message);


            DLQResult result = this.orchestrateSystemFailure(out);

            return result;
        }catch(Exception e){
            Console.log("error_processing_dlq_system_failure", String.valueOf(e));
            return null;
        }
    }

    private DLQResult orchestrateSystemFailure(JsonObject ddEventJson) {
        if(ddEventJson == null){
            return null;
        }

        if (httpPoster == null || serializer == null) {
            return null;
        }

        CaptureStore store = CaptureStore.getInstance();
        String endpoint = SYSTEM_ENDPOINT;

        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEventJson);

        //for IT Test
        store.addSystemFailure(ddEventJson.toString());
        store.setDlqResult(result);

        return result;
    }
}
