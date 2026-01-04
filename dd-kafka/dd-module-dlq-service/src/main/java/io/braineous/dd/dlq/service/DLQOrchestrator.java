package io.braineous.dd.dlq.service;

import com.google.gson.JsonObject;
import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.core.processor.GsonJsonSerializer;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.dlq.model.DLQResult;
import io.braineous.dd.dlq.service.client.DLQClient;

public class DLQOrchestrator {
    private static final String DOMAIN_ENDPOINT = "/domain_failure";
    private static final String SYSTEM_ENDPOINT = "/system_failure";


    private static final DLQOrchestrator orchestrator = new DLQOrchestrator();

    private HttpPoster httpPoster;
    private final JsonSerializer serializer = new GsonJsonSerializer();


    private DLQOrchestrator() {
    }

    public static DLQOrchestrator getInstance(){
        return orchestrator;
    }

    public void setHttpPoster(HttpPoster httpPoster) {
        if(httpPoster == null || this.httpPoster != null){
            return;
        }
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

    public void orchestrateSystemFailure(JsonObject ddEventJson) {
        if(ddEventJson == null){
            return;
        }

        if (httpPoster == null || serializer == null) {
            return;
        }

        CaptureStore store = CaptureStore.getInstance();
        String endpoint = SYSTEM_ENDPOINT;

        DLQResult result = DLQClient.getInstance()
                .invoke(httpPoster, serializer, endpoint, ddEventJson, ddEventJson);

        //for IT Test
        store.addSystemFailure(ddEventJson.toString());
        store.setDlqResult(result);
    }
}
