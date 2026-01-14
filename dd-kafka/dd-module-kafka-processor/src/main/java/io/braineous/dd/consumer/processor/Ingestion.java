package io.braineous.dd.consumer.processor;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.consumer.service.DDEventOrchestrator;
import io.braineous.dd.core.model.CaptureStore;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;


/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class Ingestion {

    @Inject
    private DDEventOrchestrator orchestrator;


    @Incoming("ingestion")
    public void process(String ingestionJson) {
        CaptureStore store = CaptureStore.getInstance();

        if(ingestionJson == null || ingestionJson.trim().length() == 0){
            return;
        }

        JsonObject ingestion = JsonParser.parseString(ingestionJson).getAsJsonObject();
        if(ingestion == null){
            return;
        }

        JsonObject view = ingestion.get("view").getAsJsonObject();

        IngestionReceipt receipt = this.orchestrator.orchestrate(ingestionJson);


        //for IT tests
        store.add(ingestionJson);

        Console.log("dd_event_ingestion", ingestionJson);
        store.setSnapshot(view);
    }

}
