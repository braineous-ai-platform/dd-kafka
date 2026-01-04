package io.braineous.dd.consumer.processor;

import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;
import io.braineous.dd.consumer.service.DDEventOrchestrator;
import io.braineous.dd.core.model.CaptureStore;
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

        //for IT tests
        store.add(ingestionJson);

        Console.log("dd_event_ingestion", ingestionJson);

        //orchestrate with CGO Graph
        GraphView view = this.orchestrator.orchestrate(ingestionJson);

        store.setSnapshot((GraphSnapshot) view);
    }

}
