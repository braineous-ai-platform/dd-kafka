package io.braineous.dd.dlq;


import jakarta.enterprise.context.ApplicationScoped;



/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class DLQ {


   /* @Incoming("ingestion_in")
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
    }*/

}
