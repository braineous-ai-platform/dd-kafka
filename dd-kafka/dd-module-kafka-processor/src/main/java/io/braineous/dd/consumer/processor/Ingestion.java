package io.braineous.dd.consumer.processor;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class Ingestion {

    static CaptureStore store = new CaptureStore();

    static CaptureStore getStore(){
        return store;
    }


    @Incoming("ingestion")
    public void process(String ingestionJson) {
        Ingestion.getStore().add(ingestionJson);
        System.out.println("INGESTION: " + ingestionJson);
    }

}
