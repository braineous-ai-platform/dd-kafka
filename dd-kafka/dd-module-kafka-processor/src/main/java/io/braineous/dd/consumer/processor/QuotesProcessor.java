package io.braineous.dd.consumer.processor;

import io.braineous.dd.consumer.model.Quote;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.Random;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class QuotesProcessor {

    private Random random = new Random();

    @Incoming("requests")
    @Outgoing("quotes")
    @Blocking
    public Quote process(String quoteRequest) throws InterruptedException {
        try {
            // simulate some hard working task
            System.out.println("__________________");
            System.out.println(quoteRequest);
            System.out.println("__________________");


            System.out.println("WAIT_STARTED___");
            Thread.sleep(2000);
            System.out.println("WAIT_ENDED___");


            return new Quote(quoteRequest, random.nextInt(100));
        }finally{
            /*if(true){
                throw new RuntimeException("commit_understanding");
            }*/
        }
    }
}
