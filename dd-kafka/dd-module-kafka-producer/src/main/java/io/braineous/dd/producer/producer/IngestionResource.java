package io.braineous.dd.producer.producer;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;


@Path("/api")
public class IngestionResource {

    @Inject
    @Channel("ingestion_out")
    Emitter<String> emitter;

    /**
     * Endpoint to generate a new quote request id and send it to "dd_cgo_ingestion_out" Kafka topic using the emitter.
     */
    @POST
    @Path("/ingestion")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String createRequest(
            String body
    ) {
        //TODO: guardrails

        System.out.println("____INGESTION_PRODUCER_____");
        System.out.println(body);
        System.out.println("___________________________");

        emitter.send(body);
        return body;
    }
}
