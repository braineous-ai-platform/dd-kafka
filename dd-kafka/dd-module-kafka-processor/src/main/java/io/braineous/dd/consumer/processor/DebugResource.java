package io.braineous.dd.consumer.processor;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@Path("/debug")
public class DebugResource {

    @GET
    @Path("/captures/size")
    @Produces(MediaType.TEXT_PLAIN)
    public int captureSize() {
        return Ingestion.getStore().size();
    }

    @POST
    @Path("/captures/clear")
    public void clearCaptures() {
        Ingestion.getStore().clear();
    }

}
