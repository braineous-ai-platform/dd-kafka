package io.braineous.dd.consumer.processor;

import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import com.google.gson.JsonObject;
import io.braineous.dd.core.model.CaptureStore;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@Path("/debug")
public class DebugResource {

    @GET
    @Path("/captures/size")
    @Produces(MediaType.TEXT_PLAIN)
    public int captureSize() {
        CaptureStore store = CaptureStore.getInstance();
        return store.size();
    }

    @POST
    @Path("/captures/clear")
    public void clearCaptures() {
        CaptureStore store = CaptureStore.getInstance();
        store.clear();
    }


    @GET
    @Path("/graph")
    @Produces(MediaType.APPLICATION_JSON)
    public String graph() {
        CaptureStore store = CaptureStore.getInstance();
        JsonObject view = store.getSnapshot();
        if (view == null) {
            return "{\"status\":\"EMPTY\"}";
        }
        // Prefer: if GraphSnapshot has toJson()
        return view.toString();

        // fallback: minimal
        //return "{\"status\":\"OK\"}";
    }


}
