package io.braineous.dd.resources;


import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.ingestion.persistence.IngestionStore;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.processor.ProcessorResult;
import io.braineous.dd.replay.model.IngestionRequest;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IngestionResource {

    @Inject
    private ProcessorOrchestrator orch;

    @Inject
    private IngestionStore store;

    //test seam
    void setHttpPoster(HttpPoster poster){
        this.orch.setHttpPoster(poster);
    }

    @POST
    @Path("/ingestion")
    public jakarta.ws.rs.core.Response ingestion(IngestionRequest request) {

        // ---- fail-fast: request ----
        if (request == null) {
            JsonObject out = new JsonObject();
            out.addProperty("ok", false);
            out.add("data", com.google.gson.JsonNull.INSTANCE);

            JsonObject why = new JsonObject();
            why.addProperty("code", "DD-INGEST-request_null");
            why.addProperty("msg", "request cannot be null");
            out.add("why", why);

            return jakarta.ws.rs.core.Response.status(400).entity(out.toString()).build();
        }

        String payload = request.getPayload();
        if (payload == null || payload.trim().isEmpty()) {
            JsonObject out = new JsonObject();
            out.addProperty("ok", false);
            out.add("data", com.google.gson.JsonNull.INSTANCE);

            JsonObject why = new JsonObject();
            why.addProperty("code", "DD-INGEST-payload_blank");
            why.addProperty("msg", "payload cannot be blank");
            out.add("why", why);

            return jakarta.ws.rs.core.Response.status(400).entity(out.toString()).build();
        }

        // ---- parse guard ----
        final JsonObject payloadJson;
        try {
            payloadJson = JsonParser.parseString(payload).getAsJsonObject();
        } catch (Exception e) {
            JsonObject out = new JsonObject();
            out.addProperty("ok", false);
            out.add("data", com.google.gson.JsonNull.INSTANCE);

            JsonObject why = new JsonObject();
            why.addProperty("code", "DD-INGEST-payload_invalid_json");
            why.addProperty("msg", "payload must be a JSON object");
            out.add("why", why);

            return jakarta.ws.rs.core.Response.status(400).entity(out.toString()).build();
        }

        // ---- orchestrate ----
        ProcessorResult pr = this.orch.orchestrate(payloadJson);

        // ---- map ProcessorResult -> Envelope A ----
        JsonObject data = new JsonObject();

        boolean businessOk = false;

        if (pr != null) {
            businessOk = pr.isOk();

            if (pr.getId() != null) {
                data.addProperty("processorResultId", pr.getId());
            }

            data.addProperty("ok", pr.isOk());

            if (pr.getIngestionId() != null) {
                data.addProperty("ingestionId", pr.getIngestionId());
            }

            if (pr.getDdEventJson() != null) {
                data.add("ddEventJson", pr.getDdEventJson());
            }

            if (pr.getWhy() != null) {
                JsonObject prWhy = new JsonObject();

                String code = pr.getWhy().reason();
                if (code != null) {
                    prWhy.addProperty("code", code);
                }

                String msg = pr.getWhy().getDetails();
                if (msg != null) {
                    prWhy.addProperty("msg", msg);
                }

                data.add("processorWhy", prWhy);
            } else {
                data.add("processorWhy", com.google.gson.JsonNull.INSTANCE);
            }
        } else {
            // orchestrator returned null → deterministic failure
            businessOk = false;

            data.addProperty("ok", false);

            JsonObject prWhy = new JsonObject();
            prWhy.addProperty("code", "DD-INGEST-orchestrate_null");
            prWhy.addProperty("msg", "orchestrator returned null ProcessorResult");
            data.add("processorWhy", prWhy);
        }

        JsonObject out = new JsonObject();
        out.addProperty("ok", businessOk);
        out.add("data", data);
        out.add("why", com.google.gson.JsonNull.INSTANCE);

        int status = businessOk ? 200 : 400;

        return jakarta.ws.rs.core.Response
                .status(status)
                .entity(out.toString())
                .build();
    }


    @Path("/ingestion/events/time-window")
    @GET
    @Produces("application/json")
    public jakarta.ws.rs.core.Response findEventsByTimeWindow(
            @QueryParam("fromTime") String fromTime,
            @QueryParam("toTime") String toTime
    ) {
        // ---- fail-fast: params ----
        if (fromTime == null || fromTime.trim().isEmpty()) {
            return badRequest("DD-INGEST-fromTime_blank", "fromTime cannot be blank");
        }
        if (toTime == null || toTime.trim().isEmpty()) {
            return badRequest("DD-INGEST-toTime_blank", "toTime cannot be blank");
        }

        // ---- validate: ISO instants + window ----
        java.time.Instant from;
        java.time.Instant to;
        try {
            from = java.time.Instant.parse(fromTime.trim());
            to = java.time.Instant.parse(toTime.trim());
        } catch (Exception e) {
            return badRequest("DD-INGEST-time_invalid", "fromTime/toTime must be ISO-8601 instants (e.g. 2026-01-15T00:00:00Z)");
        }

        if (!from.isBefore(to)) {
            return badRequest("DD-INGEST-time_window_invalid", "fromTime must be < toTime");
        }

        // ---- orchestrate/store ----
        com.google.gson.JsonArray events = store.findEventsByTimeWindow(fromTime.trim(), toTime.trim());

        com.google.gson.JsonObject data = new com.google.gson.JsonObject();
        data.addProperty("fromTime", fromTime.trim());
        data.addProperty("toTime", toTime.trim());
        data.addProperty("count", events == null ? 0 : events.size());
        data.add("events", events == null ? new com.google.gson.JsonArray() : events);

        com.google.gson.JsonObject out = new com.google.gson.JsonObject();
        out.addProperty("ok", true);
        out.add("data", data);
        out.add("why", com.google.gson.JsonNull.INSTANCE);

        return jakarta.ws.rs.core.Response.status(200).entity(out.toString()).build();
    }


    @Path("/ingestion/events/{ingestionId}")
    @GET
    public Response findEventsByIngestionId(
            String ingestionId
    ){
        //-----fail-fast------------

        //--------validate--------

        //------orchestrate---------
        JsonObject event = store.findEventsByIngestionId(ingestionId);

        Response response = Response.
                status(200).
                entity(event.toString())
                .build();

        return response;
    }

    //---------------------------------------------------------------
    private static jakarta.ws.rs.core.Response badRequest(String code, String msg) {
        com.google.gson.JsonObject out = new com.google.gson.JsonObject();
        out.addProperty("ok", false);
        out.add("data", com.google.gson.JsonNull.INSTANCE);

        com.google.gson.JsonObject why = new com.google.gson.JsonObject();
        why.addProperty("code", code);
        why.addProperty("msg", msg);
        out.add("why", why);

        return jakarta.ws.rs.core.Response.status(400).entity(out.toString()).build();
    }
}


