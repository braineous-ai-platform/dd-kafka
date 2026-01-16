package io.braineous.dd.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.braineous.dd.dlq.persistence.MongoDLQStore;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api/dlq")
@Produces(MediaType.APPLICATION_JSON)
public class DLQResource {

    @Inject
    MongoDLQStore store;

    // -------------------- SYSTEM --------------------

    @GET
    @Path("/system")
    public Response systemByTimeWindow(
            @QueryParam("fromTime") String fromTime,
            @QueryParam("toTime") String toTime
    ) {
        if (isBlank(fromTime) || isBlank(toTime)) {
            return bad("fromTime and toTime are required (ISO-8601)");
        }

        JsonArray out = store.findSystemFailureByTimeWindow(fromTime, toTime);
        return Response.ok(out.toString()).build();
    }

    @GET
    @Path("/system/by-id")
    public Response systemById(@QueryParam("dlqId") String dlqId) {
        if (isBlank(dlqId)) {
            return bad("dlqId is required");
        }

        JsonObject out = store.findSystemFailureById(dlqId);
        return Response.ok(out.toString()).build();
    }

    // -------------------- DOMAIN --------------------

    @GET
    @Path("/domain")
    public Response domainByTimeWindow(
            @QueryParam("fromTime") String fromTime,
            @QueryParam("toTime") String toTime
    ) {
        if (isBlank(fromTime) || isBlank(toTime)) {
            return bad("fromTime and toTime are required (ISO-8601)");
        }

        JsonArray out = store.findDomainFailureByTimeWindow(fromTime, toTime);
        return Response.ok(out.toString()).build();
    }

    @GET
    @Path("/domain/by-id")
    public Response domainById(@QueryParam("dlqId") String dlqId) {
        if (isBlank(dlqId)) {
            return bad("dlqId is required");
        }

        JsonObject out = store.findDomainFailureById(dlqId);
        return Response.ok(out.toString()).build();
    }

    // -------------------- helpers --------------------

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static Response bad(String msg) {
        JsonObject jo = new JsonObject();
        jo.addProperty("ok", false);
        jo.addProperty("error", msg);
        return Response.status(400).entity(jo.toString()).build();
    }
}
