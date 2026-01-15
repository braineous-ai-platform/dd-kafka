package io.braineous.dd.resources;

import ai.braineous.cgo.config.ConfigGate;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.services.ReplayService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api/replay")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ReplayResource {

    @Inject
    ReplayService service;

    @Inject
    ConfigGate gate;

    // test seams
    void setService(ReplayService svc) {
        this.service = svc;
    }

    void setGate(ConfigGate gate) {
        this.gate = gate;
    }

    // -------------------------------------------------------------------------
    // Endpoints
    // -------------------------------------------------------------------------

    @POST
    @Path("/time-window")
    public Response replayByTimeWindow(ReplayRequest request) {

        Response disabled = replayDisabledIfOff();
        if (disabled != null) return disabled;

        ReplayResult bad = validateTimeWindow(request);
        if (bad != null) return Response.status(Response.Status.BAD_REQUEST).entity(bad).build();

        // normalize before passing down (boring safety)
        try { request.normalize(); } catch (Exception ignored) { }

        ReplayResult out = service.replayByTimeWindow(request);
        return toHttp(out);
    }

    @POST
    @Path("/ingestion")
    public Response replayByIngestion(ReplayRequest request) {

        Response disabled = replayDisabledIfOff();
        if (disabled != null) return disabled;

        ReplayResult bad = validateIngestionId(request);
        if (bad != null) return Response.status(Response.Status.BAD_REQUEST).entity(bad).build();

        try { request.normalize(); } catch (Exception ignored) { }

        // same underlying behavior as prior "timeObjectKey", but selector is ingestionId now
        ReplayResult out = service.replayByTimeObjectKey(request);
        return toHttp(out);
    }

    @POST
    @Path("/dlq/domain")
    public Response replayByDomainDlq(ReplayRequest request) {

        Response disabled = replayDisabledIfOff();
        if (disabled != null) return disabled;

        ReplayResult bad = validateDlqId(request);
        if (bad != null) return Response.status(Response.Status.BAD_REQUEST).entity(bad).build();

        try { request.normalize(); } catch (Exception ignored) { }

        ReplayResult out = service.replayByDomainDlqId(request);
        return toHttp(out);
    }

    @POST
    @Path("/dlq/system")
    public Response replayBySystemDlq(ReplayRequest request) {

        Response disabled = replayDisabledIfOff();
        if (disabled != null) return disabled;

        ReplayResult bad = validateDlqId(request);
        if (bad != null) return Response.status(Response.Status.BAD_REQUEST).entity(bad).build();

        try { request.normalize(); } catch (Exception ignored) { }

        ReplayResult out = service.replayBySystemDlqId(request);
        return toHttp(out);
    }

    // -------------------------------------------------------------------------
    // Gate + HTTP mapping
    // -------------------------------------------------------------------------

    private Response replayDisabledIfOff() {
        if (!gate.on("dd.feature.replay.enabled")) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(ReplayResult.fail("DD-CONFIG-replay_disabled"))
                    .build();
        }
        return null;
    }

    private Response toHttp(ReplayResult r) {

        if (r == null) {
            return Response.serverError()
                    .entity(ReplayResult.fail("DD-REPLAY-null_result"))
                    .build();
        }

        if (r.ok()) {
            return Response.ok(r).build();
        }

        // Keep API boring: bad_request stays 400, everything else is 500.
        String reason = safeReason(r);
        if (reason != null && reason.startsWith("DD-REPLAY-bad_request-")) {
            return Response.status(Response.Status.BAD_REQUEST).entity(r).build();
        }

        return Response.serverError().entity(r).build();
    }

    private String safeReason(ReplayResult r) {
        try {
            return r.reason();
        } catch (Exception ignored) {
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Validators
    // -------------------------------------------------------------------------

    private ReplayResult validateCommon(ReplayRequest req) {
        if (req == null) return ReplayResult.badRequest("DD-REPLAY-bad_request-null");

        // stream removed from API contract: do NOT validate it anymore
        String reason = req.reason();
        if (reason == null || reason.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-reason_missing");

        return null; // ok
    }

    private ReplayResult validateTimeWindow(ReplayRequest req) {
        ReplayResult common = validateCommon(req);
        if (common != null) return common;

        String from = req.fromTime();
        String to = req.toTime();

        from = (from == null) ? null : from.trim();
        to = (to == null) ? null : to.trim();

        if (from == null || from.isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-fromTime_missing");
        if (to == null || to.isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-toTime_missing");

        // parse check at API surface (keeps store boring)
        try {
            java.time.Instant f = java.time.Instant.parse(from);
            java.time.Instant t = java.time.Instant.parse(to);
            if (!f.isBefore(t)) return ReplayResult.badRequest("DD-REPLAY-bad_request-window_invalid");
        } catch (Exception e) {
            return ReplayResult.badRequest("DD-REPLAY-bad_request-window_parse");
        }

        return null;
    }

    private ReplayResult validateIngestionId(ReplayRequest req) {
        ReplayResult common = validateCommon(req);
        if (common != null) return common;

        String id = req.ingestionId();
        if (id == null || id.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-ingestionId_missing");

        return null;
    }

    private ReplayResult validateDlqId(ReplayRequest req) {
        ReplayResult common = validateCommon(req);
        if (common != null) return common;

        String id = req.dlqId();
        if (id == null || id.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-dlqId_missing");

        return null;
    }
}



