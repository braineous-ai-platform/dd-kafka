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

@Path("/api/replay")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ReplayResource {

    @Inject
    ReplayService service;

    @Inject
    ConfigGate gate;

    // test seam (same idea as ReplayService.setStore)
    void setService(ReplayService svc){
        this.service = svc;
    }

    void setGate(ConfigGate gate){
        this.gate = gate;
    }

    @POST
    @Path("/time-window")
    public jakarta.ws.rs.core.Response replayByTimeWindow(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(ReplayResult.fail("DD-CONFIG-replay_disabled"))
                    .build();

        ReplayResult bad = validateTimeWindow(request);
        if (bad != null)
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(bad)
                    .build();

        return jakarta.ws.rs.core.Response.ok(service.replayByTimeWindow(request)).build();
    }

    @POST
    @Path("/time-object-key")
    public jakarta.ws.rs.core.Response replayByTimeObjectKey(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(ReplayResult.fail("DD-CONFIG-replay_disabled"))
                    .build();

        ReplayResult bad = validateObjectKey(request);
        if (bad != null)
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(bad)
                    .build();

        return jakarta.ws.rs.core.Response.ok(service.replayByTimeObjectKey(request)).build();
    }

    @POST
    @Path("/domain-dlq-id")
    public jakarta.ws.rs.core.Response replayByDomainDlqId(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(ReplayResult.fail("DD-CONFIG-replay_disabled"))
                    .build();

        ReplayResult bad = validateDlqId(request);
        if (bad != null)
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(bad)
                    .build();

        return jakarta.ws.rs.core.Response.ok(service.replayByDomainDlqId(request)).build();
    }

    @POST
    @Path("/system-dlq-id")
    public jakarta.ws.rs.core.Response replayBySystemDlqId(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(ReplayResult.fail("DD-CONFIG-replay_disabled"))
                    .build();

        ReplayResult bad = validateDlqId(request);
        if (bad != null)
            return jakarta.ws.rs.core.Response.status(jakarta.ws.rs.core.Response.Status.BAD_REQUEST)
                    .entity(bad)
                    .build();

        return jakarta.ws.rs.core.Response.ok(service.replayBySystemDlqId(request)).build();
    }

    //-------Validators --------------------------
    private ReplayResult validateCommon(ReplayRequest req) {
        if (req == null) return ReplayResult.badRequest("DD-REPLAY-bad_request-null");

        String stream = req.stream();
        String reason = req.reason();

        if (stream == null || stream.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-stream_missing");

        if (reason == null || reason.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-reason_missing");

        return null; // ok
    }

    private ReplayResult validateTimeWindow(ReplayRequest req) {
        ReplayResult common = validateCommon(req);
        if (common != null) return common;

        String from = req.fromTime();
        String to   = req.toTime();

        if (from == null || from.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-fromTime_missing");
        if (to == null || to.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-toTime_missing");

        // optional: parse check at API surface (keeps store boring)
        try {
            java.time.Instant f = java.time.Instant.parse(from);
            java.time.Instant t = java.time.Instant.parse(to);
            if (!f.isBefore(t)) return ReplayResult.badRequest("DD-REPLAY-bad_request-window_invalid");
        } catch (Exception e) {
            return ReplayResult.badRequest("DD-REPLAY-bad_request-window_parse");
        }

        return null;
    }

    private ReplayResult validateObjectKey(ReplayRequest req) {
        ReplayResult common = validateCommon(req);
        if (common != null) return common;

        String key = req.objectKey();
        if (key == null || key.trim().isEmpty())
            return ReplayResult.badRequest("DD-REPLAY-bad_request-objectKey_missing");

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


