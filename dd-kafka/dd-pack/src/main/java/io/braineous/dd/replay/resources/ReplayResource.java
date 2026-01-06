package io.braineous.dd.replay.resources;

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
    public ReplayResult replayByTimeWindow(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return ReplayResult.fail("DD-CONFIG-replay_disabled");

        return service.replayByTimeWindow(request);
    }

    @POST
    @Path("/time-object-key")
    public ReplayResult replayByTimeObjectKey(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return ReplayResult.fail("DD-CONFIG-replay_disabled");

        return service.replayByTimeObjectKey(request);
    }

    @POST
    @Path("/domain-dlq-id")
    public ReplayResult replayByDomainDlqId(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return ReplayResult.fail("DD-CONFIG-replay_disabled");

        return service.replayByDomainDlqId(request);
    }

    @POST
    @Path("/system-dlq-id")
    public ReplayResult replayBySystemDlqId(ReplayRequest request){
        if(!gate.on("dd.feature.replay.enabled"))
            return ReplayResult.fail("DD-CONFIG-replay_disabled");

        return service.replayBySystemDlqId(request);
    }
}


