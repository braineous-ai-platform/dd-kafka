package io.braineous.dd.replay.services;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.ReplayStore;
import jakarta.inject.Inject;

public class ReplayService {

    @Inject
    private ReplayStore store;

    //To facilate unit tests with an in-memory store. System store will be based on MongoDB
    void setStore(ReplayStore store){
        this.store = store;
    }

    public ReplayResult replayByTimeWindow(ReplayRequest request){
        if (request == null) return ReplayResult.badRequest("request_null");
        if (store == null) return ReplayResult.badRequest("store_null");

        java.util.List<ReplayEvent> events = store.findByTimeWindow(request);
        return replayEvents(events, request);
    }

    public ReplayResult replayByTimeObjectKey(ReplayRequest request){
        if (request == null) return ReplayResult.badRequest("request_null");
        if (store == null) return ReplayResult.badRequest("store_null");

        java.util.List<ReplayEvent> events = store.findByTimeObjectKey(request);
        return replayEvents(events, request);
    }



    public ReplayResult replayByDomainDlqId(ReplayRequest request){
        if (request == null) return ReplayResult.badRequest("request_null");
        if (store == null) return ReplayResult.badRequest("store_null");

        java.util.List<ReplayEvent> events = store.findByDomainDlqId(request);
        return replayEvents(events, request);
    }

    public ReplayResult replayBySystemDlqId(ReplayRequest request){
        if (request == null) return ReplayResult.badRequest("request_null");
        if (store == null) return ReplayResult.badRequest("store_null");

        java.util.List<ReplayEvent> events = store.findBySystemDlqId(request);
        return replayEvents(events, request);
    }

    //---------helper-----------------------------------------------
    ReplayResult replayEvents(java.util.List<ReplayEvent> events, ReplayRequest request) {
        if (events == null || events.isEmpty()) {
            return ReplayResult.empty(request);
        }

        // deterministic ordering (timestamp, then stable tie-breaker)
        events.sort((a, b) -> {
            int c = a.timestamp().compareTo(b.timestamp());
            if (c != 0) return c;
            return a.id().compareTo(b.id());
        });

        int limit = request.limitOrDefault(500);
        int replayed = 0;

        for (ReplayEvent e : events) {
            if (replayed >= limit) break;

            try {
                JsonObject payloadJson = JsonParser.parseString(e.payload()).getAsJsonObject();
                orchestrate(payloadJson);
                replayed++;
            } catch (Exception ex) {
                Console.log("replay_bad_payload_skip", e.payload());
                Console.log("replay_bad_payload_err", ex.getClass().getSimpleName());
                // skip and continue
            }
        }

        return ReplayResult.ok(request, replayed, events.size());
    }

    void orchestrate(JsonObject payloadJson){
        ProcessorOrchestrator.getInstance().orchestrate(payloadJson);
    }

}
