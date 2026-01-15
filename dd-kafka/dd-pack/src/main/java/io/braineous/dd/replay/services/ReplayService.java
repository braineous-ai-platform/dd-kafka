package io.braineous.dd.replay.services;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.persistence.ReplayStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ReplayService {

    @Inject
    private ReplayStore store;

    @Inject
    private ProcessorOrchestrator processorOrchestrator;

    //To facilate unit tests with an in-memory store. System store will be based on MongoDB
    public void setStore(ReplayStore store){
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

        java.util.List<ReplayEvent> list = new java.util.ArrayList<ReplayEvent>(events);

        // deterministic ordering: timestamp (nulls last) then id (nulls last) then payload hash tie-breaker
        java.util.Collections.sort(list, new java.util.Comparator<ReplayEvent>() {
            @Override
            public int compare(ReplayEvent a, ReplayEvent b) {

                // null safety (should not happen, but deterministic anyway)
                if (a == null && b == null) return 0;
                if (a == null) return 1;
                if (b == null) return -1;

                java.time.Instant ta = a.timestamp();
                java.time.Instant tb = b.timestamp();

                if (ta == null && tb != null) return 1;
                if (ta != null && tb == null) return -1;
                if (ta != null && tb != null) {
                    int c = ta.compareTo(tb);
                    if (c != 0) return c;
                }

                String ia = a.id();
                String ib = b.id();

                if (ia == null && ib != null) return 1;
                if (ia != null && ib == null) return -1;
                if (ia != null && ib != null) {
                    int c2 = ia.compareTo(ib);
                    if (c2 != 0) return c2;
                }

                // final deterministic tie-breaker: payload string compare (or empty)
                String pa = a.payload();
                String pb = b.payload();

                if (pa == null) pa = "";
                if (pb == null) pb = "";

                return pa.compareTo(pb);
            }
        });

        int attempted = 0;

        for (int i = 0; i < list.size(); i++) {
            ReplayEvent e = list.get(i);
            if (e == null) {
                Console.log("replay_null_event_skip", Integer.valueOf(i));
                continue;
            }

            try {
                String payload = e.payload();
                if (payload == null || payload.trim().isEmpty()) {
                    Console.log("replay_bad_payload_skip", payload);
                    Console.log("replay_bad_payload_err", "payload_blank");
                    continue;
                }

                JsonObject payloadJson = JsonParser.parseString(payload).getAsJsonObject();

                // handoff only; anchoring doctrine lives in ProcessorOrchestrator
                orchestrate(payloadJson);

                attempted++;

            } catch (Exception ex) {
                Console.log("replay_bad_payload_skip", e.payload());
                Console.log("replay_bad_payload_err", ex.getClass().getSimpleName());
                // skip and continue
            }
        }

        // NOTE: attempted == how many we handed to orchestrator without throwing here
        return ReplayResult.ok(request, attempted, list.size());
    }


    void orchestrate(JsonObject payloadJson){

        this.processorOrchestrator.orchestrate(payloadJson);
    }

}
