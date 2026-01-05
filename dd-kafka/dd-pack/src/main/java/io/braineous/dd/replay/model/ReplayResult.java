package io.braineous.dd.replay.model;

import com.google.gson.JsonObject;
import io.braineous.dd.core.model.Why;

public class ReplayResult {

    private static final java.util.concurrent.atomic.AtomicLong EVENT_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    private static final String ID_PREFIX = "DD-DLQ-";

    private static final String WHY_CODE_NULL_EVENT = "DD-DLQ-FAIL-ddEventJson_null";
    private static final String WHY_MSG_NULL_EVENT  = "ddEventJson cannot be null for ok=true";

    private static final String WHY_CODE_MISSING_WHY = "DD-DLQ-FAIL-missing_why";
    private static final String WHY_MSG_MISSING_WHY  = "ok=false requires a non-null why";

    private final String id;

    // payload attempted/sent (may be null on failure)
    private final JsonObject ddEventJson;

    // client-call metadata (optional but useful)
    private final String endpoint;     // e.g. "/dlq/publish" or "DLQEndpoints.publish"
    private final Integer httpStatus;  // null if call never reached server
    private final Long durationMs;     // null if unknown

    private final boolean ok;
    private final Why why;

    public ReplayResult(JsonObject ddEventJson,
                        String endpoint,
                        Integer httpStatus,
                        Long durationMs,
                        boolean ok,
                        Why why) {

        this.id = nextEventId(nextSeq());

        if (ok) {
            if (ddEventJson == null) {
                // DLQ-friendly conversion: no throw
                this.ddEventJson = null;
                this.endpoint = endpoint;
                this.httpStatus = httpStatus;
                this.durationMs = durationMs;
                this.ok = false;
                this.why = new Why(WHY_CODE_NULL_EVENT, WHY_MSG_NULL_EVENT);
                return;
            }

            this.ddEventJson = ddEventJson;
            this.endpoint = endpoint;
            this.httpStatus = httpStatus;
            this.durationMs = durationMs;
            this.ok = true;
            this.why = null;
            return;
        }

        // ok == false
        this.ddEventJson = ddEventJson;     // allowed null
        this.endpoint = endpoint;
        this.httpStatus = httpStatus;
        this.durationMs = durationMs;
        this.ok = false;
        this.why = (why != null) ? why : new Why(WHY_CODE_MISSING_WHY, WHY_MSG_MISSING_WHY);
    }

    // ---------- factories (preferred) ----------

    public static ReplayResult ok(JsonObject ddEventJson,
                               String endpoint,
                               Integer httpStatus,
                               Long durationMs) {
        return new ReplayResult(ddEventJson, endpoint, httpStatus, durationMs, true, null);
    }

    public static ReplayResult fail(JsonObject ddEventJson,
                                 String endpoint,
                                 Integer httpStatus,
                                 Long durationMs,
                                 Why why) {
        return new ReplayResult(ddEventJson, endpoint, httpStatus, durationMs, false, why);
    }

    public static ReplayResult fail(String endpoint,
                                 Integer httpStatus,
                                 Long durationMs,
                                 Why why) {
        return new ReplayResult(null, endpoint, httpStatus, durationMs, false, why);
    }

    // ---------- getters ----------

    public String getId() { return id; }
    public JsonObject getDdEventJson() { return ddEventJson; }
    public String getEndpoint() { return endpoint; }
    public Integer getHttpStatus() { return httpStatus; }
    public Long getDurationMs() { return durationMs; }
    public boolean isOk() { return ok; }
    public Why getWhy() { return why; }

    // ---------- debug / wire ----------

    @Override
    public String toString() {
        return "DLQResult{" +
                "id='" + id + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", httpStatus=" + httpStatus +
                ", durationMs=" + durationMs +
                ", ddEventJson=" + ddEventJson +
                ", ok=" + ok +
                ", why=" + why +
                '}';
    }

    public String toJson() {
        return new com.google.gson.Gson().toJson(this);
    }

    // ---------- id helpers ----------

    private static long nextSeq() {
        return EVENT_SEQ.incrementAndGet();
    }

    private static String nextEventId(long seq) {
        return ID_PREFIX + seq;
    }
}


