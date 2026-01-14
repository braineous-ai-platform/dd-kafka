package io.braineous.dd.processor;

import com.google.gson.JsonObject;
import io.braineous.dd.core.model.Why;

public class ProcessorResult {

    // field init helper (thread-safe)
    private static final java.util.concurrent.atomic.AtomicLong EVENT_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    private static final String ID_PREFIX = "DD-PR-";
    private static final String WHY_CODE_NULL_EVENT = "DD-PR-FAIL-ddEventJson_null";
    private static final String WHY_MSG_NULL_EVENT  = "ddEventJson cannot be null for ok=true";

    private String id;
    private JsonObject ddEventJson;
    private boolean ok;
    private Why why;

    private String ingestionId;


    // --------- Canonical constructor (single source of truth) ---------


    public ProcessorResult() {
    }

    public ProcessorResult(JsonObject ddEventJson, boolean ok, Why why) {
        long seq = nextSeq();
        this.id = nextEventId(seq);

        // Invariants:
        // 1) ok=true => ddEventJson must be non-null, why must be null
        // 2) ok=false => why must be non-null (ddEventJson may be null)
        if (ok) {
            if (ddEventJson == null) {
                // convert to clean failure result (DLQ-friendly, no throw)
                this.ddEventJson = null;
                this.ok = false;
                this.why = new Why(WHY_CODE_NULL_EVENT, WHY_MSG_NULL_EVENT);
                return;
            }
            this.ddEventJson = ddEventJson;
            this.ok = true;
            this.why = null; // enforce: success carries no failure why
            return;
        }

        // ok == false
        this.ddEventJson = ddEventJson; // allowed null for failure
        this.ok = false;
        this.why = (why != null)
                ? why
                : new Why("DD-PR-FAIL-missing_why", "ok=false requires a non-null why");
    }

    // Convenience 2-arg ctor (keeps your old call sites alive)
    public ProcessorResult(JsonObject ddEventJson, boolean ok) {
        this(ddEventJson, ok, null);
    }

    // --------- Static factories (preferred) ---------

    public static ProcessorResult ok(JsonObject ddEventJson) {
        return new ProcessorResult(ddEventJson, true, null);
    }

    public static ProcessorResult fail(JsonObject ddEventJson, Why why) {
        return new ProcessorResult(ddEventJson, false, why);
    }

    public static ProcessorResult fail(Why why) {
        return new ProcessorResult(null, false, why);
    }

    // --------- Getters ---------


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JsonObject getDdEventJson() {
        return ddEventJson;
    }

    public void setDdEventJson(JsonObject ddEventJson) {
        this.ddEventJson = ddEventJson;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public Why getWhy() {
        return why;
    }

    public void setWhy(Why why) {
        this.why = why;
    }

    public String getIngestionId() {
        return ingestionId;
    }

    public void setIngestionId(String ingestionId) {
        this.ingestionId = ingestionId;
    }

    @Override
    public String toString() {
        return "ProcessorResult{" +
                "id='" + id + '\'' +
                ", ddEventJson=" + ddEventJson +
                ", ok=" + ok +
                ", why=" + why +
                ", ingestionId='" + ingestionId + '\'' +
                '}';
    }

    // --------- Internal helpers ---------
    private static long nextSeq() {
        return EVENT_SEQ.incrementAndGet();
    }

    private static String nextEventId(long seq) {
        return ID_PREFIX + seq;
    }

    public com.google.gson.JsonObject toJson() {

        com.google.gson.JsonObject out = new com.google.gson.JsonObject();

        if (this.id != null) {
            out.addProperty("id", this.id);
        }

        out.addProperty("ok", this.ok);

        if (this.ingestionId != null) {
            out.addProperty("ingestionId", this.ingestionId);
        }

        if (this.why != null) {
            com.google.gson.JsonObject w = new com.google.gson.JsonObject();

            // assuming Why has reason() and message() (based on your IT assertions)
            String r = this.why.reason();
            if (r != null) {
                w.addProperty("reason", r);
            }

            String m = this.why.getDetails();
            if (m != null) {
                w.addProperty("details", m);
            }

            out.add("why", w);
        } else {
            out.add("why", null);
        }

        if (this.ddEventJson != null) {
             out.add("ddEventJson", this.ddEventJson);
        }

        return out;
    }

    public String toJsonString() {
        return toJson().toString();
    }
}

