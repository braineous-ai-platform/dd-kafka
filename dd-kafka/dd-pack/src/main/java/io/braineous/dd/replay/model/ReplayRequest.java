package io.braineous.dd.replay.model;

public class ReplayRequest {

    // time-window replay
    private String fromTime;         // ISO-8601, optional
    private String toTime;           // ISO-8601, optional

    // replay-by-ingestion (canonical selector)
    private String ingestionId;      // optional

    // replay-by-dlq
    private String dlqId;            // optional (domain or system)

    // always required at API surface
    private String reason;           // human reason (required)

    public ReplayRequest() {
    }

    public ReplayRequest(String fromTime, String toTime) {
        this.fromTime = fromTime;
        this.toTime = toTime;
    }

    // ---------- JavaBean getters/setters (for JSON binding) ----------

    public String getFromTime() { return fromTime; }
    public void setFromTime(String fromTime) { this.fromTime = fromTime; }

    public String getToTime() { return toTime; }
    public void setToTime(String toTime) { this.toTime = toTime; }

    public String getIngestionId() { return ingestionId; }
    public void setIngestionId(String ingestionId) { this.ingestionId = ingestionId; }

    public String getDlqId() { return dlqId; }
    public void setDlqId(String dlqId) { this.dlqId = dlqId; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    // ---------- internal / domain-style accessors ----------

    public String fromTime() { return fromTime; }
    public String toTime() { return toTime; }
    public String ingestionId() { return ingestionId; }
    public String dlqId() { return dlqId; }
    public String reason() { return reason; }

    // -----------------------------------------------------------------
    public void normalize() {
        this.fromTime = trimToNull(this.fromTime);
        this.toTime = trimToNull(this.toTime);
        this.ingestionId = trimToNull(this.ingestionId);
        this.dlqId = trimToNull(this.dlqId);
        this.reason = trimToNull(this.reason);
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.length() == 0) return null;
        return t;
    }
}



