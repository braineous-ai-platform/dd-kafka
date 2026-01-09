package io.braineous.dd.replay.model;

public class ReplayRequest {

    private String stream;           // logical stream name (required)
    private String fromTime;         // ISO-8601, optional
    private String toTime;           // ISO-8601, optional
    private String objectKey;        // optional (business key)
    private String dlqId;            // optional (domain or system)
    private String reason;           // human reason (required)

    public ReplayRequest() {
    }

    public ReplayRequest(String fromTime, String toTime) {
        this.fromTime = fromTime;
        this.toTime = toTime;
    }

    // ---------- JavaBean getters/setters (for JSON binding) ----------

    public String getStream() { return stream; }
    public void setStream(String stream) { this.stream = stream; }

    public String getFromTime() { return fromTime; }
    public void setFromTime(String fromTime) { this.fromTime = fromTime; }

    public String getToTime() { return toTime; }
    public void setToTime(String toTime) { this.toTime = toTime; }

    public String getObjectKey() { return objectKey; }
    public void setObjectKey(String objectKey) { this.objectKey = objectKey; }

    public String getDlqId() { return dlqId; }
    public void setDlqId(String dlqId) { this.dlqId = dlqId; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    // ---------- internal / domain-style accessors (kept) ----------

    public String stream() { return stream; }
    public String fromTime() { return fromTime; }
    public String toTime() { return toTime; }
    public String objectKey() { return objectKey; }
    public String dlqId() { return dlqId; }
    public String reason() { return reason; }
}


