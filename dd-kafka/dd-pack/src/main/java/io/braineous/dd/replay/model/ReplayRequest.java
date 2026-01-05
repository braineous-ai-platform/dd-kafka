package io.braineous.dd.replay.model;

public class ReplayRequest {

    private String stream;          // logical stream name (required)
    private String fromTime;         // ISO-8601, optional
    private String toTime;           // ISO-8601, optional
    private String objectKey;        // optional (business key)
    private String dlqId;            // optional (domain or system)
    private Integer limit;           // optional
    private String reason;           // human reason (required)

    public String stream() { return stream; }
    public String fromTime() { return fromTime; }
    public String toTime() { return toTime; }
    public String objectKey() { return objectKey; }
    public String dlqId() { return dlqId; }
    public Integer limit() { return limit; }
    public String reason() { return reason; }

    public int limitOrDefault(int def) {
        return limit == null ? def : limit;
    }
}

