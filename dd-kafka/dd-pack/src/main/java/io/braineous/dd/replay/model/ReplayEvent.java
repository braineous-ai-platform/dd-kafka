package io.braineous.dd.replay.model;

public class ReplayEvent {

    private String id;          // stable unique id
    private String payload;     // original ingestion payload (String)
    private java.time.Instant timestamp;

    public ReplayEvent(String id, String payload, java.time.Instant timestamp) {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String id() { return id; }
    public String payload() { return payload; }
    public java.time.Instant timestamp() { return timestamp; }
}

