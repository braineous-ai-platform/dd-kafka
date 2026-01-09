package io.braineous.dd.replay.model;

public class IngestionRequest {

    private String stream;           // logical stream name (required)

    private String payload;

    public IngestionRequest() {
    }

    public IngestionRequest(String stream, String payload) {
        this.stream = stream;
        this.payload = payload;
    }

    // ---------- JavaBean getters/setters (for JSON binding) ----------

    public String getStream() { return stream; }
    public void setStream(String stream) { this.stream = stream; }


    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    // ---------- internal / domain-style accessors (kept) ----------

    public String stream() { return stream; }
}


