package io.braineous.dd.replay.model;

public class ReplayEvent {

    private String id;          // stable unique id (best-effort; may be null)
    private String payload;     // original ingestion payload (String; best-effort; may be null)
    private java.time.Instant timestamp; // best-effort; may be null

    public ReplayEvent(String id, String payload, java.time.Instant timestamp) {
        // Normalization only (no validation / no throwing).
        // Replay is a best-effort control surface; dirty inputs must not crash construction.
        this.id = trimToNull(id);
        this.payload = trimToNull(payload);
        this.timestamp = timestamp;
    }

    public String id() { return id; }
    public String payload() { return payload; }
    public java.time.Instant timestamp() { return timestamp; }

    // ---------------- internal helpers ----------------

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.length() == 0) return null;
        return t;
    }
}


