package io.braineous.dd.replay.model;

public class ReplayResult {

    private boolean ok;
    private String replayId;
    private int replayedCount;
    private int matchedCount;
    private String reason;   // error / disabled / bad request

    // Jackson + factory-only usage
    private ReplayResult() {}

    // ---------- factories ----------

    public static ReplayResult ok(ReplayRequest req, int replayed, int matched) {
        ReplayResult r = new ReplayResult();
        r.ok = true;
        r.replayId = "REPLAY-" + System.currentTimeMillis();
        r.replayedCount = replayed;
        r.matchedCount = matched;
        return r;
    }

    public static ReplayResult empty(ReplayRequest req) {
        ReplayResult r = new ReplayResult();
        r.ok = true;
        r.replayId = "REPLAY-" + System.currentTimeMillis();
        r.replayedCount = 0;
        r.matchedCount = 0;
        return r;
    }

    public static ReplayResult badRequest(String reason) {
        ReplayResult r = new ReplayResult();
        r.ok = false;
        r.reason = reason;
        return r;
    }

    public static ReplayResult fail(String reason) {
        ReplayResult r = new ReplayResult();
        r.ok = false;
        r.reason = reason;
        return r;
    }

    // ---------- Jackson-visible getters ----------
    // (method names intentionally JavaBean-compliant)

    public boolean isOk() {
        return ok;
    }

    public String getReplayId() {
        return replayId;
    }

    public int getReplayedCount() {
        return replayedCount;
    }

    public int getMatchedCount() {
        return matchedCount;
    }

    public String getReason() {
        return reason;
    }

    // ---------- internal / domain-style accessors ----------
    // (kept for consistency with your call sites)

    public boolean ok() { return ok; }
    public String replayId() { return replayId; }
    public int replayedCount() { return replayedCount; }
    public int matchedCount() { return matchedCount; }
    public String reason() { return reason; }
}




