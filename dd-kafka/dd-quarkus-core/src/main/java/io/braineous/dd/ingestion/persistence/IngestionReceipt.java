package io.braineous.dd.ingestion.persistence;

import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;

import java.time.Instant;
import java.util.StringJoiner;

public final class IngestionReceipt {

    // -------- Identity --------
    private final String ingestionId;

    // -------- Outcome --------
    private final boolean ok;
    private final Why why;                 // invariant-bound

    // -------- Correlation / Determinism --------
    private final String payloadHash;
    private final SnapshotHash snapshotHash;

    // -------- Provenance --------
    private final String storeType;         // mongo | memory | future

    // -------- Time --------
    private final Instant createdAt;

    //------------------------------------------------------------------

    private boolean sysDlqEnabled = false;

    //------------------------------------------------------------------

    private IngestionReceipt(
            String ingestionId,
            boolean ok,
            Why why,
            String payloadHash,
            SnapshotHash snapshotHash,
            String storeType,
            Instant createdAt
    ) {
        this.ingestionId = ingestionId;
        this.ok = ok;
        this.why = why;
        this.payloadHash = payloadHash;
        this.snapshotHash = snapshotHash;
        this.storeType = storeType;
        this.createdAt = createdAt;
    }

    public String ingestionId() { return ingestionId; }
    public boolean ok() { return ok; }
    public Why why() { return why; }
    public String payloadHash() { return payloadHash; }
    public SnapshotHash snapshotHash() { return snapshotHash; }
    public String storeType() { return storeType; }
    public Instant createdAt() { return createdAt; }

    public boolean isSysDlqEnabled() {
        return sysDlqEnabled;
    }

    public void setSysDlqEnabled(boolean sysDlqEnabled) {
        this.sysDlqEnabled = sysDlqEnabled;
    }

    //--------------------------------------------------------------------

    public static IngestionReceipt ok(
            String ingestionId,
            String payloadHash,
            SnapshotHash snapshotHash,
            String storeType
    ) {
        return new IngestionReceipt(
                ingestionId,
                true,
                null,
                payloadHash,
                snapshotHash,
                storeType,
                Instant.now()
        ).validate();
    }

    public static IngestionReceipt failDomain(
            String ingestionId,
            String payloadHash,
            SnapshotHash snapshotHash,
            Why why,
            String storeType
    ) {
        return new IngestionReceipt(
                ingestionId,
                false,
                why,
                payloadHash,
                snapshotHash,
                storeType,
                Instant.now()
        ).validate();
    }

    public IngestionReceipt validate() {
        if (this.ingestionId == null || this.ingestionId.trim().isEmpty()) {
            throw new IllegalArgumentException("ingestionId_required");
        }
        if (this.storeType == null || this.storeType.trim().isEmpty()) {
            throw new IllegalArgumentException("storeType_required");
        }

        // invariant: ok=true => why null; ok=false => why non-null
        if (this.ok && this.why != null) {
            throw new IllegalArgumentException("receipt_invariant_failed: ok=true requires why=null");
        }
        if (!this.ok && this.why == null) {
            throw new IllegalArgumentException("receipt_invariant_failed: ok=false requires why!=null");
        }

        if (this.ok) {
            if (this.payloadHash == null || this.payloadHash.trim().isEmpty()) {
                throw new IllegalArgumentException("payloadHash_required");
            }
            if (this.snapshotHash == null
                    || this.snapshotHash.getValue() == null
                    || this.snapshotHash.getValue().trim().isEmpty()) {
                throw new IllegalArgumentException("snapshotHash_required");
            }
        }

        // ok=false receipts can be partial (fail-fast before hashes/snapshot computed)
        return this;
    }

    public static String sha256Hex(String s) {
        if (s == null) return null;
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("sha256_failed: " + e.getMessage(), e);
        }
    }

    public JsonObject toJson() {
        var j = new JsonObject();
        j.addProperty("ingestionId", ingestionId);
        j.addProperty("ok", ok);

        if (why != null) {
            // why.toJson() assumed to return a JSON string
            j.add("why", JsonParser.parseString(why.toJson()).getAsJsonObject());
        }

        if (payloadHash != null) j.addProperty("payloadHash", payloadHash);
        if (snapshotHash != null) j.addProperty("snapshotHash", snapshotHash.getValue());

        j.addProperty("storeType", storeType);
        j.addProperty("createdAt", createdAt.toString());
        return j;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "IngestionReceipt{", "}")
                .add("ingestionId='" + ingestionId + "'")
                .add("ok=" + ok)
                .add("why=" + why)
                .add("payloadHash='" + payloadHash + "'")
                .add("snapshotHash=" + (snapshotHash == null ? null : snapshotHash.getValue()))
                .add("storeType='" + storeType + "'")
                .add("createdAt=" + createdAt)
                .toString();
    }
}

