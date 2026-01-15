package io.braineous.dd.support;

import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.ingestion.persistence.IngestionStore;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-support store.
 *
 * Goal: mirror MongoIngestionStore's ingestion contract + idempotency semantics,
 * but keep only the minimum state needed for assertions in unit tests.
 *
 * No Mongo-level behavior, no paging/queries, no indexes.
 */
public class InMemoryIngestionStore implements IngestionStore {

    private static final String STORE_TYPE = "memory";

    // idempotency axis: snapshot_hash
    private final Map<String, Entry> bySnapshotHash = new ConcurrentHashMap<String, Entry>();

    // assertion support
    private final List<String> storedPayloads = new ArrayList<String>();
    private final List<IngestionReceipt> receipts = new ArrayList<IngestionReceipt>();
    private int storeCalls;

    @Override
    public synchronized IngestionReceipt storeIngestion(String payload) {
        String ingestionId = null;

        // ---------- fail-fast: payload ----------
        if (payload == null || payload.trim().isEmpty()) {
            IngestionReceipt r = IngestionReceipt.failDomain(
                    ingestionId,
                    null,
                    null,
                    new Why("DD-ING-payload_blank", "payload cannot be blank"),
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        JsonObject ingestionJson;
        try {
            ingestionJson = JsonParser.parseString(payload).getAsJsonObject();
        } catch (Exception e) {
            IngestionReceipt r = IngestionReceipt.failDomain(
                    ingestionId,
                    IngestionReceipt.sha256Hex(payload),
                    null,
                    new Why("DD-ING-payload_not_json", e.getMessage()),
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        if (!ingestionJson.has("ingestionId")) {
            IngestionReceipt r = IngestionReceipt.failDomain(
                    ingestionId,
                    null,
                    null,
                    new Why("DD-ING-ingestion-id-missing", "ingestion id must be assigned by the consumer"),
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        ingestionId = ingestionJson.get("ingestionId").getAsString();

        JsonObject view = null;
        try {
            view = ingestionJson.get("view").getAsJsonObject();
        } catch (Exception ignored) {
            // keep view null for invariant behavior below
        }

        // ---------- fail-fast: view ----------
        if (view == null) {
            IngestionReceipt r = IngestionReceipt.failDomain(
                    ingestionId,
                    IngestionReceipt.sha256Hex(payload),
                    null,
                    new Why("DD-ING-graphView_null", "graphView cannot be null"),
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        String payloadHash = IngestionReceipt.sha256Hex(payload);

        String snap = null;
        try {
            snap = view.get(MongoIngestionStore.F_SNAPSHOT_HASH).getAsString();
        } catch (Exception ignored) {
            // keep snap null
        }

        SnapshotHash snapshotHash = new SnapshotHash(snap);

        if (snap == null || snap.trim().isEmpty()) {
            IngestionReceipt r = IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-snapshotHash_blank", "snapshotHash cannot be blank"),
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        // ---------- in-memory insert-or-touch (idempotent on snapshotHash) ----------
        Entry existing = bySnapshotHash.get(snap);
        if (existing != null) {

            // keep axis stable if we can read it
            if (existing.ingestionId != null && existing.ingestionId.trim().length() > 0) {
                ingestionId = existing.ingestionId;
            }

            // touch createdAt only (their reality: last time they sent it)
            existing.createdAt = Date.from(Instant.now());

            IngestionReceipt r = IngestionReceipt.ok(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    STORE_TYPE
            );
            record(payload, r);
            return r;
        }

        Entry e = new Entry();
        e.ingestionId = ingestionId;
        e.snapshotHash = snap;
        e.payloadHash = payloadHash;
        e.payload = payload;
        e.createdAt = Date.from(Instant.now());

        bySnapshotHash.put(snap, e);

        IngestionReceipt r = IngestionReceipt.ok(
                ingestionId,
                payloadHash,
                snapshotHash,
                STORE_TYPE
        );
        record(payload, r);
        return r;
    }

    @Override
    public String resolveIngestionId(String payload, String snap) {

        if (snap == null || snap.trim().length() == 0) {
            return null;
        }

        Entry existing = bySnapshotHash.get(snap);
        if (existing != null) {
            String id = existing.ingestionId;
            if (id != null && id.trim().length() > 0) {
                return id;
            }
        }

        // axis birth (first time this snapshot is seen)
        String day = LocalDate.now(ZoneOffset.UTC).toString().replace("-", "");
        long nano = System.nanoTime();
        return "DD-ING-" + day + "-" + nano;
    }

    @Override
    public JsonArray findEventsByTimeWindow(String fromTime, String toTime) {
        return new JsonArray();
    }

    @Override
    public JsonObject findEventsByIngestionId(String ingestionId) {
        return new JsonObject();
    }

    // ---------------- assertion support ----------------

    public synchronized void reset() {
        bySnapshotHash.clear();
        storedPayloads.clear();
        receipts.clear();
        storeCalls = 0;
    }

    public synchronized int storeCalls() {
        return storeCalls;
    }

    public synchronized String lastStoredPayload() {
        if (storedPayloads.isEmpty()) {
            return null;
        }
        return storedPayloads.get(storedPayloads.size() - 1);
    }

    public synchronized List<String> storedPayloads() {
        return Collections.unmodifiableList(new ArrayList<String>(storedPayloads));
    }

    public synchronized IngestionReceipt lastReceipt() {
        if (receipts.isEmpty()) {
            return null;
        }
        return receipts.get(receipts.size() - 1);
    }

    public synchronized List<IngestionReceipt> receipts() {
        return Collections.unmodifiableList(new ArrayList<IngestionReceipt>(receipts));
    }

    public Entry snapshotEntry(String snapshotHash) {
        if (snapshotHash == null) {
            return null;
        }
        return bySnapshotHash.get(snapshotHash);
    }

    private synchronized void record(String payload, IngestionReceipt receipt) {
        storeCalls = storeCalls + 1;
        storedPayloads.add(payload);
        receipts.add(receipt);
    }

    // ---------------- minimal entry ----------------

    public static class Entry {
        public String ingestionId;
        public String snapshotHash;
        public String payloadHash;
        public String payload;
        public Date createdAt;
    }
}