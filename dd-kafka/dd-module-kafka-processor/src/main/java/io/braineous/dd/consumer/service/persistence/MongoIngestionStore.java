package io.braineous.dd.consumer.service.persistence;

import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import io.braineous.dd.core.model.Why;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import jakarta.inject.Inject;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class MongoIngestionStore implements IngestionStore {

    public static final String DB  = "dd";
    public static final String COL = "ingestion";

    private static final String F_SNAPSHOT_HASH = "snapshotHash";
    private static final String F_PAYLOAD_HASH  = "payloadHash";

    private final AtomicBoolean ingestionIndexed = new AtomicBoolean(false);

    @Inject
    MongoClient mongoClient;

    // Best-effort only. Never break ingestion.
    private void ensureIndexes(MongoCollection<Document> col) {
        try {
            // If a UNIQUE index exists and a race happens, treat as idempotent success.
            // NOTE: ingestionId returned may not match the stored document (we did not read-back).


            col.createIndex(new Document("createdAt", -1));
            col.createIndex(new Document(F_SNAPSHOT_HASH, 1));
            col.createIndex(new Document(F_PAYLOAD_HASH, 1));
        } catch (Exception ignored) {
            // swallow: indexes are not a runtime concern in this phase
        }
    }

    private MongoCollection<Document> collection() {
        return mongoClient
                .getDatabase(DB)
                .getCollection(COL);
    }

    @Override
    public IngestionReceipt storeIngestion(String payload, GraphView view) {
        String ingestionId = IngestionReceipt.nextIngestionId();

        // ---------- fail-fast: payload ----------
        if (payload == null || payload.trim().isEmpty()) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    null,
                    null,
                    new Why("DD-ING-payload_blank", "payload cannot be blank"),
                    0, 0,
                    "mongo"
            );
        }

        // ---------- fail-fast: view ----------
        if (view == null) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    IngestionReceipt.sha256Hex(payload),
                    null,
                    new Why("DD-ING-graphView_null", "graphView cannot be null"),
                    0, 0,
                    "mongo"
            );
        }

        String payloadHash = IngestionReceipt.sha256Hex(payload);

        GraphSnapshot snapshot = (GraphSnapshot) view;
        SnapshotHash snapshotHash = snapshot.snapshotHash();

        String snap = (snapshotHash == null ? null : snapshotHash.getValue());
        if (snap == null || snap.trim().isEmpty()) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-snapshotHash_blank", "snapshotHash cannot be blank"),
                    0, 0,
                    "mongo"
            );
        }

        int nodeCount = snapshot.nodes().size();
        int edgeCount = snapshot.edges().size();

        MongoCollection<Document> col = collection();

        if (indexBootstrapEnabled() && ingestionIndexed.compareAndSet(false, true)) {
            ensureIndexes(col);
        }


        // ---------- mongo upsert (idempotent) ----------
        try {
            var filter = Filters.and(
                    Filters.eq(F_SNAPSHOT_HASH, snap),
                    Filters.eq(F_PAYLOAD_HASH, payloadHash)
            );

            var doc = new Document()
                    .append("ingestionId", ingestionId)
                    .append(F_SNAPSHOT_HASH, snap)
                    .append(F_PAYLOAD_HASH, payloadHash)
                    .append("payload", payload)
                    .append("nodeCount", nodeCount)
                    .append("edgeCount", edgeCount)
                    .append("createdAt", Date.from(Instant.now()));

            var update = new Document("$setOnInsert", doc);
            var opts = new UpdateOptions().upsert(true);

            col.updateOne(filter, update, opts);

        } catch (MongoWriteException mwx) {
            // If a UNIQUE index exists and a race happens, treat as idempotent success.
            if (mwx.getError() != null && mwx.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                return IngestionReceipt.ok(
                        ingestionId,
                        payloadHash,
                        snapshotHash,
                        nodeCount,
                        edgeCount,
                        "mongo"
                );
            }

            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_write_failed", mwx.getMessage()),
                    nodeCount,
                    edgeCount,
                    "mongo"
            );

        } catch (Exception e) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_upsert_failed", e.getMessage()),
                    nodeCount,
                    edgeCount,
                    "mongo"
            );
        }

        // ---------- success ----------
        return IngestionReceipt.ok(
                ingestionId,
                payloadHash,
                snapshotHash,
                nodeCount,
                edgeCount,
                "mongo"
        );
    }

    private boolean indexBootstrapEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("dd.ingestion.index.bootstrap", "false"));
    }

}



