package io.braineous.dd.ingestion.persistence;

import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.braineous.dd.core.model.Why;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class MongoIngestionStore implements IngestionStore {

    public static final String DB  = "dd";
    public static final String COL = "ingestion";

    private static final String F_ID_KEY        = "idKey";
    public static final String F_SNAPSHOT_HASH = "snapshotHash";
    private static final String F_PAYLOAD_HASH  = "payloadHash";

    private final AtomicBoolean ingestionIndexed = new AtomicBoolean(false);

    @Inject
    MongoClient mongoClient;

    // Best-effort only. Never break ingestion.
    private void ensureIndexes(MongoCollection<Document> col) {
        try {
            // Idempotency contract: UNIQUE on idKey
            // If race happens, treat duplicate key as idempotent success (no read-back).
            col.createIndex(new Document(F_ID_KEY, 1), new com.mongodb.client.model.IndexOptions().unique(true));

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
    public IngestionReceipt storeIngestion(String payload) {
        String ingestionId = null;
        // ---------- fail-fast: payload ----------
        if (payload == null || payload.trim().isEmpty()) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    null, null,
                    new Why("DD-ING-payload_blank", "payload cannot be blank"),
                    "mongo"
            );
        }

        JsonObject ingestionJson = JsonParser.parseString(payload).getAsJsonObject();
        if(!ingestionJson.has("ingestionId")) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    null, null,
                    new Why("DD-ING-ingestion-id-missing", "ingestion id must be assigned by the consumer"),
                    "mongo"
            );
        }
        ingestionId = ingestionJson.get("ingestionId").getAsString();

        JsonObject view = ingestionJson.get("view").getAsJsonObject();
        // ---------- fail-fast: view ----------
        if (view == null) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    IngestionReceipt.sha256Hex(payload),
                    null,
                    new Why("DD-ING-graphView_null", "graphView cannot be null"),
                    "mongo"
            );
        }

        String payloadHash = IngestionReceipt.sha256Hex(payload);

        String snap = view.get(MongoIngestionStore.F_SNAPSHOT_HASH).getAsString();

        SnapshotHash snapshotHash = new SnapshotHash(snap);

        if (snap == null || snap.trim().isEmpty()) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-snapshotHash_blank", "snapshotHash cannot be blank"),
                    "mongo"
            );
        }


        MongoCollection<Document> col = collection();

        if (indexBootstrapEnabled() && ingestionIndexed.compareAndSet(false, true)) {
            ensureIndexes(col);
        }

        // ---------- mongo insert-or-touch (idempotent on snapshotHash) ----------
        try {
            Bson filter = Filters.eq(F_SNAPSHOT_HASH, snap);

            Document existing = col.find(filter).first();

            if (existing != null) {

                Object existingIngestionIdObj = existing.get("ingestionId");
                if (existingIngestionIdObj != null) {
                    ingestionId = String.valueOf(existingIngestionIdObj);
                }

                // touch only createdAt (their reality: last time they sent it)
                Document touch = new Document("$set",
                        new Document("createdAt", Date.from(Instant.now()))
                );

                col.updateOne(filter, touch);

                return IngestionReceipt.ok(
                        ingestionId,
                        payloadHash,
                        snapshotHash,
                        "mongo"
                );
            }

            Document doc = new Document()
                    .append("ingestionId", ingestionId)
                    .append(F_SNAPSHOT_HASH, snap)
                    .append(F_PAYLOAD_HASH, payloadHash)
                    .append("payload", payload)
                    .append("createdAt", Date.from(Instant.now()));

            col.insertOne(doc);

        } catch (MongoWriteException mwx) {
            if (mwx.getError() != null
                    && mwx.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {

                // race: someone inserted after our find; touch createdAt only
                Bson filter = Filters.eq(F_SNAPSHOT_HASH, snap);
                Document touch = new Document("$set",
                        new Document("createdAt", Date.from(Instant.now()))
                );
                col.updateOne(filter, touch);

                // keep axis stable if we can read it
                Document existing = col.find(filter).first();
                if (existing != null && existing.get("ingestionId") != null) {
                    ingestionId = String.valueOf(existing.get("ingestionId"));
                }

                return IngestionReceipt.ok(
                        ingestionId,
                        payloadHash,
                        snapshotHash,
                        "mongo"
                );
            }

            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_write_failed", mwx.getMessage()),
                    "mongo"
            );

        } catch (Exception e) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_insert_failed", e.getMessage()),
                    "mongo"
            );
        }


        // ---------- success ----------
        return IngestionReceipt.ok(
                ingestionId,
                payloadHash,
                snapshotHash,
                "mongo"
        );
    }

    @Override
    public String resolveIngestionId(String payload, String snap) {

        if (snap == null || snap.trim().length()==0) {
            return null;
        }


        MongoCollection<Document> col = collection();

        try {
            Bson filter = Filters.eq(F_SNAPSHOT_HASH, snap);
            Document existing = col.find(filter).first();

            if (existing != null) {
                Object existingIngestionIdObj = existing.get("ingestionId");
                if (existingIngestionIdObj != null) {
                    String id = String.valueOf(existingIngestionIdObj);
                    if (id != null && id.trim().length() > 0) {
                        return id;
                    }
                }
            }
        } catch (Exception ignored) {
            // fall through to generate
        }

        // axis birth (first time this snapshot is seen OR mongo lookup hiccup)
        String day = LocalDate.now(ZoneOffset.UTC).toString().replace("-", "");
        long nano = System.nanoTime();
        return "DD-ING-" + day + "-" + nano;
    }



    private boolean indexBootstrapEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("dd.ingestion.index.bootstrap", "false"));
    }
}




