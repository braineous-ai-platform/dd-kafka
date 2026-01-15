package io.braineous.dd.ingestion.persistence;

import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import com.google.gson.JsonArray;
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
            col.createIndex(new Document("createdAt", -1));
            col.createIndex(new Document(F_SNAPSHOT_HASH, 1), new com.mongodb.client.model.IndexOptions().unique(true));
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

        JsonObject view = null;

        if (ingestionJson.has("view")
                && ingestionJson.get("view") != null
                && ingestionJson.get("view").isJsonObject()) {
            view = ingestionJson.get("view").getAsJsonObject();
        }

        if (view == null) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    IngestionReceipt.sha256Hex(payload),
                    null,
                    new Why("DD-ING-graphView_null", "graphView cannot be null"),
                    "mongo"
            );
        }

        String snap = null;
        if (view.has(F_SNAPSHOT_HASH)
                && view.get(F_SNAPSHOT_HASH) != null
                && view.get(F_SNAPSHOT_HASH).isJsonPrimitive()) {
            snap = view.get(F_SNAPSHOT_HASH).getAsString();
        }

        String payloadHash = IngestionReceipt.sha256Hex(payload);
        if (snap == null || snap.trim().isEmpty()) {
            return IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    null,
                    new Why("DD-ING-snapshotHash_blank", "snapshotHash cannot be blank"),
                    "mongo"
            );
        }

        SnapshotHash snapshotHash = new SnapshotHash(snap);



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

            IngestionReceipt dlqReceipt = IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_write_failed", mwx.getMessage()),
                    "mongo"
            );
            dlqReceipt.setSysDlqEnabled(true);

            return dlqReceipt;

        } catch (Exception e) {
            IngestionReceipt dlqReceipt = IngestionReceipt.failDomain(
                    ingestionId,
                    payloadHash,
                    snapshotHash,
                    new Why("DD-ING-mongo_insert_failed", e.getMessage()),
                    "mongo"
            );
            dlqReceipt.setSysDlqEnabled(true);
            return dlqReceipt;
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

    @Override
    public JsonArray findEventsByTimeWindow(String fromTime, String toTime) {

        JsonArray out = new JsonArray();

        if (fromTime == null || fromTime.trim().isEmpty()) {
            return out;
        }
        if (toTime == null || toTime.trim().isEmpty()) {
            return out;
        }

        Instant from;
        Instant to;
        try {
            from = Instant.parse(fromTime.trim());
            to = Instant.parse(toTime.trim());
        } catch (Exception ignored) {
            return out;
        }

        // If caller violates contract, return empty (store shouldn't throw for query utilities)
        if (!from.isBefore(to)) {
            return out;
        }

        MongoCollection<Document> col = collection();

        try {
            Date fromDate = Date.from(from);
            Date toDate = Date.from(to);

            // inclusive from, exclusive to
            Bson filter = Filters.and(
                    Filters.gte("createdAt", fromDate),
                    Filters.lt("createdAt", toDate)
            );

            com.mongodb.client.FindIterable<Document> it =
                    col.find(filter).sort(new Document("createdAt", 1));

            com.mongodb.client.MongoCursor<Document> cur = it.iterator();

            try {
                while (cur.hasNext()) {
                    Document d = cur.next();

                    JsonObject e = new JsonObject();

                    Object idObj = d.get("ingestionId");
                    if (idObj != null) {
                        e.addProperty("ingestionId", String.valueOf(idObj));
                    }

                    Object createdAtObj = d.get("createdAt");
                    if (createdAtObj instanceof Date) {
                        Date dt = (Date) createdAtObj;
                        e.addProperty("createdAt", dt.toInstant().toString());
                    }

                    Object snapObj = d.get(F_SNAPSHOT_HASH);
                    if (snapObj != null) {
                        e.addProperty(F_SNAPSHOT_HASH, String.valueOf(snapObj));
                    }

                    Object phObj = d.get(F_PAYLOAD_HASH);
                    if (phObj != null) {
                        e.addProperty(F_PAYLOAD_HASH, String.valueOf(phObj));
                    }

                    Object payloadObj = d.get("payload");
                    if (payloadObj != null) {
                        e.addProperty("payload", String.valueOf(payloadObj));
                    }

                    out.add(e);
                }
            } finally {
                try {
                    cur.close();
                } catch (Exception ignored2) {
                    // deterministic: ignore
                }
            }

        } catch (Exception ignored) {
            // best-effort query utility: swallow and return empty
            return new JsonArray();
        }

        return out;
    }

    @Override
    public JsonObject findEventsByIngestionId(String ingestionId) {

        JsonObject out = new JsonObject();

        if (ingestionId == null || ingestionId.trim().isEmpty()) {
            return out;
        }

        MongoCollection<Document> col = collection();

        try {
            Bson filter = Filters.eq("ingestionId", ingestionId.trim());
            Document d = col.find(filter).first();

            if (d == null) {
                return out;
            }

            Object idObj = d.get("ingestionId");
            if (idObj != null) {
                out.addProperty("ingestionId", String.valueOf(idObj));
            }

            Object createdAtObj = d.get("createdAt");
            if (createdAtObj instanceof Date) {
                Date dt = (Date) createdAtObj;
                out.addProperty("createdAt", dt.toInstant().toString());
            }

            Object snapObj = d.get(F_SNAPSHOT_HASH);
            if (snapObj != null) {
                out.addProperty(F_SNAPSHOT_HASH, String.valueOf(snapObj));
            }

            Object phObj = d.get(F_PAYLOAD_HASH);
            if (phObj != null) {
                out.addProperty(F_PAYLOAD_HASH, String.valueOf(phObj));
            }

            Object payloadObj = d.get("payload");
            if (payloadObj != null) {
                out.addProperty("payload", String.valueOf(payloadObj));
            }

            return out;

        } catch (Exception ignored) {
            // swallow
            return new JsonObject();
        }
    }



    //------------------------------------------------------------------------------------------------
    private boolean indexBootstrapEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("dd.ingestion.index.bootstrap", "false"));
    }
}




