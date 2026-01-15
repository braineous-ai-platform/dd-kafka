package io.braineous.dd.replay.persistence;

import ai.braineous.rag.prompt.observe.Console;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bson.Document;

import java.util.List;

@jakarta.enterprise.context.ApplicationScoped
public class MongoReplayStore implements ReplayStore {

    public static final String DB  = "dd";
    public static final String INGESTION_COL = "ingestion";

    @jakarta.inject.Inject
    com.mongodb.client.MongoClient mongoClient;

    private com.mongodb.client.MongoCollection<org.bson.Document> collection() {
        return mongoClient
                .getDatabase(DB)
                .getCollection(INGESTION_COL);
    }

    private com.mongodb.client.MongoCollection<org.bson.Document> domainDlqCol() {
        return mongoClient
                .getDatabase(DB)
                .getCollection("dlq_domain");
    }

    private com.mongodb.client.MongoCollection<org.bson.Document> systemDlqCol() {
        return mongoClient
                .getDatabase(DB)
                .getCollection("dlq_system");
    }


    @Override
    public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request) {

        Console.log("REPLAY_STORE_IMPL", this.getClass().getName());

        if (request == null) return java.util.Collections.emptyList();

        final java.time.Instant from;
        final java.time.Instant to;

        try {
            String fromS = request.fromTime();
            String toS   = request.toTime();

            if (fromS == null) return java.util.Collections.emptyList();
            if (toS == null)   return java.util.Collections.emptyList();

            fromS = fromS.trim();
            toS   = toS.trim();

            if (fromS.length() == 0) return java.util.Collections.emptyList();
            if (toS.length() == 0)   return java.util.Collections.emptyList();

            from = java.time.Instant.parse(fromS);
            to   = java.time.Instant.parse(toS);
        } catch (Exception e) {
            Console.log("REPLAY_TIMEWINDOW_PARSE_FAIL", String.valueOf(e));
            return java.util.Collections.emptyList();
        }

        if (!from.isBefore(to)) return java.util.Collections.emptyList();

        Console.log("REPLAY_QUERY_DUMP",
                "db=" + DB +
                        " col=" + INGESTION_COL +
                        " from=" + from +
                        " to=" + to +
                        " fromDate=" + java.util.Date.from(from) +
                        " toDate=" + java.util.Date.from(to)
        );

        org.bson.conversions.Bson filter = com.mongodb.client.model.Filters.and(
                com.mongodb.client.model.Filters.gte("createdAt", java.util.Date.from(from)),
                com.mongodb.client.model.Filters.lt("createdAt", java.util.Date.from(to))
        );

        org.bson.conversions.Bson sort = com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

        java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<ReplayEvent>();
        for (org.bson.Document d : collection().find(filter).sort(sort)) {
            ReplayEvent ev = safeMapDocToReplayEvent(d);
            if (ev != null) {
                out.add(ev);
            }
        }

        Console.log("REPLAY_MATCHED_COUNT", Integer.valueOf(out.size()));
        Console.log("REPLAY_MONGO_COUNT", Long.valueOf(collection().countDocuments(filter)));

        return java.util.Collections.unmodifiableList(out);
    }

    @Override
    public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {

        if (request == null) return java.util.Collections.emptyList();

        final String key;
        try {
            key = request.ingestionId();
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }

        if (key == null || key.trim().length() == 0) return java.util.Collections.emptyList();

        try {
            org.bson.conversions.Bson filter = com.mongodb.client.model.Filters.eq("ingestionId", key.trim());
            org.bson.conversions.Bson sort   = com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

            java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<ReplayEvent>();
            for (org.bson.Document d : collection().find(filter).sort(sort)) {
                ReplayEvent ev = safeMapDocToReplayEvent(d);
                if (ev != null) {
                    out.add(ev);
                }
            }
            return java.util.Collections.unmodifiableList(out);
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }
    }


    @Override
    public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {

        if (request == null) return java.util.Collections.emptyList();

        final String dlqId;
        try {
            dlqId = request.dlqId();
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }

        if (dlqId == null || dlqId.trim().length() == 0) {
            return java.util.Collections.emptyList();
        }

        try {
            org.bson.conversions.Bson filter =
                    com.mongodb.client.model.Filters.eq("dlqId", dlqId.trim());
            org.bson.conversions.Bson sort =
                    com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

            java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<ReplayEvent>();
            for (org.bson.Document d : domainDlqCol().find(filter).sort(sort)) {
                ReplayEvent ev = safeMapDocToReplayEvent(d);
                if (ev != null) {
                    out.add(ev);
                }
            }
            return java.util.Collections.unmodifiableList(out);
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }
    }

    @Override
    public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {

        if (request == null) return java.util.Collections.emptyList();

        final String dlqId;
        try {
            dlqId = request.dlqId();
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }

        if (dlqId == null || dlqId.trim().length() == 0) {
            return java.util.Collections.emptyList();
        }

        try {
            org.bson.conversions.Bson filter =
                    com.mongodb.client.model.Filters.eq("dlqId", dlqId.trim());
            org.bson.conversions.Bson sort =
                    com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

            java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<ReplayEvent>();
            for (org.bson.Document d : systemDlqCol().find(filter).sort(sort)) {
                ReplayEvent ev = safeMapDocToReplayEvent(d);
                if (ev != null) {
                    out.add(ev);
                }
            }
            return java.util.Collections.unmodifiableList(out);
        } catch (Exception e) {
            return java.util.Collections.emptyList();
        }
    }


    //------------------------------------------------------------------------------
    private ReplayEvent safeMapDocToReplayEvent(org.bson.Document d) {

        if (d == null) {
            return null;
        }

        String id = null;
        String payload = null;
        java.time.Instant ts = null;

        try {
            Object oid = d.get("_id");
            if (oid != null) {
                id = String.valueOf(oid);
            }
        } catch (Exception ignored) {
            // ignore
        }

        if (id == null) {
            try {
                Object ingestionIdObj = d.get("ingestionId");
                if (ingestionIdObj != null) {
                    id = String.valueOf(ingestionIdObj);
                }
            } catch (Exception ignored) {
                // ignore
            }
        }

        try {
            Object payloadObj = d.get("payload");
            if (payloadObj != null) {
                payload = String.valueOf(payloadObj);
            }
        } catch (Exception ignored) {
            // ignore
        }

        try {
            Object createdAtObj = d.get("createdAt");
            if (createdAtObj instanceof java.util.Date) {
                java.util.Date createdAt = (java.util.Date) createdAtObj;
                ts = createdAt.toInstant();
            }
        } catch (Exception ignored) {
            // ignore
        }

        return new ReplayEvent(id, payload, ts);
    }
}


