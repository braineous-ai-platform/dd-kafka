package io.braineous.dd.replay.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.braineous.dd.consumer.service.persistence.MongoIngestionStore;
import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bson.Document;

import java.util.List;

@ApplicationScoped
public class MongoReplayStore implements ReplayStore{

    public static final String DB  = "dd";
    public static final String INGESTION_COL = MongoIngestionStore.COL;

    @Inject
    MongoClient mongoClient;

    private MongoCollection<Document> collection() {
        return mongoClient
                .getDatabase(DB)
                .getCollection(INGESTION_COL);
    }

    @Override
    public List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
        // window semantics: [from, to)  (to is exclusive)

        if (request == null) return List.of();

        final java.time.Instant from;
        final java.time.Instant to;

        try {
            String fromS = request.fromTime();
            String toS   = request.toTime();

            if (fromS == null || fromS.trim().isEmpty()) return List.of();
            if (toS == null   || toS.trim().isEmpty())   return List.of();

            from = java.time.Instant.parse(fromS);
            to   = java.time.Instant.parse(toS);
        } catch (Exception e) {
            return List.of();
        }

        if (!from.isBefore(to)) return List.of();


        var filter = com.mongodb.client.model.Filters.and(
                com.mongodb.client.model.Filters.gte("createdAt", java.util.Date.from(from)),
                com.mongodb.client.model.Filters.lt("createdAt", java.util.Date.from(to))
        );

        var sort = com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

        java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<>();
        for (Document d : collection().find(filter).sort(sort)) {
            out.add(mapDocToReplayEvent(d));
        }
        return java.util.List.copyOf(out);
    }

    @Override
    public List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {

        if (request == null) return List.of();

        final String key;
        try {
            key = request.objectKey();
        } catch (Exception e) {
            return List.of();
        }

        if (key == null || key.trim().isEmpty()) return List.of();

        try {
            var filter = com.mongodb.client.model.Filters.eq("objectKey", key.trim());
            var sort   = com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

            java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<>();
            for (org.bson.Document d : collection().find(filter).sort(sort)) {
                out.add(mapDocToReplayEvent(d));
            }
            return java.util.List.copyOf(out);
        } catch (Exception e) {
            return List.of();
        }
    }

    @Override
    public List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {

        if (request == null) return List.of();

        final String dlqId;
        try {
            dlqId = request.dlqId();
        } catch (Exception e) {
            return List.of();
        }

        if (dlqId == null || dlqId.trim().isEmpty()) return List.of();

        try {
            var filter = com.mongodb.client.model.Filters.eq("dlqId", dlqId.trim());
            var sort   = com.mongodb.client.model.Sorts.ascending("createdAt", "_id");

            java.util.ArrayList<ReplayEvent> out = new java.util.ArrayList<>();
            for (org.bson.Document d : collection().find(filter).sort(sort)) {
                out.add(mapDocToReplayEvent(d));
            }
            return java.util.List.copyOf(out);
        } catch (Exception e) {
            return List.of();
        }
    }


    @Override
    public List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
        return null;
    }

    //----------helpers----------------------------------
    private ReplayEvent mapDocToReplayEvent(org.bson.Document d) {
        if (d == null) {
            return null;
        }

        String id = d.getString("ingestionId");
        String payload = d.getString("payload");

        java.util.Date createdAt = d.getDate("createdAt");
        java.time.Instant ts = (createdAt == null)
                ? null
                : createdAt.toInstant();

        return new ReplayEvent(id, payload, ts);
    }
}
