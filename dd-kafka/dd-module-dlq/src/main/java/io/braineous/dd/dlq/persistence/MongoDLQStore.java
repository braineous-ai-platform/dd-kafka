package io.braineous.dd.dlq.persistence;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MongoDLQStore implements DLQStore {
    private final java.util.concurrent.atomic.AtomicBoolean domainIndexed =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private final java.util.concurrent.atomic.AtomicBoolean systemIndexed =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    @Inject
    com.mongodb.client.MongoClient mongoClient;

    private void ensureIndexes(com.mongodb.client.MongoCollection<org.bson.Document> col) {

        // unique dlqId ONLY when dlqId exists
        col.createIndex(
                new org.bson.Document("dlqId", 1),
                new com.mongodb.client.model.IndexOptions()
                        .unique(true)
                        .partialFilterExpression(new org.bson.Document("dlqId", new org.bson.Document("$exists", true)))
        );

        col.createIndex(new org.bson.Document("createdAt", -1));
        col.createIndex(new org.bson.Document("payloadSha256", 1));
    }

    private com.mongodb.client.MongoCollection<org.bson.Document> domainCol() {
        return mongoClient
                .getDatabase("dd")
                .getCollection("dlq_domain");
    }

    private com.mongodb.client.MongoCollection<org.bson.Document> systemCol() {
        return mongoClient
                .getDatabase("dd")
                .getCollection("dlq_system");
    }

    @Override
    public void storeDomainFailure(String payload) {
        if (payload == null || payload.trim().isEmpty()) return;

        if (domainIndexed.compareAndSet(false, true)) {
            ensureIndexes(domainCol());
        }

        String dlqId = newDlqId();
        org.bson.Document doc = new org.bson.Document()
                .append("dlqId", dlqId)
                .append("kind", "domain")

                //very important meta data to map a system event (kafka)
                //to a business event (CGO)
                .append("createdAt", java.util.Date.from(java.time.Instant.now()))

                .append("payloadSha256", sha256(payload))
                .append("payload", payload);

        domainCol().insertOne(doc);
        Console.log("dlq_store_domain_ok", dlqId);
    }

    @Override
    public void storeSystemFailure(String payload) {
        if (payload == null || payload.trim().isEmpty()) return;

        if (systemIndexed.compareAndSet(false, true)) {
            ensureIndexes(systemCol());
        }

        String dlqId = newDlqId();
        org.bson.Document doc = new org.bson.Document()
                .append("dlqId", dlqId)
                .append("kind", "system")

                //very important meta data to map a system event (kafka)
                //to a business event (CGO)
                .append("createdAt", java.util.Date.from(java.time.Instant.now()))

                .append("payloadSha256", sha256(payload))
                .append("payload", payload);

        systemCol().insertOne(doc);
        Console.log("dlq_store_system_ok", dlqId);
    }

    @Override
    public JsonArray findSystemFailureByTimeWindow(String fromTime, String toTime) {
        return findByTimeWindow(systemCol(), fromTime, toTime);
    }

    @Override
    public JsonObject findSystemFailureById(String dlqId) {
        return findById(systemCol(), dlqId);
    }

    @Override
    public JsonArray findDomainFailureByTimeWindow(String fromTime, String toTime) {
        return findByTimeWindow(domainCol(), fromTime, toTime);
    }

    @Override
    public JsonObject findDomainFailureById(String dlqId) {
        return findById(domainCol(), dlqId);
    }



    //-----helpers----------------------------------------------------
    private static String newDlqId() {
        return "DD-DLQ-" + java.util.UUID.randomUUID();
    }

    private static String sha256(String s) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            var sb = new StringBuilder(h.length * 2);
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonArray findByTimeWindow(
            com.mongodb.client.MongoCollection<org.bson.Document> col,
            String fromTime,
            String toTime
    ) {
        JsonArray arr = new JsonArray();

        if (fromTime == null || toTime == null) {
            return arr;
        }

        java.time.Instant from;
        java.time.Instant to;

        try {
            from = java.time.Instant.parse(fromTime);
            to   = java.time.Instant.parse(toTime);
        } catch (Exception e) {
            return arr;
        }

        org.bson.Document filter = new org.bson.Document("createdAt",
                new org.bson.Document("$gte", java.util.Date.from(from))
                        .append("$lte", java.util.Date.from(to))
        );

        com.mongodb.client.MongoCursor<org.bson.Document> it =
                col.find(filter)
                        .sort(new org.bson.Document("createdAt", -1))
                        .iterator();

        try {
            while (it.hasNext()) {
                org.bson.Document d = it.next();
                arr.add(
                        com.google.gson.JsonParser
                                .parseString(d.toJson())
                                .getAsJsonObject()
                );
            }
        } finally {
            it.close();
        }

        return arr;
    }

    private JsonObject findById(
            com.mongodb.client.MongoCollection<org.bson.Document> col,
            String dlqId
    ) {
        if (dlqId == null || dlqId.trim().isEmpty()) {
            return new JsonObject();
        }

        org.bson.Document d =
                col.find(new org.bson.Document("dlqId", dlqId))
                        .first();

        if (d == null) {
            return new JsonObject();
        }

        return com.google.gson.JsonParser
                .parseString(d.toJson())
                .getAsJsonObject();
    }
}
