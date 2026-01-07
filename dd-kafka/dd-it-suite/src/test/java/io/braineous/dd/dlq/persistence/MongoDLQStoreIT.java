package io.braineous.dd.dlq.persistence;

import org.junit.jupiter.api.Assertions;

@io.quarkus.test.junit.QuarkusTest
public class MongoDLQStoreIT {

    @jakarta.inject.Inject
    com.mongodb.client.MongoClient mongo;

    @jakarta.inject.Inject
    DLQStore store;

    @org.junit.jupiter.api.BeforeEach
    void setup() {
        var db = mongo.getDatabase("dd");

        db.getCollection("dlq_system").drop();
        db.getCollection("dlq_domain").drop();
    }


    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        var db = mongo.getDatabase("dd");

        db.getCollection("dlq_system").drop();
        db.getCollection("dlq_domain").drop();
    }


    @org.junit.jupiter.api.Test
    void storeSystemFailure_insertsDocument() {
        String payload = "it-" + java.util.UUID.randomUUID();

        // act
        store.storeSystemFailure(payload);

        // assert (read-back)
        var doc = mongo.getDatabase("dd")
                .getCollection("dlq_system")
                .find(com.mongodb.client.model.Filters.eq("payload", payload))
                .first();

        org.junit.jupiter.api.Assertions.assertNotNull(doc);
        org.junit.jupiter.api.Assertions.assertEquals(payload, doc.getString("payload"));
        Assertions.assertNotNull(doc.getString("dlqId"));
        Assertions.assertNotNull(doc.getDate("createdAt"));      // ← correct
        Assertions.assertNotNull(doc.getString("payloadSha256"));
        Assertions.assertEquals(payload, doc.getString("payload"));


    }

    @org.junit.jupiter.api.Test
    void storeDomainFailure_insertsDocument() {
        String payload = "it-" + java.util.UUID.randomUUID();

        store.storeDomainFailure(payload);

        var doc = mongo.getDatabase("dd")
                .getCollection("dlq_domain")
                .find(com.mongodb.client.model.Filters.eq("payload", payload))
                .first();

        org.junit.jupiter.api.Assertions.assertNotNull(doc);
        org.junit.jupiter.api.Assertions.assertEquals(payload, doc.getString("payload"));
        Assertions.assertNotNull(doc.getString("dlqId"));
        Assertions.assertNotNull(doc.getDate("createdAt"));      // ← correct
        Assertions.assertNotNull(doc.getString("payloadSha256"));
        Assertions.assertEquals(payload, doc.getString("payload"));
    }

    @org.junit.jupiter.api.Test
    void indexes_areCreated_onFirstWrite_forBothCollections() {
        // act (first write triggers ensureIndexes)
        store.storeSystemFailure("it-" + java.util.UUID.randomUUID());
        store.storeDomainFailure("it-" + java.util.UUID.randomUUID());

        // assert system indexes
        var sysIndexes = mongo.getDatabase("dd").getCollection("dlq_system")
                .listIndexes().into(new java.util.ArrayList<>());

        org.junit.jupiter.api.Assertions.assertTrue(
                sysIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    var partial = (org.bson.Document) d.get("partialFilterExpression");
                    var unique = d.getBoolean("unique", false);
                    return key != null
                            && Integer.valueOf(1).equals(key.getInteger("dlqId"))
                            && unique
                            && partial != null
                            && partial.containsKey("dlqId");
                }),
                "Expected unique partial index on dlqId for dlq_system"
        );

        org.junit.jupiter.api.Assertions.assertTrue(
                sysIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    return key != null && Integer.valueOf(-1).equals(key.getInteger("createdAt"));
                }),
                "Expected index on createdAt desc for dlq_system"
        );

        org.junit.jupiter.api.Assertions.assertTrue(
                sysIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    return key != null && Integer.valueOf(1).equals(key.getInteger("payloadSha256"));
                }),
                "Expected index on payloadSha256 for dlq_system"
        );

        // assert domain indexes
        var domIndexes = mongo.getDatabase("dd").getCollection("dlq_domain")
                .listIndexes().into(new java.util.ArrayList<>());

        org.junit.jupiter.api.Assertions.assertTrue(
                domIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    var partial = (org.bson.Document) d.get("partialFilterExpression");
                    var unique = d.getBoolean("unique", false);
                    return key != null
                            && Integer.valueOf(1).equals(key.getInteger("dlqId"))
                            && unique
                            && partial != null
                            && partial.containsKey("dlqId");
                }),
                "Expected unique partial index on dlqId for dlq_domain"
        );

        org.junit.jupiter.api.Assertions.assertTrue(
                domIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    return key != null && Integer.valueOf(-1).equals(key.getInteger("createdAt"));
                }),
                "Expected index on createdAt desc for dlq_domain"
        );

        org.junit.jupiter.api.Assertions.assertTrue(
                domIndexes.stream().anyMatch(d -> {
                    var key = (org.bson.Document) d.get("key");
                    return key != null && Integer.valueOf(1).equals(key.getInteger("payloadSha256"));
                }),
                "Expected index on payloadSha256 for dlq_domain"
        );
    }

    @org.junit.jupiter.api.Test
    void blankOrNullPayload_isIgnored_noInsert() {
        var db = mongo.getDatabase("dd");

        // act
        store.storeSystemFailure(null);
        store.storeSystemFailure("   ");
        store.storeDomainFailure(null);
        store.storeDomainFailure("\n\t  ");

        // assert: collections remain empty
        long sysCount = db.getCollection("dlq_system").countDocuments();
        long domCount = db.getCollection("dlq_domain").countDocuments();

        org.junit.jupiter.api.Assertions.assertEquals(0L, sysCount);
        org.junit.jupiter.api.Assertions.assertEquals(0L, domCount);
    }


}

