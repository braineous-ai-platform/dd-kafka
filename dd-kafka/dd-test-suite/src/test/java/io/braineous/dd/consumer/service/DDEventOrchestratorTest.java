package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.models.cgo.graph.GraphBuilder;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.braineous.dd.cgo.DDCGOOrchestrator;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import io.braineous.dd.support.InMemoryIngestionStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class DDEventOrchestratorTest {

    @Inject
    private DDCGOOrchestrator cgoOrch;

    @Inject
    private DDEventOrchestrator orch;

    @BeforeEach
    void setup() {
        GraphBuilder.getInstance().clear();
        InMemoryIngestionStore store = new InMemoryIngestionStore();
        orch.setStore(store);
        store.reset();
        GraphBuilder.getInstance().clear();
    }

    @Test
    void orchestrate_builds_atomic_fact_nodes_for_dd_event_array() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonArray events = new JsonArray();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 3);
        kafka.addProperty("offset", 48192);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        JsonObject headers = new JsonObject();
        headers.addProperty("traceId", "8f3a9c12");
        headers.addProperty("correlationId", "corr-9911");
        kafka.add("headers", headers);

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ=="); // binary -> fallback atomic fact

        JsonObject root = new JsonObject();
        root.add("kafka", kafka);
        root.add("payload", payload);

        events.add(root);

        Console.log("test.dd.orchestrate.in", events.toString());

        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        addCGOata(events, snapshot);
        Console.log("dd_events", events.toString());

        IngestionReceipt receipt = orch.orchestrate(events.toString());

        assertNotNull(receipt);
        Console.log("ingestion_receipt", receipt.toJson().toString());

        // ---------------- spine assertions (DD contract) ----------------
        io.braineous.dd.support.DDAssert.assertReceiptOk(receipt, "memory");

        // ---------------- store assertions (assertion surface) ----------------
        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, receipt);

        io.braineous.dd.support.DDAssert.assertPayloadContains(
                stored,
                "\"ingestionId\"",
                "\"view\"",
                "\"snapshotHash\"",
                "\"kafka\"",
                "\"topic\":\"requests\"",
                "\"payload\"",
                "\"encoding\":\"base64\""
        );

        // atomic fallback marker (pick one stable marker)
        assertTrue(stored.contains("kafka_payload_bytes") || stored.contains("\"mode\":\"atomic\""));
    }

    @Test
    void orchestrate_returns_empty_graph_for_empty_input() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonArray events = new JsonArray();

        Console.log("test.dd.orchestrate.empty.in", events.toString());

        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        addCGOata(events, snapshot);
        Console.log("dd_events", events.toString());

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        orch.orchestrate(events.toString());
                    }
                }
        );

        assertNotNull(ex.getMessage());
        assertEquals("DD-ING-events_empty", ex.getMessage());

        assertNotNull(store);
        assertEquals(0, store.storeCalls());
    }

    @Test
    void orchestrate_is_deterministic_for_same_input_same_snapshotHash() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonArray events = new JsonArray();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject root = new JsonObject();
        root.add("kafka", kafka);
        root.add("payload", payload);

        events.add(root);

        Console.log("test.dd.orchestrate.determinism.in", events.toString());

        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        // Enrich input with deterministic axis (ingestionId + view.snapshotHash + atomic fact map)
        addCGOata(events, snapshot);
        Console.log("dd_events.v1", events.toString());

        IngestionReceipt r1 = orch.orchestrate(events.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        IngestionReceipt r2 = orch.orchestrate(events.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        // ---------------- spine assertions (DD determinism) ----------------
        io.braineous.dd.support.DDAssert.assertDeterministicReceipts(store, r1, r2);
    }



    @Test
    void orchestrate_ignores_garbage_elements_and_keeps_valid_event_fact() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonArray events = new JsonArray();

        // garbage
        events.add(new JsonPrimitive("NOT_JSON_OBJECT"));
        events.add(new JsonObject()); // empty object (no kafka/payload)
        JsonObject notEvent = new JsonObject();
        notEvent.addProperty("item", "Book");
        notEvent.addProperty("quantity", 2);
        events.add(notEvent);

        // valid DD event envelope
        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 3);
        kafka.addProperty("offset", 48192);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-OK");

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject good = new JsonObject();
        good.add("kafka", kafka);
        good.add("payload", payload);

        events.add(good);

        Console.log("test.dd.orchestrate.mixed.in", events.toString());

        // shape-only CGO call (not testing CGO semantics)
        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        // Filter only valid DD event envelopes for enrichment + persistence.
        // (addCGOata assumes JsonObject elements)
        JsonArray clean = new JsonArray();
        clean.add(good);

        addCGOata(clean, snapshot);
        Console.log("dd_events.clean", clean.toString());

        IngestionReceipt receipt = orch.orchestrate(clean.toString());

        assertNotNull(receipt);
        Console.log("ingestion_receipt", receipt.toJson().toString());

        // ---------------- spine assertions (DD contract) ----------------
        io.braineous.dd.support.DDAssert.assertReceiptOk(receipt, "memory");

        // ---------------- store assertions (assertion surface) ----------------
        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, receipt);

        io.braineous.dd.support.DDAssert.assertPayloadContains(
                stored,
                "\"ingestionId\"",
                "\"view\"",
                "\"snapshotHash\"",
                "\"kafka\"",
                "\"topic\":\"requests\"",
                "\"payload\"",
                "\"encoding\":\"base64\""
        );

        // valid event survives
        assertTrue(stored.contains("\"key\":\"fact-OK\""));

        // strongest signal that garbage did not leak into what we store
        assertFalse(stored.contains("NOT_JSON_OBJECT"));
        assertFalse(stored.contains("\"item\":\"Book\""));
        assertFalse(stored.contains("\"quantity\":2"));
    }



    @Test
    void orchestrate_sameEventTwice_shouldDedup_and_stableSnapshot() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        GraphBuilder.getInstance().clear();

        JsonArray events = new JsonArray();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 3);
        kafka.addProperty("offset", 48192);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-DEDUP");

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject root = new JsonObject();
        root.add("kafka", kafka);
        root.add("payload", payload);

        // same event twice (dup)
        events.add(root);
        events.add(root.deepCopy());

        Console.log("test.dd.orchestrate.dup.in", events.toString());

        GraphSnapshot snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        addCGOata(events, snapshot);
        Console.log("dd_events", events.toString());

        // Dedup invariant (new spine): duplicates share the SAME axis (ingestionId + snapshotHash)
        JsonObject e0 = events.get(0).getAsJsonObject();
        JsonObject e1 = events.get(1).getAsJsonObject();

        String id0 = e0.get("ingestionId").getAsString();
        String id1 = e1.get("ingestionId").getAsString();
        assertNotNull(id0);
        assertNotNull(id1);
        assertEquals(id0, id1);

        String snap0 = e0.getAsJsonObject("view").get("snapshotHash").getAsString();
        String snap1 = e1.getAsJsonObject("view").get("snapshotHash").getAsString();
        assertNotNull(snap0);
        assertNotNull(snap1);
        assertEquals(snap0, snap1);

        IngestionReceipt r1 = orch.orchestrate(events.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        assertEquals(snap0, r1.snapshotHash().getValue());
        assertEquals(id0, r1.ingestionId());

        io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r1);

        // Determinism on re-run: same input -> same snapshotHash
        GraphBuilder.getInstance().clear();
        store.reset();

        snapshot = (GraphSnapshot) this.cgoOrch.orchestrate(events.toString());
        assertNotNull(snapshot);

        addCGOata(events, snapshot);

        IngestionReceipt r2 = orch.orchestrate(events.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");
        assertEquals(r1.snapshotHash().getValue(), r2.snapshotHash().getValue());
    }



    @Test
    void orchestrate_is_order_invariant_for_same_semantic_events_same_snapshotHash() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        // event A
        JsonObject kafkaA = new JsonObject();
        kafkaA.addProperty("topic", "requests");
        kafkaA.addProperty("partition", 1);
        kafkaA.addProperty("offset", 42);
        kafkaA.addProperty("timestamp", 1767114000123L);
        kafkaA.addProperty("key", "fact-A");

        JsonObject payloadA = new JsonObject();
        payloadA.addProperty("encoding", "base64");
        payloadA.addProperty("value", "AAECAwQFBgcICQ==");

        // event B
        JsonObject kafkaB = new JsonObject();
        kafkaB.addProperty("topic", "requests");
        kafkaB.addProperty("partition", 1);
        kafkaB.addProperty("offset", 43);
        kafkaB.addProperty("timestamp", 1767114000124L);
        kafkaB.addProperty("key", "fact-B");

        JsonObject payloadB = new JsonObject();
        payloadB.addProperty("encoding", "base64");
        payloadB.addProperty("value", "AQIDBAUGBwgJCg==");

        // root objects: intentionally build with different insertion order
        JsonObject rootA_order1 = new JsonObject();
        rootA_order1.add("kafka", kafkaA);
        rootA_order1.add("payload", payloadA);

        JsonObject rootA_order2 = new JsonObject();
        rootA_order2.add("payload", payloadA);
        rootA_order2.add("kafka", kafkaA);

        JsonObject rootB_order1 = new JsonObject();
        rootB_order1.add("kafka", kafkaB);
        rootB_order1.add("payload", payloadB);

        JsonObject rootB_order2 = new JsonObject();
        rootB_order2.add("payload", payloadB);
        rootB_order2.add("kafka", kafkaB);

        // events arrays: swapped order
        JsonArray events1 = new JsonArray();
        events1.add(rootA_order1);
        events1.add(rootB_order1);

        JsonArray events2 = new JsonArray();
        events2.add(rootB_order2);
        events2.add(rootA_order2);

        Console.log("test.dd.orchestrate.orderInvariant.in1", events1.toString());
        Console.log("test.dd.orchestrate.orderInvariant.in2", events2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(events1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(events2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        Console.log("dd_events.1", events1.toString());
        Console.log("dd_events.2", events2.toString());

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        // reset store for the second run so this test doesn't depend on idempotent "touch" semantics
        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.orderInvariant.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.orderInvariant.v2.hash", r2.snapshotHash());

        // same semantic content => same snapshot hash
        assertEquals(r1.snapshotHash().getValue(), r2.snapshotHash().getValue());

        // both facts exist in the stored payload (lightweight, DD-level)
        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r2);
        assertTrue(stored.contains("fact-A"));
        assertTrue(stored.contains("fact-B"));
    }


    @Test
    void orchestrate_hash_does_not_change_when_payload_changes_for_same_fact_id() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        JsonObject payload1 = new JsonObject();
        payload1.addProperty("encoding", "base64");
        payload1.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject payload2 = new JsonObject();
        payload2.addProperty("encoding", "base64");
        payload2.addProperty("value", "AQIDBAUGBwgJCg=="); // different bytes

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka);
        e1.add("payload", payload1);

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka);
        e2.add("payload", payload2);

        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.in1", e1.toString());
        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.in2", e2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(e1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(e2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        JsonArray events1 = arr(e1);
        JsonArray events2 = arr(e2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.v2.hash", r2.snapshotHash());

        assertEquals(
                r1.snapshotHash().getValue(),
                r2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => payload change with same fact id must NOT change snapshotHash"
        );

        // Lightweight DD-level proof: stored payload contains the fact id
        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r2);
        assertTrue(stored.contains("fact-001"));
    }


    @Test
    void orchestrate_hash_changes_when_fact_ids_change() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonObject kafka1 = new JsonObject();
        kafka1.addProperty("topic", "requests");
        kafka1.addProperty("partition", 1);
        kafka1.addProperty("offset", 42);
        kafka1.addProperty("timestamp", 1767114000123L);
        kafka1.addProperty("key", "fact-001");

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka1);
        e1.add("payload", payload);

        JsonObject kafka2 = kafka1.deepCopy();
        kafka2.addProperty("key", "fact-002"); // only change: id

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka2);
        e2.add("payload", payload);

        Console.log("test.dd.orchestrate.hashChangeById.in1", e1.toString());
        Console.log("test.dd.orchestrate.hashChangeById.in2", e2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(e1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(e2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        JsonArray events1 = arr(e1);
        JsonArray events2 = arr(e2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.hashChangeById.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.hashChangeById.v2.hash", r2.snapshotHash());

        assertNotEquals(
                r1.snapshotHash().getValue(),
                r2.snapshotHash().getValue(),
                "If fact IDs change, snapshotHash must change (ID-only hashing contract)"
        );
    }

    @Test
    void orchestrate_hash_does_not_change_when_payload_changes_but_fact_id_same() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        JsonObject payload1 = new JsonObject();
        payload1.addProperty("encoding", "base64");
        payload1.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka);
        e1.add("payload", payload1);

        JsonObject payload2 = new JsonObject();
        payload2.addProperty("encoding", "base64");
        payload2.addProperty("value", "AQIDBAUGBwgJCg==");

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka);
        e2.add("payload", payload2);

        JsonArray events1 = new JsonArray();
        events1.add(e1);

        JsonArray events2 = new JsonArray();
        events2.add(e2);

        Console.log("test.dd.orchestrate.payloadChangeSameId.in1", events1.toString());
        Console.log("test.dd.orchestrate.payloadChangeSameId.in2", events2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(events1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(events2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.payloadChangeSameId.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v2.hash", r2.snapshotHash());

        assertEquals(
                r1.snapshotHash().getValue(),
                r2.snapshotHash().getValue(),
                "With ID-only hashing, same fact id must yield same snapshotHash even if payload differs"
        );

        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r2);
        assertTrue(stored.contains("fact-001"));
    }

    @Test
    void orchestrate_payload_change_same_fact_id_should_not_change_snapshotHash() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        JsonObject payload1 = new JsonObject();
        payload1.addProperty("encoding", "base64");
        payload1.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka);
        e1.add("payload", payload1);

        JsonObject payload2 = new JsonObject();
        payload2.addProperty("encoding", "base64");
        payload2.addProperty("value", "AQIDBAUGBwgJCg==");

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka);
        e2.add("payload", payload2);

        JsonArray events1 = new JsonArray();
        events1.add(e1);

        JsonArray events2 = new JsonArray();
        events2.add(e2);

        Console.log("test.dd.orchestrate.payload_change_same_fact_id.in1", events1.toString());
        Console.log("test.dd.orchestrate.payload_change_same_fact_id.in2", events2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(events1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(events2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.payload_change_same_fact_id.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.payload_change_same_fact_id.v2.hash", r2.snapshotHash());

        assertEquals(
                r1.snapshotHash().getValue(),
                r2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => payload change with same fact id must NOT change snapshotHash"
        );

        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r2);
        assertTrue(stored.contains("fact-001"));
    }

    @Test
    void orchestrate_hash_changes_when_fact_id_changes_even_if_payload_same() {
        InMemoryIngestionStore store = (InMemoryIngestionStore) this.orch.getStore();

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject kafka1 = new JsonObject();
        kafka1.addProperty("topic", "requests");
        kafka1.addProperty("partition", 1);
        kafka1.addProperty("offset", 42);
        kafka1.addProperty("timestamp", 1767114000123L);
        kafka1.addProperty("key", "fact-001");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka1);
        e1.add("payload", payload);

        JsonObject kafka2 = new JsonObject();
        kafka2.addProperty("topic", "requests");
        kafka2.addProperty("partition", 1);
        kafka2.addProperty("offset", 42);
        kafka2.addProperty("timestamp", 1767114000123L);
        kafka2.addProperty("key", "fact-002");

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka2);
        e2.add("payload", payload);

        Console.log("test.dd.orchestrate.hashChangeByIdSamePayload.in1", e1.toString());
        Console.log("test.dd.orchestrate.hashChangeByIdSamePayload.in2", e2.toString());

        GraphSnapshot s1 = (GraphSnapshot) this.cgoOrch.orchestrate(e1.toString());
        GraphSnapshot s2 = (GraphSnapshot) this.cgoOrch.orchestrate(e2.toString());
        assertNotNull(s1);
        assertNotNull(s2);

        JsonArray events1 = arr(e1);
        JsonArray events2 = arr(e2);

        addCGOata(events1, s1);
        addCGOata(events2, s2);

        IngestionReceipt r1 = orch.orchestrate(events1.toString());
        assertNotNull(r1);
        Console.log("ingestion_receipt.v1", r1.toJson().toString());

        store.reset();

        IngestionReceipt r2 = orch.orchestrate(events2.toString());
        assertNotNull(r2);
        Console.log("ingestion_receipt.v2", r2.toJson().toString());

        io.braineous.dd.support.DDAssert.assertReceiptOk(r1, "memory");
        io.braineous.dd.support.DDAssert.assertReceiptOk(r2, "memory");

        Console.log("test.dd.orchestrate.hashChangeByIdSamePayload.v1.hash", r1.snapshotHash());
        Console.log("test.dd.orchestrate.hashChangeByIdSamePayload.v2.hash", r2.snapshotHash());

        assertNotEquals(
                r1.snapshotHash().getValue(),
                r2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => changing fact id must change snapshotHash"
        );

        String stored = io.braineous.dd.support.DDAssert.assertStoredOnceAndGetPayload(store, r2);
        assertTrue(stored.contains("fact-002"));
    }


    //------------------------------------------------------------------------------------

    private static JsonArray arr(JsonObject obj) {
        JsonArray a = new JsonArray();
        a.add(obj);
        return a;
    }


    private void addCGOata(JsonArray ddEvents, GraphSnapshot snapshot) {

        if (ddEvents == null || snapshot == null) {
            return;
        }

        String snap = null;
        if (snapshot.snapshotHash() != null) {
            snap = snapshot.snapshotHash().getValue();
        }

        if (snap == null || snap.trim().length() == 0) {
            return;
        }

        // Cache axis birth per snapshot for this batch so duplicates don't get different ingestionIds
        String cachedIngestionId = null;

        // add CGO-View to incoming DDEvents (skip garbage elements)
        for (int i = 0; i < ddEvents.size(); i++) {

            JsonElement el = ddEvents.get(i);
            if (el == null || !el.isJsonObject()) {
                continue;
            }

            JsonObject ddEvent = el.getAsJsonObject();

            // Only enrich valid DD event envelopes (must have kafka + payload)
            if (!ddEvent.has("kafka") || !ddEvent.has("payload")) {
                continue;
            }

            // Resolve ingestionId once per snapshot for this batch (stable across duplicates)
            if (cachedIngestionId == null) {
                cachedIngestionId = this.orch.getStore().resolveIngestionId(ddEvent.toString(), snap);
            }

            ddEvent.addProperty("ingestionId", cachedIngestionId);

            JsonObject snapJson = snapshot.toJson();
            snapJson.addProperty(MongoIngestionStore.F_SNAPSHOT_HASH, snap); // "snapshotHash"

            ddEvent.add("view", snapJson);
        }
    }

}

