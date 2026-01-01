package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.models.cgo.graph.GraphBuilder;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.observe.Console;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DDEventOrchestratorTest {

    @BeforeEach
    void setup() {
        GraphBuilder.getInstance().clear();
    }

    @Test
    void orchestrate_builds_atomic_fact_nodes_for_dd_event_array() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();
        GraphView view0 = orch.orchestrate(events);

        assertNotNull(view0);
        assertTrue(view0 instanceof GraphSnapshot);

        GraphSnapshot view = (GraphSnapshot) view0;

        Console.log("test.dd.orchestrate.out.nodes", "" + view.nodes().size());
        Console.log("test.dd.orchestrate.out.edges", "" + view.edges().size());
        Console.log("test.dd.orchestrate.out.snapshotHash", view.snapshotHash());

        // Node invariant: contains fact-001
        Fact f = view.getFactById("fact-001");
        assertNotNull(f, "Expected GraphSnapshot to contain Fact with id fact-001");

        assertEquals("fact-001", f.getId());
        assertEquals("kafka_payload_bytes", f.getText());
        assertEquals("atomic", f.getMode());
    }

    @Test
    void orchestrate_returns_empty_graph_for_empty_input() {
        JsonArray events = new JsonArray();

        Console.log("test.dd.orchestrate.empty.in", events.toString());

        DDEventOrchestrator orch = new DDEventOrchestrator();
        GraphView view0 = orch.orchestrate(events);

        assertNotNull(view0);
        assertTrue(view0 instanceof GraphSnapshot);

        GraphSnapshot view = (GraphSnapshot) view0;

        Console.log("test.dd.orchestrate.empty.out.nodes", "" + view.nodes().size());
        Console.log("test.dd.orchestrate.empty.out.edges", "" + view.edges().size());

        assertEquals(0, view.nodes().size());
        assertEquals(0, view.edges().size());
    }

    @Test
    void orchestrate_is_deterministic_for_same_input_same_snapshotHash() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(events);
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(events);

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.determinism.v1.nodes", "" + v1.nodes().size());
        Console.log("test.dd.orchestrate.determinism.v2.nodes", "" + v2.nodes().size());
        Console.log("test.dd.orchestrate.determinism.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.determinism.v2.hash", v2.snapshotHash());

        assertEquals(v1.nodes().size(), v2.nodes().size());
        assertEquals(v1.edges().size(), v2.edges().size());

        // deterministic keys
        assertEquals(v1.nodes().keySet(), v2.nodes().keySet());
        assertEquals(v1.edges().keySet(), v2.edges().keySet());

        // strongest determinism signal
        assertEquals(v1.snapshotHash().getValue(), v2.snapshotHash().getValue());
    }

    @Test
    void orchestrate_ignores_garbage_elements_and_keeps_valid_event_fact() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();
        GraphSnapshot view = (GraphSnapshot) orch.orchestrate(events);

        assertNotNull(view);

        Console.log("test.dd.orchestrate.mixed.out.nodes", "" + view.nodes().size());
        Console.log("test.dd.orchestrate.mixed.out.edges", "" + view.edges().size());

        // valid event survives
        assertNotNull(view.getFactById("fact-OK"));

        // garbage did not create extra nodes
        assertEquals(1, view.nodes().size(), "Expected only the valid event to produce a node");
    }


    @Test
    void orchestrate_sameEventTwice_shouldDedup_and_stableSnapshot() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();
        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(events);

        assertNotNull(v1);
        Console.log("test.dd.orchestrate.dup.out.nodes", "" + v1.nodes().size());
        Console.log("test.dd.orchestrate.dup.out.hash", v1.snapshotHash());

        // Dedup invariant
        assertEquals(1, v1.nodes().size(), "Duplicate events must dedupe to a single atomic fact");
        assertNotNull(v1.getFactById("fact-DEDUP"));

        // Determinism on re-run
        GraphBuilder.getInstance().clear();
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(events);

        Console.log("test.dd.orchestrate.dup.rerun.nodes", "" + v2.nodes().size());
        Console.log("test.dd.orchestrate.dup.rerun.hash", v2.snapshotHash());

        assertEquals(v1.nodes().keySet(), v2.nodes().keySet());
        assertEquals(v1.snapshotHash().getValue(), v2.snapshotHash().getValue());
    }

    @Test
    void orchestrate_is_order_invariant_for_same_semantic_events_same_snapshotHash() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(events1);
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(events2);

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.orderInvariant.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.orderInvariant.v2.hash", v2.snapshotHash());

        // same semantic content => same snapshot hash
        assertEquals(v1.snapshotHash().getValue(), v2.snapshotHash().getValue());

        // and both facts exist
        assertNotNull(v1.getFactById("fact-A"));
        assertNotNull(v1.getFactById("fact-B"));
        assertNotNull(v2.getFactById("fact-A"));
        assertNotNull(v2.getFactById("fact-B"));
    }

    @Test
    void orchestrate_hash_does_not_change_when_payload_changes_for_same_fact_id() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(arr(e1));
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(arr(e2));

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.hashSameOnPayloadChange.v2.hash", v2.snapshotHash());

        // Phase-1: snapshotHash depends on node/edge IDs only.
        // Same fact id => same snapshotHash even if payload differs.
        assertEquals(
                v1.snapshotHash().getValue(),
                v2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => payload change with same fact id must NOT change snapshotHash"
        );

        // sanity: fact exists in both
        assertNotNull(v1.getFactById("fact-001"));
        assertNotNull(v2.getFactById("fact-001"));
    }


    @Test
    void orchestrate_hash_changes_when_fact_ids_change() {
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(arr(e1));
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(arr(e2));

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.hashChangeById.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.hashChangeById.v2.hash", v2.snapshotHash());

        assertNotEquals(
                v1.snapshotHash().getValue(),
                v2.snapshotHash().getValue(),
                "If fact IDs change, snapshotHash must change (ID-only hashing contract)"
        );
    }

    @Test
    void orchestrate_hash_does_not_change_when_payload_changes_but_fact_id_same() {
        // same fact id
        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        // payload #1
        JsonObject payload1 = new JsonObject();
        payload1.addProperty("encoding", "base64");
        payload1.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka);
        e1.add("payload", payload1);

        // payload #2 (different bytes)
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(events1);
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(events2);

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.payloadChangeSameId.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v2.hash", v2.snapshotHash());

        // Phase-1 contract: snapshotHash depends on node/edge IDs only
        // so changing payload while keeping the same fact id must NOT change the hash.
        assertEquals(
                v1.snapshotHash().getValue(),
                v2.snapshotHash().getValue(),
                "With ID-only hashing, same fact id must yield same snapshotHash even if payload differs"
        );

        // sanity: the fact exists in both
        assertNotNull(v1.getFactById("fact-001"));
        assertNotNull(v2.getFactById("fact-001"));
    }

    @Test
    void orchestrate_payload_change_same_fact_id_should_not_change_snapshotHash() {
        // same fact id
        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 1);
        kafka.addProperty("offset", 42);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "fact-001");

        // payload #1
        JsonObject payload1 = new JsonObject();
        payload1.addProperty("encoding", "base64");
        payload1.addProperty("value", "AAECAwQFBgcICQ==");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka);
        e1.add("payload", payload1);

        // payload #2 (different bytes)
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

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(events1);
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(events2);

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.payloadChangeSameId.v1.nodes", "" + v1.nodes().size());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v2.nodes", "" + v2.nodes().size());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v1.edges", "" + v1.edges().size());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v2.edges", "" + v2.edges().size());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.payloadChangeSameId.v2.hash", v2.snapshotHash());

        // Phase-1 contract: snapshotHash depends on node/edge IDs only.
        // Same fact id => same snapshotHash even if payload differs.
        assertEquals(
                v1.snapshotHash().getValue(),
                v2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => payload change with same fact id must NOT change snapshotHash"
        );

        // sanity: fact exists in both
        assertNotNull(v1.getFactById("fact-001"));
        assertNotNull(v2.getFactById("fact-001"));
    }

    @Test
    void orchestrate_hash_changes_when_fact_id_changes_even_if_payload_same() {
        // shared payload (same bytes)
        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "base64");
        payload.addProperty("value", "AAECAwQFBgcICQ==");

        // event #1 => fact-001
        JsonObject kafka1 = new JsonObject();
        kafka1.addProperty("topic", "requests");
        kafka1.addProperty("partition", 1);
        kafka1.addProperty("offset", 42);
        kafka1.addProperty("timestamp", 1767114000123L);
        kafka1.addProperty("key", "fact-001");

        JsonObject e1 = new JsonObject();
        e1.add("kafka", kafka1);
        e1.add("payload", payload);

        // event #2 => fact-002 (only change is the id)
        JsonObject kafka2 = new JsonObject();
        kafka2.addProperty("topic", "requests");
        kafka2.addProperty("partition", 1);
        kafka2.addProperty("offset", 42);
        kafka2.addProperty("timestamp", 1767114000123L);
        kafka2.addProperty("key", "fact-002");

        JsonObject e2 = new JsonObject();
        e2.add("kafka", kafka2);
        e2.add("payload", payload);

        DDEventOrchestrator orch = new DDEventOrchestrator();

        GraphSnapshot v1 = (GraphSnapshot) orch.orchestrate(arr(e1));
        GraphSnapshot v2 = (GraphSnapshot) orch.orchestrate(arr(e2));

        assertNotNull(v1);
        assertNotNull(v2);

        Console.log("test.dd.orchestrate.hashChangeById.v1.hash", v1.snapshotHash());
        Console.log("test.dd.orchestrate.hashChangeById.v2.hash", v2.snapshotHash());

        assertNotEquals(
                v1.snapshotHash().getValue(),
                v2.snapshotHash().getValue(),
                "Phase-1: ID-only hashing => changing fact id must change snapshotHash"
        );

        // sanity
        assertNotNull(v1.getFactById("fact-001"));
        assertNotNull(v2.getFactById("fact-002"));
    }




    private static JsonArray arr(JsonObject obj) {
        JsonArray a = new JsonArray();
        a.add(obj);
        return a;
    }


}
