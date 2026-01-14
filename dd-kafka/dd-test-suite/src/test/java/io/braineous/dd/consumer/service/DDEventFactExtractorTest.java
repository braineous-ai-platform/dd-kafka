package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import io.braineous.dd.cgo.DDEventFactExtractor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


public class DDEventFactExtractorTest {

    @Test
    void extract_should_create_single_fact_for_base64_binary_payload() {
        // given
        String json = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 48192,
        "timestamp": 1767114000123,
        "key": "fact-001",
        "headers": {
          "traceId": "8f3a9c12",
          "correlationId": "corr-9911"
        }
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAwQFBgcICQ=="
      }
    }
    """;

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(json);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact fact = facts.get(0);
        Console.log("fact", fact);

        assertEquals("fact-001", fact.getId());
        assertEquals("kafka_payload_bytes", fact.getText());
        assertEquals("atomic", fact.getMode());

        Set<String> attrs = fact.getAttributes();
        Console.log("attributes", attrs);

        assertTrue(attrs.contains("payload.encoding=base64"));
        assertTrue(attrs.stream().anyMatch(a -> a.startsWith("payload.base64=")));
        assertTrue(attrs.stream().anyMatch(a -> a.startsWith("payload.hex=")));

        assertTrue(attrs.contains("kafka.topic=requests"));
        assertTrue(attrs.contains("kafka.partition=3"));
        assertTrue(attrs.contains("kafka.offset=48192"));
        assertTrue(attrs.contains("kafka.key=fact-001"));
        assertTrue(attrs.contains("kafka.header.traceId=8f3a9c12"));
        assertTrue(attrs.contains("kafka.header.correlationId=corr-9911"));
    }

    @Test
    void extract_should_parse_base64_inner_json_and_return_facts_list() {
        // given: inner JSON that will be base64-encoded
        String innerJson = """
    {
      "facts": [
        { "id": "f1", "text": "A", "attributes": ["x"], "mode": "atomic" },
        { "text": "B", "attributes": ["y"] }
      ]
    }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 1,
        "offset": 42,
        "timestamp": 1767114000123,
        "key": "k1",
        "headers": { "traceId": "t-1" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(2, facts.size());

        Fact f1 = facts.get(0);
        Fact f2 = facts.get(1);

        Console.log("f1", f1);
        Console.log("f2", f2);

        // f1 keeps id from payload
        assertEquals("f1", f1.getId());
        assertEquals("A", f1.getText());
        assertTrue(f1.getAttributes().contains("x"));

        // f2 gets stable id from text (nameUUIDFromBytes(text))
        assertNotNull(f2.getId());
        assertFalse(f2.getId().isBlank());
        assertEquals("B", f2.getText());
        assertTrue(f2.getAttributes().contains("y"));

        // kafka context enrichment should be present on both
        assertTrue(f1.getAttributes().contains("kafka.topic=requests"));
        assertTrue(f1.getAttributes().contains("kafka.partition=1"));
        assertTrue(f1.getAttributes().contains("kafka.offset=42"));
        assertTrue(f1.getAttributes().contains("kafka.key=k1"));

        assertTrue(f2.getAttributes().contains("kafka.topic=requests"));
        assertTrue(f2.getAttributes().contains("kafka.partition=1"));
        assertTrue(f2.getAttributes().contains("kafka.offset=42"));
        assertTrue(f2.getAttributes().contains("kafka.key=k1"));
    }

    @Test
    void extract_should_dedupe_facts_by_id_last_wins() {
        // given
        String innerJson = """
    {
      "facts": [
        { "id": "dup", "text": "A", "attributes": ["x"] },
        { "id": "dup", "text": "B", "attributes": ["y"] }
      ]
    }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 9,
        "offset": 99,
        "timestamp": 1767114000123,
        "key": "k9",
        "headers": { "traceId": "t-9" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("dedup.fact", f);
        Console.log("dedup.attrs", f.getAttributes());

        assertEquals("dup", f.getId());

        // last wins -> "B" with ["y"]
        assertEquals("B", f.getText());
        assertTrue(f.getAttributes().contains("y"));
        assertFalse(f.getAttributes().contains("x"));
    }

    @Test
    void extract_should_parse_base64_inner_json_array_of_facts() {
        // given: inner JSON is an array
        String innerJson = """
    [
      { "id": "a1", "text": "Alpha", "attributes": ["p1"] },
      { "id": "b2", "text": "Beta",  "attributes": ["p2"], "mode": "atomic" }
    ]
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 2,
        "offset": 200,
        "timestamp": 1767114000123,
        "key": "k-array",
        "headers": { "traceId": "t-array" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(2, facts.size());

        Fact f1 = facts.get(0);
        Fact f2 = facts.get(1);

        Console.log("f1", f1);
        Console.log("f2", f2);

        assertEquals("a1", f1.getId());
        assertEquals("Alpha", f1.getText());
        assertTrue(f1.getAttributes().contains("p1"));

        assertEquals("b2", f2.getId());
        assertEquals("Beta", f2.getText());
        assertTrue(f2.getAttributes().contains("p2"));

        // kafka enrichment should exist on both
        assertTrue(f1.getAttributes().contains("kafka.topic=requests"));
        assertTrue(f2.getAttributes().contains("kafka.topic=requests"));
    }

    @Test
    void extract_should_parse_base64_inner_single_fact_object() {
        // given: inner JSON is a single fact object
        String innerJson = """
    { "id": "solo-1", "text": "SoloFact", "attributes": ["s1"], "mode": "atomic" }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 5,
        "offset": 500,
        "timestamp": 1767114000123,
        "key": "k-solo",
        "headers": { "traceId": "t-solo" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("fact", f);

        assertEquals("solo-1", f.getId());
        assertEquals("SoloFact", f.getText());
        assertEquals("atomic", f.getMode());
        assertTrue(f.getAttributes().contains("s1"));

        // kafka enrichment
        assertTrue(f.getAttributes().contains("kafka.topic=requests"));
        assertTrue(f.getAttributes().contains("kafka.partition=5"));
        assertTrue(f.getAttributes().contains("kafka.offset=500"));
        assertTrue(f.getAttributes().contains("kafka.key=k-solo"));
    }

    @Test
    void extract_should_flatten_attributes_object_into_key_value_strings() {
        // given
        String innerJson = """
    {
      "facts": [
        {
          "id": "map-1",
          "text": "AttrMap",
          "attributes": {
            "traceId": "t-123",
            "flag": true,
            "count": 7
          }
        }
      ]
    }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 7,
        "offset": 700,
        "timestamp": 1767114000123,
        "key": "k-map",
        "headers": { "traceId": "t-map" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("fact", f);
        Console.log("attrs", f.getAttributes());

        assertEquals("map-1", f.getId());
        assertEquals("AttrMap", f.getText());

        // object-map flattened
        assertTrue(f.getAttributes().contains("traceId=t-123"));
        assertTrue(f.getAttributes().contains("flag=true"));
        assertTrue(f.getAttributes().contains("count=7"));
    }

    @Test
    void extract_should_assume_base64_when_encoding_missing() {
        // given
        String innerJson = """
    { "facts": [ { "id": "e1", "text": "EncMissing", "attributes": ["ok"] } ] }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 0,
        "offset": 1,
        "timestamp": 1767114000123,
        "key": "k-enc-missing",
        "headers": { "traceId": "t-enc" }
      },
      "payload": {
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("fact", f);

        assertEquals("e1", f.getId());
        assertEquals("EncMissing", f.getText());
        assertTrue(f.getAttributes().contains("ok"));

        // kafka enrichment still
        assertTrue(f.getAttributes().contains("kafka.key=k-enc-missing"));
    }

    @Test
    void extract_should_return_empty_list_for_invalid_json_envelope() {
        // given
        String garbage = "}{ not-json :::";

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(garbage);

        // then
        Console.log("facts", facts);
        assertNotNull(facts);
        assertTrue(facts.isEmpty());
    }

    @Test
    void extract_should_return_empty_list_when_payload_value_missing() {
        // given
        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 4,
        "offset": 444,
        "timestamp": 1767114000123,
        "key": "k-missing-value",
        "headers": { "traceId": "t-missing" }
      },
      "payload": {
        "encoding": "base64"
      }
    }
    """;

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts", facts);
        assertNotNull(facts);
        assertTrue(facts.isEmpty());
    }

    @Test
    void extract_should_return_empty_list_for_invalid_base64_payload() {
        // given
        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 6,
        "offset": 666,
        "timestamp": 1767114000123,
        "key": "k-bad-b64",
        "headers": { "traceId": "t-bad-b64" }
      },
      "payload": {
        "encoding": "base64",
        "value": "###not-base64###"
      }
    }
    """;

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts", facts);
        assertNotNull(facts);
        assertTrue(facts.isEmpty());
    }

    @Test
    void extract_should_be_deterministic_across_replays_same_input_same_ids() {
        // given: one fact with NO id -> extractor generates stable id from text
        String innerJson = """
    {
      "facts": [
        { "text": "DeterministicText", "attributes": ["a1"] }
      ]
    }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 1,
        "offset": 101,
        "timestamp": 1767114000123,
        "key": "k-replay",
        "headers": { "traceId": "t-replay" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when: replay same message twice
        List<Fact> run1 = extractor.extract(envelope);
        List<Fact> run2 = extractor.extract(envelope);

        // then
        Console.log("run1", run1);
        Console.log("run2", run2);

        assertEquals(1, run1.size());
        assertEquals(1, run2.size());

        Fact f1 = run1.get(0);
        Fact f2 = run2.get(0);

        Console.log("id1", f1.getId());
        Console.log("id2", f2.getId());

        assertNotNull(f1.getId());
        assertNotNull(f2.getId());
        assertEquals(f1.getId(), f2.getId()); // determinism lock ✅
        assertEquals("DeterministicText", f1.getText());
        assertEquals("DeterministicText", f2.getText());
    }

    @Test
    void extract_should_generate_different_ids_for_different_texts_when_id_missing() {
        // given: two facts, no ids, different text
        String innerJson = """
    {
      "facts": [
        { "text": "Text-One" },
        { "text": "Text-Two" }
      ]
    }
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(innerJson.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 1,
        "offset": 202,
        "timestamp": 1767114000123,
        "key": "k-diff",
        "headers": { "traceId": "t-diff" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts", facts);
        assertEquals(2, facts.size());

        Fact f1 = facts.get(0);
        Fact f2 = facts.get(1);

        Console.log("id1", f1.getId());
        Console.log("id2", f2.getId());

        assertNotNull(f1.getId());
        assertNotNull(f2.getId());
        assertNotEquals(f1.getId(), f2.getId()); // key assertion ✅

        assertEquals("Text-One", f1.getText());
        assertEquals("Text-Two", f2.getText());
    }

    @Test
    void extract_should_fallback_when_base64_decodes_to_json_string_instead_of_json_object() {
        // given: decoded bytes are a JSON *string* containing JSON (starts with quote)
        // e.g. "\"{ \\\"facts\\\": [ ... ] }\""
        String decodedButWrong = """
    "{ \\"facts\\": [ { \\"id\\": \\"x1\\", \\"text\\": \\"BadProducer\\" } ] }"
    """.trim();

        String base64 = Base64.getEncoder()
                .encodeToString(decodedButWrong.getBytes(StandardCharsets.UTF_8));

        String envelope = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 3,
        "offset": 303,
        "timestamp": 1767114000123,
        "key": "k-double",
        "headers": { "traceId": "t-double" }
      },
      "payload": {
        "encoding": "base64",
        "value": "%s"
      }
    }
    """.formatted(base64);

        DDEventFactExtractor extractor = new DDEventFactExtractor();

        // when
        List<Fact> facts = extractor.extract(envelope);

        // then
        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("fact", f);
        Console.log("attrs", f.getAttributes());

        // current behavior: fallback fact (because decoded string starts with quotes, not { or [)
        assertEquals("k-double", f.getId());                 // kafka.key wins
        assertEquals("kafka_payload_bytes", f.getText());    // fallback marker
        assertTrue(f.getAttributes().contains("kafka.topic=requests"));
        assertTrue(f.getAttributes().contains("kafka.offset=303"));

        // still preserves payload
        assertTrue(f.getAttributes().stream().anyMatch(a -> a.startsWith("payload.base64=")));
        assertTrue(f.getAttributes().stream().anyMatch(a -> a.startsWith("payload.hex=")));
    }

    @Test
    void extract_should_return_empty_list_for_null_or_blank_input() {
        DDEventFactExtractor extractor = new DDEventFactExtractor();

        List<Fact> a = extractor.extract(null);
        Console.log("null", a);
        assertNotNull(a);
        assertTrue(a.isEmpty());

        List<Fact> b = extractor.extract("");
        Console.log("empty", b);
        assertNotNull(b);
        assertTrue(b.isEmpty());

        List<Fact> c = extractor.extract("   \n\t  ");
        Console.log("whitespace", c);
        assertNotNull(c);
        assertTrue(c.isEmpty());
    }

    @Test
    void extract_should_parse_direct_utf8_json_when_encoding_utf8() {
        String innerJson = "{ \"facts\": [ { \"id\": \"u1\", \"text\": \"Utf8Fact\", \"attributes\": [\"z\"] } ] }";

        JsonObject root = new JsonObject();
        JsonObject kafka = new JsonObject();
        kafka.addProperty("topic", "requests");
        kafka.addProperty("partition", 8);
        kafka.addProperty("offset", 808);
        kafka.addProperty("timestamp", 1767114000123L);
        kafka.addProperty("key", "k-utf8");
        JsonObject headers = new JsonObject();
        headers.addProperty("traceId", "t-utf8");
        kafka.add("headers", headers);
        root.add("kafka", kafka);

        JsonObject payload = new JsonObject();
        payload.addProperty("encoding", "utf8");
        payload.addProperty("value", innerJson);
        root.add("payload", payload);

        String envelope = root.toString();

        DDEventFactExtractor extractor = new DDEventFactExtractor();
        List<Fact> facts = extractor.extract(envelope);

        Console.log("facts.size", facts.size());
        assertEquals(1, facts.size());

        Fact f = facts.get(0);
        Console.log("fact", f);

        assertEquals("u1", f.getId());
        assertEquals("Utf8Fact", f.getText());
        assertTrue(f.getAttributes().contains("z"));
        assertTrue(f.getAttributes().contains("kafka.key=k-utf8"));
    }

}
