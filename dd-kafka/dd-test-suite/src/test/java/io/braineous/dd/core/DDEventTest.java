package io.braineous.dd.core;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.DDEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DDEventTest {

    @Test
    void toJson_should_emit_expected_envelope_shape_and_values() {
        // given
        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("correlationId", "corr-9911");

        // when
        String json = event.toJson();
        Console.log("json", json);

        // then: verify structure + values (JSON-level, not string-equality brittle)
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        assertTrue(root.has("kafka"));
        assertTrue(root.has("payload"));

        JsonObject kafka = root.getAsJsonObject("kafka");
        assertEquals("requests", kafka.get("topic").getAsString());
        assertEquals(3, kafka.get("partition").getAsInt());
        assertEquals(48192L, kafka.get("offset").getAsLong());
        assertEquals(1767114000123L, kafka.get("timestamp").getAsLong());
        assertEquals("fact-001", kafka.get("key").getAsString());

        JsonObject headers = kafka.getAsJsonObject("headers");
        assertEquals("8f3a9c12", headers.get("traceId").getAsString());
        assertEquals("corr-9911", headers.get("correlationId").getAsString());

        JsonObject payload = root.getAsJsonObject("payload");
        assertEquals("base64", payload.get("encoding").getAsString());
        assertEquals("AAECAwQFBgcICQ==", payload.get("value").getAsString());
    }


    @Test
    void fromJson_should_round_trip_with_toJson_preserving_fields() {
        // given
        DDEvent event = DDEvent.of(
                        "requests",
                        3,
                        48192L,
                        1767114000123L,
                        "fact-001",
                        "base64",
                        "AAECAwQFBgcICQ=="
                ).header("traceId", "8f3a9c12")
                .header("correlationId", "corr-9911");

        String json = event.toJson();
        Console.log("json", json);

        // when
        DDEvent parsed = DDEvent.fromJson(json);

        // then
        assertNotNull(parsed);
        assertNotNull(parsed.getKafka());
        assertNotNull(parsed.getPayload());

        assertEquals("requests", parsed.getKafka().getTopic());
        assertEquals(3, parsed.getKafka().getPartition());
        assertEquals(48192L, parsed.getKafka().getOffset());
        assertEquals(1767114000123L, parsed.getKafka().getTimestamp());
        assertEquals("fact-001", parsed.getKafka().getKey());

        assertNotNull(parsed.getKafka().getHeaders());
        assertEquals("8f3a9c12", parsed.getKafka().getHeaders().get("traceId"));
        assertEquals("corr-9911", parsed.getKafka().getHeaders().get("correlationId"));

        assertEquals("base64", parsed.getPayload().getEncoding());
        assertEquals("AAECAwQFBgcICQ==", parsed.getPayload().getValue());
    }

    @Test
    void header_should_initialize_headers_map_and_add_values() {
        // given
        DDEvent event = DDEvent.of(
                "requests",
                1,
                10L,
                111L,
                "k1",
                "base64",
                "AAECAw=="
        );

        // when
        event.header("traceId", "t-1")
                .header("correlationId", "c-1");

        // then
        assertNotNull(event.getKafka());
        assertNotNull(event.getKafka().getHeaders());
        assertEquals("t-1", event.getKafka().getHeaders().get("traceId"));
        assertEquals("c-1", event.getKafka().getHeaders().get("correlationId"));

        Console.log("headers", event.getKafka().getHeaders());
    }


    @Test
    void header_should_create_kafka_meta_when_null() {
        // given
        DDEvent event = new DDEvent();
        assertNull(event.getKafka());

        // when
        event.header("traceId", "t-0");

        // then
        assertNotNull(event.getKafka());
        assertNotNull(event.getKafka().getHeaders());
        assertEquals("t-0", event.getKafka().getHeaders().get("traceId"));

        Console.log("kafka", event.getKafka());
        Console.log("headers", event.getKafka().getHeaders());
    }

    @Test
    void toJson_should_not_emit_headers_field_when_none_set() {
        // given
        DDEvent event = DDEvent.of(
                "requests",
                2,
                22L,
                222L,
                "k-nohdr",
                "base64",
                "AAECAw=="
        );

        // when
        String json = event.toJson();
        Console.log("json", json);

        // then
        com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        com.google.gson.JsonObject kafka = root.getAsJsonObject("kafka");

        assertFalse(kafka.has("headers")); // because headers == null and Gson omits nulls by default
    }

    @Test
    void fromJson_should_keep_headers_null_when_not_present() {
        // given
        String json = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 4,
        "offset": 44,
        "timestamp": 444,
        "key": "k-no-headers"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAw=="
      }
    }
    """;

        // when
        DDEvent event = DDEvent.fromJson(json);

        // then
        assertNotNull(event);
        assertNotNull(event.getKafka());
        assertNull(event.getKafka().getHeaders()); // explicit contract

        Console.log("kafka", event.getKafka());
        Console.log("headers", event.getKafka().getHeaders());
    }

    @Test
    void of_should_set_kafka_and_payload_fields() {
        // when
        DDEvent event = DDEvent.of(
                "requests",
                7,
                700L,
                1767114000123L,
                "k-of",
                "base64",
                "AAECAwQ="
        );

        // then
        assertNotNull(event);
        assertNotNull(event.getKafka());
        assertNotNull(event.getPayload());

        assertEquals("requests", event.getKafka().getTopic());
        assertEquals(7, event.getKafka().getPartition());
        assertEquals(700L, event.getKafka().getOffset());
        assertEquals(1767114000123L, event.getKafka().getTimestamp());
        assertEquals("k-of", event.getKafka().getKey());

        assertEquals("base64", event.getPayload().getEncoding());
        assertEquals("AAECAwQ=", event.getPayload().getValue());
    }

    @Test
    void toJson_should_not_html_escape_payload_value() {
        // given
        String special = "<tag>&\"quote\"</tag>"; // includes <, >, &, quotes

        DDEvent event = DDEvent.of(
                "requests",
                1,
                1L,
                1L,
                "k-html",
                "utf8",
                special
        );

        // when
        String json = event.toJson();
        Console.log("json", json);

        // then
        // We expect raw characters, NOT unicode-escaped \u003c \u003e \u0026
        assertTrue(json.contains("<tag>"));
        assertTrue(json.contains("&"));
        assertFalse(json.contains("\\u003c"));
        assertFalse(json.contains("\\u003e"));
        assertFalse(json.contains("\\u0026"));

        // also: round-trip should give same value
        DDEvent parsed = DDEvent.fromJson(json);
        assertEquals(special, parsed.getPayload().getValue());
    }

    @Test
    void fromJson_should_ignore_unknown_fields_and_still_parse_core_fields() {
        // given
        String json = """
    {
      "kafka": {
        "topic": "requests",
        "partition": 9,
        "offset": 999,
        "timestamp": 123456789,
        "key": "k-unknown",
        "headers": { "traceId": "t-unk" },
        "extraKafkaField": "ignore-me"
      },
      "payload": {
        "encoding": "base64",
        "value": "AAECAw==",
        "extraPayloadField": { "nested": true }
      },
      "extraRootField": [1,2,3]
    }
    """;

        // when
        DDEvent event = DDEvent.fromJson(json);

        // then
        assertNotNull(event);
        assertNotNull(event.getKafka());
        assertNotNull(event.getPayload());

        assertEquals("requests", event.getKafka().getTopic());
        assertEquals(9, event.getKafka().getPartition());
        assertEquals(999L, event.getKafka().getOffset());
        assertEquals(123456789L, event.getKafka().getTimestamp());
        assertEquals("k-unknown", event.getKafka().getKey());
        assertEquals("t-unk", event.getKafka().getHeaders().get("traceId"));

        assertEquals("base64", event.getPayload().getEncoding());
        assertEquals("AAECAw==", event.getPayload().getValue());

        Console.log("event", event);
    }

    @Test
    void toJson_should_omit_payload_encoding_when_null() {
        // given
        DDEvent event = DDEvent.of(
                "requests",
                1,
                11L,
                111L,
                "k-null-enc",
                null,              // encoding intentionally null
                "AAECAw=="
        );

        // when
        String json = event.toJson();
        Console.log("json", json);

        // then
        com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        com.google.gson.JsonObject payload = root.getAsJsonObject("payload");

        assertFalse(payload.has("encoding"));     // omitted because null
        assertEquals("AAECAw==", payload.get("value").getAsString());
    }


}

