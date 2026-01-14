package io.braineous.dd.cgo;

import ai.braineous.rag.prompt.cgo.api.Fact;
import ai.braineous.rag.prompt.cgo.api.FactExtractor;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class DDEventFactExtractor implements FactExtractor {
    @Override
    public List<Fact> extract(String json) {
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyList();
        }

        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(json);
        } catch (Exception e) {
            return Collections.emptyList();
        }

        if (parsed.isJsonArray()) {
            Map<String, Fact> dedup = new LinkedHashMap<>();
            for (JsonElement el : parsed.getAsJsonArray()) {
                if (el == null || el.isJsonNull() || !el.isJsonObject()) continue;
                List<Fact> fs = extract(el.toString()); // reuse existing single-envelope logic
                for (Fact f : fs) {
                    if (f == null || f.getId() == null || f.getId().isBlank()) continue;
                    dedup.put(f.getId(), f);
                }
            }
            return new ArrayList<>(dedup.values());
        }

        // existing path: single object envelope
        JsonObject root = parsed.getAsJsonObject();
        JsonObject kafka = obj(root, "kafka");
        JsonObject payload = obj(root, "payload");

        String key = str(kafka, "key"); // e.g. "fact-001"
        String topic = str(kafka, "topic");
        Long partition = lng(kafka, "partition");
        Long offset = lng(kafka, "offset");
        String encoding = str(payload, "encoding");
        String value = str(payload, "value");

        if (value == null || value.isBlank()) {
            return Collections.emptyList();
        }

        byte[] bytes = decodePayload(value, encoding);
        if (bytes == null) {
            return Collections.emptyList();
        }

        // Try: inner payload is JSON (UTF-8)
        String inner = tryUtf8(bytes);
        if (inner != null) {
            String trimmed = inner.trim();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                List<Fact> facts = extractFactsFromInnerJson(trimmed);
                if (!facts.isEmpty()) {
                    // optional: enrich facts with kafka context
                    for (Fact f : facts) {
                        if (f.getId() == null || f.getId().isBlank()) {
                            f.setId(stableIdFromText(f.getText()));
                        }
                        if (topic != null) f.addAttribute("kafka.topic=" + topic);
                        if (partition != null) f.addAttribute("kafka.partition=" + partition);
                        if (offset != null) f.addAttribute("kafka.offset=" + offset);
                        if (key != null) f.addAttribute("kafka.key=" + key);
                    }
                    return facts;
                }
            }
        }

        // Fallback: bytes are not JSON (like your example) -> create one Fact preserving payload + context
        Fact f = new Fact();
        f.setId((key != null && !key.isBlank()) ? key : stableIdFromBytes(bytes));
        f.setText("kafka_payload_bytes"); // keep it explicit + stable
        f.setMode("atomic");
        f.addAttribute("payload.encoding=" + (encoding == null ? "unknown" : encoding));
        f.addAttribute("payload.base64=" + value);
        f.addAttribute("payload.hex=" + toHex(bytes));

        if (topic != null) f.addAttribute("kafka.topic=" + topic);
        if (partition != null) f.addAttribute("kafka.partition=" + partition);
        if (offset != null) f.addAttribute("kafka.offset=" + offset);
        if (key != null) f.addAttribute("kafka.key=" + key);

        // headers (optional)
        JsonObject headers = obj(kafka, "headers");
        if (headers != null) {
            for (Map.Entry<String, JsonElement> e : headers.entrySet()) {
                String hv = (e.getValue() == null || e.getValue().isJsonNull()) ? null : e.getValue().getAsString();
                if (hv != null) f.addAttribute("kafka.header." + e.getKey() + "=" + hv);
            }
        }

        return Collections.singletonList(f);
    }

    private static byte[] decodePayload(String value, String encoding) {
        try {
            if (encoding == null || encoding.isBlank() || "base64".equalsIgnoreCase(encoding)) {
                return Base64.getDecoder().decode(value);
            }
            if ("plain".equalsIgnoreCase(encoding) || "utf8".equalsIgnoreCase(encoding)) {
                return value.getBytes(StandardCharsets.UTF_8);
            }
            // unknown encoding -> try base64 anyway
            return Base64.getDecoder().decode(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static String tryUtf8(byte[] bytes) {
        try {
            String s = new String(bytes, StandardCharsets.UTF_8);
            // quick sanity: avoid returning mostly-control-char garbage
            int printable = 0;
            int total = Math.min(s.length(), 256);
            for (int i = 0; i < total; i++) {
                char c = s.charAt(i);
                if (c == '\n' || c == '\r' || c == '\t' || (c >= 32 && c < 127)) printable++;
            }
            if (total == 0) return null;
            return (printable >= (int) (0.85 * total)) ? s : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static List<Fact> extractFactsFromInnerJson(String innerJson) {
        JsonElement el;
        try {
            el = JsonParser.parseString(innerJson);
        } catch (Exception e) {
            return Collections.emptyList();
        }

        // Support: inner is array of facts OR {facts:[...]} OR {payload:{facts:[...]}} etc.
        JsonArray arr = null;

        if (el.isJsonArray()) {
            arr = el.getAsJsonArray();
        } else if (el.isJsonObject()) {
            JsonObject o = el.getAsJsonObject();
            arr = firstArray(
                    arr(o, "facts"),
                    arr(obj(o, "event"), "facts"),
                    arr(obj(o, "payload"), "facts"),
                    arr(obj(o, "data"), "facts")
            );
            // also allow: a single fact object
            if (arr == null && looksLikeFact(o)) {
                Fact f = toFact(o);
                return (f == null) ? Collections.emptyList() : Collections.singletonList(f);
            }
        }

        if (arr == null || arr.size() == 0) return Collections.emptyList();

        Map<String, Fact> dedup = new LinkedHashMap<>();
        for (JsonElement e : arr) {
            if (e == null || e.isJsonNull() || !e.isJsonObject()) continue;
            JsonObject fo = e.getAsJsonObject();
            if (!looksLikeFact(fo)) continue;

            Fact f = toFact(fo);
            if (f == null) continue;
            if (f.getId() == null || f.getId().isBlank()) f.setId(stableIdFromText(f.getText()));
            dedup.put(f.getId(), f);
        }
        return new ArrayList<>(dedup.values());
    }

    private static boolean looksLikeFact(JsonObject o) {
        if (o == null) return false;
        return (o.has("text") && !o.get("text").isJsonNull()) || (o.has("id") && !o.get("id").isJsonNull());
    }

    private static Fact toFact(JsonObject o) {
        if (o == null) return null;

        Fact f = new Fact();
        f.setId(str(o, "id"));
        f.setText(str(o, "text"));

        String mode = str(o, "mode");
        if (mode != null && !mode.isBlank()) f.setMode(mode);

        // attributes can be array, object, or string
        Set<String> attrs = new HashSet<>();
        JsonElement a = o.get("attributes");
        if (a != null && !a.isJsonNull()) {
            if (a.isJsonArray()) {
                for (JsonElement x : a.getAsJsonArray()) {
                    if (x != null && !x.isJsonNull()) attrs.add(x.getAsString());
                }
            } else if (a.isJsonObject()) {
                for (Map.Entry<String, JsonElement> e : a.getAsJsonObject().entrySet()) {
                    String k = e.getKey();
                    String v = (e.getValue() == null || e.getValue().isJsonNull()) ? null : e.getValue().getAsString();
                    attrs.add(v == null ? k : (k + "=" + v));
                }
            } else {
                attrs.add(a.getAsString());
            }
        }
        f.setAttributes(attrs);

        // validationRule ignored by design
        return f;
    }

    private static JsonArray firstArray(JsonArray... candidates) {
        for (JsonArray c : candidates) {
            if (c != null && c.size() > 0) return c;
        }
        return null;
    }

    private static JsonObject obj(JsonObject parent, String key) {
        if (parent == null || key == null) return null;
        JsonElement e = parent.get(key);
        return (e != null && e.isJsonObject()) ? e.getAsJsonObject() : null;
    }

    private static JsonArray arr(JsonObject parent, String key) {
        if (parent == null || key == null) return null;
        JsonElement e = parent.get(key);
        return (e != null && e.isJsonArray()) ? e.getAsJsonArray() : null;
    }

    private static String str(JsonObject parent, String key) {
        if (parent == null || key == null) return null;
        JsonElement e = parent.get(key);
        return (e == null || e.isJsonNull()) ? null : e.getAsString();
    }

    private static Long lng(JsonObject parent, String key) {
        if (parent == null || key == null) return null;
        JsonElement e = parent.get(key);
        try {
            return (e == null || e.isJsonNull()) ? null : e.getAsLong();
        } catch (Exception ex) {
            return null;
        }
    }

    private static String stableIdFromText(String text) {
        if (text == null) text = "";
        UUID uuid = UUID.nameUUIDFromBytes(text.getBytes(StandardCharsets.UTF_8));
        return uuid.toString();
    }

    private static String stableIdFromBytes(byte[] bytes) {
        if (bytes == null) bytes = new byte[0];
        UUID uuid = UUID.nameUUIDFromBytes(bytes);
        return uuid.toString();
    }

    private static String toHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
