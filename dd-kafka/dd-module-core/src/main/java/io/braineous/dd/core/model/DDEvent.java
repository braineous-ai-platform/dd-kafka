package io.braineous.dd.core.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public class DDEvent {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private KafkaMeta kafka;
    private Payload payload;

    public DDEvent() {
        // default ctor for Gson
    }

    public DDEvent(KafkaMeta kafka, Payload payload) {
        this.kafka = kafka;
        this.payload = payload;
    }

    public KafkaMeta getKafka() { return kafka; }
    public Payload getPayload() { return payload; }

    public void setKafka(KafkaMeta kafka) { this.kafka = kafka; }
    public void setPayload(Payload payload) { this.payload = payload; }

    // ---- factory / fluent helpers ----

    public static DDEvent of(String topic,
                             int partition,
                             long offset,
                             long timestamp,
                             String key,
                             String payloadEncoding,
                             String payloadValue) {

        KafkaMeta k = new KafkaMeta(topic, partition, offset, timestamp, key);
        Payload p = new Payload(payloadEncoding, payloadValue);
        return new DDEvent(k, p);
    }

    public DDEvent header(String name, String value) {
        if (this.kafka == null) this.kafka = new KafkaMeta();
        this.kafka.header(name, value);
        return this;
    }

    // ---- serialization ----

    public String toJson() {
        return GSON.toJson(this);
    }

    public static DDEvent fromJson(String json) {
        return GSON.fromJson(json, DDEvent.class);
    }

    // =========================
    // nested types
    // =========================

    public static class KafkaMeta {
        private String topic;
        private int partition;
        private long offset;
        private long timestamp;
        private String key;
        private Map<String, String> headers;

        public KafkaMeta() {
            // default ctor for Gson
        }

        public KafkaMeta(String topic, int partition, long offset, long timestamp, String key) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
        }

        public String getTopic() { return topic; }
        public int getPartition() { return partition; }
        public long getOffset() { return offset; }
        public long getTimestamp() { return timestamp; }
        public String getKey() { return key; }
        public Map<String, String> getHeaders() { return headers; }

        public void setTopic(String topic) { this.topic = topic; }
        public void setPartition(int partition) { this.partition = partition; }
        public void setOffset(long offset) { this.offset = offset; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public void setKey(String key) { this.key = key; }
        public void setHeaders(Map<String, String> headers) { this.headers = headers; }

        public KafkaMeta header(String name, String value) {
            if (this.headers == null) this.headers = new LinkedHashMap<>();
            this.headers.put(name, value);
            return this;
        }
    }

    public static class Payload {
        private String encoding; // e.g. "base64" or "utf8"
        private String value;    // base64 string or raw json string (if utf8/plain)

        public Payload() {
            // default ctor for Gson
        }

        public Payload(String encoding, String value) {
            this.encoding = encoding;
            this.value = value;
        }

        public String getEncoding() { return encoding; }
        public String getValue() { return value; }

        public void setEncoding(String encoding) { this.encoding = encoding; }
        public void setValue(String value) { this.value = value; }
    }
}


