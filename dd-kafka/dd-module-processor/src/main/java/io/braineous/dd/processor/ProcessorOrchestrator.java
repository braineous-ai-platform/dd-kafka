package io.braineous.dd.processor;


import com.google.gson.JsonObject;

public class ProcessorOrchestrator {
    private static final ProcessorOrchestrator orchestrator = new ProcessorOrchestrator();

    private HttpPoster httpPoster;

    private ProcessorOrchestrator() {
    }

    public static ProcessorOrchestrator getInstance(){
        return orchestrator;
    }

    public void setHttpPoster(HttpPoster httpPoster) {
        if(httpPoster == null || this.httpPoster != null){
            return;
        }
        this.httpPoster = httpPoster;
    }

    public ProcessorResult orchestrate(JsonObject ddEventJson) {
        ProcessorResult validation = validate(ddEventJson);
        if (!validation.isOk()) {
            return validation;
        }

        // Direction: choose one transport later (Kafka emit OR REST call).
        // For now route through a client stub so wiring stays stable.
        String ingestionEndpoint = "/api/ingestion";
        return DDProducerClient.getInstance().invoke(httpPoster,
                new GsonJsonSerializer(),
                ingestionEndpoint,
                ddEventJson,
                ddEventJson);
    }


    private ProcessorResult validate(JsonObject ddEventJson) {
        if (ddEventJson == null) {
            return ProcessorResult.fail(new Why("DD-ORCH-VALIDATE-null_root", "ddEventJson is null"));
        }

        // --- root required: kafka, payload ---
        JsonObject kafka = ddEventJson.get("kafka").getAsJsonObject();
        if (kafka == null) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-missing_kafka", "Missing root.kafka object"));
        }

        JsonObject payload = ddEventJson.get("payload").getAsJsonObject();
        if (payload == null) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-missing_payload", "Missing root.payload object"));
        }

        // --- kafka required fields ---
        String topic = kafka.get("topic").getAsString();
        if (topic == null || topic.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_topic", "kafka.topic is required"));
        }

        Integer partition = kafka.get("partition").getAsInt();
        if (partition == null || partition < 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_partition", "kafka.partition must be >= 0"));
        }

        Long offset = kafka.get("offset").getAsLong();
        if (offset == null || offset < 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_offset", "kafka.offset must be >= 0"));
        }

        Long timestamp = kafka.get("timestamp").getAsLong();
        if (timestamp == null || timestamp <= 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_timestamp", "kafka.timestamp must be > 0"));
        }

        // key is optional (schema shows it, but allow null/blank)
        // headers optional, but if present must be an object
        Object headersAny = kafka.get("headers").getAsJsonObject();
        if (headersAny != null && !(headersAny instanceof JsonObject)) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_headers_type", "kafka.headers must be a JSON object if present"));
        }

        // --- payload required fields ---
        String encoding = payload.get("encoding").getAsString();
        if (encoding == null || encoding.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_encoding", "payload.encoding is required"));
        }
        if (!"base64".equalsIgnoreCase(encoding)) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_encoding_unsupported",
                    "Unsupported payload.encoding: " + encoding + " (expected base64)"));
        }

        String value = payload.get("value").getAsString();
        if (value == null || value.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_value", "payload.value is required"));
        }

        // Lightweight base64 sanity (no decode yet; just validate chars/structure)
        if (!isLikelyBase64(value)) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_value_base64",
                    "payload.value is not valid base64"));
        }

        // extras ignored by design: kafka.extraKafkaField, payload.extraPayloadField, extraRootField
        return ProcessorResult.ok(ddEventJson);
    }

    private boolean isLikelyBase64(String s) {
        // quick sanity: length multiple of 4 and only base64 chars + up to 2 '=' padding at end
        if (s == null) return false;
        int len = s.length();
        if (len == 0 || (len % 4) != 0) return false;

        // allow standard base64 chars; reject whitespace
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            boolean ok =
                    (c >= 'A' && c <= 'Z') ||
                            (c >= 'a' && c <= 'z') ||
                            (c >= '0' && c <= '9') ||
                            c == '+' || c == '/' || c == '=';
            if (!ok) return false;
        }

        // padding rules: only at end, max two
        int firstPad = s.indexOf('=');
        if (firstPad == -1) return true;

        for (int i = firstPad; i < len; i++) {
            if (s.charAt(i) != '=') return false;
        }

        int padCount = len - firstPad;
        return padCount == 1 || padCount == 2;
    }
}
