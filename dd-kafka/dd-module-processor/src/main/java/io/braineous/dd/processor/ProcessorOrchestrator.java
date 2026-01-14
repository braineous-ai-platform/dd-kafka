package io.braineous.dd.processor;


import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.models.cgo.graph.SnapshotHash;
import com.google.gson.JsonObject;

import io.braineous.dd.cgo.DDCGOOrchestrator;
import io.braineous.dd.ingestion.persistence.MongoIngestionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.braineous.dd.ingestion.persistence.IngestionStore;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.core.processor.JsonSerializer;
import io.braineous.dd.processor.client.DDProducerClient;
import io.braineous.dd.core.processor.GsonJsonSerializer;

@ApplicationScoped
public class ProcessorOrchestrator {

    @Inject
    private HttpPoster httpPoster;

    @Inject
    private IngestionStore ingestionStore;

    @Inject
    private DDCGOOrchestrator cgoOrchestrator;


    public void setHttpPoster(HttpPoster httpPoster) {
        this.httpPoster = httpPoster;
    }

    public ProcessorResult orchestrate(JsonObject ddEventJson) {
        ProcessorResult validation = validate(ddEventJson);
        if (!validation.isOk()) {
            return validation;
        }

        //orchestrate with CGO Graph
        String ddEventStr = ddEventJson.toString();
        GraphView view = this.cgoOrchestrator.orchestrate(ddEventStr);
        //Add view details
        GraphSnapshot snapshot = (GraphSnapshot)view;
        if(snapshot == null ||
                snapshot.snapshotHash() == null ||
                snapshot.snapshotHash().getValue() == null ||
                snapshot.snapshotHash().getValue().trim().length() == 0
        ){
            //TODO: send_to_dlq_system
            return ProcessorResult.fail(ddEventJson,
                    new Why("DD-ORCH-INGESTION_ID-cgo", "system_view_is_null"));
        }

        // Direction: choose one transport later (Kafka emit OR REST call).
        // For now route through a client stub so wiring stays stable.
        String ingestionEndpoint = "/api/ingestion";
        JsonSerializer serializer = new GsonJsonSerializer();

        String ingestionId = this.nextIngestionId(ddEventStr, view);
        if(ingestionId == null){
            //TODO: send_to_dlq_system
            return ProcessorResult.fail(ddEventJson,
                    new Why("DD-ORCH-INGESTION_ID-cgo", "system_ingestion_id_is_null"));
        }

        ddEventJson.addProperty("ingestionId", ingestionId);

        JsonObject viewJson = snapshot.toJson();
        String snap = snapshot.snapshotHash().getValue();
        viewJson.addProperty(MongoIngestionStore.F_SNAPSHOT_HASH, snap);
        ddEventJson.add("view", viewJson);



        ProcessorResult result = DDProducerClient.getInstance().invoke(
                this.httpPoster,
                serializer,
                ingestionEndpoint,
                ddEventJson,
                ddEventJson);
        result.setIngestionId(ingestionId);
        return result;
    }


    private ProcessorResult validate(JsonObject ddEventJson) {
        if (ddEventJson == null) {
            return ProcessorResult.fail(new Why("DD-ORCH-VALIDATE-null_root", "ddEventJson is null"));
        }

        // --- guard: kafka + payload must exist and be objects ---
        if (!ddEventJson.has("kafka") || ddEventJson.get("kafka") == null || ddEventJson.get("kafka").isJsonNull()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-missing_kafka", "Missing root.kafka object"));
        }
        if (!ddEventJson.get("kafka").isJsonObject()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_type", "root.kafka must be a JSON object"));
        }

        if (!ddEventJson.has("payload") || ddEventJson.get("payload") == null || ddEventJson.get("payload").isJsonNull()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-missing_payload", "Missing root.payload object"));
        }
        if (!ddEventJson.get("payload").isJsonObject()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_type", "root.payload must be a JSON object"));
        }

        JsonObject kafka = ddEventJson.getAsJsonObject("kafka");
        JsonObject payload = ddEventJson.getAsJsonObject("payload");

        // --- kafka required fields (guard type before read) ---
        if (!hasString(kafka, "topic")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_topic", "kafka.topic is required"));
        }
        String topic = kafka.get("topic").getAsString();
        if (topic.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_topic", "kafka.topic is required"));
        }

        if (!hasNumber(kafka, "partition")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_partition", "kafka.partition is required"));
        }
        int partition = kafka.get("partition").getAsInt();
        if (partition < 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_partition", "kafka.partition must be >= 0"));
        }

        if (!hasNumber(kafka, "offset")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_offset", "kafka.offset is required"));
        }
        long offset = kafka.get("offset").getAsLong();
        if (offset < 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_offset", "kafka.offset must be >= 0"));
        }

        if (!hasNumber(kafka, "timestamp")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_timestamp", "kafka.timestamp is required"));
        }
        long timestamp = kafka.get("timestamp").getAsLong();
        if (timestamp <= 0) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_timestamp", "kafka.timestamp must be > 0"));
        }

        // headers optional but if present must be object
        if (kafka.has("headers") && kafka.get("headers") != null && !kafka.get("headers").isJsonNull()) {
            if (!kafka.get("headers").isJsonObject()) {
                return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-kafka_headers_type",
                        "kafka.headers must be a JSON object if present"));
            }
        }

        // --- payload required fields ---
        if (!hasString(payload, "encoding")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_encoding", "payload.encoding is required"));
        }
        String encoding = payload.get("encoding").getAsString();
        if (encoding.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_encoding", "payload.encoding is required"));
        }
        if (!"base64".equalsIgnoreCase(encoding)) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_encoding_unsupported",
                    "Unsupported payload.encoding: " + encoding + " (expected base64)"));
        }

        if (!hasString(payload, "value")) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_value", "payload.value is required"));
        }
        String value = payload.get("value").getAsString();
        if (value.isBlank()) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_value", "payload.value is required"));
        }

        if (!isLikelyBase64(value)) {
            return ProcessorResult.fail(ddEventJson, new Why("DD-ORCH-VALIDATE-payload_value_base64",
                    "payload.value is not valid base64"));
        }

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

    private static boolean hasString(JsonObject obj, String key) {
        return obj != null
                && obj.has(key)
                && obj.get(key) != null
                && !obj.get(key).isJsonNull()
                && obj.get(key).isJsonPrimitive()
                && obj.get(key).getAsJsonPrimitive().isString();
    }

    private static boolean hasNumber(JsonObject obj, String key) {
        return obj != null
                && obj.has(key)
                && obj.get(key) != null
                && !obj.get(key).isJsonNull()
                && obj.get(key).isJsonPrimitive()
                && obj.get(key).getAsJsonPrimitive().isNumber();
    }

    String nextIngestionId(String ddEventStr, GraphView view) {
        if(view == null){
            return null;
        }

        GraphSnapshot snapshot = (GraphSnapshot) view;
        if(snapshot == null){
            return null;
        }

        SnapshotHash snapshotHash = ((GraphSnapshot) view).snapshotHash();
        if(snapshotHash == null){
            return null;
        }

        String snap = snapshotHash.getValue();
        if(snap == null || snap.trim().length() == 0){
            return null;
        }

        return this.ingestionStore.resolveIngestionId(ddEventStr, snap);
    }

}
