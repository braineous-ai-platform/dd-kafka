package io.braineous.dd.ingestion.persistence;

import ai.braineous.rag.prompt.cgo.api.GraphView;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public interface IngestionStore {
    IngestionReceipt storeIngestion(String payload);

    public String resolveIngestionId(String payload, String snap);

    public JsonArray findEventsByTimeWindow(String fromTime, String toTime);

    public JsonObject findEventsByIngestionId(String ingestionId);
}
