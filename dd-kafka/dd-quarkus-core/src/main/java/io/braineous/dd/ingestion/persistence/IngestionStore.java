package io.braineous.dd.ingestion.persistence;

import ai.braineous.rag.prompt.cgo.api.GraphView;

public interface IngestionStore {
    IngestionReceipt storeIngestion(String payload);

    public String resolveIngestionId(String payload, String snap);
}
