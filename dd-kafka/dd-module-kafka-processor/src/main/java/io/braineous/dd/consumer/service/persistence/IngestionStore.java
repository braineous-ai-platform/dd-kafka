package io.braineous.dd.consumer.service.persistence;

import ai.braineous.rag.prompt.cgo.api.GraphView;

public interface IngestionStore {
    IngestionReceipt storeIngestion(String payload, GraphView view);
}
