package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.dlq.service.DLQOrchestrator;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.ingestion.persistence.IngestionStore;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DDEventOrchestrator {

    @Inject
    private DLQOrchestrator dlqOrch;

    @Inject
    private IngestionStore store;
    public void setStore(IngestionStore store){
        this.store = store;
    }

    public IngestionStore getStore() {
        return store;
    }

    public IngestionReceipt orchestrate(String ingestionStr) {
        try {

            // ---------- fail-fast: payload ----------
            if (ingestionStr == null || ingestionStr.trim().isEmpty()) {
                throw new IllegalArgumentException("DD-ING-events_blank");
            }

            JsonElement ddEventElement = JsonParser.parseString(ingestionStr);

            JsonObject ddEvent;
            if (ddEventElement.isJsonArray()) {
                JsonArray arr = ddEventElement.getAsJsonArray();
                if (arr == null || arr.size() == 0) {
                    throw new IllegalArgumentException("DD-ING-events_empty");
                }
                ddEvent = arr.get(0).getAsJsonObject();
            } else if (ddEventElement.isJsonObject()) {
                ddEvent = ddEventElement.getAsJsonObject();
            } else {
                throw new IllegalArgumentException("DD-ING-events_not_object");
            }

            // ---- persist raw envelope for Replay/time-window surfaces ----
            IngestionReceipt receipt = store.storeIngestion(ddEvent.toString());

            //check for DLQ-S qualified failures and emit to the channel
            //still send receipt to the caller
            if(!receipt.ok() && receipt.isSysDlqEnabled()){
                Exception dlqException = new Exception(receipt.toJson().toString());
                this.dlqOrch.orchestrateSystemFailure(dlqException, ingestionStr);
            }

            return receipt;

        } catch (IllegalArgumentException iae) {
            // hard domain fail: no ingestionId axis possible
            //record as DLQ System Failure
            this.dlqOrch.orchestrateSystemFailure(iae, ingestionStr);

            throw iae;
        } catch (Exception e) {
            //record as DLQ System Failure
            this.dlqOrch.orchestrateSystemFailure(e, ingestionStr);

            throw new RuntimeException(e);
        }
    }
}
