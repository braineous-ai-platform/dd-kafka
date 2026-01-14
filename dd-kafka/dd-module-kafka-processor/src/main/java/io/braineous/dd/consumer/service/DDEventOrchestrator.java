package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.ingestion.persistence.IngestionReceipt;
import io.braineous.dd.ingestion.persistence.IngestionStore;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DDEventOrchestrator {

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
            return store.storeIngestion(ddEvent.toString());

        } catch (IllegalArgumentException iae) {
            // hard domain fail: no ingestionId axis possible
            throw iae;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
