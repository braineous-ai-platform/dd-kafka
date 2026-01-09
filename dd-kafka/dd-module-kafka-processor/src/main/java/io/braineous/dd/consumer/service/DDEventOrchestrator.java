package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.*;
import ai.braineous.rag.prompt.observe.Console;
import ai.braineous.rag.prompt.services.cgo.causal.CausalLLMBridge;
import com.google.gson.JsonArray;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.consumer.service.persistence.IngestionReceipt;
import io.braineous.dd.consumer.service.persistence.IngestionStore;
import io.braineous.dd.consumer.service.persistence.MongoIngestionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DDEventOrchestrator {
    private LLMBridge llmBridge = new CausalLLMBridge();

    @Inject
    private IngestionStore store;


    public GraphView orchestrate(String ingestionStr) {
        try {
            LLMContext context = new LLMContext();

            FactExtractor factExtractor = new DDEventFactExtractor();

            JsonArray ddEvents = new JsonArray();
            JsonElement ddEventElement = JsonParser.parseString(ingestionStr);
            if (ddEventElement.isJsonArray()) {
                ddEvents = ddEventElement.getAsJsonArray();
            } else {
                ddEvents.add(ddEventElement.getAsJsonObject());
            }

            context.build("kafka_events",
                    ddEvents.toString(),
                    factExtractor,
                    null,
                    null);

            // bridge to CGO
            GraphView view = this.llmBridge.submit(context);

            // ---- persist raw envelope for Replay/time-window surfaces ----
            IngestionReceipt receipt = store.storeIngestion(
                    ingestionStr,
                    view

            );


            return view;

        } catch (Exception e) {
            /*//TODO: send_to_dlq


            //also print in service log
            //TODO: think what to return or throw exception
            return null;*/
            throw new RuntimeException(e);
            
        }
    }

}
