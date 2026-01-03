package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.*;
import ai.braineous.rag.prompt.services.cgo.causal.CausalLLMBridge;
import com.google.gson.JsonArray;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DDEventOrchestrator {
    private LLMBridge llmBridge = new CausalLLMBridge();


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
            return this.llmBridge.submit(context);

        } catch (Exception e) {
            //TODO: send_to_dlq


            //also print in service log
            //TODO: think what to return or throw exception
            return null;
        }
    }

}
