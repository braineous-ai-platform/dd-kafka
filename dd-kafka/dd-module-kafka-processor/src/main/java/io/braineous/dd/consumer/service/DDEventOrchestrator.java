package io.braineous.dd.consumer.service;

import ai.braineous.rag.prompt.cgo.api.*;
import ai.braineous.rag.prompt.services.cgo.causal.CausalLLMBridge;
import com.google.gson.JsonArray;

public class DDEventOrchestrator {
    private LLMBridge llmBridge = new CausalLLMBridge();

    public GraphView orchestrate(JsonArray flightsJsonArray) {
        try {
            LLMContext context = new LLMContext();

            FactExtractor factExtractor = new DDEventFactExtractor();

            context.build("kafka_events",
                    flightsJsonArray.toString(),
                    factExtractor,
                    null,
                    null);

            // bridge to CGO
            return this.llmBridge.submit(context);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
