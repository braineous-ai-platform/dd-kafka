package io.braineous.dd.cgo;

import ai.braineous.rag.prompt.cgo.api.FactExtractor;
import ai.braineous.rag.prompt.cgo.api.GraphView;
import ai.braineous.rag.prompt.cgo.api.LLMBridge;
import ai.braineous.rag.prompt.cgo.api.LLMContext;
import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;
import ai.braineous.rag.prompt.services.cgo.causal.CausalLLMBridge;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DDCGOOrchestrator {

    //TODO: integrate DLQOrchestrator

    private LLMBridge llmBridge = new CausalLLMBridge();

    public GraphView orchestrate(String ingestionStr){
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

            //debug
            Console.log("___cgo___", (((GraphSnapshot)view).toString()));

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
