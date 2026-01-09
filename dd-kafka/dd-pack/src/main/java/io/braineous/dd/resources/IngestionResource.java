package io.braineous.dd.resources;


import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.core.processor.HttpPoster;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.processor.ProcessorResult;
import io.braineous.dd.processor.client.DDIngestionHttpPoster;
import io.braineous.dd.replay.model.IngestionRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IngestionResource {

    private ProcessorOrchestrator orch;

    public IngestionResource() {
        this.orch = ProcessorOrchestrator.getInstance();
        this.orch.setHttpPoster(new DDIngestionHttpPoster());
    }

    //test seam
    void setHttpPoster(HttpPoster poster){
        this.orch.setHttpPoster(poster);
    }

    @POST
    @Path("/ingestion")
    public jakarta.ws.rs.core.Response ingestion(IngestionRequest request){

        //temp
        Console.log("it_request_payload_raw", request == null ? null : request.getPayload());


        // ---- fail-fast: request ----
        if (request == null) {
            Console.log("ingestion_400", "request_null");
            return jakarta.ws.rs.core.Response
                    .status(400)
                    .entity(ProcessorResult.fail(new Why("DD-INGEST-request_null", "request cannot be null")))
                    .build();
        }

        String payload = request.getPayload();
        if (payload == null || payload.trim().isEmpty()) {
            Console.log("ingestion_400", "payload_blank");
            return jakarta.ws.rs.core.Response
                    .status(400)
                    .entity(ProcessorResult.fail(new Why("DD-INGEST-payload_blank", "payload cannot be blank")))
                    .build();
        }

        // ---- parse guard ----
        final JsonObject payloadJson;
        try {
            payloadJson = JsonParser.parseString(payload).getAsJsonObject();
        } catch (Exception e) {
            Console.log("ingestion_400", "payload_invalid_json :: " + e.getClass().getSimpleName());
            return jakarta.ws.rs.core.Response
                    .status(400)
                    .entity(ProcessorResult.fail(new Why("DD-INGEST-payload_invalid_json", "payload must be a JSON object")))
                    .build();
        }

        // ---- orchestrate ----
        ProcessorResult result = this.orch.orchestrate(payloadJson);
        Console.log("ingestion_200", result);

        // serialize with Gson so Gson JsonObject is handled correctly
                String json = new Gson().toJson(result);

                return jakarta.ws.rs.core.Response
                        .ok(json, MediaType.APPLICATION_JSON)
                        .build();

    }


    //-------Validators --------------------------
}


