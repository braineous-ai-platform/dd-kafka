package io.braineous.dd.processor.client;

import ai.braineous.cgo.config.ConfigService;
import ai.braineous.rag.prompt.observe.Console;
import io.braineous.dd.core.config.DDConfigService;
import io.braineous.dd.core.processor.HttpPoster;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DDIngestionHttpPoster implements HttpPoster {

    @Override
    public int post(String endpoint, String jsonBody) throws Exception {
        DDConfigService ddCfgSvc = new DDConfigService();
        ConfigService cfg = ddCfgSvc.configService();
        String env = cfg.getProperty(DDConfigService.dd_env);

        String base = ddCfgSvc.internalProducerBase(env);

        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();

        String baseUrl = base + "/" + endpoint;
        Console.log("__________producer_url_______", baseUrl);
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(baseUrl))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        java.net.http.HttpResponse<String> resp =
                client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

        return resp.statusCode();
    }

}
