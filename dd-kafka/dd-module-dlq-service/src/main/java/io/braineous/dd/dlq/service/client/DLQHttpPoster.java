package io.braineous.dd.dlq.service.client;

import ai.braineous.cgo.config.ConfigService;
import io.braineous.dd.core.config.DDConfigService;
import io.braineous.dd.core.processor.HttpPoster;

public class DLQHttpPoster implements HttpPoster {

    @Override
    public int post(String endpoint, String jsonBody) throws Exception {
        DDConfigService ddCfgSvc = new DDConfigService();
        ConfigService cfg = ddCfgSvc.configService();
        String env = cfg.getProperty(DDConfigService.dd_env);

        String base = ddCfgSvc.internalDlqBase(env) + "/dlq";

        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();

        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(base + "/" + endpoint))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        java.net.http.HttpResponse<String> resp =
                client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

        return resp.statusCode();
    }

}
