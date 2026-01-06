package io.braineous.dd.processor.client;

import io.braineous.dd.core.processor.HttpPoster;

public class DDIngestionHttpPoster implements HttpPoster {

    //TODO: make this dynamic and env-agnostic with config service
    private static final String PRODUCER_BASE = "http://localhost:8081";

    @Override
    public int post(String endpoint, String jsonBody) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();

        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(PRODUCER_BASE + "/" + endpoint))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        java.net.http.HttpResponse<String> resp =
                client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

        return resp.statusCode();
    }

}
