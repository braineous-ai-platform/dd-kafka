package io.braineous.dd.processor;

public class DDIngestionHttpPoster implements HttpPoster{

    @Override
    public int post(String endpoint, String jsonBody) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();

        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(endpoint))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        java.net.http.HttpResponse<String> resp =
                client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

        return resp.statusCode();
    }

}
