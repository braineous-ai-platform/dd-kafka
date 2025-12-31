package io.braineous.dd.processor;

public interface HttpPoster {
    int post(String endpoint, String jsonBody) throws Exception;
}
