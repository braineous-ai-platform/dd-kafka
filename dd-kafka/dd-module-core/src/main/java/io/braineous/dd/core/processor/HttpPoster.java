package io.braineous.dd.core.processor;

public interface HttpPoster {
    int post(String endpoint, String jsonBody) throws Exception;
}
