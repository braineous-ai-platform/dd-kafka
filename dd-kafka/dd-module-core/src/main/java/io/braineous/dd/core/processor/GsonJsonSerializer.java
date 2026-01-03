package io.braineous.dd.core.processor;

import io.braineous.dd.core.processor.JsonSerializer;

public class GsonJsonSerializer implements JsonSerializer {
    private final com.google.gson.Gson gson = new com.google.gson.Gson();

    public String toJson(Object o) {
        return gson.toJson(o);
    }
}
