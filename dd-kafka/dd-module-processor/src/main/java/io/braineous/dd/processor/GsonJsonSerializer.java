package io.braineous.dd.processor;

public class GsonJsonSerializer implements JsonSerializer {
    private final com.google.gson.Gson gson = new com.google.gson.Gson();

    public String toJson(Object o) {
        return gson.toJson(o);
    }
}
