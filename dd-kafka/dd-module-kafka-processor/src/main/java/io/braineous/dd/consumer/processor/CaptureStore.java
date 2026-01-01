package io.braineous.dd.consumer.processor;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CaptureStore {
    private final java.util.concurrent.ConcurrentLinkedQueue<String> q = new java.util.concurrent.ConcurrentLinkedQueue<>();
    public void add(String s) { q.add(s); }
    public int size() { return q.size(); }
    public String first() { return q.peek(); }
    public void clear() { q.clear(); }
}
