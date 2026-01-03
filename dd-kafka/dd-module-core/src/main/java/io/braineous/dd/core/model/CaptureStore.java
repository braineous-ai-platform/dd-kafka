package io.braineous.dd.core.model;

import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;

public class CaptureStore {
    private static final CaptureStore store = new CaptureStore();

    public static CaptureStore getInstance(){
        return store;
    }

    private final java.util.concurrent.ConcurrentLinkedQueue<String> q = new java.util.concurrent.ConcurrentLinkedQueue<>();


    private GraphSnapshot snapshot;

    public GraphSnapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(GraphSnapshot snapshot) {
        Console.log("DD_DEBUG_GRAPH", snapshot.toJson().toString());
        this.snapshot = snapshot;
    }

    public void add(String s) { q.add(s); }
    public int size() { return q.size(); }
    public String first() { return q.peek(); }
    public void clear() {
        q.clear();
        this.snapshot = null;
    }
}
