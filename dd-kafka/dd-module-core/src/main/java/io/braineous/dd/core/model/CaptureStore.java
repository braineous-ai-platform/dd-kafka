package io.braineous.dd.core.model;

import ai.braineous.rag.prompt.models.cgo.graph.GraphSnapshot;
import ai.braineous.rag.prompt.observe.Console;

public class CaptureStore {
    private static final CaptureStore store = new CaptureStore();

    public static CaptureStore getInstance(){
        return store;
    }

    private final java.util.concurrent.ConcurrentLinkedQueue<String> q = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final java.util.concurrent.ConcurrentLinkedQueue<String> sq = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final java.util.concurrent.ConcurrentLinkedQueue<String> dq = new java.util.concurrent.ConcurrentLinkedQueue<>();

    private Object dlqResult;

    public Object getDlqResult() {
        return dlqResult;
    }

    public void setDlqResult(Object dlqResult) {
        this.dlqResult = dlqResult;
    }

    private GraphSnapshot snapshot;

    public GraphSnapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(GraphSnapshot snapshot) {
        Console.log("DD_DEBUG_GRAPH", snapshot.toJson().toString());
        this.snapshot = snapshot;
    }

    //ingestion_queue
    public void add(String s) { q.add(s); }
    public int size() { return q.size(); }
    public String first() { return q.peek(); }
    public void clear() {
        q.clear();
        sq.clear();
        dq.clear();
        this.snapshot = null;
        this.dlqResult = null;
    }

    //ddl_system_queue
    public void addSystemFailure(String s) { sq.add(s); }
    public int sizeSystemFailure() { return sq.size(); }
    public String firstSystemFailure() { return sq.peek(); }

    //ddl_domain_queue
    public void addDomainFailure(String s) { dq.add(s); }
    public int sizeDomainFailure() { return dq.size(); }
    public String firstDomainFailure() { return dq.peek(); }
}
