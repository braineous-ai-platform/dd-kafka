package io.braineous.dd.dlq.persistence;

public interface DLQStore {

    public void storeDomainFailure(String payload);

    public void storeSystemFailure(String payload);
}
