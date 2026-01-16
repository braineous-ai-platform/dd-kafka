package io.braineous.dd.dlq.persistence;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public interface DLQStore {

    public void storeDomainFailure(String payload);

    public void storeSystemFailure(String payload);

    public JsonArray findSystemFailureByTimeWindow(String fromTime, String toTime);
    public JsonObject findSystemFailureById(String dlqId);

    public JsonArray findDomainFailureByTimeWindow(String fromTime, String toTime);
    public JsonObject findDomainFailureById(String dlqId);
}
