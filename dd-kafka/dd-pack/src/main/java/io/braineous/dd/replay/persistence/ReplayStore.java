package io.braineous.dd.replay.persistence;

import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;

public interface ReplayStore {

    public java.util.List<ReplayEvent> findByTimeWindow(ReplayRequest request);

    public java.util.List<ReplayEvent> findByTimeObjectKey(ReplayRequest request);

    public java.util.List<ReplayEvent> findByDomainDlqId(ReplayRequest request);

    public java.util.List<ReplayEvent> findBySystemDlqId(ReplayRequest request);
}
