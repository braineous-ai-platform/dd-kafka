package io.braineous.dd.replay.persistence;

import io.braineous.dd.replay.model.ReplayEvent;
import io.braineous.dd.replay.model.ReplayRequest;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
public class NoopReplayStore implements ReplayStore {

    @Override
    public List<ReplayEvent> findByTimeWindow(ReplayRequest request) {
        return List.of();
    }

    @Override
    public List<ReplayEvent> findByTimeObjectKey(ReplayRequest request) {
        return List.of();
    }

    @Override
    public List<ReplayEvent> findByDomainDlqId(ReplayRequest request) {
        return List.of();
    }

    @Override
    public List<ReplayEvent> findBySystemDlqId(ReplayRequest request) {
        return List.of();
    }
}

