package io.braineous.dd.replay.resources;

import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.services.ReplayService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

public class ReplayResourceTest {

    @Test
    public void replayByTimeWindow_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("TW");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeWindow(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);

        ReplayResult actual = r.replayByTimeWindow(req);

        assertSame(expected, actual);
        verify(svc).replayByTimeWindow(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByTimeObjectKey_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("TOK");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeObjectKey(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);

        ReplayResult actual = r.replayByTimeObjectKey(req);

        assertSame(expected, actual);
        verify(svc).replayByTimeObjectKey(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByDomainDlqId_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("DD");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByDomainDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);

        ReplayResult actual = r.replayByDomainDlqId(req);

        assertSame(expected, actual);
        verify(svc).replayByDomainDlqId(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayBySystemDlqId_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("SD");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayBySystemDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);

        ReplayResult actual = r.replayBySystemDlqId(req);

        assertSame(expected, actual);
        verify(svc).replayBySystemDlqId(req);
        verifyNoMoreInteractions(svc);
    }
}


