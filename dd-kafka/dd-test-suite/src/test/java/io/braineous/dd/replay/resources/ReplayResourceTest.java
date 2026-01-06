package io.braineous.dd.replay.resources;

import ai.braineous.cgo.config.ConfigGate;
import ai.braineous.cgo.config.ConfigGates;
import ai.braineous.cgo.config.ConfigService;
import io.braineous.dd.core.config.DDConfigService;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.services.ReplayService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ReplayResourceTest {

    @Test
    public void replayByTimeWindow_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("TW");
        ConfigService cfgSvc = new DDConfigService().configService();
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeWindow(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayByTimeWindow(req);

        assertSame(expected, actual);
        verify(svc).replayByTimeWindow(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByTimeObjectKey_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("TOK");
        ConfigService cfgSvc = new DDConfigService().configService();
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeObjectKey(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayByTimeObjectKey(req);

        assertSame(expected, actual);
        verify(svc).replayByTimeObjectKey(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByDomainDlqId_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("DD");
        ConfigService cfgSvc = new DDConfigService().configService();
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByDomainDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayByDomainDlqId(req);

        assertSame(expected, actual);
        verify(svc).replayByDomainDlqId(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayBySystemDlqId_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        ReplayResult expected = ReplayResult.badRequest("SD");
        ConfigService cfgSvc = new DDConfigService().configService();
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayBySystemDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayBySystemDlqId(req);

        assertSame(expected, actual);
        verify(svc).replayBySystemDlqId(req);
        verifyNoMoreInteractions(svc);
    }



    @Test
    public void replayByTimeWindow_returns_fail_when_disabled() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "false"); // use the real key
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayByTimeWindow(req);

        assertFalse(actual.ok());
        assertEquals("DD-CONFIG-replay_disabled", actual.reason());

        verifyNoInteractions(svc); // 🔥 this is the whole point
    }

    @Test
    public void replayByTimeWindow_delegates_to_service_when_enabled() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");
        ConfigGate cfgGate = ConfigGates.from(cfgSvc);

        ReplayResult expected = ReplayResult.empty(req);

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeWindow(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        ReplayResult actual = r.replayByTimeWindow(req);

        assertSame(expected, actual);
        verify(svc).replayByTimeWindow(req);
        verifyNoMoreInteractions(svc);
    }


}


