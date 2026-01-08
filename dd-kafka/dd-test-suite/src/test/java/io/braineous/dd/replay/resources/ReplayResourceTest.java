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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ReplayResourceTest {

    // ---------------- helpers ----------------

    private static void validCommon(ReplayRequest req) {
        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");
    }

    private static void validTimeWindow(ReplayRequest req) {
        validCommon(req);
        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");
    }

    private static void validObjectKey(ReplayRequest req) {
        validCommon(req);
        when(req.objectKey()).thenReturn("OBJ-A");
    }

    private static void validDlqId(ReplayRequest req) {
        validCommon(req);
        when(req.dlqId()).thenReturn("DLQ-A");
    }

    private static ConfigGate enabledGate() {
        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "true");
        return ConfigGates.from(cfgSvc);
    }

    private static ConfigGate disabledGate() {
        ConfigService cfgSvc = new DDConfigService().configService();
        cfgSvc.setProperty("dd.feature.replay.enabled", "false");
        return ConfigGates.from(cfgSvc);
    }

    // ---------------- tests ----------------

    @Test
    public void replayByTimeWindow_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        validTimeWindow(req);

        ReplayResult expected = ReplayResult.badRequest("TW");
        ConfigGate cfgGate = enabledGate();

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
        validObjectKey(req);

        ReplayResult expected = ReplayResult.badRequest("TOK");
        ConfigGate cfgGate = enabledGate();

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
        validDlqId(req);

        ReplayResult expected = ReplayResult.badRequest("DD");
        ConfigGate cfgGate = enabledGate();

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
        validDlqId(req);

        ReplayResult expected = ReplayResult.badRequest("SD");
        ConfigGate cfgGate = enabledGate();

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

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(disabledGate());

        ReplayResult actual = r.replayByTimeWindow(req);

        assertFalse(actual.ok());
        assertEquals("DD-CONFIG-replay_disabled", actual.reason());
        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_delegates_to_service_when_enabled() {
        ReplayRequest req = mock(ReplayRequest.class);
        validTimeWindow(req);

        ConfigGate cfgGate = enabledGate();

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



