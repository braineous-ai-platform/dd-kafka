package io.braineous.dd.replay.resources;

import ai.braineous.cgo.config.ConfigGate;
import ai.braineous.cgo.config.ConfigGates;
import ai.braineous.cgo.config.ConfigService;
import io.braineous.dd.core.config.DDConfigService;
import io.braineous.dd.replay.model.ReplayRequest;
import io.braineous.dd.replay.model.ReplayResult;
import io.braineous.dd.replay.services.ReplayService;
import org.junit.jupiter.api.Test;

import jakarta.ws.rs.core.Response;

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

    private static ReplayResult entity(Response resp) {
        assertNotNull(resp);
        Object e = resp.getEntity();
        assertNotNull(e, "Expected Response.entity to be non-null");
        assertTrue(e instanceof ReplayResult, "Expected Response.entity to be ReplayResult but was: " + e.getClass());
        return (ReplayResult) e;
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

        Response resp = r.replayByTimeWindow(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
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

        Response resp = r.replayByTimeObjectKey(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
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

        Response resp = r.replayByDomainDlqId(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
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

        Response resp = r.replayBySystemDlqId(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayBySystemDlqId(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_returns_409_when_disabled() {
        ReplayRequest req = mock(ReplayRequest.class);

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(disabledGate());

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
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

        Response resp = r.replayByTimeWindow(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayByTimeWindow(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_badRequest_when_stream_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("   ");
        when(req.reason()).thenReturn("it-test");
        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-stream_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_badRequest_when_window_invalid_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");

        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T00:00:00Z"); // from == to => invalid

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-window_invalid", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_badRequest_when_window_parse_fails_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");

        when(req.fromTime()).thenReturn("NOT_INSTANT");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-window_parse", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeObjectKey_badRequest_when_objectKey_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");
        when(req.objectKey()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayByTimeObjectKey(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-objectKey_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByDomainDlqId_badRequest_when_dlqId_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");
        when(req.dlqId()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayByDomainDlqId(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-dlqId_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayBySystemDlqId_badRequest_when_dlqId_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ConfigGate cfgGate = enabledGate();

        when(req.stream()).thenReturn("ingestion");
        when(req.reason()).thenReturn("it-test");
        when(req.dlqId()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(cfgGate);

        Response resp = r.replayBySystemDlqId(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-dlqId_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeObjectKey_returns_409_when_disabled_andDoesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(disabledGate());

        Response resp = r.replayByTimeObjectKey(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-CONFIG-replay_disabled", actual.reason());

        verifyNoInteractions(svc);
    }
}




