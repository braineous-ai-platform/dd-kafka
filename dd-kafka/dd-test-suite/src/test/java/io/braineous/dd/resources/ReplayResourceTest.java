package io.braineous.dd.resources;

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
        when(req.reason()).thenReturn("it-test");
    }

    private static void validTimeWindow(ReplayRequest req) {
        validCommon(req);
        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");
    }

    private static void validIngestionId(ReplayRequest req) {
        validCommon(req);
        when(req.ingestionId()).thenReturn("ID-123");
    }

    private static void validDlqId(ReplayRequest req) {
        validCommon(req);
        when(req.dlqId()).thenReturn("DLQ-A");
    }

    private static ReplayResult okResult() {
        ReplayResult r = mock(ReplayResult.class);
        when(r.ok()).thenReturn(true);
        // reason not needed when ok=true, but keep safe
        when(r.reason()).thenReturn(null);
        return r;
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

        ReplayResult expected = okResult();

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeWindow(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByTimeWindow(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayByTimeWindow(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByIngestion_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        validIngestionId(req);

        ReplayResult expected = okResult();

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByTimeObjectKey(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByIngestion(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayByTimeObjectKey(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByDomainDlq_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        validDlqId(req);

        ReplayResult expected = okResult();

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayByDomainDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByDomainDlq(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayByDomainDlqId(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayBySystemDlq_delegates_to_service() {
        ReplayRequest req = mock(ReplayRequest.class);
        validDlqId(req);

        ReplayResult expected = okResult();

        ReplayService svc = mock(ReplayService.class);
        when(svc.replayBySystemDlqId(req)).thenReturn(expected);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayBySystemDlq(req);
        assertEquals(200, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertSame(expected, actual);

        verify(svc).replayBySystemDlqId(req);
        verifyNoMoreInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_returns_403_when_disabled() {
        ReplayRequest req = mock(ReplayRequest.class);

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(disabledGate());

        Response resp = r.replayByTimeWindow(req);
        assertEquals(403, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-CONFIG-replay_disabled", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_badRequest_when_reason_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        when(req.reason()).thenReturn("   ");
        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-reason_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByTimeWindow_badRequest_when_window_invalid_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        when(req.reason()).thenReturn("it-test");
        when(req.fromTime()).thenReturn("2026-01-07T00:00:00Z");
        when(req.toTime()).thenReturn("2026-01-07T00:00:00Z"); // invalid (from == to)

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

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

        when(req.reason()).thenReturn("it-test");
        when(req.fromTime()).thenReturn("NOT_INSTANT");
        when(req.toTime()).thenReturn("2026-01-07T01:00:00Z");

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByTimeWindow(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-window_parse", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByIngestion_badRequest_when_ingestionId_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        when(req.reason()).thenReturn("it-test");
        when(req.ingestionId()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByIngestion(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-ingestionId_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayByDomainDlq_badRequest_when_dlqId_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        when(req.reason()).thenReturn("it-test");
        when(req.dlqId()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayByDomainDlq(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-dlqId_missing", actual.reason());

        verifyNoInteractions(svc);
    }

    @Test
    public void replayBySystemDlq_badRequest_when_dlqId_missing_and_doesNotCallService() {
        ReplayRequest req = mock(ReplayRequest.class);

        when(req.reason()).thenReturn("it-test");
        when(req.dlqId()).thenReturn("   "); // missing

        ReplayService svc = mock(ReplayService.class);

        ReplayResource r = new ReplayResource();
        r.setService(svc);
        r.setGate(enabledGate());

        Response resp = r.replayBySystemDlq(req);
        assertEquals(400, resp.getStatus());

        ReplayResult actual = entity(resp);
        assertFalse(actual.ok());
        assertEquals("DD-REPLAY-bad_request-dlqId_missing", actual.reason());

        verifyNoInteractions(svc);
    }
}






