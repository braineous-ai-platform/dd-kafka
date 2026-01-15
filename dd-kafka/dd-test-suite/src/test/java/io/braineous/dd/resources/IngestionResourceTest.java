package io.braineous.dd.resources;

import ai.braineous.rag.prompt.observe.Console;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.braineous.dd.processor.ProcessorOrchestrator;
import io.braineous.dd.processor.ProcessorResult;
import io.braineous.dd.core.model.Why;
import io.braineous.dd.replay.model.IngestionRequest;
import io.braineous.dd.ingestion.persistence.IngestionStore;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

public class IngestionResourceTest {

    private IngestionResource res;
    private ProcessorOrchestrator orch;
    private IngestionStore store;

    @BeforeEach
    void beforeEach() {
        res = new IngestionResource();

        orch = Mockito.mock(ProcessorOrchestrator.class);
        store = Mockito.mock(IngestionStore.class);

        set(res, "orch", orch);
        set(res, "store", store);
    }

    // ---------------- ingestion() tests ----------------

    @Test
    void ingestion_requestNull_returns400_andWhy() {
        Response resp = res.ingestion(null);

        Console.log("ut_ingestion_requestNull_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_requestNull_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_requestNull_parsed", jo);

        assertEquals(400, resp.getStatus());
        assertFalse(jo.get("ok").getAsBoolean());

        JsonObject why = jo.get("why").getAsJsonObject();
        assertEquals("DD-INGEST-request_null", why.get("code").getAsString());
    }

    @Test
    void ingestion_payloadBlank_returns400_andWhy() {
        IngestionRequest req = Mockito.mock(IngestionRequest.class);
        Mockito.when(req.getPayload()).thenReturn("   ");

        Response resp = res.ingestion(req);

        Console.log("ut_ingestion_payloadBlank_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_payloadBlank_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_payloadBlank_parsed", jo);

        assertEquals(400, resp.getStatus());
        assertFalse(jo.get("ok").getAsBoolean());

        JsonObject why = jo.get("why").getAsJsonObject();
        assertEquals("DD-INGEST-payload_blank", why.get("code").getAsString());
    }

    @Test
    void ingestion_payloadInvalidJson_returns400_andWhy() {
        IngestionRequest req = Mockito.mock(IngestionRequest.class);
        Mockito.when(req.getPayload()).thenReturn("{not-json");

        Response resp = res.ingestion(req);

        Console.log("ut_ingestion_invalidJson_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_invalidJson_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_invalidJson_parsed", jo);

        assertEquals(400, resp.getStatus());

        JsonObject why = jo.get("why").getAsJsonObject();
        assertEquals("DD-INGEST-payload_invalid_json", why.get("code").getAsString());
    }

    @Test
    void ingestion_orchOkTrue_returns200_andEnvelopeOkTrue() {
        IngestionRequest req = Mockito.mock(IngestionRequest.class);
        Mockito.when(req.getPayload()).thenReturn("{\"a\":1}");

        JsonObject ddEvent = new JsonObject();
        ddEvent.addProperty("x", "y");

        ProcessorResult pr = ProcessorResult.ok(ddEvent);
        pr.setIngestionId("ING-1");
        pr.setId("DD-PR-1");

        Mockito.when(orch.orchestrate(Mockito.any(JsonObject.class))).thenReturn(pr);

        Response resp = res.ingestion(req);

        Console.log("ut_ingestion_okTrue_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_okTrue_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_okTrue_parsed", jo);

        assertEquals(200, resp.getStatus());
        assertTrue(jo.get("ok").getAsBoolean());

        JsonObject data = jo.get("data").getAsJsonObject();
        Console.log("ut_ingestion_okTrue_data", data);

        assertTrue(data.get("ok").getAsBoolean());
        assertEquals("ING-1", data.get("ingestionId").getAsString());
        assertTrue(data.get("processorWhy").isJsonNull());
    }

    @Test
    void ingestion_orchOkFalse_returns400_andEnvelopeOkFalse() {
        IngestionRequest req = Mockito.mock(IngestionRequest.class);
        Mockito.when(req.getPayload()).thenReturn("{\"a\":1}");

        Why why = new Why("DD-DOWNSTREAM-fail", "downstream said no");
        ProcessorResult pr = ProcessorResult.fail(why);
        pr.setIngestionId("ING-2");
        pr.setId("DD-PR-2");

        Mockito.when(orch.orchestrate(Mockito.any(JsonObject.class))).thenReturn(pr);

        Response resp = res.ingestion(req);

        Console.log("ut_ingestion_okFalse_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_okFalse_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_okFalse_parsed", jo);

        assertEquals(400, resp.getStatus());
        assertFalse(jo.get("ok").getAsBoolean());

        JsonObject data = jo.get("data").getAsJsonObject();
        Console.log("ut_ingestion_okFalse_data", data);

        JsonObject pwhy = data.get("processorWhy").getAsJsonObject();
        assertEquals("DD-DOWNSTREAM-fail", pwhy.get("code").getAsString());
    }

    @Test
    void ingestion_orchReturnsNull_returns400_andOrchestrateNullWhy() {
        IngestionRequest req = Mockito.mock(IngestionRequest.class);
        Mockito.when(req.getPayload()).thenReturn("{\"a\":1}");

        Mockito.when(orch.orchestrate(Mockito.any(JsonObject.class))).thenReturn(null);

        Response resp = res.ingestion(req);

        Console.log("ut_ingestion_orchNull_status", resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_ingestion_orchNull_resp", json);

        JsonObject jo = JsonParser.parseString(json).getAsJsonObject();
        Console.log("ut_ingestion_orchNull_parsed", jo);

        assertEquals(400, resp.getStatus());

        JsonObject data = jo.get("data").getAsJsonObject();
        JsonObject pwhy = data.get("processorWhy").getAsJsonObject();

        assertEquals("DD-INGEST-orchestrate_null", pwhy.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_fromTimeNull_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow(null, "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);
        org.junit.jupiter.api.Assertions.assertNotNull(json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertNotNull(why);
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-fromTime_blank", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_fromTimeBlank_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("   ", "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-fromTime_blank", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_toTimeNull_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", null);
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-toTime_blank", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_toTimeBlank_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", "   ");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-toTime_blank", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_fromTimeInvalidIso_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("not-iso", "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-time_invalid", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_toTimeInvalidIso_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", "bad-time");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-time_invalid", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_windowEqual_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", "2026-01-15T00:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-time_window_invalid", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_windowReversed_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T02:00:00Z", "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());

        com.google.gson.JsonObject why = jo.getAsJsonObject("why");
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-time_window_invalid", why.get("code").getAsString());
    }

    @Test
    void http_findEventsByTimeWindow_trimsParams_andPassesTrimmedToStore_returns200() {
        // Arrange
        com.google.gson.JsonArray arr = new com.google.gson.JsonArray();
        com.google.gson.JsonObject e1 = new com.google.gson.JsonObject();
        e1.addProperty("id", "evt-1");
        arr.add(e1);

        org.mockito.Mockito.when(store.findEventsByTimeWindow(
                org.mockito.Mockito.eq("2026-01-15T00:00:00Z"),
                org.mockito.Mockito.eq("2026-01-15T01:00:00Z")
        )).thenReturn(arr);

        // Act
        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("  2026-01-15T00:00:00Z  ", "  2026-01-15T01:00:00Z  ");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject out = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertTrue(out.get("ok").getAsBoolean());
        org.junit.jupiter.api.Assertions.assertTrue(out.get("why").isJsonNull());

        com.google.gson.JsonObject data = out.getAsJsonObject("data");
        org.junit.jupiter.api.Assertions.assertEquals("2026-01-15T00:00:00Z", data.get("fromTime").getAsString());
        org.junit.jupiter.api.Assertions.assertEquals("2026-01-15T01:00:00Z", data.get("toTime").getAsString());
        org.junit.jupiter.api.Assertions.assertEquals(1, data.get("count").getAsInt());

        com.google.gson.JsonArray events = data.getAsJsonArray("events");
        org.junit.jupiter.api.Assertions.assertNotNull(events);
        org.junit.jupiter.api.Assertions.assertEquals(1, events.size());

        // Verify store call
        org.mockito.Mockito.verify(store, org.mockito.Mockito.times(1))
                .findEventsByTimeWindow("2026-01-15T00:00:00Z", "2026-01-15T01:00:00Z");
    }

    @Test
    void http_findEventsByTimeWindow_storeReturnsNull_mapsToEmptyEventsAndCount0_returns200() {
        org.mockito.Mockito.when(store.findEventsByTimeWindow(
                org.mockito.Mockito.anyString(),
                org.mockito.Mockito.anyString()
        )).thenReturn(null);

        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject out = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertTrue(out.get("ok").getAsBoolean());
        org.junit.jupiter.api.Assertions.assertTrue(out.get("why").isJsonNull());

        com.google.gson.JsonObject data = out.getAsJsonObject("data");
        org.junit.jupiter.api.Assertions.assertEquals(0, data.get("count").getAsInt());

        com.google.gson.JsonArray events = data.getAsJsonArray("events");
        org.junit.jupiter.api.Assertions.assertNotNull(events);
        org.junit.jupiter.api.Assertions.assertEquals(0, events.size());
    }

    @Test
    void http_findEventsByTimeWindow_storeReturnsEmptyArray_count0_returns200() {
        org.mockito.Mockito.when(store.findEventsByTimeWindow(
                org.mockito.Mockito.anyString(),
                org.mockito.Mockito.anyString()
        )).thenReturn(new com.google.gson.JsonArray());

        jakarta.ws.rs.core.Response resp = res.findEventsByTimeWindow("2026-01-15T00:00:00Z", "2026-01-15T01:00:00Z");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject out = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        com.google.gson.JsonObject data = out.getAsJsonObject("data");

        org.junit.jupiter.api.Assertions.assertEquals(0, data.get("count").getAsInt());
        org.junit.jupiter.api.Assertions.assertEquals(0, data.getAsJsonArray("events").size());
    }

    @Test
    void http_findEventsByIngestionId_ingestionIdBlank_returns400() {
        jakarta.ws.rs.core.Response resp = res.findEventsByIngestionId("   ");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(400, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-ingestionId_blank", jo.getAsJsonObject("why").get("code").getAsString());
    }

    @Test
    void http_findEventsByIngestionId_notFound_returns404() {
        org.mockito.Mockito.when(store.findEventsByIngestionId(org.mockito.Mockito.eq("ID-404")))
                .thenReturn(new com.google.gson.JsonObject());

        jakarta.ws.rs.core.Response resp = res.findEventsByIngestionId("ID-404");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(404, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject jo = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertFalse(jo.get("ok").getAsBoolean());
        org.junit.jupiter.api.Assertions.assertEquals("DD-INGEST-ingestionId_not_found", jo.getAsJsonObject("why").get("code").getAsString());
    }

    @Test
    void http_findEventsByIngestionId_happyPath_returns200_envelopeA() {
        com.google.gson.JsonObject e = new com.google.gson.JsonObject();
        e.addProperty("ingestionId", "ID-1");
        e.addProperty("createdAt", "2026-01-15T00:10:00Z");

        org.mockito.Mockito.when(store.findEventsByIngestionId(org.mockito.Mockito.eq("ID-1")))
                .thenReturn(e);

        jakarta.ws.rs.core.Response resp = res.findEventsByIngestionId(" ID-1 ");
        Console.log("ut_status", resp.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(200, resp.getStatus());

        String json = (String) resp.getEntity();
        Console.log("ut_body", json);

        com.google.gson.JsonObject out = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
        org.junit.jupiter.api.Assertions.assertTrue(out.get("ok").getAsBoolean());
        org.junit.jupiter.api.Assertions.assertTrue(out.get("why").isJsonNull());

        com.google.gson.JsonObject data = out.getAsJsonObject("data");
        org.junit.jupiter.api.Assertions.assertEquals("ID-1", data.get("ingestionId").getAsString());

        org.mockito.Mockito.verify(store, org.mockito.Mockito.times(1)).findEventsByIngestionId("ID-1");
    }



    // ---------------- reflection helper ----------------

    private static void set(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

