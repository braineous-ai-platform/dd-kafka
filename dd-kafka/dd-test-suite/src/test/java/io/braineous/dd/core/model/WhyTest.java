package io.braineous.dd.core.model;

import ai.braineous.rag.prompt.observe.Console;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WhyTest {

    @Test
    void toJson_serializesReasonAndDetails() {
        Why why = new Why("DD-REST-call_failed", "ConnectException");

        String json = why.toJson();
        Console.log("WHY_JSON", json);

        assertNotNull(json);
        assertTrue(json.contains("\"reason\":\"DD-REST-call_failed\""));
        assertTrue(json.contains("\"details\":\"ConnectException\""));
    }

    @Test
    void toJson_handlesNullFields() {
        Why why = new Why(null, null);

        String json = why.toJson();
        Console.log("WHY_JSON_NULLS", json);

        assertNotNull(json);
        assertEquals("{}", json);
    }

    @Test
    void toJson_escapesSpecialCharacters() {
        Why why = new Why("A\"B\\C\nD", "tab\tcr");

        String json = why.toJson();
        Console.log("WHY_JSON_ESCAPES", json);

        assertNotNull(json);

        // parse back to verify correctness (no fromJson method needed)
        com.google.gson.JsonObject obj =
                com.google.gson.JsonParser.parseString(json).getAsJsonObject();

        assertEquals("A\"B\\C\nD", obj.get("reason").getAsString());
        assertEquals("tab\tcr", obj.get("details").getAsString());
    }

    @Test
    void toString_formatsReasonAndDetails() {
        Why why = new Why("DD-REST-call_failed", "ConnectException");

        String s = why.toString();
        Console.log("WHY_TOSTRING", s);

        assertEquals("Why{reason='DD-REST-call_failed', details='ConnectException'}", s);
    }


}
