package io.braineous.dd.support;


import io.braineous.dd.ingestion.persistence.IngestionReceipt;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public final class DDAssert {

    private DDAssert() {
        // no instances
    }

    public static void assertReceiptOk(IngestionReceipt r, String expectedStoreType) {
        assertNotNull(r);
        assertTrue(r.ok());

        assertNotNull(r.ingestionId());
        assertTrue(r.ingestionId().trim().length() > 0);

        assertEquals(expectedStoreType, r.storeType());

        assertNotNull(r.payloadHash());
        assertTrue(r.payloadHash().trim().length() > 0);

        assertNotNull(r.snapshotHash());
        assertNotNull(r.snapshotHash().getValue());
        assertTrue(r.snapshotHash().getValue().trim().length() > 0);
    }

    public static String assertStoredOnceAndGetPayload(InMemoryIngestionStore store, IngestionReceipt r) {
        assertNotNull(store);
        assertEquals(1, store.storeCalls());

        String stored = store.lastStoredPayload();
        assertNotNull(stored);
        assertTrue(stored.trim().length() > 0);

        assertEquals(IngestionReceipt.sha256Hex(stored), r.payloadHash());
        return stored;
    }

    public static void assertPayloadContains(String stored, String... markers) {
        assertNotNull(stored);
        if (markers == null) {
            return;
        }
        for (int i = 0; i < markers.length; i++) {
            String m = markers[i];
            if (m == null) {
                continue;
            }
            assertTrue(stored.contains(m));
        }
    }

    public static void assertDeterministicReceipts(InMemoryIngestionStore store, IngestionReceipt r1, IngestionReceipt r2) {
        assertReceiptOk(r1, "memory");
        assertReceiptOk(r2, "memory");

        assertEquals(r1.snapshotHash().getValue(), r2.snapshotHash().getValue());
        assertEquals(r1.ingestionId(), r2.ingestionId());

        assertNotNull(store);
        assertEquals(2, store.storeCalls());

        List<String> payloads = store.storedPayloads();
        assertNotNull(payloads);
        assertEquals(2, payloads.size());

        String p1 = payloads.get(0);
        String p2 = payloads.get(1);

        assertNotNull(p1);
        assertNotNull(p2);

        assertEquals(p1, p2);

        assertEquals(IngestionReceipt.sha256Hex(p1), r1.payloadHash());
        assertEquals(IngestionReceipt.sha256Hex(p2), r2.payloadHash());
    }
}
