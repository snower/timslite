package io.github.snower.timslite;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmokeTest {

    @Test
    void versionIsNotNull() {
        String version = Timslite.version();
        assertNotNull(version, "Timslite.version() should not return null");
        assertTrue(version.length() > 0, "Timslite.version() should return a non-empty string");
    }
}
