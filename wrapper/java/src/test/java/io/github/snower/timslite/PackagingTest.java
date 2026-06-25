package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Packaging tests for Maven Central publication readiness.
 * <p>
 * Verifies that the native library can be loaded and the public API
 * surface is accessible when the library is on {@code jna.library.path}.
 */
class PackagingTest {

    /**
     * The native library is loaded by JNA when the first UniFFI
     * binding call is made. If loading fails, this test will throw
     * {@link UnsatisfiedLinkError}.
     */
    @Test
    void nativeLibraryCanBeLoaded() {
        assertDoesNotThrow(() -> {
            String version = Timslite.version();
            assertNotNull(version, "Timslite.version() should not return null");
        }, "Native library should load without UnsatisfiedLinkError");
    }

    @Test
    void versionReturnsNonEmptyString() {
        String version = Timslite.version();
        assertNotNull(version, "version must not be null");
        assertTrue(version.length() > 0, "version must be non-empty");
    }

    /**
     * If jna.library.path is set (it is in our surefire config),
     * it should be non-empty.
     */
    @Test
    void jnaLibraryPathIsSet() {
        String jnaPath = System.getProperty("jna.library.path");
        if (jnaPath != null) {
            assertFalse(jnaPath.isEmpty(),
                "jna.library.path should be non-empty if set by surefire");
        }
    }
}
