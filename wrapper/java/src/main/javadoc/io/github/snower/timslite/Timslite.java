package io.github.snower.timslite;

/**
 * Entry point for the timslite Java wrapper.
 *
 * <p>The runtime artifact exposes the complete Store, Dataset, Queue, and
 * Journal facade APIs. This documentation source is used only for the Maven
 * Central javadoc artifact so Javadoc does not need to parse UniFFI-generated
 * Kotlin bridge types.</p>
 */
public final class Timslite {
    private Timslite() {
    }

    /**
     * Returns the native timslite library version.
     *
     * @return version string
     */
    public static String version() {
        throw new UnsupportedOperationException("Documentation stub");
    }
}
