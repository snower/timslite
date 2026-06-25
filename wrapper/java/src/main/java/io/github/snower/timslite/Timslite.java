package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.TimsliteKt;

/**
 * Entry point for the timslite Java wrapper.
 * <p>
 * This facade class delegates to the UniFFI-generated Kotlin/JVM bindings.
 * The native library is loaded automatically by JNA when the first
 * UniFFI binding call is made. Ensure the native library directory is
 * on {@code jna.library.path} or {@code java.library.path}.
 */
public final class Timslite {

    private Timslite() {
    }

    public static String version() {
        NativeLibraryLoader.load();
        return TimsliteKt.version();
    }
}
