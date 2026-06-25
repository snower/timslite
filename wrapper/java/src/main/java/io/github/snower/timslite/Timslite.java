package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.TimsliteKt;

/**
 * Entry point for the timslite Java wrapper.
 * <p>
 * This facade class delegates to the UniFFI-generated Kotlin/JVM bindings.
 * The native library is loaded automatically from {@code META-INF/native}
 * resources when present. Development builds may also place the native
 * library directory on {@code timslite.native.library.path},
 * {@code jna.library.path}, or {@code java.library.path}.
 */
public final class Timslite {

    private Timslite() {
    }

    public static String version() {
        NativeLibraryLoader.load();
        return TimsliteKt.version();
    }
}
