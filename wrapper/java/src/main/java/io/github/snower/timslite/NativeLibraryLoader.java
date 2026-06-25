package io.github.snower.timslite;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class NativeLibraryLoader {

    private static volatile boolean loaded = false;

    private NativeLibraryLoader() {
    }

    public static synchronized void load() {
        if (loaded) {
            return;
        }

        String libraryName = resolveLibraryName();

        if (loadFromLibraryPath(libraryName)) {
            loaded = true;
            return;
        }

        if (loadFromJnaLibraryPath(libraryName)) {
            loaded = true;
            return;
        }

        try {
            extractAndLoad(libraryName);
            loaded = true;
        } catch (IOException e) {
            String fallbackName = getFallbackLibraryName();
            if (fallbackName != null && !fallbackName.equals(libraryName)) {
                try {
                    extractAndLoad(fallbackName);
                    loaded = true;
                    return;
                } catch (IOException ignored) {
                }
            }
            throw new UnsatisfiedLinkError("Failed to load native library: " + e.getMessage());
        }
    }

    private static boolean loadFromLibraryPath(String libraryName) {
        try {
            System.loadLibrary(libraryName);
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    private static boolean loadFromJnaLibraryPath(String libraryName) {
        String jnaPath = System.getProperty("jna.library.path");
        if (jnaPath == null || jnaPath.isEmpty()) {
            return false;
        }

        java.io.File libFile = new java.io.File(jnaPath, libraryName);
        if (libFile.exists()) {
            try {
                System.load(libFile.getAbsolutePath());
                return true;
            } catch (UnsatisfiedLinkError e) {
                return false;
            }
        }

        String fallbackName = getFallbackLibraryName();
        if (fallbackName != null && !fallbackName.equals(libraryName)) {
            java.io.File fallbackFile = new java.io.File(jnaPath, fallbackName);
            if (fallbackFile.exists()) {
                try {
                    System.load(fallbackFile.getAbsolutePath());
                    return true;
                } catch (UnsatisfiedLinkError e) {
                    return false;
                }
            }
        }

        return false;
    }

    private static void extractAndLoad(String libraryName) throws IOException {
        String resourcePath = "/" + libraryName;
        InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
        if (is == null) {
            throw new IOException("Native library not found in classpath: " + resourcePath);
        }

        String suffix = libraryName.substring(libraryName.lastIndexOf('.'));
        Path tempFile = Files.createTempFile("timslite_native", suffix);
        tempFile.toFile().deleteOnExit();

        try {
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            is.close();
        }

        System.load(tempFile.toAbsolutePath().toString());
        System.setProperty("uniffi.component.timslite.libraryOverride", tempFile.toAbsolutePath().toString());
    }

    private static String resolveLibraryName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        boolean isMac = os.contains("mac") || os.contains("darwin");
        boolean isLinux = os.contains("linux");
        boolean isWindows = os.contains("windows");
        boolean isArm = arch.equals("aarch64") || arch.equals("arm64");
        boolean isX86 = arch.equals("x86_64") || arch.equals("amd64");

        if (isMac && isArm) {
            return "libtimslite_java-macos-aarch64.dylib";
        } else if (isLinux && isX86) {
            return "libtimslite_java-linux-x86_64.so";
        } else if (isLinux && isArm) {
            return "libtimslite_java-linux-aarch64.so";
        } else if (isWindows && isX86) {
            return "timslite_java-windows-x86_64.dll";
        } else if (isWindows && isArm) {
            return "timslite_java-windows-aarch64.dll";
        }

        throw new UnsatisfiedLinkError("Unsupported platform: " + os + "-" + arch);
    }

    private static String getFallbackLibraryName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        boolean isMac = os.contains("mac") || os.contains("darwin");
        boolean isLinux = os.contains("linux");
        boolean isWindows = os.contains("windows");

        if (isMac) {
            return "libtimslite_java.dylib";
        } else if (isLinux) {
            return "libtimslite_java.so";
        } else if (isWindows) {
            return "timslite_java.dll";
        }
        return null;
    }
}
