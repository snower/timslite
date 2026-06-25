package io.github.snower.timslite;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class NativeLibraryLoader {

    private static final String BASE_LIBRARY_NAME = "timslite_java";
    private static final String LIBRARY_OVERRIDE_PROPERTY = "uniffi.component.timslite.libraryOverride";
    private static final String NATIVE_RESOURCE_ROOT = "META-INF/native";

    private static volatile boolean loaded = false;

    private NativeLibraryLoader() {
    }

    public static synchronized void load() {
        if (loaded) {
            return;
        }

        if (loadFromLibraryPath()) {
            loaded = true;
            return;
        }

        if (loadFromConfiguredLibraryPath()) {
            loaded = true;
            return;
        }

        try {
            extractAndLoad(resolveResourcePath());
            loaded = true;
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to load native library: " + e.getMessage());
        }
    }

    private static boolean loadFromLibraryPath() {
        try {
            System.loadLibrary(BASE_LIBRARY_NAME);
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    private static boolean loadFromConfiguredLibraryPath() {
        String configuredPath = System.getProperty("timslite.native.library.path");
        if (configuredPath == null || configuredPath.isEmpty()) {
            configuredPath = System.getProperty("jna.library.path");
        }
        if (configuredPath == null || configuredPath.isEmpty()) {
            return false;
        }

        String platform = platformClassifier(
            System.getProperty("os.name", ""),
            System.getProperty("os.arch", "")
        );
        String mappedName = mappedLibraryName();

        for (String entry : configuredPath.split(File.pathSeparator)) {
            if (entry == null || entry.isEmpty()) {
                continue;
            }

            File root = new File(entry);
            File[] candidates = {
                new File(root, mappedName),
                new File(new File(root, platform), mappedName),
                new File(new File(new File(new File(root, "META-INF"), "native"), platform), mappedName)
            };

            for (File candidate : candidates) {
                if (candidate.exists()) {
                    try {
                        System.load(candidate.getAbsolutePath());
                        System.setProperty(LIBRARY_OVERRIDE_PROPERTY, candidate.getAbsolutePath());
                        return true;
                    } catch (UnsatisfiedLinkError e) {
                        return false;
                    }
                }
            }
        }

        return false;
    }

    private static void extractAndLoad(String resourcePath) throws IOException {
        String classpathResource = "/" + resourcePath;
        InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
        if (is == null) {
            is = NativeLibraryLoader.class.getResourceAsStream(classpathResource);
        }
        if (is == null) {
            throw new IOException("Native library not found in classpath: " + classpathResource);
        }

        String libraryName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        String suffix = libraryName.substring(libraryName.lastIndexOf('.'));
        Path tempFile = Files.createTempFile("timslite_native", suffix);
        tempFile.toFile().deleteOnExit();

        try {
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            is.close();
        }

        System.load(tempFile.toAbsolutePath().toString());
        System.setProperty(LIBRARY_OVERRIDE_PROPERTY, tempFile.toAbsolutePath().toString());
    }

    static String resolveResourcePath(String osName, String archName) {
        return NATIVE_RESOURCE_ROOT + "/" + platformClassifier(osName, archName) + "/" + mappedLibraryName(osName);
    }

    private static String resolveResourcePath() {
        return resolveResourcePath(
            System.getProperty("os.name", ""),
            System.getProperty("os.arch", "")
        );
    }

    static String platformClassifier(String osName, String archName) {
        String os = osName.toLowerCase();
        String arch = archName.toLowerCase();

        boolean isMac = os.contains("mac") || os.contains("darwin");
        boolean isLinux = os.contains("linux");
        boolean isWindows = os.contains("windows");
        boolean isArm = arch.equals("aarch64") || arch.equals("arm64");
        boolean isX86 = arch.equals("x86_64") || arch.equals("amd64");

        if (isMac && isX86) {
            return "macos-x86_64";
        } else if (isMac && isArm) {
            return "macos-aarch64";
        } else if (isLinux && isX86) {
            return "linux-x86_64";
        } else if (isLinux && isArm) {
            return "linux-aarch64";
        } else if (isWindows && isX86) {
            return "windows-x86_64";
        } else if (isWindows && isArm) {
            return "windows-aarch64";
        }

        throw new UnsatisfiedLinkError("Unsupported platform: " + os + "-" + arch);
    }

    private static String mappedLibraryName() {
        return mappedLibraryName(System.getProperty("os.name", ""));
    }

    private static String mappedLibraryName(String osName) {
        String os = osName.toLowerCase();
        boolean isMac = os.contains("mac") || os.contains("darwin");
        boolean isLinux = os.contains("linux");
        boolean isWindows = os.contains("windows");

        if (isMac) {
            return "lib" + BASE_LIBRARY_NAME + ".dylib";
        } else if (isLinux) {
            return "lib" + BASE_LIBRARY_NAME + ".so";
        } else if (isWindows) {
            return BASE_LIBRARY_NAME + ".dll";
        }
        return System.mapLibraryName(BASE_LIBRARY_NAME);
    }
}
