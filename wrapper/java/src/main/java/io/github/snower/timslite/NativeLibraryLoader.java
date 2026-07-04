package io.github.snower.timslite;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public final class NativeLibraryLoader {

    private static final String BASE_LIBRARY_NAME = "timslite_java";
    private static final String LIBRARY_OVERRIDE_PROPERTY = "uniffi.component.timslite.libraryOverride";
    private static final String NATIVE_RESOURCE_ROOT = "META-INF/native";
    private static final String LINUX_LIBGCC_NAME = "libgcc_s.so.1";

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
        String libraryName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        String resourceDir = resourcePath.substring(0, resourcePath.lastIndexOf('/'));
        Path tempDir = Files.createTempDirectory("timslite_native");
        tempDir.toFile().deleteOnExit();
        extractResourceIfPresent(resourceDir + "/" + LINUX_LIBGCC_NAME, tempDir.resolve(LINUX_LIBGCC_NAME));

        Path tempFile = tempDir.resolve(libraryName);
        extractRequiredResource(resourcePath, tempFile);

        System.load(tempFile.toAbsolutePath().toString());
        System.setProperty(LIBRARY_OVERRIDE_PROPERTY, tempFile.toAbsolutePath().toString());
    }

    private static void extractRequiredResource(String resourcePath, Path targetPath) throws IOException {
        String classpathResource = "/" + resourcePath;
        InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
        if (is == null) {
            is = NativeLibraryLoader.class.getResourceAsStream(classpathResource);
        }
        if (is == null) {
            throw new IOException("Native library not found in classpath: " + classpathResource);
        }

        try {
            Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            is.close();
        }
        targetPath.toFile().deleteOnExit();
    }

    private static void extractResourceIfPresent(String resourcePath, Path targetPath) throws IOException {
        String classpathResource = "/" + resourcePath;
        InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
        if (is == null) {
            is = NativeLibraryLoader.class.getResourceAsStream(classpathResource);
        }
        if (is == null) {
            return;
        }

        try {
            Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            is.close();
        }
        targetPath.toFile().deleteOnExit();
    }

    static String resolveResourcePath(String osName, String archName) {
        return NATIVE_RESOURCE_ROOT + "/" + platformClassifier(osName, archName) + "/" + mappedLibraryName(osName);
    }

    static String resolveResourcePath(String osName, String archName, boolean musl) {
        return NATIVE_RESOURCE_ROOT + "/" + platformClassifier(osName, archName, musl) + "/" + mappedLibraryName(osName);
    }

    private static String resolveResourcePath() {
        return resolveResourcePath(
            System.getProperty("os.name", ""),
            System.getProperty("os.arch", "")
        );
    }

    static String platformClassifier(String osName, String archName) {
        return platformClassifier(osName, archName, isMusl(osName));
    }

    static String platformClassifier(String osName, String archName, boolean musl) {
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
            return musl ? "linux-x86_64-musl" : "linux-x86_64";
        } else if (isLinux && isArm) {
            return musl ? "linux-aarch64-musl" : "linux-aarch64";
        } else if (isWindows && isX86) {
            return "windows-x86_64";
        } else if (isWindows && isArm) {
            return "windows-aarch64";
        }

        throw new UnsatisfiedLinkError("Unsupported platform: " + os + "-" + arch);
    }

    static boolean isMusl(String osName) {
        String os = osName.toLowerCase();
        if (!os.contains("linux")) {
            return false;
        }

        String configuredLibc = System.getProperty("timslite.native.libc", "").toLowerCase();
        if ("musl".equals(configuredLibc)) {
            return true;
        }
        if ("gnu".equals(configuredLibc) || "glibc".equals(configuredLibc)) {
            return false;
        }

        Boolean filesystemResult = isMuslFromFilesystem();
        if (filesystemResult != null) {
            return filesystemResult.booleanValue();
        }
        return isMuslFromLddVersion();
    }

    private static Boolean isMuslFromFilesystem() {
        try {
            String ldd = new String(Files.readAllBytes(Paths.get("/usr/bin/ldd")), StandardCharsets.UTF_8);
            return Boolean.valueOf(ldd.contains("musl"));
        } catch (IOException e) {
            return null;
        }
    }

    private static boolean isMuslFromLddVersion() {
        Process process = null;
        try {
            process = new ProcessBuilder("ldd", "--version").redirectErrorStream(true).start();
            byte[] output = readProcessOutput(process);
            int exitCode = process.waitFor();
            return exitCode == 0 && new String(output, StandardCharsets.UTF_8).contains("musl");
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    private static byte[] readProcessOutput(Process process) throws IOException {
        InputStream stream = process.getInputStream();
        try {
            byte[] buffer = new byte[4096];
            int total = 0;
            int read;
            byte[] output = new byte[0];
            while ((read = stream.read(buffer)) != -1) {
                byte[] next = new byte[total + read];
                System.arraycopy(output, 0, next, 0, total);
                System.arraycopy(buffer, 0, next, total, read);
                output = next;
                total += read;
            }
            return output;
        } finally {
            stream.close();
        }
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
