package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

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

    @Test
    void nativeResourcePathsUsePlatformDirectoriesAndStandardLibraryNames() {
        assertEquals("META-INF/native/linux-x86_64/libtimslite_java.so",
            NativeLibraryLoader.resolveResourcePath("Linux", "amd64", false));
        assertEquals("META-INF/native/linux-aarch64/libtimslite_java.so",
            NativeLibraryLoader.resolveResourcePath("Linux", "aarch64", false));
        assertEquals("META-INF/native/linux-x86_64-musl/libtimslite_java.so",
            NativeLibraryLoader.resolveResourcePath("Linux", "amd64", true));
        assertEquals("META-INF/native/linux-aarch64-musl/libtimslite_java.so",
            NativeLibraryLoader.resolveResourcePath("Linux", "aarch64", true));
        assertEquals("META-INF/native/macos-x86_64/libtimslite_java.dylib",
            NativeLibraryLoader.resolveResourcePath("Mac OS X", "x86_64"));
        assertEquals("META-INF/native/macos-aarch64/libtimslite_java.dylib",
            NativeLibraryLoader.resolveResourcePath("Mac OS X", "aarch64"));
        assertEquals("META-INF/native/windows-x86_64/timslite_java.dll",
            NativeLibraryLoader.resolveResourcePath("Windows 11", "amd64"));
        assertEquals("META-INF/native/windows-aarch64/timslite_java.dll",
            NativeLibraryLoader.resolveResourcePath("Windows 11", "arm64"));
    }

    @Test
    void pomUsesCentralPortalPluginWithoutLegacyDistributionManagement() throws Exception {
        assertFalse(Files.readAllLines(Paths.get("pom.xml")).stream()
            .anyMatch(line -> line.contains("<distributionManagement>")),
            "Central Portal plugin should own release upload instead of legacy distributionManagement");

        Document document = DocumentBuilderFactory.newInstance()
            .newDocumentBuilder()
            .parse(Paths.get("pom.xml").toFile());

        NodeList artifactIds = document.getElementsByTagName("artifactId");
        boolean foundCentralPlugin = false;
        for (int i = 0; i < artifactIds.getLength(); i++) {
            if ("central-publishing-maven-plugin".equals(artifactIds.item(i).getTextContent())) {
                foundCentralPlugin = true;
                break;
            }
        }
        assertTrue(foundCentralPlugin, "pom.xml should configure central-publishing-maven-plugin");
    }
}
