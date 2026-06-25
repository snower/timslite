package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void closeAndReopen() {
        String path = tempDir.toString();

        Store store1 = Store.open(path);
        try {
            store1.createDataset("persist", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset1 = store1.openDataset("persist", "test");
            try {
                byte[] payload = new byte[]{10, 20, 30};
                dataset1.write(100L, payload);
                dataset1.flush();
            } finally {
                dataset1.close();
            }
        } finally {
            store1.close();
        }

        Store store2 = Store.open(path);
        try {
            Dataset dataset2 = store2.openDataset("persist", "test");
            try {
                Record record = dataset2.read(100L);
                assertNotNull(record);
                assertEquals(100L, record.getTimestamp());
                assertArrayEquals(new byte[]{10, 20, 30}, record.getData());
            } finally {
                dataset2.close();
            }
        } finally {
            store2.close();
        }
    }

    @Test
    void datasetNamesPersist() {
        String path = tempDir.toString();

        Store store1 = Store.open(path);
        try {
            store1.createDataset("test", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
        } finally {
            store1.close();
        }

        Store store2 = Store.open(path);
        try {
            List<String> names = store2.getDatasetNames();
            assertNotNull(names);
            assertTrue(names.contains("test"),
                    "dataset names should persist across store reopen");
        } finally {
            store2.close();
        }
    }
}
