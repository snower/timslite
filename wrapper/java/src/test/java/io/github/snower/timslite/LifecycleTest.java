package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LifecycleTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("timslite-lifecycle");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Test
    void openAndCloseStore() {
        Store store = Store.open(tempDir.toString());
        assertNotNull(store);
        assertFalse(store.isClosed());
        store.close();
        assertTrue(store.isClosed());
    }

    @Test
    void closeIdempotency() {
        Store store = Store.open(tempDir.toString());
        store.close();
        assertTrue(store.isClosed());
        store.close();
        assertTrue(store.isClosed());
    }

    @Test
    void storeOperationsAfterCloseThrows() {
        Store store = Store.open(tempDir.toString());
        store.close();

        assertThrows(IllegalStateException.class, () -> store.isReadOnly());
        assertThrows(IllegalStateException.class, () -> store.getDatasetNames());
    }

    @Test
    void createAndOpenDataset() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("testds", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("testds", "metrics");
            assertNotNull(dataset);
            assertFalse(dataset.isClosed());
            dataset.close();
            assertTrue(dataset.isClosed());
        } finally {
            store.close();
        }
    }

    @Test
    void openDatasetByIdentifier() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("ds_by_id", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            InspectResult inspect = store.inspectDataset("ds_by_id", "logs");
            long identifier = inspect.getInfo().getIdentifier();

            Dataset dataset = store.openDatasetByIdentifier(identifier);
            assertNotNull(dataset);
            assertFalse(dataset.isClosed());
            dataset.close();
        } finally {
            store.close();
        }
    }

    @Test
    void dropDataset() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("todrop", "temp",
                    CreateDatasetOptionsBuilder.builder().build());
            List<String> names = store.getDatasetNames();
            assertTrue(names.contains("todrop"));

            store.dropDataset("todrop", "temp");
            names = store.getDatasetNames();
            assertFalse(names.contains("todrop"));
        } finally {
            store.close();
        }
    }

    @Test
    void datasetOperationsAfterCloseThrows() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("ds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("ds", "test");
            dataset.close();

            assertThrows(IllegalStateException.class, () -> dataset.read(1L));
            assertThrows(IllegalStateException.class, () -> dataset.readLatest());
            assertThrows(IllegalStateException.class, () -> dataset.write(1L, new byte[]{1}));
            assertThrows(IllegalStateException.class, () -> dataset.append(1L, new byte[]{1}));
            assertThrows(IllegalStateException.class, () -> dataset.delete(1L));
            assertThrows(IllegalStateException.class, () -> dataset.flush());
        } finally {
            store.close();
        }
    }

    @Test
    void writeAndReadRecord() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("sensor", "temperature",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("sensor", "temperature");
            try {
                byte[] payload = new byte[]{2, 1, 5};
                dataset.write(1700000001L, payload);

                Record record = dataset.read(1700000001L);
                assertNotNull(record);
                assertEquals(1700000001L, record.getTimestamp());
                assertArrayEquals(payload, record.getData());

                Record latest = dataset.readLatest();
                assertNotNull(latest);
                assertEquals(1700000001L, latest.getTimestamp());

                assertTrue(dataset.readExist(1700000001L));
                assertFalse(dataset.readExist(1700000002L));

                Integer len = dataset.readLength(1700000001L);
                assertNotNull(len);
                assertEquals(payload.length, len.intValue());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void writeAndAppend() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("appendtest", "log",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("appendtest", "log");
            try {
                dataset.write(1700000100L, new byte[]{1, 2});
                dataset.append(1700000100L, new byte[]{3, 4});

                Record record = dataset.read(1700000100L);
                assertNotNull(record);
                assertArrayEquals(new byte[]{1, 2, 3, 4}, record.getData());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryRecords() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("queryds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("queryds", "data");
            try {
                dataset.write(1700000001L, new byte[]{10});
                dataset.write(1700000002L, new byte[]{20});
                dataset.write(1700000003L, new byte[]{30});

                List<Record> records = dataset.query(1700000001L, 1700000003L);
                assertEquals(3, records.size());
                assertEquals(1700000001L, records.get(0).getTimestamp());
                assertEquals(1700000002L, records.get(1).getTimestamp());
                assertEquals(1700000003L, records.get(2).getTimestamp());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void deleteRecord() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("dels", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("dels", "test");
            try {
                dataset.write(1700000500L, new byte[]{99});
                assertTrue(dataset.readExist(1700000500L));

                dataset.delete(1700000500L);
                assertFalse(dataset.readExist(1700000500L));
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void readLatestReturnsNullForEmptyDataset() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("empty", "void",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("empty", "void");
            try {
                Record latest = dataset.readLatest();
                assertNull(latest);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryLength() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("len", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("len", "metrics");
            try {
                dataset.write(1700000001L, new byte[]{1, 2, 3});
                dataset.write(1700000002L, new byte[]{4, 5});

                List<LengthEntry> entries = dataset.queryLength(1700000001L, 1700000002L);
                assertEquals(2, entries.size());
                assertEquals(1700000001L, entries.get(0).getTimestamp());
                assertEquals(3, entries.get(0).getLength());
                assertEquals(1700000002L, entries.get(1).getTimestamp());
                assertEquals(2, entries.get(1).getLength());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void inspectDataset() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("insp", "info",
                    CreateDatasetOptionsBuilder.builder().build());
            InspectResult result = store.inspectDataset("insp", "info");
            assertNotNull(result);
            assertNotNull(result.getInfo());
            assertEquals("insp", result.getInfo().getName());
            assertEquals("info", result.getInfo().getDatasetType());
            assertNotNull(result.getState());
        } finally {
            store.close();
        }
    }
}
