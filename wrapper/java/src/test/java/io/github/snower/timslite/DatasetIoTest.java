package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasetIoTest {

    @TempDir
    Path tempDir;

    @Test
    void writeAndRead() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("testds", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("testds", "metrics");
            try {
                byte[] payload = new byte[]{1, 2, 3, 4, 5};
                dataset.write(100L, payload);

                Record record = dataset.read(100L);
                assertNotNull(record);
                assertEquals(100L, record.getTimestamp());
                assertArrayEquals(payload, record.getData());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void readNonExistent() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("testds", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("testds", "metrics");
            try {
                Record record = dataset.read(999L);
                assertNull(record);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void appendForward() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("appds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("appds", "logs");
            try {
                dataset.write(100L, new byte[]{10, 20});
                dataset.append(200L, new byte[]{30, 40});

                Record r1 = dataset.read(100L);
                assertNotNull(r1);
                assertArrayEquals(new byte[]{10, 20}, r1.getData());

                Record r2 = dataset.read(200L);
                assertNotNull(r2);
                assertArrayEquals(new byte[]{30, 40}, r2.getData());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void appendLatest() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("appds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("appds", "logs");
            try {
                dataset.write(100L, new byte[]{1, 2});
                dataset.append(100L, new byte[]{3, 4});

                Record latest = dataset.readLatest();
                assertNotNull(latest);
                assertEquals(100L, latest.getTimestamp());
                assertArrayEquals(new byte[]{1, 2, 3, 4}, latest.getData());
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
            store.createDataset("delds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("delds", "test");
            try {
                dataset.write(100L, new byte[]{99});
                assertTrue(dataset.readExist(100L));

                dataset.delete(100L);
                assertNull(dataset.read(100L));
                assertFalse(dataset.readExist(100L));
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void readLatest() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("latds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("latds", "test");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                Record latest = dataset.readLatest();
                assertNotNull(latest);
                assertEquals(300L, latest.getTimestamp());
                assertArrayEquals(new byte[]{3}, latest.getData());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void readExist() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("exds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("exds", "test");
            try {
                dataset.write(100L, new byte[]{1});
                assertTrue(dataset.readExist(100L));
                assertFalse(dataset.readExist(999L));
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void readLength() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("lends", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lends", "test");
            try {
                byte[] payload = new byte[]{1, 2, 3, 4, 5, 6, 7};
                dataset.write(100L, payload);

                Integer len = dataset.readLength(100L);
                assertNotNull(len);
                assertEquals(payload.length, len.intValue());

                Integer noLen = dataset.readLength(999L);
                assertNull(noLen);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void flushNoException() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("flushds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("flushds", "test");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                assertDoesNotThrow(() -> dataset.flush());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void closedDatasetThrows() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("closedds", "test",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("closedds", "test");
            dataset.close();
            assertTrue(dataset.isClosed());

            assertThrows(IllegalStateException.class,
                    () -> dataset.write(1L, new byte[]{1}));
            assertThrows(IllegalStateException.class,
                    () -> dataset.read(1L));
            assertThrows(IllegalStateException.class,
                    () -> dataset.delete(1L));
            assertThrows(IllegalStateException.class,
                    () -> dataset.append(1L, new byte[]{1}));
            assertThrows(IllegalStateException.class,
                    () -> dataset.flush());
        } finally {
            store.close();
        }
    }

    @Test
    void writeNowAndAppendNow() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("nowapi", "metrics",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("nowapi", "metrics");
            try {
                long before = System.currentTimeMillis() / 1000;
                byte[] payload = new byte[]{1, 2, 3};
                dataset.writeNow(payload);
                long after = System.currentTimeMillis() / 1000;

                Record record = dataset.readLatest();
                assertNotNull(record);
                assertTrue(record.getTimestamp() >= before && record.getTimestamp() <= after,
                        "write_now timestamp should be in [" + before + ", " + after + "]");
                assertArrayEquals(payload, record.getData());

                // Test appendNow
                byte[] appendPayload = new byte[]{4, 5};
                dataset.appendNow(appendPayload);
                long afterAppend = System.currentTimeMillis() / 1000;

                Record appendRecord = dataset.readLatest();
                assertNotNull(appendRecord);
                assertTrue(appendRecord.getTimestamp() >= record.getTimestamp()
                        && appendRecord.getTimestamp() <= afterAppend);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }
}
