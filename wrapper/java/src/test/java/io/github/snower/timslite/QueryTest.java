package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryTest {

    @TempDir
    Path tempDir;

    @Test
    void queryRange() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                List<Record> records = dataset.query(150L, 250L);
                assertEquals(1, records.size());
                assertEquals(200L, records.get(0).getTimestamp());
                assertArrayEquals(new byte[]{2}, records.get(0).getData());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryAll() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "data");
            try {
                dataset.write(100L, new byte[]{10});
                dataset.write(200L, new byte[]{20});
                dataset.write(300L, new byte[]{30});

                List<Record> records = dataset.query(0L, 400L);
                assertEquals(3, records.size());
                assertEquals(100L, records.get(0).getTimestamp());
                assertEquals(200L, records.get(1).getTimestamp());
                assertEquals(300L, records.get(2).getTimestamp());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryExistBitmap() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("exqds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("exqds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                byte[] bitmap = dataset.queryExist(0L, 400L);
                assertNotNull(bitmap);
                assertTrue(bitmap.length > 0);

                boolean hasSetBits = false;
                for (byte b : bitmap) {
                    if (b != 0) {
                        hasSetBits = true;
                        break;
                    }
                }
                assertTrue(hasSetBits, "bitmap should have set bits for existing timestamps");
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
            store.createDataset("lends", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lends", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});

                List<LengthEntry> entries = dataset.queryLength(0L, 300L);
                assertEquals(2, entries.size());
                assertEquals(100L, entries.get(0).getTimestamp());
                assertEquals(3, entries.get(0).getLength());
                assertEquals(200L, entries.get(1).getTimestamp());
                assertEquals(2, entries.get(1).getLength());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryIter() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("iterds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("iterds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                QueryIterator iter = dataset.queryIter(0L, 400L);
                int count = 0;
                try {
                    assertTrue(iter.hasNext());
                    Record r1 = iter.next();
                    assertEquals(300L, r1.getTimestamp());
                    count++;

                    assertTrue(iter.hasNext());
                    Record r2 = iter.next();
                    assertEquals(200L, r2.getTimestamp());
                    count++;

                    assertTrue(iter.hasNext());
                    Record r3 = iter.next();
                    assertEquals(100L, r3.getTimestamp());
                    count++;
                } finally {
                    iter.close();
                }
                assertEquals(3, count);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryLengthIter() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("leniterds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("leniterds", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});

                QueryLengthIterator iter = dataset.queryLengthIter(0L, 300L);
                int count = 0;
                try {
                    assertTrue(iter.hasNext());
                    LengthEntry e1 = iter.next();
                    assertEquals(200L, e1.getTimestamp());
                    assertEquals(2, e1.getLength());
                    count++;

                    assertTrue(iter.hasNext());
                    LengthEntry e2 = iter.next();
                    assertEquals(100L, e2.getTimestamp());
                    assertEquals(3, e2.getLength());
                    count++;
                } finally {
                    iter.close();
                }
                assertEquals(2, count);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void queryEmptyRange() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("emptyqds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("emptyqds", "data");
            try {
                dataset.write(100L, new byte[]{1});

                List<Record> records = dataset.query(0L, 10L);
                assertNotNull(records);
                assertTrue(records.isEmpty());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void iteratorExhaustion() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("exhaustds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("exhaustds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});

                QueryIterator iter = dataset.queryIter(0L, 400L);
                try {
                    iter.next();
                    iter.next();

                    assertFalse(iter.hasNext());
                    assertNull(iter.next());
                } finally {
                    iter.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }
}
