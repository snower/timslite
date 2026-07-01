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
                    assertEquals(100L, r1.getTimestamp());
                    count++;

                    assertTrue(iter.hasNext());
                    Record r2 = iter.next();
                    assertEquals(200L, r2.getTimestamp());
                    count++;

                    assertTrue(iter.hasNext());
                    Record r3 = iter.next();
                    assertEquals(300L, r3.getTimestamp());
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
                    assertEquals(100L, e1.getTimestamp());
                    assertEquals(3, e1.getLength());
                    count++;

                    assertTrue(iter.hasNext());
                    LengthEntry e2 = iter.next();
                    assertEquals(200L, e2.getTimestamp());
                    assertEquals(2, e2.getLength());
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
    void queryIteratorReverse() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("revds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("revds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});
                dataset.write(400L, new byte[]{4});
                dataset.write(500L, new byte[]{5});

                QueryIterator iter = dataset.queryIter(0L, 600L);
                try {
                    iter.reverse();

                    Record r1 = iter.next();
                    assertNotNull(r1);
                    assertEquals(500L, r1.getTimestamp());
                    assertArrayEquals(new byte[]{5}, r1.getData());

                    Record r2 = iter.next();
                    assertNotNull(r2);
                    assertEquals(400L, r2.getTimestamp());
                    assertArrayEquals(new byte[]{4}, r2.getData());

                    Record r3 = iter.next();
                    assertNotNull(r3);
                    assertEquals(300L, r3.getTimestamp());
                    assertArrayEquals(new byte[]{3}, r3.getData());

                    Record r4 = iter.next();
                    assertNotNull(r4);
                    assertEquals(200L, r4.getTimestamp());
                    assertArrayEquals(new byte[]{2}, r4.getData());

                    Record r5 = iter.next();
                    assertNotNull(r5);
                    assertEquals(100L, r5.getTimestamp());
                    assertArrayEquals(new byte[]{1}, r5.getData());

                    assertFalse(iter.hasNext());
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

    @Test
    void queryIteratorSkip() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("skipds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("skipds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});
                dataset.write(400L, new byte[]{4});
                dataset.write(500L, new byte[]{5});

                QueryIterator iter = dataset.queryIter(0L, 600L);
                try {
                    iter.skip(2);

                    Record r1 = iter.next();
                    assertNotNull(r1);
                    assertEquals(300L, r1.getTimestamp());
                    assertArrayEquals(new byte[]{3}, r1.getData());

                    Record r2 = iter.next();
                    assertNotNull(r2);
                    assertEquals(400L, r2.getTimestamp());
                    assertArrayEquals(new byte[]{4}, r2.getData());

                    Record r3 = iter.next();
                    assertNotNull(r3);
                    assertEquals(500L, r3.getTimestamp());
                    assertArrayEquals(new byte[]{5}, r3.getData());

                    assertFalse(iter.hasNext());
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

    @Test
    void queryIteratorCollectAll() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("colallds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("colallds", "data");
            try {
                dataset.write(100L, new byte[]{10});
                dataset.write(200L, new byte[]{20});
                dataset.write(300L, new byte[]{30});
                dataset.write(400L, new byte[]{40});
                dataset.write(500L, new byte[]{50});

                QueryIterator iter = dataset.queryIter(0L, 600L);
                try {
                    List<Record> records = iter.collectAll();
                    assertEquals(5, records.size());
                    assertEquals(100L, records.get(0).getTimestamp());
                    assertArrayEquals(new byte[]{10}, records.get(0).getData());
                    assertEquals(200L, records.get(1).getTimestamp());
                    assertArrayEquals(new byte[]{20}, records.get(1).getData());
                    assertEquals(300L, records.get(2).getTimestamp());
                    assertArrayEquals(new byte[]{30}, records.get(2).getData());
                    assertEquals(400L, records.get(3).getTimestamp());
                    assertArrayEquals(new byte[]{40}, records.get(3).getData());
                    assertEquals(500L, records.get(4).getTimestamp());
                    assertArrayEquals(new byte[]{50}, records.get(4).getData());
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

    @Test
    void queryIteratorCollectTake() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("coltakds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("coltakds", "data");
            try {
                dataset.write(100L, new byte[]{10});
                dataset.write(200L, new byte[]{20});
                dataset.write(300L, new byte[]{30});
                dataset.write(400L, new byte[]{40});
                dataset.write(500L, new byte[]{50});

                QueryIterator iter = dataset.queryIter(0L, 600L);
                try {
                    List<Record> records = iter.collectTake(3);
                    assertEquals(3, records.size());
                    assertEquals(100L, records.get(0).getTimestamp());
                    assertArrayEquals(new byte[]{10}, records.get(0).getData());
                    assertEquals(200L, records.get(1).getTimestamp());
                    assertArrayEquals(new byte[]{20}, records.get(1).getData());
                    assertEquals(300L, records.get(2).getTimestamp());
                    assertArrayEquals(new byte[]{30}, records.get(2).getData());
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

    @Test
    void queryIteratorSkipAndReverse() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("skiprevds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("skiprevds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});
                dataset.write(400L, new byte[]{4});
                dataset.write(500L, new byte[]{5});

                QueryIterator iter = dataset.queryIter(0L, 600L);
                try {
                    Record r1 = iter.next();
                    assertNotNull(r1);
                    assertEquals(100L, r1.getTimestamp());

                    Record r2 = iter.next();
                    assertNotNull(r2);
                    assertEquals(200L, r2.getTimestamp());

                    iter.reverse();

                    Record r3 = iter.next();
                    assertNotNull(r3);
                    assertEquals(500L, r3.getTimestamp());
                    assertArrayEquals(new byte[]{5}, r3.getData());

                    Record r4 = iter.next();
                    assertNotNull(r4);
                    assertEquals(400L, r4.getTimestamp());
                    assertArrayEquals(new byte[]{4}, r4.getData());

                    Record r5 = iter.next();
                    assertNotNull(r5);
                    assertEquals(300L, r5.getTimestamp());
                    assertArrayEquals(new byte[]{3}, r5.getData());

                    assertFalse(iter.hasNext());
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

    @Test
    void queryIteratorSkipMoreThanAvailable() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("skipmoreds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("skipmoreds", "data");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                QueryIterator iter = dataset.queryIter(0L, 400L);
                try {
                    iter.skip(10);

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

    @Test
    void queryLengthIteratorReverse() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("lenrevds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lenrevds", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});
                dataset.write(300L, new byte[]{6, 7, 8, 9});
                dataset.write(400L, new byte[]{10});
                dataset.write(500L, new byte[]{11, 12});

                QueryLengthIterator iter = dataset.queryLengthIter(0L, 600L);
                try {
                    iter.reverse();

                    LengthEntry e1 = iter.next();
                    assertNotNull(e1);
                    assertEquals(500L, e1.getTimestamp());
                    assertEquals(2, e1.getLength());

                    LengthEntry e2 = iter.next();
                    assertNotNull(e2);
                    assertEquals(400L, e2.getTimestamp());
                    assertEquals(1, e2.getLength());

                    LengthEntry e3 = iter.next();
                    assertNotNull(e3);
                    assertEquals(300L, e3.getTimestamp());
                    assertEquals(4, e3.getLength());

                    LengthEntry e4 = iter.next();
                    assertNotNull(e4);
                    assertEquals(200L, e4.getTimestamp());
                    assertEquals(2, e4.getLength());

                    LengthEntry e5 = iter.next();
                    assertNotNull(e5);
                    assertEquals(100L, e5.getTimestamp());
                    assertEquals(3, e5.getLength());

                    assertFalse(iter.hasNext());
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

    @Test
    void queryLengthIteratorSkip() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("lenskipds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lenskipds", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});
                dataset.write(300L, new byte[]{6, 7, 8, 9});
                dataset.write(400L, new byte[]{10});
                dataset.write(500L, new byte[]{11, 12});

                QueryLengthIterator iter = dataset.queryLengthIter(0L, 600L);
                try {
                    iter.skip(2);

                    LengthEntry e1 = iter.next();
                    assertNotNull(e1);
                    assertEquals(300L, e1.getTimestamp());
                    assertEquals(4, e1.getLength());

                    LengthEntry e2 = iter.next();
                    assertNotNull(e2);
                    assertEquals(400L, e2.getTimestamp());
                    assertEquals(1, e2.getLength());

                    LengthEntry e3 = iter.next();
                    assertNotNull(e3);
                    assertEquals(500L, e3.getTimestamp());
                    assertEquals(2, e3.getLength());

                    assertFalse(iter.hasNext());
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

    @Test
    void queryLengthIteratorCollectAll() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("lencolallds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lencolallds", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});
                dataset.write(300L, new byte[]{6, 7, 8, 9});
                dataset.write(400L, new byte[]{10});
                dataset.write(500L, new byte[]{11, 12});

                QueryLengthIterator iter = dataset.queryLengthIter(0L, 600L);
                try {
                    List<LengthEntry> entries = iter.collectAll();
                    assertEquals(5, entries.size());
                    assertEquals(100L, entries.get(0).getTimestamp());
                    assertEquals(3, entries.get(0).getLength());
                    assertEquals(200L, entries.get(1).getTimestamp());
                    assertEquals(2, entries.get(1).getLength());
                    assertEquals(300L, entries.get(2).getTimestamp());
                    assertEquals(4, entries.get(2).getLength());
                    assertEquals(400L, entries.get(3).getTimestamp());
                    assertEquals(1, entries.get(3).getLength());
                    assertEquals(500L, entries.get(4).getTimestamp());
                    assertEquals(2, entries.get(4).getLength());
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

    @Test
    void queryLengthIteratorCollectTake() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("lencoltakds", "data",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("lencoltakds", "data");
            try {
                dataset.write(100L, new byte[]{1, 2, 3});
                dataset.write(200L, new byte[]{4, 5});
                dataset.write(300L, new byte[]{6, 7, 8, 9});
                dataset.write(400L, new byte[]{10});
                dataset.write(500L, new byte[]{11, 12});

                QueryLengthIterator iter = dataset.queryLengthIter(0L, 600L);
                try {
                    List<LengthEntry> entries = iter.collectTake(3);
                    assertEquals(3, entries.size());
                    assertEquals(100L, entries.get(0).getTimestamp());
                    assertEquals(3, entries.get(0).getLength());
                    assertEquals(200L, entries.get(1).getTimestamp());
                    assertEquals(2, entries.get(1).getLength());
                    assertEquals(300L, entries.get(2).getTimestamp());
                    assertEquals(4, entries.get(2).getLength());
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
