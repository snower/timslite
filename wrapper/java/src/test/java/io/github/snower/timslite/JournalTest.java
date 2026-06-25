package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JournalTest {

    @TempDir
    Path tempDir;

    @Test
    void journalWriteCreatesSequence() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                dataset.write(100L, new byte[]{10, 20});

                Long seq = store.journalLatestSequence();
                assertNotNull(seq);
                assertTrue(seq > 0);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalRead() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                byte[] payload = new byte[]{1, 2, 3};
                dataset.write(100L, payload);

                Long seq = store.journalLatestSequence();
                assertNotNull(seq);

                JournalRecord jr = store.journalRead(seq);
                assertNotNull(jr);
                assertEquals(seq.longValue(), jr.getSequence());
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalQuery() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                dataset.write(100L, new byte[]{1});
                dataset.write(200L, new byte[]{2});
                dataset.write(300L, new byte[]{3});

                Long latestSeq = store.journalLatestSequence();
                assertNotNull(latestSeq);

                List<JournalRecord> records = store.journalQuery(1L, latestSeq);
                assertNotNull(records);
                assertTrue(records.size() >= 3);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalQueuePushAndPoll() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                JournalQueue jq = store.openJournalQueue();
                try {
                    JournalQueueConsumer consumer = jq.openConsumer("group1");
                    try {
                        dataset.write(100L, new byte[]{5, 6, 7});

                        JournalRecord jr = consumer.poll(5000L);
                        assertNotNull(jr);
                        assertTrue(jr.getSequence() > 0);
                    } finally {
                        consumer.close();
                    }
                } finally {
                    jq.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalQueueAck() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                JournalQueue jq = store.openJournalQueue();
                try {
                    JournalQueueConsumer consumer = jq.openConsumer("group1");
                    try {
                        dataset.write(100L, new byte[]{8, 9});

                        JournalRecord jr = consumer.poll(5000L);
                        assertNotNull(jr);
                        consumer.ack(jr.getSequence());

                        JournalRecord jr2 = consumer.poll(100L);
                        assertNull(jr2);
                    } finally {
                        consumer.close();
                    }
                } finally {
                    jq.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalQueueTimeoutReturnsNull() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                JournalQueue jq = store.openJournalQueue();
                try {
                    JournalQueueConsumer consumer = jq.openConsumer("group1");
                    try {
                        JournalRecord jr = consumer.poll(100L);
                        assertNull(jr);
                    } finally {
                        consumer.close();
                    }
                } finally {
                    jq.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void journalReadNonExistentReturnsNull() {
        io.github.snower.timslite.uniffi.StoreConfig config =
                StoreConfigBuilder.builder().enableJournal(true).build();
        Store store = Store.open(tempDir.toString(), config);
        try {
            store.createDataset("jds", "logs",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("jds", "logs");
            try {
                JournalRecord jr = store.journalRead(999999L);
                assertNull(jr);
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }
}
