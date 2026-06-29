package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.nio.file.Path;
import java.util.List;

import io.github.snower.timslite.errors.TmslException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueueTest {

    @TempDir
    Path tempDir;

    @Test
    void pushAndPoll() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    try {
                        byte[] payload = new byte[]{10, 20, 30};
                        long ts = queue.push(payload);

                        Record record = consumer.poll(5000L);
                        assertNotNull(record);
                        assertEquals(ts, record.getTimestamp());
                        assertArrayEquals(payload, record.getData());
                    } finally {
                        consumer.close();
                    }
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void ackPreventsRedelivery() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    try {
                        queue.push(new byte[]{1, 2, 3});

                        Record record = consumer.poll(5000L);
                        assertNotNull(record);
                        consumer.ack(record.getTimestamp());

                        Record record2 = consumer.poll(100L);
                        assertNull(record2);
                    } finally {
                        consumer.close();
                    }
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void timeoutReturnsNull() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    try {
                        Record record = consumer.poll(100L);
                        assertNull(record);
                    } finally {
                        consumer.close();
                    }
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void openConsumerWithDefaultOptions() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    assertNotNull(consumer);
                    consumer.close();
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void dropConsumer() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    consumer.close();
                    queue.dropConsumer("group1");
                    // dropping after close is fine as group still exists
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void groupNamesInspectFlushAndCloseReleasePending() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("shared");
                    QueueConsumer alias = queue.openConsumer("shared");
                    queue.openConsumer("other");

                    List<String> names = queue.getConsumerGroupNames();
                    assertEquals(Arrays.asList("other", "shared"), names);

                    queue.push(new byte[]{1, 2, 3});
                    Record record = consumer.poll(5000L);
                    assertNotNull(record);
                    assertEquals(1L, record.getTimestamp());
                    consumer.flush();

                    QueueConsumerInspectResult inspected = consumer.inspect();
                    assertEquals("shared", inspected.getInfo().getGroupName());
                    assertEquals(900L, inspected.getInfo().getRunningExpiredSeconds());
                    assertEquals(3, inspected.getInfo().getMaxRetryCount());
                    assertEquals(Long.MIN_VALUE, inspected.getState().getProcessedTs());
                    assertEquals(1L, inspected.getState().getPendingEntries().get(0).getTimestamp());

                    consumer.close();
                    assertThrows(TmslException.class, () -> alias.poll(100L));

                    QueueConsumer reopened = queue.openConsumer("shared");
                    Record redelivered = reopened.poll(5000L);
                    assertNotNull(redelivered);
                    assertEquals(1L, redelivered.getTimestamp());
                    reopened.ack(redelivered.getTimestamp());
                    reopened.close();
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void closedQueueThrowsOnPush() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                queue.close();
                assertTrue(queue.isClosed());
                assertThrows(IllegalStateException.class, () -> queue.push(new byte[]{1}));
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void closedQueueThrowsOnOpenConsumer() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                queue.close();
                assertThrows(IllegalStateException.class, () -> queue.openConsumer("group1"));
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void closedConsumerThrowsOnPoll() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    consumer.close();
                    assertTrue(consumer.isClosed());
                    assertThrows(IllegalStateException.class, () -> consumer.poll(100L));
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }

    @Test
    void closedConsumerThrowsOnAck() {
        Store store = Store.open(tempDir.toString());
        try {
            store.createDataset("qds", "events",
                    CreateDatasetOptionsBuilder.builder().build());
            Dataset dataset = store.openDataset("qds", "events");
            try {
                Queue queue = store.openQueue(dataset);
                try {
                    QueueConsumer consumer = queue.openConsumer("group1");
                    consumer.close();
                    assertThrows(IllegalStateException.class, () -> consumer.ack(100L));
                } finally {
                    queue.close();
                }
            } finally {
                dataset.close();
            }
        } finally {
            store.close();
        }
    }
}
