package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

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
