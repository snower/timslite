package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.snower.timslite.uniffi.CreateDatasetOptions;
import io.github.snower.timslite.uniffi.DatasetConfig;
import io.github.snower.timslite.uniffi.QueueConsumerConfig;
import io.github.snower.timslite.uniffi.QueueConsumerOptions;
import io.github.snower.timslite.uniffi.StoreConfig;

import org.junit.jupiter.api.Test;

class ConfigTest {

    @Test
    void defaultStoreConfigProducesNonNullResult() {
        StoreConfig config = StoreConfigBuilder.builder().build();
        assertNotNull(config);
    }

    @Test
    void customStoreConfigValuesArePreserved() {
        assertDoesNotThrow(() -> {
            StoreConfig config = StoreConfigBuilder.builder()
                    .flushIntervalSecs(60)
                    .idleTimeoutSecs(300)
                    .dataSegmentSize(65536)
                    .indexSegmentSize(32768)
                    .compressLevel((byte) 3)
                    .cacheMaxMemory(1048576)
                    .enableBackgroundThread(true)
                    .enableJournal(false)
                    .build();
            assertNotNull(config);
        });
    }

    @Test
    void negativeDataSegmentSizeThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            StoreConfigBuilder.builder().dataSegmentSize(-1).build();
        });
    }

    @Test
    void negativeIndexSegmentSizeThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            StoreConfigBuilder.builder().indexSegmentSize(-1).build();
        });
    }

    @Test
    void defaultDatasetConfigProducesNonNullResult() {
        DatasetConfig config = DatasetConfigBuilder.builder().build();
        assertNotNull(config);
    }

    @Test
    void defaultCreateDatasetOptionsProducesNonNullResult() {
        CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder().build();
        assertNotNull(options);
    }

    @Test
    void createDatasetOptionsWithConfig() {
        DatasetConfig datasetConfig = DatasetConfigBuilder.builder()
                .dataSegmentSize(65536)
                .compressLevel((byte) 3)
                .build();
        CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
                .config(datasetConfig)
                .build();
        assertNotNull(options);
    }

    @Test
    void defaultQueueConsumerConfigProducesNonNullResult() {
        QueueConsumerConfig config = QueueConsumerConfigBuilder.builder().build();
        assertNotNull(config);
    }

    @Test
    void defaultQueueConsumerOptionsProducesNonNullResult() {
        QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder().build();
        assertNotNull(options);
    }

    @Test
    void queueConsumerOptionsWithConfig() {
        QueueConsumerConfig consumerConfig = QueueConsumerConfigBuilder.builder()
                .runningExpiredSeconds(60)
                .maxRetryCount((short) 3)
                .build();
        QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
                .config(consumerConfig)
                .build();
        assertNotNull(options);
    }

    @Test
    void negativeCacheMaxMemoryThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            StoreConfigBuilder.builder().cacheMaxMemory(-1).build();
        });
    }
}
