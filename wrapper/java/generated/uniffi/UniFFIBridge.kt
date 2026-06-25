package io.github.snower.timslite

import io.github.snower.timslite.uniffi.StoreConfig
import io.github.snower.timslite.uniffi.DatasetConfig
import io.github.snower.timslite.uniffi.CreateDatasetOptions
import io.github.snower.timslite.uniffi.QueueConsumerConfig
import io.github.snower.timslite.uniffi.QueueConsumerOptions

object UniFFIBridge {
    @JvmStatic
    fun buildStoreConfig(
        flushIntervalSecs: java.lang.Long?,
        idleTimeoutSecs: java.lang.Long?,
        dataSegmentSize: java.lang.Long?,
        indexSegmentSize: java.lang.Long?,
        initialDataSegmentSize: java.lang.Long?,
        initialIndexSegmentSize: java.lang.Long?,
        compressLevel: java.lang.Byte?,
        cacheMaxMemory: java.lang.Long?,
        cacheIdleTimeoutSecs: java.lang.Long?,
        retentionCheckHour: java.lang.Byte?,
        enableBackgroundThread: java.lang.Boolean?,
        enableJournal: java.lang.Boolean?,
        readOnly: java.lang.Boolean?
    ): StoreConfig = StoreConfig(
        flushIntervalSecs?.toLong()?.toULong(),
        idleTimeoutSecs?.toLong()?.toULong(),
        dataSegmentSize?.toLong()?.toULong(),
        indexSegmentSize?.toLong()?.toULong(),
        initialDataSegmentSize?.toLong()?.toULong(),
        initialIndexSegmentSize?.toLong()?.toULong(),
        compressLevel?.toByte()?.toUByte(),
        cacheMaxMemory?.toLong()?.toULong(),
        cacheIdleTimeoutSecs?.toLong()?.toULong(),
        retentionCheckHour?.toByte()?.toUByte(),
        enableBackgroundThread?.let { it.booleanValue() },
        enableJournal?.let { it.booleanValue() },
        readOnly?.let { it.booleanValue() }
    )

    @JvmStatic
    fun buildDatasetConfig(
        dataSegmentSize: java.lang.Long?,
        indexSegmentSize: java.lang.Long?,
        initialDataSegmentSize: java.lang.Long?,
        initialIndexSegmentSize: java.lang.Long?,
        compressLevel: java.lang.Byte?,
        compressType: java.lang.Byte?,
        indexContinuous: java.lang.Byte?,
        retentionWindow: java.lang.Long?,
        enableJournal: java.lang.Boolean?
    ): DatasetConfig = DatasetConfig(
        dataSegmentSize?.toLong()?.toULong(),
        indexSegmentSize?.toLong()?.toULong(),
        initialDataSegmentSize?.toLong()?.toULong(),
        initialIndexSegmentSize?.toLong()?.toULong(),
        compressLevel?.toByte()?.toUByte(),
        compressType?.toByte()?.toUByte(),
        indexContinuous?.toByte()?.toUByte(),
        retentionWindow?.toLong()?.toULong(),
        enableJournal?.let { it.booleanValue() }
    )

    @JvmStatic
    fun buildCreateDatasetOptions(config: DatasetConfig?): CreateDatasetOptions =
        CreateDatasetOptions(config)

    @JvmStatic
    fun buildQueueConsumerConfig(
        runningExpiredSeconds: java.lang.Long?,
        maxRetryCount: java.lang.Short?
    ): QueueConsumerConfig = QueueConsumerConfig(
        runningExpiredSeconds?.toLong()?.toULong(),
        maxRetryCount?.toShort()?.toUShort()
    )

    @JvmStatic
    fun buildQueueConsumerOptions(config: QueueConsumerConfig?): QueueConsumerOptions =
        QueueConsumerOptions(config)
}
