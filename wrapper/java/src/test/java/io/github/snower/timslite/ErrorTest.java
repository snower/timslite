package io.github.snower.timslite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.snower.timslite.errors.AlreadyExistsException;
import io.github.snower.timslite.errors.CompressionException;
import io.github.snower.timslite.errors.ConsumerGroupExistsException;
import io.github.snower.timslite.errors.ConsumerGroupNotFoundException;
import io.github.snower.timslite.errors.DatasetClosedException;
import io.github.snower.timslite.errors.DecompressionException;
import io.github.snower.timslite.errors.ExpiredException;
import io.github.snower.timslite.errors.InvalidDataException;
import io.github.snower.timslite.errors.InvalidMagicException;
import io.github.snower.timslite.errors.InvalidVersionException;
import io.github.snower.timslite.errors.IoException;
import io.github.snower.timslite.errors.IteratorExhaustedException;
import io.github.snower.timslite.errors.MmapException;
import io.github.snower.timslite.errors.NotFoundException;
import io.github.snower.timslite.errors.PendingFullException;
import io.github.snower.timslite.errors.QueueAlreadyOpenException;
import io.github.snower.timslite.errors.QueueBridgeClosedException;
import io.github.snower.timslite.errors.QueueClosedException;
import io.github.snower.timslite.errors.QueueNotOpenException;
import io.github.snower.timslite.errors.SegmentFullException;
import io.github.snower.timslite.errors.StoreClosedException;
import io.github.snower.timslite.errors.TmslErrorCode;
import io.github.snower.timslite.errors.TmslException;

import org.junit.jupiter.api.Test;

class ErrorTest {

    @Test
    void ioExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.Io kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.Io("io error");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof IoException);
        assertEquals(TmslErrorCode.IO, ex.code());
        assertEquals("io error", ex.getMessage());
    }

    @Test
    void invalidMagicExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.InvalidMagic kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.InvalidMagic("bad magic");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof InvalidMagicException);
        assertEquals(TmslErrorCode.INVALID_MAGIC, ex.code());
    }

    @Test
    void invalidVersionExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.InvalidVersion kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.InvalidVersion("bad version");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof InvalidVersionException);
        assertEquals(TmslErrorCode.INVALID_VERSION, ex.code());
    }

    @Test
    void mmapExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.MmapException kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.MmapException("mmap error");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof MmapException);
        assertEquals(TmslErrorCode.MMAP, ex.code());
    }

    @Test
    void compressionExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.CompressionException kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.CompressionException("compress error");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof CompressionException);
        assertEquals(TmslErrorCode.COMPRESSION, ex.code());
    }

    @Test
    void decompressionExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.DecompressionException kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.DecompressionException("decompress error");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof DecompressionException);
        assertEquals(TmslErrorCode.DECOMPRESSION, ex.code());
    }

    @Test
    void invalidDataExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.InvalidData kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.InvalidData("bad data");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof InvalidDataException);
        assertEquals(TmslErrorCode.INVALID_DATA, ex.code());
    }

    @Test
    void notFoundExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.NotFound kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.NotFound("not found");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof NotFoundException);
        assertEquals(TmslErrorCode.NOT_FOUND, ex.code());
    }

    @Test
    void expiredExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.Expired kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.Expired("expired");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof ExpiredException);
        assertEquals(TmslErrorCode.EXPIRED, ex.code());
    }

    @Test
    void alreadyExistsExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.AlreadyExists kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.AlreadyExists("already exists");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof AlreadyExistsException);
        assertEquals(TmslErrorCode.ALREADY_EXISTS, ex.code());
    }

    @Test
    void segmentFullExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.SegmentFull kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.SegmentFull("segment full");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof SegmentFullException);
        assertEquals(TmslErrorCode.SEGMENT_FULL, ex.code());
    }

    @Test
    void queueAlreadyOpenExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.QueueAlreadyOpen kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.QueueAlreadyOpen("queue open");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof QueueAlreadyOpenException);
        assertEquals(TmslErrorCode.QUEUE_ALREADY_OPEN, ex.code());
    }

    @Test
    void queueNotOpenExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.QueueNotOpen kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.QueueNotOpen("queue not open");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof QueueNotOpenException);
        assertEquals(TmslErrorCode.QUEUE_NOT_OPEN, ex.code());
    }

    @Test
    void consumerGroupNotFoundExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.ConsumerGroupNotFound kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.ConsumerGroupNotFound("group not found");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof ConsumerGroupNotFoundException);
        assertEquals(TmslErrorCode.CONSUMER_GROUP_NOT_FOUND, ex.code());
    }

    @Test
    void consumerGroupExistsExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.ConsumerGroupExists kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.ConsumerGroupExists("group exists");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof ConsumerGroupExistsException);
        assertEquals(TmslErrorCode.CONSUMER_GROUP_EXISTS, ex.code());
    }

    @Test
    void queueClosedExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.QueueClosed kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.QueueClosed("queue closed");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof QueueClosedException);
        assertEquals(TmslErrorCode.QUEUE_CLOSED, ex.code());
    }

    @Test
    void pendingFullExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.PendingFull kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.PendingFull("pending full");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof PendingFullException);
        assertEquals(TmslErrorCode.PENDING_FULL, ex.code());
    }

    @Test
    void storeClosedExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.StoreClosed kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.StoreClosed("store closed");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof StoreClosedException);
        assertEquals(TmslErrorCode.STORE_CLOSED, ex.code());
    }

    @Test
    void datasetClosedExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.DatasetClosed kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.DatasetClosed("dataset closed");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof DatasetClosedException);
        assertEquals(TmslErrorCode.DATASET_CLOSED, ex.code());
    }

    @Test
    void queueBridgeClosedExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.QueueBridgeClosed kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.QueueBridgeClosed("bridge closed");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof QueueBridgeClosedException);
        assertEquals(TmslErrorCode.QUEUE_BRIDGE_CLOSED, ex.code());
    }

    @Test
    void iteratorExhaustedExceptionMapping() {
        io.github.snower.timslite.uniffi.TmslException.IteratorExhausted kotlinEx =
                new io.github.snower.timslite.uniffi.TmslException.IteratorExhausted("iterator exhausted");
        TmslException ex = TmslException.fromUniFFI(kotlinEx);
        assertTrue(ex instanceof IteratorExhaustedException);
        assertEquals(TmslErrorCode.ITERATOR_EXHAUSTED, ex.code());
    }

    @Test
    void allExceptionSubclassesExtendTmslException() {
        assertNotNull(new IoException("msg"));
        assertNotNull(new InvalidMagicException("msg"));
        assertNotNull(new InvalidVersionException("msg"));
        assertNotNull(new MmapException("msg"));
        assertNotNull(new CompressionException("msg"));
        assertNotNull(new DecompressionException("msg"));
        assertNotNull(new InvalidDataException("msg"));
        assertNotNull(new NotFoundException("msg"));
        assertNotNull(new ExpiredException("msg"));
        assertNotNull(new AlreadyExistsException("msg"));
        assertNotNull(new SegmentFullException("msg"));
        assertNotNull(new QueueAlreadyOpenException("msg"));
        assertNotNull(new QueueNotOpenException("msg"));
        assertNotNull(new ConsumerGroupNotFoundException("msg"));
        assertNotNull(new ConsumerGroupExistsException("msg"));
        assertNotNull(new QueueClosedException("msg"));
        assertNotNull(new PendingFullException("msg"));
        assertNotNull(new StoreClosedException("msg"));
        assertNotNull(new DatasetClosedException("msg"));
        assertNotNull(new QueueBridgeClosedException("msg"));
        assertNotNull(new IteratorExhaustedException("msg"));
    }

    @Test
    void tmslExceptionHasCodeAndMessage() {
        TmslException ex = new TmslException("test", TmslErrorCode.STORE_CLOSED);
        assertEquals(TmslErrorCode.STORE_CLOSED, ex.code());
        assertEquals("test", ex.getMessage());
    }
}
