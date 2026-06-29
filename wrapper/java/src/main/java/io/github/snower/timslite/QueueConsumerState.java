package io.github.snower.timslite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Durable queue consumer state returned by {@link QueueConsumer#inspect()}.
 */
public final class QueueConsumerState {
    private final long processedTs;
    private final List<QueueConsumerPendingEntry> pendingEntries;

    QueueConsumerState(io.github.snower.timslite.uniffi.QueueConsumerState kotlinState) {
        this.processedTs = kotlinState.getProcessedTs();
        List<QueueConsumerPendingEntry> entries = new ArrayList<>();
        for (io.github.snower.timslite.uniffi.QueueConsumerPendingEntry entry
                : kotlinState.getPendingEntries()) {
            entries.add(new QueueConsumerPendingEntry(entry));
        }
        this.pendingEntries = Collections.unmodifiableList(entries);
    }

    public long getProcessedTs() {
        return processedTs;
    }

    public List<QueueConsumerPendingEntry> getPendingEntries() {
        return pendingEntries;
    }
}
