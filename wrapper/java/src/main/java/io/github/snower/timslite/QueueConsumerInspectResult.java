package io.github.snower.timslite;

/**
 * Result of inspecting a queue consumer group.
 */
public final class QueueConsumerInspectResult {
    private final QueueConsumerInfo info;
    private final QueueConsumerState state;

    QueueConsumerInspectResult(
            io.github.snower.timslite.uniffi.QueueConsumerInspectResult kotlinResult) {
        this.info = new QueueConsumerInfo(kotlinResult.getInfo());
        this.state = new QueueConsumerState(kotlinResult.getState());
    }

    public QueueConsumerInfo getInfo() {
        return info;
    }

    public QueueConsumerState getState() {
        return state;
    }
}
