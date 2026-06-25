package io.github.snower.timslite;

/**
 * Result of a background task tick, reporting executed tasks and next delay.
 *
 * <p>Returned by {@link Store#tickBackgroundTasks()}.</p>
 */
public final class TickResult {
    private final long executedTasks;
    private final long nextDelayMs;

    TickResult(io.github.snower.timslite.uniffi.TickResult kotlinResult) {
        this.executedTasks = KotlinConversions.getULong(kotlinResult, "getExecutedTasks");
        this.nextDelayMs = KotlinConversions.getULong(kotlinResult, "getNextDelayMs");
    }

    /**
     * Returns the number of background tasks executed in this tick.
     *
     * @return executed task count
     */
    public long getExecutedTasks() {
        return executedTasks;
    }

    /**
     * Returns the recommended delay in milliseconds before the next tick.
     *
     * @return next delay in milliseconds
     */
    public long getNextDelayMs() {
        return nextDelayMs;
    }
}
