package io.github.snower.timslite;

/**
 * Result of inspecting a dataset, containing both info and current state.
 *
 * <p>Returned by {@link Store#inspectDataset(String, String)}.</p>
 */
public final class InspectResult {
    private final DatasetInfo info;
    private final DatasetState state;

    InspectResult(io.github.snower.timslite.uniffi.DataSetInspectResult kotlinResult) {
        this.info = new DatasetInfo(kotlinResult.getInfo());
        this.state = new DatasetState(kotlinResult.getState());
    }

    /**
     * Returns the dataset configuration and metadata.
     *
     * @return dataset info
     */
    public DatasetInfo getInfo() {
        return info;
    }

    /**
     * Returns the current runtime state of the dataset.
     *
     * @return dataset state
     */
    public DatasetState getState() {
        return state;
    }
}
