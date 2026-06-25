package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.CreateDatasetOptions;
import io.github.snower.timslite.uniffi.DatasetConfig;

/**
 * Builder for {@link CreateDatasetOptions}.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
 *         .config(DatasetConfigBuilder.builder().build())
 *         .build();
 * store.createDataset("metrics", "cpu", options);
 * }</pre>
 */
public final class CreateDatasetOptionsBuilder {
    private DatasetConfig config;

    private CreateDatasetOptionsBuilder() {
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static CreateDatasetOptionsBuilder builder() {
        return new CreateDatasetOptionsBuilder();
    }

    /**
     * Sets the dataset configuration to use when creating the dataset.
     *
     * @param config dataset configuration
     * @return this builder
     */
    public CreateDatasetOptionsBuilder config(DatasetConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Builds the {@link CreateDatasetOptions}.
     *
     * @return the built options
     */
    public CreateDatasetOptions build() {
        return UniFFIBridge.buildCreateDatasetOptions(config);
    }
}
