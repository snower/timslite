#!/bin/bash

# Build timslite library
echo "Building timslite..."
cargo build --release

# Build with C FFI support
echo "Building with C FFI..."
cargo build --release --features ffi

# Run tests
echo "Running tests..."
cargo test

# Generate documentation
echo "Generating documentation..."
cargo doc --no-deps --open

# Run examples
echo "Running basic example..."
cargo run --example basic

echo "Running config example..."
cargo run --example config

echo "Running performance example..."
cargo run --example performance

echo "Build complete!"