@echo off
REM Build script for Windows

echo Building timslite...
cargo build --release

echo Building with C FFI...
cargo build --release --features ffi

echo Running tests...
cargo test

echo Generating documentation...
cargo doc --no-deps --open

echo Running basic example...
cargo run --example basic

echo Running config example...
cargo run --example config

echo Running performance example...
cargo run --example performance

echo Build complete!
pause