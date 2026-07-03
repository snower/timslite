#!/usr/bin/env bash
# Prepare a local NuGet package for Timslite.
# Usage: bash wrapper/dotnet/scripts/prepare-publish.sh [--release]
# Prerequisites: Rust toolchain with cross-compilation targets, .NET 8 SDK.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOTNET_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$(dirname "$DOTNET_ROOT")")"

RELEASE=false
if [[ "${1:-}" == "--release" ]]; then
    RELEASE=true
fi

CONFIG="Debug"
CARGO_FLAG=""
if $RELEASE; then
    CONFIG="Release"
    CARGO_FLAG="--release"
fi

get_crate_version() {
    local manifest="$1"
    grep -m1 'version' "$manifest" | head -1 | sed 's/.*"\([^"]*\)".*/\1/'
}

get_csproj_version() {
    local csproj="$1"
    grep -oP '<Version>\K[^<]+' "$csproj"
}

build_native_lib() {
    local target="$1"
    local rid="$2"

    echo "Building native library for $rid (target: $target)..."

    local native_dir="$DOTNET_ROOT/native"
    cargo build --manifest-path "$native_dir/Cargo.toml" --target "$target" $CARGO_FLAG

    local build_subdir="debug"
    if $RELEASE; then
        build_subdir="release"
    fi

    local build_dir="$native_dir/target/$target/$build_subdir"
    local runtimes_dir="$DOTNET_ROOT/src/Timslite/runtimes/$rid/native"
    mkdir -p "$runtimes_dir"

    local lib_name
    if [[ "$rid" == win-* ]]; then
        lib_name="timslite_dotnet.dll"
    elif [[ "$rid" == osx-* ]]; then
        lib_name="libtimslite_dotnet.dylib"
    else
        lib_name="libtimslite_dotnet.so"
    fi

    local src="$build_dir/$lib_name"
    if [[ ! -f "$src" ]]; then
        echo "ERROR: Expected native library not found: $src" >&2
        exit 1
    fi

    cp "$src" "$runtimes_dir/$lib_name"
    echo "  Copied to $runtimes_dir/$lib_name"
}

echo "Checking version alignment..."
root_version=$(get_crate_version "$REPO_ROOT/Cargo.toml")
native_version=$(get_crate_version "$DOTNET_ROOT/native/Cargo.toml")
csproj_version=$(get_csproj_version "$DOTNET_ROOT/src/Timslite/Timslite.csproj")

if [[ "$root_version" != "$native_version" ]]; then
    echo "ERROR: Version mismatch!" >&2
    echo "  Root crate:    $root_version" >&2
    echo "  Native crate:  $native_version" >&2
    exit 1
fi

if [[ "$csproj_version" != "$root_version" ]]; then
    echo "ERROR: Version mismatch!" >&2
    echo "  Root crate: $root_version" >&2
    echo "  Csproj:     $csproj_version" >&2
    exit 1
fi

echo "  All versions aligned: $root_version"

targets=(
    "x86_64-pc-windows-msvc|win-x64"
    "aarch64-pc-windows-msvc|win-arm64"
    "x86_64-unknown-linux-gnu|linux-x64"
    "aarch64-unknown-linux-gnu|linux-arm64"
    "x86_64-unknown-linux-musl|linux-musl-x64"
    "aarch64-unknown-linux-musl|linux-musl-arm64"
    "aarch64-apple-darwin|osx-arm64"
)

for entry in "${targets[@]}"; do
    target="${entry%%|*}"
    rid="${entry##*|}"
    build_native_lib "$target" "$rid"
done

echo ""
echo "Packing NuGet package..."
dotnet pack "$DOTNET_ROOT/src/Timslite/Timslite.csproj" -c "$CONFIG" --no-build

echo ""
echo "Done! Package is in src/Timslite/bin/$CONFIG/"
