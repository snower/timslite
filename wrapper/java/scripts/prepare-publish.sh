#!/usr/bin/env bash
#
# prepare-publish.sh — prepare the timslite Java wrapper for Maven Central publication.
#
# Usage: ./scripts/prepare-publish.sh
#
# This script:
#   1. Checks that the version in pom.xml matches Cargo.toml.
#   2. Rewrites the Cargo.toml path dependency to the crates.io version.
#   3. Runs `mvn clean verify` to validate the publication artifacts.
#
# After running this script, the Cargo.toml is modified for release.
# Commit the change before publishing.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

POM_FILE="${PROJECT_DIR}/pom.xml"
CARGO_FILE="${PROJECT_DIR}/native/Cargo.toml"
CARGO_BAK="${CARGO_FILE}.publish-bak"

# ---- 1. Check version alignment ----
echo "==> Checking version alignment..."

POM_VERSION=$(grep -m1 '<version>' "${POM_FILE}" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
CARGO_VERSION=$(grep -m1 '^version' "${CARGO_FILE}" | sed 's/version *= *"\(.*\)"/\1/')

if [ -z "${POM_VERSION}" ]; then
    echo "ERROR: Could not extract version from ${POM_FILE}"
    exit 1
fi

if [ -z "${CARGO_VERSION}" ]; then
    echo "ERROR: Could not extract version from ${CARGO_FILE}"
    exit 1
fi

if [ "${POM_VERSION}" != "${CARGO_VERSION}" ]; then
    echo "ERROR: Version mismatch!"
    echo "  pom.xml:    ${POM_VERSION}"
    echo "  Cargo.toml: ${CARGO_VERSION}"
    exit 1
fi

echo "  pom.xml version:    ${POM_VERSION}"
echo "  Cargo.toml version: ${CARGO_VERSION}"
echo "  Versions match."

# ---- 2. Rewrite Cargo.toml dependency ----
echo ""
echo "==> Rewriting Cargo.toml dependency from path to crates.io..."

if grep -q 'timslite = { path = "\.\./\.\./\.\."' "${CARGO_FILE}"; then
    cp "${CARGO_FILE}" "${CARGO_BAK}"
    echo "  Backed up to ${CARGO_BAK}"

    sed -i.bak 's/timslite = { path = "\.\.\/\.\.\/\.\.", version = "=\([^"]*\)" }/timslite = "=\1"/' "${CARGO_FILE}"
    rm -f "${CARGO_FILE}.bak"

    echo "  Updated timslite dependency in ${CARGO_FILE}"
else
    echo "  Path dependency not found — may already be using crates.io version."
fi

# ---- 3. Verify ----
echo ""
echo "==> Running mvn clean verify..."

mvn -f "${POM_FILE}" clean verify

echo ""
echo "==> Done."
echo "Cargo.toml has been modified for crates.io release."
echo "If needed, restore from backup: cp ${CARGO_BAK} ${CARGO_FILE}"
echo "Then commit changes and publish with: mvn deploy"
