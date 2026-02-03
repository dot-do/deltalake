#!/bin/bash
# Download Delta Acceptance Testing (DAT) test data
# https://github.com/delta-incubator/dat

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_DATA_DIR="$PROJECT_ROOT/test-data"
DAT_DIR="$TEST_DATA_DIR/dat"

# Get latest release version from GitHub API
echo "Fetching latest DAT release version..."
LATEST_VERSION=$(curl -s https://api.github.com/repos/delta-incubator/dat/releases/latest | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$LATEST_VERSION" ]; then
    echo "Error: Could not determine latest version. Using fallback v0.0.3"
    LATEST_VERSION="v0.0.3"
fi

echo "Latest DAT version: $LATEST_VERSION"

# Construct download URL
DOWNLOAD_URL="https://github.com/delta-incubator/dat/releases/download/${LATEST_VERSION}/deltalake-dat-${LATEST_VERSION}.tar.gz"
TARBALL="$TEST_DATA_DIR/deltalake-dat-${LATEST_VERSION}.tar.gz"

# Create test-data directory if it doesn't exist
mkdir -p "$TEST_DATA_DIR"

# Check if already downloaded (either in out/ or directly in reader_tests/)
if [ -d "$DAT_DIR/reader_tests/generated" ] || [ -d "$DAT_DIR/out/reader_tests/generated" ]; then
    echo "DAT test data already exists at $DAT_DIR"
    echo "To re-download, remove the directory first: rm -rf $DAT_DIR"
    exit 0
fi

# Download the tarball
echo "Downloading DAT test data from $DOWNLOAD_URL..."
curl -L -o "$TARBALL" "$DOWNLOAD_URL"

# Extract to test-data/dat/
echo "Extracting to $DAT_DIR..."
mkdir -p "$DAT_DIR"
tar -xzf "$TARBALL" -C "$DAT_DIR" --strip-components=1

# Clean up tarball
rm "$TARBALL"

echo "DAT test data downloaded successfully!"

# Check for test cases in either location (depends on DAT release structure)
if [ -d "$DAT_DIR/reader_tests/generated" ]; then
    echo "Test cases available at: $DAT_DIR/reader_tests/generated/"
    echo ""
    echo "Available test cases:"
    ls "$DAT_DIR/reader_tests/generated/"
elif [ -d "$DAT_DIR/out/reader_tests/generated" ]; then
    echo "Test cases available at: $DAT_DIR/out/reader_tests/generated/"
    echo ""
    echo "Available test cases:"
    ls "$DAT_DIR/out/reader_tests/generated/"
fi
