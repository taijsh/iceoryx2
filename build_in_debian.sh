#!/bin/bash
set -e

echo "========================================================="
echo " Building iceoryx2 for x86_64 and aarch64 in Debian WSL  "
echo "========================================================="

# 1. Install required packages
echo ""
echo "[1/5] Installing build dependencies..."
sudo apt-get update
sudo apt-get install -y build-essential cmake libclang-dev curl gcc-aarch64-linux-gnu g++-aarch64-linux-gnu python3 python3-dev

# 2. Install Rust if not present
echo ""
echo "[2/5] Checking Rust installation..."
if ! command -v cargo &> /dev/null; then
    echo "Rust/Cargo not found. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo "Rust is already installed."
fi

# 3. Add ARM64 target
echo ""
echo "[3/5] Adding aarch64 rust target..."
rustup target add aarch64-unknown-linux-gnu

# 4. Build x86_64
echo ""
echo "[4/5] Building native x86_64 version..."
cargo build --release --examples

# 5. Build ARM64
echo ""
echo "[5/5] Building ARM64 (aarch64) version..."
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
export CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++
cargo build --workspace --exclude iceoryx2-ffi-python --target aarch64-unknown-linux-gnu --release --examples

echo ""
echo "========================================================="
echo " Build successful! "
echo " x86_64 binaries are in: target/release/"
echo " aarch64 binaries are in: target/aarch64-unknown-linux-gnu/release/"
echo "========================================================="
