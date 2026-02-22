#!/bin/bash
set -e

echo "========================================================="
echo " Building iceoryx2 for aarch64 in Debian-gcc730 WSL  "
echo "========================================================="

# 1. Install required packages
echo ""
echo "[1/5] Installing build dependencies..."
sudo apt-get update
sudo apt-get install -y build-essential cmake libclang-dev curl python3 python3-dev xz-utils

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

# 4. Install Linaro GCC 7.3.1
echo ""
echo "[4/5] Installing Linaro GCC 7.3.1 toolchain..."
if [ ! -d "/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu" ]; then
    curl -sSL https://releases.linaro.org/components/toolchain/binaries/7.3-2018.05/aarch64-linux-gnu/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu.tar.xz -o /tmp/gcc-linaro.tar.xz
    sudo tar -xf /tmp/gcc-linaro.tar.xz -C /opt/
    rm /tmp/gcc-linaro.tar.xz
else
    echo "GCC 7.3.1 is already installed in /opt/."
fi
export PATH=/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin:$PATH

# 5. Build ARM64
echo ""
echo "[5/5] Building ARM64 (aarch64) version..."
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-gcc
export CC_aarch64_unknown_linux_gnu=/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-gcc
export CXX_aarch64_unknown_linux_gnu=/opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-g++

# Rust build
export CARGO_TARGET_DIR=/tmp/target_gcc730
cargo build --workspace --exclude iceoryx2-ffi-python --target aarch64-unknown-linux-gnu --release --lib --bins --examples

# C examples build
echo ""
echo "Building ARM64 (aarch64) C examples..."
mkdir -p target_gcc730/aarch64
cat << 'EOF' > target_gcc730/aarch64/aarch64_toolchain.cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_C_COMPILER /opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER /opt/gcc-linaro-7.3.1-2018.05-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-g++)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
EOF

cmake -S . -B target_gcc730/aarch64/cc_build \
    -DCMAKE_TOOLCHAIN_FILE=target_gcc730/aarch64/aarch64_toolchain.cmake \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_CXX=OFF \
    -DRUST_TARGET_TRIPLET=aarch64-unknown-linux-gnu \
    -DRUST_BUILD_ARTIFACT_PATH="/tmp/target_gcc730/aarch64-unknown-linux-gnu/release"

cmake --build target_gcc730/aarch64/cc_build

# Copy the rust artifacts back to windows mount for the user
echo "Copying built artifacts back to windows mount..."
cp -R /tmp/target_gcc730/aarch64-unknown-linux-gnu target_gcc730/aarch64-unknown-linux-gnu

echo ""
echo "========================================================="
echo " Build successful! "
echo " aarch64 binaries are in: target_gcc730/aarch64-unknown-linux-gnu/release/"
echo " aarch64 C examples are in: target_gcc730/aarch64/cc_build/examples/"
echo "========================================================="
