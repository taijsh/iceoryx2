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

# 6. Build C/C++ examples (x86_64)
echo ""
echo "[6/7] Building native C/C++ examples (x86_64)..."
cmake -S . -B target/x86_64/cc_build -DBUILD_EXAMPLES=ON -DBUILD_CXX=ON
cmake --build target/x86_64/cc_build

# 7. Build C/C++ examples (aarch64)
echo ""
echo "[7/7] Building ARM64 (aarch64) C/C++ examples..."
mkdir -p target/aarch64
cat << 'EOF' > target/aarch64/aarch64_toolchain.cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
EOF

cmake -S . -B target/aarch64/cc_build \
    -DCMAKE_TOOLCHAIN_FILE=target/aarch64/aarch64_toolchain.cmake \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_CXX= \
    -DRUST_TARGET_TRIPLET=aarch64-unknown-linux-gnu \
    -DRUST_BUILD_ARTIFACT_PATH="$PWD/target/aarch64-unknown-linux-gnu/release"

cmake --build target/aarch64/cc_build

echo ""
echo "========================================================="
echo " C/C++ Build successful! "
echo " x86_64 C/C++ examples are in: target/x86_64/cc_build/examples/"
echo " aarch64 C/C++ examples are in: target/aarch64/cc_build/examples/"
echo "========================================================="
