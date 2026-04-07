#!/bin/bash
# Generate Python gRPC stubs from the proto definition.
# Run from the client/python/ directory.
set -euo pipefail

PROTO_DIR="../../proto"
OUT_DIR="src/at_runner/_generated"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    at/runner/v1/runner.proto

# Add __init__.py files to generated subdirectories
touch "$OUT_DIR/at/__init__.py"
touch "$OUT_DIR/at/runner/__init__.py"
touch "$OUT_DIR/at/runner/v1/__init__.py"

# Fix imports in generated grpc file to use package-relative paths
sed -i 's/^from at\.runner\.v1/from at_runner._generated.at.runner.v1/' \
    "$OUT_DIR/at/runner/v1/runner_pb2_grpc.py"

echo "Stubs generated in $OUT_DIR"
