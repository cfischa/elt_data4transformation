#!/bin/bash
set -e

# Install additional requirements if they exist
if [ -f "/requirements.txt" ]; then
    echo "Installing additional requirements..."
    pip install -r /requirements.txt
fi

# Execute the original entrypoint with all arguments
exec /entrypoint "$@"
