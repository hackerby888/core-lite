#!/bin/bash
set -e

# Determine epoch: use ENV variable, or fall back to compiled epoch
if [ -z "$EPOCH" ]; then
    EPOCH=$(cat /qubic/epoch.txt)
fi

echo "=== Qubic Core Lite Mainnet Node ==="
echo "Epoch: $EPOCH"

EPOCH_URL="https://storage.qubic.li/network/${EPOCH}/ep${EPOCH}-full.zip"
EPOCH_ZIP="/qubic/epoch_files.zip"

# Function to download epoch files
download_epoch_files() {
    echo "Downloading epoch files from: $EPOCH_URL"

    while true; do
        HTTP_STATUS=$(curl -s -o "$EPOCH_ZIP" -w "%{http_code}" "$EPOCH_URL" || echo "000")

        if [ "$HTTP_STATUS" = "200" ] && [ -f "$EPOCH_ZIP" ]; then
            echo "Download successful, extracting files..."
            unzip -o "$EPOCH_ZIP" -d /qubic/
            rm -f "$EPOCH_ZIP"

            # Rename files to include correct epoch number
            echo "Renaming files to epoch $EPOCH..."
            cd /qubic
            for file in spectrum universe contract*; do
                [ -e "$file" ] || continue

                # Get base name without extension
                basename="${file%.*}"
                extension="${file##*.}"

                # Skip if already correctly named
                if [ "$extension" = "$EPOCH" ]; then
                    continue
                fi

                # Handle files without extension or with .000
                if [ "$file" = "$basename" ] || [ "$extension" = "000" ]; then
                    if [ -f "$file" ]; then
                        mv "$file" "${basename}.${EPOCH}"
                        echo "  Renamed: $file -> ${basename}.${EPOCH}"
                    fi
                fi
            done

            echo "Epoch files extracted and renamed successfully"
            return 0
        else
            echo "Waiting for initial files... (HTTP status: $HTTP_STATUS)"
            echo "Will retry in 60 seconds..."
            rm -f "$EPOCH_ZIP"
            sleep 60
        fi
    done
}

# Check if epoch files already exist
check_epoch_files() {
    # Check for spectrum and universe files (they should have .XXX extension where XXX is epoch)
    if ls /qubic/spectrum.* 1>/dev/null 2>&1 && ls /qubic/universe.* 1>/dev/null 2>&1; then
        echo "Epoch files already present"
        return 0
    fi
    return 1
}

# Download epoch files if not present or if FORCE_DOWNLOAD is set
if [ "$FORCE_DOWNLOAD" = "true" ] || [ "$FORCE_DOWNLOAD" = "1" ]; then
    echo "Force download enabled, removing existing epoch files..."
    rm -f /qubic/spectrum.* /qubic/universe.* /qubic/contract*.* 2>/dev/null || true
    download_epoch_files
elif ! check_epoch_files; then
    download_epoch_files
fi

# Build command arguments
ARGS=""

if [ -n "$PEERS" ]; then
    ARGS="$ARGS --peers $PEERS"
    echo "Peers: $PEERS"
fi

if [ -n "$SECURITY_TICK" ]; then
    ARGS="$ARGS --security-tick $SECURITY_TICK"
    echo "Security tick: $SECURITY_TICK"
fi

# Add any additional arguments passed to the container
if [ $# -gt 0 ]; then
    ARGS="$ARGS $@"
fi

echo "Starting Qubic node..."
echo "Command: ./Qubic $ARGS"
echo "========================================"

exec /qubic/Qubic $ARGS
