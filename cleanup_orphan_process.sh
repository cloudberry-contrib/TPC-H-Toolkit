#!/bin/bash
set -e

# Get the basename of the current working directory as the keyword
keyword=$(basename "${PWD}")
echo "Cleaning up processes containing keyword '${keyword}'..."

# Find processes containing the keyword, excluding grep and current script
processes=$(ps aux | grep "${keyword}" | grep -v "grep" | grep -v "cleanup_orphan_process.sh")

# Check if any processes were found
if [ -z "$processes" ]; then
  echo "No processes found containing keyword '${keyword}'"
  exit 0
fi

# Display the found processes
echo "Found the following processes:"
echo "$processes"

# Extract PIDs
pids=$(echo "$processes" | awk '{print $2}')
echo "Process IDs to terminate: $pids"

# Terminate processes
for pid in $pids; do
  echo "Terminating process $pid"
  kill -15 "$pid" 2>/dev/null || echo "Warning: Failed to terminate process $pid"
done

# Wait and check
sleep 1
echo "\nCleanup completed."