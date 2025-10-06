#!/bin/bash

# Change to the project directory
cd "$(dirname "$0")"

# Activate virtual environment
source .venv/bin/activate

# Use caffeinate to prevent sleep and run the script
# -i prevents idle sleep
# -d prevents display sleep
# -m prevents disk from sleeping
caffeinate -i -d -m python "DataPipeline/DataAquisition.py"

# Calculate duration
end_time=$(date +%s)
duration=$((end_time - start_time))
hours=$((duration / 3600))
minutes=$(( (duration % 3600) / 60 ))
seconds=$((duration % 60))

# Log runtime
echo "Total runtime: ${hours}h ${minutes}m ${seconds}s" >> "${HOME}/scraper_runtime.log"