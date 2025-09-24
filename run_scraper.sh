#!/bin/bash

# Change to the project directory
cd "$(dirname "$0")"

# Activate virtual environment
source .venv/bin/activate

# Use caffeinate to prevent sleep and run the script
# -i prevents idle sleep
# -d prevents display sleep
# -m prevents disk from sleeping
caffeinate -i -d -m python "Data Cleaning/DataAquisition.py"