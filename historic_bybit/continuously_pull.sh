#!/bin/bash

# Define the Python script you want to run
PYTHON_SCRIPT="pullData.py"

# Infinite loop to keep restarting the Python script
while true; do
    echo "Starting $PYTHON_SCRIPT..."
    python3 $PYTHON_SCRIPT

    # Check the exit status of the Python script
    if [ $? -ne 0 ]; then
        echo "The script $PYTHON_SCRIPT crashed. Restarting..."
    else
        echo "The script $PYTHON_SCRIPT finished successfully. Exiting."
        break
    fi

    # Optional: add a delay before restarting (to avoid rapid restarts)
    sleep 1
done
