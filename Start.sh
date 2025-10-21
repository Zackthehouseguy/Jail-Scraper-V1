#!/bin/bash

# Start script for Render deployment
# This runs the scraper on a schedule

echo "Starting Kentucky Mugshot Scraper..."
echo "Running on schedule: 09:00, 12:00, 15:00, 18:00"

# Run the scraper with schedule enabled
python scraper.py --schedule --times 09:00 12:00 15:00 18:00
