#!/bin/bash

# RecruitScout Job Scraper - Startup Script

echo "Starting RecruitScout Job Scraper..."
echo "======================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "======================================"
echo "Starting FastAPI server..."
echo "Open your browser to: http://localhost:8000"
echo "Press Ctrl+C to stop the server"
echo "======================================"
echo ""

# Run the application
python main.py
