#!/bin/bash

echo "Starting E-Commerce Data Pipeline setup..."

# Check Python
python --version || {
  echo "Python is not installed. Please install Python 3.8+"
  exit 1
}

# Create virtual environment
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python -m venv venv
fi

# Activate virtual environment (Windows)
source venv/Scripts/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

echo "Setup completed successfully!"
