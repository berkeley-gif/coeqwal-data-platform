#!/bin/bash

# PDF table scraper installation script

set -e  # Exit on any error

echo " Installing PDF Table Scraper"
echo "==============================="

# Check if we're in a virtual environment
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo " Virtual environment detected: $VIRTUAL_ENV"
else
    echo "  No virtual environment detected. Consider using one:"
    echo "   python -m venv venv"
    echo "   source venv/bin/activate  # On macOS/Linux"
    echo ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check Python version
echo "� Checking Python version..."
python_version=$(python --version 2>&1 | cut -d' ' -f2)
echo "Python version: $python_version"

# Check if pip is available
if ! command -v pip &> /dev/null; then
    echo " pip not found. Please install pip first."
    exit 1
fi

echo " pip found: $(pip --version)"

# Update pip
echo "� Updating pip..."
pip install --upgrade pip

# Install dependencies
echo " Installing PDF processing libraries..."

echo "Installing pandas and numpy..."
pip install pandas numpy

echo "Installing pdfplumber (text-based PDF extraction)..."
pip install pdfplumber

echo "Installing PyMuPDF (fast PDF processing)..."
pip install PyMuPDF

echo " All dependencies installed successfully!"

# Test the installation
echo "� Testing installation..."
script_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$script_dir"

if python -c "
import pdfplumber
import fitz  # PyMuPDF
import pandas as pd
import numpy as np
from pdf_table_scraper import extract_tables_with_headers
print(' All imports successful')
print(' PDF scraper ready to use')
"; then
    echo ""
    echo " Installation completed successfully!"
    echo ""
    echo " Quick Start:"
else
    echo ""
    echo "  Installation completed but tests failed"
    echo "   Some features may not work properly"
fi

echo ""
echo " Installation completed"
echo ""
