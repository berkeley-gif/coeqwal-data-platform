#!/usr/bin/env python3
"""
PDF table to csv extractor

Requires user to specify expected column headers.
Manual cleanup usually necessary.

Usage:
    python extract_tables_to_csv.py --input document.pdf --pages 1-5 --headers "Col1,Col2,Col3"
"""

import argparse
import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    """Extract tables from PDF to CSV files using headers."""
    
    parser = argparse.ArgumentParser(
        description="PDF table extraction with user-defined headers"
    )
    
    parser.add_argument('--input', '-i', required=True,
                       help='Path to PDF file')
    parser.add_argument('--pages', '-p', required=True,
                       help='Page range (e.g., "1-5", "1,3,5-7")')
    parser.add_argument('--headers', required=True,
                       help='Expected column headers (comma-separated)')
    parser.add_argument('--output', '-o', default='./extracted_tables',
                       help='Output directory')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show detailed extraction process')
    
    # Check if headers are provided (custom error message)
    if '--headers' not in sys.argv and '-h' not in sys.argv and '--help' not in sys.argv:
        print("Missing required --headers parameter")
        print('Example: --headers "Column1,Column2,Column3"')
        return False
    
    args = parser.parse_args()
    
    # Validate inputs
    pdf_path = Path(args.input)
    if not pdf_path.exists():
        print(f"ERROR: PDF file not found: {pdf_path}")
        return False
    
    # Parse headers
    headers = [h.strip() for h in args.headers.split(',')]
    
    # Check for common quoting issues
    if len(headers) == 1 and ' ' in headers[0] and len(headers[0].split()) > 3:
        print("Headers need commas between them")
        print('Use: --headers "Header One,Header Two,Header Three"')
        return False
    
    if len(headers) < 2:
        print("Need at least 2 column headers")
        return False
    
    # Parse pages
    try:
        pages = []
        for part in args.pages.split(','):
            part = part.strip()
            if '-' in part:
                start, end = map(int, part.split('-'))
                pages.extend(range(start - 1, end))  # Convert to 0-based
            else:
                pages.append(int(part) - 1)  # Convert to 0-based
        pages = sorted(list(set(pages)))
    except ValueError as e:
        print(f" Invalid page range: {e}")
        return False
    
    print(" PDF table scraper")
    print("=" * 45)
    print(f" Input: {pdf_path.name}")
    print(f" Pages: {args.pages} ({len(pages)} pages)")
    print(f" Expected columns: {len(headers)}")
    
    if args.verbose:
        print(f" Headers: {headers}")
    
    # Import and run the scraper
    try:
        from pdf_table_scraper import extract_tables_with_headers
        
        success = extract_tables_with_headers(
            str(pdf_path), 
            pages, 
            headers, 
            args.output
        )
        
        if success:
            print(f"\n Extraction completed successfully!")
            print(f" Check results in: {args.output}")
        
        return success
        
    except ImportError as e:
        print(f" Could not import scraper: {e}")
        print("Make sure dependencies are installed: ./install_pdf_scraper.sh")
        return False
    except Exception as e:
        print(f" Extraction failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)