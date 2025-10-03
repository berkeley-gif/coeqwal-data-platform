#!/usr/bin/env python3
"""
PDF table to csv extractor

Requires user to specify expected column headers.
Manual cleanup usually necessary.

Usage:
    python extract_tables_to_csv.py --input document.pdf --pages 1-5 --headers "Col1,Col2,Col3"
"""

import argparse
import re
import csv
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

import fitz  # PyMuPDF
import pandas as pd


class Span:
    """Text span with position information."""
    def __init__(self, text, bbox):
        self.text = text.strip()
        self.x0, self.y0, self.x1, self.y1 = bbox
    
    @property
    def xmid(self): 
        return (self.x0 + self.x1) / 2
    
    @property
    def ymid(self): 
        return (self.y0 + self.y1) / 2


def get_spans(page) -> List[Span]:
    """Extract all text spans with positions from a page."""
    spans = []
    d = page.get_text("dict")
    for block in d.get("blocks", []):
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = span.get("text", "").strip()
                if text:
                    spans.append(Span(text, span["bbox"]))
    return spans


def group_lines(spans: List[Span], tolerance=3.5) -> List[List[Span]]:
    """Group spans into lines based on y-coordinate."""
    lines: List[List[Span]] = []
    
    for span in sorted(spans, key=lambda s: (s.ymid, s.x0)):
        placed = False
        for line in lines:
            avg_y = sum(s.ymid for s in line) / len(line)
            if abs(span.ymid - avg_y) <= tolerance:
                line.append(span)
                placed = True
                break
        if not placed:
            lines.append([span])
    
    return lines


def detect_header(lines: List[List[Span]], header_hints: List[str]) -> Tuple[List[Span], float, List[float], Tuple[float, float]]:
    """
    Detect header section using hint words from expected headers.
    Returns: header_spans, y_bottom, column_centers, page_bounds
    """
    best_window = None
    best_hits = -1
    
    # Try 1-3 line windows to find the best header match
    for i in range(len(lines)):
        window = lines[i:i+3]
        if not window:
                            continue
            
        # Combine all text in this window
        window_text = " ".join(
            span.text for line in window 
            for span in sorted(line, key=lambda s: s.x0)
        )
        
        # Count how many header hints appear in this window
        hits = sum(1 for hint in header_hints 
                  if re.search(rf"\b{re.escape(hint)}\b", window_text, re.I))
        
        if hits > best_hits:
            best_window = window
            best_hits = hits
    
    if best_window is None:
        raise RuntimeError("Could not find header section. Check that header hints match the PDF content.")
    
    # Extract header information
    header_spans = sorted([s for line in best_window for s in line], key=lambda s: s.x0)
    y_bottom = max(s.y1 for s in header_spans)
    page_left = min(s.x0 for s in header_spans) - 5
    page_right = max(s.x1 for s in header_spans) + 5
    
    # Calculate column centers from header positions
    x_positions = [s.xmid for s in header_spans]
    column_centers = []
    
    if x_positions:
        column_centers.append(x_positions[0])
        for x in x_positions[1:]:
            # Only add as new center if far enough from last center
            if abs(x - column_centers[-1]) > 35:
                column_centers.append(x)
            else:
                # Merge close positions
                column_centers[-1] = (column_centers[-1] + x) / 2
    
    return header_spans, y_bottom, column_centers, (page_left, page_right)


def cluster_columns_1d(x_positions: List[float], num_columns: int, 
                      initial_centers: List[float], bounds: Tuple[float, float], 
                      iterations: int = 12) -> List[float]:
    """1D k-means clustering to find optimal column positions."""
    left_bound, right_bound = bounds
    
    if not x_positions:
        # Fallback: evenly spaced columns
        return [left_bound + (right_bound - left_bound) * (i + 0.5) / num_columns 
                for i in range(num_columns)]
    
    # Initialize centers
    centers = initial_centers[:num_columns] if initial_centers else x_positions[:num_columns]
    
    # Fill missing centers with evenly spaced positions
    while len(centers) < num_columns:
        centers.append(left_bound + (right_bound - left_bound) * (len(centers) + 0.5) / num_columns)
    
    centers = sorted(centers)[:num_columns]
    
    # K-means iterations
    for _ in range(iterations):
        # Assign each x-position to nearest center
        buckets = [[] for _ in centers]
        for x in x_positions:
            closest_idx = min(range(len(centers)), key=lambda j: abs(x - centers[j]))
            buckets[closest_idx].append(x)
        
        # Update centers to bucket averages
        new_centers = []
        for i, bucket in enumerate(buckets):
            if bucket:
                new_centers.append(sum(bucket) / len(bucket))
            else:
                new_centers.append(centers[i])  # Keep old center if no data
        centers = new_centers
    
    return centers


def extract_table_rows(spans: List[Span], header_y_bottom: float, num_columns: int, 
                      initial_centers: List[float], bounds: Tuple[float, float]) -> List[List[str]]:
    """Extract table rows using header-guided column detection."""
    
    # Get spans below header
    body_spans = [s for s in spans if s.ymid > header_y_bottom + 3]
    body_lines = group_lines(body_spans, tolerance=4.0)
    
    # Get all x-coordinates for column clustering
    all_x = [s.xmid for s in body_spans]
    column_centers = cluster_columns_1d(all_x, num_columns, initial_centers, bounds)
    column_centers = sorted(column_centers)
    
    # Group lines into table rows (handle continuation rows)
    table_rows: List[List[List[Span]]] = []
    
    for line in body_lines:
        # Check if this line starts a new row (has text near first column)
        first_col_threshold = (abs(column_centers[1] - column_centers[0]) / 2 
                              if num_columns > 1 else 50)
        starts_new_row = any(abs(s.xmid - column_centers[0]) < first_col_threshold 
                           for s in line)
        
        if starts_new_row or not table_rows:
            table_rows.append([line])  # Start new row
        else:
            table_rows[-1].append(line)  # Continue previous row
    
    # Convert span groups to text rows
    text_rows: List[List[str]] = []
    
    for row_lines in table_rows:
        # Collect all spans from all lines in this row
        all_spans = []
        for line in row_lines:
            all_spans.extend(sorted(line, key=lambda s: s.x0))
        
        # Assign each span to nearest column
        columns = [[] for _ in range(num_columns)]
        for span in all_spans:
            closest_col = min(range(num_columns), 
                            key=lambda j: abs(span.xmid - column_centers[j]))
            columns[closest_col].append(span.text)
        
        # Join text in each column
        row = [" ".join(col).strip() for col in columns]
        text_rows.append(row)
    
    return text_rows


def clean_table_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and finalize extracted table data."""
    if df.empty:
        return df
    
    # Merge continuation rows (rows with blank first column)
    first_col = df.columns[0]
    merged_rows = []
    
    for _, row in df.iterrows():
        if str(row[first_col]).strip() == "" and merged_rows:
            # Continuation row - merge with previous
            prev_row = merged_rows[-1].copy()
            for col in df.columns:
                prev_text = str(prev_row[col]).strip()
                curr_text = str(row[col]).strip()
                if curr_text:
                    prev_row[col] = (prev_text + " " + curr_text).strip() if prev_text else curr_text
            merged_rows[-1] = prev_row
        else:
            # New row
            merged_rows.append(row.to_dict())
    
    df_merged = pd.DataFrame(merged_rows, columns=df.columns)
    
    # Clean whitespace
    for col in df_merged.columns:
        df_merged[col] = (df_merged[col].astype(str)
                         .str.replace(r"\s{2,}", " ", regex=True)
                         .str.strip())
    
    # Remove boilerplate rows (notes, chapter headers, etc.)
    mask = df_merged.apply(
        lambda row: not re.match(
            r"^(Notes:|Key:|CalSim|Chapter|August\s+\d{4}|3-\d+|Table\s+\d).*$",
            " ".join(row.values.astype(str)), re.I
        ), axis=1
    )
    df_clean = df_merged[mask].reset_index(drop=True)
    
    return df_clean


def extract_tables_with_headers(pdf_path: str, pages: List[int], headers: List[str], output_dir: str) -> bool:
    """Extract tables using user-defined headers for structure guidance."""
    
    pdf_path_obj = Path(pdf_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f" Header-guided extraction")
    print(f" Expected columns ({len(headers)}): {headers}")
    
    # Generate hints from headers for detection
    header_hints = []
    for header in headers:
        header_hints.extend(header.split())
    
    doc = fitz.open(pdf_path)
    all_rows = []
    global_column_centers = []
    global_bounds = None
    
    for page_num in pages:
        if page_num >= doc.page_count:
            print(f"  Page {page_num + 1} exceeds PDF length ({doc.page_count} pages)")
            continue
            
        print(f" Processing page {page_num + 1}...")
        page = doc.load_page(page_num)
        spans = get_spans(page)
        
        if not spans:
            print(f"  No text found on page {page_num + 1}")
            continue
        
        lines = group_lines(spans)
        
        try:
            header_spans, y_bottom, column_centers, bounds = detect_header(lines, header_hints)
            print(f" Found header with {len(column_centers)} column centers")
        except Exception as e:
            print(f"  Header detection failed on page {page_num + 1}: {e}")
            continue
        
        # Extract rows from this page
        rows = extract_table_rows(
            spans, 
            y_bottom, 
            len(headers), 
            column_centers or global_column_centers, 
            bounds or global_bounds or (min(s.x0 for s in spans), max(s.x1 for s in spans))
        )
        
        print(f" Extracted {len(rows)} rows from page {page_num + 1}")
        all_rows.extend(rows)
        
        # Update global settings for consistency across pages
        global_column_centers = column_centers or global_column_centers
        global_bounds = bounds or global_bounds
    
    doc.close()
    
    if not all_rows:
        print(" No table data extracted")
        return False
    
    # Create DataFrame and clean
    # Pad/trim rows to match header count
    width = len(headers)
    padded_rows = []
    for row in all_rows:
        if len(row) < width:
            padded_rows.append(row + [""] * (width - len(row)))
        else:
            padded_rows.append(row[:width])
    
    df = pd.DataFrame(padded_rows, columns=headers)
    df_clean = clean_table_data(df)
    
    # Count empty cells
    empty_count = 0
    for r in range(len(df_clean)):
        for c in df_clean.columns:
            if str(df_clean.iloc[r][c]).strip() == "":
                empty_count += 1
    
    # Save results with simple filename
    filename = f"{pdf_path_obj.stem}.csv"
    filepath = output_path / filename
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        # Just the csv data
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)  # Quote all fields for LibreOffice
        writer.writerow(headers)
        for _, row in df_clean.iterrows():
            writer.writerow(row.values)
    
    print(f"Saved {len(df_clean)} rows to: {filename}")
    
    return True


def parse_page_range(page_str: str) -> List[int]:
    """Parse page range string into list of 0-based page numbers."""
    pages = []
    for part in page_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = map(int, part.split('-'))
            pages.extend(range(start - 1, end))  # Convert to 0-based
        else:
            pages.append(int(part) - 1)  # Convert to 0-based
    return sorted(list(set(pages)))


def main():
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
    
    args = parser.parse_args()
    
    # Validate inputs
    pdf_path = Path(args.input)
    if not pdf_path.exists():
        print(f" PDF file not found: {pdf_path}")
        return False
    
    # Parse headers
    headers = [h.strip() for h in args.headers.split(',')]
    if len(headers) < 2:
        print(" Need at least 2 column headers")
        return False
    
    # Parse pages
    try:
        pages = parse_page_range(args.pages)
    except ValueError as e:
        print(f" Invalid page range: {e}")
        return False
    
    print(" PDF Table Scraper - Header Guided")
    print("=" * 45)
    print(f" Input: {pdf_path.name}")
    print(f" Pages: {args.pages} ({len(pages)} pages)")
    print(f" Expected columns: {len(headers)}")
    
    if args.verbose:
        print(f" Headers: {headers}")
    
    # Extract tables
    try:
        success = extract_tables_with_headers(str(pdf_path), pages, headers, args.output)
        return success
    except Exception as e:
        print(f" Extraction failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        print("\n Tips:")
        print("  - Make sure headers match the PDF table structure")
        print("  - Use --verbose for detailed error information")
        print("  - Check that pages contain actual tables")
        exit(1)
