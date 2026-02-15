#!/usr/bin/env python3
"""
Visualize reservoir percentile band charts.

Fetches data from the COEQWAL API and generates percentile band charts
for the 8 major reservoirs.

Usage:
    python visualize_percentile_bands.py --scenario s0020
    python visualize_percentile_bands.py --scenario s0020 --api-url http://localhost:8000
"""

import argparse
import json
import requests
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

# Water month labels (1=October, 12=September)
WATER_MONTHS = [
    'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar',
    'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'
]

# Major reservoirs
MAJOR_RESERVOIRS = ['SHSTA', 'TRNTY', 'OROVL', 'FOLSM', 'MELON', 'MLRTN', 'SLUIS_CVP', 'SLUIS_SWP']


def fetch_storage_data(api_url: str, scenario_id: str) -> dict:
    """Fetch storage-monthly data from the API."""
    url = f"{api_url}/api/statistics/scenarios/{scenario_id}/storage-monthly?group=major"
    print(f"Fetching data from: {url}")
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def plot_percentile_band_chart(
    reservoir_code: str,
    reservoir_name: str,
    monthly_data: dict,
    capacity_taf: float,
    scenario_id: str,
    output_dir: Path
):
    """
    Create a percentile band chart for a single reservoir.
    
    Args:
        reservoir_code: Reservoir short code (e.g., 'SHSTA')
        reservoir_name: Human-readable name (e.g., 'Shasta')
        monthly_data: Dict with keys 1-12 containing percentile data
        capacity_taf: Reservoir capacity in TAF
        scenario_id: Scenario identifier
        output_dir: Directory to save the chart
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # X-axis: water months 1-12
    x = np.arange(1, 13)
    
    # Extract percentile values for each month
    q0 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q0', 0) for m in range(1, 13)]
    q10 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q10', 0) for m in range(1, 13)]
    q30 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q30', 0) for m in range(1, 13)]
    q50 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q50', 0) for m in range(1, 13)]
    q70 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q70', 0) for m in range(1, 13)]
    q90 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q90', 0) for m in range(1, 13)]
    q100 = [monthly_data.get(str(m), monthly_data.get(m, {})).get('q100', 0) for m in range(1, 13)]
    mean = [monthly_data.get(str(m), monthly_data.get(m, {})).get('mean', 0) for m in range(1, 13)]
    
    # Plot bands from outer to inner
    # Outer band: q10-q90 (lightest blue)
    ax.fill_between(x, q10, q90, alpha=0.3, color='#3182bd', label='10th-90th percentile')
    
    # Middle band: q30-q70 (medium blue)
    ax.fill_between(x, q30, q70, alpha=0.5, color='#3182bd', label='30th-70th percentile')
    
    # Median line: q50
    ax.plot(x, q50, color='#08519c', linewidth=2.5, label='Median (50th)', marker='o', markersize=4)
    
    # Min/max lines (dashed, for reference)
    ax.plot(x, q0, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7, label='Min/Max')
    ax.plot(x, q100, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7)
    
    # Styling
    ax.set_xlim(0.5, 12.5)
    ax.set_ylim(0, max(105, max(q100) * 1.05))
    ax.set_xticks(x)
    ax.set_xticklabels(WATER_MONTHS)
    ax.set_xlabel('Water Year Month', fontsize=12)
    ax.set_ylabel('Storage (% of Capacity)', fontsize=12)
    ax.set_title(f'{reservoir_name} ({reservoir_code}) - Monthly Storage Percentiles\n'
                 f'Scenario: {scenario_id} | Capacity: {capacity_taf:,.0f} TAF', fontsize=14)
    
    # Add horizontal line at 100% capacity
    ax.axhline(y=100, color='red', linewidth=1, linestyle=':', alpha=0.5, label='100% Capacity')
    
    # Grid
    ax.grid(True, alpha=0.3, linestyle='-')
    ax.set_axisbelow(True)
    
    # Legend
    ax.legend(loc='lower right', fontsize=9)
    
    # Save
    output_path = output_dir / f'{reservoir_code}_{scenario_id}_percentile_bands.png'
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {output_path}")
    return output_path


def plot_all_reservoirs_grid(data: dict, scenario_id: str, output_dir: Path):
    """Create a 2x4 grid showing all 8 major reservoirs."""
    fig, axes = plt.subplots(2, 4, figsize=(20, 10))
    axes = axes.flatten()
    
    reservoirs = data.get('reservoirs', {})
    
    for idx, res_code in enumerate(MAJOR_RESERVOIRS):
        if res_code not in reservoirs:
            print(f"  Warning: {res_code} not found in data")
            continue
            
        ax = axes[idx]
        res_data = reservoirs[res_code]
        monthly = res_data.get('monthly_percent', res_data.get('monthly', {}))
        name = res_data.get('name', res_code)
        capacity = res_data.get('capacity_taf', 0)
        
        x = np.arange(1, 13)
        
        # Extract percentiles
        q10 = [monthly.get(str(m), monthly.get(m, {})).get('q10', 0) for m in range(1, 13)]
        q30 = [monthly.get(str(m), monthly.get(m, {})).get('q30', 0) for m in range(1, 13)]
        q50 = [monthly.get(str(m), monthly.get(m, {})).get('q50', 0) for m in range(1, 13)]
        q70 = [monthly.get(str(m), monthly.get(m, {})).get('q70', 0) for m in range(1, 13)]
        q90 = [monthly.get(str(m), monthly.get(m, {})).get('q90', 0) for m in range(1, 13)]
        
        # Plot bands
        ax.fill_between(x, q10, q90, alpha=0.3, color='#3182bd')
        ax.fill_between(x, q30, q70, alpha=0.5, color='#3182bd')
        ax.plot(x, q50, color='#08519c', linewidth=2, marker='o', markersize=3)
        
        # Styling
        ax.set_xlim(0.5, 12.5)
        ax.set_ylim(0, 110)
        ax.set_xticks([1, 4, 7, 10])
        ax.set_xticklabels(['Oct', 'Jan', 'Apr', 'Jul'])
        ax.axhline(y=100, color='red', linewidth=0.5, linestyle=':', alpha=0.5)
        ax.grid(True, alpha=0.3)
        ax.set_title(f'{name}\n({capacity:,.0f} TAF)', fontsize=10)
        
        if idx >= 4:
            ax.set_xlabel('Month')
        if idx % 4 == 0:
            ax.set_ylabel('% Capacity')
    
    fig.suptitle(f'Major Reservoirs - Monthly Storage Percentiles (Scenario: {scenario_id})', 
                 fontsize=14, fontweight='bold')
    
    # Add legend
    legend_elements = [
        mpatches.Patch(color='#3182bd', alpha=0.3, label='10th-90th %ile'),
        mpatches.Patch(color='#3182bd', alpha=0.5, label='30th-70th %ile'),
        plt.Line2D([0], [0], color='#08519c', linewidth=2, label='Median'),
    ]
    fig.legend(handles=legend_elements, loc='lower center', ncol=3, fontsize=10)
    
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    
    output_path = output_dir / f'all_major_reservoirs_{scenario_id}_grid.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved grid: {output_path}")
    return output_path


def print_data_summary(data: dict):
    """Print a summary of the data for debugging."""
    print("\n=== Data Summary ===")
    print(f"Scenario: {data.get('scenario_id')}")
    print(f"Group: {data.get('group', 'N/A')}")
    
    reservoirs = data.get('reservoirs', {})
    print(f"Reservoirs: {len(reservoirs)}")
    
    for code, res_data in reservoirs.items():
        name = res_data.get('name', code)
        capacity = res_data.get('capacity_taf', 0)
        monthly = res_data.get('monthly_percent', res_data.get('monthly', {}))
        
        # Get October (month 1) data as sample
        oct_data = monthly.get('1', monthly.get(1, {}))
        
        print(f"\n  {code} ({name}):")
        print(f"    Capacity: {capacity:,.0f} TAF")
        print(f"    October percentiles: q10={oct_data.get('q10', 'N/A')}, "
              f"q50={oct_data.get('q50', 'N/A')}, q90={oct_data.get('q90', 'N/A')}")


def create_pdf_report(data: dict, scenario_id: str, output_dir: Path):
    """Create a multi-page PDF with all reservoir charts."""
    from matplotlib.backends.backend_pdf import PdfPages
    
    pdf_path = output_dir / f'reservoir_percentile_bands_{scenario_id}.pdf'
    
    with PdfPages(pdf_path) as pdf:
        # Title page / Grid view
        fig, axes = plt.subplots(2, 4, figsize=(16, 10))
        axes = axes.flatten()
        
        reservoirs = data.get('reservoirs', {})
        
        for idx, res_code in enumerate(MAJOR_RESERVOIRS):
            if res_code not in reservoirs:
                continue
                
            ax = axes[idx]
            res_data = reservoirs[res_code]
            monthly = res_data.get('monthly_percent', res_data.get('monthly', {}))
            name = res_data.get('name', res_code)
            capacity = res_data.get('capacity_taf', 0)
            
            x = np.arange(1, 13)
            
            q10 = [monthly.get(str(m), monthly.get(m, {})).get('q10', 0) for m in range(1, 13)]
            q30 = [monthly.get(str(m), monthly.get(m, {})).get('q30', 0) for m in range(1, 13)]
            q50 = [monthly.get(str(m), monthly.get(m, {})).get('q50', 0) for m in range(1, 13)]
            q70 = [monthly.get(str(m), monthly.get(m, {})).get('q70', 0) for m in range(1, 13)]
            q90 = [monthly.get(str(m), monthly.get(m, {})).get('q90', 0) for m in range(1, 13)]
            
            ax.fill_between(x, q10, q90, alpha=0.3, color='#3182bd')
            ax.fill_between(x, q30, q70, alpha=0.5, color='#3182bd')
            ax.plot(x, q50, color='#08519c', linewidth=2, marker='o', markersize=3)
            
            ax.set_xlim(0.5, 12.5)
            ax.set_ylim(0, 110)
            ax.set_xticks([1, 4, 7, 10])
            ax.set_xticklabels(['Oct', 'Jan', 'Apr', 'Jul'])
            ax.axhline(y=100, color='red', linewidth=0.5, linestyle=':', alpha=0.5)
            ax.grid(True, alpha=0.3)
            ax.set_title(f'{name}\n({capacity:,.0f} TAF)', fontsize=10)
            
            if idx >= 4:
                ax.set_xlabel('Month')
            if idx % 4 == 0:
                ax.set_ylabel('% Capacity')
        
        fig.suptitle(f'Major Reservoirs - Monthly Storage Percentiles\nScenario: {scenario_id}', 
                     fontsize=14, fontweight='bold')
        
        legend_elements = [
            mpatches.Patch(color='#3182bd', alpha=0.3, label='10th-90th %ile'),
            mpatches.Patch(color='#3182bd', alpha=0.5, label='30th-70th %ile'),
            plt.Line2D([0], [0], color='#08519c', linewidth=2, label='Median'),
        ]
        fig.legend(handles=legend_elements, loc='lower center', ncol=3, fontsize=10)
        plt.tight_layout(rect=[0, 0.05, 1, 0.93])
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()
        
        # Individual pages for each reservoir
        for res_code in MAJOR_RESERVOIRS:
            if res_code not in reservoirs:
                continue
                
            res_data = reservoirs[res_code]
            monthly = res_data.get('monthly_percent', res_data.get('monthly', {}))
            name = res_data.get('name', res_code)
            capacity = res_data.get('capacity_taf', 0)
            
            fig, ax = plt.subplots(figsize=(12, 7))
            x = np.arange(1, 13)
            
            q0 = [monthly.get(str(m), monthly.get(m, {})).get('q0', 0) for m in range(1, 13)]
            q10 = [monthly.get(str(m), monthly.get(m, {})).get('q10', 0) for m in range(1, 13)]
            q30 = [monthly.get(str(m), monthly.get(m, {})).get('q30', 0) for m in range(1, 13)]
            q50 = [monthly.get(str(m), monthly.get(m, {})).get('q50', 0) for m in range(1, 13)]
            q70 = [monthly.get(str(m), monthly.get(m, {})).get('q70', 0) for m in range(1, 13)]
            q90 = [monthly.get(str(m), monthly.get(m, {})).get('q90', 0) for m in range(1, 13)]
            q100 = [monthly.get(str(m), monthly.get(m, {})).get('q100', 0) for m in range(1, 13)]
            
            ax.fill_between(x, q10, q90, alpha=0.3, color='#3182bd', label='10th-90th percentile')
            ax.fill_between(x, q30, q70, alpha=0.5, color='#3182bd', label='30th-70th percentile')
            ax.plot(x, q50, color='#08519c', linewidth=2.5, label='Median (50th)', marker='o', markersize=5)
            ax.plot(x, q0, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7, label='Min/Max')
            ax.plot(x, q100, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7)
            
            ax.set_xlim(0.5, 12.5)
            ax.set_ylim(0, max(105, max(q100) * 1.05))
            ax.set_xticks(x)
            ax.set_xticklabels(WATER_MONTHS)
            ax.set_xlabel('Water Year Month', fontsize=12)
            ax.set_ylabel('Storage (% of Capacity)', fontsize=12)
            ax.set_title(f'{name} ({res_code}) - Monthly Storage Percentiles\n'
                         f'Scenario: {scenario_id} | Capacity: {capacity:,.0f} TAF', fontsize=14)
            ax.axhline(y=100, color='red', linewidth=1, linestyle=':', alpha=0.5, label='100% Capacity')
            ax.grid(True, alpha=0.3)
            ax.legend(loc='lower right', fontsize=9)
            
            plt.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
    
    print(f"  Saved PDF: {pdf_path}")
    return pdf_path


def main():
    parser = argparse.ArgumentParser(description='Visualize reservoir percentile band charts')
    parser.add_argument('--scenario', '-s', default='s0020', help='Scenario ID (default: s0020)')
    parser.add_argument('--api-url', default='https://api.coeqwal.org', 
                        help='API base URL (default: https://api.coeqwal.org)')
    parser.add_argument('--output-dir', '-o', default='./charts', help='Output directory')
    parser.add_argument('--summary-only', action='store_true', help='Just print data summary')
    parser.add_argument('--pdf', action='store_true', help='Generate PDF report')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Fetch data
    print(f"Fetching data for scenario {args.scenario}...")
    data = fetch_storage_data(args.api_url, args.scenario)
    
    # Print summary
    print_data_summary(data)
    
    if args.summary_only:
        return
    
    # Generate charts
    print("\nGenerating charts...")
    
    # Grid view of all reservoirs
    plot_all_reservoirs_grid(data, args.scenario, output_dir)
    
    # Individual charts
    reservoirs = data.get('reservoirs', {})
    for code in MAJOR_RESERVOIRS:
        if code not in reservoirs:
            print(f"  Skipping {code} - not in data")
            continue
            
        res_data = reservoirs[code]
        monthly = res_data.get('monthly_percent', res_data.get('monthly', {}))
        
        plot_percentile_band_chart(
            reservoir_code=code,
            reservoir_name=res_data.get('name', code),
            monthly_data=monthly,
            capacity_taf=res_data.get('capacity_taf', 0),
            scenario_id=args.scenario,
            output_dir=output_dir
        )
    
    # Generate PDF if requested
    if args.pdf:
        print("\nGenerating PDF report...")
        create_pdf_report(data, args.scenario, output_dir)
    
    print(f"\nDone! Charts saved to {output_dir}/")


if __name__ == '__main__':
    main()
