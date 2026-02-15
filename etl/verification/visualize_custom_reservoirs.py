#!/usr/bin/env python3
"""
Visualize reservoir percentile band charts for custom reservoir sets.

Generates both percent-of-capacity AND volume (TAF) charts.

Usage:
    python visualize_custom_reservoirs.py --scenario s0020
"""

import argparse
import requests
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path
from matplotlib.backends.backend_pdf import PdfPages

# Water month labels (1=October, 12=September)
WATER_MONTHS = [
    'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar',
    'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'
]

# Custom reservoir set for verification
CUSTOM_RESERVOIRS = {
    'ANTLP': {'name': 'Antelope', 'capacity': 179},
    'FRNCH': {'name': 'French Faucherie Sawmill', 'capacity': 13.8},
    'HTCHY': {'name': 'Hetch Hetchy', 'capacity': 360},
    'THRMF': {'name': 'Thermalito Forebay', 'capacity': 73.5},
    'EBMUD': {'name': 'EBMUD Terminal Reservoirs', 'capacity': 200},
    'ALMNR': {'name': 'Almanor', 'capacity': 1143},
    'KSWCK': {'name': 'Keswick', 'capacity': 23.8},
    'PYRMD': {'name': 'Pyramid', 'capacity': 173},
}


def fetch_storage_data(api_url: str, scenario_id: str, reservoir_codes: list) -> dict:
    """Fetch storage-monthly data from the API for specific reservoirs."""
    codes_param = ','.join(reservoir_codes)
    url = f"{api_url}/api/statistics/scenarios/{scenario_id}/storage-monthly?reservoirs={codes_param}"
    print(f"Fetching data from: {url}")
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def create_dual_pdf_report(data: dict, scenario_id: str, output_dir: Path, reservoir_order: list):
    """
    Create a multi-page PDF with both percent and TAF charts for each reservoir.
    
    Structure:
    - Page 1: 2x4 grid of percent-of-capacity charts
    - Page 2: 2x4 grid of volume (TAF) charts
    - Pages 3+: Individual reservoir pages with side-by-side percent and TAF
    """
    pdf_path = output_dir / f'verification_reservoirs_{scenario_id}.pdf'
    
    with PdfPages(pdf_path) as pdf:
        reservoirs = data.get('reservoirs', {})
        
        # === PAGE 1: Grid of percent-of-capacity charts ===
        fig, axes = plt.subplots(2, 4, figsize=(16, 10))
        axes = axes.flatten()
        
        for idx, res_code in enumerate(reservoir_order):
            if res_code not in reservoirs:
                axes[idx].text(0.5, 0.5, f'{res_code}\nNo Data', 
                              ha='center', va='center', fontsize=12)
                axes[idx].set_title(CUSTOM_RESERVOIRS.get(res_code, {}).get('name', res_code))
                continue
                
            ax = axes[idx]
            res_data = reservoirs[res_code]
            monthly = res_data.get('monthly_percent', {})
            name = res_data.get('name', CUSTOM_RESERVOIRS.get(res_code, {}).get('name', res_code))
            capacity = res_data.get('capacity_taf', CUSTOM_RESERVOIRS.get(res_code, {}).get('capacity', 0))
            
            x = np.arange(1, 13)
            
            q10 = [monthly.get(str(m), {}).get('q10', 0) for m in range(1, 13)]
            q30 = [monthly.get(str(m), {}).get('q30', 0) for m in range(1, 13)]
            q50 = [monthly.get(str(m), {}).get('q50', 0) for m in range(1, 13)]
            q70 = [monthly.get(str(m), {}).get('q70', 0) for m in range(1, 13)]
            q90 = [monthly.get(str(m), {}).get('q90', 0) for m in range(1, 13)]
            q100 = [monthly.get(str(m), {}).get('q100', 0) for m in range(1, 13)]
            
            ax.fill_between(x, q10, q90, alpha=0.3, color='#3182bd')
            ax.fill_between(x, q30, q70, alpha=0.5, color='#3182bd')
            ax.plot(x, q50, color='#08519c', linewidth=2, marker='o', markersize=3)
            
            ax.set_xlim(0.5, 12.5)
            max_val = max(q100) if q100 else 100
            ax.set_ylim(0, max(110, max_val * 1.05))
            ax.set_xticks([1, 4, 7, 10])
            ax.set_xticklabels(['Oct', 'Jan', 'Apr', 'Jul'])
            ax.axhline(y=100, color='red', linewidth=0.5, linestyle=':', alpha=0.5)
            ax.grid(True, alpha=0.3)
            ax.set_title(f'{name}\n({capacity:,.1f} TAF)', fontsize=9)
            
            if idx >= 4:
                ax.set_xlabel('Month')
            if idx % 4 == 0:
                ax.set_ylabel('% Capacity')
        
        fig.suptitle(f'Verification Reservoirs - Percent of Capacity\nScenario: {scenario_id}', 
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
        
        # === PAGE 2: Grid of volume (TAF) charts ===
        fig, axes = plt.subplots(2, 4, figsize=(16, 10))
        axes = axes.flatten()
        
        for idx, res_code in enumerate(reservoir_order):
            if res_code not in reservoirs:
                axes[idx].text(0.5, 0.5, f'{res_code}\nNo Data', 
                              ha='center', va='center', fontsize=12)
                axes[idx].set_title(CUSTOM_RESERVOIRS.get(res_code, {}).get('name', res_code))
                continue
                
            ax = axes[idx]
            res_data = reservoirs[res_code]
            monthly = res_data.get('monthly_taf', {})
            name = res_data.get('name', CUSTOM_RESERVOIRS.get(res_code, {}).get('name', res_code))
            capacity = res_data.get('capacity_taf', CUSTOM_RESERVOIRS.get(res_code, {}).get('capacity', 0))
            
            x = np.arange(1, 13)
            
            q10 = [monthly.get(str(m), {}).get('q10', 0) for m in range(1, 13)]
            q30 = [monthly.get(str(m), {}).get('q30', 0) for m in range(1, 13)]
            q50 = [monthly.get(str(m), {}).get('q50', 0) for m in range(1, 13)]
            q70 = [monthly.get(str(m), {}).get('q70', 0) for m in range(1, 13)]
            q90 = [monthly.get(str(m), {}).get('q90', 0) for m in range(1, 13)]
            q100 = [monthly.get(str(m), {}).get('q100', 0) for m in range(1, 13)]
            
            ax.fill_between(x, q10, q90, alpha=0.3, color='#2ca02c')
            ax.fill_between(x, q30, q70, alpha=0.5, color='#2ca02c')
            ax.plot(x, q50, color='#1a6e1a', linewidth=2, marker='o', markersize=3)
            
            ax.set_xlim(0.5, 12.5)
            max_val = max(q100) if q100 else capacity
            ax.set_ylim(0, max(capacity * 1.1, max_val * 1.05))
            ax.set_xticks([1, 4, 7, 10])
            ax.set_xticklabels(['Oct', 'Jan', 'Apr', 'Jul'])
            ax.axhline(y=capacity, color='red', linewidth=0.5, linestyle=':', alpha=0.5)
            ax.grid(True, alpha=0.3)
            ax.set_title(f'{name}\n({capacity:,.1f} TAF capacity)', fontsize=9)
            
            if idx >= 4:
                ax.set_xlabel('Month')
            if idx % 4 == 0:
                ax.set_ylabel('Storage (TAF)')
        
        fig.suptitle(f'Verification Reservoirs - Storage Volume (TAF)\nScenario: {scenario_id}', 
                     fontsize=14, fontweight='bold')
        
        legend_elements = [
            mpatches.Patch(color='#2ca02c', alpha=0.3, label='10th-90th %ile'),
            mpatches.Patch(color='#2ca02c', alpha=0.5, label='30th-70th %ile'),
            plt.Line2D([0], [0], color='#1a6e1a', linewidth=2, label='Median'),
        ]
        fig.legend(handles=legend_elements, loc='lower center', ncol=3, fontsize=10)
        plt.tight_layout(rect=[0, 0.05, 1, 0.93])
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()
        
        # === INDIVIDUAL PAGES: Side-by-side percent and TAF for each reservoir ===
        for res_code in reservoir_order:
            if res_code not in reservoirs:
                print(f"  Skipping {res_code} - no data")
                continue
                
            res_data = reservoirs[res_code]
            monthly_pct = res_data.get('monthly_percent', {})
            monthly_taf = res_data.get('monthly_taf', {})
            name = res_data.get('name', CUSTOM_RESERVOIRS.get(res_code, {}).get('name', res_code))
            capacity = res_data.get('capacity_taf', CUSTOM_RESERVOIRS.get(res_code, {}).get('capacity', 0))
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
            x = np.arange(1, 13)
            
            # Left: Percent of Capacity
            q0_pct = [monthly_pct.get(str(m), {}).get('q0', 0) for m in range(1, 13)]
            q10_pct = [monthly_pct.get(str(m), {}).get('q10', 0) for m in range(1, 13)]
            q30_pct = [monthly_pct.get(str(m), {}).get('q30', 0) for m in range(1, 13)]
            q50_pct = [monthly_pct.get(str(m), {}).get('q50', 0) for m in range(1, 13)]
            q70_pct = [monthly_pct.get(str(m), {}).get('q70', 0) for m in range(1, 13)]
            q90_pct = [monthly_pct.get(str(m), {}).get('q90', 0) for m in range(1, 13)]
            q100_pct = [monthly_pct.get(str(m), {}).get('q100', 0) for m in range(1, 13)]
            
            ax1.fill_between(x, q10_pct, q90_pct, alpha=0.3, color='#3182bd', label='10th-90th %ile')
            ax1.fill_between(x, q30_pct, q70_pct, alpha=0.5, color='#3182bd', label='30th-70th %ile')
            ax1.plot(x, q50_pct, color='#08519c', linewidth=2.5, label='Median', marker='o', markersize=5)
            ax1.plot(x, q0_pct, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7, label='Min/Max')
            ax1.plot(x, q100_pct, color='#6baed6', linewidth=1, linestyle='--', alpha=0.7)
            
            ax1.set_xlim(0.5, 12.5)
            max_pct = max(q100_pct) if q100_pct else 100
            ax1.set_ylim(0, max(110, max_pct * 1.05))
            ax1.set_xticks(x)
            ax1.set_xticklabels(WATER_MONTHS)
            ax1.set_xlabel('Water Year Month', fontsize=11)
            ax1.set_ylabel('Storage (% of Capacity)', fontsize=11)
            ax1.set_title('Percent of Capacity', fontsize=12, fontweight='bold')
            ax1.axhline(y=100, color='red', linewidth=1, linestyle=':', alpha=0.5, label='100% Capacity')
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='lower right', fontsize=8)
            
            # Right: Volume (TAF)
            q0_taf = [monthly_taf.get(str(m), {}).get('q0', 0) for m in range(1, 13)]
            q10_taf = [monthly_taf.get(str(m), {}).get('q10', 0) for m in range(1, 13)]
            q30_taf = [monthly_taf.get(str(m), {}).get('q30', 0) for m in range(1, 13)]
            q50_taf = [monthly_taf.get(str(m), {}).get('q50', 0) for m in range(1, 13)]
            q70_taf = [monthly_taf.get(str(m), {}).get('q70', 0) for m in range(1, 13)]
            q90_taf = [monthly_taf.get(str(m), {}).get('q90', 0) for m in range(1, 13)]
            q100_taf = [monthly_taf.get(str(m), {}).get('q100', 0) for m in range(1, 13)]
            
            ax2.fill_between(x, q10_taf, q90_taf, alpha=0.3, color='#2ca02c', label='10th-90th %ile')
            ax2.fill_between(x, q30_taf, q70_taf, alpha=0.5, color='#2ca02c', label='30th-70th %ile')
            ax2.plot(x, q50_taf, color='#1a6e1a', linewidth=2.5, label='Median', marker='o', markersize=5)
            ax2.plot(x, q0_taf, color='#7fcc7f', linewidth=1, linestyle='--', alpha=0.7, label='Min/Max')
            ax2.plot(x, q100_taf, color='#7fcc7f', linewidth=1, linestyle='--', alpha=0.7)
            
            ax2.set_xlim(0.5, 12.5)
            max_taf = max(q100_taf) if q100_taf else capacity
            ax2.set_ylim(0, max(capacity * 1.1, max_taf * 1.05))
            ax2.set_xticks(x)
            ax2.set_xticklabels(WATER_MONTHS)
            ax2.set_xlabel('Water Year Month', fontsize=11)
            ax2.set_ylabel('Storage (TAF)', fontsize=11)
            ax2.set_title('Volume (TAF)', fontsize=12, fontweight='bold')
            ax2.axhline(y=capacity, color='red', linewidth=1, linestyle=':', alpha=0.5, label=f'Capacity ({capacity:,.1f} TAF)')
            ax2.grid(True, alpha=0.3)
            ax2.legend(loc='lower right', fontsize=8)
            
            fig.suptitle(f'{name} ({res_code}) - Monthly Storage Percentiles\n'
                         f'Scenario: {scenario_id} | Capacity: {capacity:,.1f} TAF', 
                         fontsize=14, fontweight='bold')
            
            plt.tight_layout(rect=[0, 0, 1, 0.93])
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            print(f"  Added page for {res_code}")
    
    print(f"\nSaved PDF: {pdf_path}")
    return pdf_path


def print_data_summary(data: dict, reservoir_order: list):
    """Print a summary of the data."""
    print("\n=== Data Summary ===")
    print(f"Scenario: {data.get('scenario_id')}")
    
    reservoirs = data.get('reservoirs', {})
    print(f"Reservoirs returned: {len(reservoirs)}")
    
    for code in reservoir_order:
        if code not in reservoirs:
            print(f"\n  {code}: NO DATA")
            continue
            
        res_data = reservoirs[code]
        name = res_data.get('name', code)
        capacity = res_data.get('capacity_taf', 0)
        monthly_pct = res_data.get('monthly_percent', {})
        monthly_taf = res_data.get('monthly_taf', {})
        
        # Get October (month 1) data as sample
        oct_pct = monthly_pct.get('1', {})
        oct_taf = monthly_taf.get('1', {})
        
        print(f"\n  {code} ({name}):")
        print(f"    Capacity: {capacity:,.1f} TAF")
        print(f"    October %: q10={oct_pct.get('q10', 'N/A'):.1f}, "
              f"q50={oct_pct.get('q50', 'N/A'):.1f}, q90={oct_pct.get('q90', 'N/A'):.1f}")
        print(f"    October TAF: q10={oct_taf.get('q10', 'N/A'):.1f}, "
              f"q50={oct_taf.get('q50', 'N/A'):.1f}, q90={oct_taf.get('q90', 'N/A'):.1f}")


def main():
    parser = argparse.ArgumentParser(description='Visualize reservoir percentile band charts')
    parser.add_argument('--scenario', '-s', default='s0020', help='Scenario ID (default: s0020)')
    parser.add_argument('--api-url', default='https://api.coeqwal.org', 
                        help='API base URL (default: https://api.coeqwal.org)')
    
    args = parser.parse_args()
    
    # Output directory
    output_dir = Path(__file__).parent
    output_dir.mkdir(exist_ok=True)
    
    # Reservoir order
    reservoir_order = list(CUSTOM_RESERVOIRS.keys())
    
    # Fetch data
    print(f"Fetching data for scenario {args.scenario}...")
    print(f"Reservoirs: {', '.join(reservoir_order)}")
    
    try:
        data = fetch_storage_data(args.api_url, args.scenario, reservoir_order)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return 1
    
    # Print summary
    print_data_summary(data, reservoir_order)
    
    # Generate PDF
    print("\nGenerating PDF report...")
    create_dual_pdf_report(data, args.scenario, output_dir, reservoir_order)
    
    print("\nDone!")
    return 0


if __name__ == '__main__':
    exit(main())
