#!/usr/bin/env python3
"""
Analyze validation results across multiple scenarios
Downloads and analyzes validation reports from S3
"""
import boto3
import json
import pandas as pd
import argparse
import sys
from datetime import datetime
import io

def download_validation_summary(bucket_name, scenario_id):
    """Download validation summary from S3"""
    
    s3_client = boto3.client('s3')
    summary_key = f"scenario/{scenario_id}/validation/{scenario_id}_validation_summary.json"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=summary_key)
        summary = json.loads(response['Body'].read())
        return summary
    except Exception as e:
        print(f"❌ Could not download summary for {scenario_id}: {e}")
        return None

def download_validation_mismatches(bucket_name, scenario_id):
    """Download validation mismatches CSV from S3"""
    
    s3_client = boto3.client('s3')
    csv_key = f"scenario/{scenario_id}/validation/{scenario_id}_validation_mismatches.csv"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        return df
    except Exception as e:
        print(f"⚠️  Could not download mismatches for {scenario_id}: {e}")
        return None

def analyze_scenario_results(bucket_name, scenarios):
    """Analyze validation results across scenarios"""
    
    print(f"📊 Analyzing validation results for {len(scenarios)} scenarios...")
    
    results = []
    all_mismatches = []
    
    for scenario_id in scenarios:
        print(f"   📄 Processing {scenario_id}...")
        
        # Download summary
        summary = download_validation_summary(bucket_name, scenario_id)
        if not summary:
            continue
        
        # Add to results
        result = {
            'scenario_id': scenario_id,
            'status': summary['status'],
            'columns_common': summary['columns_common'],
            'mismatch_columns': summary['mismatch_columns'],
            'mismatch_cells': summary['mismatch_cells'],
            'rows_in_overlap': summary['rows_in_overlap'],
            'overlap_start': summary['overlap_start'],
            'overlap_end': summary['overlap_end']
        }
        results.append(result)
        
        # Download mismatches if validation failed
        if summary['status'] != 'PASSED':
            mismatches_df = download_validation_mismatches(bucket_name, scenario_id)
            if mismatches_df is not None and not mismatches_df.empty:
                mismatches_df['scenario_id'] = scenario_id
                all_mismatches.append(mismatches_df)
    
    return results, all_mismatches

def generate_summary_report(results, output_file=None):
    """Generate summary report"""
    
    if not results:
        print("❌ No results to analyze")
        return
    
    df = pd.DataFrame(results)
    
    # Calculate statistics
    total_scenarios = len(df)
    passed_scenarios = len(df[df['status'] == 'PASSED'])
    failed_scenarios = total_scenarios - passed_scenarios
    
    success_rate = (passed_scenarios / total_scenarios * 100) if total_scenarios > 0 else 0
    
    # Print summary
    print("\n" + "="*60)
    print("📊 VALIDATION SUMMARY REPORT")
    print("="*60)
    print(f"Total scenarios tested: {total_scenarios}")
    print(f"Passed validations: {passed_scenarios}")
    print(f"Failed validations: {failed_scenarios}")
    print(f"Success rate: {success_rate:.1f}%")
    print()
    
    if failed_scenarios > 0:
        print("❌ FAILED SCENARIOS:")
        failed_df = df[df['status'] != 'PASSED']
        for _, row in failed_df.iterrows():
            print(f"   {row['scenario_id']}: {row['mismatch_cells']} mismatched cells")
        print()
    
    # Show detailed statistics
    print("📈 DETAILED STATISTICS:")
    print(f"   Average columns per scenario: {df['columns_common'].mean():.1f}")
    print(f"   Average rows compared: {df['rows_in_overlap'].mean():.0f}")
    
    if df['mismatch_cells'].sum() > 0:
        print(f"   Total mismatched cells: {df['mismatch_cells'].sum()}")
        print(f"   Average mismatches per failed scenario: {df[df['mismatch_cells'] > 0]['mismatch_cells'].mean():.1f}")
    
    # Save to file if requested
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"📄 Detailed results saved to: {output_file}")

def generate_mismatch_analysis(all_mismatches, output_file=None):
    """Analyze patterns in mismatches"""
    
    if not all_mismatches:
        print("✅ No mismatches to analyze!")
        return
    
    # Combine all mismatches
    combined_df = pd.concat(all_mismatches, ignore_index=True)
    
    print("\n" + "="*60)
    print("🔍 MISMATCH PATTERN ANALYSIS")
    print("="*60)
    
    # Most common mismatch variables
    print("Top variables with mismatches:")
    variable_counts = combined_df['C'].value_counts().head(10)
    for var, count in variable_counts.items():
        print(f"   {var}: {count} mismatches")
    print()
    
    # Most common mismatch locations
    print("Top locations (B parts) with mismatches:")
    location_counts = combined_df['B'].value_counts().head(10)
    for loc, count in location_counts.items():
        print(f"   {loc}: {count} mismatches")
    print()
    
    # Scenarios with most mismatches
    print("Scenarios with most mismatches:")
    scenario_counts = combined_df['scenario_id'].value_counts()
    for scenario, count in scenario_counts.items():
        print(f"   {scenario}: {count} mismatches")
    print()
    
    # Magnitude analysis
    combined_df['abs_diff_numeric'] = pd.to_numeric(combined_df['abs_diff'], errors='coerce')
    if not combined_df['abs_diff_numeric'].isna().all():
        print("Mismatch magnitude statistics:")
        print(f"   Mean absolute difference: {combined_df['abs_diff_numeric'].mean():.6f}")
        print(f"   Median absolute difference: {combined_df['abs_diff_numeric'].median():.6f}")
        print(f"   Max absolute difference: {combined_df['abs_diff_numeric'].max():.6f}")
    
    # Save to file if requested
    if output_file:
        combined_df.to_csv(output_file, index=False)
        print(f"📄 Mismatch details saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Analyze DSS validation results from AWS"
    )
    parser.add_argument("--bucket", required=True,
                       help="S3 bucket containing validation results")
    parser.add_argument("--scenarios", nargs='+', required=True,
                       help="List of scenario IDs to analyze")
    parser.add_argument("--summary-output", 
                       help="Output file for summary CSV")
    parser.add_argument("--mismatch-output",
                       help="Output file for detailed mismatches CSV")
    parser.add_argument("--generate-report", action="store_true",
                       help="Generate comprehensive report")
    
    args = parser.parse_args()
    
    # Analyze results
    results, all_mismatches = analyze_scenario_results(args.bucket, args.scenarios)
    
    if not results:
        print("❌ No validation results found")
        return 1
    
    # Generate summary
    generate_summary_report(results, args.summary_output)
    
    # Analyze mismatches
    if args.generate_report:
        generate_mismatch_analysis(all_mismatches, args.mismatch_output)
    
    # Final recommendation
    passed_count = len([r for r in results if r['status'] == 'PASSED'])
    total_count = len(results)
    
    print("\n" + "="*60)
    print("🎯 RECOMMENDATION")
    print("="*60)
    
    if passed_count == total_count:
        print("✅ ALL VALIDATIONS PASSED!")
        print("   Your Python DSS extractor produces identical results to the Java extractor.")
        print("   Recommendation: Proceed with confidence using Python extractor as single source of truth.")
    elif passed_count / total_count >= 0.9:
        print("🟡 MOSTLY SUCCESSFUL VALIDATION")
        print(f"   {passed_count}/{total_count} scenarios passed validation.")
        print("   Recommendation: Review failed scenarios for specific issues, but overall confidence is high.")
    else:
        print("🔴 VALIDATION ISSUES DETECTED")
        print(f"   Only {passed_count}/{total_count} scenarios passed validation.")
        print("   Recommendation: Investigate systematic differences before proceeding.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
