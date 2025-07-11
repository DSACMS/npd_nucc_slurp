#!/usr/bin/env python3
"""
NUCC Data Comparison Script

This script compares two NUCC taxonomy datasets:
1. Downloaded official NUCC taxonomy CSV
2. Scraped NUCC data from our web scraping process

The script performs an outer join on the taxonomy codes to identify:
- Codes that exist in both datasets
- Codes that exist only in the downloaded dataset
- Codes that exist only in the scraped dataset
"""

import pandas as pd
import argparse
import sys
import os
from pathlib import Path

def load_and_validate_csv(file_path, file_description):
    """Load and validate a CSV file."""
    try:
        if not os.path.exists(file_path):
            print(f"Error: {file_description} file not found at {file_path}")
            return None
        
        # Count actual lines in file for verification
        with open(file_path, 'r') as f:
            total_lines = len(f.readlines())
        
        # Load with pandas
        df = pd.read_csv(file_path)
        
        # Detailed verification output
        print(f"=== {file_description} Import Verification ===")
        print(f"  File path: {file_path}")
        print(f"  Total lines in file (wc -l equivalent): {total_lines}")
        print(f"  Expected data rows (total - header): {total_lines - 1}")
        print(f"  Pandas DataFrame rows loaded: {len(df)}")
        print(f"  ✅ Import verification: {len(df) == total_lines - 1}")
        print(f"  Columns: {list(df.columns)}")
        
        return df
    except Exception as e:
        print(f"Error loading {file_description} from {file_path}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Compare NUCC taxonomy datasets')
    parser.add_argument('--download_csv', required=True, 
                       help='Path to the downloaded official NUCC taxonomy CSV file')
    parser.add_argument('--scrapped_csv', required=True,
                       help='Path to the scraped NUCC data CSV file')
    
    args = parser.parse_args()
    
    # Load the datasets
    print("Loading datasets...")
    download_df = load_and_validate_csv(args.download_csv, "Downloaded NUCC data")
    scraped_df = load_and_validate_csv(args.scrapped_csv, "Scraped NUCC data")
    
    if download_df is None or scraped_df is None:
        sys.exit(1)
    
    # Display basic info about the datasets
    print(f"\nDownloaded dataset columns: {list(download_df.columns)}")
    print(f"Scraped dataset columns: {list(scraped_df.columns)}")
    
    # Verify join columns exist
    if 'Code' not in download_df.columns:
        print("Error: 'Code' column not found in downloaded dataset")
        sys.exit(1)
    
    if 'code_text' not in scraped_df.columns:
        print("Error: 'code_text' column not found in scraped dataset")
        sys.exit(1)
    
    # Clean the join columns (remove whitespace, but preserve NaN/empty values)
    download_df['Code'] = download_df['Code'].astype(str).str.strip()
    scraped_df['code_text'] = scraped_df['code_text'].astype(str).str.strip()
    
    # Convert 'nan' string back to actual NaN for proper handling in merge
    download_df['Code'] = download_df['Code'].replace('nan', pd.NA)
    scraped_df['code_text'] = scraped_df['code_text'].replace('nan', pd.NA)
    
    # Convert empty strings to NaN for consistent handling
    download_df['Code'] = download_df['Code'].replace('', pd.NA)
    scraped_df['code_text'] = scraped_df['code_text'].replace('', pd.NA)
    
    print(f"\nAfter cleaning (preserving ALL rows):")
    print(f"Downloaded dataset: {len(download_df)} rows")
    print(f"Scraped dataset: {len(scraped_df)} rows")
    print(f"Downloaded dataset with valid codes: {download_df['Code'].notna().sum()}")
    print(f"Scraped dataset with valid alphanumeric codes: {scraped_df['code_text'].notna().sum()}")
    print(f"Scraped dataset with blank code_text (parent nodes): {scraped_df['code_text'].isna().sum()}")
    
    # Add prefixes to column names to avoid conflicts (except join columns)
    download_df_renamed = download_df.rename(columns={col: f"download_{col}" for col in download_df.columns if col != 'Code'})
    scraped_df_renamed = scraped_df.rename(columns={col: f"scraped_{col}" for col in scraped_df.columns if col != 'code_text'})
    
    # Perform outer join - this will automatically include ALL rows from both datasets
    # Join on alphanumeric codes: downloaded Code with scraped code_text
    print("\nPerforming outer join on alphanumeric codes...")
    merged_df = pd.merge(
        download_df_renamed, 
        scraped_df_renamed, 
        left_on='Code', 
        right_on='code_text', 
        how='outer',
        suffixes=('_download', '_scraped')
    )
    
    # Create a combined code column for analysis (use code_text for alphanumeric, code_id for parent nodes)
    merged_df['combined_code'] = merged_df['Code'].fillna(merged_df['code_text'])
    
    # Reorder columns to put the combined code first
    cols = ['combined_code'] + [col for col in merged_df.columns if col not in ['combined_code', 'Code', 'code_text']] + ['Code', 'code_text']
    merged_df = merged_df[cols]
    
    # Analysis
    print(f"\nMerge Results:")
    print(f"Total records in merged dataset: {len(merged_df)}")
    
    # Count matches and mismatches
    both_present = merged_df['Code'].notna() & merged_df['code_text'].notna()
    only_in_download = merged_df['Code'].notna() & merged_df['code_text'].isna()
    only_in_scraped = merged_df['Code'].isna() & merged_df['scraped_code_id'].notna()
    
    print(f"Records in both datasets: {both_present.sum()}")
    print(f"Records only in downloaded dataset: {only_in_download.sum()}")
    print(f"Records only in scraped dataset: {only_in_scraped.sum()}")
    
    # Create output directory if it doesn't exist
    output_dir = Path('./data')
    output_dir.mkdir(exist_ok=True)
    
    # Create subsets output directory
    subsets_dir = output_dir / 'subsets_from_merge'
    subsets_dir.mkdir(exist_ok=True)
    
    # Save the merged dataset
    output_file = output_dir / 'merged_nucc_data.csv'
    merged_df.to_csv(output_file, index=False)
    print(f"\nMerged dataset saved to: {output_file}")
    
    # Save subset datasets
    print(f"\nSaving subset datasets to: {subsets_dir}")
    
    # Records that are in both datasets
    both_present_df = merged_df[both_present]
    both_present_file = subsets_dir / 'in_both_datasets.csv'
    both_present_df.to_csv(both_present_file, index=False)
    print(f"Records in both datasets ({len(both_present_df)}): {both_present_file}")
    
    # Records only in downloaded dataset - use original downloaded data only
    only_in_download_codes = merged_df[only_in_download]['Code'].dropna()
    only_in_download_original = download_df_renamed[download_df_renamed['Code'].isin(only_in_download_codes)]
    only_in_download_file = subsets_dir / 'only_in_downloaded.csv'
    only_in_download_original.to_csv(only_in_download_file, index=False)
    print(f"Records only in downloaded dataset ({len(only_in_download_original)}): {only_in_download_file}")
    
    # Records only in scraped dataset - use original scraped data only
    only_in_scraped_codes = merged_df[only_in_scraped]['scraped_code_id'].dropna()
    only_in_scraped_original = scraped_df_renamed[scraped_df_renamed['scraped_code_id'].isin(only_in_scraped_codes)]
    only_in_scraped_file = subsets_dir / 'only_in_scrapped.csv'
    only_in_scraped_original.to_csv(only_in_scraped_file, index=False)
    print(f"Records only in scraped dataset ({len(only_in_scraped_original)}): {only_in_scraped_file}")
    
    # Create summary report
    summary_file = output_dir / 'nucc_comparison_summary.txt'
    with open(summary_file, 'w') as f:
        f.write("NUCC Data Comparison Summary\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Downloaded dataset: {args.download_csv}\n")
        f.write(f"Scraped dataset: {args.scrapped_csv}\n\n")
        f.write(f"Total records in merged dataset: {len(merged_df)}\n")
        f.write(f"Records in both datasets: {both_present.sum()}\n")
        f.write(f"Records only in downloaded dataset: {only_in_download.sum()}\n")
        f.write(f"Records only in scraped dataset: {only_in_scraped.sum()}\n\n")
        
        if only_in_download.sum() > 0:
            f.write("Sample codes only in downloaded dataset:\n")
            sample_download_only = merged_df[only_in_download]['Code'].head(10).tolist()
            for code in sample_download_only:
                f.write(f"  - {code}\n")
            f.write("\n")
            
        if only_in_scraped.sum() > 0:
            f.write("Sample codes only in scraped dataset:\n")
            sample_scraped_only = merged_df[only_in_scraped]['scraped_code_id'].head(10).tolist()
            for code in sample_scraped_only:
                f.write(f"  - {code}\n")
            f.write("\n")
    
    print(f"Summary report saved to: {summary_file}")
    
    # Validate that all codes from nucc_parent_code.csv are present in merged data
    print("\nValidating code coverage...")
    parent_codes_file = Path('./data/nucc_parent_code.csv')
    
    if parent_codes_file.exists():
        try:
            parent_codes_df = pd.read_csv(parent_codes_file)
            
            # Get all unique code IDs from both ancestor and child columns
            ancestor_codes = set(parent_codes_df['ancestor_nucc_code_id'].dropna().astype(str))
            child_codes = set(parent_codes_df['child_nucc_code_id'].dropna().astype(str))
            all_parent_codes = ancestor_codes.union(child_codes)
            
            print(f"Total unique codes in nucc_parent_code.csv: {len(all_parent_codes)}")
            
            # Get all codes present in merged data (from both download and scraped datasets)
            merged_codes = set()
            
            # Add codes from download dataset
            if 'Code' in merged_df.columns:
                download_codes = set(merged_df['Code'].dropna().astype(str))
                merged_codes.update(download_codes)
            
            # Add codes from scraped dataset  
            if 'scraped_code_id' in merged_df.columns:
                scraped_codes = set(merged_df['scraped_code_id'].dropna().astype(int).astype(str))
                merged_codes.update(scraped_codes)
            
            # Add codes from combined_code column
            if 'combined_code' in merged_df.columns:
                combined_codes = set(merged_df['combined_code'].dropna().astype(str))
                merged_codes.update(combined_codes)
            
            print(f"Total unique codes in merged data: {len(merged_codes)}")
            
            # Find missing codes
            missing_codes = all_parent_codes - merged_codes
            
            if missing_codes:
                print(f"\n⚠️  WARNING: {len(missing_codes)} codes from nucc_parent_code.csv are missing from merged data:")
                sorted_missing = sorted(missing_codes, key=lambda x: int(x) if x.isdigit() else float('inf'))
                for i, code in enumerate(sorted_missing[:20]):  # Show first 20
                    print(f"  - {code}")
                if len(missing_codes) > 20:
                    print(f"  ... and {len(missing_codes) - 20} more")
                    
                # Write missing codes to file
                missing_codes_file = output_dir / 'missing_codes.txt'
                with open(missing_codes_file, 'w') as f:
                    f.write("Codes from nucc_parent_code.csv missing from merged data:\n")
                    f.write("=" * 50 + "\n\n")
                    for code in sorted_missing:
                        f.write(f"{code}\n")
                print(f"\nComplete list of missing codes saved to: {missing_codes_file}")
            else:
                print("\n✅ All codes from nucc_parent_code.csv are present in merged data!")
            
            # Find extra codes (codes in merged data but not in parent codes)
            extra_codes = merged_codes - all_parent_codes
            if extra_codes:
                print(f"\nℹ️  {len(extra_codes)} additional codes found in merged data that are not in nucc_parent_code.csv")
                if len(extra_codes) <= 10:
                    sorted_extra = sorted(extra_codes, key=lambda x: int(x) if x.isdigit() else float('inf'))
                    for code in sorted_extra:
                        print(f"  - {code}")
                        
        except Exception as e:
            print(f"Error validating code coverage: {e}")
    else:
        print(f"Warning: {parent_codes_file} not found - skipping code coverage validation")
    
    # Display first few rows of merged data for verification
    print("\nFirst 5 rows of merged data:")
    print(merged_df.head())
    
    print("\nComparison completed successfully!")

if __name__ == "__main__":
    main()
