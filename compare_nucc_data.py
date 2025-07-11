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
        
        df = pd.read_csv(file_path)
        print(f"Loaded {file_description}: {len(df)} rows")
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
    
    # Clean the join columns (remove whitespace, handle NaN values)
    download_df['Code'] = download_df['Code'].astype(str).str.strip()
    scraped_df['code_text'] = scraped_df['code_text'].astype(str).str.strip()
    
    # Remove rows where the join column is empty or 'nan'
    download_df = download_df[download_df['Code'].notna() & (download_df['Code'] != '') & (download_df['Code'] != 'nan')]
    scraped_df = scraped_df[scraped_df['code_text'].notna() & (scraped_df['code_text'] != '') & (scraped_df['code_text'] != 'nan')]
    
    print(f"\nAfter cleaning:")
    print(f"Downloaded dataset: {len(download_df)} rows with valid codes")
    print(f"Scraped dataset: {len(scraped_df)} rows with valid codes")
    
    # Add prefixes to column names to avoid conflicts (except join columns)
    download_df_renamed = download_df.rename(columns={col: f"download_{col}" for col in download_df.columns if col != 'Code'})
    scraped_df_renamed = scraped_df.rename(columns={col: f"scraped_{col}" for col in scraped_df.columns if col != 'code_text'})
    
    # Perform outer join
    print("\nPerforming outer join...")
    merged_df = pd.merge(
        download_df_renamed, 
        scraped_df_renamed, 
        left_on='Code', 
        right_on='code_text', 
        how='outer',
        suffixes=('_download', '_scraped')
    )
    
    # Create a combined code column for analysis
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
    only_in_scraped = merged_df['Code'].isna() & merged_df['code_text'].notna()
    
    print(f"Records in both datasets: {both_present.sum()}")
    print(f"Records only in downloaded dataset: {only_in_download.sum()}")
    print(f"Records only in scraped dataset: {only_in_scraped.sum()}")
    
    # Create output directory if it doesn't exist
    output_dir = Path('./data')
    output_dir.mkdir(exist_ok=True)
    
    # Save the merged dataset
    output_file = output_dir / 'merged_nucc_data.csv'
    merged_df.to_csv(output_file, index=False)
    print(f"\nMerged dataset saved to: {output_file}")
    
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
            sample_scraped_only = merged_df[only_in_scraped]['code_text'].head(10).tolist()
            for code in sample_scraped_only:
                f.write(f"  - {code}\n")
            f.write("\n")
    
    print(f"Summary report saved to: {summary_file}")
    
    # Display first few rows of merged data for verification
    print("\nFirst 5 rows of merged data:")
    print(merged_df.head())
    
    print("\nComparison completed successfully!")

if __name__ == "__main__":
    main()
