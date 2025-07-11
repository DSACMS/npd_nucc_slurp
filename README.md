# NUCC Slurp

A comprehensive toolkit for scraping and analyzing NUCC (National Uniform Claim Committee) taxonomy data from the official taxonomy website.

## Overview

This project provides a complete pipeline for extracting, processing, and analyzing NUCC taxonomy codes and their hierarchical relationships. The NUCC taxonomy is used to classify healthcare provider types and specialties in the United States.

## Scripts and Execution Order

The scripts should be executed in the following order:

### 1. `scrape_nucc_ancestors.py`
**Purpose**: Scrapes the main NUCC taxonomy website to extract hierarchical relationships between codes.

**What it does**:
- Fetches HTML from https://taxonomy.nucc.org/
- Parses the JavaScript treenodes data structure
- Extracts all ancestor-child relationships in the taxonomy hierarchy
- Creates self-referencing relationships (each code is its own ancestor)

**Output**: `data/nucc_parent_code.csv` with columns:
- `ancestor_nucc_code_id`: The ancestor code ID
- `child_nucc_code_id`: The child code ID

**Usage**:
```bash
python3 scrape_nucc_ancestors.py
```

### 2. `scrape_nucc_nodes.py`
**Purpose**: Scrapes detailed information for each individual taxonomy code from the NUCC API.

**What it does**:
- Reads all unique node IDs from `data/nucc_parent_code.csv`
- Downloads detailed information for each node from the NUCC API
- Parses HTML content to extract structured data (name, definition, notes, etc.)
- Caches HTML snippets in `data/tables/` for analysis
- Uses intelligent caching to avoid re-downloading recently fetched data

**Output**: 
- `data/nucc_codes.csv` with detailed code information
- `data/tables/node_*.html` files containing raw HTML snippets

**Usage**:
```bash
python3 scrape_nucc_nodes.py
```

### 3. `parse_nucc_sources.py`
**Purpose**: Extracts and structures source information from the notes column of the NUCC codes.

**What it does**:
- Parses the `code_notes` column from `data/nucc_codes.csv`
- Extracts source citations that follow the pattern "Source: text [date: note]"
- Automatically extracts URLs from source text
- Handles multiple sources per code
- Creates normalized source records

**Output**: `data/nucc_sources.csv` with columns:
- `nucc_code_id`: The NUCC code ID
- `full_source_text`: Complete source text
- `source_date`: Date from source citation
- `source_date_note`: Note from source citation
- `extracted_urls`: URLs found in source text

**Usage**:
```bash
python3 parse_nucc_sources.py
```

### 4. `compare_nucc_data.py`
**Purpose**: Compares scraped data with official NUCC taxonomy CSV files to identify differences.

**What it does**:
- Loads both the scraped data and an official NUCC taxonomy CSV
- Performs outer join on taxonomy codes
- Identifies codes that exist in only one dataset
- Creates a merged dataset with all available information
- Generates summary statistics and reports

**Output**:
- `data/merged_nucc_data.csv`: Combined dataset from both sources
- `data/nucc_comparison_summary.txt`: Summary report of differences

**Usage**:
```bash
python3 compare_nucc_data.py --download_csv /path/to/official/nucc_taxonomy.csv --scrapped_csv ./data/nucc_codes.csv
```

## Data Files Generated

- `data/nucc_parent_code.csv`: Hierarchical relationships between codes
- `data/nucc_codes.csv`: Detailed information for each taxonomy code
- `data/nucc_sources.csv`: Structured source information
- `data/merged_nucc_data.csv`: Comparison between scraped and official data
- `data/nucc_comparison_summary.txt`: Summary of data comparison
- `data/tables/`: Directory containing raw HTML snippets for each code

## Requirements

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Features

- **Intelligent Caching**: Avoids re-downloading recently fetched data
- **Robust Error Handling**: Gracefully handles network issues and parsing errors
- **Future-Proof Parsing**: Automatically detects and includes new data fields
- **URL Extraction**: Automatically extracts and normalizes URLs from source text
- **Data Validation**: Includes data cleaning and validation steps
