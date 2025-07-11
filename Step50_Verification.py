#!/usr/bin/env python3
"""
Step50_Verification.py

Verifies that specific node lineages and their ancestors are present and correct in the ETL output,
using two approaches as described in AI_instructions/verification.md.

- Approach 1: Uses data/nucc_parent_code.csv joined with data/merged_nucc_data.csv.
- Approach 2: Uses only data/merged_nucc_data.csv and its parent pointers.

All results are printed to stdout.
"""

import pandas as pd
import sys
import os

# Define the lineage test cases (from AI_instructions/verification.md)
# Each lineage is a list from leaf to root, using codes or human-readable names.
LINEAGES = [
    # "261QA1903X" -> "261Q00000X" -> "Ambulatory Health Care Facilities" -> "Non-individual"
    ["261QA1903X", "261Q00000X", "Ambulatory Health Care Facilities", "Non-individual"],
    # "273100000X" -> "Hospital Units" -> "Non-individual"
    ["273100000X", "Hospital Units", "Non-individual"],
    # "281PC2000X" -> "281P00000X" -> Hospitals ->  "Non-individual"
    ["281PC2000X", "281P00000X", "Hospitals", "Non-individual"],
    # "207NP0225X" -> "207N00000X" -> "Allopathic & Osteopathic Physicians" -> "Individual or Groups (of Individuals)"
    ["207NP0225X", "207N00000X", "Allopathic & Osteopathic Physicians", "Individual or Groups (of Individuals)"],
    # "101YM0800X" -> "101Y00000X" -> "Behavioral Health & Social Service Providers" -> "Individual or Groups (of Individuals)"
    ["101YM0800X", "101Y00000X", "Behavioral Health & Social Service Providers", "Individual or Groups (of Individuals)"],
]

# Helper: Map a code or description to its scraped_code_id
def get_code_id(node, merged_df):
    """
    node: str, either a 10-digit code or a human-readable name
    merged_df: DataFrame of merged_nucc_data.csv
    Returns: scraped_code_id (int) or None if not found
    """
    if isinstance(node, str) and len(node) == 10 and node.isalnum():
        # 10-digit code: use combined_code
        row = merged_df.loc[merged_df['combined_code'] == node]
    else:
        # Otherwise, use scraped_code_short_name
        row = merged_df.loc[merged_df['scraped_code_short_name'] == node]
    if not row.empty:
        return int(row.iloc[0]['scraped_code_id'])
    return None

# Helper: Map a code_id back to code or description for reporting
def get_node_name(code_id, merged_df):
    row = merged_df.loc[merged_df['scraped_code_id'] == code_id]
    if not row.empty:
        code = row.iloc[0]['combined_code']
        name = row.iloc[0]['scraped_code_short_name']
        return f"{code} ({name})"
    return f"Unknown code_id {code_id}"

def verify_lineage_approach1(lineage, merged_df, parent_df):
    """
    Approach 1: Use nucc_parent_code.csv and merged_nucc_data.csv to verify lineage.
    Returns: (success: bool, details: str)
    """
    # Map all nodes in lineage to code_ids
    code_ids = []
    for node in lineage:
        code_id = get_code_id(node, merged_df)
        if code_id is None:
            return False, f"Node '{node}' not found in merged_nucc_data.csv"
        code_ids.append(code_id)
    # For each child-parent pair, check if parent-child exists in parent_df
    for i in range(len(code_ids) - 1):
        parent_id = code_ids[i+1]
        child_id = code_ids[i]
        match = parent_df[
            (parent_df['ancestor_nucc_code_id'] == parent_id) &
            (parent_df['child_nucc_code_id'] == child_id)
        ]
        if match.empty:
            parent_name = get_node_name(parent_id, merged_df)
            child_name = get_node_name(child_id, merged_df)
            return False, f"Parent-child relationship missing: {parent_name} -> {child_name}"
    return True, "All parent-child relationships found"

def verify_lineage_approach2(lineage, merged_df):
    """
    Approach 2: Use only merged_nucc_data.csv and parent pointers.
    Returns: (success: bool, details: str)
    """
    # Map all nodes in lineage to code_ids
    code_ids = []
    for node in lineage:
        code_id = get_code_id(node, merged_df)
        if code_id is None:
            return False, f"Node '{node}' not found in merged_nucc_data.csv"
        code_ids.append(code_id)
    # Walk up the lineage from leaf to root, checking parent pointers
    for i in range(len(code_ids) - 1):
        child_id = code_ids[i]
        expected_parent_id = code_ids[i+1]
        row = merged_df.loc[merged_df['scraped_code_id'] == child_id]
        if row.empty:
            return False, f"Child code_id {child_id} not found in merged_nucc_data.csv"
        actual_parent_id = row.iloc[0]['scraped_immediate_parent_code_id']
        if pd.isna(actual_parent_id):
            return False, f"Child {get_node_name(child_id, merged_df)} has no parent"
        if int(actual_parent_id) != expected_parent_id:
            return False, (
                f"Child {get_node_name(child_id, merged_df)} expected parent {get_node_name(expected_parent_id, merged_df)}, "
                f"but found {get_node_name(actual_parent_id, merged_df)}"
            )
    return True, "All parent pointers correct"

def main():
    # Load data
    merged_path = os.path.join("data", "merged_nucc_data.csv")
    parent_path = os.path.join("data", "nucc_parent_code.csv")
    try:
        merged_df = pd.read_csv(merged_path, dtype=str)
        # Ensure numeric columns are int where needed
        merged_df['scraped_code_id'] = pd.to_numeric(merged_df['scraped_code_id'], errors='coerce')
        merged_df['scraped_immediate_parent_code_id'] = pd.to_numeric(merged_df['scraped_immediate_parent_code_id'], errors='coerce')
    except Exception as e:
        print(f"Failed to load {merged_path}: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        parent_df = pd.read_csv(parent_path, dtype=str)
        parent_df['ancestor_nucc_code_id'] = pd.to_numeric(parent_df['ancestor_nucc_code_id'], errors='coerce')
        parent_df['child_nucc_code_id'] = pd.to_numeric(parent_df['child_nucc_code_id'], errors='coerce')
    except Exception as e:
        print(f"Failed to load {parent_path}: {e}", file=sys.stderr)
        sys.exit(1)

    print("=== NUCC Lineage Verification ===\n")
    for idx, lineage in enumerate(LINEAGES, 1):
        print(f"Lineage {idx}: {' -> '.join(lineage)}")
        # Approach 1
        ok1, details1 = verify_lineage_approach1(lineage, merged_df, parent_df)
        print(f"  Approach 1 (parent_code.csv): {'PASS' if ok1 else 'FAIL'} - {details1}")
        # Approach 2
        ok2, details2 = verify_lineage_approach2(lineage, merged_df)
        print(f"  Approach 2 (parent pointers): {'PASS' if ok2 else 'FAIL'} - {details2}")

    print("Verification complete.")

if __name__ == "__main__":
    main()
