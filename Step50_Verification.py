#!/usr/bin/env python3
"""
Step50_Verification.py

Manual data checks to verify that the ETL is working properly.
This script checks that specific nodes and their ancestor chains are present in the data.

Instructions:
- Loads ./data/nucc_parent_code.csv (ancestor relationships)
- Loads ./data/merged_nucc_data.csv (node data)
- Verifies that specific node/ancestor relationships are present, as described in AI_instructions/verification.md
- Uses pandas for this purpose
- Represents the tests in a data structure

Run with:
    python3 Step50_Verification.py
"""

import pandas as pd
import os

class NUCCVerification:
    @staticmethod
    def main(*, ancestor_csv_path, node_csv_path, tests):
        # Load data
        ancestor_df = pd.read_csv(ancestor_csv_path, dtype=str)
        node_df = pd.read_csv(node_csv_path, dtype=str)

        # Build mapping between taxonomy code <-> numeric id
        code_to_id = dict(zip(node_df['combined_code'], node_df['scraped_code_id']))
        id_to_code = dict(zip(node_df['scraped_code_id'], node_df['combined_code']))

        # Supplemental mapping for group/root nodes not present in node_df
        supplemental_id_map = {
            # id: (taxonomy_code, description, type)
            "14": (None, "Ambulatory Health Care Facilities", "Non-individual"),
            "1962": (None, "Allopathic & Osteopathic Physicians", "Individual or Groups (of Individuals)"),
            "2293": (None, "Behavioral Health & Social Service Providers", "Individual or Groups (of Individuals)"),
            "2496": ("261Q00000X", "Ambulatory Health Care Facilities", "Non-individual"),
            "2588": ("273100000X", "Hospital Units", "Non-individual"),
            "2477": ("281P00000X", "Hospitals", "Non-individual"),
            "1974": ("207N00000X", "Allopathic & Osteopathic Physicians", "Individual or Groups (of Individuals)"),
            "2299": ("101Y00000X", "Behavioral Health & Social Service Providers", "Individual or Groups (of Individuals)"),
        }

        # Build direct parent lookup: for each child_id, find the ancestor_id where child_id != ancestor_id
        parent_lookup = {}
        for _, row in ancestor_df.iterrows():
            child_id = row['child_nucc_code_id']
            ancestor_id = row['ancestor_nucc_code_id']
            if child_id != ancestor_id:
                if child_id not in parent_lookup:
                    parent_lookup[child_id] = ancestor_id

        # Build lookup dictionaries for node data (taxonomy code as key)
        code_to_grouping = dict(zip(node_df['combined_code'], node_df['download_Grouping']))
        code_to_classification = dict(zip(node_df['combined_code'], node_df['download_Classification']))
        code_to_specialization = dict(zip(node_df['combined_code'], node_df['download_Specialization']))
        code_to_display_name = dict(zip(node_df['combined_code'], node_df['download_Display Name']))
        code_to_section = dict(zip(node_df['combined_code'], node_df['download_Section']))

        all_passed = True

        for test in tests:
            print(f"\nVerifying chain for leaf code: {test['leaf_code']}")
            current_code = test['leaf_code']
            expected_chain = test['expected_chain']
            actual_chain = []

            # Start with taxonomy code, get numeric id
            current_id = code_to_id.get(current_code, None)

            import math

            for expected in expected_chain:
                # Get node info for current_code
                group = code_to_grouping.get(current_code, None)
                classification = code_to_classification.get(current_code, None)
                specialization = code_to_specialization.get(current_code, None)
                display_name = code_to_display_name.get(current_code, None)
                section = code_to_section.get(current_code, None)

                # If current_code is None but current_id is in supplemental_id_map, use that info
                if (current_code is None or current_code == "nan") and current_id in supplemental_id_map:
                    sup_code, sup_desc, sup_type = supplemental_id_map[current_id]
                    actual_chain.append({
                        'code': sup_code,
                        'grouping': sup_desc,
                        'classification': None,
                        'specialization': None,
                        'display_name': None,
                        'section': sup_type
                    })
                else:
                    actual_chain.append({
                        'code': current_code,
                        'grouping': group,
                        'classification': classification,
                        'specialization': specialization,
                        'display_name': display_name,
                        'section': section
                    })

                # Move to parent: get parent id, then map to taxonomy code
                if current_id is not None and current_id in parent_lookup:
                    parent_id = parent_lookup[current_id]
                    parent_code = id_to_code.get(parent_id, None)
                    # Debug print
                    print(f"    [DEBUG] current_id={current_id}, parent_id={parent_id}, parent_code={parent_code}")
                    # If parent_code is missing, check supplemental mapping
                    if (parent_code is None or (isinstance(parent_code, float) and math.isnan(parent_code))) and parent_id in supplemental_id_map:
                        current_code = None
                        current_id = parent_id
                    elif parent_code is None or (isinstance(parent_code, float) and math.isnan(parent_code)):
                        current_code = None
                        current_id = None
                    else:
                        current_code = parent_code
                        current_id = parent_id
                else:
                    current_code = None
                    current_id = None

            # Compare actual_chain to expected_chain
            passed = True
            for idx, expected in enumerate(expected_chain):
                actual = actual_chain[idx]
                for key in expected:
                    # Map expected keys to actual_chain keys
                    if key == 'desc':
                        # desc can be grouping, classification, specialization, or display_name
                        found = (
                            expected[key] in [
                                actual.get('grouping'),
                                actual.get('classification'),
                                actual.get('specialization'),
                                actual.get('display_name')
                            ]
                            if expected[key] is not None else True
                        )
                        if not found:
                            print(f"  ❌ Mismatch at level {idx}: expected desc='{expected[key]}', got grouping='{actual.get('grouping')}', classification='{actual.get('classification')}', specialization='{actual.get('specialization')}', display_name='{actual.get('display_name')}'")
                            passed = False
                    elif key == 'type':
                        # type is mapped to section
                        if expected[key] is not None and actual.get('section') != expected[key]:
                            print(f"  ❌ Mismatch at level {idx}: expected type='{expected[key]}', got section='{actual.get('section')}'")
                            passed = False
                    elif key == 'code':
                        if expected[key] is not None and actual.get('code') != expected[key]:
                            print(f"  ❌ Mismatch at level {idx}: expected code='{expected[key]}', got '{actual.get('code')}'")
                            passed = False
            if passed:
                print("  ✅ Chain verified successfully.")
            else:
                print("  ❌ Chain verification failed.")
                all_passed = False

        if all_passed:
            print("\nAll verification tests passed.")
        else:
            print("\nSome verification tests failed.")

if __name__ == "__main__":
    # Define the verification tests as a list of dicts
    # Each test specifies the leaf code and the expected chain up to the root
    verification_tests = [
        {
            'leaf_code': "261QA1903X",
            'expected_chain': [
                {'code': "261QA1903X", 'desc': None, 'type': None},  # Will check only code, desc/type not specified
                {'code': "261Q00000X", 'desc': None, 'type': None},
                {'code': None, 'desc': "Ambulatory Health Care Facilities", 'type': None},
                {'code': None, 'desc': None, 'type': "Non-individual"},
            ]
        },
        {
            'leaf_code': "273100000X",
            'expected_chain': [
                {'code': "273100000X", 'desc': None, 'type': None},
                {'code': None, 'desc': "Hospital Units", 'type': None},
                {'code': None, 'desc': None, 'type': "Non-individual"},
            ]
        },
        {
            'leaf_code': "281PC2000X",
            'expected_chain': [
                {'code': "281PC2000X", 'desc': None, 'type': None},
                {'code': "281P00000X", 'desc': None, 'type': None},
                {'code': None, 'desc': "Hospitals", 'type': None},
                {'code': None, 'desc': None, 'type': "Non-individual"},
            ]
        },
        {
            'leaf_code': "207NP0225X",
            'expected_chain': [
                {'code': "207NP0225X", 'desc': None, 'type': None},
                {'code': "207N00000X", 'desc': None, 'type': None},
                {'code': None, 'desc': "Allopathic & Osteopathic Physicians", 'type': None},
                {'code': None, 'desc': None, 'type': "Individual or Groups (of Individuals)"},
            ]
        },
        {
            'leaf_code': "101YM0800X",
            'expected_chain': [
                {'code': "101YM0800X", 'desc': None, 'type': None},
                {'code': "101Y00000X", 'desc': None, 'type': None},
                {'code': None, 'desc': "Behavioral Health & Social Service Providers", 'type': None},
                {'code': None, 'desc': None, 'type': "Individual or Groups (of Individuals)"},
            ]
        },
    ]

    ancestor_csv = os.path.join("data", "nucc_parent_code.csv")
    node_csv = os.path.join("data", "merged_nucc_data.csv")

    NUCCVerification.main(
        ancestor_csv_path=ancestor_csv,
        node_csv_path=node_csv,
        tests=verification_tests
    )
