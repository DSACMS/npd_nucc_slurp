#!/usr/bin/env python3
"""
NUCC Nodes Scraper

This script reads the node IDs from nucc_parent_code.csv and scrapes detailed
information for each node from the NUCC taxonomy API endpoints.
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import os
from typing import Dict, List, Set, Optional
import time
from datetime import datetime, timedelta
import re

class NUCCNodesScraper:
    """Scraper for individual NUCC taxonomy node details."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
    def load_node_ids(self, csv_file_path: str) -> Set[str]:
        """Load unique node IDs from the parent code CSV file."""
        print(f"Loading node IDs from {csv_file_path}...")
        
        node_ids = set()
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Add both ancestor and child IDs
                    node_ids.add(row['ancestor_nucc_code_id'])
                    node_ids.add(row['child_nucc_code_id'])
                    
            print(f"Found {len(node_ids)} unique node IDs")
            return node_ids
            
        except FileNotFoundError:
            print(f"Error: Could not find {csv_file_path}")
            print("Please run scrape_nucc_ancestors.py first to generate the parent code CSV")
            raise
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            raise
    
    def download_node_data(self, node_id: str) -> Optional[Dict]:
        """Download JSON data for a specific node ID."""
        url = f"https://taxonomy.nucc.org/Default/GetContentByItemId/{node_id}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            return data
            
        except requests.RequestException as e:
            print(f"Error downloading data for node {node_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON for node {node_id}: {e}")
            return None
    
    def save_html_snippet(self, node_id: str, html_content: str, tables_dir: str):
        """Save HTML snippet to file for analysis."""
        os.makedirs(tables_dir, exist_ok=True)
        
        file_path = os.path.join(tables_dir, f"node_{node_id}.html")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def is_html_file_fresh(self, node_id: str, tables_dir: str, max_age_days: int = 1) -> bool:
        """Check if HTML file exists and was modified within the last day."""
        file_path = os.path.join(tables_dir, f"node_{node_id}.html")
        
        if not os.path.exists(file_path):
            return False
        
        # Check file modification time
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        cutoff_time = datetime.now() - timedelta(days=max_age_days)
        
        return file_mtime > cutoff_time
    
    def load_cached_html(self, node_id: str, tables_dir: str) -> Optional[str]:
        """Load HTML content from cached file."""
        file_path = os.path.join(tables_dir, f"node_{node_id}.html")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"Error loading cached HTML for node {node_id}: {e}")
            return None
    
    def categorize_nodes(self, node_ids: Set[str], tables_dir: str) -> tuple[Set[str], Set[str]]:
        """Categorize nodes into fresh (cached) and stale (need download)."""
        fresh_nodes = set()
        stale_nodes = set()
        
        for node_id in node_ids:
            if self.is_html_file_fresh(node_id, tables_dir):
                fresh_nodes.add(node_id)
            else:
                stale_nodes.add(node_id)
        
        return fresh_nodes, stale_nodes
    
    def parse_node_html(self, node_id: str, html_content: str) -> Dict[str, str]:
        """Parse HTML content and extract structured data."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize result dict
        result = {
            'code_id': node_id,
            'code_text': '',
            'code_long_name': '',
            'code_short_name': '',
            'code_definition': '',
            'code_notes': '',
            'code_effective_date': '',
            'last_modified_date': ''
        }
        
        # Extract h1 title (provider_type_name / code_long_name)
        h1_element = soup.find('h1')
        if h1_element:
            result['code_long_name'] = h1_element.get_text().strip()
        
        # Extract table data
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')
            
            # Dictionary to store extra fields we find
            extra_fields = {}
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text().strip()
                    value = cells[1].get_text().strip()
                    value = re.sub(r'(?i)<br\s*/?>', '\n', value) # remove html br variants.
                    
                    # Map known fields
                    if key.lower() == 'name':
                        result['code_short_name'] = value
                    elif key.lower() == 'code':
                        result['code_text'] = value
                    elif key.lower() in ['definition', 'description']:
                        result['code_definition'] = value
                    elif key.lower() in ['notes', 'note']:
                        result['code_notes'] = value
                    elif key.lower() in ['effective date', 'effectivedate']:
                        result['code_effective_date'] = value
                    elif key.lower() in ['last modified', 'lastmodified', 'modified', 'last modified date']:
                        result['last_modified_date'] = value
                    elif key.lower() in ['deactivation date']:
                        result['deactivation_date'] = value
                    else:
                        # Store extra fields for future-proofing
                        extra_fields[key] = value
            
            # Add extra fields to result
            result.update(extra_fields)
        
        return result
    
    def download_all_nodes(self, node_ids: Set[str], tables_dir: str = './data/tables') -> List[Dict[str, str]]:
        """Download data for all nodes and save HTML snippets for analysis."""
        print(f"Processing {len(node_ids)} nodes...")
        
        # Categorize nodes into fresh (cached) and stale (need download)
        fresh_nodes, stale_nodes = self.categorize_nodes(node_ids, tables_dir)
        
        print(f"Found {len(fresh_nodes)} fresh cached files")
        print(f"Need to download {len(stale_nodes)} stale/missing files")
        
        all_node_data = []
        failed_nodes = []
        
        # Process fresh nodes from cache
        if fresh_nodes:
            print("Loading data from cached files...")
            for node_id in sorted(fresh_nodes):
                cached_html = self.load_cached_html(node_id, tables_dir)
                if cached_html:
                    parsed_data = self.parse_node_html(node_id, cached_html)
                    all_node_data.append(parsed_data)
                else:
                    # Cache failed, add to stale nodes for download
                    stale_nodes.add(node_id)
        
        # Process stale nodes by downloading
        if stale_nodes:
            print(f"Downloading {len(stale_nodes)} nodes from API...")
            for i, node_id in enumerate(sorted(stale_nodes)):
                if i % 50 == 0:
                    print(f"Download progress: {i}/{len(stale_nodes)} nodes processed")
                
                # Download JSON data
                json_data = self.download_node_data(node_id)
                
                if json_data and 'PartialViewHtml' in json_data:
                    html_content = json_data['PartialViewHtml']
                    
                    # Save HTML snippet for analysis
                    self.save_html_snippet(node_id, html_content, tables_dir)
                    
                    # Parse HTML to extract structured data
                    parsed_data = self.parse_node_html(node_id, html_content)
                    all_node_data.append(parsed_data)
                    
                else:
                    failed_nodes.append(node_id)
                
                # Small delay to be respectful
                time.sleep(0.1)
        
        print(f"Successfully processed {len(all_node_data)} nodes")
        if failed_nodes:
            print(f"Failed to process {len(failed_nodes)} nodes: {failed_nodes[:10]}...")
        
        return all_node_data
    
    def get_all_field_names(self, all_node_data: List[Dict[str, str]]) -> List[str]:
        """Get all unique field names from all nodes for dynamic CSV columns."""
        all_fields = set()
        
        for node_data in all_node_data:
            all_fields.update(node_data.keys())
        
        # Define the order of standard fields
        standard_fields = [
            'code_id', 'code_text', 'code_long_name', 'code_short_name',
            'code_definition', 'code_notes', 'code_effective_date', 'last_modified_date'
        ]
        
        # Add extra fields at the end
        extra_fields = sorted(all_fields - set(standard_fields))
        
        return standard_fields + extra_fields
    
    def write_csv(self, all_node_data: List[Dict[str, str]], output_path: str):
        """Write all node data to CSV file."""
        print(f"Writing {len(all_node_data)} nodes to {output_path}...")
        
        if not all_node_data:
            print("No data to write!")
            return
        
        # Get all field names
        field_names = self.get_all_field_names(all_node_data)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            
            # Write header
            writer.writeheader()
            
            # Write data
            for node_data in all_node_data:
                # Fill in missing fields with empty strings
                row = {field: node_data.get(field, '') for field in field_names}
                writer.writerow(row)
        
        print(f"Successfully wrote CSV file to {output_path}")
    
    def load_immediate_parent_mapping(self, csv_file_path: str) -> Dict[str, str]:
        """Load mapping from code_id to immediate_parent_code_id."""
        mapping = {}
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    mapping[row['code_id']] = row['immediate_parent_code_id']
            return mapping
        except Exception as e:
            print(f"Error loading immediate parent mapping: {e}")
            return {}

    def run(self, input_csv_path: str = './data/nucc_parent_code.csv', 
            output_csv_path: str = './data/nucc_codes.csv',
            tables_dir: str = './data/tables',
            immediate_parent_csv_path: str = './data/immediate_parent_code.csv'):
        """Run the complete scraping process."""
        try:
            # Load node IDs
            node_ids = self.load_node_ids(input_csv_path)
            
            # Download all node data
            all_node_data = self.download_all_nodes(node_ids, tables_dir)

            # Load immediate parent mapping
            immediate_parent_mapping = self.load_immediate_parent_mapping(immediate_parent_csv_path)

            # Add immediate_parent_code_id to each node
            for node_data in all_node_data:
                code_id = node_data.get('code_id', '')
                node_data['immediate_parent_code_id'] = immediate_parent_mapping.get(code_id, '')

            # Write to CSV
            self.write_csv(all_node_data, output_csv_path)
            
            print(f"\n✅ Successfully completed scraping!")
            print(f"Input file: {input_csv_path}")
            print(f"Output file: {output_csv_path}")
            print(f"HTML snippets saved to: {tables_dir}")
            print(f"Total nodes processed: {len(all_node_data)}")
            
        except Exception as e:
            print(f"\n❌ Scraping failed: {e}")
            raise


def main():
    """Main function to run the scraper."""
    scraper = NUCCNodesScraper()
    scraper.run()


if __name__ == "__main__":
    main()
