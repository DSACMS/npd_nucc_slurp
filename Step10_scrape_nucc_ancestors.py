#!/usr/bin/env python3
"""
NUCC Ancestor Scraper

This script scrapes the provider taxonomy data from https://taxonomy.nucc.org/
and extracts the hierarchical relationships between codes, creating a CSV file
with ancestor-child relationships.
"""

import requests
from bs4 import BeautifulSoup
import csv
import os
import json
import re
from typing import Dict, List, Set, Tuple, Optional


class NUCCAncestorScraper:
    """Scraper for NUCC taxonomy hierarchy data."""
    
    def __init__(self):
        self.url = "https://taxonomy.nucc.org/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        # Explicitly ignore these nid values during scraping. They are not taxonomies but rather comments on the taxonomy website.
        self.ignored_nids = {5, 2714, 2712}
        
    def fetch_html(self):
        """Fetch HTML content from the NUCC taxonomy site."""
        print("Fetching HTML from NUCC taxonomy site...")
        try:
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
            print(f"Successfully fetched HTML ({len(response.text)} characters)")
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching HTML: {e}")
            raise
    
    def parse_html(self, html_content: str):
        """Parse HTML content and extract the treenodes JavaScript variable."""
        print("Parsing HTML content...")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the JavaScript containing the treenodes data
        scripts = soup.find_all('script')
        treenodes_data = None
        
        for script in scripts:
            script_text = script.get_text()
            if 'var treenodes' in script_text:
                print("Found treenodes variable in JavaScript")
                # Extract the JSON array from the JavaScript
                match = re.search(r'var treenodes = (\[.*?\]);', script_text, re.DOTALL)
                if match:
                    treenodes_json = match.group(1)
                    try:
                        treenodes_data = json.loads(treenodes_json)
                        print(f"Successfully parsed {len(treenodes_data)} tree nodes")
                        break
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON: {e}")
                        continue
        
        if not treenodes_data:
            raise ValueError("Could not find or parse treenodes data in HTML")
        
        return treenodes_data
    
    def extract_nid_from_link(self, link_element) -> Optional[str]:
        """Extract nid value from a link element."""
        if not link_element:
            return None
            
        # Check for nid attribute
        nid = link_element.get('nid')
        if nid:
            return nid
            
        # Check for href with nid parameter
        href = link_element.get('href', '')
        if 'nid=' in href:
            try:
                nid = href.split('nid=')[1].split('&')[0]
                return nid
            except IndexError:
                pass
                
        return None
    
    def build_hierarchy_relationships(self, treenodes_data: List[Dict]) -> List[Tuple[str, str]]:
        """
        Build ancestor-child relationships from the treenodes JSON data.
        
        Args:
            treenodes_data: List of tree node dictionaries with id, pId, and name fields
            
        Returns:
            List of (ancestor_id, child_id) tuples
        """
        relationships = []
        
        # Create a mapping of node_id to node data
        nodes_by_id = {node['id']: node for node in treenodes_data}
        
        # Filter out ignored nodes
        ignored_count = 0
        
        # For each node, find all its ancestors
        for node in treenodes_data:
            node_id = node['id']
            
            # Skip ignored nid values
            if node_id in self.ignored_nids:
                ignored_count += 1
                continue
                
            node_id_str = str(node_id)
            
            # Add self-relationship (node is its own ancestor)
            relationships.append((node_id_str, node_id_str))
            
            # Walk up the parent chain to find all ancestors
            current_node = node
            while current_node['pId'] != 0:  # 0 means root level
                parent_id = current_node['pId']
                
                # Skip if parent is in ignored list
                if parent_id in self.ignored_nids:
                    break
                    
                if parent_id in nodes_by_id:
                    parent_node = nodes_by_id[parent_id]
                    relationships.append((str(parent_id), node_id_str))
                    current_node = parent_node
                else:
                    # Parent not found, break the chain
                    break
        
        if ignored_count > 0:
            print(f"Ignored {ignored_count} nodes with nid values: {self.ignored_nids}")
        
        return relationships
    
    def deduplicate_relationships(self, relationships: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """Remove duplicate relationships."""
        print(f"Deduplicating {len(relationships)} relationships...")
        unique_relationships = list(set(relationships))
        print(f"Found {len(unique_relationships)} unique relationships")
        return unique_relationships
    
    def write_csv(self, relationships: List[Tuple[str, str]], output_path: str):
        """Write relationships to CSV file."""
        print(f"Writing {len(relationships)} relationships to {output_path}...")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['ancestor_nucc_code_id', 'child_nucc_code_id'])
            
            # Sort relationships for consistent output
            sorted_relationships = sorted(relationships, key=lambda x: (x[0], x[1]))
            
            # Write data
            for ancestor, child in sorted_relationships:
                writer.writerow([ancestor, child])
        
        print(f"Successfully wrote CSV file to {output_path}")
    
    def run(self, output_path: str = './data/nucc_parent_code.csv'):
        """Run the complete scraping process."""
        try:
            # Fetch HTML
            html_content = self.fetch_html()
            
            # Parse HTML and extract treenodes data
            treenodes_data = self.parse_html(html_content)
            
            # Extract relationships
            print("Extracting hierarchy relationships...")
            relationships = self.build_hierarchy_relationships(treenodes_data)
            
            # Deduplicate
            unique_relationships = self.deduplicate_relationships(relationships)
            
            # Write to CSV
            self.write_csv(unique_relationships, output_path)
            
            print(f"\n✅ Successfully completed scraping!")
            print(f"Output file: {output_path}")
            print(f"Total relationships: {len(unique_relationships)}")
            
        except Exception as e:
            print(f"\n❌ Scraping failed: {e}")
            raise


def main():
    """Main function to run the scraper."""
    scraper = NUCCAncestorScraper()
    scraper.run()


if __name__ == "__main__":
    main()
