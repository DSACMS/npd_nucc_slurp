#!/usr/bin/env python3
"""
Parse NUCC source information from notes column
"""

import csv
import re
from typing import List, Dict, Optional, Tuple, Set

def extract_urls(text: str) -> List[str]:
    """
    Extract URLs from text and normalize them.
    Returns list of normalized URLs without duplicates where one is a substring of another.
    """
    urls = set()
    
    # Pattern for full URLs (with protocol)
    url_pattern = r'https?://[^\s,\[\]()"]+'
    full_urls = re.findall(url_pattern, text)
    for url in full_urls:
        # Clean up trailing punctuation
        url = re.sub(r'[.,;:!?]+$', '', url)
        urls.add(url)
    
    # Pattern for domain names (without protocol)
    # Look for common domain patterns like www.example.com, example.org, etc.
    domain_pattern = r'\b(?:www\.)?[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}\b'
    potential_domains = re.findall(domain_pattern, text)
    
    for domain in potential_domains:
        # Clean up trailing punctuation
        domain = re.sub(r'[.,;:!?]+$', '', domain)
        
        # Skip if it's already a full URL or looks like an email
        if domain.startswith('http') or '@' in domain:
            continue
            
        # Skip common false positives that aren't actually domains
        if domain.lower() in ['u.s.', 'p.o.', 'etc.', 'vs.', 'no.', 'vol.', 'ed.', 'pp.']:
            continue
            
        # Add https:// prefix
        normalized_url = f'https://{domain}'
        
        # Check if this domain is already covered by a more specific URL
        domain_already_covered = False
        for existing_url in urls:
            # Remove protocol for comparison
            existing_domain = re.sub(r'^https?://', '', existing_url)
            new_domain = re.sub(r'^https?://', '', normalized_url)
            
            # If the existing URL contains this domain as a subdomain/path, skip adding this domain
            if existing_domain.startswith(new_domain):
                domain_already_covered = True
                break
        
        if not domain_already_covered:
            urls.add(normalized_url)
    
    # Remove any URLs that are substrings of other URLs
    final_urls = []
    sorted_urls = sorted(list(urls), key=len, reverse=True)  # Sort by length, longest first
    
    for url in sorted_urls:
        is_substring = False
        for existing_url in final_urls:
            if url != existing_url and url in existing_url:
                is_substring = True
                break
        if not is_substring:
            final_urls.append(url)
    
    return sorted(final_urls)

def extract_sources(notes_text: str) -> List[Dict[str, str]]:
    """
    Extract source information from notes text.
    Returns list of dictionaries with source information.
    """
    if not notes_text or 'Source:' not in notes_text:
        return []
    
    sources = []
    
    # Split by "Source:" to handle multiple sources
    parts = notes_text.split('Source:')
    
    for i, part in enumerate(parts):
        if i == 0:  # First part is before any "Source:"
            continue
            
        part = part.strip()
        if not part:
            continue
            
        # Pattern to match: text [date: note]
        # This regex captures the source text and the bracketed date/note
        match = re.search(r'^(.*?)\s*\[([^:]+):\s*([^\]]+)\](.*)$', part)
        
        if match:
            source_text = match.group(1).strip()
            date_str = match.group(2).strip()
            note = match.group(3).strip()
            additional_info = match.group(4).strip()
            
            # Clean up source text - remove trailing punctuation and whitespace
            source_text = re.sub(r'[,.\s]+$', '', source_text)
            
            # If there's additional info after the bracket, add it to source text
            if additional_info:
                source_text += ' ' + additional_info
            
            # Extract URLs from the full source text
            urls = extract_urls(source_text)
            
            sources.append({
                'source_text': source_text,
                'date': date_str,
                'note': note,
                'urls': urls
            })
        else:
            # No bracketed date found, treat entire text as source
            urls = extract_urls(part.strip())
            sources.append({
                'source_text': part.strip(),
                'date': '',
                'note': '',
                'urls': urls
            })
    
    return sources

def parse_nucc_sources(input_file: str, output_file: str) -> None:
    """
    Parse NUCC codes file and extract source information.
    """
    results = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            code_id = row['code_id']
            notes = row.get('code_notes', '')
            
            if not notes:
                continue
                
            sources = extract_sources(notes)
            
            for source in sources:
                # If there are URLs, create a separate row for each URL
                if source['urls']:
                    for url in source['urls']:
                        results.append({
                            'nucc_code_id': code_id,
                            'full_source_text': source['source_text'],
                            'source_date': source['date'],
                            'source_date_note': source['note'],
                            'extracted_urls': url
                        })
                else:
                    # If no URLs, create one row with empty URL field
                    results.append({
                        'nucc_code_id': code_id,
                        'full_source_text': source['source_text'],
                        'source_date': source['date'],
                        'source_date_note': source['note'],
                        'extracted_urls': ''
                    })
    
    # Write results to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['nucc_code_id', 'full_source_text', 'source_date', 'source_date_note', 'extracted_urls']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Extracted {len(results)} source records from {input_file}")
    print(f"Results written to {output_file}")

def main():
    """Main function to run the parser."""
    input_file = './data/nucc_codes.csv'
    output_file = './data/nucc_sources.csv'
    
    print("Parsing NUCC source information...")
    parse_nucc_sources(input_file, output_file)
    
    print("\nSample of extracted sources:")
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 10:  # Show first 10 rows
                break
            print(f"ID: {row['nucc_code_id']}")
            print(f"  Source: {row['full_source_text']}")
            print(f"  Date: {row['source_date']}")
            print(f"  Note: {row['source_date_note']}")
            print(f"  URLs: {row['extracted_urls']}")
            print()

if __name__ == '__main__':
    main()
