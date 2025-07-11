#!/bin/bash
python Step10_scrape_nucc_ancestors.py
# If you need to redo step20 from-scratch.. you must delete the cache.
#rm ./data/tables/*
python Step20_scrape_nucc_nodes.py
python Step30_parse_nucc_sources.py
python Step40_compare_nucc_data.py --download_csv ~/Downloads/nucc_taxonomy_251.csv --scrapped_csv ./data/nucc_codes.csv
