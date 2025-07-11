We need a few manual data chcecks to make sure that our ETL is working properly.

This file should make sure that these nodes, and their ancestors are present in the data.

"261QA1903X" -> "261Q00000X" -> "Ambulatory Health Care Facilities" -> "Non-individual"

"273100000X" -> "Hospital Units" -> "Non-individual"

"281PC2000X" -> "281P00000X" -> Hospitals ->  "Non-individual"

"207NP0225X" -> "207N00000X" -> "Allopathic & Osteopathic Physicians" -> "Individual or Groups (of Individuals)"

"101YM0800X" -> "101Y00000X" -> "Behavioral Health & Social Service Providers" -> "Individual or Groups (of Individuals)"

Please write a script that loads the ancestor csv in ./data/nucc_parent_code.csv, joins in the node data in ./data/merged_nucc_data.csv
and then verifies that these relationships are correctly represented. Use pandas for this purpose.

First represent the tests above in a data structure.

Please place the code in Step50_Verification.py
