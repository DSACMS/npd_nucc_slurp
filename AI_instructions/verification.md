Verification
==================

We need a few manual data chcecks to make sure that our ETL is working properly.

This file should make sure that these nodes, and their ancestors are present in the data.

"261QA1903X" -> "261Q00000X" -> "Ambulatory Health Care Facilities" -> "Non-individual"

"273100000X" -> "Hospital Units" -> "Non-individual"

"281PC2000X" -> "281P00000X" -> Hospitals ->  "Non-individual"

"207NP0225X" -> "207N00000X" -> "Allopathic & Osteopathic Physicians" -> "Individual or Groups (of Individuals)"

"101YM0800X" -> "101Y00000X" -> "Behavioral Health & Social Service Providers" -> "Individual or Groups (of Individuals)"

When the lineage data above is using a 10 digit alpha-numeric code, it should be loaded from the node database in data/merged_nucc_data.csv using the 'combined_code' column.
When the lineage data above uses a english description the relevant node should be loaded from the scraped_code_short_name column.

Using the code_id loaded in this way, verify that the ancestory for each leaf (the child is on the left in the diagram above), has the appropriate parent.

There are two ways to do this verification and I would like the code to implement both.

First loads the ancestor csv in ./data/nucc_parent_code.csv, joins in the node data in ./data/merged_nucc_data.csv
and then verifies that these relationships are correctly represented.

Second, you can use the ./data/merged_nucc_data.csv data by itself using the scraped_code_id column and the scraped_immediate_parent_code_id column.

both approaches should result in the same answer.

Use pandas for all of the data processing in this task.

First represent the tests above in a data lineage data structure. Do not include the numeric code_id values in this data structure, because that negates the value of this test.
The relationships diagramed above are correct, and the whole point is to ensure that the various code_id based systems are properly reflecting this.
IF you embed the code_ids in the data, then the test passes but becomes valueless.

Please place the code in Step50_Verification.py

The verification output should be to the stdout.

The verification should test all the linages and print which failed and succeeded.

I have no preference for how the linage data should be stored in the program.

This should be a single process thread, invoked from a main function in case we later need to handle CLI arguments.
