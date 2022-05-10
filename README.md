# Aerospike Summit 2022 Batch Anything Presentation
Companion code for the Aerospike Summit 2022 roadshow presentation "Building with Batch Writes in Aerospike Database 6".

## Dependencies

 - Aerospike Database 6.0
 - Python 3.7+
 - Aerospike Python Client >= 7.0.0

## Sequence
The presentation mentions which scripts are being run for each slide. The
sequence is as follows:

 1. `fill.py` to fill the database with sample data using single and batch writes
 2. `batch-reads.py` to demonstrate batch reading of records and batch read-operations over multiple records
 3. `new-score.py` to demonstrate updating records with read and write expressions
 4. `update-leaderboard.py` to demonstrate rolling up staged personal-best scores into top scores with batch reads and then batch writes
