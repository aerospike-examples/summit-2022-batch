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

## Options
All the scripts support the following options:

```
optional arguments:
  --help                Displays this message.
  -U <USERNAME>, --username <USERNAME>
                        Username to connect to database.
  -P <PASSWORD>, --password <PASSWORD>
                        Password to connect to database.
  -h <ADDRESS>, --host <ADDRESS>
                        Address of Aerospike server.
  -p <PORT>, --port <PORT>
                        Port of the Aerospike server.
  -n <NS>, --namespace <NS>
                        Namespace name to use
  -s <SET>, --set <SET>
                        Set name to use.
  --services-alternate  Use services alternate
```
