SQL query Example : 
SELECT region
FROM sales
WHERE amount > 100;

Operators know the operands but not where the data come from. 

TableProvider trait hides those storage differences behind one common interface.

SQL: FROM sales
       │
       ▼
Catalog: "sales" → TableProvider
                       │
            ┌──────────┼──────────┐
            ▼          ▼          ▼
        MemoryTable  CsvTable  ParquetTable

Catalog is basically a table-name directory

Storage adapter translates any storage format into Arrow representation. 

TableProvider methods:
schema() describes the table’s columns. AKA Arrow contract since it promise names and types of columns that scans will produce.

partition_count() tells the scheduler how many independent pieces can be read. So SparkX give these partitions to workers.
More independent partitions generally allow more scan parallelism.

estimated_bytes() estimates how much data the table contains. Cost-based optimizer could use it to choose between plans.

partition_may_match(partition, filters) lets a storage adapter skip a partition when metadata proves its rows cannot match. Parquet uses row-group min/max/null statistics; uncertain cases are always read.

scan_partition(partition, projection, batch_size) performs the actual read.
partition — which independent piece to read.
projection — which columns are actually needed.
batch_size — how many rows should be placed in each Arrow batch.

