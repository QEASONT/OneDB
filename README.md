# OneDB: A Distributed Multi-Metric Data Similarity Search System

## Overview

**OneDB** is a high-performance, distributed similarity search framework built to handle large-scale **multi-modal** and **multi-metric** data. This system enables efficient multi-metric **range** and **k-nearest neighbor (kNN)** queries by leveraging flexible indexing strategies and automatic parameter tuning. Designed for use cases across healthcare, e-commerce, and other domains, OneDB supports a variety of data types and distance metrics.

### Key Features:
- **Multi-Metric Weight Learning:** A lightweight learning model captures the importance of different modalities dynamically.
- **Dual-Layer Indexing:** A two-level index structure ensures both global partitioning and optimized local search.
- **End-to-End Parameter Tuning:** Deep reinforcement learning optimizes system performance automatically.

---

## Development Environment

We recommend **IntelliJ IDEA** for development, with Spark and Hadoop setup following the [official guide](http://spark.apache.org/developer-tools.html).

### Steps:
1. Install Spark and Hadoop dependencies.
2. Configure Maven projects with:
    - **Profiles:** `hadoop-2.6`, `hive-provided`, `yarn`
3. Reimport and generate sources using **Maven Projects** in IntelliJ.
4. Mark generated sources as appropriate:
   - `target/scala-2.11/src_managed/main/compiled_avro` for `spark-streaming-flume-sink`.
   - `src/gen/java` for `spark-hive-thriftserver`.

---

## Example Usage

To perform similarity search using OneDB's SQL interface:  

**1. Multi-Metric Range Query:**
```sql
SELECT * FROM my_table WHERE col IN ODBRANGE(query_point, weight_vector, radius);
```

**2. Multi-Metric kNN Query:**
```sql
SELECT * FROM my_table WHERE col IN ODBKNN(query_point, weight_vector, k);
```

For sample code, refer to:
`examples/src/main/scala/org/apache/spark/examples/sql/onedb/ODBMultiSQLExample.scala`

[ODBMultiDataFrameExample](spark/examples/src/main/scala/org/apache/spark/examples/sql/onedb/ODBMultiDataFrameExample.scala) for how to use DataFrame for multi-metric similarity search

---

## Source Code Structure

- **Core Logic:**
  - `/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/onedb`
- **Catalyst Expressions:**
  - `/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/onedb`
- **Examples:**
  - `/spark/examples/src/main/scala/org/apache/spark/examples/sql/onedb`

---

## Datasets

| **Dataset** | **Size**  | **Metric** | **Link**                                                     |
| ----------- | --------- | ---------- | ------------------------------------------------------------ |
| Air Quality | 1,150,000 | 13         | [Link](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india) |
| Food        | 38,757    | 9          | [Link](https://world.openfoodfacts.org/data)                 |
| Rental      | 113,176   | 5          | [Link](https://www.kaggle.com/c/two-sigma-connect-rental-listing-inquiries) |

Datasets are stored in **TXT** format and can be accessed from HDFS or a local directory. Each row represents one data point with dimensions separated by spaces.

---

## Compared Algorithms

| **Algorithm** | **Description**                        | **Year** |
| ------------- | -------------------------------------- | -------- |
| **DIMS**      | Distributed similarity search          | 2024     |
| **DESIRE**    | Multi-metric clustering-based indexing | 2022     |
| **Milvus**    | Vector-based similarity search         | 2021     |



---

## How to Run

1. **Build the Project:**
   
   ```bash
   mvn clean install
   ```
   
2. **Launch OneDB with Spark:**
   ```bash
   spark-submit --class org.apache.spark.examples.sql.onedb.ODBMultiDataFrameExample --master yarn
   ```

3. **Test SQL Queries:** Use Spark SQL CLI or connect to a running Spark instance.

---

## License

This project follows the ACM Copyright policy for SIGMOD Conference works. For personal or classroom use, copying is permitted without fee. Redistribution for commercial purposes requires specific permission from ACM.

