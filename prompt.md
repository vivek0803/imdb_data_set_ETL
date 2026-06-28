# AI Prompts Used

This document captures the AI prompts used during the project setup and implementation. It is included to showcase how AI assistance was used for research, architectural decisions, and code generation.

## OLAP Selection Research

Tool/model used: ChatGPT, OpenAI GPT-5.5 Instant

Purpose: Quick research and comparison to choose a suitable OLAP engine for local deployment.

### Prompt 1

```text
What is the best OLAP services used for local deployments?
```

### Prompt 2

```text
Can you compare Druid vs ClickHouse and provide which is best for local deployments?

Please consider below metrics:
1. Ease of managing the resources and setup
2. Latency
3. Scale
4. Cost if any
```

## Code Build Prompt

Tool/model used: Cursor with GPT-5.5 Medium

Purpose: Build the local IMDb data pipeline using Spark, Parquet, and ClickHouse.

Prompting approach: Chain-of-thought (CoT) style prompting was used only for the Cursor prompts in this section to help reason through implementation choices, local resource constraints, and transformation behavior.

### Prompt 1

```text
Build a local data pipeline that:

Downloads the 2GB IMDb dataset from Kaggle (login required) - https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset
Extract the necessary data for analysis, such as: Movie titles, ratings, episodes and other pertinent information.

Processes the IMDb dataset using PySpark.
Saves the data as partitioned Parquet files (the "Lake").
Ingests or mounts that data into an OLAP engine of your choice for high-speed analytics.

Requirements

The Environment:
- Use Docker Compose to spin up a Spark cluster (Master/Worker).
- Ensure your environment can run your OLAP of choice locally.

Transformation (PySpark):
- Clean and transform the IMDb dataset (titles, ratings, and episodes).
- Export the result as Snappy-compressed Parquet files.
- Crucial: Apply a partitioning strategy that makes sense for time-series or category-based analysis.
- Use proper sort key and partition strategy based on a low-cardinality column.

OLAP Integration (The "Load" Step):
- Load the Parquet data into your chosen OLAP engine.

Analytics Layer:
- Demonstrate that the OLAP engine is significantly faster than raw Spark for these queries.

Deliverables:
- Infrastructure: A docker-compose.yml that orchestrates Spark and ClickHouse as the OLAP.
- Select proper stable versions for both Spark and ClickHouse.
- The Pipeline: A PySpark script (etl_job.py) and a loading script (load_to_olap.py).
- The Schema: DDL files for your OLAP tables, including indexes or primary keys.
- Please add a brief section in README explaining why ClickHouse is best for local environments.

Use Kagglehub to download the files.

About Dataset:
Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set.
The first line in each file contains headers that describe what is in each column.
A "\N" is used to denote that a particular field is missing or null for that title/name.
The available datasets are as follows:
```

### Prompt 2

```text
Give less memory for Spark, considering ideal local deployments.
```

### Prompt 3

```text
Don't drop the data which is partitioned by titleType when titleType is empty.
Just fill the values as unknown and then create a partition for unknown titleType also.

Add similar changes to any other transformations if any.
```
