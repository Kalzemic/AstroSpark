# AstroSpark

A distributed ADQL query processing system over the [Gaia DR3](https://www.cosmos.esa.int/web/gaia/dr3) astronomical dataset, built on Apache Kafka and Apache Spark. Inspired by the [ASTEROIDE](https://arxiv.org/abs/1609.01014) paper's HEALPix-partitioned, filter-refine paradigm for efficient spatial query execution.

---

## System Overview

AstroSpark ingests ADQL queries through a Kafka topic and processes them through a multi-stage pipeline that translates, spatially prunes, and executes them over partitioned Gaia DR3 data using Spark.

```
Kafka Producer
     │
     ▼
Kafka Consumer          ← receives raw ADQL query strings from the topic
     │
     ▼
Query Optimizer         ← parses the ADQL query tree (via queryparser-python3)
                           extracts spatial parameters (cone center, radius)
                           computes intersecting HEALPix partitions at nside=64
                           returns only the relevant partition file paths
     │
     ▼
Spark Engine            ← loads only the required partitions
                           caches each loaded DataFrame in memory
                           unions all relevant partitions into a single temp view
                           translates ADQL → PostgreSQL SQL dialect
                           executes via pyspark.sql and displays results
```

### HEALPix Partition Pruning

The Query Optimizer avoids a full-table scan by resolving which HEALPix sky partitions a spatial query touches. For cone searches, it computes the set of HEALPix pixels at `nside=64` that intersect the query circle using `healpy`'s `ang2vec` + `query_disc`. Only the corresponding Gaia DR3 CSV partition files are passed to the Spark Engine — unrelated partitions are never loaded.

### DataFrame Caching

The Spark Engine maintains a partition cache . When a partition is requested for the first time, it is read from disk, cast to proper numeric types, and pinned in Spark's memory. Subsequent queries that touch the same partitions skip disk I/O entirely and operate on the already-resident DataFrames. In practice, re-running a query or issuing a nearby cone search that overlaps a previously loaded partition is dramatically faster than the initial cold load.

### Dynamic Temp View

The Spark Engine unions all relevant partition DataFrames and registers them as a single `gaia_source` temporary view before executing the query. The user's ADQL references `gaiadr3.gaia_source` as if it were a monolithic table — the partitioned nature of the underlying data is fully abstracted away.

---

## Setup & Running

### 1. Start Kafka

You need an active Kafka producer on the `adql-queries` topic before running the system. Two options:

#### Option A — Local Kafka + Zookeeper

```bash
bash local_setup.sh
```

This starts Zookeeper and Kafka, creates the `adql-queries` topic, and opens an interactive console producer.

#### Option B — Docker Compose

```bash
docker compose up -d
bash docker_setup.sh
```

This brings up the Kafka network in detached containers and opens a console producer against the running broker.

Either way, you end up with an interactive terminal where you can type or paste ADQL queries for the system to consume.

---

### 3. Run the System

#### Option A — Python Project

Download the Gaia DR3 partitions into `data/`:

```bash
bash dataset.sh
```

Install dependencies:

```bash
pip install .
```

Run the system:

```bash
python main.py
```

#### Option B — PySpark / Jupyter Notebook 

Launch PySpark (which opens Jupyter):

```bash
pyspark
```

Open `AstroStream.ipynb` in the browser, then run all cells sequentially. The notebook installs its own dependencies and downloads data internally, so no prior setup is needed.

---

### 4. Submit Queries

With the system running and the Kafka producer open, paste ADQL queries from `adql-examples.md` directly into the producer terminal. The system will consume each query, resolve the relevant HEALPix partitions, and print the Spark query result.

Queries that target the same sky region as a previous query will benefit from the partition cache and return results significantly faster than the first load.

---

## Dependencies

| Package | Role |
|---|---|
| `pyspark` | Distributed query execution |
| `confluent_kafka` | Kafka consumer |
| `queryparser-python3` | ADQL parsing and SQL translation |
| `healpy` | HEALPix spatial indexing |

---

## References

- Louppe et al. (2016), *ASTEROIDE: Astronomical distributed data access with HEALPix and Spark* — the foundational architecture this system is based on.
- [Gaia DR3 data release](https://www.cosmos.esa.int/web/gaia/dr3)
- [HEALPix](https://healpix.sourceforge.io/)