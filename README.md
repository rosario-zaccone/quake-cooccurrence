# quake-cooccurrence

University project for the Scalable and Cloud Programming course. The project implements a distributed earthquake co-occurrence analysis in Scala + Apache Spark: given a CSV dataset, it finds the pair of distinct locations that co-occurs most often on the same day and prints the sorted list of dates.

## Project structure

```text
.
|-- build.sbt
|-- project/
|   `-- build.properties
|-- src/
|   `-- main/scala/
|       |-- Main.scala
|       |-- Solver.scala
|       |-- Solvers.scala
|       `-- model/
|           |-- Coordinate.scala
|           `-- Solution.scala
|-- scripts/
|   |-- config.sh
|   |-- create_bucket.sh
|   |-- create_cluster.sh
|   |-- delete_bucket.sh
|   |-- delete_cluster.sh
|   |-- upload_dataset.sh
|   |-- submit_job.sh
|   |-- runner.py
|   `-- plot.py
|-- docs/
|   |-- project-description.pdf
|   `-- relazione.md
|-- data/
|   `-- dataset.csv
|-- logs/
`-- results/
    |-- all_results.csv
    |-- results.csv
    `-- *.png
```

The `data/`, `logs/`, `results/`, `target/`, and IDE directories are ignored because they contain local data, generated output, or environment-specific files.

## Requirements

- Java 11
- sbt
- Google Cloud SDK (`gcloud` and `gsutil`)
- a Google Cloud project with Dataproc enabled
- Python 3 with `pandas` and `matplotlib`, only needed to generate plots

## Configuration

Edit `scripts/config.sh` with your project settings:

```bash
export PROJECT_ID="..."
export REGION="europe-west1"
export NUM_WORKERS=2
export CLUSTER_NAME="spark-cluster-2w"
export MACHINE_TYPE="n2-standard-4"
export DATA_BUCKET="${PROJECT_ID}-spark-data"
export TEMP_BUCKET="${PROJECT_ID}-spark-temp"
export LOCAL_FILE="./data/dataset.csv"
export DEST_FILE_NAME="dataset.csv"
```

The application expects the full dataset to be uploaded to the data bucket as:

```text
gs://<DATA_BUCKET>/dataset.csv
```

## Build

```bash
sbt package
```

The JAR is generated at:

```text
target/scala-2.12/quake-cooccurrence_2.12-0.1.jar
```

## Running on Dataproc

Authenticate with Google Cloud:

```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
```

Create the buckets and upload the dataset:

```bash
./scripts/create_bucket.sh
./scripts/upload_dataset.sh
```

Create the cluster:

```bash
./scripts/create_cluster.sh
```

Submit a Spark job:

```bash
./scripts/submit_job.sh \
  target/scala-2.12/quake-cooccurrence_2.12-0.1.jar \
  Main \
  Solver2 \
  <DATA_BUCKET> \
  <CLUSTER_NAME> \
  4
```

The application arguments are:

```text
Main <solver-name> <bucket-name> <cluster-name> <partition-multiplier>
```

- `solver-name`: one of `Solver1`, `Solver2`, `Solver3`, `Solver4`, `Solver5`
- `bucket-name`: name of the bucket containing `dataset.csv`
- `cluster-name`: name of the Dataproc cluster used for the job
- `partition-multiplier`: multiplier applied to `sc.defaultParallelism`, for example `2`, `3`, or `4`

Delete the cluster at the end to avoid unnecessary costs:

```bash
./scripts/delete_cluster.sh
```

## Benchmark

The `runner.py` script automates compilation, solver execution, and timing collection:

```bash
python3 scripts/runner.py
```

From the menu, you can create buckets, upload the dataset, create/delete clusters, and run all solvers. Logs are saved in `logs/`, while summary results are written to `results/results.csv`.

To generate plots from the aggregated metrics:

```bash
python3 scripts/plot.py results/all_results.csv --output-dir results
```

## Report

The report source is located at:

```text
docs/relazione.md
```

For submission, convert it to PDF and fill the `Repository` field with the public GitHub repository URL.
