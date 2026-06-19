# Distributed Analysis of Earthquake Co-occurrences

University project for the Scalable and Cloud Programming course. The project implements a distributed earthquake co-occurrence analysis in Scala and Apache Spark: given a CSV dataset, it finds the pair of distinct locations that co-occurs most often on the same day and prints the sorted list of dates.

## Requirements

- Java 11
- sbt
- Google Cloud SDK, including `gcloud` and `gsutil`
- A Google Cloud project with Dataproc enabled
- Python 3 with `pandas`, `matplotlib`, and `seaborn`, only needed to generate plots
- A dataset CSV uploaded as `dataset.csv` to the configured Google Cloud Storage data bucket

## Configuration

Edit `scripts/config.sh` with your Google Cloud and benchmark settings:

```bash
export PROJECT_ID="..."
export REGION="europe-west1"
export NUM_WORKERS=2
export CLUSTER_NAME="spark-cluster-2w"
export MACHINE_TYPE="n2-standard-4"
export NETWORK="default"
export DATA_BUCKET="${PROJECT_ID}-spark-data"
export TEMP_BUCKET="${PROJECT_ID}-spark-temp"
export BUCKET_LOCATION="${REGION}"
export LOCAL_FILE="./data/dataset.csv"
export DEST_FILE_NAME="dataset.csv"
```

The Spark application reads the input dataset from:

```text
gs://<DATA_BUCKET>/dataset.csv
```

## Build and Execution

The recommended way to run the project is through `scripts/runner.py`, which provides an interactive menu for the common setup, execution, and benchmark operations:

```bash
python3 scripts/runner.py
```

```text
===== RUNNER MENU =====
1. Create bucket
2. Delete bucket
3. Upload dataset
4. Create cluster
5. Delete cluster
6. Run solvers / benchmark
7. Run solvers / benchmark weak scaling
8. Source config.sh
9. Edit NUM_WORKERS in config.sh
10. Exit
```

The runner loads `scripts/config.sh`, can update `NUM_WORKERS` and `CLUSTER_NAME`, creates or deletes Google Cloud resources, uploads the dataset, builds the JAR, runs all solvers, and writes timing results to CSV files. Option 6 runs the standard benchmark. Option 7 runs the weak-scaling benchmark by passing `--weak-scaling` to the Spark application.

You can also run the same workflow manually with the shell commands below.

Build the Scala application:

```bash
sbt package
```

The JAR is generated at:

```text
target/scala-2.12/quake-cooccurrence_2.12-0.1.jar
```

Authenticate with Google Cloud and select the project:

```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
```

Load the configuration variables:

```bash
source ./scripts/config.sh
```

Create the buckets and upload the dataset:

```bash
./scripts/create_bucket.sh
./scripts/upload_dataset.sh
```

Create the Dataproc cluster:

```bash
./scripts/create_cluster.sh
```

Submit a standard Spark job:

```bash
./scripts/submit_job.sh \
  target/scala-2.12/quake-cooccurrence_2.12-0.1.jar \
  Main \
  <SOLVER_NAME> \
  <DATA_BUCKET> \
  <CLUSTER_NAME> \
  4 \
  <NUM_WORKERS>
```

Submit a weak-scaling Spark job by adding `--weak-scaling`:

```bash
./scripts/submit_job.sh \
  target/scala-2.12/quake-cooccurrence_2.12-0.1.jar \
  Main \
  Solver2 \
  <DATA_BUCKET> \
  <CLUSTER_NAME> \
  4 \
  <NUM_WORKERS> \
  --weak-scaling
```

The application arguments are:

```text
Main <solver-name> <bucket-name> <cluster-name> <partition-multiplier> <workers> [--weak-scaling]
```

- `solver-name`: one of `Solver1`, `Solver2`, `Solver3`, `Solver4`, `Solver5`
- `bucket-name`: name of the bucket containing `dataset.csv`
- `cluster-name`: name of the Dataproc cluster used for the job
- `partition-multiplier`: multiplier applied to `sc.defaultParallelism`, for example `2`, `3`, or `4`
- `workers`: number of Dataproc worker nodes used for the run
- `--weak-scaling`: optional flag that scales the input workload with the worker count

In weak-scaling mode, the code uses 2 workers as the baseline. For `n` workers, the dataset is replicated by `n / 2`, so a 4-worker run processes twice the baseline workload. The worker count must be at least 2 and must be a multiple of 2.

Delete the cluster at the end to avoid unnecessary costs:

```bash
./scripts/delete_cluster.sh
```

## Output and Results

The file `results/output.txt` contains the final solution produced by the Spark job: the pair of locations with the maximum number of daily co-occurrences and the ordered list of dates where the co-occurrence happens.

During execution, the application writes this output to the configured Google Cloud Storage bucket. The copy in this repository was downloaded from the bucket after the Dataproc run, so the final result can be inspected without re-running the cloud job.

Benchmark data is split into these CSV files:

- `results/results.csv`: produced by `scripts/runner.py` during a standard benchmark run.
- `results/results_weak.csv`: produced by `scripts/runner.py` during a weak-scaling benchmark run.
- `results/all_results.csv`: manually aggregated standard benchmark dataset used for the final analysis.
- `results/all_results_weak.csv`: manually aggregated weak-scaling benchmark dataset used for the final weak-scaling analysis.
- `results/plot/speedup.csv`: generated by `scripts/plot.py`; contains speedup values relative to the 2-worker baseline.
- `results/plot/strong.csv`: generated by `scripts/plot.py`; contains strong scaling efficiency values.
- `results/plot/weak.csv`: generated by `scripts/plot.py`; contains weak scaling efficiency values.

The generated PNG plots include mean execution time, speedup, strong scaling efficiency, and weak scaling efficiency figures.

## Plot Generation

The `scripts/plot.py` script reads the aggregated benchmark CSV files and generates derived tables and PNG figures under `results/plot/`:

```bash
python3 scripts/plot.py results/all_results.csv
```

By default, the script also reads weak-scaling metrics from `results/all_results_weak.csv` when that file exists. A different weak-scaling input file can be passed with:

```bash
python3 scripts/plot.py results/all_results.csv --weak-metrics-path <weak-results.csv>
```

The weak-scaling figure reports the 4-worker efficiency only, with one panel per partition multiplier and solvers on the x-axis.

## Report

The report is located at:

```text
docs/report.pdf
```
