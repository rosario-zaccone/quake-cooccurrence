#!/usr/bin/env python3
import subprocess
import os
import csv
import shlex
from datetime import datetime
import argparse

# =====================================
# CONFIGURATION
# =====================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

JAR_PATH = os.path.join(PROJECT_ROOT, "target/scala-2.12/quake-cooccurrence_2.12-0.1.jar")
SOLVERS = ["Solver1", "Solver2", "Solver3", "Solver4", "Solver5"]
PARTITION_MULTIPLIERS = [2, 3, 4]

SUBMIT_SCRIPT = os.path.join(SCRIPT_DIR, "submit_job.sh")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.sh")
JAVA_11_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
RESULTS_FILE = os.path.join(PROJECT_ROOT, "results/results.csv")
WEAK_RESULTS_FILE = os.path.join(PROJECT_ROOT, "results/results_weak.csv")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)

# =====================================
# UTILS
# =====================================
def source_config():
    cmd = f"source {shlex.quote(CONFIG_FILE)} && env"
    result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, check=True)

    for line in result.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            os.environ[k] = v

    print("[INFO] config.sh loaded into environment")

def get_config_value(name):
    if name not in os.environ:
        source_config()
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set. Check {CONFIG_FILE}.")
    return value

def run_script(script, *args):
    cmd = [script] + list(map(str, args))
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

def compile_jar():
    env = os.environ.copy()
    if os.path.isdir(JAVA_11_HOME):
        env["JAVA_HOME"] = JAVA_11_HOME
        env["PATH"] = os.path.join(JAVA_11_HOME, "bin") + os.pathsep + env["PATH"]
    result = subprocess.run(["sbt", "package"], cwd=PROJECT_ROOT, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print("ERROR during compilation:")
        print(result.stdout)
        print(result.stderr)
        raise SystemExit(1)

def parse_time_from_log(log_file):
    try:
        with open(log_file, "r") as f:
            for line in f:
                if "Elapsed time:" in line:
                    time_str = line.strip().split("Elapsed time:")[1].split()[0]
                    return float(time_str)
    except FileNotFoundError:
        pass
    return None

def read_config():
    source_config()
    return int(get_config_value("NUM_WORKERS")), get_config_value("CLUSTER_NAME")

def edit_config(nodes):
    lines = []
    with open(CONFIG_FILE, "r") as f:
        for line in f:
            if line.startswith("export NUM_WORKERS="):
                line = f'export NUM_WORKERS={nodes}\n'
            elif line.startswith("export CLUSTER_NAME="):
                line = f'export CLUSTER_NAME="spark-cluster-{nodes}w"\n'
            lines.append(line)
    with open(CONFIG_FILE, "w") as f:
        f.writelines(lines)
    print(f"config.sh updated with NUM_WORKERS={nodes} and CLUSTER_NAME=spark-cluster-{nodes}w")

# =====================================
# GCP CLUSTER / BUCKET / JOBS
# =====================================
def create_cluster():
    run_script(os.path.join(SCRIPT_DIR, "create_cluster.sh"))

def delete_cluster():
    run_script(os.path.join(SCRIPT_DIR, "delete_cluster.sh"))

def create_bucket():
    run_script(os.path.join(SCRIPT_DIR, "create_bucket.sh"))

def delete_bucket():
    run_script(os.path.join(SCRIPT_DIR, "delete_bucket.sh"))

def upload_dataset():
    run_script(os.path.join(SCRIPT_DIR, "upload_dataset.sh"))

def submit_job(solver, cluster_name, bucket, multiplier, weak_scaling=False):
    log_file = os.path.join(LOG_DIR, f"{solver}_p{multiplier}x_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    cmd = [SUBMIT_SCRIPT, JAR_PATH, "Main", solver, bucket, cluster_name, str(multiplier)]
    if weak_scaling:
        cmd.append("--weak-scaling")
    with open(log_file, "w") as f:
        process = subprocess.Popen(cmd, cwd=PROJECT_ROOT, stdout=f, stderr=subprocess.STDOUT)
        process.wait()
    if process.returncode != 0:
        print(f"Job {solver} with multiplier {multiplier}x FAILED. Check {log_file}")
        return None
    return parse_time_from_log(log_file)

def update_results(solver, multiplier_times, results_file=RESULTS_FILE):
    results = {}
    fieldnames = ["Solver"] + [f"time_p{m}x" for m in PARTITION_MULTIPLIERS]
    if os.path.exists(results_file):
        with open(results_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                results[row["Solver"]] = row
    row = results.get(solver, {"Solver": solver, **{f"time_p{m}x": "" for m in PARTITION_MULTIPLIERS}})
    for multiplier, t in multiplier_times.items():
        row[f"time_p{multiplier}x"] = f"{t:.3f}" if t else ""
    results[solver] = row
    with open(results_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results.values():
            writer.writerow(r)

def run_solvers(bucket=None, weak_scaling=False):
    num_workers, cluster_name = read_config()
    bucket_source = "--bucket" if bucket else "config.sh"
    bucket = bucket or get_config_value("DATA_BUCKET")
    results_file = WEAK_RESULTS_FILE if weak_scaling else RESULTS_FILE
    print(f"[INFO] Using NUM_WORKERS={num_workers}, CLUSTER_NAME={cluster_name}, DATA_BUCKET={bucket} from {bucket_source}")
    print(f"[INFO] Weak scaling: {'enabled' if weak_scaling else 'disabled'}")
    compile_jar()
    for solver in SOLVERS:
        multiplier_times = {}
        for multiplier in PARTITION_MULTIPLIERS:
            print(f"[INFO] Running {solver} with partition multiplier {multiplier}x")
            t = submit_job(solver, cluster_name, bucket, multiplier, weak_scaling)
            multiplier_times[multiplier] = t
            print(f"[INFO] {solver} p{multiplier}x -> {t:.3f}s" if t else f"[WARN] {solver} p{multiplier}x -> FAILED")
        update_results(solver, multiplier_times, results_file)
        print(f"[INFO] Updated {os.path.relpath(results_file, PROJECT_ROOT)} for {solver}")

# =====================================
# MAIN
# =====================================
def main():
    parser = argparse.ArgumentParser(description="Manage GCP resources and run solver benchmarks.")
    parser.add_argument("--bucket", help="Data bucket to pass to the Spark job. Defaults to DATA_BUCKET from config.sh.")
    args = parser.parse_args()
    config_loaded = False

    while True:
        print("\n===== RUNNER MENU =====")

        if config_loaded:
            print("[Config] loaded from config.sh")
        else:
            num_workers, cluster_name = read_config()
            print(f"[Config] NUM_WORKERS={num_workers}, CLUSTER_NAME={cluster_name}")

        print("1. Create bucket")
        print("2. Delete bucket")
        print("3. Upload dataset")
        print("4. Create cluster")
        print("5. Delete cluster")
        print("6. Run solvers / benchmark")
        print("7. Run solvers / benchmark weak scaling")
        print("8. Source config.sh")
        print("9. Edit NUM_WORKERS in config.sh")
        print("10. Exit")

        choice = input("Select an option: ").strip()

        if choice == "1":
            create_bucket()

        elif choice == "2":
            delete_bucket()

        elif choice == "3":
            upload_dataset()

        elif choice == "4":
            create_cluster()

        elif choice == "5":
            delete_cluster()

        elif choice == "6":
            run_solvers(args.bucket)

        elif choice == "7":
            run_solvers(args.bucket, weak_scaling=True)

        elif choice == "8":
            source_config()
            config_loaded = True

        elif choice == "9":
            nodes = int(input("Enter NUM_WORKERS to set in config.sh (2,3,4): ").strip())
            edit_config(nodes)
            config_loaded = False

        elif choice == "10":
            print("Exiting.")
            break

        else:
            print("Invalid option. Try again.")

if __name__ == "__main__":
    main()
