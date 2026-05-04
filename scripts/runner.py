#!/usr/bin/env python3
import subprocess
import os
import csv
from datetime import datetime
import argparse

# =====================================
# CONFIGURAZIONE PATH E PARAMETRI
# =====================================
JAR_PATH = "/home/rosario/Documents/computer_science/projects/quake-cooccurrence/target/scala-2.12/quake-cooccurrence_2.12-0.1.jar"
BUCKET = "scalableunibo2026-spark-data"
SOLVERS = ["Solver1", "Solver2", "Solver3", "Solver4", "Solver5"]
PARTITION_MULTIPLIERS = [2, 3, 4]

SUBMIT_SCRIPT = "./scripts/submit_job.sh"
CONFIG_FILE = "./scripts/config.sh"

LOG_DIR = "./logs"
RESULTS_FILE = "./results/results.csv"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)

# =====================================
# FUNZIONI UTILI
# =====================================
def run_script(script, *args):
    cmd = [script] + list(map(str, args))
    subprocess.run(cmd, check=True)

def compile_jar():
    result = subprocess.run(["sbt", "package"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR during compilation:")
        print(result.stdout)
        print(result.stderr)
        exit(1)

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
    num_workers = None
    cluster_name = None
    with open(CONFIG_FILE, "r") as f:
        for line in f:
            if line.startswith("export NUM_WORKERS="):
                num_workers = int(line.strip().split("=")[1])
            elif line.startswith("export CLUSTER_NAME="):
                cluster_name = line.strip().split("=")[1].strip('"')
    return num_workers, cluster_name

# =====================================
# MODIFICA CONFIG.SH (solo menu)
# =====================================
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
    run_script("./scripts/create_cluster.sh")

def delete_cluster():
    run_script("./scripts/delete_cluster.sh")

def create_bucket():
    run_script("./scripts/create_bucket.sh")

def delete_bucket():
    run_script("./scripts/delete_bucket.sh")

def upload_dataset():
    run_script("./scripts/upload_dataset.sh")

def submit_job(solver, cluster_name, multiplier):
    log_file = os.path.join(LOG_DIR, f"{solver}_p{multiplier}x_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    cmd = [SUBMIT_SCRIPT, JAR_PATH, "Main", solver, BUCKET, cluster_name, str(multiplier)]
    with open(log_file, "w") as f:
        process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
        process.wait()
    if process.returncode != 0:
        print(f"Job {solver} with multiplier {multiplier}x FAILED. Check {log_file}")
        return None
    return parse_time_from_log(log_file)

def update_results(solver, multiplier_times):
    results = {}
    fieldnames = ["Solver"] + [f"time_p{m}x" for m in PARTITION_MULTIPLIERS]
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                results[row["Solver"]] = row
    row = results.get(solver, {"Solver": solver, **{f"time_p{m}x": "" for m in PARTITION_MULTIPLIERS}})
    for multiplier, t in multiplier_times.items():
        row[f"time_p{multiplier}x"] = f"{t:.3f}" if t else ""
    results[solver] = row
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results.values():
            writer.writerow(r)

def run_solvers():
    num_workers, cluster_name = read_config()
    print(f"[INFO] Using NUM_WORKERS={num_workers}, CLUSTER_NAME={cluster_name} from config.sh")
    compile_jar()
    for solver in SOLVERS:
        multiplier_times = {}
        for multiplier in PARTITION_MULTIPLIERS:
            print(f"[INFO] Running {solver} with partition multiplier {multiplier}x")
            t = submit_job(solver, cluster_name, multiplier)
            multiplier_times[multiplier] = t
            print(f"[INFO] {solver} p{multiplier}x -> {t:.3f}s" if t else f"[WARN] {solver} p{multiplier}x -> FAILED")
        update_results(solver, multiplier_times)
        print(f"[INFO] Updated results.csv for {solver}")

# =====================================
# MAIN / MENU INTERATTIVO
# =====================================
def main():
    while True:
        print("\n===== RUNNER MENU =====")
        num_workers, cluster_name = read_config()
        print(f"[Config] NUM_WORKERS={num_workers}, CLUSTER_NAME={cluster_name}")
        print("1. Create bucket")
        print("2. Delete bucket")
        print("3. Upload dataset")
        print("4. Create cluster")
        print("5. Delete cluster")
        print("6. Run solvers / benchmark")
        print("7. Edit NUM_WORKERS in config.sh")
        print("8. Exit")
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
            run_solvers()
        elif choice == "7":
            nodes = int(input("Enter NUM_WORKERS to set in config.sh (2,3,4): ").strip())
            edit_config(nodes)
        elif choice == "8":
            print("Exiting.")
            break
        else:
            print("Invalid option. Try again.")

if __name__ == "__main__":
    main()