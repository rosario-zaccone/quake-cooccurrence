#!/usr/bin/env python3

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


METRICS = ["time_p2x", "time_p3x", "time_p4x"]
BASE_WORKERS = 2


def plot_metric(df: pd.DataFrame, metric: str, output_dir: Path) -> None:
    summary = (
        df.groupby(["solver", "workers"])[metric]
        .agg(["mean", "std"])
        .reset_index()
    )

    pivot_mean = summary.pivot(index="solver", columns="workers", values="mean")
    pivot_std = summary.pivot(index="solver", columns="workers", values="std")

    ax = pivot_mean.plot(
        kind="bar",
        yerr=pivot_std,
        capsize=4,
        figsize=(10, 6),
        rot=0,
    )

    ax.set_title(f"{metric}: mean time by solver and workers")
    ax.set_xlabel("Solver")
    ax.set_ylabel("Time")
    ax.legend(title="Workers")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    output_path = output_dir / f"{metric}_by_solver_workers.png"
    plt.savefig(output_path, dpi=200)
    plt.close()

    print(f"Saved {output_path}")


def plot_speedup(df: pd.DataFrame, metric: str, output_dir: Path) -> None:
    mean_times = (
        df.groupby(["solver", "workers"])[metric]
        .mean()
        .reset_index()
    )

    base = mean_times[mean_times["workers"] == BASE_WORKERS][
        ["solver", metric]
    ].rename(columns={metric: "base_time"})

    speedup = mean_times.merge(base, on="solver", how="left")
    speedup["speedup"] = speedup["base_time"] / speedup[metric]

    pivot_speedup = speedup.pivot(
        index="solver",
        columns="workers",
        values="speedup",
    )

    ax = pivot_speedup.plot(
        kind="bar",
        figsize=(10, 6),
        rot=0,
    )

    ax.axhline(1.0, linestyle="--", linewidth=1)
    ax.set_title(f"{metric}: speedup relative to {BASE_WORKERS} workers")
    ax.set_xlabel("Solver")
    ax.set_ylabel("Speedup")
    ax.legend(title="Workers")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    output_path = output_dir / f"{metric}_speedup_vs_{BASE_WORKERS}_workers.png"
    plt.savefig(output_path, dpi=200)
    plt.close()

    print(f"Saved {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create grouped bar plots and speedup plots from solver timing metrics."
    )
    parser.add_argument(
        "metrics_path",
        type=Path,
        help="Path to the CSV metrics file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("plots"),
        help="Directory where plots will be saved. Default: plots",
    )

    args = parser.parse_args()

    df = pd.read_csv(args.metrics_path)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    required_columns = {"workers", "run", "solver", *METRICS}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["workers"] = df["workers"].astype(int)

    for metric in METRICS:
        plot_metric(df, metric, args.output_dir)
        plot_speedup(df, metric, args.output_dir)


if __name__ == "__main__":
    main()