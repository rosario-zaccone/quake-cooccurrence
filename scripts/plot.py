#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


METRICS = ["time_p2x", "time_p3x", "time_p4x"]
PARTITION_LABELS = {
    "time_p2x": "2x",
    "time_p3x": "3x",
    "time_p4x": "4x",
}
BASE_WORKERS = 2
LINE_WIDTH = 1.45
MARKER_SIZE = 4.6


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate publication-ready benchmark plots from solver timings."
    )
    parser.add_argument(
        "metrics_path",
        nargs="?",
        default=Path("results/all_results.csv"),
        type=Path,
        help="Path to the aggregated CSV metrics file.",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("results"),
        type=Path,
        help="Directory where plots will be saved.",
    )
    return parser.parse_args()


def load_metrics(metrics_path: Path) -> pd.DataFrame:
    df = pd.read_csv(metrics_path)
    required_columns = {"workers", "run", "solver", *METRICS}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    df["workers"] = df["workers"].astype(int)
    df["solver"] = pd.Categorical(
        df["solver"],
        categories=sorted(df["solver"].unique()),
        ordered=True,
    )
    return df


def to_long_format(df: pd.DataFrame) -> pd.DataFrame:
    long_df = df.melt(
        id_vars=["workers", "run", "solver"],
        value_vars=METRICS,
        var_name="metric",
        value_name="time",
    )
    long_df["partition_multiplier"] = long_df["metric"].map(PARTITION_LABELS)
    long_df["partition_multiplier"] = pd.Categorical(
        long_df["partition_multiplier"],
        categories=list(PARTITION_LABELS.values()),
        ordered=True,
    )
    return long_df


def summarize(long_df: pd.DataFrame) -> pd.DataFrame:
    return (
        long_df.groupby(
            ["workers", "solver", "partition_multiplier"],
            observed=True,
            as_index=False,
        )
        .agg(
            mean_time=("time", "mean"),
            std_time=("time", "std"),
        )
        .sort_values(["workers", "solver", "partition_multiplier"])
    )


def compute_speedup(avg_df: pd.DataFrame) -> pd.DataFrame:
    baseline = (
        avg_df[avg_df["workers"] == BASE_WORKERS][
            ["solver", "partition_multiplier", "mean_time"]
        ]
        .rename(columns={"mean_time": "baseline_mean_time"})
        .copy()
    )
    speedup_df = avg_df.merge(
        baseline,
        on=["solver", "partition_multiplier"],
        how="left",
    )
    speedup_df["speedup"] = (
        speedup_df["baseline_mean_time"] / speedup_df["mean_time"]
    )
    return speedup_df[speedup_df["workers"] != BASE_WORKERS].copy()


def compute_strong_scaling_efficiency(speedup_df: pd.DataFrame) -> pd.DataFrame:
    efficiency_df = speedup_df.copy()
    efficiency_df["ideal_relative_speedup"] = (
        efficiency_df["workers"] / BASE_WORKERS
    )
    efficiency_df["efficiency"] = (
        efficiency_df["speedup"] / efficiency_df["ideal_relative_speedup"]
    )
    return efficiency_df


def setup_style() -> None:
    sns.set_theme(
        context="paper",
        style="whitegrid",
        rc={
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "legend.title_fontsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
        },
    )


def save_figure(fig: plt.Figure, output_dir: Path, stem: str) -> None:
    png_path = output_dir / f"{stem}.png"
    pdf_path = output_dir / f"{stem}.pdf"
    fig.savefig(png_path, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {png_path}")
    print(f"Saved {pdf_path}")


def save_metric_tables(
    speedup_df: pd.DataFrame,
    efficiency_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    speedup_path = output_dir / "speedup.csv"
    strong_path = output_dir / "strong.csv"

    speedup_columns = [
        "solver",
        "partition_multiplier",
        "workers",
        "speedup",
    ]
    strong_columns = [
        "solver",
        "partition_multiplier",
        "workers",
        "efficiency",
    ]

    speedup_df[speedup_columns].sort_values(
        ["solver", "partition_multiplier", "workers"]
    ).to_csv(speedup_path, index=False)
    efficiency_df[strong_columns].sort_values(
        ["solver", "partition_multiplier", "workers"]
    ).to_csv(strong_path, index=False)

    print(f"Saved {speedup_path}")
    print(f"Saved {strong_path}")


def add_figure_legend(fig: plt.Figure, handles: list, labels: list) -> None:
    fig.legend(
        handles,
        labels,
        title="Solver",
        loc="lower center",
        bbox_to_anchor=(0.5, -0.035),
        ncol=len(labels),
        frameon=True,
        fancybox=False,
        edgecolor="#d0d0d0",
    )


def plot_mean_time(avg_df: pd.DataFrame, output_dir: Path) -> None:
    workers = sorted(avg_df["workers"].unique())
    solvers = list(avg_df["solver"].cat.categories)
    palette = dict(zip(solvers, sns.color_palette("colorblind", len(solvers))))
    markers = dict(zip(solvers, ["o", "s", "^", "D", "P"]))

    fig, axes = plt.subplots(
        1,
        len(workers),
        figsize=(12.6, 4.5),
        sharey=True,
        constrained_layout=False,
    )

    if len(workers) == 1:
        axes = [axes]

    best_row = avg_df.loc[avg_df["mean_time"].idxmin()]
    legend_handles = []
    legend_labels = []

    for ax, worker_count in zip(axes, workers):
        worker_df = avg_df[avg_df["workers"] == worker_count]

        for solver in solvers:
            solver_df = worker_df[worker_df["solver"] == solver]
            (line,) = ax.plot(
                solver_df["partition_multiplier"].astype(str),
                solver_df["mean_time"],
                label=solver,
                color=palette[solver],
                marker=markers[solver],
                linewidth=LINE_WIDTH,
                markersize=MARKER_SIZE,
            )
            if worker_count == workers[0]:
                legend_handles.append(line)
                legend_labels.append(solver)

        ax.set_title(f"{worker_count} workers")
        ax.set_xlabel("Partition multiplier")
        ax.grid(axis="y", linestyle="--", linewidth=0.7, alpha=0.45)
        ax.grid(axis="x", visible=False)

        if worker_count == int(best_row["workers"]):
            ax.scatter(
                [str(best_row["partition_multiplier"])],
                [best_row["mean_time"]],
                s=130,
                facecolors="none",
                edgecolors="#111111",
                linewidths=1.8,
                zorder=5,
            )
            ax.annotate(
                f"best: {best_row['solver']}\n{best_row['mean_time']:.1f}s",
                xy=(str(best_row["partition_multiplier"]), best_row["mean_time"]),
                xytext=(10, 18),
                textcoords="offset points",
                fontsize=8.5,
                arrowprops={
                    "arrowstyle": "->",
                    "color": "#333333",
                    "lw": 0.9,
                },
            )

    axes[0].set_ylabel("Mean execution time (s)")
    fig.suptitle(
        "Mean execution time by solver, workers and partitioning",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )
    add_figure_legend(fig, legend_handles, legend_labels)
    fig.subplots_adjust(top=0.82, bottom=0.22, left=0.07, right=0.99, wspace=0.12)
    save_figure(fig, output_dir, "mean_time_by_workers")


def plot_speedup(speedup_df: pd.DataFrame, output_dir: Path) -> None:
    workers = sorted(speedup_df["workers"].unique())
    solvers = list(speedup_df["solver"].cat.categories)
    palette = dict(zip(solvers, sns.color_palette("colorblind", len(solvers))))
    markers = dict(zip(solvers, ["o", "s", "^", "D", "P"]))

    fig, axes = plt.subplots(
        1,
        len(workers),
        figsize=(9.6, 4.3),
        sharey=True,
        constrained_layout=False,
    )

    if len(workers) == 1:
        axes = [axes]

    legend_handles = []
    legend_labels = []

    for ax, worker_count in zip(axes, workers):
        worker_df = speedup_df[speedup_df["workers"] == worker_count]

        for solver in solvers:
            solver_df = worker_df[worker_df["solver"] == solver]
            (line,) = ax.plot(
                solver_df["partition_multiplier"].astype(str),
                solver_df["speedup"],
                label=solver,
                color=palette[solver],
                marker=markers[solver],
                linewidth=LINE_WIDTH,
                markersize=MARKER_SIZE,
            )
            if worker_count == workers[0]:
                legend_handles.append(line)
                legend_labels.append(solver)

        ax.axhline(
            1.0,
            linestyle="--",
            color="#444444",
            linewidth=1,
            alpha=0.75,
        )
        ax.set_title(f"{worker_count} workers")
        ax.set_xlabel("Partition multiplier")
        ax.grid(axis="y", linestyle="--", linewidth=0.7, alpha=0.45)
        ax.grid(axis="x", visible=False)

    axes[0].set_ylabel(f"Speedup vs {BASE_WORKERS} workers")
    fig.suptitle(
        "Scalability relative to the 2-worker baseline",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )
    add_figure_legend(fig, legend_handles, legend_labels)
    fig.subplots_adjust(top=0.80, bottom=0.23, left=0.08, right=0.99, wspace=0.12)
    save_figure(fig, output_dir, "speedup_by_workers")


def plot_strong_scaling_efficiency(
    efficiency_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    workers = sorted(efficiency_df["workers"].unique())
    solvers = list(efficiency_df["solver"].cat.categories)
    palette = dict(zip(solvers, sns.color_palette("colorblind", len(solvers))))
    markers = dict(zip(solvers, ["o", "s", "^", "D", "P"]))

    fig, axes = plt.subplots(
        1,
        len(workers),
        figsize=(9.6, 4.3),
        sharey=True,
        constrained_layout=False,
    )

    if len(workers) == 1:
        axes = [axes]

    legend_handles = []
    legend_labels = []

    for ax, worker_count in zip(axes, workers):
        worker_df = efficiency_df[efficiency_df["workers"] == worker_count]

        for solver in solvers:
            solver_df = worker_df[worker_df["solver"] == solver]
            (line,) = ax.plot(
                solver_df["partition_multiplier"].astype(str),
                solver_df["efficiency"],
                label=solver,
                color=palette[solver],
                marker=markers[solver],
                linewidth=LINE_WIDTH,
                markersize=MARKER_SIZE,
            )
            if worker_count == workers[0]:
                legend_handles.append(line)
                legend_labels.append(solver)

        ax.axhline(
            1.0,
            linestyle="--",
            color="#444444",
            linewidth=1,
            alpha=0.75,
        )
        ax.set_ylim(0, 1.12)
        ax.set_title(f"{worker_count} workers")
        ax.set_xlabel("Partition multiplier")
        ax.grid(axis="y", linestyle="--", linewidth=0.7, alpha=0.45)
        ax.grid(axis="x", visible=False)

    axes[0].set_ylabel(f"Efficiency vs {BASE_WORKERS}-worker baseline")
    fig.suptitle(
        "Strong scaling efficiency",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )
    fig.text(
        0.5,
        0.09,
        "Relative efficiency: (T2 / Tn) / (n / 2). Ideal value = 1.",
        ha="center",
        fontsize=9,
        color="#555555",
    )
    add_figure_legend(fig, legend_handles, legend_labels)
    fig.subplots_adjust(top=0.80, bottom=0.30, left=0.08, right=0.99, wspace=0.12)
    save_figure(fig, output_dir, "strong_scaling_efficiency_by_workers")


def plot_time_heatmap(avg_df: pd.DataFrame, output_dir: Path) -> None:
    heatmap_df = avg_df.copy()
    heatmap_df["configuration"] = (
        heatmap_df["workers"].astype(str)
        + "w / "
        + heatmap_df["partition_multiplier"].astype(str)
    )
    pivot = heatmap_df.pivot(
        index="solver",
        columns="configuration",
        values="mean_time",
    )

    ordered_columns = sorted(
        pivot.columns,
        key=lambda value: (int(value.split("w")[0]), value.split("/ ")[1]),
    )
    pivot = pivot[ordered_columns]

    fig, ax = plt.subplots(figsize=(11.5, 4.6))
    sns.heatmap(
        pivot,
        ax=ax,
        annot=True,
        fmt=".0f",
        cmap="viridis_r",
        linewidths=0.6,
        linecolor="white",
        cbar_kws={"label": "Mean execution time (s)"},
    )
    ax.set_title("Mean execution time matrix", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Cluster configuration")
    ax.set_ylabel("Solver")
    ax.tick_params(axis="x", rotation=35)
    fig.subplots_adjust(bottom=0.24, left=0.10, right=1.0, top=0.87)
    save_figure(fig, output_dir, "mean_time_heatmap")


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    setup_style()
    df = load_metrics(args.metrics_path)
    long_df = to_long_format(df)
    avg_df = summarize(long_df)
    speedup_df = compute_speedup(avg_df)
    efficiency_df = compute_strong_scaling_efficiency(speedup_df)

    save_metric_tables(speedup_df, efficiency_df, args.output_dir)
    plot_mean_time(avg_df, args.output_dir)
    plot_speedup(speedup_df, args.output_dir)
    plot_strong_scaling_efficiency(efficiency_df, args.output_dir)
    plot_time_heatmap(avg_df, args.output_dir)


if __name__ == "__main__":
    main()
