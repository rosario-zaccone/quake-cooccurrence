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
        default=Path("results/plot"),
        type=Path,
        help="Directory where plots will be saved.",
    )
    parser.add_argument(
        "--weak-metrics-path",
        default=Path("results/all_results_weak.csv"),
        type=Path,
        help=(
            "Path to the aggregated weak-scaling CSV metrics file. "
            "Weak-scaling plots are skipped when the file does not exist."
        ),
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


def compute_weak_scaling_metrics(
    avg_df: pd.DataFrame,
) -> pd.DataFrame:
    baseline = (
        avg_df[avg_df["workers"] == BASE_WORKERS][
            ["solver", "partition_multiplier", "mean_time"]
        ]
        .rename(columns={"mean_time": "baseline_mean_time"})
        .copy()
    )
    weak_df = avg_df.merge(
        baseline,
        on=["solver", "partition_multiplier"],
        how="left",
    )

    missing_baseline = weak_df["baseline_mean_time"].isna()
    if missing_baseline.any():
        missing_groups = (
            weak_df.loc[missing_baseline, ["solver", "partition_multiplier"]]
            .drop_duplicates()
            .sort_values(["solver", "partition_multiplier"])
        )
        formatted_groups = ", ".join(
            f"{row.solver}/{row.partition_multiplier}"
            for row in missing_groups.itertuples(index=False)
        )
        print(
            "Skipped weak-scaling series without a "
            f"{BASE_WORKERS}-worker baseline: {formatted_groups}"
        )
        weak_df = weak_df.loc[~missing_baseline].copy()

    weak_df["workload_scale"] = weak_df["workers"] / BASE_WORKERS
    weak_df["efficiency"] = weak_df["baseline_mean_time"] / weak_df["mean_time"]

    return weak_df[
        [
            "solver",
            "partition_multiplier",
            "workers",
            "workload_scale",
            "efficiency",
        ]
    ].copy()


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
    fig.savefig(png_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {png_path}")


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


def save_weak_metric_table(weak_efficiency_df: pd.DataFrame, output_dir: Path) -> None:
    weak_path = output_dir / "weak.csv"

    weak_columns = [
        "solver",
        "partition_multiplier",
        "workers",
        "workload_scale",
        "efficiency",
    ]

    weak_efficiency_df[weak_columns].sort_values(
        ["solver", "partition_multiplier", "workers"]
    ).to_csv(weak_path, index=False)

    print(f"Saved {weak_path}")


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


def plot_weak_scaling_efficiency(
    weak_efficiency_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    plotted_df = weak_efficiency_df[weak_efficiency_df["workers"] == 4].copy()
    if plotted_df.empty:
        print("Skipped weak-scaling plot; no 4-worker weak-scaling metrics found.")
        return

    partition_multipliers = list(
        plotted_df["partition_multiplier"].cat.categories
    )
    partition_multipliers = [
        multiplier
        for multiplier in partition_multipliers
        if multiplier in set(plotted_df["partition_multiplier"])
    ]
    solvers = list(plotted_df["solver"].cat.categories)
    solvers = [
        solver
        for solver in solvers
        if solver in set(plotted_df["solver"])
    ]
    palette = dict(zip(solvers, sns.color_palette("colorblind", len(solvers))))

    fig, axes = plt.subplots(
        1,
        len(partition_multipliers),
        figsize=(9.6, 4.3),
        sharey=True,
        constrained_layout=False,
    )

    if len(partition_multipliers) == 1:
        axes = [axes]

    legend_handles = []
    legend_labels = []
    max_efficiency = plotted_df["efficiency"].max()

    for ax, partition_multiplier in zip(axes, partition_multipliers):
        partition_df = plotted_df[
            plotted_df["partition_multiplier"] == partition_multiplier
        ].set_index("solver")

        x_positions = list(range(len(solvers)))
        heights = [
            partition_df.loc[solver, "efficiency"]
            if solver in partition_df.index
            else float("nan")
            for solver in solvers
        ]

        bars = ax.bar(
            x_positions,
            heights,
            color=[palette[solver] for solver in solvers],
            width=0.65,
        )

        if partition_multiplier == partition_multipliers[0]:
            for solver, bar in zip(solvers, bars):
                bar.set_label(solver)
                legend_handles.append(bar)
                legend_labels.append(solver)

        for x_position, efficiency in zip(x_positions, heights):
            if pd.isna(efficiency):
                continue
            ax.text(
                x_position,
                efficiency,
                f"{efficiency:.2f}",
                ha="center",
                va="bottom",
                fontsize=8,
                color="#333333",
            )

        ax.axhline(
            1.0,
            linestyle="--",
            color="#444444",
            linewidth=1,
            alpha=0.75,
        )
        ax.set_title(f"{partition_multiplier} partitions")
        ax.set_xlabel("Solver")
        ax.set_xticks(x_positions)
        ax.set_xticklabels(solvers, rotation=30, ha="right")
        ax.set_ylim(0, max(1.08, max_efficiency * 1.18))
        ax.grid(axis="y", linestyle="--", linewidth=0.7, alpha=0.45)
        ax.grid(axis="x", visible=False)

    axes[0].set_ylabel("Weak scaling efficiency")
    fig.suptitle(
        "4-worker weak scaling efficiency",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )
    fig.text(
        0.5,
        0.09,
        f"Computed as T{BASE_WORKERS} / T4 with workload scaled by 4 / {BASE_WORKERS}. Ideal value = 1.",
        ha="center",
        fontsize=9,
        color="#555555",
    )
    if legend_handles:
        add_figure_legend(fig, legend_handles, legend_labels)
    fig.subplots_adjust(top=0.80, bottom=0.32, left=0.08, right=0.99, wspace=0.12)
    save_figure(fig, output_dir, "weak_scaling_efficiency_by_workers")


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

    if args.weak_metrics_path.exists():
        weak_df = load_metrics(args.weak_metrics_path)
        weak_long_df = to_long_format(weak_df)
        weak_avg_df = summarize(weak_long_df)
        weak_efficiency_df = compute_weak_scaling_metrics(weak_avg_df)

        save_weak_metric_table(weak_efficiency_df, args.output_dir)
        plot_weak_scaling_efficiency(weak_efficiency_df, args.output_dir)
    else:
        print(f"Skipped weak-scaling plots; {args.weak_metrics_path} does not exist.")


if __name__ == "__main__":
    main()
