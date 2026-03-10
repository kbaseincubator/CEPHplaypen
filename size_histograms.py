"""Generate object size histograms from files containing one size (bytes) per line."""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB
TiB = 1024 * GiB

BOUNDARIES = [0, 4 * KiB, 4 * MiB, 4 * GiB, 4 * TiB, float("inf")]
LABELS = ["< 4 KiB", "4 KiB–4 MiB", "4 MiB–4 GiB", "4 GiB–4 TiB", "≥ 4 TiB"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot object size histograms from size files."
    )
    parser.add_argument("files", nargs="+", help="Input files (one size in bytes per line)")
    parser.add_argument(
        "--output-dir", default=".", help="Directory to write PNG files (default: .)"
    )
    return parser.parse_args()


def read_sizes(path: Path) -> list[int]:
    sizes = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                sizes.append(int(line))
            except ValueError:
                print(f"Error: {path}:{i}: not an integer: {line!r}", file=sys.stderr)
                sys.exit(1)
    return sizes


def bucket(sizes: list[int]) -> tuple[list[int], list[int]]:
    counts = [0] * len(LABELS)
    totals = [0] * len(LABELS)
    for size in sizes:
        for i in range(len(BOUNDARIES) - 1):
            if BOUNDARIES[i] <= size < BOUNDARIES[i + 1]:
                counts[i] += 1
                totals[i] += size
                break
    return counts, totals


def format_bytes(n: int) -> str:
    for unit, threshold in [("TiB", TiB), ("GiB", GiB), ("MiB", MiB), ("KiB", KiB)]:
        if n >= threshold:
            return f"{n / threshold:.1f} {unit}"
    return f"{n} B"


def print_stats(path: Path, counts: list[int], totals: list[int]) -> None:
    print(f"\n{path.name}  —  {sum(counts):,} objects, {format_bytes(sum(totals))} total")
    print(f"  {'Object size':<16} {'Count':>12} {'Total size':>12}")
    print(f"  {'-'*16} {'-'*12} {'-'*12}")
    for label, count, total in zip(LABELS, counts, totals):
        print(f"  {label:<16} {count:>12,} {format_bytes(total):>12}")


def plot(path: Path, counts: list[int], totals: list[int], output_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(LABELS, counts)

    ax.set_title(f"{path.name}  —  {sum(counts):,} objects, {format_bytes(sum(totals))} total")
    ax.set_xlabel("Object size")
    ax.set_ylabel("Count")
    ax.set_ylim(top=max(counts) * 1.1)  # space for the text annotation

    for bar, count, total in zip(bars, counts, totals):
        if count == 0:
            continue
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{count:,}\n{format_bytes(total)}",
            ha="center", va="bottom", fontsize=9,
        )

    fig.tight_layout()
    out = output_dir / (path.stem + "_histogram.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = []
    for filepath in args.files:
        path = Path(filepath)
        sizes = read_sizes(path)
        counts, totals = bucket(sizes)
        print_stats(path, counts, totals)
        written.append(plot(path, counts, totals, output_dir))

    print()
    for out in written:
        print(f"Wrote {out}")


if __name__ == "__main__":
    main()
