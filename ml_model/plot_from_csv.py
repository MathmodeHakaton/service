"""Render LSI dashboard charts from an already-produced lsi_timeseries.csv.

Usage:
    python plot_from_csv.py                       # default: outputs/lsi_timeseries.csv -> outputs/charts/
    python plot_from_csv.py --csv path.csv --out-dir charts/
"""
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.plotting import plot_dashboard


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="outputs/lsi_timeseries.csv")
    parser.add_argument("--out-dir", type=str, default="outputs/charts")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    paths = plot_dashboard(df, Path(args.out_dir))
    for p in paths:
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
