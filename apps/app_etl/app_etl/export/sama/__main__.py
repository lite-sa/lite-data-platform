"""Run every SAMA data point for one month into a single CSV, one row per
value: `python -m app_etl.export.sama --month 2026-08`.
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil

import pandas as pd

import app_etl.export.sama as sama
from app_etl.export.sama.common import run

# Hardcoded zeros, filed by hand for now.
SKIPPED = {"emi.s3_03_in_sadad", "pi.s1_03_sadad_billers"}

COLUMNS = ["sheet", "section", "question", "item", "value", "filed_value"]


def data_points() -> list[str]:
    names = [
        m.name
        for m in pkgutil.walk_packages(sama.__path__, prefix=f"{sama.__name__}.")
        if m.name.rsplit(".", 1)[-1][:2] in {"s1", "s2", "s3", "s4"}
    ]
    return sorted(n for n in names if n.split(".", 3)[-1] not in SKIPPED)


def flatten(
    answer: pd.DataFrame, sheet: str, section: str, question: str
) -> pd.DataFrame:
    # In a grid answer the string columns label each row; a one-row answer
    # has no labels, so its string columns are values.
    labels = [
        c
        for c in answer.columns
        if len(answer) > 1 and not pd.api.types.is_numeric_dtype(answer[c])
    ]
    long = answer.astype({c: object for c in answer.columns if c not in labels}).melt(
        id_vars=labels, var_name="column", value_name="value"
    )
    long["item"] = long[labels + ["column"]].astype(str).agg(" / ".join, axis=1)
    long["sheet"], long["section"], long["question"] = sheet, section, question
    long["filed_value"] = None
    return long[COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="report month, YYYY-MM")
    parser.add_argument("--out", help="output path, default sama_<month>.csv")
    args = parser.parse_args()

    frames = []
    for name in data_points():
        module = importlib.import_module(name)
        # first docstring line: "Section I.1: total outstanding e-money ..."
        heading = module.__doc__.strip().splitlines()[0]
        section, question = heading.removeprefix("Section ").split(": ", 1)
        sheet = name.split(".")[-2].upper()
        print(f"{sheet} {section}")
        answer = run(module.ANSWER_SQL, args.month)
        frames.append(flatten(answer, sheet, section, question.rstrip(".")))

    out = args.out or f"sama_{args.month}.csv"
    pd.concat(frames, ignore_index=True).to_csv(out, index=False)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
