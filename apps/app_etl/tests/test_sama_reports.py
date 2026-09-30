import importlib
import pkgutil
from datetime import date
from pathlib import Path

import pandas as pd

import app_etl.export.sama as sama
from app_etl.export.sama.common import (
    DELETED_MERCHANT_IDS,
    SCHEME_CORRECTIONS,
    TEST_MERCHANT_IDS,
    month_params,
)

DATA_POINTS = [
    m.name
    for m in pkgutil.walk_packages(sama.__path__, prefix=f"{sama.__name__}.")
    if m.name.rsplit(".", 1)[-1][:2] in {"s1", "s2", "s3", "s4"}
]


def test_test_merchant_ids_match_dbt_seed():
    seed = pd.read_csv(
        Path(__file__).parents[1] / "dbt" / "seeds" / "test_merchants.csv"
    )
    assert sorted(seed["merchant_id"]) == sorted(TEST_MERCHANT_IDS)


def test_deleted_merchant_ids_match_dbt_seed():
    seed = pd.read_csv(
        Path(__file__).parents[1] / "dbt" / "seeds" / "deleted_merchants.csv"
    )
    assert sorted(seed["merchant_id"]) == sorted(DELETED_MERCHANT_IDS)


def test_scheme_corrections_match_dbt_seed():
    seed = pd.read_csv(
        Path(__file__).parents[1] / "dbt" / "seeds" / "scheme_corrections.csv"
    )
    assert dict(zip(seed["payment_id"], seed["actual_scheme"])) == SCHEME_CORRECTIONS


def test_deleted_merchants_are_not_test_merchants():
    assert not set(DELETED_MERCHANT_IDS) & set(TEST_MERCHANT_IDS)


def test_month_params_cover_the_whole_calendar_month():
    params = {p.name: getattr(p, "value", None) for p in month_params("2028-02")}
    assert params["month_start"] == date(2028, 2, 1)
    assert params["month_end"] == date(2028, 2, 29)
    assert params["tz"] == "Asia/Riyadh"


def test_every_data_point_module_defines_its_queries():
    assert any(".emi." in name for name in DATA_POINTS)
    assert any(".pi." in name for name in DATA_POINTS)
    for name in DATA_POINTS:
        module = importlib.import_module(name)
        for attr in ("REPORT_MONTH", "DETAIL_SQL", "ANSWER_SQL"):
            assert getattr(module, attr, None), f"{name} lacks {attr}"


def test_runner_skips_the_hardcoded_zeros():
    from app_etl.export.sama.__main__ import data_points

    names = data_points()
    assert not any("sadad" in n for n in names)
    assert len(names) == len(DATA_POINTS) - 2


def test_every_run_data_point_heading_names_its_section():
    from app_etl.export.sama.__main__ import data_points

    for name in data_points():
        heading = importlib.import_module(name).__doc__.strip().splitlines()[0]
        assert heading.startswith("Section ") and ": " in heading, name


def test_flatten_one_row_answer_keeps_string_columns_as_values():
    from app_etl.export.sama.__main__ import flatten

    answer = pd.DataFrame({"volume": [3], "value_sar": [12.5], "entry_types": ["A, B"]})
    rows = flatten(answer, "EMI", "III.4", "q")
    assert list(rows["item"]) == ["volume", "value_sar", "entry_types"]
    assert list(rows["value"]) == [3, 12.5, "A, B"]


def test_flatten_grid_answer_labels_each_value():
    from app_etl.export.sama.__main__ import flatten

    answer = pd.DataFrame(
        {"channel": ["POS", "ECOM"], "scheme": ["MADA", "VISA"], "transactions": [5, 7]}
    )
    rows = flatten(answer, "PI", "III", "q")
    assert list(rows["item"]) == [
        "POS / MADA / transactions",
        "ECOM / VISA / transactions",
    ]
    assert list(rows["value"]) == [5, 7]
    assert rows["filed_value"].isna().all()
