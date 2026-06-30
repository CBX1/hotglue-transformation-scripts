#!/usr/bin/env python
# coding: utf-8

"""Tests for the NaN/Infinity-spelling string scrub in ``prepare_for_singer``.

Regression for the CBX1 -> HubSpot export failure where a contact whose
``jobtitle`` was the literal string ``"NaN"`` caused the hotglue target-hubspot
to re-serialize the value as the non-standard JSON token ``NaN``, which the
HubSpot (Jackson) API rejects with
``Non-standard token 'NaN': enable JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS``.

Run standalone:  python hubspot/tests/test_prepare_for_singer_nan.py
Run via pytest:  pytest hubspot/tests/test_prepare_for_singer_nan.py
"""

import os
import sys

import numpy as np
import pandas as pd

# Make the connector modules (utils.py lives at hubspot/) importable whether
# the tests are run from the repo root or from hubspot/.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (  # noqa: E402
    _spells_nonfinite_float,
    prepare_for_singer,
)


def test_detects_nonfinite_spellings():
    for s in ["NaN", "nan", "NAN", "-NaN", "Infinity", "-Infinity",
              "inf", "-inf", "Inf", "INF", "+inf", "  NaN  "]:
        assert _spells_nonfinite_float(s), s


def test_preserves_safe_values():
    # NA-like sentinels pandas keeps as strings (valid JSON downstream) and
    # ordinary values that merely contain the substring must be preserved.
    for s in ["N/A", "None", "NA", "n/a", "null", "NULL", "<NA>", "#N/A",
              "1.#IND", "", "CEO", "Infinity Ward", "nanjing", "Siyu"]:
        assert not _spells_nonfinite_float(s), s
    for v in [None, 42, 3.14, True]:
        assert not _spells_nonfinite_float(v), v


def test_nulls_top_level_nan_string():
    # The exact production record that broke the export.
    row = {
        "email": "siyu.liu2024@gmail.com",
        "firstname": "Siyu",
        "jobtitle": "NaN",
        "lastname": "Liu",
    }
    out = prepare_for_singer(pd.DataFrame([row])).to_dict(orient="records")[0]
    assert out["jobtitle"] is None
    assert out["firstname"] == "Siyu"
    assert out["email"] == row["email"]


def test_scrubs_nested_and_leaves_numeric_and_datetime():
    df = pd.DataFrame([{
        "jobtitle": "Infinity",
        "good": "CEO",
        "na_string": "N/A",                  # must be preserved
        "nested": {"a": "inf", "b": "ok"},   # nested spelling -> None
        "arr": ["NaN", "keep"],              # spelling inside a list -> None
        "score": np.float64("nan"),          # numeric NaN: left for the writer
        "when": pd.Timestamp("2026-06-30T01:11:40.684Z"),
    }])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["jobtitle"] is None
    assert out["good"] == "CEO"
    assert out["na_string"] == "N/A"
    assert out["nested"] == {"a": None, "b": "ok"}
    assert out["arr"] == [None, "keep"]
    assert out["when"] == "2026-06-30T01:11:40.684000Z"


def test_does_not_mutate_input():
    df = pd.DataFrame([{"jobtitle": "NaN"}])
    prepare_for_singer(df)
    assert df["jobtitle"].iloc[0] == "NaN"  # original untouched (df.copy())


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
