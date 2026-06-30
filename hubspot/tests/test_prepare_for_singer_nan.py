#!/usr/bin/env python
# coding: utf-8

"""Tests for the NaN/Infinity-token scrub in ``prepare_for_singer``.

Regression for the CBX1 -> HubSpot export failure where a contact whose
``jobtitle`` was the literal string ``"NaN"`` caused the hotglue target-hubspot
to re-serialize the value as the non-standard JSON token ``NaN``, which the
HubSpot (Jackson) API rejects with
``Non-standard token 'NaN': enable JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS``.

Matching is CASE-SENSITIVE and exact: production proved the downstream
distinguishes ``"NaN"`` (broke the export) from ``"Nan"`` (a given name that
synced fine), so only the exact ECMAScript tokens are nulled.

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


def test_detects_nonfinite_tokens():
    # Only the exact ECMAScript non-numeric number literals are dangerous.
    for s in ["NaN", "Infinity", "-Infinity"]:
        assert _spells_nonfinite_float(s), s


def test_preserves_safe_values():
    # Case/spelling variants that the downstream does NOT re-emit as bare
    # tokens, NA-like sentinels, names, and substrings — all must be preserved.
    for s in ["Nan", "nan", "NAN", "-NaN", "inf", "-inf", "Inf", "INF", "+inf",
              "  NaN  ", "infinity", "N/A", "None", "NA", "n/a", "null", "NULL",
              "<NA>", "#N/A", "1.#IND", "", "CEO", "Infinity Ward", "nanjing",
              "Siyu"]:
        assert not _spells_nonfinite_float(s), s
    for v in [None, 42, 3.14, True]:
        assert not _spells_nonfinite_float(v), v


def test_case_sensitive_preserves_nan_name():
    # Regression: a case-insensitive match silently nulled the given name "Nan"
    # (4 real contacts in the thoughtspot data) even though it syncs fine.
    df = pd.DataFrame([{"firstname": "Nan", "lastname": "Patel", "jobtitle": "nan"}])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["firstname"] == "Nan"
    assert out["lastname"] == "Patel"
    assert out["jobtitle"] == "nan"   # lowercase, not the exact token -> kept


def test_nulls_top_level_nan_token():
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
        "jobtitle": "Infinity",                    # exact token -> None
        "good": "CEO",
        "na_string": "N/A",                        # must be preserved
        "nested": {"a": "-Infinity", "b": "ok"},   # nested token -> None
        "arr": ["NaN", "keep", "inf"],             # only exact "NaN" -> None
        "score": np.float64("nan"),                # numeric NaN: left for writer
        "when": pd.Timestamp("2026-06-30T01:11:40.684Z"),
    }])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["jobtitle"] is None
    assert out["good"] == "CEO"
    assert out["na_string"] == "N/A"
    assert out["nested"] == {"a": None, "b": "ok"}
    assert out["arr"] == [None, "keep", "inf"]     # "inf" kept (not exact token)
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
