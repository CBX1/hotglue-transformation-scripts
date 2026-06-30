#!/usr/bin/env python
# coding: utf-8

"""Tests for the container-literal scrub in ``prepare_for_singer``.

Regression for the CBX1 -> HubSpot export failure where a contact whose
``zip`` was the string ``"1,316"`` caused the hotglue target-hubspot to reject
the record with:

    Cannot deserialize value of type `java.lang.String` from Array value
    (token `JsonToken.START_ARRAY`)

The downstream read path unstringifies values with ``ast.literal_eval``, and
``"1,316"`` is a valid Python tuple literal ``(1, 316)`` -> JSON array
``[1, 316]``. HubSpot's ``zip`` property is a String, so the array is rejected.

Run standalone:  python hubspot/tests/test_prepare_for_singer_container_literals.py
Run via pytest:  pytest hubspot/tests/test_prepare_for_singer_container_literals.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (  # noqa: E402
    _parses_to_python_container,
    prepare_for_singer,
)


def test_detects_container_literals():
    # These become tuples/lists/dicts via ast.literal_eval -> JSON arrays/objects.
    for s in ["1,316", "1,234,567", "12,345.67", "1,2", "(1, 2)", "[1, 2]",
              "{'a': 1}", "1, 2, 3"]:
        assert _parses_to_python_container(s), s


def test_ignores_scalars_and_text():
    # Plain scalars (incl. numeric strings) and ordinary text are NOT containers.
    for s in ["94538", "1316", "01,316", "CEO", "Smith, John", "New York, NY",
              "Director of Marketing", "fremontbank.com", "+18003592265", ""]:
        assert not _parses_to_python_container(s), s


def test_regroups_grouped_number_zip():
    # The exact production record that broke the export.
    row = {
        "email": "laura.owen@fremontbank.com",
        "firstname": "Laura",
        "zip": "1,316",
        "state": "CA",
    }
    out = prepare_for_singer(pd.DataFrame([row])).to_dict(orient="records")[0]
    assert out["zip"] == "1316"          # comma stripped, stays a scalar string
    assert out["state"] == "CA"
    assert out["firstname"] == "Laura"


def test_grouped_number_variants():
    df = pd.DataFrame([{
        "a": "1,234,567",   # -> "1234567"
        "b": "12,345.67",   # -> "12345.67"
        "c": "1,316",       # -> "1316"
    }])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["a"] == "1234567"
    assert out["b"] == "12345.67"
    assert out["c"] == "1316"


def test_drops_non_numeric_container_literal():
    # A container literal that is NOT a grouped number is dropped (can't be a
    # valid scalar property value) rather than emitted as an array/object.
    df = pd.DataFrame([{"weird": "[1, 2, 3]", "ok": "Engineer"}])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["weird"] is None
    assert out["ok"] == "Engineer"


def test_preserves_comma_text_and_numeric_strings():
    # Only values that WOULD break downstream are altered; everything else
    # passes through untouched.
    df = pd.DataFrame([{
        "name": "Smith, John",       # literal_eval fails -> untouched
        "city2": "New York, NY",     # untouched
        "zip_ok": "94538",           # plain numeric string -> untouched
        "zip_zero": "01,316",        # SyntaxError (leading zero) -> untouched
    }])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["name"] == "Smith, John"
    assert out["city2"] == "New York, NY"
    assert out["zip_ok"] == "94538"
    assert out["zip_zero"] == "01,316"


def test_does_not_mutate_input():
    df = pd.DataFrame([{"zip": "1,316"}])
    prepare_for_singer(df)
    assert df["zip"].iloc[0] == "1,316"  # original untouched (df.copy())


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
