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

import ast
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
    for s in ["1,316", "1,234,567", "12,345.67", "298,", "1,2", "(1, 2)",
              "[1, 2]", "{'a': 1}", "1, 2, 3"]:
        assert _parses_to_python_container(s), s


def test_ignores_scalars_and_text():
    # Plain scalars (incl. numeric strings) and ordinary text are NOT containers.
    for s in ["94538", "1316", "01,316", "CEO", "Smith, John", "New York, NY",
              "Director of Marketing", "fremontbank.com", "+18003592265", ""]:
        assert not _parses_to_python_container(s), s


def test_regroups_grouped_number_zip():
    # The thoughtspot production record (zip "1,316").
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


def test_repairs_trailing_comma_address():
    # The descope production record: address "298," -> (298,) tuple -> array.
    row = {"email": "x@y.com", "address": "298,", "city": "Berlin"}
    out = prepare_for_singer(pd.DataFrame([row])).to_dict(orient="records")[0]
    assert out["address"] == "298"
    assert out["city"] == "Berlin"


def test_comma_number_variants():
    df = pd.DataFrame([{
        "a": "1,234,567",   # grouping            -> "1234567"
        "b": "12,345.67",   # grouping + decimal  -> "12345.67"
        "c": "1,316",       # grouping            -> "1316"
        "d": "298,",        # trailing comma      -> "298"
        "e": "1,2,3",       # multi-comma integer -> "123"
    }])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["a"] == "1234567"
    assert out["b"] == "12345.67"
    assert out["c"] == "1316"
    assert out["d"] == "298"
    assert out["e"] == "123"


def test_preserves_non_numeric_container_literal():
    # A container literal that is NOT a grouped number has no safe, connector-
    # agnostic rewrite, so it is left UNCHANGED (and flagged) rather than
    # silently dropped or corrupted. A European decimal "12,34" is the key case:
    # stripping its comma would wrongly yield "1234".
    df = pd.DataFrame([{"euro": "12,34", "weird": "[1, 2, 3]", "ok": "Engineer"}])
    out = prepare_for_singer(df).to_dict(orient="records")[0]
    assert out["euro"] == "12,34"
    assert out["weird"] == "[1, 2, 3]"
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


# --- Write direction (CBX1 -> CRM): neutralize_containers=True ---------------
# The CRM target (target-hubspot/-salesforce/-marketo) unstringifies values with
# ast.literal_eval, so a container-parseable string with no numeric rewrite is
# emitted as repr(value) and reconstructed downstream as the ORIGINAL scalar
# string. Regression for the thoughtspot jobtitle="120, -6" EXPORT_FAILED
# (ast.literal_eval("120, -6") -> (120, -6) -> JSON array [120, -6]).


def test_write_neutralizes_coordinate_like_jobtitle():
    # The production record: jobtitle "120, -6" -> would arrayify -> HubSpot 400.
    row = {"email": "ianbak@gmail.com", "firstname": "Ioannis", "jobtitle": "120, -6"}
    out = prepare_for_singer(
        pd.DataFrame([row]), neutralize_containers=True
    ).to_dict(orient="records")[0]
    # Emitted value round-trips through the downstream ast.literal_eval back to
    # the original scalar string (never an array/tuple).
    assert ast.literal_eval(out["jobtitle"]) == "120, -6"
    assert not isinstance(ast.literal_eval(out["jobtitle"]), (tuple, list, dict, set))
    assert out["firstname"] == "Ioannis"  # untouched


def test_write_neutralizes_all_container_shapes_losslessly():
    df = pd.DataFrame([{
        "coord": "120, -6",     # tuple of ints
        "euro": "12,34",        # ambiguous decimal (no numeric rewrite)
        "list": "[1, 2, 3]",    # list literal
        "tuple": "(1, 2)",      # tuple literal
        "dict": "{'a': 1}",     # dict literal
        "seq": "1, 2, 3",       # bare tuple
    }])
    out = prepare_for_singer(df, neutralize_containers=True).to_dict(orient="records")[0]
    for col, original in {
        "coord": "120, -6", "euro": "12,34", "list": "[1, 2, 3]",
        "tuple": "(1, 2)", "dict": "{'a': 1}", "seq": "1, 2, 3",
    }.items():
        # ast.literal_eval(repr(s)) == s -> downstream reconstructs the string.
        assert ast.literal_eval(out[col]) == original, col


def test_write_still_repairs_comma_numbers_not_repr():
    # Numeric repair takes precedence over repr-wrap: a grouped number stays the
    # clean scalar "1316", not "'1,316'".
    out = prepare_for_singer(
        pd.DataFrame([{"zip": "1,316", "address": "298,"}]), neutralize_containers=True
    ).to_dict(orient="records")[0]
    assert out["zip"] == "1316"
    assert out["address"] == "298"


def test_write_still_leaves_plain_text_and_scalars_untouched():
    # Only values ast.literal_eval turns into a container are altered; ordinary
    # comma text and plain numeric strings pass through even on the write path.
    out = prepare_for_singer(
        pd.DataFrame([{
            "name": "Smith, John",   # literal_eval fails -> untouched
            "title": "Engineer",     # untouched
            "zip": "94538",          # plain numeric string -> untouched
        }]),
        neutralize_containers=True,
    ).to_dict(orient="records")[0]
    assert out["name"] == "Smith, John"
    assert out["title"] == "Engineer"
    assert out["zip"] == "94538"


def test_read_default_leaves_container_unchanged_not_repr():
    # Direction asymmetry: the read path (default, cbx1-target does not
    # ast.literal_eval) must leave "120, -6" byte-for-byte, NOT repr-wrap it
    # (repr quotes would leak into the CBX1 payload).
    out = prepare_for_singer(
        pd.DataFrame([{"jobtitle": "120, -6"}])
    ).to_dict(orient="records")[0]
    assert out["jobtitle"] == "120, -6"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
