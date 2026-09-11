#!/usr/bin/env python
# coding: utf-8

"""Tests that a run starts from an empty Singer output file.

HotGlue retries the transform script in-process without clearing etl-output, and
both writers append. Without a reset the retry appends a second full copy of the
dataset — duplicate records downstream, and ENOSPC on a large tenant.

Run standalone:  python hubspot/tests/test_singer_output_reset.py
Run via pytest:  pytest hubspot/tests/test_singer_output_reset.py
"""

import errno
import json
import os
import sys
import tempfile

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import append_singer_records, reset_singer_output  # noqa: E402


CHUNK = pd.DataFrame([
    {"data": {"email": "a@b.com"}, "sourceRecordId": "1", "source": "HUBSPOT",
     "lookupKey": "a@b.com"},
])


def _messages(output_dir):
    with open(os.path.join(output_dir, "data.singer"), encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def test_reset_removes_previous_attempt_output():
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)
        assert os.path.isfile(os.path.join(out, "data.singer"))

        reset_singer_output(out)
        assert not os.path.exists(os.path.join(out, "data.singer"))


def test_reset_is_a_noop_when_nothing_was_written():
    with tempfile.TemporaryDirectory() as out:
        reset_singer_output(out)
        assert not os.path.exists(os.path.join(out, "data.singer"))


def test_retry_after_reset_does_not_duplicate_records():
    """A second attempt of the same job must not append onto the first's file."""
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)
        first_attempt = _messages(out)

        reset_singer_output(out)
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)

        assert _messages(out) == first_attempt
        assert [m["type"] for m in _messages(out)] == ["SCHEMA", "RECORD"]


def test_without_reset_a_retry_would_duplicate():
    """Guards the premise: append mode alone re-emits everything."""
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)

        assert [m["type"] for m in _messages(out)] == [
            "SCHEMA", "RECORD", "SCHEMA", "RECORD",
        ]


def test_multiple_streams_in_one_run_still_share_the_file():
    """The reset is per run, not per stream — later streams must still append."""
    with tempfile.TemporaryDirectory() as out:
        reset_singer_output(out)
        append_singer_records(CHUNK, "contacts", out, first_chunk=True)
        append_singer_records(CHUNK, "companies", out, first_chunk=True)

        assert [m["stream"] for m in _messages(out)] == [
            "contacts", "contacts", "companies", "companies",
        ]


def test_enospc_is_reported_before_it_propagates():
    """ENOSPC must be logged naming the output file, then re-raised unchanged."""
    import logging
    from unittest.mock import patch

    import utils

    messages = []

    class _Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    capture = _Capture()
    utils.logger.addHandler(capture)
    try:
        with tempfile.TemporaryDirectory() as out:
            with patch("builtins.open", side_effect=OSError(errno.ENOSPC, "No space left on device")):
                try:
                    append_singer_records(CHUNK, "contacts", out, first_chunk=True)
                except OSError as exc:
                    assert exc.errno == errno.ENOSPC
                else:
                    raise AssertionError("ENOSPC was swallowed")
    finally:
        utils.logger.removeHandler(capture)

    assert any("ENOSPC" in m and "data.singer" in m for m in messages), messages


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
