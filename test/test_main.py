import argparse

import pytest

from faers_ingestion.main import parse_quarter


@pytest.mark.parametrize(
    ("value", "expected"),
    [("2021q1", (2021, 1)), ("2025q4", (2025, 4)), ("2021Q3", (2021, 3))],
)
def test_parse_quarter_accepts_quarter_keys(value, expected):
    assert parse_quarter(value) == expected


@pytest.mark.parametrize("value", ["2021q5", "2021q0", "21q1", "2021-q1", "1999q1", "2021q1x"])
def test_parse_quarter_rejects_malformed_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_quarter(value)
