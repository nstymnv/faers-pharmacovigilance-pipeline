import pytest

from faers_ingestion.extract.schema_drift import unknown_elements


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, statement):
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return FakeCursor(self.rows)


def test_unstaged_quarter_fails_instead_of_passing_the_drift_check():
    # No staged files means no element names at all. Passing that as "no drift"
    # would let the load replace the quarter's rows with an empty read.
    with pytest.raises(FileNotFoundError, match="no XML is staged"):
        unknown_elements(FakeConnection([]), 2021, 1)


def test_element_missing_from_the_schema_is_reported():
    rows = [("safetyreport",), ("safetyreportid",), ("notinschema",)]

    assert unknown_elements(FakeConnection(rows), 2021, 1) == ["notinschema"]
