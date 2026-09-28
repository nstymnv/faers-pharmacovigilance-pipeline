"""Find where a staged quarter no longer fits the frozen safetyreport schema.

FAERS changes its XML from time to time. 2021q4 added an element
(drugrecuractionmeddraversion) and started writing decimals and dotted
identifiers into fields that earlier quarters only ever filled with whole
numbers. Snowpark Connect reads each report's nested blocks as typed objects, so
neither kind of change is dropped quietly: it breaks the whole block, with an
error naming whichever column was read first rather than the actual cause.
Scanning the XML text first turns that into an immediate, named failure.

Two things are checked: element names the schema does not declare, and values
that do not fit a numeric field's type. An existing element whose shape changes
(a scalar becoming a nested block) is not caught and still fails at load time.
"""

import logging

from pyspark.sql.types import ArrayType, DataType, DoubleType, LongType, StructType

from faers_ingestion.config import stage_xml_prefix
from faers_ingestion.extract.scanner import safetyreport_schema

# The ichicsr message envelope around the reports. It sits outside the
# <safetyreport> rowTag, so it is never read against the schema.
ENVELOPE_ELEMENTS = frozenset(
    {
        "ichicsr",
        "ichicsrmessageheader",
        "messagetype",
        "messageformatversion",
        "messageformatrelease",
        "messagenumb",
        "messagesenderidentifier",
        "messagereceiveridentifier",
        "messagedateformat",
        "messagedate",
        "safetyreport",
    }
)

LINES_FILE_FORMAT = "faers_xml_lines"

# What a value must look like to convert into each numeric schema type.
NUMERIC_VALUE_PATTERNS = {
    LongType: r"^-?[0-9]+$",
    # A trailing dot ("62.") appears from 2022 and parses as a double.
    DoubleType: r"^-?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][-+]?[0-9]+)?$",
}

logger = logging.getLogger(__name__)


def schema_element_names(data_type: DataType) -> set[str]:
    """Every field name in a schema, at any depth, including inside arrays."""
    if isinstance(data_type, ArrayType):
        return schema_element_names(data_type.elementType)

    if not isinstance(data_type, StructType):
        return set()

    names = set()
    for field in data_type.fields:
        names.add(field.name)
        names |= schema_element_names(field.dataType)

    return names


def schema_numeric_fields(data_type: DataType) -> dict[str, type]:
    """Field name -> numeric type class, for every long or double field at any depth."""
    if isinstance(data_type, ArrayType):
        return schema_numeric_fields(data_type.elementType)

    if not isinstance(data_type, StructType):
        return {}

    fields = {}
    for field in data_type.fields:
        if type(field.dataType) in NUMERIC_VALUE_PATTERNS:
            fields[field.name] = type(field.dataType)
        fields |= schema_numeric_fields(field.dataType)

    return fields


def _create_lines_file_format(cursor) -> None:
    # One line per record, never split into fields: the XML is scanned as text.
    cursor.execute(
        f"create temporary file format if not exists {LINES_FILE_FORMAT}"
        " type = csv field_delimiter = none escape_unenclosed_field = none"
    )


def unknown_elements(conn, year: int, quarter: int) -> list[str]:
    """Element names in the quarter's staged XML that the schema does not declare.

    Runs in Snowflake over the staged files as plain text lines, one pass per
    quarter.
    """
    cursor = conn.cursor()
    _create_lines_file_format(cursor)
    rows = cursor.execute(
        f"""
        with lines as (
            select $1 as line
            from {stage_xml_prefix(year, quarter)}/ (file_format => {LINES_FILE_FORMAT})
        )
        select distinct tag.value::string
        from lines,
            lateral flatten(input => regexp_substr_all(line, '<([A-Za-z0-9_]+)', 1, 1, 'e')) as tag
        """
    ).fetchall()

    found = {row[0] for row in rows}
    known = schema_element_names(safetyreport_schema()) | ENVELOPE_ELEMENTS
    unknown = sorted(found - known)

    logger.info(
        "%sq%s: %d element names in the XML, %d not in the schema",
        year,
        quarter,
        len(found),
        len(unknown),
    )

    return unknown


def mistyped_values(conn, year: int, quarter: int) -> list[str]:
    """Numeric schema fields whose values in the quarter would not convert.

    Returns one description per field, e.g. "drugseparatedosagenumb (long):
    24 values like '0.5'". Assumes one element per line, which is how FAERS
    writes its XML.
    """
    cursor = conn.cursor()
    _create_lines_file_format(cursor)

    problems = []
    for type_class, pattern in NUMERIC_VALUE_PATTERNS.items():
        names = sorted(
            name
            for name, field_type in schema_numeric_fields(safetyreport_schema()).items()
            if field_type is type_class
        )
        tags = "|".join(names)
        rows = cursor.execute(
            f"""
            with field_values as (
                select
                    regexp_substr(trim($1), '^<({tags})>', 1, 1, 'e') as tag,
                    regexp_substr(trim($1), '^<[A-Za-z0-9_]+>([^<]*)<', 1, 1, 'e') as value
                from {stage_xml_prefix(year, quarter)}/ (file_format => {LINES_FILE_FORMAT})
                where regexp_like(trim($1), '^<({tags})>.*')
            )
            select tag, count(*), any_value(value)
            from field_values
            where value <> '' and not regexp_like(value, %s)
            group by tag
            order by tag
            """,
            (pattern,),
        ).fetchall()

        type_name = type_class.typeName()
        problems += [
            f"{tag} ({type_name}): {count} values like {example!r}" for tag, count, example in rows
        ]

    logger.info(
        "%sq%s: %d numeric fields with values that do not fit", year, quarter, len(problems)
    )

    return problems
