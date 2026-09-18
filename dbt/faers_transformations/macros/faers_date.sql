{% macro faers_to_date(column_name) %}
    {#
        FAERS sometimes reports these date fields with only year ("2023") or
        year+month ("202303") precision instead of a full 8-digit date. A
        straight `try_to_date(..., 'YYYYMMDD')` silently returns null for
        those, dropping the record from any date-bucketed trend query. This
        falls back through progressively looser masks and takes the first
        match, defaulting the missing month/day to 01, so a real date is
        produced whenever any precision is available.
    #}
    coalesce(
        try_to_date(to_varchar({{ column_name }}), 'YYYYMMDD'),
        try_to_date(to_varchar({{ column_name }}), 'YYYYMM'),
        try_to_date(to_varchar({{ column_name }}), 'YYYY')
    )
{% endmacro %}
