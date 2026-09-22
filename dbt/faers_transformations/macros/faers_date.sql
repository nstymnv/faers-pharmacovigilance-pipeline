{% macro faers_to_date(column_name) %}
    {#
        FAERS reports these date fields at whatever precision the sender had:
        a full 8-digit YYYYMMDD, a 6-digit YYYYMM, or a bare 4-digit YYYY. A
        straight try_to_date(..., 'YYYYMMDD') returns null for the shorter two,
        dropping 29% of drug start dates from any date-bucketed trend, so each
        supported precision is parsed and the missing month/day default to 01.

        The length is checked explicitly rather than left to try_to_date,
        because try_to_date is lenient about how much of the mask it consumes:
        try_to_date('150', 'YYYY') happily returns year 0150 instead of
        rejecting a value that is too short to be a year. Gating on an exact
        length of 8, 6 or 4 means anything else — a truncated value, a stray
        digit — becomes null rather than a plausible-looking wrong date.

        Padding partial dates to the 1st makes year- and quarter-grain trends
        sound, but NOT month- or day-grain seasonality: 81,428 drug start dates
        are year-only and would all land on 1 January.
    #}
    {%- set value = "trim(to_varchar(" ~ column_name ~ "))" -%}
    case length({{ value }})
        when 8 then try_to_date({{ value }}, 'YYYYMMDD')
        when 6 then try_to_date({{ value }}, 'YYYYMM')
        when 4 then try_to_date({{ value }}, 'YYYY')
    end
{% endmacro %}
