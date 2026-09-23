{% macro faers_age_years(age_column, unit_column) %}
    {#
        Normalizes a FAERS patient age to years.

        patientonsetage is meaningless on its own: a value of 6 is six years,
        six days or six decades depending on patientonsetageunit. 2020q1 carries
        263,661 ages in Year but also 2,690 in Day, 1,818 in Month, 1,732 in
        Decade, 175 in Week and 11 in Hour, so treating the raw number as years
        would put newborns in their sixties and centenarians in infancy.

        Day/Week/Hour conversions use the mean Gregorian year (365.25 days) —
        the alternative, 365, drifts by about a year over a century-long age,
        which is larger than the precision the source supports anyway.

        The time_unit_code_mapping seed also carries codes that are dosing
        schedules rather than durations — Cyclical, Trimester, As Necessary,
        Total. None appear as an age unit in 2020q1, but they are valid values
        of the column, and an age expressed in "As Necessary" has no numeric
        meaning, so they fall through to null rather than being coerced.

        Results outside 0-120 years become null. 2020q1 carries onset ages down
        to -5 years, which cannot be repaired — only excluded — and a negative
        age silently drags down any mean. The raw onset_age and age_unit stay in
        int_demographics, so nothing is lost and the discarded values remain
        countable by the accepted_range test on this column.
    #}
    {%- set years -%}
        case {{ unit_column }}
            when 'Decade' then {{ age_column }} * 10
            when 'Year' then {{ age_column }}
            when 'Month' then {{ age_column }} / 12
            when 'Week' then {{ age_column }} * 7 / 365.25
            when 'Day' then {{ age_column }} / 365.25
            when 'Hour' then {{ age_column }} / (365.25 * 24)
            when 'Minute' then {{ age_column }} / (365.25 * 24 * 60)
            when 'Second' then {{ age_column }} / (365.25 * 24 * 60 * 60)
        end
    {%- endset -%}
    case
        when ({{ years }}) between 0 and 120 then round(({{ years }})::float, 2)
    end
{% endmacro %}
