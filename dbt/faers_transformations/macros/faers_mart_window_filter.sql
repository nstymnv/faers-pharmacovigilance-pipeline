{% macro faers_mart_window_filter(first_quarter_column='first_seen_quarter', last_quarter_column='last_seen_quarter') %}
    {#
        Restricts the marts to reports published within the analysis window
        (mart_start_quarter..mart_end_quarter). Quarters outside it stay loaded
        in RAW and the lower layers, so moving the window is a var change rather
        than a reload.

        A report is in the window when the span of quarters its versions were
        published in overlaps the window. Quarter keys are 'YYYYqN', which sort
        chronologically as strings.

        Skipped when the faers_quarters dev slice is set: the slice (2020q1)
        sits outside the window, and filtering it again would leave the marts
        empty on every dev and CI run.
    #}
    {%- if var('faers_quarters', []) -%}
        true
    {%- else -%}
        {{ first_quarter_column }} <= '{{ var("mart_end_quarter") }}'
        and {{ last_quarter_column }} >= '{{ var("mart_start_quarter") }}'
    {%- endif -%}
{% endmacro %}
