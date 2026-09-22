{% macro faers_quarter_filter(column_name='source_quarter') %}
    {#
        Restricts a staging model to the quarters named by the faers_quarters
        var, so a dev or CI run can rebuild against one quarter instead of a
        full backfill. An empty var (the default) means all quarters, and the
        macro emits `true` rather than nothing so it composes with a following
        `and` without the caller having to know which form it produced.
    #}
    {%- set quarters = var('faers_quarters', []) -%}
    {%- if quarters -%}
        {{ column_name }} in ({{ "'" ~ quarters | join("', '") ~ "'" }})
    {%- else -%}
        true
    {%- endif -%}
{% endmacro %}
