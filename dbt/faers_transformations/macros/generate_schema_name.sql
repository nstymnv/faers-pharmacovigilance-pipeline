{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set schema_name = target.schema if custom_schema_name is none else custom_schema_name | trim -%}

    {#-
        The ci target builds into CI_-prefixed copies of each layer, so a CI run
        never replaces the tables the dev target and the queries/ folder read.
        sql/06_create_ci_identity.sql creates those schemas.
    -#}
    {%- if target.name == 'ci' -%}
        ci_{{ schema_name }}
    {%- else -%}
        {{ schema_name }}
    {%- endif -%}

{% endmacro %}
