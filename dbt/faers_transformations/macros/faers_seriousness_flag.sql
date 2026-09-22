{% macro faers_seriousness_flag(column_name) %}
    {#
        Decodes one of the six FAERS seriousness-criteria fields to a boolean.

        These are presence flags, not 1/2 enumerations: FAERS emits 1 when the
        criterion applies and omits the element entirely when it does not. The
        code 2 never appears. 2020q1 confirms the reading exactly — all 260,578
        reports with serious = 1 carry at least one flag, and all 199,749 with
        serious = 2 carry none, with no exceptions either way.

        Absence therefore decodes to false, not null. Leaving it null would make
        the column true-or-null and silently break any aggregate that ignores
        nulls: avg(death::int) over a set of reports would return 1.0. 2 is
        mapped to false as well, so a future quarter that does populate it
        behaves sensibly.
    #}
    coalesce({{ column_name }} = 1, false)
{% endmacro %}
