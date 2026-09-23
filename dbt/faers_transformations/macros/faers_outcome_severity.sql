{% macro faers_outcome_severity(column_name) %}
    {#
        Clinical severity rank for a FAERS reaction outcome, highest = worst.

        A report can list the same reaction more than once with conflicting
        outcomes (5,612 such pairs in 2020q1 alone — e.g. "fatal" alongside
        "recovered/resolved"). Collapsing them has to be deterministic, and
        standard pharmacovigilance practice is to let the worst outcome stand:
        a fatal outcome is never masked by a co-reported recovery.

        Sequelae outranks "not recovered" because sequelae is permanent damage,
        whereas an unresolved event may still resolve after the report was filed.
        "unknown" ranks lowest so it never wins over a substantive outcome.
    #}
    case {{ column_name }}
        when 'fatal' then 6
        when 'recovered/resolved with sequelae' then 5
        when 'not recovered/not resolved' then 4
        when 'recovering/resolving' then 3
        when 'recovered/resolved' then 2
        when 'unknown' then 1
        else 0
    end
{% endmacro %}
