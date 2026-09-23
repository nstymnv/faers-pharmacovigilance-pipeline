{% macro faers_drug_role_rank(column_name) %}
    {#
        Attribution rank for a FAERS drug characterization, highest = most
        strongly implicated in the event.

        A report lists a drug once per administered dose, and the same product
        can carry different characterizations across its entries — one dose
        recorded as suspect, another as concomitant. Collapsing those entries to
        one row per (report, product) therefore has to pick a role, and the
        strongest attribution has to win: a product the reporter named as
        suspect even once is a suspect drug for that case, and letting an
        arbitrary concomitant entry mask it would drop the report out of every
        disproportionality calculation.

        Ranking rather than a boolean because the surviving role is published on
        fct_report_drug, not just the yes/no flag derived from it.
    #}
    case {{ column_name }}
        when 'suspect' then 3
        when 'interacting' then 2
        when 'concomitant' then 1
        else 0
    end
{% endmacro %}
