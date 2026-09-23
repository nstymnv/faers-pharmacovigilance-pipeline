-- Grain: one row per (outcome, year_quarter).
--
-- Answers "what are the most common outcomes for these reactions" and "which
-- outcome categories are growing over the last 5 years".
--
-- reaction_count and report_count differ here and both are meaningful: one
-- report can carry several reactions that resolved differently, so a single
-- fatal case contributes one report to 'fatal' and may also contribute
-- reactions to 'recovered/resolved'. Counting reports across outcomes therefore
-- double-counts reports on purpose — the question is about outcomes, not cases.
select
    coalesce(x.outcome, 'Not reported') as outcome,
    t.year_quarter,
    max(x.outcome_severity_rank) as outcome_severity_rank,
    count(*) as reaction_count,
    count(distinct x.report_id) as report_count
from {{ ref('fct_report_reaction') }} as x
inner join {{ ref('dim_date') }} as t
    on x.receipt_date_key = t.date_key
group by coalesce(x.outcome, 'Not reported'), t.year_quarter
