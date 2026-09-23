-- Grain: one row per (reaction_key, year_quarter).
--
-- Answers "what are the most commonly reported adverse reactions" and "which
-- reactions are becoming more frequently reported". expedited_report_count is
-- here so that "what reactions are reported in expedited cases" is answerable
-- from the same table rather than needing a second pass over the facts.
--
-- report_count and reaction_count are the same number at this grain, because
-- int_reactions holds one row per (report, reaction) — a report cannot report
-- the same reaction twice. Only report_count is published, to avoid offering
-- two names for one measure.
select
    x.reaction_key,
    t.year_quarter,
    count(distinct x.report_id) as report_count,
    count(distinct iff(x.is_fatal, x.report_id, null)) as fatal_report_count,
    count(distinct iff(r.is_serious, x.report_id, null)) as serious_report_count,
    count(distinct iff(r.is_expedited, x.report_id, null)) as expedited_report_count
from {{ ref('fct_report_reaction') }} as x
inner join {{ ref('fct_report') }} as r
    on x.report_id = r.report_id
inner join {{ ref('dim_date') }} as t
    on x.receipt_date_key = t.date_key
group by x.reaction_key, t.year_quarter
