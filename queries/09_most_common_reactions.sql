-- Business question: what are the most commonly reported adverse reactions over
-- the last 5 years?
--
-- Ranked by the number of reports listing the reaction (MedDRA preferred term).
-- A report lists several reactions, so shares do not add up to 100%, and the
-- denominator is the report total from fct_report, not a sum over reactions.
--
-- custom_reaction_group_basis says how far to trust the grouping: 'rule' means
-- an explicit mapping rule assigned it, 'fallback' that it landed in a
-- catch-all.
set window_start = '2021Q1';
set window_end = '2025Q4';

with report_totals as (

    select count(*) as report_count
    from faers_db.mart.fct_report as r
    inner join faers_db.mart.dim_date as t
        on r.receipt_date_key = t.date_key
    where t.year_quarter between $window_start and $window_end

),

reaction_totals as (

    select
        reaction_key,
        sum(report_count) as report_count,
        sum(serious_report_count) as serious_report_count,
        sum(fatal_report_count) as fatal_report_count
    from faers_db.mart.agg_reaction_frequency
    where year_quarter between $window_start and $window_end
    group by reaction_key

)

select
    x.reaction,
    x.custom_reaction_group_label,
    x.custom_reaction_group_basis,
    k.report_count,
    round(k.report_count / t.report_count, 4) as share_of_all_reports,
    round(k.serious_report_count / k.report_count, 3) as serious_share,
    k.fatal_report_count
from reaction_totals as k
inner join faers_db.mart.dim_reaction as x
    on k.reaction_key = x.reaction_key
cross join report_totals as t
order by k.report_count desc
limit 20;
