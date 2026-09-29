-- Business question: what adverse reactions are reported in expedited cases?
--
-- Ranked by the number of expedited reports listing the reaction. The
-- over-representation column compares the reaction's share of expedited reports
-- with its share of all reports: above 1 means the reaction is more typical of
-- expedited (serious and unexpected) cases than of FAERS as a whole.
--
-- Shares are of reports, and a report lists several reactions, so they do not
-- add up to 100%. The denominators come from fct_report, because summing report
-- counts across reactions would count each report once per reaction.
set window_start = '2021Q1';
set window_end = '2025Q4';

with report_totals as (

    select
        count(*) as report_count,
        count_if(r.is_expedited) as expedited_report_count
    from faers_db.mart.fct_report as r
    inner join faers_db.mart.dim_date as t
        on r.receipt_date_key = t.date_key
    where t.year_quarter between $window_start and $window_end

),

reaction_totals as (

    select
        reaction_key,
        sum(report_count) as report_count,
        sum(expedited_report_count) as expedited_report_count
    from faers_db.mart.agg_reaction_frequency
    where year_quarter between $window_start and $window_end
    group by reaction_key

)

select
    x.reaction,
    x.custom_reaction_group_label,
    k.expedited_report_count,
    round(k.expedited_report_count / t.expedited_report_count, 4) as share_of_expedited_reports,
    round(k.report_count / t.report_count, 4) as share_of_all_reports,
    round(
        (k.expedited_report_count / t.expedited_report_count)
        / (k.report_count / t.report_count),
        2
    ) as expedited_over_representation
from reaction_totals as k
inner join faers_db.mart.dim_reaction as x
    on k.reaction_key = x.reaction_key
cross join report_totals as t
order by k.expedited_report_count desc
limit 20;
