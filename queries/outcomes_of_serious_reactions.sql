-- Business question: what are the most common outcomes for serious reactions?
--
-- Outcome is recorded per reaction, so this counts reactions on serious reports,
-- with the same breakdown for non-serious reports alongside for contrast.
-- Where a report gave conflicting outcomes for one reaction, the intermediate
-- layer kept the most severe. Ordered by clinical severity, fatal first.
set window_start = '2021Q1';
set window_end = '2025Q4';

with outcome_totals as (

    select
        coalesce(x.outcome, 'Not reported') as outcome,
        max(x.outcome_severity_rank) as outcome_severity_rank,
        count_if(r.is_serious) as serious_reaction_count,
        count_if(not r.is_serious) as non_serious_reaction_count
    from faers_db.mart.fct_report_reaction as x
    inner join faers_db.mart.fct_report as r
        on x.report_id = r.report_id
    inner join faers_db.mart.dim_date as t
        on x.receipt_date_key = t.date_key
    where t.year_quarter between $window_start and $window_end
    group by coalesce(x.outcome, 'Not reported')

)

select
    outcome,
    serious_reaction_count,
    round(ratio_to_report(serious_reaction_count) over (), 3) as share_of_serious_reactions,
    round(ratio_to_report(non_serious_reaction_count) over (), 3) as share_of_non_serious_reactions
from outcome_totals
order by outcome_severity_rank desc;
