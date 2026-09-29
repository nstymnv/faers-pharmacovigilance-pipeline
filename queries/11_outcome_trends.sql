-- Business question: which outcome categories are growing over the last 5
-- years?
--
-- Counts reactions by their resolved outcome. Two measures, because total
-- report volume changed over the window: relative_growth_per_quarter (same
-- method as 07) says whether the raw count grew, the first- and last-year
-- shares say whether the outcome grew as a proportion of all reactions.
set window_start = '2021Q1';
set window_end = '2025Q4';

with quarters as (

    select
        year_quarter,
        row_number() over (order by year_quarter) - 1 as quarter_index,
        count(*) over () as quarter_count
    from (
        select distinct year_quarter
        from faers_db.mart.dim_date
        where year_quarter between $window_start and $window_end
    ) as window_quarters

),

quarterly as (

    select
        a.outcome,
        a.outcome_severity_rank,
        q.quarter_index,
        q.quarter_count,
        a.reaction_count
    from faers_db.mart.agg_outcome_trend as a
    inner join quarters as q
        on a.year_quarter = q.year_quarter

),

outcome_totals as (

    select
        outcome,
        max(outcome_severity_rank) as outcome_severity_rank,
        sum(iff(quarter_index < 4, reaction_count, 0)) as first_year_reactions,
        sum(iff(quarter_index >= quarter_count - 4, reaction_count, 0)) as last_year_reactions,
        -- Every outcome is reported in every quarter, so no zero-filling is
        -- needed here, unlike the drug and reaction growth queries.
        regr_slope(reaction_count, quarter_index) / avg(reaction_count)
            as relative_growth_per_quarter
    from quarterly
    group by outcome

)

select
    outcome,
    first_year_reactions,
    last_year_reactions,
    round(ratio_to_report(first_year_reactions) over (), 3) as first_year_share,
    round(ratio_to_report(last_year_reactions) over (), 3) as last_year_share,
    round(relative_growth_per_quarter, 3) as relative_growth_per_quarter
from outcome_totals
order by outcome_severity_rank desc;
