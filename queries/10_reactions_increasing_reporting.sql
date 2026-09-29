-- Business question: which adverse reactions are becoming more frequently
-- reported over the last 5 years?
--
-- Same method as 07_drugs_increasing_reporting.sql: least-squares slope of the
-- quarterly report counts over the 20 quarters, divided by the mean quarterly
-- count, with empty quarters as zero. Established and new-entrant reactions are
-- ranked separately; a "new" reaction term usually means a new MedDRA term or a
-- new product's characteristic reaction rather than a new phenomenon.
set window_start = '2021Q1';
set window_end = '2025Q4';
set min_window_reports = 1000;
set min_first_year_reports = 100;
set top_n = 15;

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

reactions as (

    select reaction_key
    from faers_db.mart.agg_reaction_frequency
    where year_quarter between $window_start and $window_end
    group by reaction_key
    having sum(report_count) >= $min_window_reports

),

quarterly as (

    select
        x.reaction_key,
        q.quarter_index,
        q.quarter_count,
        coalesce(a.report_count, 0) as report_count
    from reactions as x
    cross join quarters as q
    left join faers_db.mart.agg_reaction_frequency as a
        on
            x.reaction_key = a.reaction_key
            and q.year_quarter = a.year_quarter

),

growth as (

    select
        reaction_key,
        sum(report_count) as window_reports,
        sum(iff(quarter_index < 4, report_count, 0)) as first_year_reports,
        sum(iff(quarter_index >= quarter_count - 4, report_count, 0)) as last_year_reports,
        regr_slope(report_count, quarter_index) / avg(report_count) as relative_growth_per_quarter
    from quarterly
    group by reaction_key

)

select
    iff(g.first_year_reports >= $min_first_year_reports, 'established', 'new entrant') as cohort,
    x.reaction,
    x.custom_reaction_group_label,
    g.first_year_reports,
    g.last_year_reports,
    g.window_reports,
    round(g.relative_growth_per_quarter, 3) as relative_growth_per_quarter
from growth as g
inner join faers_db.mart.dim_reaction as x
    on g.reaction_key = x.reaction_key
qualify
    row_number() over (partition by cohort order by g.relative_growth_per_quarter desc)
    <= $top_n
order by cohort asc, relative_growth_per_quarter desc;
