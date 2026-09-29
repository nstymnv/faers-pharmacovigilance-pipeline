-- Business question: which drugs show a sustained increase in adverse-event
-- reporting over the last 5 years?
--
-- Growth is the least-squares slope of quarterly report counts over the 20
-- quarters, divided by the drug's mean quarterly count: 0.05 means the drug
-- gains about 5% of its average quarterly volume every quarter. Using every
-- quarter, rather than comparing two end years, ranks steady growth above a
-- single spike. Quarters with no reports count as zero.
--
-- Two cohorts, ranked separately:
-- - established: at least min_first_year_reports in the window's first year.
--   Growth here is a change in an existing drug's reporting.
-- - new entrant: below that, mostly drugs launched during the window. Their
--   growth is largely the launch itself, which would otherwise fill the whole
--   list.
-- Drugs with fewer than min_window_reports in total are not ranked.
--
-- Report volume across FAERS as a whole fell slightly over the window
-- (00_report_volume_by_year.sql), so growth here is not a tide lifting all drugs.
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

drugs as (

    select drug_key
    from faers_db.mart.agg_drug_quarterly_trend
    where year_quarter between $window_start and $window_end
    group by drug_key
    having sum(report_count) >= $min_window_reports

),

-- Every drug x every quarter, so a quarter without reports is a zero in the
-- regression rather than a missing point.
quarterly as (

    select
        d.drug_key,
        q.quarter_index,
        q.quarter_count,
        coalesce(a.report_count, 0) as report_count
    from drugs as d
    cross join quarters as q
    left join faers_db.mart.agg_drug_quarterly_trend as a
        on
            d.drug_key = a.drug_key
            and q.year_quarter = a.year_quarter

),

growth as (

    select
        drug_key,
        sum(report_count) as window_reports,
        sum(iff(quarter_index < 4, report_count, 0)) as first_year_reports,
        sum(iff(quarter_index >= quarter_count - 4, report_count, 0)) as last_year_reports,
        regr_slope(report_count, quarter_index) / avg(report_count) as relative_growth_per_quarter
    from quarterly
    group by drug_key

)

select
    iff(g.first_year_reports >= $min_first_year_reports, 'established', 'new entrant') as cohort,
    d.medicinal_product,
    d.active_substance,
    g.first_year_reports,
    g.last_year_reports,
    g.window_reports,
    round(g.relative_growth_per_quarter, 3) as relative_growth_per_quarter
from growth as g
inner join faers_db.mart.dim_drug as d
    on g.drug_key = d.drug_key
qualify
    row_number() over (partition by cohort order by g.relative_growth_per_quarter desc)
    <= $top_n
order by cohort asc, relative_growth_per_quarter desc;
