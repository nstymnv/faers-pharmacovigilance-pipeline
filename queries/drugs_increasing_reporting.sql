-- Business question: which drugs show a sustained increase in adverse-event
-- reporting over the last 5 years?
--
-- Growth is the least-squares slope of quarterly report counts over the 20
-- quarters, divided by the drug's mean quarterly count: 0.05 means the drug
-- gains about 5% of its average quarterly volume every quarter. Using every
-- quarter, rather than comparing two end years, ranks steady growth above a
-- single spike. Quarters with no reports count as zero.
--
-- Only implicated reports are counted (the drug named suspect or interacting).
-- Counting every role would rank a widely co-prescribed drug as increasing on
-- reports it is not suspected in.
--
-- Two cohorts, ranked separately:
-- - established: at least min_first_year_reports in the window's first year.
--   Growth here is a change in an existing drug's reporting.
-- - new entrant: below that, mostly drugs launched during the window. Their
--   growth is largely the launch itself, which would otherwise fill the whole
--   list.
-- Drugs with fewer than min_window_reports in total are not ranked.
--
-- A drug here is a product name, across every active-substance spelling it was
-- reported under. dim_drug is product x substance, and senders changed the
-- substance string of existing products mid-window (KISQALI from RIBOCICLIB to
-- RIBOCICLIB SUCCINATE in 2025), which made an old product look like a new
-- entrant with zero reports in the first year.
--
-- Report volume across FAERS as a whole fell slightly over the window
-- (report_volume_by_year.sql), so growth here is not a tide lifting all drugs.
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

-- Counted per product name from the report-level fact, not from the
-- (product, substance) rows of agg_drug_quarterly_trend: summing those would
-- count a report twice when it lists one product under two substance spellings.
product_quarterly as (

    select
        d.medicinal_product,
        t.year_quarter,
        count(distinct f.report_id) as report_count
    from faers_db.mart.fct_report_drug as f
    inner join faers_db.mart.dim_drug as d
        on f.drug_key = d.drug_key
    inner join faers_db.mart.dim_date as t
        on f.receipt_date_key = t.date_key
    where
        f.is_implicated
        and t.year_quarter between $window_start and $window_end
    group by d.medicinal_product, t.year_quarter

),

products as (

    select medicinal_product
    from product_quarterly
    group by medicinal_product
    having sum(report_count) >= $min_window_reports

),

-- Every product x every quarter, so a quarter without reports is a zero in the
-- regression rather than a missing point.
quarterly as (

    select
        p.medicinal_product,
        q.quarter_index,
        q.quarter_count,
        coalesce(a.report_count, 0) as report_count
    from products as p
    cross join quarters as q
    left join product_quarterly as a
        on
            p.medicinal_product = a.medicinal_product
            and q.year_quarter = a.year_quarter

),

growth as (

    select
        medicinal_product,
        sum(report_count) as window_reports,
        sum(iff(quarter_index < 4, report_count, 0)) as first_year_reports,
        sum(iff(quarter_index >= quarter_count - 4, report_count, 0)) as last_year_reports,
        regr_slope(report_count, quarter_index) / avg(report_count) as relative_growth_per_quarter
    from quarterly
    group by medicinal_product

),

product_substances as (

    select
        medicinal_product,
        listagg(distinct active_substance, '; ') within group (order by active_substance)
            as active_substances
    from faers_db.mart.dim_drug
    group by medicinal_product

)

select
    iff(g.first_year_reports >= $min_first_year_reports, 'established', 'new entrant') as cohort,
    g.medicinal_product,
    s.active_substances,
    g.first_year_reports,
    g.last_year_reports,
    g.window_reports,
    round(g.relative_growth_per_quarter, 3) as relative_growth_per_quarter
from growth as g
inner join product_substances as s
    on g.medicinal_product = s.medicinal_product
qualify
    row_number() over (partition by cohort order by g.relative_growth_per_quarter desc)
    <= $top_n
order by cohort asc, relative_growth_per_quarter desc;
