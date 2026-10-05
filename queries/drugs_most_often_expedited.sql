-- Business question: which drugs most frequently meet the expedited-reporting
-- criteria over the last 5 years?
--
-- Ranked by the number of expedited reports where the drug was implicated
-- (suspect or interacting): the volume counterpart of
-- drugs_highest_expedited_share.sql, which ranks by share. Without the role
-- filter, drugs that are simply taken by many patients (aspirin, statins,
-- amlodipine) fill the list on reports they are not suspected in.
--
-- The first- and last-year columns show whether a drug's expedited volume is
-- rising or falling.
set window_start = '2021Q1';
set window_end = '2025Q4';

with window_years as (

    select
        min(year) as first_year,
        max(year) as last_year
    from faers_db.mart.dim_date
    where year_quarter between $window_start and $window_end

),

drug_totals as (

    -- fct_report_drug has one row per (report, drug), so count_if counts
    -- distinct reports.
    select
        f.drug_key,
        count(*) as report_count,
        count_if(r.is_expedited) as expedited_report_count,
        count_if(r.is_expedited and t.year = y.first_year) as first_year_expedited_reports,
        count_if(r.is_expedited and t.year = y.last_year) as last_year_expedited_reports
    from faers_db.mart.fct_report_drug as f
    inner join faers_db.mart.fct_report as r
        on f.report_id = r.report_id
    inner join faers_db.mart.dim_date as t
        on f.receipt_date_key = t.date_key
    cross join window_years as y
    where
        t.year_quarter between $window_start and $window_end
        and f.is_implicated
    group by f.drug_key

)

select
    d.medicinal_product,
    d.active_substance,
    k.expedited_report_count,
    round(k.expedited_report_count / k.report_count, 3) as expedited_share,
    k.first_year_expedited_reports,
    k.last_year_expedited_reports
from drug_totals as k
inner join faers_db.mart.dim_drug as d
    on k.drug_key = d.drug_key
order by k.expedited_report_count desc
limit 20;
