-- Business question: which drugs have the highest share of reports meeting the
-- expedited criteria?
--
-- A manufacturer must send FDA an expedited (15-day) report when an adverse
-- event is both serious and unexpected, i.e. not described in the drug's label.
-- is_expedited is therefore this project's proxy for "unexpected reactions".
--
-- Only reports where the drug was implicated (suspect or interacting) count; a
-- drug the patient merely also took says nothing about that drug. This is why
-- the query reads the facts rather than agg_expedited_drug_profile, which counts
-- every role.
--
-- The share is expedited / all over the whole window, never an average of
-- quarterly shares. Drugs below min_reports are left out: with a handful of
-- reports the share is 0% or 100% by chance and the top of the list fills with
-- noise. Ties are broken by volume.
set window_start = '2021Q1';
set window_end = '2025Q4';
set min_reports = 1000;

with drug_totals as (

    -- fct_report_drug has one row per (report, drug), so counting rows counts
    -- distinct reports.
    select
        f.drug_key,
        count(*) as report_count,
        count_if(r.is_expedited) as expedited_report_count
    from faers_db.mart.fct_report_drug as f
    inner join faers_db.mart.fct_report as r
        on f.report_id = r.report_id
    inner join faers_db.mart.dim_date as t
        on f.receipt_date_key = t.date_key
    where
        t.year_quarter between $window_start and $window_end
        and f.is_implicated
    group by f.drug_key
    having count(*) >= $min_reports

)

select
    d.medicinal_product,
    d.active_substance,
    k.report_count,
    k.expedited_report_count,
    round(k.expedited_report_count / k.report_count, 3) as expedited_share
from drug_totals as k
inner join faers_db.mart.dim_drug as d
    on k.drug_key = d.drug_key
order by expedited_share desc, k.expedited_report_count desc
limit 20;
