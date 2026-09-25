-- Grain: one row per (drug_key, year_quarter).
--
-- Answers "which drugs have the highest share of reports meeting the expedited
-- criteria" — a share, which is why this table exists alongside
-- agg_drug_quarterly_trend rather than being read off it: the ratio is computed
-- here once, next to its own denominator, instead of being reassembled in DAX.
--
-- The counts are the same measures as in agg_drug_quarterly_trend and are
-- sourced the same way, so the two tables must agree. A dbt test asserts that.
--
-- expedited_share is NOT additive. Rolling quarters up to a year means summing
-- expedited_report_count and report_count and dividing again, never averaging
-- the share. Power BI measures should be written that way.
--
-- Denominator caveat: report_count is over reports that survived the staging
-- filters (stg_drugs drops entries missing product, substance or a known
-- characterization) and excludes FDA-retracted reports.
with drug_reports as (

    select
        d.drug_key,
        t.year_quarter,
        d.report_id,
        r.is_expedited
    from {{ ref('fct_report_drug') }} as d
    inner join {{ ref('fct_report') }} as r
        on d.report_id = r.report_id
    inner join {{ ref('dim_date') }} as t
        on d.receipt_date_key = t.date_key

),

aggregated as (

    select
        drug_key,
        year_quarter,
        count(distinct report_id) as report_count,
        count(distinct iff(is_expedited, report_id, null)) as expedited_report_count
    from drug_reports
    group by drug_key, year_quarter

)

select
    drug_key,
    year_quarter,
    report_count,
    expedited_report_count,
    round(expedited_report_count / nullif(report_count, 0)::float, 4) as expedited_share
from aggregated
