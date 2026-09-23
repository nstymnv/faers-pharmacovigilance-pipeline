-- Grain: one row per (drug_key, year_quarter).
--
-- Answers "which drugs show a sustained increase in reporting" and "which drugs
-- most frequently meet the expedited criteria". Quarter grain rather than year:
-- a report has one receipt date and so falls in exactly one quarter, which makes
-- every count here additive, and Power BI can roll quarters up to years itself.
--
-- Counts are distinct reports, never rows. A report listing a drug 100 times is
-- one report (models/README.md).
with drug_reports as (

    select
        d.drug_key,
        t.year_quarter,
        d.report_id,
        d.is_implicated,
        r.is_serious,
        r.is_expedited
    from {{ ref('fct_report_drug') }} as d
    inner join {{ ref('fct_report') }} as r
        on d.report_id = r.report_id
    inner join {{ ref('dim_date') }} as t
        on d.receipt_date_key = t.date_key

),

reaction_variety as (

    select
        d.drug_key,
        t.year_quarter,
        count(distinct x.reaction_key) as distinct_reaction_count
    from {{ ref('fct_report_drug') }} as d
    inner join {{ ref('fct_report_reaction') }} as x
        on d.report_id = x.report_id
    inner join {{ ref('dim_date') }} as t
        on d.receipt_date_key = t.date_key
    group by d.drug_key, t.year_quarter

),

aggregated as (

    select
        drug_key,
        year_quarter,
        count(distinct report_id) as report_count,
        -- Reports where the drug was named suspect or interacting, as opposed
        -- to merely also being taken. The denominator for attribution work.
        count(distinct iff(is_implicated, report_id, null)) as implicated_report_count,
        count(distinct iff(is_serious, report_id, null)) as serious_report_count,
        count(distinct iff(is_expedited, report_id, null)) as expedited_report_count
    from drug_reports
    group by drug_key, year_quarter

)

select
    a.drug_key,
    a.year_quarter,
    a.report_count,
    a.implicated_report_count,
    a.serious_report_count,
    a.expedited_report_count,
    coalesce(v.distinct_reaction_count, 0) as distinct_reaction_count
from aggregated as a
left join reaction_variety as v
    on a.drug_key = v.drug_key
    and a.year_quarter = v.year_quarter
