-- Grain: one row per (report_id, drug_index) — one row per drug *entry*, not
-- per distinct product.
--
-- A FAERS report lists a drug once per administered dose: report 12610564 carries
-- 100 AFSTYLA entries, each with its own dose and start date. Those entries are
-- kept here so the dosing detail survives into the warehouse. Collapsing them to
-- one row per product happens in the marts, where counting distinct reports per
-- drug is what stops a single patient's treatment diary from outweighing a drug
-- reported by a hundred separate patients.
--
-- The previous key was six drug attributes, which silently destroyed 155,043 of
-- 1,899,889 rows (8.2%) in 2020q1 — 55,032 of the collapsed groups differed in
-- dose, start date or route, so they were distinct exposures, not repeats.
with latest_version as (

    -- drug_index is assigned per report version, so index 3 in version 1 need
    -- not be the same product as index 3 in version 2. Taking the newest
    -- version's drug list whole, rather than resolving each index across
    -- versions, keeps the list internally consistent — a later version
    -- supersedes the entire drug list of the one before it.
    --
    -- FDA occasionally republishes the same version in a later quarter (25
    -- report versions across 2020q1-2022q4), which would bring the list in
    -- twice. The most recent publication wins, as it does in int_reports.
    select *
    from {{ ref('stg_drugs') }}

    qualify dense_rank() over (
        partition by report_id
        order by version desc, source_quarter desc
    ) = 1

)

select d.*
from latest_version as d
left join {{ ref('stg_deleted_cases') }} as x
    on d.report_id = x.report_id
where x.report_id is null
