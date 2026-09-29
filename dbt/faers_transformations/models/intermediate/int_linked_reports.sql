-- Grain: one row per live report that belongs to a linked case — a group of
-- reports that name the same case identifier in their duplicate block and so
-- describe the same patient's event.
--
-- From 2021q4 the FAERS duplicate block (duplicate_numb) mostly carries another
-- system's identifier for the case: a regulator's number (AFSSAPS, MHRA,
-- NMPA), a literature service's article case id (Wipro, Adis), or another
-- company's case number. When two reports carry the same one, they are the same
-- case arriving through two channels. Before 2021q4 the block only restated the
-- sender's own case number and never links two reports. See models/README.md.
--
-- Only groups that look like one patient qualify:
-- - 2 to linked_case_max_reports reports. Larger groups are mostly literature
--   articles describing several patients under one article id (up to 463
--   reports), not one patient reported many times.
-- - no conflict between the known sexes, or between the known ages in whole
--   years. A missing value does not count as a conflict.
-- Measured on the full load, groups of 2-5 agree on sex 99% and on age 97% of
-- the time where both are recorded, so the conditions remove the rare
-- accidental collision rather than most of the groups.
--
-- The duplicates are removed in fct_report, not here: which copy survives
-- depends on the analysis window, which only the marts apply.
with reports as (

    select
        r.report_id,
        r.duplicate_numb as linked_case_id,
        d.sex,
        floor({{ faers_age_years('d.onset_age', 'd.age_unit') }}) as age_years
    from {{ ref('int_reports') }} as r
    left join {{ ref('int_demographics') }} as d
        on r.report_id = d.report_id
    where r.duplicate_numb is not null

),

linked_cases as (

    -- int_reports holds one row per report, so count(*) counts reports.
    select linked_case_id
    from reports
    group by linked_case_id
    having
        count(*) between 2 and {{ var('linked_case_max_reports') }}
        and count(distinct sex) <= 1
        and count(distinct age_years) <= 1

)

select
    r.report_id,
    r.linked_case_id
from reports as r
inner join linked_cases as c
    on r.linked_case_id = c.linked_case_id
