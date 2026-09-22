-- Grain: one row per retracted FAERS case id.
--
-- FDA publishes a retracted-case list with every quarter, and a retraction can
-- name a report published in that same quarter (152 of 2020q1's 4,489 ids refer
-- to 2020q1 reports). The union across quarters is therefore applied to the whole
-- warehouse, which is why source_quarter is dropped here rather than carried.
--
-- Deliberately not filtered by the faers_quarters dev-slice var: a retraction
-- published in any quarter applies to every report it names, so narrowing this
-- model would let a dev build keep reports that FDA has withdrawn.
with source as (
    select * from {{ source('faers_db', 'deleted_cases') }}
),

normalized as (
    -- Cast to number to match reports.safetyreportid; every published id is
    -- numeric, so a try_cast that returns null means a malformed line rather
    -- than a differently-formatted case id, and dropping it is correct.
    select distinct try_cast(trim(cast(case_id as string)) as number) as report_id
    from source
    where try_cast(trim(cast(case_id as string)) as number) is not null
)

select * from normalized
