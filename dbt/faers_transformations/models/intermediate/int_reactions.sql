-- Grain: one row per (report_id, reaction, meddra_version) — i.e. one row
-- per distinct reaction on a report, deduplicated across versions.
--
-- Unlike reports/demographics, a reaction record isn't expected to gain new
-- non-null fields across versions independently, so the latest version's row
-- is taken as-is instead of merging fields with max_by.
with ranked_versions as (

    select *
    from {{ ref('stg_reaction') }}

    qualify row_number() over (
        partition by
            report_id,
            reaction,
            meddra_version
        order by version desc
    ) = 1

)

select *
from ranked_versions
