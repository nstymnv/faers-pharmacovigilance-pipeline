-- Grain: one row per (report_id, medicinal_product, active_substance,
-- drug_characterization, drug_indication, drug_authorization_number) — i.e.
-- one row per distinct drug on a report, deduplicated across versions.
--
-- Unlike reports/demographics, a drug record isn't expected to gain new
-- non-null fields across versions independently, so the latest version's row
-- is taken as-is instead of merging fields with max_by.
with ranked_versions as (

    select *
    from {{ ref('stg_drugs') }}

    qualify row_number() over (
        partition by
            report_id,
            medicinal_product,
            active_substance,
            drug_characterization,
            drug_indication,
            drug_authorization_number
        order by version desc
    ) = 1

)

select *
from ranked_versions
