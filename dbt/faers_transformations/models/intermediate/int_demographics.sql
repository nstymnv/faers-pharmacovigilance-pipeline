-- Grain: one row per report_id — versions are consolidated by merging the
-- latest non-null value per field (see the max_by pattern below).
with ranked_versions as (

    select *
    from {{ ref('stg_demographics') }}

    qualify row_number() over (
        partition by report_id, version
        order by version desc
    ) = 1

),

deduplicated as (

    select
        report_id,
        max(version) as version,

        max_by(
            age_group,
            iff(age_group is not null, version, null)
        ) as age_group,

        max_by(
            onset_age,
            iff(onset_age is not null, version, null)
        ) as onset_age,

        max_by(
            age_unit,
            iff(age_unit is not null, version, null)
        ) as age_unit,

        max_by(
            sex,
            iff(sex is not null, version, null)
        ) as sex,

        max_by(
            weight_kg,
            iff(weight_kg is not null, version, null)
        ) as weight_kg

    from ranked_versions

    group by report_id

)

select *
from deduplicated
