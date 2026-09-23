-- Grain: one row per report_id — versions are consolidated by merging the
-- latest non-null value per field (see the max_by pattern below).
with ranked_versions as (

    select *
    from {{ ref('stg_demographics') }}

    -- FAERS carries one demographics block per report version, so this is a
    -- safety net rather than a routine collapse (2020q1 has no (report_id,
    -- version) collisions at all). Ordering by version here would order by the
    -- partition key itself and leave the survivor arbitrary, so where a
    -- collision does appear the most complete record wins instead.
    qualify row_number() over (
        partition by report_id, version
        order by
            (
                iff(age_group is not null, 1, 0)
                + iff(onset_age is not null, 1, 0)
                + iff(age_unit is not null, 1, 0)
                + iff(sex is not null, 1, 0)
                + iff(weight_kg is not null, 1, 0)
            ) desc,
            onset_age desc nulls last,
            weight_kg desc nulls last
    ) = 1

),

live_demographics as (

    select d.*
    from ranked_versions as d
    left join {{ ref('stg_deleted_cases') }} as x
        on d.report_id = x.report_id
    where x.report_id is null

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

    from live_demographics

    group by report_id

)

select *
from deduplicated
