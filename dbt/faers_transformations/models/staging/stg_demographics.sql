-- Grain: one row per (report_id, version) — one row per FAERS report
-- version, prior to consolidating versions in int_demographics.
with source as (
    select *
    from {{ source('faers_db', 'demographics') }}
),

normalized as (
    select
        safetyreportid as report_id,
        safetyreportversion as version,
        try_cast(patientagegroup as int) as age_group,
        try_cast(patientonsetage as int) as onset_age,
        try_cast(patientonsetageunit as int) as age_unit_code,
        try_cast(patientsex as int) as sex,
        try_cast(patientweight as float) as weight_kg
    from source
    where
        {{ faers_quarter_filter() }}
        and safetyreportid is not null
),

standardized as (
    select
        report_id,
        version,
        case
            when age_group = 1 then 'neonate'
            when age_group = 2 then 'infant'
            when age_group = 3 then 'child'
            when age_group = 4 then 'adolescent'
            when age_group = 5 then 'adult'
            when age_group = 6 then 'elderly'
        end as age_group,
        onset_age,
        age_unit_code,
        -- 0 is a reported value ("unknown", per the FAERS spec) and null is an
        -- absent one. The previous else-branch merged 47,681 unreported rows
        -- into the 92 genuinely reported as unknown, which would overstate a
        -- demographic category ~500x in any "which patient groups" breakdown.
        case
            when sex = 0 then 'unknown'
            when sex = 1 then 'male'
            when sex = 2 then 'female'
        end as sex,
        weight_kg

    from normalized
),

final as (
    select
        s.* exclude (age_unit_code),
        t.unit_name as age_unit
    from standardized as s
    left join {{ ref('time_unit_code_mapping') }} as t
        on t.unit_code = s.age_unit_code
)

select * from final
