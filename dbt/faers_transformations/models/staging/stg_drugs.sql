-- Grain: one row per drug entry per (report_id, version), keyed by drug_index.
--
-- A report lists a drug once per administered dose, not once per product, so the
-- same product legitimately recurs dozens of times on one report with its own
-- dose and dates. drug_index keeps those entries distinct; collapsing them to one
-- row per product happens in the marts, not here.
with source as (
    select * from {{ source('faers_db', 'drug') }}
),

normalized as (
    select
        safetyreportid as report_id,
        safetyreportversion as version,
        drug_index,
        upper(trim(medicinalproduct)) as medicinal_product,
        upper(trim(activesubstancename)) as active_substance,
        cast(drugcharacterization as int) as drug_characterization,
        drugindication as drug_indication,
        drugbatchnumb as drug_batch_number,
        drugauthorizationnumb as drug_authorization_number,
        cast(drugadministrationroute as string) as administration_route,
        drugstructuredosagenumb as dosage_amount,
        try_cast(drugstructuredosageunit as int) as dosage_unit_code,
        {{ faers_to_date('drugstartdate') }} as start_date,
        {{ faers_to_date('drugenddate') }} as end_date,
        try_cast(drugtreatmentduration as float) as duration,
        cast(drugtreatmentdurationunit as int) as duration_unit,
        -- whether the event recurred on re-administration (yes/no/unknown) —
        -- distinct from recurrence_action below, which is the action code
        -- from that recurrence event, not whether recurrence happened at all
        cast(drugrecurreadministration as int) as drug_recurrence,
        -- last valid (non-null) recurrence-action code for this drug; parser.py
        -- collapses the drugrecurrence explosion back to one row per drug
        drugrecuraction as recurrence_action,
        cast(actiondrug as int) as taken_action,
        -- try_cast: 2023q3 carries one free-text value in this coded field.
        try_cast(drugadditional as int) as use_stopped_reduced,
        source_quarter

    from source
    where
        {{ faers_quarter_filter() }}
        and safetyreportid is not null
        and medicinalproduct is not null
        and activesubstancename is not null
        -- Only the three drug roles. 2023q3 has one entry coded 5 (product
        -- "DEVICE"), a code FAERS does not define for drugs.
        and drugcharacterization in (1, 2, 3)

),

standardized as (
    select
        report_id,
        version,
        drug_index,
        medicinal_product,
        active_substance,
        case
            when drug_characterization = 1 then 'suspect'
            when drug_characterization = 2 then 'concomitant'
            when drug_characterization = 3 then 'interacting'
        end as drug_characterization,
        drug_indication,
        drug_batch_number,
        drug_authorization_number,
        administration_route,
        dosage_amount,
        dosage_unit_code,
        start_date,
        end_date,
        duration,
        duration_unit,
        case
            when drug_recurrence = 1 then 'yes'
            when drug_recurrence = 2 then 'no'
            when drug_recurrence = 3 then 'unknown'
        end as drug_recurrence,
        recurrence_action,
        case
            when taken_action = 1 then 'drug withdrawn'
            when taken_action = 2 then 'dose reduced'
            when taken_action = 3 then 'dose increased'
            when taken_action = 4 then 'dose not changed'
            when taken_action = 5 then 'unknown'
            when taken_action = 6 then 'not applicable'
        end as taken_action,
        case
            when use_stopped_reduced = 1 then 'yes'
            when use_stopped_reduced = 2 then 'no'
            when use_stopped_reduced = 3 then 'doesn''t Apply'
        end as use_stopped_reduced,
        source_quarter
    from normalized
),

final as (
    -- dosage_unit_code is kept alongside the decoded name on purpose. FDA
    -- documents only codes 1-4 and refers the rest to the ICH E2B(R2) spec,
    -- which is not bundled with the FAERS download, so ~26% of dosed rows
    -- decode to null. Surfacing the raw code keeps a dose of "500 <unknown
    -- unit>" visibly unresolved rather than silently unitless.
    select
        s.* exclude (administration_route, duration_unit),
        a.administration_route,
        t.unit_name as duration_unit,
        du.dosage_unit
    from standardized as s
    left join {{ ref('administration_route_mapping') }} as a
        on s.administration_route = a.administration_route_code
    left join {{ ref('time_unit_code_mapping') }} as t
        on s.duration_unit = t.unit_code
    left join {{ ref('dosage_unit_mapping') }} as du
        on s.dosage_unit_code = du.dosage_unit_code
)

select * from final
