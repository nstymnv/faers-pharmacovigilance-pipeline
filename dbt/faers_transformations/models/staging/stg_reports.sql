-- Grain: one row per (report_id, version) — one row per FAERS report
-- version, prior to consolidating versions in int_reports.
with source as (
    select * from {{ source('faers_db', 'reports') }}
),

normalized as (
    select
        safetyreportid as report_id,
        safetyreportversion as version,
        {{ faers_to_date('receiptdate') }} as receipt_date,
        {{ faers_to_date('transmissiondate') }} as transmission_date,
        primarysourcecountry as source_country,
        occurcountry as occurrence_country,
        reporttype as report_type,
        try_cast(serious as int) as serious,
        try_cast(seriousnesscongenitalanomali as int) as congenital_anomaly,
        try_cast(seriousnessdeath as int) as death,
        try_cast(seriousnessdisabling as int) as disabling,
        try_cast(seriousnesshospitalization as int) as hospitalization,
        try_cast(seriousnesslifethreatening as int) as lifethreatening,
        try_cast(seriousnessother as int) as other_serious,
        try_cast(fulfillexpeditecriteria as int) as fulfill_expedite_criteria,
        try_cast(duplicate as int) as duplicate_flag,
        duplicatenumb as duplicate_numb,
        duplicatesource as duplicate_source,
        authoritynumb as authority_number,
        companynumb as company_number
    from source
    where
        {{ faers_quarter_filter() }}
        and safetyreportid is not null
        and serious is not null
),

standardized as (
    select
        report_id,
        version,
        receipt_date,
        transmission_date,
        source_country,
        occurrence_country,
        report_type,
        case
            when serious = 1 then 'yes'
            when serious = 2 then 'no'
        end as serious,
        -- The seriousness criteria are presence flags: FAERS sets them to 1
        -- when the criterion applies and omits the element otherwise. Passed
        -- through raw they are unsummable, so they are decoded to total
        -- booleans here — see the macro for why absence means false.
        {{ faers_seriousness_flag('congenital_anomaly') }} as congenital_anomaly,
        {{ faers_seriousness_flag('death') }} as death,
        {{ faers_seriousness_flag('disabling') }} as disabling,
        {{ faers_seriousness_flag('hospitalization') }} as hospitalization,
        {{ faers_seriousness_flag('lifethreatening') }} as lifethreatening,
        {{ faers_seriousness_flag('other_serious') }} as other_serious,
        case
            when fulfill_expedite_criteria = 1 then 'identified'
            when fulfill_expedite_criteria = 2 then 'other'
        end as fulfill_expedite_criteria,
        duplicate_flag,
        duplicate_numb,
        duplicate_source,
        authority_number,
        company_number
    from normalized
),

final as (
    select
        s.* exclude (source_country, occurrence_country),
        c_source.country_code as source_country,
        c_source.country_name as source_country_name,
        c_occurrence.country_code as occurrence_country,
        c_occurrence.country_name as occurrence_country_name
    from standardized as s
    left join {{ ref('country_mapping') }} as c_source
        on c_source.raw_country = s.source_country
    left join {{ ref('country_mapping') }} as c_occurrence
        on c_occurrence.raw_country = s.occurrence_country
)

select * from final
