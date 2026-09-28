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
        try_cast(reporttype as int) as report_type,
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
        companynumb as company_number,
        source_quarter
    from source
    where
        {{ faers_quarter_filter() }}
        and safetyreportid is not null
        -- Reports with no serious value are kept, as unknown seriousness. None
        -- existed before 2025q4, which has 10,603 (2.8%); dropping them would cut
        -- valid reports from every count and signal and fake a dip in the trend.
),

standardized as (
    select
        report_id,
        version,
        receipt_date,
        transmission_date,
        source_country,
        occurrence_country,
        case
            when report_type = 1 then 'spontaneous'
            when report_type = 2 then 'report from study'
            when report_type = 3 then 'other'
            -- 4 is "not available to sender", i.e. the sender knows the report
            -- type is unknown. Distinct from null, which is the element being
            -- absent altogether, so it keeps its own decoded value.
            when report_type = 4 then 'not available to sender'
        end as report_type,
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
        company_number,
        source_quarter
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
