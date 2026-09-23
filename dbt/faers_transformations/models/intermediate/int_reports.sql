-- Grain: one row per report_id — versions are consolidated by merging the
-- latest non-null value per field (see the max_by pattern below).
--
-- Reports retracted by FDA are removed here rather than flagged, so every
-- downstream denominator excludes them by default and no mart can forget to
-- filter. See models/README.md for the duplicate/retraction policy.
with ranked_versions as (

    select *
    from {{ ref('stg_reports') }}

    qualify row_number() over (
        partition by report_id, version
        order by transmission_date desc
    ) = 1

),

live_reports as (

    select r.*
    from ranked_versions as r
    left join {{ ref('stg_deleted_cases') }} as d
        on r.report_id = d.report_id
    where d.report_id is null

),

deduplicated as (

    select

        report_id,
        max(version) as version,

        -- Every field below takes the latest version that actually supplied a
        -- value. A bare max_by(field, version) would return null whenever the
        -- newest version happens to omit a field an earlier one carried, which
        -- is why the dates use the same null-guarded form as the rest.
        max_by(
            receipt_date,
            iff(receipt_date is not null, version, null)
        ) as receipt_date,

        max_by(
            transmission_date,
            iff(transmission_date is not null, version, null)
        ) as transmission_date,

        max_by(
            source_country,
            iff(source_country is not null, version, null)
        ) as source_country,

        max_by(
            source_country_name,
            iff(source_country_name is not null, version, null)
        ) as source_country_name,

        max_by(
            occurrence_country,
            iff(occurrence_country is not null, version, null)
        ) as occurrence_country,

        max_by(
            occurrence_country_name,
            iff(occurrence_country_name is not null, version, null)
        ) as occurrence_country_name,

        max_by(
            report_type,
            iff(report_type is not null, version, null)
        ) as report_type,

        max_by(
            serious,
            iff(serious is not null, version, null)
        ) as serious,

        max_by(
            congenital_anomaly,
            iff(congenital_anomaly is not null, version, null)
        ) as congenital_anomaly,

        max_by(
            death,
            iff(death is not null, version, null)
        ) as death,

        max_by(
            disabling,
            iff(disabling is not null, version, null)
        ) as disabling,

        max_by(
            hospitalization,
            iff(hospitalization is not null, version, null)
        ) as hospitalization,

        max_by(
            lifethreatening,
            iff(lifethreatening is not null, version, null)
        ) as lifethreatening,

        max_by(
            other_serious,
            iff(other_serious is not null, version, null)
        ) as other_serious,

        max_by(
            fulfill_expedite_criteria,
            iff(fulfill_expedite_criteria is not null, version, null)
        ) as fulfill_expedite_criteria,

        max_by(
            duplicate_flag,
            iff(duplicate_flag is not null, version, null)
        ) as duplicate_flag,

        max_by(
            duplicate_numb,
            iff(duplicate_numb is not null, version, null)
        ) as duplicate_numb,

        max_by(
            duplicate_source,
            iff(duplicate_source is not null, version, null)
        ) as duplicate_source,

        max_by(
            authority_number,
            iff(authority_number is not null, version, null)
        ) as authority_number,

        max_by(
            company_number,
            iff(company_number is not null, version, null)
        ) as company_number

    from live_reports

    group by report_id

)

select *
from deduplicated
