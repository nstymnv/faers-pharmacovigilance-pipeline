-- Grain: one row per report_id. The spine of the star.
--
-- int_reports is left joined to int_demographics, not inner joined: the
-- relationships test proves demographics is a subset of reports, not the
-- reverse, so an inner join would silently drop every report that arrived
-- without a demographics block.
--
-- Identifiers nobody groups by — authority_number, company_number, version, and
-- the duplicate_* columns — are deliberately not carried. They stay one join
-- from report_id away in int_reports, which is where a case drill-through
-- belongs. The duplicate_* columns are also withheld because their name invites
-- exactly the filter that would delete 94% of the data (models/README.md).
--
-- Denominator caveat: FDA-retracted reports are excluded upstream, so every
-- "share of" metric built on this table is computed over that filtered
-- population. is_serious is null where a report gives no seriousness value
-- (from 2025q4), so a serious share is over reports whose seriousness is known.
--
-- This is the only mart that reads int_reports, and every other fact reaches
-- its reports through an inner join to it, so the analysis-window filter here
-- windows the whole star.
with reports as (

    select * from {{ ref('int_reports') }}
    where {{ faers_mart_window_filter() }}

),

demographics as (

    select * from {{ ref('int_demographics') }}

),

joined as (

    select
        r.report_id,

        coalesce(cast(to_char(r.receipt_date, 'YYYYMMDD') as int), -1) as receipt_date_key,
        coalesce(cast(to_char(r.transmission_date, 'YYYYMMDD') as int), -1) as transmission_date_key,

        -- The hash has to be built from the same two columns in the same order
        -- as dim_country, and a null country needs the unknown member rather
        -- than a hash of nulls, which would be a key pointing at nothing.
        case
            when r.source_country is null then '-1'
            else {{ dbt_utils.generate_surrogate_key(['r.source_country', 'r.source_country_name']) }}
        end as source_country_key,
        case
            when r.occurrence_country is null then '-1'
            else {{ dbt_utils.generate_surrogate_key(['r.occurrence_country', 'r.occurrence_country_name']) }}
        end as occurrence_country_key,

        r.serious = 'yes' as is_serious,

        r.congenital_anomaly,
        r.death,
        r.disabling,
        r.hospitalization,
        r.lifethreatening,
        r.other_serious,

        -- One report can meet several seriousness criteria at once. Summing the
        -- six presence flags gives a single sortable measure of how severe the
        -- case was, without having to decide which criterion outranks which.
        (
            r.congenital_anomaly::int
            + r.death::int
            + r.disabling::int
            + r.hospitalization::int
            + r.lifethreatening::int
            + r.other_serious::int
        ) as seriousness_criteria_count,

        r.fulfill_expedite_criteria = 'identified' as is_expedited,
        r.report_type,

        d.age_group,
        d.sex,
        {{ faers_age_years('d.onset_age', 'd.age_unit') }} as onset_age_years,
        d.weight_kg

    from reports as r
    left join demographics as d
        on r.report_id = d.report_id

)

select * from joined
