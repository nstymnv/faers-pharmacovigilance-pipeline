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
-- Linked duplicates (int_linked_reports) are collapsed here: of the reports
-- in the window that describe the same linked case, only the most recently
-- received survives, as it carries the latest information about the case, and
-- merged_duplicate_count records how many copies it stands for. Done after the
-- window filter so a case never drops out because its surviving copy was
-- published outside the window.
--
-- Denominator caveat: FDA-retracted reports are excluded upstream, and linked
-- duplicates here, so every "share of" metric built on this table is computed
-- over that filtered population. is_serious is null where a report gives no seriousness value
-- (from 2025q4), so a serious share is over reports whose seriousness is known.
--
-- This is the only mart that reads int_reports, and every other fact reaches
-- its reports through an inner join to it, so the analysis-window filter here
-- windows the whole star.
with windowed_reports as (

    select
        r.*,
        -- The prefixes keep a numeric case identifier from ever colliding with
        -- an unrelated report_id.
        iff(
            l.linked_case_id is null,
            'report:' || r.report_id,
            'case:' || l.linked_case_id
        ) as case_key
    from {{ ref('int_reports') }} as r
    left join {{ ref('int_linked_reports') }} as l
        on r.report_id = l.report_id
    where {{ faers_mart_window_filter('r.first_seen_quarter', 'r.last_seen_quarter') }}

),

reports as (

    select
        *,
        count(*) over (partition by case_key) - 1 as merged_duplicate_count
    from windowed_reports
    qualify row_number() over (
        partition by case_key
        order by receipt_date desc nulls last, report_id desc
    ) = 1

),

demographics as (

    select * from {{ ref('int_demographics') }}

),

joined as (

    select
        r.report_id,

        coalesce(to_char(r.receipt_date, 'YYYYMMDD')::int, -1) as receipt_date_key,
        coalesce(to_char(r.transmission_date, 'YYYYMMDD')::int, -1) as transmission_date_key,

        -- The hash has to be built from the same two columns in the same order
        -- as dim_country, and a null country needs the unknown member rather
        -- than a hash of nulls, which would be a key pointing at nothing.
        case
            when r.source_country is null then '-1'
            else {{ dbt_utils.generate_surrogate_key(
                ['r.source_country', 'r.source_country_name']
            ) }}
        end as source_country_key,
        case
            when r.occurrence_country is null then '-1'
            else {{ dbt_utils.generate_surrogate_key(
                ['r.occurrence_country', 'r.occurrence_country_name']
            ) }}
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
        d.weight_kg,

        r.merged_duplicate_count

    from reports as r
    left join demographics as d
        on r.report_id = d.report_id

)

select * from joined
