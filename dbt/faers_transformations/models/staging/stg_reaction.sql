-- Grain: one row per reaction per (report_id, version) — a report version
-- can list multiple reactions, prior to deduplication in int_reactions.
with source as (
    select * from {{ source('faers_db', 'reaction') }}
),

normalized as (
    select
        safetyreportid as report_id,
        safetyreportversion as version,
        initcap(trim(cast(reactionmeddrapt as string))) as reaction,
        try_cast(reactionmeddraversionpt as float) as meddra_version,
        try_cast(reactionoutcome as int) as outcome
    from source
    where
        {{ faers_quarter_filter() }}
        and safetyreportid is not null
        and reactionmeddrapt is not null
),

standardized as (
    select
        report_id,
        version,
        reaction,
        meddra_version,
        case
            when outcome = 1 then 'recovered/resolved'
            when outcome = 2 then 'recovering/resolving'
            when outcome = 3 then 'not recovered/not resolved'
            when outcome = 4 then 'recovered/resolved with sequelae'
            when outcome = 5 then 'fatal'
            when outcome = 6 then 'unknown'
        end as outcome
    from normalized
),

final as (
    select
        s.*,
        c_reac.custom_group as custom_reaction_group,
        c_reac.custom_group_label as custom_reaction_group_label,
        -- Distinguishes a reaction grouped by an explicit rule from one that
        -- landed in its group by fallback, so consumers can tell how much
        -- confidence a grouping carries instead of treating all alike.
        c_reac.mapping_basis as custom_reaction_group_basis
    from standardized as s
    left join {{ ref('custom_reaction_groups_mapping') }} as c_reac
        on s.reaction = c_reac.reaction_normalized
)

select * from final
