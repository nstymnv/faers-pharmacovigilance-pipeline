-- Grain: one row per (report_id, reaction) — one row per distinct reaction on a
-- report.
--
-- meddra_version used to sit in the key, so the same reaction re-coded under a
-- newer MedDRA version stayed as two rows on one report and every distinct-
-- reaction count over-reported. It is now resolved to the latest version seen
-- for that reaction instead of keying on it.
--
-- Where a report lists the same reaction twice with conflicting outcomes (5,612
-- such pairs in 2020q1), the most severe outcome wins — see the
-- faers_outcome_severity macro for the ranking and the reasoning.
with latest_version as (

    -- FDA occasionally republishes the same version in a later quarter. The
    -- most recent publication wins, as in int_drugs: keeping both would union
    -- the two reaction lists and revive a reaction the republication dropped.
    select *
    from {{ ref('stg_reaction') }}

    qualify dense_rank() over (
        partition by report_id
        order by version desc, source_quarter desc
    ) = 1

),

live_reactions as (

    select r.*
    from latest_version as r
    left join {{ ref('stg_deleted_cases') }} as x
        on r.report_id = x.report_id
    where x.report_id is null

),

deduplicated as (

    select
        report_id,
        reaction,
        max(version) as version,
        max(meddra_version) as meddra_version,

        max_by(
            outcome,
            {{ faers_outcome_severity('outcome') }}
        ) as outcome,

        max_by(
            custom_reaction_group,
            iff(custom_reaction_group is not null, version, null)
        ) as custom_reaction_group,

        max_by(
            custom_reaction_group_label,
            iff(custom_reaction_group_label is not null, version, null)
        ) as custom_reaction_group_label,

        max_by(
            custom_reaction_group_basis,
            iff(custom_reaction_group_basis is not null, version, null)
        ) as custom_reaction_group_basis

    from live_reactions

    group by report_id, reaction

)

select *
from deduplicated
