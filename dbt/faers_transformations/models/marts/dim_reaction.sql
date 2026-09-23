-- Grain: one row per MedDRA preferred term, plus one unknown member.
--
-- The custom grouping columns come from the custom_reaction_groups_mapping seed
-- and are a function of the reaction alone, but they are aggregated rather than
-- selected distinct so that a future duplicate row in the seed degrades into a
-- deterministic pick instead of fanning the dimension out and breaking its
-- uniqueness test.
with reactions as (

    select
        reaction,
        max(custom_reaction_group) as custom_reaction_group,
        max(custom_reaction_group_label) as custom_reaction_group_label,
        max(custom_reaction_group_basis) as custom_reaction_group_basis
    from {{ ref('int_reactions') }}
    group by reaction

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['reaction']) }} as reaction_key,
        reaction,
        custom_reaction_group,
        custom_reaction_group_label,
        -- 'rule' means an explicit mapping rule put this reaction in its group;
        -- 'fallback' means it landed there by catch-all. A dashboard that groups
        -- reactions without surfacing this presents a guess as a finding.
        custom_reaction_group_basis
    from reactions

),

unknown_member as (

    select
        '-1' as reaction_key,
        'Not reported' as reaction,
        cast(null as string) as custom_reaction_group,
        'Not reported' as custom_reaction_group_label,
        cast(null as string) as custom_reaction_group_basis

)

select * from final
union all
select * from unknown_member
