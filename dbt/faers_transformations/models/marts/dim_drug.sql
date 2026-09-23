-- Grain: one row per (active_substance, medicinal_product) pair, plus one
-- unknown member.
--
-- Both columns are in the key because the relationship runs both ways: one
-- substance is sold as many branded products, and one product name is reported
-- against slightly different substance strings. Keying on the pair means the
-- dimension never merges two things that FAERS kept apart.
--
-- No name normalization is attempted: "NEXIUM", "NEXIUM 40MG" and
-- "NEXIUM (ESOMEPRAZOLE)" are three rows here. That is the largest single limit
-- on any "which drugs" answer this warehouse gives, and resolving it needs an
-- external vocabulary (RxNorm, openFDA) rather than string cleaning.
with drugs as (

    select distinct
        medicinal_product,
        active_substance
    from {{ ref('int_drugs') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['medicinal_product', 'active_substance']) }} as drug_key,
        medicinal_product,
        active_substance
    from drugs

),

unknown_member as (

    -- stg_drugs already drops entries missing a product or a substance, so no
    -- fact points here today. Kept for convention; '-1' cannot collide with the
    -- md5 hashes generate_surrogate_key produces.
    select
        '-1' as drug_key,
        'Not reported' as medicinal_product,
        'Not reported' as active_substance

)

select * from final
union all
select * from unknown_member
