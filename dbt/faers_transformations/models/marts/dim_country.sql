-- Grain: one row per ISO country code, plus one unknown member.
--
-- Built from the country_mapping seed rather than from the countries observed in
-- reports, so the dimension is the same shape on every build and does not grow a
-- row the first time a quarter happens to mention Andorra. The seed maps many
-- raw FAERS strings onto one code, hence the distinct.
--
-- The unknown member is load-bearing here, unlike in the other dimensions:
-- 16,732 reports in 2020q1 have no source country and 3,776 no occurrence
-- country. Without a row to point at, a Power BI relationship drops those
-- reports out of every country-sliced visual with no indication that it did.
with mapped as (

    select distinct
        country_code,
        country_name
    from {{ ref('country_mapping') }}
    where country_code is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['country_code', 'country_name']) }} as country_key,
        country_code,
        country_name
    from mapped

),

unknown_member as (

    select
        '-1' as country_key,
        'Not reported' as country_code,
        'Not reported' as country_name

)

select * from final
union all
select * from unknown_member
