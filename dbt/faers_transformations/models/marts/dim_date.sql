-- Grain: one row per calendar day, plus one unknown member.
--
-- Spans whole calendar years around the receipt and transmission dates in
-- fct_report, so the spine follows the analysis window as it moves and a year is
-- never half-covered. Both are keyed against it, and transmission runs later than
-- receipt: reports in the 2022q4 file were transmitted in January 2023. It is bounded by fct_report rather than int_reports because
-- quarters outside the window stay loaded upstream, and a calendar reaching
-- into them would show those years as empty trend points. Report dates are the only dates keyed against this dimension:
-- they are clean 8-digit values, whereas drug dates are padded from partial
-- precision (see the faers_to_date macro) and are published as plain dates
-- rather than as keys.
--
-- Its main job is that a quarter with no reports still produces a row in a
-- trend, instead of the gap silently closing up and turning a reporting outage
-- into a flat line.
--
-- The bounds are inlined as subqueries rather than lifted into a CTE because
-- date_spine emits its own WITH block, which cannot see one, and because
-- dbt_utils runs the interval count as a query at compile time.
{%- set report_date_keys -%}
    (
        select receipt_date_key as date_key from {{ ref('fct_report') }}
        union all
        select transmission_date_key from {{ ref('fct_report') }}
    ) where date_key <> -1
{%- endset %}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select date_trunc('year', min(to_date(to_char(date_key), 'YYYYMMDD'))) from " ~ report_date_keys ~ ")",
        end_date="(select dateadd(year, 1, date_trunc('year', max(to_date(to_char(date_key), 'YYYYMMDD')))) from " ~ report_date_keys ~ ")"
    ) }}

),

calendar as (

    select
        cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
        cast(date_day as date) as date_day,
        year(date_day) as year,
        quarter(date_day) as quarter,
        to_char(date_day, 'YYYY') || 'Q' || quarter(date_day) as year_quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,
        to_char(date_day, 'YYYY-MM') as year_month
    from spine

),

unknown_member as (

    -- Report dates are never null today, so this row is insurance rather than a
    -- load-bearing member: it keeps the convention uniform across the four
    -- dimensions and lets the relationships tests on the facts run at error
    -- severity even if a future quarter arrives with a missing date.
    select
        -1 as date_key,
        cast(null as date) as date_day,
        cast(null as int) as year,
        cast(null as int) as quarter,
        'Not reported' as year_quarter,
        cast(null as int) as month,
        'Not reported' as month_name,
        'Not reported' as year_month

)

select * from calendar
union all
select * from unknown_member
