-- fct_report must hold exactly the int_reports rows inside the analysis window:
-- the left join to demographics may not fan out, and the window filter may not
-- drop or keep anything else. Replaces a plain equal_rowcount against
-- int_reports, which stops holding as soon as the window excludes a quarter.
with expected as (

    select count(*) as row_count
    from {{ ref('int_reports') }}
    where {{ faers_mart_window_filter() }}

),

actual as (

    select count(*) as row_count
    from {{ ref('fct_report') }}

)

select
    expected.row_count as expected_rows,
    actual.row_count as actual_rows
from expected
cross join actual
where expected.row_count <> actual.row_count
