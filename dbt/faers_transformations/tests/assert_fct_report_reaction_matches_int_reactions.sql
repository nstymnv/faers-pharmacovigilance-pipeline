-- fct_report_reaction must equal int_reactions row for row for every report in
-- fct_report. The window means the two tables no longer match in total, so the
-- comparison is restricted to windowed reports rather than dropped.
with expected as (

    select count(*) as row_count
    from {{ ref('int_reactions') }} as r
    where r.report_id in (select report_id from {{ ref('fct_report') }})

),

actual as (

    select count(*) as row_count
    from {{ ref('fct_report_reaction') }}

)

select
    expected.row_count as expected_rows,
    actual.row_count as actual_rows
from expected
cross join actual
where expected.row_count <> actual.row_count
