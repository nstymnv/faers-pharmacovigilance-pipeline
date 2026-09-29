-- fct_report must hold exactly one row per case among the int_reports rows
-- inside the analysis window, where a case is a linked-duplicate group
-- (int_linked_reports) or else a single report, and its merged_duplicate_count
-- must account for every windowed report it replaced. So the left join to
-- demographics may not fan out, and neither the window filter nor the
-- duplicate collapse may drop or keep anything else.
with windowed as (

    select
        r.report_id,
        coalesce('case:' || l.linked_case_id, 'report:' || r.report_id) as case_key
    from {{ ref('int_reports') }} as r
    left join {{ ref('int_linked_reports') }} as l
        on r.report_id = l.report_id
    where {{ faers_mart_window_filter('r.first_seen_quarter', 'r.last_seen_quarter') }}

),

expected as (

    select
        count(distinct case_key) as case_count,
        count(*) as report_count
    from windowed

),

actual as (

    select
        count(*) as row_count,
        count(*) + sum(merged_duplicate_count) as report_count
    from {{ ref('fct_report') }}

)

select
    expected.case_count as expected_rows,
    actual.row_count as actual_rows,
    expected.report_count as expected_windowed_reports,
    actual.report_count as actual_windowed_reports
from expected
cross join actual
where
    expected.case_count <> actual.row_count
    or expected.report_count <> actual.report_count
