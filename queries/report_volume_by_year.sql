-- Context for every other query: how many reports FAERS received per year, and
-- what share were serious or expedited. Trends in any single drug, reaction or
-- outcome should be read against these totals.
--
-- Serious share is over reports whose seriousness is known: from 2025q4 FAERS
-- accepts reports with no seriousness value, and is_serious is null there.
--
-- The window is set in receipt quarters. MART also holds 2026Q1-Q2, because
-- receipt_date is the date of a report's latest follow-up; those quarters are
-- mostly empty and would read as a collapse at the end of every trend.
set window_start = '2021Q1';
set window_end = '2025Q4';

select
    t.year,
    count(*) as report_count,
    count_if(r.is_serious) as serious_report_count,
    round(count_if(r.is_serious) / count_if(r.is_serious is not null), 3) as serious_share,
    count_if(r.is_expedited) as expedited_report_count,
    round(count_if(r.is_expedited) / count(*), 3) as expedited_share
from faers_db.mart.fct_report as r
inner join faers_db.mart.dim_date as t
    on r.receipt_date_key = t.date_key
where t.year_quarter between $window_start and $window_end
group by t.year
order by t.year;
