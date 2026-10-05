-- Business question: do unexpected reactions disproportionately occur in
-- particular demographic groups?
--
-- For each age group x sex combination: what share of its reports were
-- expedited, relative to the share across all reports. A rate ratio of 1.20
-- means that group's reports are 20% more likely to be expedited than the
-- average report.
--
-- This compares reports with reports. FAERS has no population denominator, so
-- it cannot say whether a group is more likely to *suffer* an unexpected
-- reaction, only whether its reported cases skew towards expedited ones.
-- Groups below min_reports are left out as too small for a stable rate.
--
-- Age group is the reported one, or derived from onset_age_years where missing;
-- see patient_groups_in_expedited_reports.sql.
set window_start = '2021Q1';
set window_end = '2025Q4';
set min_reports = 10000;

with reports as (

    select
        coalesce(
            r.age_group,
            case
                when r.onset_age_years < 28 / 365.25 then 'neonate'
                when r.onset_age_years < 2 then 'infant'
                when r.onset_age_years < 12 then 'child'
                when r.onset_age_years < 18 then 'adolescent'
                when r.onset_age_years < 65 then 'adult'
                when r.onset_age_years >= 65 then 'elderly'
            end,
            'Not reported'
        ) as age_group,
        coalesce(r.sex, 'Not reported') as sex,
        r.is_expedited
    from faers_db.mart.fct_report as r
    inner join faers_db.mart.dim_date as t
        on r.receipt_date_key = t.date_key
    where t.year_quarter between $window_start and $window_end

),

group_totals as (

    select
        age_group,
        sex,
        count(*) as report_count,
        count_if(is_expedited) as expedited_report_count
    from reports
    group by age_group, sex

),

rates as (

    select
        age_group,
        sex,
        report_count,
        expedited_report_count,
        expedited_report_count / report_count as expedited_rate,
        sum(expedited_report_count) over () / sum(report_count) over () as overall_expedited_rate
    from group_totals

)

select
    age_group,
    sex,
    report_count,
    expedited_report_count,
    round(expedited_rate, 3) as expedited_rate,
    round(expedited_rate / overall_expedited_rate, 2) as rate_ratio_vs_all_reports
from rates
where report_count >= $min_reports
order by rate_ratio_vs_all_reports desc;
