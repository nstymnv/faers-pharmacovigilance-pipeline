-- Business question: which patient groups are most represented among expedited
-- reports?
--
-- One result set for two breakdowns, by age group and by sex, each summing to
-- 100% within its dimension. Each report has exactly one age group and one sex,
-- so these counts, unlike drug or reaction counts, are additive.
--
-- Read share_of_expedited_reports next to share_of_all_reports: a group can be
-- the largest among expedited reports simply because it is the largest overall.
-- 'Not reported' (the sender left the field empty) is kept distinct from
-- 'unknown' (the sender said it is not known).
--
-- Age group: senders fill FAERS's age-group field on only 29% of reports, but
-- give a numeric age on many more. Where the group is missing it is derived
-- from onset_age_years using the ICH E2B(R3) bands the field itself is defined
-- by, which leaves 37% of reports without an age instead of 71%. A reported
-- group is always kept as reported, even where it disagrees with the age.
-- Queries fct_report rather than agg_patient_group_profile for this reason: the
-- aggregate carries only the reported group.
set window_start = '2021Q1';
set window_end = '2025Q4';

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
        iff(grouping(age_group) = 0, 'age_group', 'sex') as dimension,
        -- Neither column is null in reports, so whichever one is null here is
        -- the one this grouping set rolled up.
        coalesce(age_group, sex) as patient_group,
        count(*) as report_count,
        count_if(is_expedited) as expedited_report_count
    from reports
    group by grouping sets (age_group, sex)

)

select
    dimension,
    patient_group,
    expedited_report_count,
    round(ratio_to_report(expedited_report_count) over (partition by dimension), 3)
        as share_of_expedited_reports,
    round(ratio_to_report(report_count) over (partition by dimension), 3)
        as share_of_all_reports
from group_totals
order by dimension asc, expedited_report_count desc;
