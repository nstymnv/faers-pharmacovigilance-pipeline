-- Grain: one row per (age_group, sex, year_quarter).
--
-- Answers "which patient groups are most represented among expedited reports"
-- and "do unexpected reactions disproportionately occur in particular
-- demographic groups".
--
-- Unreported age groups and sexes become 'Not reported' rather than staying
-- null, so the grain has no null components and the group stays visible in a
-- breakdown instead of silently dropping out of it. 'Not reported' is distinct
-- from the reported value 'unknown', which is a sender explicitly saying the
-- patient's sex is not known — stg_demographics keeps those apart deliberately.
--
-- Reading these as rates needs an external population denominator that FAERS
-- does not provide: a group can dominate simply because more of its members
-- take more drugs. Compare shares between expedited and all reports, not
-- absolute counts.
select
    coalesce(r.age_group, 'Not reported') as age_group,
    coalesce(r.sex, 'Not reported') as sex,
    t.year_quarter,
    count(*) as report_count,
    count_if(r.is_expedited) as expedited_report_count,
    count_if(r.is_serious) as serious_report_count,
    count_if(r.death) as death_report_count,
    round(avg(r.onset_age_years), 2) as mean_onset_age_years
from {{ ref('fct_report') }} as r
inner join {{ ref('dim_date') }} as t
    on r.receipt_date_key = t.date_key
group by
    coalesce(r.age_group, 'Not reported'),
    coalesce(r.sex, 'Not reported'),
    t.year_quarter
