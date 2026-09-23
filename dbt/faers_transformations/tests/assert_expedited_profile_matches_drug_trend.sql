-- agg_expedited_drug_profile and agg_drug_quarterly_trend compute the same two
-- counts from the same facts, kept as separate models so the share sits next to
-- its own denominator. They must agree: if they ever diverge, one of them has
-- picked up a filter or a join the other has not, and two dashboard pages will
-- quietly disagree about the same drug.
select
    coalesce(p.drug_key, t.drug_key) as drug_key,
    coalesce(p.year_quarter, t.year_quarter) as year_quarter,
    p.report_count as profile_report_count,
    t.report_count as trend_report_count,
    p.expedited_report_count as profile_expedited_count,
    t.expedited_report_count as trend_expedited_count
from {{ ref('agg_expedited_drug_profile') }} as p
full outer join {{ ref('agg_drug_quarterly_trend') }} as t
    on p.drug_key = t.drug_key
    and p.year_quarter = t.year_quarter
where
    p.drug_key is null
    or t.drug_key is null
    or p.report_count <> t.report_count
    or p.expedited_report_count <> t.expedited_report_count
