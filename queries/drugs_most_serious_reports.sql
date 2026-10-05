-- Business question: which drugs have the largest number of serious reactions?
--
-- In FAERS seriousness is recorded per report, not per reaction: a report is
-- serious if the case led to death, a life-threatening event, hospitalisation,
-- disability, a congenital anomaly or another medically important condition.
-- So this counts serious reports per drug.
--
-- Only reports where the drug was implicated (suspect or interacting) count
-- towards the ranking. A drug the patient merely also took (concomitant) is
-- shown alongside for comparison, because widely co-prescribed drugs would
-- otherwise rank high on cases they are not suspected of causing.
--
-- Queries the facts directly rather than agg_drug_quarterly_trend, which does
-- not split its serious count by drug role. The serious share is over reports
-- whose seriousness is known (null from 2025q4 is possible).
set window_start = '2021Q1';
set window_end = '2025Q4';

with drug_reports as (

    select
        f.drug_key,
        f.is_implicated,
        r.is_serious,
        r.death
    from faers_db.mart.fct_report_drug as f
    inner join faers_db.mart.fct_report as r
        on f.report_id = r.report_id
    inner join faers_db.mart.dim_date as t
        on f.receipt_date_key = t.date_key
    where t.year_quarter between $window_start and $window_end

),

drug_totals as (

    -- fct_report_drug has one row per (report, drug), so count_if counts
    -- distinct reports.
    select
        drug_key,
        count_if(is_implicated and is_serious) as serious_implicated_report_count,
        count_if(is_implicated and is_serious is not null) as implicated_known_seriousness_count,
        count_if(is_implicated and death) as death_implicated_report_count,
        count_if(not is_implicated and is_serious) as serious_concomitant_report_count
    from drug_reports
    group by drug_key

)

select
    d.medicinal_product,
    d.active_substance,
    k.serious_implicated_report_count,
    round(k.serious_implicated_report_count / nullif(k.implicated_known_seriousness_count, 0), 3)
        as serious_share_when_implicated,
    k.death_implicated_report_count,
    k.serious_concomitant_report_count
from drug_totals as k
inner join faers_db.mart.dim_drug as d
    on k.drug_key = d.drug_key
order by k.serious_implicated_report_count desc
limit 20;
