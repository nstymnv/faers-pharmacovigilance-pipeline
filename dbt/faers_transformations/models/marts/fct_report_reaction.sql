-- Grain: one row per (report_id, reaction_key). Equals int_reactions row for
-- row.
--
-- A factless fact table: it carries no measure because the row's existence is
-- the fact — this reaction was reported on this report. You count rows.
--
-- Version consolidation and conflicting-outcome resolution already happened in
-- int_reactions, which keeps the most clinically severe outcome where one
-- report gave the same reaction two of them.
select
    r.report_id,
    {{ dbt_utils.generate_surrogate_key(['r.reaction']) }} as reaction_key,
    r.outcome,
    -- The rank behind the most-severe-wins resolution, published so a dashboard
    -- can sort or threshold on severity without restating the ordering in DAX.
    {{ faers_outcome_severity('r.outcome') }} as outcome_severity_rank,
    r.outcome = 'fatal' as is_fatal,
    -- Not part of the key: keying on meddra_version kept a re-coded reaction as
    -- two rows on one report and over-counted every distinct-reaction measure.
    r.meddra_version,
    f.receipt_date_key
from {{ ref('int_reactions') }} as r
inner join {{ ref('fct_report') }} as f
    on r.report_id = f.report_id
