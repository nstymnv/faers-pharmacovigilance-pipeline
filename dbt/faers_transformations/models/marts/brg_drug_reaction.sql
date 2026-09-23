{{ config(cluster_by = ['report_id']) }}

-- Grain: one row per (report_id, drug_key, reaction_key), for implicated drugs
-- only.
--
-- This is a deliberate N x M cross product within each report: a report with 3
-- implicated drugs and 4 reactions produces 12 rows. FAERS never records which
-- drug caused which reaction, so every implicated drug is paired with every
-- reaction on the case and disproportionality analysis sorts out which pairings
-- occur more often than chance would give.
--
-- Two consequences that have to be stated wherever this table is used:
--   * Summing report counts across drugs double-counts reports, by
--     construction. Only per-pair counts mean anything.
--   * Joining int_drugs to int_reactions directly produces the same shape at
--     dose grain — 100 AFSTYLA entries times every reaction — and looks
--     entirely plausible. That is the mistake this table exists to prevent.
--
-- Which roles count as implicated is set by the implicated_drug_roles var
-- (suspect and interacting): both are causal attributions, whereas concomitant
-- is what the patient happened to also be taking.
select
    d.report_id,
    d.drug_key,
    r.reaction_key,
    d.receipt_date_key
from {{ ref('fct_report_drug') }} as d
inner join {{ ref('fct_report_reaction') }} as r
    on d.report_id = r.report_id
where d.is_implicated
