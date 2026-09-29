-- Beyond the listed questions: which drug-reaction pairs are reported together
-- far more often than chance would predict?
--
-- mart_drug_reaction_signal computes the proportional reporting ratio (PRR),
-- reporting odds ratio (ROR) and chi-square for every co-reported pair, and
-- flags is_signal on the Evans criteria (at least 3 cases, PRR >= 2,
-- chi-square >= 4). That screen alone passes about a million pairs, most on a
-- handful of cases. This ranks by the lower bound of the PRR's 95% confidence
-- interval, so a pair ranks high only if its disproportion is both large and
-- well supported, and applies three floors:
-- - min_cases: reports carrying both the drug and the reaction;
-- - min_drug_reports and min_reaction_reports: how often the drug and the
--   reaction are reported at all. Without these the top is taken by clusters of
--   near-identical reports (one sender, one patient, re-reported with dozens of
--   drugs): a reaction seen almost only in that cluster pairs with every drug
--   in it at a PRR in the tens of thousands.
--
-- A signal is a reason for a human to look, not evidence that the drug causes
-- the reaction. Many top pairs are expected: the reaction is the condition the
-- drug treats, or a known labelled effect.
--
-- The mart covers every report in MART, so no window variables apply here.
set min_cases = 100;
set min_drug_reports = 1000;
set min_reaction_reports = 1000;

select
    d.medicinal_product,
    d.active_substance,
    x.reaction,
    s.a as case_count,
    s.a + s.b as drug_report_count,
    s.a + s.c as reaction_report_count,
    round(s.prr, 1) as prr,
    round(s.prr_lower_95, 1) as prr_lower_95,
    round(s.ror, 1) as ror,
    round(s.chi2_yates, 0) as chi2_yates
from faers_db.mart.mart_drug_reaction_signal as s
inner join faers_db.mart.dim_drug as d
    on s.drug_key = d.drug_key
inner join faers_db.mart.dim_reaction as x
    on s.reaction_key = x.reaction_key
where
    s.is_signal
    and s.a >= $min_cases
    and s.a + s.b >= $min_drug_reports
    and s.a + s.c >= $min_reaction_reports
order by s.prr_lower_95 desc
limit 25;
