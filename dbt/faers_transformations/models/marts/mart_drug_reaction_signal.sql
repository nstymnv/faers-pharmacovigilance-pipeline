-- Grain: one row per (drug_key, reaction_key) pair actually co-reported at
-- least once.
--
-- Disproportionality analysis: is this drug-reaction pair reported more often
-- than you would expect, given how often the drug and the reaction are each
-- reported overall? It is the standard first-pass pharmacovigilance screen, and
-- it is pure SQL over the bridge.
--
-- What this is NOT: evidence of causation, and not what FDA itself runs. FDA
-- uses Bayesian shrinkage (EBGM/MGPS), which is far less prone to calling a
-- signal on three cases. Treat a signal here as "worth a human look".
--
-- Universe: every report carrying at least one implicated drug. Not all
-- reports, and not only reports for the drug in question — reports with no
-- implicated drug belong to no cell, and including them would inflate d for
-- every pair in the table.
with universe as (

    select distinct report_id
    from {{ ref('fct_report_drug') }}
    where is_implicated

),

universe_size as (

    select count(*) as n_total from universe

),

reports_per_drug as (

    select
        drug_key,
        count(distinct report_id) as reports_with_drug
    from {{ ref('fct_report_drug') }}
    where is_implicated
    group by drug_key

),

reports_per_reaction as (

    select
        r.reaction_key,
        count(distinct r.report_id) as reports_with_reaction
    from {{ ref('fct_report_reaction') }} as r
    inner join universe as u
        on r.report_id = u.report_id
    group by r.reaction_key

),

pairs as (

    -- Distinct reports, never bridge rows: a report listing the same product
    -- under two spellings would otherwise count twice in the cell it defines.
    select
        drug_key,
        reaction_key,
        count(distinct report_id) as a
    from {{ ref('brg_drug_reaction') }}
    group by drug_key, reaction_key

),

cells as (

    select
        p.drug_key,
        p.reaction_key,
        p.a,
        d.reports_with_drug - p.a as b,
        x.reports_with_reaction - p.a as c,
        n.n_total - d.reports_with_drug - x.reports_with_reaction + p.a as d,
        n.n_total
    from pairs as p
    inner join reports_per_drug as d
        on p.drug_key = d.drug_key
    inner join reports_per_reaction as x
        on p.reaction_key = x.reaction_key
    cross join universe_size as n

),

adjusted as (

    -- Haldane-Anscombe correction: every cell + 0.5. Without it the ROR is
    -- undefined whenever b or c is zero, and c = 0 is not a rare edge case —
    -- it is exactly the shape of a reaction only ever reported with this one
    -- drug, which is the most interesting pattern the screen can find. The
    -- uncorrected a/b/c/d are carried through as columns so the correction is
    -- auditable rather than invisible.
    --
    -- Cast to float deliberately. Snowflake divides NUMBER in fixed point and
    -- caps the result scale, which silently truncates the very small ratio
    -- c/(c+d) and threw the PRR off by 0.06% at a = 182. That error grows as
    -- the reaction gets rarer, which is precisely where the signal lives.
    select
        drug_key,
        reaction_key,
        a,
        b,
        c,
        d,
        n_total,
        (a + 0.5)::float as a_adj,
        (b + 0.5)::float as b_adj,
        (c + 0.5)::float as c_adj,
        (d + 0.5)::float as d_adj
    from cells

),

measures as (

    select
        drug_key,
        reaction_key,
        a,
        b,
        c,
        d,
        n_total,

        (
            (a_adj / (a_adj + b_adj))
            / (c_adj / (c_adj + d_adj))
        ) as prr,
        sqrt(
            1.0 / a_adj - 1.0 / (a_adj + b_adj)
            + 1.0 / c_adj - 1.0 / (c_adj + d_adj)
        ) as prr_se,

        ((a_adj * d_adj) / (b_adj * c_adj)) as ror,
        sqrt(
            1.0 / a_adj + 1.0 / b_adj
            + 1.0 / c_adj + 1.0 / d_adj
        ) as ror_se,

        -- Yates' continuity correction, on the raw cells. greatest(..., 0)
        -- because the correction can overshoot on very small tables and a
        -- negative term would square back into a spurious chi-square.
        (
            n_total
            * power(greatest(abs(a * d - b * c) - n_total / 2.0, 0), 2)
        ) / nullif(
            (a + b)::float * (c + d) * (a + c) * (b + d), 0
        ) as chi2_yates

    from adjusted

)

select
    drug_key,
    reaction_key,
    a,
    b,
    c,
    d,
    n_total,
    prr,
    exp(ln(prr) - 1.96 * prr_se) as prr_lower_95,
    exp(ln(prr) + 1.96 * prr_se) as prr_upper_95,
    ror,
    exp(ln(ror) - 1.96 * ror_se) as ror_lower_95,
    exp(ln(ror) + 1.96 * ror_se) as ror_upper_95,
    chi2_yates,
    -- Evans criteria, the conventional FAERS screen. Thresholds are vars: a
    -- pair failing this is not "safe", it is "too few reports to say", and the
    -- gate is where that judgement is encoded.
    (
        a >= {{ var('signal_min_cases') }}
        and prr >= {{ var('signal_min_prr') }}
        and chi2_yates >= {{ var('signal_min_chi2') }}
    ) as is_signal
from measures
