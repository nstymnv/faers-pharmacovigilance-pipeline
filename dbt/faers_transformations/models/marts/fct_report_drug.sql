-- Grain: one row per (report_id, drug_key) — one row per distinct product per
-- report. This is the counting fact for every drug question.
--
-- int_drugs is one row per drug *entry*: a report lists a drug once per
-- administered dose, so report 12610564 carries 100 AFSTYLA entries. Counting
-- rows there would let one haemophilia patient's dosing diary outweigh a drug
-- reported by a hundred separate patients. Those entries collapse to one row
-- here, with entry_count keeping the diary visible.
--
-- Dose amounts, routes, indications and dechallenge outcomes are not carried:
-- no documented business question needs them, and copying them would make this
-- a second copy of int_drugs at the wrong grain. They stay in int_drugs, one
-- join from report_id away.
with drugs as (

    select
        report_id,
        {{ dbt_utils.generate_surrogate_key(['medicinal_product', 'active_substance']) }} as drug_key,
        drug_characterization,
        -- 18 start dates and 3 end dates in 2020q1 fall outside 1900-today,
        -- mostly a mistyped leading digit (3019 for 2019). A four-digit typo
        -- parses cleanly, so it is excluded from the exposure window here
        -- rather than plotted; the raw value stays in int_drugs and the staging
        -- accepted_range tests keep counting it.
        iff(start_date between '1900-01-01' and current_date(), start_date, null) as start_date,
        iff(end_date between '1900-01-01' and current_date(), end_date, null) as end_date
    from {{ ref('int_drugs') }}

),

collapsed as (

    select
        report_id,
        drug_key,

        -- The same product can appear on one report as suspect in one entry and
        -- concomitant in another. The strongest attribution wins: a product the
        -- reporter named as suspect even once is a suspect drug for that case.
        max_by(
            drug_characterization,
            {{ faers_drug_role_rank('drug_characterization') }}
        ) as drug_role,

        count(*) as entry_count,
        min(start_date) as first_start_date,
        max(end_date) as last_end_date

    from drugs
    group by report_id, drug_key

)

select
    c.report_id,
    c.drug_key,
    c.drug_role,
    -- "Implicated", not "suspect": the roles counted as causal attribution are
    -- set by the implicated_drug_roles var, which includes interacting as well
    -- as suspect. Concomitant is what the patient also happened to be taking.
    c.drug_role in ({{ "'" ~ var('implicated_drug_roles') | join("', '") ~ "'" }}) as is_implicated,
    c.entry_count,
    -- Null for most rows: 64% of drug entries carry no start date at all. The
    -- window is descriptive, not a time coordinate, so it is published as dates
    -- rather than as keys into dim_date.
    c.first_start_date,
    c.last_end_date,
    -- Denormalized from fct_report because every drug trend groups by quarter
    -- and this saves a join on each one. Only the date key is copied: the
    -- seriousness and expedited flags stay on fct_report rather than being
    -- maintained in three places.
    r.receipt_date_key
from collapsed as c
inner join {{ ref('fct_report') }} as r
    on c.report_id = r.report_id
