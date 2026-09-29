# Pharmacovigilance pipeline on FDA FAERS data

Pharmacovigilance teams, at drug manufacturers and at regulators, monitor
adverse event reports to spot potential drug safety hazards, follow trends in
what is being reported, and investigate individual cases. The FDA Adverse Event
Reporting System (FAERS) publishes every report it receives, about 1.4 million
a year, as quarterly XML exports: deeply nested, versioned, and hard to analyse
as they come.

This project ingests five years of FAERS (2021–2025, 7.2 million reports) into
Snowflake, models them into a star schema with dbt, adds a
disproportionality (PRR/ROR) signal screen, and answers a set of drug-safety
business questions with plain SQL in [`queries/`](queries/).

- **Ingestion runs entirely inside Snowflake.** A Python stored procedure
  downloads each quarter from FDA straight to an internal stage, and PySpark
  code parses the XML on warehouse compute through Snowpark Connect. Nothing is
  stored locally or in a third-party bucket.
- **dbt** turns raw reports into a star schema: cleaning and decoding, report
  version consolidation, retraction handling, and the fact and dimension tables
  the questions are asked against.
- **Airflow** (Docker Compose) orchestrates stage → load → dbt per quarter.

## Contents

- [Business questions](#business-questions)
- [Findings](#findings)
- [Architecture](#architecture)
- [Data model](#data-model)
- [Data quality decisions](#data-quality-decisions)
- [Limitations](#limitations)
- [Running it](#running-it)
- [Repository layout](#repository-layout)

## Business questions

Each question has one query in [`queries/`](queries/). Every file runs as-is in
a Snowflake worksheet, reads only the `FAERS_DB.MART` schema, and sets its
parameters (analysis window, minimum counts, top N) as session variables at the
top, so they can be changed in one place. The header comment of each file says
how the query answers the question and which traps it avoids.

| # | Question | Query |
|---|---|---|
| | *Context: report volume per year* | [`00_report_volume_by_year.sql`](queries/00_report_volume_by_year.sql) |
| | **Unexpected reactions** | |
| 1 | Which drugs have the highest share of reports meeting the expedited criteria? | [`01_drugs_highest_expedited_share.sql`](queries/01_drugs_highest_expedited_share.sql) |
| 2 | What adverse reactions are reported in those cases? | [`02_reactions_in_expedited_reports.sql`](queries/02_reactions_in_expedited_reports.sql) |
| 3 | Which patient groups are most represented among expedited reports? | [`03_patient_groups_in_expedited_reports.sql`](queries/03_patient_groups_in_expedited_reports.sql) |
| 4 | Do unexpected reactions disproportionately occur in particular demographic groups? | [`04_expedited_rate_by_demographic_group.sql`](queries/04_expedited_rate_by_demographic_group.sql) |
| | **Serious reactions** | |
| 5 | Which drugs have the largest number of serious reactions? | [`05_drugs_most_serious_reports.sql`](queries/05_drugs_most_serious_reports.sql) |
| 6 | What are the most common outcomes for these reactions? | [`06_outcomes_of_serious_reactions.sql`](queries/06_outcomes_of_serious_reactions.sql) |
| | **5-year trends** | |
| 7 | Which drugs show a sustained increase in adverse-event reporting? | [`07_drugs_increasing_reporting.sql`](queries/07_drugs_increasing_reporting.sql) |
| 8 | Which drugs most frequently meet the expedited-reporting criteria? | [`08_drugs_most_often_expedited.sql`](queries/08_drugs_most_often_expedited.sql) |
| 9 | What are the most commonly reported adverse reactions? | [`09_most_common_reactions.sql`](queries/09_most_common_reactions.sql) |
| 10 | Which adverse reactions are becoming more frequently reported? | [`10_reactions_increasing_reporting.sql`](queries/10_reactions_increasing_reporting.sql) |
| 11 | Which outcome categories are growing? | [`11_outcome_trends.sql`](queries/11_outcome_trends.sql) |
| | **Signal detection** | |
| 12 | Which drug–reaction pairs are reported together far more often than chance predicts? | [`12_disproportionality_signals.sql`](queries/12_disproportionality_signals.sql) |

**Terms used throughout:**

- **Serious** — FDA's definition: the event led to death, a life-threatening
  condition, hospitalisation, disability, a congenital anomaly, or another
  medically important condition. Recorded per report, not per reaction.
- **Expedited** — a report a manufacturer had to send within 15 days because
  the event was both serious and *unexpected* (not described in the drug's
  label). This project uses it as the proxy for "unexpected reactions".
- **Implicated** — the reporter named the drug as *suspect* or *interacting*,
  as opposed to *concomitant* (something the patient was also taking). Drug
  rankings count implicated reports only, so that widely co-prescribed drugs
  don't top the lists on cases nobody suspects them of.

## Findings

Results as of **2026-09-29**, over reports received 2021Q1–2025Q4. Each table
shows the top of the query's output; run the query for the full list. These are
counts of *reports*, which reflect reporting behaviour as much as drug safety
(see [Limitations](#limitations)).

### Context: report volume

| Year | Reports | Serious share | Expedited share |
|---|---:|---:|---:|
| 2021 | 1,561,496 | 64.2% | 55.7% |
| 2022 | 1,526,834 | 58.1% | 51.5% |
| 2023 | 1,385,486 | 56.2% | 53.7% |
| 2024 | 1,340,059 | 53.1% | 50.5% |
| 2025 | 1,389,112 | 55.8% | 51.4% |

Total volume fell about 11% from 2021 to 2025, so any drug or reaction that
grew did so against the tide.

### 1. Highest share of expedited reports

Drugs implicated in at least 1,000 reports:

| Product | Implicated reports | Expedited | Share |
|---|---:|---:|---:|
| Ranitidine capsule | 31,446 | 31,446 | 100% |
| Ranitidine hydrochloride (852) | 9,314 | 9,314 | 100% |
| Ranitidine hydrochloride (156) | 5,879 | 5,879 | 100% |
| Duodopa (carbidopa/levodopa) | 2,326 | 2,326 | 100% |
| Infliximab, recombinant | 1,048 | 1,048 | 100% |
| Phthalylsulfathiazole | 5,788 | 5,781 | 99.9% |
| Crysvita (burosumab) | 3,638 | 3,633 | 99.9% |
| Jakavi (ruxolitinib) | 1,127 | 1,126 | 99.9% |

The top of this list is not a ranking of dangerous drugs; it mostly reflects
*how* a drug's reports reach FDA. Ranitidine reports arrived in bulk during the
litigation after its 2020 withdrawal, and a share near 100% usually means
almost every report came through the manufacturer as a serious, unlabelled
case. Drugs with large patient-support programmes that also generate
non-serious reports, such as Humira (43%), sit far lower.

### 2. Reactions in expedited reports

| Reaction | Expedited reports | Share of expedited | Share of all | Over-representation |
|---|---:|---:|---:|---:|
| Death | 268,245 | 7.1% | 4.1% | 1.72 |
| Off label use | 255,083 | 6.7% | 6.1% | 1.10 |
| Drug ineffective | 185,208 | 4.9% | 6.3% | 0.78 |
| Fatigue | 139,235 | 3.7% | 3.8% | 0.98 |
| Nausea | 121,740 | 3.2% | 3.3% | 0.98 |
| Dyspnoea | 110,472 | 2.9% | 2.4% | 1.21 |
| Pneumonia | 84,996 | 2.2% | 1.5% | 1.55 |
| Pyrexia | 78,900 | 2.1% | 1.5% | 1.39 |
| Fall | 75,527 | 2.0% | 1.4% | 1.43 |

Expedited cases are mostly the same common reactions as all reports, but
death, pneumonia, falls and fever are markedly over-represented in them, while
"drug ineffective" and administration issues (such as a missed dose, at 0.53)
are under-represented: they are rarely serious.

### 3. Patient groups among expedited reports

| Age group | Share of expedited | Share of all | | Sex | Share of expedited | Share of all |
|---|---:|---:|---|---|---:|---:|
| Adult (18–64) | 37.1% | 34.8% | | Female | 46.6% | 49.4% |
| Elderly (65+) | 26.2% | 23.4% | | Male | 38.9% | 35.4% |
| Adolescent / child | 3.4% | 3.6% | | Not reported | 14.5% | 15.0% |
| Infant / neonate | 1.0% | 0.7% | | | | |
| Not reported | 32.2% | 37.4% | | | | |

Women file most reports overall, but men and the elderly make up a larger share
of the expedited ones than of all reports.

### 4. Expedited rate by demographic group

| Age group | Sex | Reports | Expedited rate | vs. all reports |
|---|---|---:|---:|---:|
| Infant | Female | 11,199 | 70.2% | 1.33× |
| Infant | Male | 13,958 | 68.7% | 1.31× |
| Elderly | Male | 744,311 | 63.8% | 1.21× |
| Adult | Male | 929,176 | 62.8% | 1.19× |
| Elderly | Female | 892,698 | 55.1% | 1.05× |
| Adult | Female | 1,492,237 | 52.1% | 0.99× |
| Child | Female | 53,896 | 47.2% | 0.90× |
| Child | Male | 68,275 | 47.6% | 0.90× |
| Adolescent | Male | 63,703 | 44.5% | 0.84× |

Yes: infants' reports are a third more likely to be expedited than the average
report, and men's are more likely than women's in every adult age band. This
compares reports with reports; FAERS has no population denominator, so it shows
which groups' reported cases skew towards serious-and-unexpected, not which
groups are at higher risk.

### 5. Drugs with the most serious reports

| Product | Serious reports (implicated) | Serious share | Deaths | Serious, as concomitant |
|---|---:|---:|---:|---:|
| Zantac (ranitidine) | 280,497 | 99.8% | 16,820 | 3,584 |
| Ranitidine | 127,466 | 99.7% | 8,772 | 7,045 |
| Humira (adalimumab) | 85,720 | 48.9% | 8,179 | 6,770 |
| OxyContin (oxycodone) | 80,637 | 98.0% | 6,542 | 2,631 |
| Prednisone | 80,019 | 92.7% | 12,252 | 92,216 |
| Methotrexate | 76,383 | 92.2% | 9,394 | 38,441 |
| Rituximab | 75,465 | 93.8% | 15,172 | 9,007 |
| Cyclophosphamide | 63,806 | 97.7% | 11,356 | 13,541 |
| Dexamethasone | 60,071 | 96.3% | 10,322 | 44,542 |
| Revlimid (lenalidomide) | 54,116 | 41.0% | 8,356 | 1,444 |

Ranitidine and, most likely, OxyContin rank on litigation-driven reporting. The rest are
immunosuppressants and cancer drugs, given to patients who are already very
ill. The last column shows why the role filter matters: prednisone appears on
more serious reports as a drug the patient was merely also taking than as a
suspect.

### 6. Outcomes of serious reactions

| Outcome | Share of serious-report reactions | Share of non-serious-report reactions |
|---|---:|---:|
| Fatal | 7.0% | 0.0% |
| Recovered with sequelae | 0.4% | 0.1% |
| Not recovered / not resolved | 15.8% | 12.2% |
| Recovering / resolving | 7.6% | 3.9% |
| Recovered / resolved | 14.7% | 9.7% |
| Unknown | 50.3% | 69.0% |
| Not reported | 4.1% | 5.0% |

Half of all reactions on serious reports have an unknown outcome; of those with
a known one, "not recovered" is the most common, and 7% are fatal.

### 7. Drugs with a sustained increase in reporting

Growth is the least-squares slope of quarterly reports over the 20 quarters,
divided by the average quarterly count (0.10 ≈ +10% of the average per
quarter), so steady growth ranks above a single spike. *Established* drugs had
at least 100 reports in 2021. Drugs below that, mostly launched during the
window, are ranked separately, since their growth is largely the launch itself.
Only drugs with at least 1,000 reports in the window are ranked.

| Cohort | Product | 2021 | 2025 | Growth / quarter |
|---|---|---:|---:|---:|
| Established | Tymlos (abaloparatide) | 223 | 5,981 | 0.204 |
| Established | Benralizumab | 138 | 2,130 | 0.199 |
| Established | Acalabrutinib | 162 | 1,876 | 0.163 |
| Established | Osimertinib | 434 | 3,470 | 0.150 |
| Established | Orgovyx (relugolix) | 365 | 11,855 | 0.144 |
| Established | Nubeqa (darolutamide) | 165 | 1,990 | 0.137 |
| Established | Wegovy (semaglutide) | 483 | 5,906 | 0.129 |
| Established | Depo-Provera (medroxyprogesterone) | 180 | 2,297 | 0.128 |
| New | Bimzelx (bimekizumab) | 0 | 8,446 | 0.263 |
| New | Nemluvio (nemolizumab) | 0 | 7,163 | 0.257 |
| New | Kisunla (donanemab) | 0 | 1,361 | 0.246 |
| New | Cobenfy (xanomeline/trospium) | 0 | 1,255 | 0.232 |
| New | Winrevair (sotatercept) | 0 | 2,050 | 0.229 |

The established list is mostly recent launches still ramping up (Orgovyx,
Nubeqa, Wegovy) and oncology drugs moving into wider use. Depo-Provera is the
exception: a decades-old drug whose reporting grew twelvefold after its
meningioma association was publicised (see question 10).

### 8. Drugs most often meeting the expedited criteria

| Product | Expedited reports (implicated) | Expedited share | 2021 | 2025 |
|---|---:|---:|---:|---:|
| Zantac (ranitidine) | 263,211 | 93.6% | 134,627 | 39 |
| Ranitidine | 124,963 | 97.8% | 95,904 | 603 |
| Prednisone | 78,529 | 91.0% | 14,646 | 18,090 |
| Methotrexate | 76,268 | 92.1% | 15,832 | 13,416 |
| Humira (adalimumab) | 75,241 | 42.9% | 23,406 | 8,881 |
| Rituximab | 74,397 | 92.4% | 12,454 | 16,389 |
| Cyclophosphamide | 62,005 | 94.9% | 8,946 | 13,689 |
| Skyrizi (risankizumab) | 42,360 | 63.3% | 3,183 | 15,274 |
| Rinvoq (upadacitinib) | 40,362 | 64.7% | 4,178 | 12,019 |

Expedited reports for Zantac and generic ranitidine together fell from about
230,000 in 2021 to about 640 in 2025 as the litigation wave passed. Humira's fell as biosimilars took its market,
while its successors in immunology, Skyrizi and Rinvoq, grew three- to
fivefold.

### 9. Most commonly reported reactions

| Reaction | Reports | Share of all reports | Serious share |
|---|---:|---:|---:|
| Drug ineffective | 452,492 | 6.3% | 41.3% |
| Off label use | 439,647 | 6.1% | 58.6% |
| Death | 296,702 | 4.1% | 99.3% |
| Fatigue | 270,207 | 3.8% | 54.2% |
| Pain | 238,725 | 3.3% | 68.4% |
| Nausea | 236,877 | 3.3% | 54.7% |
| Product dose omission issue | 223,949 | 3.1% | 26.9% |
| Diarrhoea | 222,164 | 3.1% | 56.7% |
| Headache | 185,242 | 2.6% | 51.6% |
| Dyspnoea | 173,581 | 2.4% | 70.2% |

The top of the list is not clinical at all: lack of effect, off-label use and
missed doses are MedDRA terms too, and FAERS records them as reactions.

### 10. Reactions with growing reporting

Growth is the least-squares slope of quarterly reports over the 20 quarters,
divided by the average quarterly count (0.10 ≈ +10% of the average per
quarter). *Established* reactions had at least 100 reports in 2021; the others
are ranked separately, since their growth is mostly their first appearance.

| Cohort | Reaction | 2021 | 2025 | Growth / quarter |
|---|---|---:|---:|---:|
| Established | Therapeutic response changed | 193 | 1,610 | 0.162 |
| Established | Exposure via skin contact | 474 | 7,811 | 0.154 |
| Established | Meningioma | 175 | 1,779 | 0.153 |
| Established | Drug diversion | 235 | 1,099 | 0.126 |
| Established | Dissociation | 346 | 2,088 | 0.115 |
| Established | Optic ischaemic neuropathy | 105 | 599 | 0.112 |
| Established | Impaired gastric emptying | 479 | 2,472 | 0.112 |
| Established | Dermatitis atopic | 3,803 | 15,842 | 0.103 |
| New | Rebound atopic dermatitis | 58 | 1,597 | 0.192 |
| New | Rebound eczema | 82 | 1,385 | 0.179 |
| New | Eosinophilic oesophagitis | 81 | 981 | 0.166 |
| New | Brain fog | 0 | 4,821 | 0.149 |

Most of these trace back to a handful of drugs; the one most often co-reported
with each reaction, from the signal mart:

- **Meningioma** — Depo-Provera (medroxyprogesterone), which gained a
  meningioma warning in 2024.
- **Impaired gastric emptying** and **optic ischaemic neuropathy** — GLP-1
  agonists (Ozempic, Wegovy, Mounjaro, Trulicity).
- **Therapeutic response changed** — Zepbound (tirzepatide).
- **Dissociation** — Spravato (esketamine nasal spray).
- **Rebound atopic dermatitis, rebound eczema, eosinophilic oesophagitis,
  exposure via skin contact** — Dupixent (dupilumab), whose reporting grew with
  its use.

### 11. Outcome trends

| Outcome | 2021 reactions | 2025 reactions | 2021 share | 2025 share | Growth / quarter |
|---|---:|---:|---:|---:|---:|
| Fatal | 182,336 | 259,435 | 4.2% | 6.0% | +0.024 |
| Recovering / resolving | 243,850 | 316,227 | 5.6% | 7.3% | +0.017 |
| Recovered / resolved | 555,494 | 587,339 | 12.8% | 13.5% | +0.003 |
| Unknown | 2,496,186 | 2,425,610 | 57.5% | 55.7% | −0.002 |
| Not recovered / not resolved | 643,554 | 594,440 | 14.8% | 13.7% | −0.008 |
| Not reported | 206,767 | 157,561 | 4.8% | 3.6% | −0.019 |

Fatal outcomes grew fastest, up 42% in count and from 4.2% to 6.0% of reactions
while total volume fell. Part of this is better outcome reporting: "not
reported" shrank over the same period.

### 12. Disproportionality signals

Pairs passing the Evans screen with at least 100 co-reported cases, where the
drug and the reaction each appear on at least 1,000 reports, ranked by the
lower bound of the PRR's 95% confidence interval:

| Product | Reaction | Cases | PRR | PRR lower 95% |
|---|---|---:|---:|---:|
| ParaGard (copper IUD) | Reproductive complication associated with device | 3,324 | 3,029,000 | 189,400 |
| ParaGard (copper IUD) | Foreign body in reproductive tract | 6,317 | 112,900 | 76,520 |
| Belantamab mafodotin | Keratopathy | 800 | 8,261 | 7,303 |
| Viread (tenofovir DF) | Skeletal injury | 7,185 | 5,515 | 5,110 |
| Elmiron (pentosan polysulfate) | Maculopathy | 1,291 | 5,476 | 4,984 |
| Depo-Provera | Meningioma | 1,613 | 2,724 | 2,550 |
| Oxbryta (voxelotor) | Sickle cell anaemia with crisis | 9,662 | 1,821 | 1,740 |

These are textbook: ParaGard arm breakage (the subject of litigation),
belantamab's boxed warning for corneal damage, tenofovir disoproxil's bone
toxicity, Elmiron's pigmentary maculopathy, Depo-Provera's meningioma warning
(2024), and Oxbryta, withdrawn worldwide in 2024 over vaso-occlusive crises and
deaths. PRRs this large mean the reaction is almost never reported *without*
the drug. Further down, a few pairs come from clusters of near-identical reports
(phthalylsulfathiazole, desoximetasone) rather than pharmacology.

## Architecture

```mermaid
flowchart LR
    FDA["FDA FAERS<br/>quarterly XML ZIPs"]

    subgraph Snowflake
        direction LR
        SP["Stored procedure<br/>INGEST_FAERS_QUARTER<br/>(external access)"]
        STAGE[("@FAERS_RAW<br/>internal stage<br/>raw XML archive")]
        SPARK["PySpark via<br/>Snowpark Connect<br/>(parse + flatten)"]
        RAW[("RAW<br/>reports · demographics<br/>drug · reaction")]
        STG[("STAGING<br/>clean + decode")]
        INT[("INTERMEDIATE<br/>versions merged,<br/>retractions removed")]
        MART[("MART<br/>star schema,<br/>aggregates, signals")]
        SP --> STAGE --> SPARK --> RAW --> STG --> INT --> MART
    end

    FDA --> SP
    MART --> Q["queries/*.sql"]
    AF["Airflow (Docker)<br/>orchestration"] -.-> SP
    AF -.-> SPARK
    AF -.-> STG
```

1. **Stage** — `extraction.ingest_faers_quarter`
   ([`sprocs/ingest_faers_quarter.py`](src/faers_ingestion/sprocs/ingest_faers_quarter.py))
   is a Python stored procedure with an external access integration limited to
   `fis.fda.gov`. It streams a quarter's ZIP and writes each XML file to the
   `@FAERS_RAW` stage uncompressed, so the raw zone can be re-parsed without
   downloading from FDA again. It handles FDA's Deflate64-compressed quarters,
   which Python's `zipfile` cannot read, and logs every attempt to
   `EXTRACTION.INGESTION_LOG`.
2. **Load** — [`faers_ingestion.main`](src/faers_ingestion/main.py) runs PySpark
   DataFrame code on warehouse compute through Snowpark Connect for Spark: no
   local Spark, JVM or cluster. It reads the staged XML with a fixed schema
   ([`safetyreport_schema.json`](src/faers_ingestion/extract/safetyreport_schema.json)),
   first scanning each quarter for elements the schema doesn't know
   ([`schema_drift.py`](src/faers_ingestion/extract/schema_drift.py)), since FDA
   adds fields over time and an unknown element breaks the load with a
   misleading error. It then flattens the nested `safetyreport` into four RAW
   tables.
3. **Transform** — dbt ([`dbt/faers_transformations`](dbt/faers_transformations))
   builds staging → intermediate → marts, all as tables in their own schemas.
4. **Orchestrate** — [`faers_quarterly`](docker/airflow/dags/faers_quarterly.py)
   finds quarters not yet loaded, then stages and loads each one, and runs dbt
   through Cosmos with one Airflow task per model and its tests. The backfill
   goes through the same DAG, capped by a `max_quarters` parameter so an
   accidental trigger costs one quarter's credits. Airflow only issues
   commands; all compute is Snowflake's.

**Cost guardrails:** an XSMALL warehouse with 60-second auto-suspend, a
monthly resource monitor (`sql/04`), and a `faers_quarters` dbt var that
restricts a dev run to a single quarter.

## Data model

| Layer | Models | Job |
|---|---|---|
| Staging | `stg_reports`, `stg_demographics`, `stg_drugs`, `stg_reaction`, `stg_deleted_cases` | Rename, cast, decode FAERS codes into readable values through seed mappings (country, route, units, reaction groups) |
| Intermediate | `int_reports`, `int_demographics`, `int_drugs`, `int_reactions` | One row per report: consolidate versions, drop FDA retractions, resolve conflicting values |
| Marts: star | `fct_report`, `fct_report_drug`, `fct_report_reaction`, `dim_drug`, `dim_reaction`, `dim_date`, `dim_country` | The report is the spine; drugs and reactions hang off it at their own grain |
| Marts: signal | `brg_drug_reaction`, `mart_drug_reaction_signal` | Every implicated drug × every reaction on a report, and PRR / ROR / χ² with 95% CIs per pair |
| Marts: aggregates | `agg_drug_quarterly_trend`, `agg_expedited_drug_profile`, `agg_reaction_frequency`, `agg_outcome_trend`, `agg_patient_group_profile` | Pre-counted distinct reports per quarter for the trend questions |

The design reasoning, with the measurements behind each decision, is in
[`dbt/faers_transformations/models/README.md`](dbt/faers_transformations/models/README.md).
Model and column descriptions are pushed into Snowflake comments
(`persist_docs`), so they show up when browsing `MART` directly.

**Tests:** schema tests on keys and relationships, accepted values and ranges;
singular tests that cross-check the marts against each other (e.g. the four
contingency cells of every signal pair sum to the universe); dbt unit tests on
the trickiest logic (PRR/ROR math, dose-entry collapse, onset-age
normalization, the report version merge); a warning test for reaction terms
missing from the reaction-group seed; and pytest tests for the ingestion code
that runs without Snowflake (quarter parsing, config, the procedure's ZIP
handling). More in [`dbt/faers_transformations/README.md`](dbt/faers_transformations/README.md).

**CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)): ruff, pytest and
sqlfluff on `queries/` run on every push. `dbt build` on the 2020q1 slice, then
sqlfluff over the dbt project, runs on pull requests into `main` and on demand,
as a least-privilege `FAERS_CI` service user that can only read `RAW` and write
to `CI_STAGING`/`CI_INTERMEDIATE`/`CI_MART`, never the real layers.

## Data quality decisions

Things that looked like they needed cleaning but didn't, or that would silently
distort a count if handled naively:

- **Report versions are merged, not filtered.** A report is re-sent with each
  follow-up. The intermediate layer takes the latest *non-null* value per field
  across versions (`MAX_BY(field, IFF(field IS NOT NULL, version, NULL))`)
  rather than letting the latest row win, which would drop details a later
  follow-up left out.
- **FAERS's `duplicate` columns are not a duplicate flag.** Measured on the
  data, they are a sender's case-linkage identifier, so nothing is filtered on
  them. FDA retractions (the deleted-cases file) are excluded.
- **A drug is listed once per dose.** One report can list the same product
  dozens of times with different doses and dates, so every drug count here is
  `count(distinct report_id)`, never a count of rows.
- **Conflicting reaction outcomes resolve to the most severe.**
- **FAERS format drift.** From 2021Q4 the XML gained a new element and writes
  seriousness flags explicitly; before 2021Q4, 29% of product names end in a
  stray period (`PREDNISONE.`), which would split one drug into two and fake a
  jump in every drug trend. Both are handled: a drift check before each load,
  and a single trailing period stripped in `stg_drugs`.
- **Absent is not "unknown".** An empty sex field stays null, separate from a
  sender explicitly coding "unknown": merging them overstated that category
  about 500-fold.
- **Age group is derived where missing.** Senders fill FAERS's age-group field
  on only 29% of reports but give a numeric age on many more; queries 03 and 04
  derive the group from the normalised age using the ICH E2B bands, which cuts
  "not reported" from 71% to 37%.
- **The analysis window is set in receipt quarters.** `receipt_date` is the date
  of a report's *latest* follow-up, so MART also holds a few reports in 2026Q1–Q2.
  Those quarters would look like a collapse at the end of every trend, so the
  queries stop at 2025Q4.

## Limitations

- **Reports are not incidence.** FAERS is voluntary and stimulated reporting:
  media attention, litigation and new labelling all drive volume. Ranitidine
  (Zantac), withdrawn in 2020 over NDMA contamination and then the subject of
  mass litigation, dominates 2021's serious and expedited reports and nearly
  vanishes by 2025. There is no exposure denominator (how many people took
  the drug), so no rate here is a risk.
- **No drug-name normalisation.** `dim_drug` holds products as reported:
  "ZANTAC", "RANITIDINE" and "RANITIDINE CAPSULE" are separate rows, and no
  brand is mapped to its generic. This is the biggest limit on every "which
  drugs" answer; mapping to RxNorm would be a project of its own.
- **The signal screen is the simple one.** PRR/ROR with Evans criteria is the
  standard first-pass screen, but FDA uses Bayesian shrinkage (EBGM), which is
  far less prone to flagging small counts. A signal here means "worth a human
  look", not causation. Clusters of near-identical reports from one sender
  (phthalylsulfathiazole, desoximetasone) still produce implausible pairs.
- **Reaction groups are this project's own.** The custom reaction grouping was
  AI-assisted and manually reviewed, not MedDRA's official hierarchy (which is
  licensed); every reaction carries a `rule` / `fallback` flag saying how it was
  grouped.
- **True clinical duplicates are not detected.** The same case reported by a
  doctor and by a manufacturer appears twice.

## Running it

**Prerequisites:** a Snowflake account (the scripts assume a role that can
create a database, a warehouse and one external access integration), Python
3.12, and Docker if you want Airflow.

1. **Bootstrap Snowflake** by running the scripts in [`sql/`](sql/) in order in
   a worksheet: database, schemas, warehouse, resource monitor, then the stage,
   network rule, ingestion log and FDA external access integration (01–05).
   `06` creates the CI identity and is only needed for CI. All are idempotent.
2. **Configure** — copy `.env.example` to `.env` and fill in the account, user,
   role and path to a key-pair private key (key-pair auth only).
3. **Install:**
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt -r requirements-dbt.txt && pip install -e .
   pip install -r requirements-dev.txt   # optional: ruff, sqlfluff, pytest
   ```
4. **Deploy the ingestion procedure, then stage and load a quarter:**
   ```bash
   python -m faers_ingestion.sprocs.deploy
   python -m faers_ingestion.main --quarters 2021q1 --ingest
   ```
5. **Build the warehouse:**
   ```bash
   cd dbt/faers_transformations
   dbt deps && dbt seed && dbt build
   ```
   For a quick dev run on one quarter: `dbt build --vars '{faers_quarters: [2020q1]}'`.
6. **Or run it all from Airflow:** fill in `docker/airflow/.env` from its
   example, then `docker compose up -d` in `docker/airflow/` and trigger
   `faers_quarterly` (raise `max_quarters` for a backfill).
7. **Answer the questions:** open any file in [`queries/`](queries/) in a
   Snowflake worksheet and run it.
8. **Checks:** `pytest`, `ruff check src test`, `sqlfluff lint queries/`, and
   `sqlfluff lint models tests analyses` from `dbt/faers_transformations`. For
   CI's dbt job, run `sql/06_create_ci_identity.sql` and add the GitHub secrets
   `SNOWFLAKE_ACCOUNT` and `FAERS_CI_PRIVATE_KEY` (the PEM text of the CI key).

## Repository layout

```
queries/                    business-question SQL, one file per question
src/faers_ingestion/
  sprocs/                   in-Snowflake download-and-stage procedure + deploy script
  extract/                  Snowpark Connect session, XML schema, drift check
  load/                     flatten to RAW tables, deleted-cases load
  main.py                   stage → load entry point
dbt/faers_transformations/
  models/staging|intermediate|marts
  models/README.md          modelling decisions and the measurements behind them
  seeds/                    code mappings (country, route, units, reaction groups)
  tests/                    cross-model consistency tests
  ci/                       dbt profile and sqlfluff config for CI
docker/airflow/             Airflow on Docker Compose + the quarterly DAG
sql/                        one-time Snowflake bootstrap scripts (06 = CI identity)
test/                       pytest tests for the ingestion package
.github/workflows/ci.yml    lint, tests, dbt build on the dev slice
```

**Stack:** Snowflake (stages, stored procedures, external access, Snowpark
Connect for Spark), PySpark, dbt Core + dbt-utils, Apache Airflow + Astronomer
Cosmos, Docker Compose, GitHub Actions, ruff, sqlfluff, pytest.
