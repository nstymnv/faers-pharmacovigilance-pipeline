# FAERS Pipeline — Next Steps

_Drafted 2026-09-18. Status: proposal, pending review._

## Context

The pipeline today runs FAERS XML → local Spark → Snowflake RAW → dbt staging → dbt intermediate. It works, but three things block it from being a finished project:

1. **Ingestion depends on your laptop.** `downloader.py` is orphaned (nothing calls it) and its `BeautifulSoup` scrape of the FAERS index page almost certainly returns `[]`, because that page renders its file list in JavaScript. So in practice `main.py` reads XML you downloaded by hand into `data/raw/XML/`. Spark then runs `local[*]` on your machine — and can't, because the `.venv` is Python 3.14 while `pyspark==3.5.0` doesn't support 3.13+. The `docker/cluster/` directory is a verbatim copy of a public Spark-in-Docker tutorial (`da-spark-*`, `book_data`) that never sees this project's code.
2. **There is no marts layer.** The three business questions in `docs/project-overview.md` have nothing to answer them, and no dashboard exists.
3. **Nothing orchestrates anything.** Every stage is run by hand.

**The intended outcome:** ingestion runs entirely inside Snowflake — no local storage, no third-party buckets — while keeping the Spark DataFrame code you've written; a dimensional marts layer answers the documented business questions plus drug-safety signal detection; Airflow orchestrates the whole thing from containers; Power BI sits on top.

**The key enabler**, confirmed against Snowflake's docs during planning: **Snowpark Connect for Spark** is GA. It runs PySpark DataFrame code on Snowflake warehouse compute over the Spark Connect protocol — no cluster, no JVM executors, no `spark.jars.packages`. It reads XML natively from internal stages with a `rowTag` option. That means `parser.py` survives essentially unchanged, and the entire dbt project is untouched, because the RAW tables keep their names and columns.

### Decisions taken

| Decision | Choice |
|---|---|
| Extraction | Snowflake sproc downloads/unzips/PUTs raw XML to an internal stage; Snowpark Connect reads it and runs the existing parser |
| Raw zone | Uncompressed XML archived on the stage, replayable without re-downloading from FDA |
| Spark | Kept as code, retired as local infrastructure; `docker/cluster/` deleted |
| Scope | Full 5-year / 20-quarter backfill, with cost guardrails and a dev slice |
| Marts | Star schema + PRR/ROR/chi-square disproportionality mart |

### Target architecture

```
FDA fis.fda.gov
      │  (external access integration — the only egress)
      ▼
EXTRACTION.INGEST_FAERS_QUARTER(year, quarter)      Python sproc
      │  download → unzip → PUT
      ▼
@FAERS_RAW/xml/year=YYYY/quarter=Q/*.xml            internal stage = raw zone
      │
      ▼
Snowpark Connect job (snowpark-submit / Airflow)    Snowflake warehouse compute
   spark.read.format("xml").option("rowTag","safetyreport")
   → parser.extract_reports / demographics / drug / reaction
      │
      ▼
RAW.REPORTS / DEMOGRAPHICS / DRUG / REACTION
      │
      ▼
dbt: staging → intermediate → mart                  Power BI reads MART
```

---

## Phase 0 — Foundations

Small, unglamorous, and everything later depends on it.

**Python runtime.** Rebuild `.venv` on **Python 3.12**. `snowpark-connect` requires `>=3.10,<3.13`; dbt-core 1.12 is fine on 3.12. One interpreter for everything, and it removes the broken pyspark/3.14 situation.

**Dependencies.** Split into `requirements.txt` (runtime: `snowpark-connect`, `snowflake-snowpark-python`, `snowflake-connector-python`, `requests`, `python-dotenv`) and `requirements-dbt.txt` (`dbt-core==1.12.*`, `dbt-snowflake==1.12.*` — currently pinned **nowhere**, despite a full dbt project). Drop `pyspark`, `beautifulsoup4` (the scrape is being deleted), `dbt-postgres`, and the `jars/` directory.

**Packaging.** Add `pyproject.toml` and `__init__.py` files so the entry point runs as `python -m faers_ingestion` from the repo root. Today the flat `from config import ...` imports only resolve if you `cd src/ingestion` first.

**Secrets and config.**
- `.env` defines six `SNOWFLAKE_*` variables **twice** (two blocks; the second silently wins), plus dead `POSTGRES_*` and an unused `SNOWFLAKE_PASSWORD`. Collapse to one block, add a committed `.env.example`.
- `src/ingestion/config.py`: `build_sf_options()` never validates presence, so a missing var yields `sfURL = "None.snowflakecomputing.com"`. Raise instead.
- **Delete `load_snowflake_private_key()` entirely.** Its regex only matches unencrypted `PRIVATE KEY` headers and leaves the footer attached if the file has no trailing newline. The Snowflake Python connector accepts `private_key_file` directly, so the hand-rolled PEM stripping disappears with the Spark connector.

**SQL bootstrap** (`sql/`):
- `02_create_schemas.sql` is missing `USE DATABASE FAERS_DB;` — schemas currently land in whatever database is current. Fix.
- `03_create_dwh.sql`: add `IF NOT EXISTS` (re-running fails today), set `AUTO_SUSPEND = 60`.
- New `04_create_resource_monitor.sql` — credit quota with `TRIGGERS ON 90 PERCENT DO SUSPEND`. Worth doing before XML parsing starts consuming warehouse compute.

---

## Phase 1 — Snowflake-native ingestion

**New SQL** (`sql/05_create_stage_and_access.sql`):

```sql
create stage if not exists extraction.faers_raw
  directory = (enable = true);

create or replace network rule fda_faers_rule
  mode = egress type = host_port value_list = ('fis.fda.gov:443');

create or replace external access integration fda_faers_access
  allowed_network_rules = (fda_faers_rule) enabled = true;

create table if not exists extraction.ingestion_log (
  quarter_key string, source_url string, status string,
  files_staged int, bytes_staged number,
  started_at timestamp_ltz, ended_at timestamp_ltz, error_message string
);
```

**Handler** — `src/ingestion/snowflake/ingest_faers_quarter.py`:

- Build the URL from the known pattern `https://fis.fda.gov/content/Exports/faers_xml_{year}q{quarter}.zip` — confirmed live during planning. **No scraping.** This deletes `get_faers_xml_urls()` and its dead BeautifulSoup dependency. Handle 404 for quarters FDA hasn't published yet.
- Stream the download (`requests.get(stream=True)`, chunked) to the sproc's `/tmp`. The current `_download` loads ~100 MB into memory via `response.content`.
- Open the ZIP with `zipfile` and iterate **named members only** — never `extractall`, which today has no zip-slip protection. For each `xml/*.xml` member: extract, `session.file.put()` to `@FAERS_RAW/xml/year=YYYY/quarter=Q/`, delete the local copy. Bounded disk regardless of quarter size.
- Also stage the quarter's `deleted/*.txt` deleted-cases file to `@FAERS_RAW/deleted/...` — FAERS publishes retracted case IDs, and you'll want them in Phase 3.
- Keep XML **uncompressed**: Snowpark Connect does not support compression for XML reads. ~30 GB for five years, under a dollar a month of Snowflake storage.
- Write a row to `INGESTION_LOG`; skip quarters already `succeeded` unless `force => true`.

**Deployment** — `sql/10_create_ingest_procedure.sql` PUTs the handler to a `@CODE` stage and does `CREATE PROCEDURE ... IMPORTS = (...) EXTERNAL_ACCESS_INTEGRATIONS = (fda_faers_access) HANDLER = 'ingest_faers_quarter.run'`. Keeps the Python reviewable in git rather than buried in a `$$ ... $$` block.

`src/ingestion/extract/downloader.py` is replaced by this handler.

---

## Phase 2 — Snowpark Connect extract/load

**Smoke test first.** Before porting everything, stage `1_ADR20Q1.xml` (722 MB, already on disk) and push one file end-to-end: `rowTag` read → `extract_drug()` → `saveAsTable`. Record runtime and credits. This validates the translation layer against the two operations most likely to misbehave — `posexplode_outer` and the window function — and gives you a cost baseline for the backfill.

**`extract/spark.py`** — replace the `local[*]` + Maven-jars builder with a Snowpark Connect session (`from snowflake import snowpark_connect`; confirm the exact client bootstrap call against Snowflake's environment-setup docs, it's a one-liner). Connection comes from `connections.toml` or explicit parameters including `private_key_file`. No `spark.jars.packages`, no `jars/`.

**`extract/scanner.py`** — `extract_data()` now loads from the stage:

```python
spark.read.format("xml") \
    .option("rowTag", "safetyreport") \
    .schema(SAFETYREPORT_SCHEMA) \
    .load(f"@FAERS_RAW/xml/year={year}/quarter={quarter}/")
```

Add an **explicit schema** in a new `extract/safetyreport_schema.py`. Today there is none, so every run pays for full inference, and — more dangerously — inference is content-dependent: when every record in a file has at most one `<drugrecurrence>`, Spark infers a struct rather than an array and `posexplode_outer` raises. An explicit schema makes that deterministic.

**`load/parser.py`** — logic preserved, one real bug fixed. `extract_drug()` currently explodes `patient.drug` with `explode_outer` (no index), then partitions its recurrence window by *all ~17 remaining columns*:

```python
drug_key = [c for c in df_recurrence_exploded.columns
            if c not in ("recurrence_index", "drugrecuraction")]
```

Two distinct drug entries with identical field values collapse into one row, and it forces a full shuffle on 17 columns — the dominant cost of the job. Switch the first explode to `posexplode_outer`, keep `drug_index`, and partition by `(safetyreportid, safetyreportversion, drug_index)`. Correct, far cheaper, and it unblocks the `int_drugs` grain problem in Phase 3.

**`load/loader.py`** — `df.write.mode(...).saveAsTable(f"RAW.{table}")` replaces the `net.snowflake.spark.snowflake` writer. Add `source_quarter` and `load_ts` columns, and switch from blanket overwrite to **delete-then-append per quarter**, driven by `INGESTION_LOG`. This is what makes a 20-quarter backfill affordable: reloading one quarter no longer rebuilds all four tables.

**`main.py`** — argparse (`--quarters`, `--force`), logging, row counts, explicit session teardown.

**Delete:** `docker/cluster/`, `jars/`, the `pyspark` pin.

---

## Phase 3 — dbt hardening

Do this before marts. A star schema built on an unasserted grain will fan out silently.

**Packages.** Create `packages.yml` with `dbt_utils` (first use in this project) and run `dbt deps`.

**Grain tests — currently absent entirely.** There is not one `unique` test anywhere. Add `unique` on `int_reports.report_id` and `int_demographics.report_id`, `dbt_utils.unique_combination_of_columns` on `int_drugs` and `int_reactions`, and `relationships` tests from all three child models back to `int_reports`.

**Correctness fixes in the intermediate layer:**
- `int_reports`: `receipt_date` and `transmission_date` use bare `max_by(field, version)` while the other 16 fields use the null-guarded `max_by(field, iff(field is not null, version, null))`. The dates can go null when the latest version has a null date even though an earlier one didn't. Make them consistent.
- `int_demographics`: `partition by (report_id, version) order by version desc` orders by the partition key — a no-op tiebreaker, so the surviving row is arbitrary. Order by something meaningful, as `int_reports` does with `transmission_date`.
- `int_drugs`: the partition key is six drug attributes and **excludes `recurrence_action`**, so fanned-out recurrence rows collapse under a non-deterministic tiebreak. Once Phase 2 emits `drug_index`, partition by `(report_id, drug_index)` instead.
- `int_reactions`: `meddra_version` is in the partition key, so the same reaction re-coded under a new MedDRA version stays as **two rows on one report**. Any distinct-reaction count over-counts. Collapse to the latest `meddra_version` per `(report_id, reaction)`.

**Decode the seriousness flags.** `congenital_anomaly`, `death`, `disabling`, `hospitalization`, `lifethreatening`, `other_serious` pass through as raw FAERS ints (**1/2, not 1/0**). Summing them as booleans gives wrong answers. Decode in `stg_reports` alongside `serious`, which is already handled.

**Duplicate policy.** `duplicate_flag` / `duplicate_numb` / `duplicate_source` are carried but never used to filter, so FAERS duplicate reports are counted in full — which directly distorts "which drugs have the largest number of serious reactions." Decide the policy, implement it, and write the reasoning into `models/README.md`. Fold in the staged deleted-cases file from Phase 1.

**Surface two unused seed columns** the marts need: `custom_reaction_groups_mapping.mapping_basis` (16,063 `rule` vs 6,719 `fallback` — consumers currently cannot tell a confidently-grouped reaction from a fallback) and `country_mapping.country_name`.

**Also:** add a `mart` block to `dbt_project.yml`; add a `prod` target to `~/.dbt/profiles.yml` (only `dev` exists, so there's no promotion path); add `vars: faers_quarters` for the dev slice; add source `freshness` using the new `load_ts`; convert `models/intermediate/schema.yml` from the legacy `tests:` key to `data_tests:` to match staging.

---

## Phase 4 — Marts

`models/marts/`, schema `mart`, materialized as tables. The custom `generate_schema_name` macro already maps `+schema: mart` to a bare `MART` schema, which `sql/02_create_schemas.sql` creates.

**Dimensions**
- `dim_date` — `dbt_utils.date_spine` over the `receipt_date` range; year / quarter / month.
- `dim_drug` — surrogate key over `active_substance` + `medicinal_product`.
- `dim_reaction` — MedDRA PT, `custom_reaction_group`, `custom_group_label`, `mapping_basis`.
- `dim_country` — ISO code plus the previously unused `country_name`.

**Facts**
- `fct_report` — one row per `report_id`; `int_reports` ⋈ `int_demographics` (both 1:1, safe); decoded seriousness booleans, expedited flag, date keys.
- `fct_report_drug` — report × drug.
- `fct_report_reaction` — report × reaction.
- `brg_drug_reaction` — suspect drugs only (`drug_characterization = 'suspect'`, the standard PV attribution filter) × reactions. This is a **deliberate** N×M cross product per report; document it, because joining `int_drugs` and `int_reactions` accidentally produces the same thing.

**Signal detection** — `mart_drug_reaction_signal`: the 2×2 contingency table (a/b/c/d) per drug-reaction pair, with PRR, ROR, chi-square and 95% confidence intervals, gated on a minimum case count (conventionally a ≥ 3). This is textbook disproportionality analysis and it's pure SQL over `brg_drug_reaction` — an ideal candidate for **dbt unit tests** with a hand-computed fixture.

**BI aggregates**, one per documented business question: `agg_drug_quarterly_trend`, `agg_reaction_frequency`, `agg_outcome_trend`.

**Two caveats to encode in the models and their descriptions:**
- The `faers_to_date` macro pads partial dates (`YYYYMM` → the 1st, `YYYY` → Jan 1). **Year-grain trends are sound; month- and day-grain seasonality is not.** Aggregate at year or quarter.
- `onset_age` is meaningless without `age_unit` (Decade/Year/Month/Day/Hour). Normalize to years in `fct_report` rather than leaving it to Power BI.

---

## Phase 5 — Airflow

`docker/airflow/` — Docker Compose (Airflow 3.x) with a Dockerfile layering in `dbt-snowflake` and `snowpark-connect`, mounting `dbt/faers_transformations/` and `src/`. Use **astronomer-cosmos** to render each dbt model as its own Airflow task, so the DAG graph shows the real lineage and failures retry per-model.

DAG `faers_quarterly`, scheduled quarterly (FAERS publishes quarterly), `catchup=False`:

```
discover_quarters          → reads INGESTION_LOG, emits pending quarters
  └─ ingest_quarter        → dynamic task mapping; CALL INGEST_FAERS_QUARTER(y, q)
       └─ extract_load     → Snowpark Connect job per quarter
            └─ DbtTaskGroup(seed → run → test)
                 └─ refresh_powerbi   (optional, Phase 6)
```

Credentials move to an Airflow Snowflake connection with key-pair auth — **not** a `.env` baked into the image.

---

## Phase 6 — Power BI

Connect to `FAERS_DB.MART` with the native Snowflake connector, **Import** mode (the aggregates are small). You're on WSL2, so Power BI Desktop runs on the Windows host against the same warehouse.

**Check auth early:** Power BI Desktop's Snowflake connector has historically not supported key-pair auth, which is the only method this project uses. You may need a dedicated Power BI user with password or SSO, plus a read-only role scoped to `MART`. Worth confirming before building visuals.

Mark `dim_date` as the date table, wire relationships fact → dim, write measures in DAX (report counts, serious share, expedited share, PRR). Pages map to the three business questions plus a signal-detection page.

---

## Phase 7 — Quality and presentation

- **CI** (GitHub Actions): `sqlfluff lint` (`.sqlfluff` already configured but wired to nothing), `ruff` on Python, `dbt build --target ci` against a CI schema on the dev slice.
- **dbt unit tests** for the PRR/ROR math and the version-merge logic.
- **`dbt docs generate`** — with `unique`/`relationships` tests in place the lineage graph is worth showing.
- **Rewrite `dbt/faers_transformations/README.md`** (still `dbt init` boilerplate) and the root `README.md` with the architecture diagram above.
- Add **exposures** for the Power BI dashboard so lineage runs source → report.
- `pre-commit` running sqlfluff + ruff.

---

## Verification

**Phase 1** — `CALL EXTRACTION.INGEST_FAERS_QUARTER(2020, 1);` then `LIST @FAERS_RAW/xml/year=2020/quarter=1/` shows 3 files at the expected sizes; `INGESTION_LOG` has one `succeeded` row; a second call is a no-op.

**Phase 2** — smoke test on the single 722 MB file; then one full quarter and compare `RAW.REPORTS` / `DRUG` / `REACTION` / `DEMOGRAPHICS` row counts against a local Spark run on the same quarter (your `data/raw/XML/` copy makes this a real A/B, worth doing once before deleting the Spark path). Confirm `extract_drug()` no longer collapses distinct drugs by checking a report with repeated identical drug entries.

**Phase 3** — `dbt build` clean, with the new `unique` and `relationships` tests passing. Spot-check a `report_id` known to have multiple versions and confirm the merged row takes the latest non-null value per field.

**Phase 4** — `count(*)` on `fct_report` equals `count(distinct report_id)` in `int_reports`; `fct_report_drug` row count equals `int_drugs`; hand-compute PRR and ROR for one well-known drug-reaction pair and check it against the model.

**Phase 5** — trigger the DAG on the dev slice with a cleared `INGESTION_LOG`, confirm one green end-to-end run, then clear a single quarter and confirm only that quarter reprocesses.

**Phase 6** — refresh the Power BI model and reconcile a headline number (total serious reports in the last 5 years) against a direct Snowflake query.

---

## Open risks

1. **Snowpark Connect translation gaps.** Your operations are plain DataFrame ops, so risk is low — but `posexplode` is UDF-backed and slow there, and window frames need explicit `ORDER BY` for bounded frames. The Phase 2 smoke test is the mitigation; the NDJSON-in-a-sproc design remains the fallback if it fails.
2. **Backfill cost.** 20 quarters × ~2.2 GB of XML on warehouse compute. The smoke test gives you a per-quarter credit figure — extrapolate before launching the backfill, and run it once.
3. **Drug name normalization is the biggest unsolved modeling problem.** `medicinal_product` and `active_substance` are uppercased and otherwise untouched: no brand→generic resolution, no spelling-variant dedupe. "Which drugs show increasing reporting trends" is only as good as this. A future `dim_drug` enrichment against RxNorm or openFDA is the natural extension — worth naming in the README as known scope rather than silently leaving it.
4. **Staging filters already constrain denominators.** `stg_reports` drops rows where `serious is null`; `stg_drugs` drops rows missing product, substance or characterization. Any "share of reports meeting expedited criteria" metric is computed over the filtered population. Document it on the mart columns.
