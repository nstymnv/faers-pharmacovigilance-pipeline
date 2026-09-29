# faers_transformations

The dbt project that turns the raw FAERS tables into the star schema the
[`queries/`](../../queries/) folder reads. For the why behind each modelling
decision, with the measurements that drove it, read
[`models/README.md`](models/README.md) first.

## Layers

| Layer | Schema | Models | Job |
|---|---|---|---|
| Sources | `RAW` | `reports`, `demographics`, `drug`, `reaction`, `deleted_cases` | Loaded per quarter by `faers_ingestion.main`; every row carries `source_quarter` |
| Staging | `STAGING` | `stg_*` | Rename raw FAERS fields, cast, decode codes via seeds, repair format drift (e.g. trailing periods on 2021q1–q3 product names) |
| Intermediate | `INTERMEDIATE` | `int_*` | One row per report: versions merged (latest non-null value per field), FDA retractions removed, conflicting values resolved; `int_linked_reports` lists reports sharing a case id |
| Marts | `MART` | `fct_*`, `dim_*`, `brg_drug_reaction`, `mart_drug_reaction_signal`, `agg_*` | Star schema, PRR/ROR signal screen, quarterly aggregates; limited to the analysis window |

Seeds (country, administration route, time and dosage units, reaction groups)
land in `STAGING`. Schema names are used verbatim
([`macros/generate_schema_name.sql`](macros/generate_schema_name.sql)), except
on the `ci` target, which prefixes `CI_`.

## Vars

| Var | Default | Effect |
|---|---|---|
| `faers_quarters` | `[]` (all) | Restricts staging to the listed quarters, e.g. `[2020q1]`, for a cheap dev or CI run. While set, the mart window filter is lifted, since the dev slice sits outside the window |
| `mart_start_quarter`, `mart_end_quarter` | `2021q1`, `2025q4` | The analysis window: marts keep reports published within it. Quarters outside stay in RAW and the lower layers, so moving the window is a var change, not a reload |
| `linked_case_max_reports` | `5` | Largest group of reports sharing a case id that `fct_report` collapses to one; larger groups are mostly literature case series |
| `implicated_drug_roles` | `['suspect', 'interacting']` | Drug roles treated as causally implicated, which gates the bridge and the signal mart |
| `signal_min_cases`, `signal_min_prr`, `signal_min_chi2` | `3`, `2`, `4` | Evans criteria behind `is_signal` |

## Running

The dev profile lives in `~/.dbt/profiles.yml` (profile `faers_transformations`,
target `dev`). From this directory:

```bash
dbt deps && dbt seed
dbt build                                       # everything, with tests
dbt build --select stg_drugs+                   # one model and everything downstream
dbt build --vars '{faers_quarters: [2020q1]}'   # dev slice: one quarter
dbt test --select test_type:unit                # unit tests only
dbt docs generate && dbt docs serve             # browse lineage and descriptions locally
```

Mart descriptions are also pushed into Snowflake comments (`+persist_docs`), so
they show up when browsing `MART` directly.

## Tests

- **Schema tests** in each layer's `schema.yml`: keys, relationships, accepted
  values and ranges. Tests that track known data-quality noise (implausible
  drug dates, reaction terms missing from the grouping seed) are warnings, not
  errors; `analyses/unmapped_reaction_terms.sql` lists the unmapped terms.
- **Singular tests** in [`tests/`](tests/) cross-check models against each
  other, e.g. that the four contingency cells of every signal pair sum to the
  universe.
- **Unit tests** on inline data: the PRR/ROR math, dose-entry collapse,
  onset-age normalization and linked-duplicate collapse
  (`models/marts/unit_tests.yml`), and the report version merge and
  linked-case grouping (`models/intermediate/unit_tests.yml`).

## CI

`.github/workflows/ci.yml` runs `dbt build` on the 2020q1 slice for pull
requests into `main` (and on demand), as the `FAERS_CI` service user
(`sql/06_create_ci_identity.sql`) with the profile in [`ci/profiles.yml`](ci/profiles.yml).
The `ci` target builds into `CI_STAGING`, `CI_INTERMEDIATE` and `CI_MART`, so it
never touches the tables above. sqlfluff then lints the project against those
built tables (`--config ci/.sqlfluff`).

To lint locally against the dev target: `sqlfluff lint models tests analyses`.

## Exposure

`models/exposures.yml` declares the [`queries/`](../../queries/) folder as a
downstream analysis, so lineage runs from the sources to the business
questions. The queries name tables directly rather than through `ref()`, so its
`depends_on` list is maintained by hand.
