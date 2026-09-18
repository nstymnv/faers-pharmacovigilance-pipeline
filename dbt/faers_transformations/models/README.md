# Models

## Code decoding: seed tables vs. inline `CASE`

The staging models decode raw FAERS numeric codes into readable values two
different ways, and the split is intentional:

- **Seed tables** (`administration_route_mapping`, `time_unit_code_mapping`,
  `country_mapping`, `custom_reaction_groups_mapping`) are used for FAERS code
  lists that are long, externally-sourced, or expected to change independently
  of a code deploy (country names, administration routes, MedDRA-derived
  reaction groupings, time-unit codes).
- **Inline `CASE WHEN`** is used for small, stable enumerations defined
  directly in the FAERS data dictionary (`serious`, `sex`, `outcome`,
  `drug_characterization`, `dosage_unit`, `use_stopped_reduced`, `age_group`,
  `drug_recurrence`) that aren't expected to gain new codes without a
  corresponding FAERS spec change anyway.

Both are deliberate choices for the kind of code list involved, not an
inconsistency to be cleaned up.

## Identifier casing convention

Snowflake identifiers are case-insensitive unless quoted, so database/schema/
table casing hasn't mattered functionally so far, but write new SQL and YAML
lowercase (matching dbt's own generated SQL and most of the existing YAML)
rather than the all-caps style used in the one-time `sql/` bootstrap scripts.
Those bootstrap scripts are idempotent `CREATE ... IF NOT EXISTS` statements
and don't need to be rewritten just to match this convention.
