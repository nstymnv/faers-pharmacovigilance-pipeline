# Models

## Code decoding: seed tables vs. inline `CASE`

The staging models decode raw FAERS numeric codes into readable values two
different ways, and the split is intentional:

- **Seed tables** (`administration_route_mapping`, `time_unit_code_mapping`,
  `country_mapping`, `custom_reaction_groups_mapping`, `dosage_unit_mapping`)
  are used for FAERS code
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

## Duplicate and retraction policy

FAERS contains four different things that all get loosely called "duplicates".
They are handled differently, and the differences matter for every count the
marts produce. Figures below are measured on 2020q1 (460,327 reports).

### 1. The `duplicate` columns are NOT a duplicate marker — never filter on them

`duplicate_flag` / `duplicate_numb` / `duplicate_source` look like they identify
redundant reports. They do not:

| Measurement | Result |
|---|---|
| `duplicate_flag = 1` | 433,215 of 460,327 reports (94%) |
| `duplicate_numb = company_number` | 433,215 of 433,215 — 100% |
| Distinct `report_id` per `duplicate_numb` value | 1, for every value; no collisions |

This is the E2B `<reportduplicate>` block: the sender restating *its own* case
number so the case can be recognised if it reaches FDA through another channel.
It is a linkage identifier, not a redundancy flag. Filtering `duplicate_flag = 1`
would delete 94% of the data and remove no actual duplicates, so nothing in this
project filters on it. The columns are carried for traceability only.

**Caveat, to be revisited after the backfill:** this is measured on one quarter.
A sender's case number recurring under a *different* `report_id` in a later
quarter is exactly what would make this column useful for case linkage, and one
quarter cannot show that. Re-run the two measurements above across the full
5-year load before concluding anything further.

### 2. Report versions — consolidated, not filtered

The same `report_id` is republished as a new `version` when a reporter amends a
case; versions run from 1 to 92 in 2020q1. Within a single quarter each
`report_id` appears exactly once, so version collisions only occur *across*
quarters.

- `int_reports` / `int_demographics` merge fields with
  `max_by(field, iff(field is not null, version, null))`, taking the latest
  version that actually supplied a value rather than letting the newest row win
  wholesale and null out fields it happens to omit.
- `int_drugs` / `int_reactions` instead take each report's latest version list
  *whole*. `drug_index` is assigned per version, so index 3 in version 1 need not
  be the same product as index 3 in version 2; resolving indexes across versions
  would silently mix drugs. A later version supersedes the entire list.

### 3. FDA retractions — excluded outright

FDA publishes a list of withdrawn case numbers with each quarter. These are
removed in the intermediate layer by every model, so no downstream denominator
includes them and no mart can forget the filter.

Retraction is not retrospective-only: 153 of 2020q1's 4,489 withdrawn ids name
reports published *in* 2020q1. `stg_deleted_cases` therefore unions every
quarter's list and is deliberately exempt from the `faers_quarters` dev-slice
var — narrowing it would let a dev build keep reports FDA has withdrawn.

### 4. Repeated drug entries — kept here, collapsed in the marts

A report lists a drug **once per administered dose**, not once per product.
Report 12610564 carries 108 `AFSTYLA` entries, each with its own dose and start
date; report 15656224 carries ~170 `NEXIUM` entries. These are treatment
diaries, not data-entry errors.

`int_drugs` keeps every entry, keyed `(report_id, drug_index)`, so dose amounts,
dates and routes survive into the warehouse. **Marts must count distinct reports
per drug, never rows** — counting rows would let one haemophilia patient's dosing
history outrank a drug reported by a hundred separate patients.

The previous key was six drug attributes, which silently destroyed 155,043 of
1,899,889 rows (8.2%); 55,032 of the collapsed groups differed in dose, start
date or route, so they were distinct exposures rather than repeats.

### 5. True clinical duplicates — out of scope, and a real limitation

The same event reported independently by a manufacturer and a consumer arrives
as two unrelated `report_id`s, and FAERS does not resolve them. Detecting them
needs probabilistic matching on event date, age, sex, drug list and reaction
list. Nothing here attempts it, so **absolute report counts are overstated by an
unknown margin**. Disproportionality measures (PRR/ROR) are more robust to this
than raw counts, since duplicates inflate numerator and denominator together.

### Conflicting values within one report

Where a report gives the same reaction two different outcomes — 5,612 pairs in
2020q1, e.g. `fatal` alongside `recovered/resolved` — `int_reactions` keeps the
most clinically severe (`fatal` > `recovered/resolved with sequelae` >
`not recovered/not resolved` > `recovering/resolving` > `recovered/resolved` >
`unknown`). Standard pharmacovigilance practice: a fatal outcome is never masked
by a co-reported recovery. See the `faers_outcome_severity` macro.

## Date quality

FAERS senders report dates at whatever precision they had, so `faers_to_date`
parses three: 8-digit `YYYYMMDD`, 6-digit `YYYYMM`, and 4-digit `YYYY`, padding a
missing month/day to the 1st. Parsing only the full form would discard 204,129 of
699,025 non-null drug start dates (29%).

Two consequences to carry into the marts:

1. **Year and quarter grain are sound; month and day grain are not.** 81,428 drug
   start dates are year-only and all land on 1 January. Any month-level
   seasonality in a drug-date series is an artefact of the padding.
2. **A parsed date is not necessarily a plausible date.** The macro gates on an
   exact length of 8, 6 or 4 so a too-short value becomes null rather than a
   wrong date (`try_to_date('150', 'YYYY')` would otherwise return year 0150),
   but a 4-digit typo still parses cleanly. 2020q1 carries 18 drug start dates
   and 3 end dates outside 1900–today — mostly a mistyped leading digit
   (`1019-02-01` for 2019, `3019-07-25` for 2019). `dbt_utils.accepted_range`
   tests track these at warning severity; they are faults in the source, not in
   the model, so they are surfaced rather than silently repaired.

Report dates are clean by comparison: all 460,327 `receiptdate` and
`transmissiondate` values in 2020q1 are full 8-digit dates within range, which is
why `dim_date` is built from `receipt_date` rather than the drug dates.

## Coded-column decoding: watch the raw type

Every coded FAERS column arrives as a Snowflake `NUMBER`, because the loader
infers types from the XML. That makes zero-padded comparisons a trap:
`cast(1 as string)` is `'1'`, never `'001'`. `dosage_unit` was decoded that way
and silently produced **null for all 850,576 dosed rows** — the model ran, the
tests passed, and the column was simply empty. Compare on the numeric code, or
join a seed keyed by number.

An audit comparing raw non-null counts against decoded non-null counts, run
within the rows that survive the staging filter, is the cheap way to catch this
class of bug. Everything is at 0% undecoded except `dosage_unit`.

### `dosage_unit` is only partly decodable

The bundled `XML_NTS.pdf` documents **only four** codes — `001 kg`, `002 g`,
`003 mg`, `004 µg` — and refers the rest to the ICH E2B(R2) specification, which
FDA does not ship with the FAERS download. 2020q1 carries 36 distinct codes, so
**~25% of dosed rows (208,721) have a unit this project cannot yet resolve**.
`dosage_unit_code` is kept alongside the decoded name so those doses stay
visibly unresolved rather than looking unitless. Completing the seed requires
the ICH code list.

Note the symbols are the correct SI ones (`g`, `mg`), not the spec table's `G`
and `Mg`. In SI, `Mg` is a megagram and `mg` a milligram — a 10^9 difference on
a dose, which is not a distinction to reproduce faithfully in a drug-safety
warehouse.

### Absent vs. reported-as-unknown

Two columns previously merged "no value reported" into a real coded category:

- `sex` had an `else 'unknown'` branch that put 47,681 unreported rows into the
  same bucket as the 92 genuinely coded `0 = unknown` — overstating that
  category roughly 500-fold for any "which patient groups" breakdown. Null now
  stays null.
- The seriousness flags run the other way: there, absence genuinely *does* mean
  "no" (see the flag macro), so they decode to `false`, not null.

The rule is per-column and comes from the FAERS spec, not from a convention that
can be applied blindly.

### `use_stopped_reduced` is the dechallenge outcome

Despite the column name, FAERS `drugadditional` records whether the event
**abated after** the drug was stopped or reduced — not whether the drug was
stopped. A positive dechallenge is a causality signal, so an attribution
analysis that reads it as "drug was withdrawn" is measuring the wrong thing. The
name is kept for interface stability; the descriptions state what it means.
