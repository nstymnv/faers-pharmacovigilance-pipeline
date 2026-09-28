-- Reaction terms with no row in the custom_reaction_groups_mapping seed, most
-- reported first so the terms that move the grouped charts most get reviewed
-- most carefully.
--
-- The seed was classified by hand (AI-assisted) from 2020q1's terms, with no
-- reproducible rules, so it is extended rather than regenerated: classify the
-- terms listed here the same way and append them to the seed. Rows already in
-- the seed are never touched, which keeps 2020q1 results and the figures in
-- models/README.md stable. Then `dbt seed && dbt build`.
--
-- Compile with `dbt compile --select unmapped_reaction_terms` and run the SQL
-- in target/compiled/.
select
    reaction as reaction_normalized,
    count(distinct report_id) as report_count
from {{ ref('stg_reaction') }}
where custom_reaction_group is null
group by reaction
order by report_count desc, reaction
