-- The four contingency cells must partition the universe exactly. If they do
-- not, the b/c/d arithmetic has drifted from the counts it is derived from and
-- every PRR and ROR in the table is wrong in a way no range test would catch.
select
    drug_key,
    reaction_key,
    a,
    b,
    c,
    d,
    n_total,
    a + b + c + d as cell_sum
from {{ ref('mart_drug_reaction_signal') }}
where a + b + c + d <> n_total
