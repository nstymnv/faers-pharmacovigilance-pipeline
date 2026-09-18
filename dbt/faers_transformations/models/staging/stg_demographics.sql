{{ config(
	materialized='table',
	schema='staging'
) }}

with source as (
	select *
	from {{ source('faers_db', 'demographics') }}
),

normalized as (
	select
		safetyreportid as report_id,
		safetyreportversion as version,
		try_cast(patientagegroup as int) as age_group,
		try_cast(patientonsetage as int) as onset_age,
		try_cast(patientonsetageunit as int) as age_unit_code,
		try_cast(patientsex as int) as sex,
		try_cast(patientweight as float) as weight_kg
	from source
	where safetyreportid is not null
),

standardized as (
	select
		report_id,
		version,
		case
			when age_group = 1 then 'neonate'
			when age_group = 2 then 'infant'
			when age_group = 3 then 'child'
			when age_group = 4 then 'adolescent'
			when age_group = 5 then 'adult'
			when age_group = 6 then 'elderly'
			end as age_group,
		onset_age,
		age_unit_code,
		case
			when sex = 0 then 'unknown'
			when sex = 1 then 'male'
			when sex = 2 then 'female'
			else 'unknown'
			end as sex,
		weight_kg
	
	from normalized
),

final as (
	select 
	s.* EXCLUDE age_unit_code,
	t.unit_name as age_unit
	from standardized s
	left join {{ ref('time_unit_code_mapping') }} t
	on t.unit_code = s.age_unit_code
)

select * from final
