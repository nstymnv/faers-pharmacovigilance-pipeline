{{ config(
	materialized='table',
	schema='staging'
) }}

with source as (
	select * from {{ source('faers_db', 'drug') }}
),

normalized as (
	select
	safetyreportid as report_id,
	safetyreportversion as version,
	UPPER(TRIM(medicinalproduct)) as medicinal_product,
	UPPER(TRIM(activesubstancename)) as active_substance,
	CAST(drugcharacterization as int) as drug_characterization,
	drugindication as drug_indication,
	drugbatchnumb as drug_batch_number,
	drugauthorizationnumb as drug_authorization_number,
	CAST(drugadministrationroute as string) as administration_route,
	drugstructuredosagenumb as dosage_amount,
	CAST(drugstructuredosageunit as string) as dosage_unit,
	try_to_date(to_varchar(drugstartdate), 'YYYYMMDD') as start_date,
	try_to_date(to_varchar(drugenddate), 'YYYYMMDD') as end_date,
	try_cast(drugtreatmentduration as float) as duration,
	cast(drugtreatmentdurationunit as int) as duration_unit,
	cast(drugrecurreadministration as int) as drug_recurrence,
	-- last valid (non-null) recurrence-action code for this drug; parser.py
	-- collapses the drugrecurrence explosion back to one row per drug
	drugrecuraction as recurrence_action,
	cast(actiondrug as int) as taken_action,
	cast(drugadditional as int) as use_stopped_reduced

from source
where safetyreportid is not null
	  and medicinalproduct is not null
	  and activesubstancename is not null
	  and drugcharacterization is not null

),

standardized as (
	select
	report_id,
	version,
	medicinal_product,
	active_substance,
	case
		when drug_characterization = 1 then 'suspect'
		when drug_characterization = 2 then 'concomitant'
		when drug_characterization = 3 then 'interacting'
		end as drug_characterization,
	drug_indication,
	drug_batch_number,
	drug_authorization_number,
	administration_route,
	dosage_amount,
	case
		when dosage_unit = '001' then 'kg'
		when dosage_unit = '002' then 'G'
		when dosage_unit = '003' then 'Mg'
		when dosage_unit = '004' then 'µg'
		end as dosage_unit,
	start_date,
	end_date,
	duration,
	duration_unit,
	case
		when drug_recurrence = 1 then 'yes'
		when drug_recurrence = 2 then 'no'
		when drug_recurrence = 3 then 'unknown'
		end as drug_recurrence,
	recurrence_action,
	taken_action,
	case
		when use_stopped_reduced = 1 then 'yes'
		when use_stopped_reduced = 2 then 'no'
		when use_stopped_reduced = 3 then 'doesn''t Apply'
		end as use_stopped_reduced
	from normalized	
),

final as (
	select
	s.* EXCLUDE (administration_route, duration_unit),
	a.administration_route,
	t.unit_name as duration_unit
	from standardized s
	left join {{ ref('administration_route_mapping') }} a
	on s.administration_route = a.administration_route_code
	left join {{ ref('time_unit_code_mapping')}} t
	on s.duration_unit = t.unit_code
)
select * from final
