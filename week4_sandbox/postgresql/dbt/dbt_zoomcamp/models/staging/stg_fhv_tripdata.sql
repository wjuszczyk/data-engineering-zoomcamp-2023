{ { config(materialized = 'view') } }
select cast() as,
	from { { source('staging', 'fhv_2019') } }
limit 100