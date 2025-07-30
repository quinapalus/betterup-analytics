{{
	config(
		materialized = 'table'
	)
}}

WITH persisted_construct_scores AS (

    SELECT * FROM {{ ref('dbt__persisted_construct_scores') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
  ps.*
FROM persisted_construct_scores AS ps
-- Could also join this on construct_id
LEFT JOIN whole_person_subdimensions AS d ON ps.key = d.subdimension_key
WHERE d.subdimension_key IS NULL