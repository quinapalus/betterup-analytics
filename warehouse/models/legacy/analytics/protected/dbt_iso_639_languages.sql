WITH language_codes AS (

  SELECT * FROM {{ref('iso_639_language_codes')}}

)

SELECT
  alpha2,
  -- if multiple names, use the first one
  split_part(language_name, ';', 1) AS language
FROM language_codes
