WITH construct_benchmark_scores AS (

  -- filter for benchmarks with non-NULL industry and role values only
  SELECT wpb.*, ckm.derived_key FROM {{ ref('bu_whole_person_benchmarks') }} AS wpb
  -- pull in arbitrary derived construct keys that correspond to benchmarks with different key names
  LEFT OUTER JOIN {{ ref('bu_construct_key_mapping') }} AS ckm
    ON wpb.construct_key = ckm.construct_key
  WHERE wpb.industry IS NOT NULL
    OR wpb.employee_level IS NOT NULL

)

SELECT * FROM construct_benchmark_scores
