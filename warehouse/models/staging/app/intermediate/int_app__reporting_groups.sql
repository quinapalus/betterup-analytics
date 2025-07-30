WITH reporting_groups AS (
  SELECT * FROM {{ ref('stg_app__reporting_groups') }}
),

destroyed_records AS (
  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ReportingGroup'
)

SELECT
  reporting_group_id,
  created_at,
  updated_at,
  name,
  product_type,
  associated_record_type
FROM reporting_groups AS rg
LEFT JOIN destroyed_records AS v ON rg.reporting_group_id = v.item_id
WHERE v.item_id IS NULL