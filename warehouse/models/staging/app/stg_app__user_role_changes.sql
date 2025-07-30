WITH role_changes AS (

  SELECT * FROM {{ source('app', 'user_role_changes') }}

),

renamed as (
SELECT
--primary key
  id AS user_role_change_id,

--foreign keys
  user_id,
  role_id,

--logical data
  event_type,
  
  --timestamps
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM role_changes
)

select * from renamed