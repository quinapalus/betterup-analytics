WITH user_attribute_internal_fields AS (

  SELECT * FROM {{ source('app', 'user_attribute_internal_fields') }}

),

renamed as (
SELECT
-- primary key
  id AS user_attribute_internal_field_id,

  --logical data
  field_name,
  description,

  --timestamps
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM user_attribute_internal_fields
)

select * from renamed
