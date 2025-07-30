with user_attribute_fields as (
  select * from {{ source('app', 'user_attribute_fields') }}
),

renamed as (
select
-- primary key
  id AS user_attribute_field_id,

-- foreign keys
  organization_id,
  user_attribute_internal_field_id,

--logical data
  field_name,
  display_name,

  --booleans
  filterable ,
  exportable,
  archived,

  --timestamps
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
from user_attribute_fields
)

select * from renamed