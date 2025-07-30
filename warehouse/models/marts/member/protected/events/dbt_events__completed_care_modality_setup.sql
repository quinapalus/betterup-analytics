WITH care_modality_setup_completed AS (

  SELECT * FROM {{ ref('stg_segment_backend__care_modality_setup_completed') }}

),

renamed as (
SELECT DISTINCT
  user_id AS member_id,
  timestamp AS event_at,
  'completed' AS event_action,
  'care_modality_setup' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'User' AS associated_record_type,
  user_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM care_modality_setup_completed
),

final as (
  select
    {{dbt_utils.surrogate_key(['member_id', 'event_at', 'event_action_and_object', 'associated_record_id'])}} as _unique,
    *
  from renamed
)

select * from final