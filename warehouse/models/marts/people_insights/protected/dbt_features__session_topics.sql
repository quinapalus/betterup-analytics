WITH appointments AS (

  SELECT * FROM {{ ref('stg_app__appointments') }}

),

development_topics AS (
  SELECT * FROM {{ ref('stg_app__development_topics') }}
),

topic_themes AS (
    SELECT * FROM {{ ref('int_app__topic_themes') }}
),

final as (

  SELECT DISTINCT
    a.member_id,
    a.appointment_id AS associated_record_id,
    'Appointment' AS associated_record_type,
    a.starts_at AS feature_collected_at,
    'coach_classified_session_topic' AS feature_key,
    OBJECT_CONSTRUCT('translation_key', dt.translation_key) AS classification,
    OBJECT_CONSTRUCT('development_topic', dt.name, 'development_topic_key', dt.translation_key,
                    'development_theme', dtt.name, 'development_theme_key', dtt.translation_key
                    ) AS feature_attributes,
    'coaching_topic' AS feature_type
  FROM appointments AS a
  INNER JOIN development_topics AS dt
    ON a.development_topic_id = dt.development_topic_id
  INNER JOIN topic_themes AS dtt
    ON dt.topic_theme_id = dtt.topic_theme_id)

select 
{{ dbt_utils.surrogate_key(['member_id', 'associated_record_id','classification']) }} as session_topics_id,
final.*
from final
