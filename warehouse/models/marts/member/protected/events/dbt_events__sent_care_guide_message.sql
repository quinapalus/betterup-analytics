WITH messages AS (

  SELECT * FROM  {{ ref('stg_app__messages') }}

),

conversation_participants AS (

  SELECT * FROM  {{ ref('stg_app__conversation_participants') }}

),

care_guide_conversations AS (

  SELECT * FROM  {{ ref('stg_app__conversations') }}
  WHERE type = 'Conversations::CareGuideConversation'

),

users_roles AS (

  SELECT * FROM  {{ ref('stg_app__users_roles') }}

),

roles AS (

  SELECT * FROM  {{ ref('stg_app__roles') }}

),

care_guides AS (

  SELECT user_id FROM users_roles as ur
  LEFT JOIN roles as r
    ON ur.role_id = r.role_id
  WHERE r.name = 'care_guide'

),

care_guide_member_messages AS (

  SELECT
    m.message_id,
    m.created_at,
    m.sender_id,
    m.conversation_participant_id,
    cp.conversation_id,
    cp.user_id,
    c.type
  FROM care_guide_conversations AS c
  INNER JOIN conversation_participants AS cp
    ON c.conversation_id = cp.conversation_id
  INNER JOIN messages AS m
    ON cp.conversation_participant_id = m.conversation_participant_id
  WHERE cp.USER_ID NOT IN (SELECT user_id FROM care_guides)

),

final AS (
    SELECT
      user_id AS member_id,
      created_at AS event_at,
      'sent' AS event_action,
      'care_guide_message' AS event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      'Message' AS associated_record_type,
      message_id AS associated_record_id,
      OBJECT_CONSTRUCT() AS attributes
    FROM care_guide_member_messages
)

SELECT
    final.*
FROM final