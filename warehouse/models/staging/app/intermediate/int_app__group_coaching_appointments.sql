WITH group_coaching_appointments AS (
  SELECT * FROM {{ ref('stg_app__group_coaching_appointments') }}
)


SELECT
    group_coaching_appointment_id,
    created_at,
    updated_at,
    attempted_to_join_at,
    group_coaching_session_id,
    member_id,
    assigned_post_session_resources_at
FROM group_coaching_appointments
    -- select the most recent appointment record for a group session for a member
    QUALIFY row_number() over (partition by group_coaching_session_id, member_id order by created_at desc) = 1