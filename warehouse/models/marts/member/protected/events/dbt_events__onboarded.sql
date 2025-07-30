WITH users AS (

  SELECT * FROM  {{ ref('int_app__users') }}

),

care_profiles AS (

  SELECT * FROM {{ ref('stg_app__care_profiles') }}

),

join_onboarding_types AS (

  SELECT
    u.user_id AS member_id,
    COALESCE(u.completed_account_creation_at, u.completed_member_onboarding_at) AS completed_converged_onboarding_at,
    -- As per https://betterup.atlassian.net/browse/BUAPP-52869 - ensure that PAD values are not misrepresented while adjustments and backfill to care_profiles table is completed 
    -- Ticket to revert the changes and query the stg_app__care_profiles model is in que: https://betterup.atlassian.net/browse/BUAPP-53142  
    -- cp.completed_care_onboarding_at,
    case 
      when u.care_profile_id is not null 
        then COALESCE(u.completed_account_creation_at, u.completed_member_onboarding_at)
    end as completed_care_onboarding_at,
    u.completed_member_onboarding_at
  FROM users AS u
  -- As per https://betterup.atlassian.net/browse/BUAPP-52869 comments above 
  -- LEFT JOIN care_profiles AS cp ON u.care_profile_id = cp.care_profile_id

),

{%- set onboarding_types = [
  'converged',
  'care',
  'member'
  ]
  -%}


{%- for onboarding_type in onboarding_types -%}

{{onboarding_type}}_onboarding AS (

  SELECT
    member_id,
    completed_{{ onboarding_type }}_onboarding_at AS event_at,
    '{{ onboarding_type }}_onboarding' AS event_object
  FROM join_onboarding_types

),

{%- endfor -%}

unioned AS (

{%- for onboarding_type in onboarding_types -%}

  SELECT * FROM {{ onboarding_type }}_onboarding
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}

)


SELECT
  -- Surrogate Key of MEMBER_ID, EVENT_ACTION_AND_OBJECT, EVENT_AT
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at']) }} AS dbt_events__onboarded_id,
  member_id,
  event_at,
  'completed' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  NULL AS associated_record_type,
  NULL AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM unioned
WHERE event_at IS NOT NULL
