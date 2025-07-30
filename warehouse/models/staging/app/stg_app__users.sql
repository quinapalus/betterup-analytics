{{
  config(
    tags=["eu"]
  )
}}

WITH users AS (

  select * from {{ source('app', 'users') }}

),

current_users as (

  select
         id AS user_id
        , uuid AS user_uuid
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
--        , coach_id --this column does not exist anymore in the postgres DB
        , coach_profile_id  -- still functional but only for US data (keeping for versions item_id join)
        , coach_profile_uuid
        , manager_id
        , member_profile_id
        , apple_id
        , bluejeans_id
        , inviter_id
        , consumer_id
        , next_appointment_id AS next_session_id
        , organization_id
        , care_profile_id
        , hubspot_contact_id
        , zoom_id
        , skype_id
        , first_name
        , last_name
--        , {{ load_timestamp('last_active_at') }} --this column does not exist anymore in the postgres DB
        , {{ load_timestamp('last_appointment_at') }}
        , {{ load_timestamp('last_sign_in_at') }}
        , {{ load_timestamp('last_engaged_at') }}
        , {{ load_timestamp('locked_at') }}
        , {{ load_timestamp('accepted_organization_terms_at') }}
        , {{ load_timestamp('accepted_terms_at') }}
        , {{ load_timestamp('completed_member_onboarding_at') }}
        , {{ load_timestamp('confirmation_sent_at') }}
        , {{ load_timestamp('confirmed_at') }}
        , {{ load_timestamp('current_sign_in_at') }}
        , {{ load_timestamp('deactivated_at') }}
        , {{ load_timestamp('care_confirmed_at') }}
        , {{ load_timestamp('lead_confirmed_at') }}
        , {{ load_timestamp('scheduled_for_soft_deactivation_at') }}
        , {{ load_timestamp('completed_primary_modality_setup_at') }}
        , {{ load_timestamp('previously_completed_onboarding_at') }}
        , {{ load_timestamp('confirmed_through_nurture_at') }}
        , {{ load_timestamp('completed_account_creation_at') }}
        , {{ load_timestamp('remember_created_at') }}
        , {{ load_timestamp('reset_password_sent_at') }}
        , {{ load_timestamp('next_partner_nps_at') }}
        , last_sign_in_ip
        , current_sign_in_ip
        , appointments_count
        , channel
        , coaching_language
        , completed_appointments_count
        , confirmation_token
        , current_member_count
        , email
        , {{ dbt_utils.surrogate_key(['email'])}} as app_user_email_sk
        , email_messages_enabled
        , encrypted_password
        , failed_attempts
        , language
        , motivation
        , pending_primary_recommendation_count
        , phone
        , preferred_contact_method
        , reset_password_token
        , sign_in_count
        , sms_enabled
        , time_zone
        , title
        , unlock_token
        , webex_url
        , ux_state
        , account_type
        , record_free_calls
        , sticky_flash_messages
        , allow_call_recording AS is_call_recording_allowed
        , normalized_phone
        , microsoft_teams_url
        , state
        , country
        , current_mentor_count
        , current_mentee_count
        , alternate_email
        , last_sign_in_user_agent
        , current_sign_in_user_agent
        , slack_messages_enabled
        , upfront_subscription_state
        , aspirations_count
        , teams_messages_enabled
        , account_change_notifications_enabled
        , sso_preferred as is_sso_preferred
        , current_mentee_program_assignment_count
        , current_mentor_program_assignment_count
  from users

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_users as (
/*

  The archived records in this CTE are records that have been
  deleted in source db and lost due to ingestion re-replication.

  A large scale re-replication occured in 2023-06 during the Stitch upgrade
  and the creation of the new landing schema - stitch_app_v2.
  The app_archive tables found with a tag 2023_06 hold the records
  that pertain to the deleted records at that time and reference can be found in
  ../models/staging/app/sources_schema_app.yml file.

  Details of the upgrade process & postmortem can be found in the Confluence doc titled:
  "stitch_app_v2 upgrade | Process Reference Doc"
  https://betterup.atlassian.net/wiki/spaces/DATA/pages/3418750982/stitch+app+v2+upgrade+Process+Reference+Doc

*/

  select
         id AS user_id
        , uuid AS user_uuid
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
--        , coach_id --this column does not exist anymore in the postgres DB
        , coach_profile_id  -- still functional but only for US data (keeping for versions item_id join)
        , coach_profile_uuid
        , manager_id
        , member_profile_id
        , apple_id
        , bluejeans_id
        , inviter_id
        , consumer_id
        , next_appointment_id AS next_session_id
        , organization_id
        , care_profile_id
        , hubspot_contact_id
        , zoom_id
        , skype_id
        , first_name
        , last_name
--        , {{ load_timestamp('last_active_at') }} --this column does not exist anymore in the postgres DB
        , {{ load_timestamp('last_appointment_at') }}
        , {{ load_timestamp('last_sign_in_at') }}
        , {{ load_timestamp('last_engaged_at') }}
        , {{ load_timestamp('locked_at') }}
        , {{ load_timestamp('accepted_organization_terms_at') }}
        , {{ load_timestamp('accepted_terms_at') }}
        , {{ load_timestamp('completed_member_onboarding_at') }}
        , {{ load_timestamp('confirmation_sent_at') }}
        , {{ load_timestamp('confirmed_at') }}
        , {{ load_timestamp('current_sign_in_at') }}
        , {{ load_timestamp('deactivated_at') }}
        , {{ load_timestamp('care_confirmed_at') }}
        , {{ load_timestamp('lead_confirmed_at') }}
        , {{ load_timestamp('scheduled_for_soft_deactivation_at') }}
        , {{ load_timestamp('completed_primary_modality_setup_at') }}
        , {{ load_timestamp('previously_completed_onboarding_at') }}
        , {{ load_timestamp('confirmed_through_nurture_at') }}
        , {{ load_timestamp('completed_account_creation_at') }}
        , {{ load_timestamp('remember_created_at') }}
        , {{ load_timestamp('reset_password_sent_at') }}
        , {{ load_timestamp('next_partner_nps_at') }}
        , last_sign_in_ip
        , current_sign_in_ip
        , appointments_count
        , channel
        , coaching_language
        , completed_appointments_count
        , confirmation_token
        , current_member_count
        , email
        , {{ dbt_utils.surrogate_key(['email'])}} as app_user_email_sk
        , email_messages_enabled
        , encrypted_password
        , failed_attempts
        , language
        , motivation
        , pending_primary_recommendation_count
        , phone
        , preferred_contact_method
        , reset_password_token
        , sign_in_count
        , sms_enabled
        , time_zone
        , title
        , unlock_token
        , webex_url
        , ux_state
        , account_type
        , record_free_calls
        , sticky_flash_messages
        , allow_call_recording AS is_call_recording_allowed
        , normalized_phone
        , microsoft_teams_url
        , state
        , country
        , current_mentor_count
        , current_mentee_count
        , alternate_email
        , last_sign_in_user_agent
        , current_sign_in_user_agent
        , slack_messages_enabled
        , upfront_subscription_state
        , aspirations_count
        , teams_messages_enabled
        , account_change_notifications_enabled
        , sso_preferred as is_sso_preferred
        , current_mentee_program_assignment_count
        , current_mentor_program_assignment_count
  from {{ ref('base_app__users_historical') }}
  where id not in (select user_id from current_users)
)


select * from archived_users
union
{% endif -%}
select * from current_users
