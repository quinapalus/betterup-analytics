{{
  config(
    tags=["eu"]
  )
}}

WITH src_users AS (
  SELECT * FROM {{ source('analytics_eu_read_only', 'anon_app_eu__users') }}
),

users AS (
    SELECT
         id AS user_id
        , uuid AS user_uuid
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
        , coach_profile_id  -- still functional but only for US data
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
        , zoom_id
        , skype_id
--        , {{ load_timestamp('last_active_at') }} --this column does not exist anymore in the postgres DB
        , {{ load_timestamp('last_appointment_at') }}
        , {{ load_timestamp('last_sign_in_at') }}
        , {{ load_timestamp('last_engaged_at') }}
        , {{ load_timestamp('locked_at') }}
--        , {{ load_timestamp('accepted_organization_terms_at') }}
        , {{ load_timestamp('accepted_terms_at') }}
--        , {{ load_timestamp('activated_at') }}
--        , {{ load_timestamp('closed_at') }}
        , {{ load_timestamp('completed_member_onboarding_at') }}
        , {{ load_timestamp('confirmation_sent_at') }}
        , {{ load_timestamp('confirmed_at') }}
        , {{ load_timestamp('current_sign_in_at') }}
        , {{ load_timestamp('deactivated_at') }}
--        , {{ load_timestamp('care_confirmed_at') }}
        , {{ load_timestamp('lead_confirmed_at') }}
--        , {{ load_timestamp('scheduled_for_deactivation_at') }}
--        , {{ load_timestamp('scheduled_for_soft_deactivation_at') }}
        , {{ load_timestamp('completed_primary_modality_setup_at') }}
        , {{ load_timestamp('previously_completed_onboarding_at') }}
--        , {{ load_timestamp('confirmed_through_nurture_at') }}
        , {{ load_timestamp('completed_account_creation_at') }}
--        , {{ load_timestamp('remember_created_at') }}
--        , {{ load_timestamp('reset_password_sent_at') }}
        , {{ load_timestamp('next_partner_nps_at') }}
        , last_sign_in_ip
        , current_sign_in_ip
        , appointments_count
        , channel
        , coaching_language
        , completed_appointments_count
        , confirmation_token
        , current_member_count
        , email_messages_enabled
        , encrypted_password
        , failed_attempts
        , language
        , motivation
        , pending_primary_recommendation_count
        , preferred_contact_method
--        , reset_password_token
--        , roles
        , sign_in_count
        , sms_enabled
        , title
        , unlock_token
        , webex_url
        , ux_state
        , account_type
--        , record_free_calls
        , sticky_flash_messages
        , allow_call_recording AS is_call_recording_allowed
        , normalized_phone
        , microsoft_teams_url
        , current_mentor_count
        , current_mentee_count
        , last_sign_in_user_agent
        , current_sign_in_user_agent
        , slack_messages_enabled
--        , skipped_upfront_subscription
--        , upfront_subscription_state
        , aspirations_count
        , teams_messages_enabled
        , account_change_notifications_enabled
        , sso_preferred
        , current_mentee_program_assignment_count
        , current_mentor_program_assignment_count

    FROM src_users
)

SELECT * FROM users
