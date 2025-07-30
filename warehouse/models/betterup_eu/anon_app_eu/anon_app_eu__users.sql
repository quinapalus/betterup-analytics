WITH src_eu_users AS ( 
    SELECT * FROM {{ source('app_eu', 'users') }}
),

users_eu AS (
    SELECT
        ACCEPTED_TERMS_AT,
        ACCEPTED_ORGANIZATION_TERMS_AT,
        ACCOUNT_CHANGE_NOTIFICATIONS_ENABLED,
        ACCOUNT_TYPE,
        ALLOW_CALL_RECORDING,
--         ALTERNATE_EMAIL, --pii do not include
        APPLE_ID,
        APPOINTMENTS_COUNT,
        ASPIRATIONS_COUNT,
--        AVATAR, --pii do not include
        BLUEJEANS_ID,
        CARE_CONFIRMED_AT,
        CARE_PROFILE_ID,
        CHANNEL,
        COACHING_LANGUAGE,
        COACH_PROFILE_ID, -- still functional but only for US data
        COACH_PROFILE_UUID,
        COMPLETED_ACCOUNT_CREATION_AT,
        COMPLETED_APPOINTMENTS_COUNT,
        COMPLETED_MEMBER_ONBOARDING_AT,
        COMPLETED_PRIMARY_MODALITY_SETUP_AT,
        CONFIRMATION_SENT_AT,
        CONFIRMATION_TOKEN,
        CONFIRMED_AT,
        CONSUMER_ID,
        CONFIRMED_THROUGH_NURTURE_AT,
--         COUNTRY, --pii do not include
        CREATED_AT,
        CURRENT_MEMBER_COUNT,
        CURRENT_MENTEE_COUNT,
        CURRENT_MENTEE_PROGRAM_ASSIGNMENT_COUNT ,
        CURRENT_MENTOR_COUNT,
        CURRENT_MENTOR_PROGRAM_ASSIGNMENT_COUNT ,
        CURRENT_SIGN_IN_AT,
        CURRENT_SIGN_IN_IP,
        CURRENT_SIGN_IN_USER_AGENT,
        DEACTIVATED_AT,
--         EMAIL, --pii do not include
        EMAIL_MESSAGES_ENABLED,
        ENCRYPTED_PASSWORD,
        FAILED_ATTEMPTS,
--         FIRST_NAME, --pii do not include
        ID,
        INVITER_ID,
        LANGUAGE,
        LAST_APPOINTMENT_AT,
        LAST_ENGAGED_AT,
--         LAST_NAME, --pii do not include
        LAST_SIGN_IN_AT,
--        LAST_ACTIVE_AT, --this column does not exist anymore in the postgres DB
        LAST_SIGN_IN_IP,
        LAST_SIGN_IN_USER_AGENT,
        LEAD_CONFIRMED_AT,
        LOCKED_AT,
        MANAGER_ID,
        MEMBER_PROFILE_ID,
        MICROSOFT_TEAMS_URL,
        MOTIVATION,
        NEXT_APPOINTMENT_ID,
        NEXT_PARTNER_NPS_AT,
        NORMALIZED_PHONE,
        ORGANIZATION_ID,
        PENDING_PRIMARY_RECOMMENDATION_COUNT,
--         PHONE, --pii do not include
        PREFERRED_CONTACT_METHOD,
        PREVIOUSLY_COMPLETED_ONBOARDING_AT,
        REMEMBER_CREATED_AT,
        RESET_PASSWORD_SENT_AT,
        RESET_PASSWORD_TOKEN,
        RECORD_FREE_CALLS,
        SCHEDULED_FOR_SOFT_DEACTIVATION_AT,
        SIGN_IN_COUNT,
        SKYPE_ID,
        SLACK_MESSAGES_ENABLED,
        SMS_ENABLED,
        SSO_PREFERRED,
--         STATE, --pii do not include
        STICKY_FLASH_MESSAGES,
        TEAMS_MESSAGES_ENABLED,
--        TIME_ZONE, --pii do not include
        TITLE,
        UNLOCK_TOKEN,
        UPDATED_AT,
        UPFRONT_SUBSCRIPTION_STATE
        UX_STATE,
        WEBEX_URL,
        ZOOM_ID,
        UUID,
        HUBSPOT_CONTACT_ID
    FROM src_eu_users
)

SELECt * FROM users_eu