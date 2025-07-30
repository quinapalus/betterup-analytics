WITH contacts AS (

  SELECT * FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__contacts') }} 
                {% else %} 
                  {{ source('salesforce', 'contacts') }}
                {% endif %}

)

SELECT
  id AS sfdc_contact_id,
  account_id AS sfdc_account_id,
  email,
  -- map NULLs to FALSE
  coalesce(current_user_c, false) AS is_current_member,
  coalesce(past_user_c, false) AS is_past_member,
  coalesce(program_admin_c, false) AS is_program_admin,
  -- fetch status
  coalesce(is_deleted, false) AS is_deleted
FROM contacts
WHERE email IS NOT NULL