
WITH users AS (

  SELECT * FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__users') }} 
                {% else %} 
                  {{ source('salesforce', 'users') }}
                {% endif %}

)


SELECT
  id AS sfdc_user_id,
  -- some user accounts associated with system integrations have NULL first_name
  COALESCE(first_name, '') AS first_name,
  last_name,
  name,
  manager_id,
  user_role_id as sfdc_user_role_id
FROM users
