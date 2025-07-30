with user_roles as (

  select * from {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                    {{ ref('base_fed_sfdc__user_roles') }} 
                {% else %} 
                    {{ source('salesforce', 'user_roles') }}
                {% endif %}

)

select
  id AS sfdc_user_role_id,
  name as role_name
from user_roles
