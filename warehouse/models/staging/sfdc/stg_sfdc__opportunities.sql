WITH opportunities AS (

  SELECT * FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__opportunities') }} 
                {% else %} 
                  {{ source('salesforce', 'opportunities') }}
                {% endif %}

  ),

  final AS (
    SELECT *
    FROM opportunities
  )

SELECT * 
FROM final