WITH opportunity_line_items AS (

  SELECT * FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__opportunity_line_items') }} 
                {% else %} 
                  {{ source('salesforce', 'opportunity_line_items') }}
                {% endif %}

)

SELECT
  id AS sfdc_opportunity_line_item_id,
  opportunity_id AS sfdc_opportunity_id,
  product_2_id AS sfdc_product_id,
  {{ environment_varchar_to_timestamp('created_date','created_at') }},
  name,
  description,
  product_code,
  quantity
FROM opportunity_line_items
