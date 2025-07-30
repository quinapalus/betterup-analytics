WITH products AS (

  SELECT * FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__products') }} 
                {% else %} 
                  {{ source('salesforce', 'products') }}
                {% endif %}
)

SELECT
  id AS sfdc_product_id,
  name,
  product_code,
  type_c AS type,
  family,
  description,
  to_timestamp_ntz(created_date) as created_at,
  is_active
FROM products
