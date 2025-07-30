

WITH accounts AS (

  SELECT * exclude ( id,
                     name,
                     owner_id,
                     acct_csm_c,
                     created_date,
                     last_modified_date,
                     last_modified_by_id,
                     description,
                     industry,
                     account_owner_region_c,
                     account_segment_c,
                     billing_city,
                     billing_country,
                     d_b_region_c,
                     company_size_c,
                     is_deleted,
                     type) ,
          id AS sfdc_account_id,
          name AS account_name,
          owner_id AS account_owner_id,
          acct_csm_c AS account_csm_id,
          {{ environment_varchar_to_timestamp('created_date','created_at') }},
          {{ environment_varchar_to_timestamp('last_modified_date','last_modified_at') }},
          last_modified_by_id,
          description,
          industry,
          account_owner_region_c AS account_owner_region,
          account_segment_c AS account_segment,
          billing_city AS city,
          billing_country AS country,
          d_b_region_c AS region,
          company_size_c AS company_size,
          is_deleted,
          type AS account_type
  FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__accounts') }} 
                {% else %} 
                  {{ source('salesforce', 'accounts') }}
                {% endif %}
)

SELECT * 
FROM accounts
WHERE sfdc_account_id = '0012J00002RDhdqQAD' OR NOT is_deleted -- Account Exclusion is: BetterUp Direct to Consumer (Acme)
