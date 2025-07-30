
WITH app_contracts AS (

  SELECT * FROM {{ref('stg_app__contracts')}}

),

organizations AS (

  SELECT * FROM {{ref('stg_app__organizations')}}

),

contract_flattened as (
    
    SELECT
    
      contract_id,
      name,
      organization_id,
      flattened.value AS sfdc_opportunity_id
      
    FROM app_contracts as c,
    LATERAL FLATTEN (INPUT => sfdc_opportunity_ids, outer => true) flattened
    
)

    SELECT

      c_f.contract_id,
      c_f.name,
      c_f.organization_id,
      c_f.sfdc_opportunity_id,
      o.sfdc_account_id
          
    FROM contract_flattened AS c_f
    JOIN organizations AS o
      ON o.organization_id = c_f.organization_id
    WHERE c_f.name IS NOT NULL
