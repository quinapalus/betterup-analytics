
/* This utilizes the DBT PROJECT EVALUATOR's STG_NODES output to 
   verify that a given active model has at least a team owner assigned to it.
   By default, all models inherit the Analytics-Engineering-Team ownership from
   dbt_project.yml. However, in the event that file is modified and models have
   no other ownership in their respective properties yml files, this will fail.
   In any other event where an active model does not have an owner identified,
   this will fail. Failure is rows returning.
   
    Filters:
        - Resource Type = Model:
            We only want to check models, not sources, tests, etc.
        - Package Name = 'warehouse':
            We only want to check against models in our main project, not 3rd party packages.
        - Meta ILIKE '%team_owner%' OR META ILIKE '%owner%':
            We want to check for either team_owner and owner, as some models
            may have one or the other.
*/

SELECT * FROM {{ ref('stg_nodes') }} 
WHERE RESOURCE_TYPE = 'model'
  AND PACKAGE_NAME = 'warehouse'
  AND NOT (
          META ILIKE '%team_owner%'
       OR META ILIKE '%owner%'
          )
