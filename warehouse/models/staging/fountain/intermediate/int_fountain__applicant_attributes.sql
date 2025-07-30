with applicants as (

    select * from {{ ref('stg_fountain__applicants_snapshot') }}
    where is_current_version --only need current state of applicants for this model

),

language_code_mapping as (

    select * from {{ ref('iso_639_language_codes') }}

),

applicant_attributes as (

select 
  applicant_id,
  attributes.key as attribute_name,
  list_items.value::string as attribute_value,
  {{ dbt_utils.surrogate_key(['applicant_id','attribute_name','attribute_value']) }} as primary_key
from 
  applicants,
  --unnesting the custom attributes
  lateral flatten(input => custom_attributes) as attributes,
  lateral flatten(input => attributes.value) as list_items
where attributes.key in ('coaching_languages','race_ethnicity','coaching_credential') 
                        --this is the list of attributes with a many to one relationship with applicants that we need for reporting
)

select
    primary_key,
    applicant_id,
    --making the attribute names user friendly
    initcap(replace(attribute_name,'_',' ')) as attribute_name,
    coalesce(language_code_mapping.language_name,applicant_attributes.attribute_value) as attribute_value
from applicant_attributes
left join language_code_mapping --converting the language codes to the proper language names
    on language_code_mapping.alpha2 = applicant_attributes.attribute_value
       and applicant_attributes.attribute_name = 'coaching_languages'
