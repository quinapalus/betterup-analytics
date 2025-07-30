with applications as (
    --at this point in the pipeline we switch over to the naming that we will use in user facing reporting
    --hence the name change from applicant to application
    select * from {{ ref('int_fountain__applicants_snapshot') }}
    where is_current_version

),

application_attributes as (

    select * from {{ ref('int_fountain__applicant_attributes') }}

),

concatenated_application_attributes as (
--concatenating attributes into a comma seperated list.
    select 
        applicant_id as application_id,
        attribute_name,
        listagg(attribute_value, ',') within group (order by attribute_value) as attribute_values
      from 
        application_attributes
      group by 
        application_id, 
        attribute_name

),

concatenated_application_attributes_pivoted as (
--pivoting rows to columns so that we get one row per applicant
    select * 
    from concatenated_application_attributes
        pivot(max(attribute_values) for attribute_name in ('Coaching Languages','Coaching Credential','Race Ethnicity'))
        as renamed (application_id, coaching_languages_concatenated, coaching_credentials_concatenated, race_ethnicity_concatenated)
)

select
    --ids
    applicant_id as application_id,
    master_personal_email_key,

    --categorical and text attributes
    region,
    sprint,
    pay_rate_tier,
    lgbtq_status,
    professional_coaching_hours,
    extended_network_specialist_type,
    citizenship_verification,
    utm_campaign,
    utm_source,
    utm_term,
    utm_content,
    application_source,
    referrer,
    executive_cloud_rating,
    potential_sales_performance_rating,
    offering_type,
    
    --matrices
    care_clinical_matrix,
    care_coach_matrix,
    care_matrix,
    inspiring_matrix,
    thriving_matrix,

    --concatenated attributes
    concatenated_application_attributes_pivoted.coaching_languages_concatenated,
    concatenated_application_attributes_pivoted.coaching_credentials_concatenated,
    concatenated_application_attributes_pivoted.race_ethnicity_concatenated

from applications
left join concatenated_application_attributes_pivoted
    on applications.applicant_id = concatenated_application_attributes_pivoted.application_id
