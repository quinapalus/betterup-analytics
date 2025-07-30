select 
    --ids
    id as applicant_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as primary_key,
    --categorical and text attributes
    --data is a an array with custom attributes for applicants many of which we need in reporting
    --i'm also bringing in the field itself because it will be needed downstream
    --there is a JSON formatting issue with null values in the data object. the replace statement below fixes this
    parse_json(replace(data::string, 'None', 'null')) as custom_attributes,
    email,
    personal_email,
    replace(replace(phone_number,' ', ''),'+','') as phone_number,
    custom_attributes:location_region::string as region,
    custom_attributes:sprint_tracking::string as sprint,
    custom_attributes:pay_rate_tier::string as pay_rate_tier,
    custom_attributes:lgbtq::string as lgbtq_status,
    custom_attributes:professional_coaching_hours::string as professional_coaching_hours,
    custom_attributes:en_specialist_type::string as extended_network_specialist_type,
    custom_attributes:citizenship_verification[0]::string as citizenship_verification,
    custom_attributes:utm_source::string as utm_source,
    custom_attributes:utm_campaign::string as utm_campaign,
    custom_attributes:utm_term::string as utm_term,
    custom_attributes:utm_content::string as utm_content,
    custom_attributes:source::string as application_source,
    custom_attributes:referrer::string as referrer,
    custom_attributes:executive_cloud_rating::string as executive_cloud_rating,
    custom_attributes:potential_sales_performance::string as potential_sales_performance_rating,
    custom_attributes:offering_type::string as offering_type,

    --booleans
    is_duplicate,

    --matrices
    custom_attributes:care_clinical_matrix::string as care_clinical_matrix,
    custom_attributes:care_coach_matrix::string as care_coach_matrix,
    custom_attributes:care_matrix::string as care_matrix,
    custom_attributes:inspiring_matrix::string as inspiring_matrix,
    custom_attributes:thriving_matrix::string as thriving_matrix,


    --timestamps   
    {{ load_timestamp('created_at', alias='created_at') }},
    {{ load_timestamp('updated_at', alias='updated_at') }},

    --other
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current_version,
    
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,
      
    case when
      row_number() over(
        partition by id,date_trunc('day',dbt_valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day
  
from {{ ref('snapshot_fountain_applicants') }}
