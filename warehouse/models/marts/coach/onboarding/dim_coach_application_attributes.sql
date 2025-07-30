with application_attributes as (
    --at this point in the pipeline we switch over to the naming that we will use in user facing reporting
    --hence the name change from applicant to application
    select * from {{ ref('int_fountain__applicant_attributes') }}

)

select
    primary_key,
    applicant_id as application_id,
    attribute_name,
    attribute_value
from application_attributes
