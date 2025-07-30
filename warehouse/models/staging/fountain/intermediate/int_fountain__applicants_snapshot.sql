with applicants as (
    
    select * from {{ ref('stg_fountain__applicants_snapshot') }}

)

select
    all_applicants.*,
    /* when applicants start an application they use their personal email in the email field
       when they get to a certain point in the application process the email field gets overwritten with their betterup coach email address
       their personal email is copied to the personal_email field
       we need to use personal email as a unique id to get count of distinct individuals. We use this master_personal_email column to do this
    */
    coalesce(personal_email,email) as master_personal_email,
    --creating a surrogate key here so that we don't need to bring PII into the BI layer later
    {{ dbt_utils.surrogate_key(['master_personal_email']) }} as master_personal_email_key
from applicants as all_applicants
