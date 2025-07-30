with archived_experience_configuration_assignments as (

    select * from {{ source('app_archive', 'experience_configuration_assignments') }}

)

select * from archived_experience_configuration_assignments