with archived_assessment_contributors as (

    select * from {{ source('app_archive', 'assessment_contributors') }}

)

select * from archived_assessment_contributors