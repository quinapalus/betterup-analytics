with archived_assessments as (

    select * from {{ source('app_archive', 'assessments') }}

)

select * from archived_assessments