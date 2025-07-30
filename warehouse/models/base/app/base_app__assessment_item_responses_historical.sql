with archived_assessment_item_responses as (

    select * from {{ source('app_archive', 'assessment_item_responses') }}

)

select * from archived_assessment_item_responses