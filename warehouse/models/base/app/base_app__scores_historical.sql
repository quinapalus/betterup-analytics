with archived_scores as (

    select * from {{ source('app_archive', 'scores') }}

)

select * from archived_scores