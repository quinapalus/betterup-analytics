with source as (

   select * from {{ source('gsheets_assessments', 'assessments') }}

),

renamed as (

   select
       assessment_name,
       assessment_type,
       user_role,
       description AS assessment_description
   from source
),

final as (
   select
      {{ dbt_utils.surrogate_key(['assessment_name', 'assessment_type']) }} as _unique,
      *
   from renamed
   where assessment_name is not null
)

select * from final