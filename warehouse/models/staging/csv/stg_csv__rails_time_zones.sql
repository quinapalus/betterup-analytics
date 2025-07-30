with rails_time_zones as (

    select * from {{ ref('rails_time_zones')}}

)

select * from rails_time_zones

