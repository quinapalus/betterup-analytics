with iana_tz_info as (

    select * from {{ ref('iana_tz_info') }}

)

select * from iana_tz_info

