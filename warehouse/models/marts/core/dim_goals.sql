{{
  config(
    tags=['eu']
  )
}}

with goals as (

  select * from {{ ref('stg_app__goals') }}

)

select * from goals
