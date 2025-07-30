{{
  config(
    tags=["eu"]
  )
}}

select * from {{ ref('stg_app__scheduled_jobs') }}
