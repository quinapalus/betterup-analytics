{{
  config(
    tags=['eu']
  )
}}

select * from {{ ref('stg_app__resources') }}
