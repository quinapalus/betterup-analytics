{{
  config(
    tags=["eu"]
  )
}}

with source as (
    select * from {{ source('curriculum', 'solutions') }}
),

renamed as (

    select
--        id AS solution_id,  -- this ID is still functional but only for US data. We decided to only expose and use the
                              -- solution_uuid since that one can be used in the US and EU instance
        uuid AS solution_uuid,
        created_at,
        updated_at,
        key,
        PARSE_JSON(name_i18n):"en"::varchar AS name,
        name_i18n,
        type

    from source

)

select * from renamed
