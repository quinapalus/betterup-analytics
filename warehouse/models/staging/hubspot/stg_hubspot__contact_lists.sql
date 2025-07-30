with source as (
  select * from {{ source('hubspot', 'contact_lists') }}
),

renamed as (
    select
    --primary key
        id as hubspot_contact_list_id,

        --foreign keys
        author_id as hubspot_author_id,

        --attributes
        name,
        created_at,
        list_type

    from source
)

select * from renamed