with archived_users_roles as (

    select
        distinct
    -- primary keys
    {{ dbt_utils.surrogate_key(['created_at', 'role_id', 'user_id'])}} as user_role_id,

    --foreign keys
      role_id,
      user_id,

    --logical data
      {{ load_timestamp('created_at') }}
    from {{ source('app_archive', 'users_roles') }}

)

select * from archived_users_roles
