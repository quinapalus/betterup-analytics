WITH versions AS (

  select
    version_id,
    created_at,
    event,
    item_id,
    item_uuid,
    item_type,
    object,
    object_changes,
    user_agent,
    impersonated,
    impersonated_user_id,
    request_id,
    whodunnit,
    whodunnit_job,
    whodunnit_jid
  from {{ ref('base_app__versions') }}
)


select * from versions




