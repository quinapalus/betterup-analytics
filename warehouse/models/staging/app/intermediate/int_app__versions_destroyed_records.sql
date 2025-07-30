{{
  config(
    tags=["eu"]
  )
}}

WITH versions_delete AS (
  SELECT * FROM {{ ref('stg_app__versions_delete') }}
) 

SELECT
        version_id
        , {{ load_timestamp('destroyed_at', 'destroyed_at') }}
        , event
        , item_id
        , item_type
        , object
        , object_changes
        , user_agent
        , impersonated
        , impersonated_user_id
        , request_id
        , whodunnit
        , whodunnit_job
        , whodunnit_jid
FROM versions_delete
  WHERE item_type NOT IN ('Notification')