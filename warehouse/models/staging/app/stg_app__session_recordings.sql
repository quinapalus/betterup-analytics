WITH session_recordings AS (

  select * from {{ source('app', 'session_recordings') }}

),

current_session_recordings as (

  select
        call_id,
        {{ load_timestamp('created_at') }},
        ext_archive_id,
        id as session_recording_id,
        status,
        {{ load_timestamp('updated_at') }},
         {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Prod' %}
             coalesce(video_metadata__st, video_metadata__va::varchar) AS video_metadata
         {% else %}
            video_metadata
        {% endif %}
  from session_recordings

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_session_recordings as (
/*

  The archived records in this CTE are records that have been
  deleted in source db and lost due to ingestion re-replication.

  A large scale re-replication occured in 2023-06 during the Stitch upgrade
  and the creation of the new landing schema - stitch_app_v2.
  The app_archive tables found with a tag 2023_06 hold the records
  that pertain to the deleted records at that time and reference can be found in
  ../models/staging/app/sources_schema_app.yml file.

  Details of the upgrade process & postmortem can be found in the Confluence doc titled:
  "stitch_app_v2 upgrade | Process Reference Doc"
  https://betterup.atlassian.net/wiki/spaces/DATA/pages/3418750982/stitch+app+v2+upgrade+Process+Reference+Doc

*/

  select
        call_id,
        {{ load_timestamp('created_at') }},
        ext_archive_id,
        id as session_recording_id,
        status,
        {{ load_timestamp('updated_at') }},
        video_metadata
        from {{ ref('base_app__session_recordings_historical') }}
)


select * from archived_session_recordings
union
{% endif -%}
select * from current_session_recordings
