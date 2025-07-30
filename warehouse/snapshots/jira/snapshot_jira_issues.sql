{% snapshot snapshot_jira_issues %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at = 'fields:updated',
        )
    }}

    select * from {{ source('jira', 'issues') }}

{% endsnapshot %}
