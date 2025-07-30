with issues_labels as (
    select
        primary_key as snapshot_primary_key,
        jira_issue_id,
        fields:labels as labels,
        issue_labels.value::string as label
    from {{ ref('stg_jira__issues_snapshot') }},
    lateral flatten(input => fields:labels) as issue_labels
    where array_size(fields:labels) > 0 and is_current_version)

select
    {{ dbt_utils.surrogate_key(['snapshot_primary_key','label']) }} as primary_key,
    jira_issue_id,
    issues_labels.label
from issues_labels
