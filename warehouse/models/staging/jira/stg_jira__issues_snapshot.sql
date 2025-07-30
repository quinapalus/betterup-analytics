with issues as (

  select * from {{ ref('snapshot_jira_issues') }}

),

final as (

    select
        iss.id::int as jira_issue_id,
        iss.key::string as jira_issue_key,
        iss.fields,
        iss.fields:issuetype:name::string as issue_type,
        iss.fields:status:name::string as status,
        iss.fields:priority:name::string as priority,
        iss.fields:summary::string as summary,
        iss.fields:description::text as description,
        iss.fields:assignee:displayName::string as assignee,
        iss.fields:creator:displayName::string as creator,
        iss.fields:customfield_10033:value::string as impact,
        iss.fields:customfield_10034:value::string as effort,
        iss.fields:customfield_10001:title::string as team,
        iss.fields:customfield_10111:value::string as investment_category,
        iss.fields:project:key::string as project_key,
        iss.fields:customfield_10041::int as story_points,
        iss.fields:customfield_10131:value::string as fiscal_quarter,
        array_to_string(iss.fields:labels, ' | ' ) as labels,
        iss.fields:issuelinks as issue_links,

        --timestamps
        {{ load_timestamp('iss.fields:created', alias='created_at') }},
        {{ load_timestamp('iss.fields:updated', alias='updated_at') }},
        {{ load_timestamp('iss.fields:duedate', alias='due_date') }},
        {{ load_timestamp('iss.fields:customfield_10043', alias='start_date') }},
        convert_timezone('UTC', to_timestamp_tz(iss.fields:resolutiondate::string, 'YYYY-MM-DDTHH24:MI:SS.FF3-TZHTZM'))::timestamp_ntz as resolved_at,
        
        --snapshot metadata
        iss.dbt_valid_from as valid_from,
        iss.dbt_valid_to as valid_to,
        iss.dbt_valid_to is null as is_current_version,
        row_number() over(
        partition by iss.id
        order by iss.dbt_valid_from
        ) as version,

        case when
        row_number() over(
            partition by iss.id,date_trunc('day',iss.dbt_valid_from::timestamp_ntz)
            order by iss.dbt_valid_from desc
        ) = 1 then true else false end as is_last_snapshot_of_day

    from issues as iss)

select 
    {{ dbt_utils.surrogate_key(['jira_issue_id','valid_to','valid_from']) }} as primary_key,
    final.*
from final
