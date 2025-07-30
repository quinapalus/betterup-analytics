with issues as (
    
    select * from {{ ref('stg_jira__issues_snapshot') }}

),

sprint_names_list as (

    select
        primary_key,
        listagg(distinct issues_parsed.value:name::string, ' | ') within group (order by issues_parsed.value:name::string) as sprint_names
    from issues,
        lateral flatten(input=>issues.fields:customfield_10010) issues_parsed
    group by primary_key
    
),

components_list as (

    select
        primary_key,
        listagg(distinct issues_parsed.value:name::string, ' | ') within group (order by issues_parsed.value:name::string) as components
    from issues,
        lateral flatten(input=>issues.fields:components) issues_parsed
    group by primary_key
    
),

deleted_issues as (
    
    select * from {{ ref('jira_deleted_issues') }}

)

select

    issues.*,
    sprint_names_list.sprint_names,
    components_list.components,

    --booleans
    iff(issue_type in ('Story', 'Task', 'Sub-task'), true, false) as is_real_work,
    iff(issue_type = 'Bug', true, false) as is_bug,
    iff(project_key in ('DATAOPS', 'BUAPP', 'DSO', 'BUAND', 'BUIOS', 'GFSB', 'MLAI', 'PB', 'SSP', 'BS'), true, false) as is_team_product,
    iff(lower(team) like '% guild', true, false) as is_guild,

    --impact effort framework - https://betterup.atlassian.net/wiki/spaces/PT/pages/2031634/Impact+Effort+Framework
    case 
        when issue_type in ('Epic', 'Initiative') and  effort = 'Xtra Small' then 1
        when issue_type in ('Epic', 'Initiative') and  effort = 'Small' then 7
        when issue_type in ('Epic', 'Initiative') and  effort = 'Medium' then 30
        when issue_type in ('Epic', 'Initiative') and  effort = 'Large' then 90
        when issue_type in ('Epic', 'Initiative') and  effort = 'Xtra Large' then 180
        when issue_type in ('Epic', 'Initiative') then 30

        when issue_type  not in ('Epic', 'Initiative') and  effort = 'Xtra Small' then 0.0006944444444
        when issue_type  not in ('Epic', 'Initiative') and  effort = 'Small' then 0.04166666667
        when issue_type  not in ('Epic', 'Initiative') and  effort = 'Medium' then 1
        when issue_type  not in ('Epic', 'Initiative') and  effort = 'Large' then 7
        when issue_type  not in ('Epic', 'Initiative') and  effort = 'Xtra Large' then 30
        when issue_type  not in ('Epic', 'Initiative') then 1
    else null end as effort_in_days

from issues
left join sprint_names_list
    on issues.primary_key = sprint_names_list.primary_key
left join components_list
    on issues.primary_key = components_list.primary_key
left join deleted_issues
    on issues.jira_issue_id = deleted_issues.jira_issue_id
--filtering out deleted jira issues and jira issues from archived projects
where deleted_issues.jira_issue_id is null
