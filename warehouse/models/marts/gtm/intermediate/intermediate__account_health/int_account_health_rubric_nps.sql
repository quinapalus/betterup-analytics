with tracks as (
    
    select * from {{ ref('dim_tracks') }}

),

track_assignments as (
    select * from {{ ref('stg_app__track_assignments') }}

),

organizations as (

    select * from {{ ref('stg_app__organizations') }}

),

accounts as (

    select * from {{ ref('int_sfdc__accounts_snapshot') }} 
    where is_current_version and not is_deleted --only want the most recent snapshots

),

eligible_accounts as (

    select * from {{ ref('int_account_health_eligible_accounts') }}
    --all accounts that meet criteria from here
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet

),

members as (

    select * from {{ ref('dim_members') }}

),

assessments as (
    
    select * from {{ ref('fact_assessments') }}

),

assessment_item_responses as (

    select * from {{ ref('fact_assessment_item_responses') }}

),

accounts_aggregated as (

    
    select
        accounts.sfdc_account_id,
        accounts.betterup_segment,
        accounts.most_recent_partner_rps,
        count(distinct case 
                        when members.member_id is not null then members.member_id else null end) as count_activated_members,
        sum(assessment_item_responses.nps_promoter_flag) as sum_nps_promoters,
        sum(assessment_item_responses.nps_detractor_flag) as sum_nps_detractors,
        count(assessment_item_responses.assessment_id || '-' || assessment_item_responses.item_key) as count_item_responses,
        100.0 * (sum_nps_promoters - sum_nps_detractors) / nullif(count_item_responses,0) as member_net_promoter_score,
        case when member_net_promoter_score is null then 1 else 0 end +
        case when most_recent_partner_rps is null then 1 else 0 end as count_of_empty_fields,

        case
            when member_net_promoter_score > 70
                then 'Good'
            when member_net_promoter_score between 0 and 70
                then 'Okay'
            when member_net_promoter_score < 0
                then 'Poor'
            else null end as account_health_rubric_nps_driver_member_nps,
        
        case
            when most_recent_partner_rps >= 9.0
                then 'Good'
            when most_recent_partner_rps between 7.0 and 9.0
                then 'Okay'
            when most_recent_partner_rps < 7.0
                then 'Poor'
            else null end as account_health_rubric_nps_driver_partner_rps,

        case
            when member_net_promoter_score is null or count_activated_members < 10
                then 'Not Enough Data'
            when count_of_empty_fields = 0 and member_net_promoter_score >= 70 and most_recent_partner_rps >= 9.0
                then 'Good'
            when count_of_empty_fields = 0 and member_net_promoter_score <=0 and most_recent_partner_rps < 7.0
                then 'Poor'
            when member_net_promoter_score >= 70
                then 'Good'
            when member_net_promoter_score <= 0
                then 'Poor' else 'Okay' end as account_health_rubric_nps_overall_score
        
from assessments
inner join assessment_item_responses
    on assessments.assessment_id = assessment_item_responses.assessment_id
inner join track_assignments
    on track_assignments.track_assignment_id = assessments.track_assignment_id
inner join tracks
    on tracks.track_id = track_assignments.track_id
inner join organizations 
    on organizations.organization_id = tracks.organization_id
inner join accounts 
    on accounts.sfdc_account_id = organizations.sfdc_account_id
inner join eligible_accounts
    on accounts.sfdc_account_id = eligible_accounts.sfdc_account_id
inner join members
    on members.member_id = assessments.user_id

where
    assessments.assessment_name = 'Reflection Point'
    and assessment_item_responses.item_key = 'net_promoter'
    and not track_assignments.is_hidden
    and tracks.deployment_group = 'B2B / Gov Paid Contract'
    and members.activated_at < dateadd('month', -4, current_timestamp())
    and members.activated_at >= dateadd('month', -16, current_timestamp())
group by accounts.sfdc_account_id,accounts.betterup_segment,accounts.most_recent_partner_rps

)

select 
* 
from accounts_aggregated
where
    (betterup_segment in ('Enterprise','Strategic','Government') and count_activated_members >= 30)
    or
    (betterup_segment in ('SMB','MidMarket','Emerging Enterprise') and count_activated_members >= 10)

