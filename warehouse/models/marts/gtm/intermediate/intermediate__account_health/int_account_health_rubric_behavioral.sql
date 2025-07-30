with construct_scores_internal as (

    select 
        *,
        row_number() over( 
                    partition by member_id, construct_type, construct_key, reference_population_id
                    order by submitted_at) as score_sequence,
        row_number() over( 
                    partition by member_id, construct_type, construct_key, reference_population_id
                    order by submitted_at desc) as score_reverse_sequence  
    
    from {{ ref('fact_reporting_group_reference_population_construct_scores') }}
    where 
        deployment_group = 'B2B / Gov Paid Contract'
        and is_primary_reflection_point
),

matched_reference_scores_internal as (

    select 
        *,
        row_number() over( 
                    partition by member_id, construct_type, construct_key, reference_population_id
                    order by submitted_at) as score_sequence,
        row_number() over( 
                    partition by member_id, construct_type, construct_key, reference_population_id
                    order by submitted_at desc) as score_reverse_sequence  
        
    from {{ ref('fact_reporting_group_reference_population_construct_scores') }}
    where 
        deployment_group = 'B2B / Gov Paid Contract'
        and is_onboarding
),

tracks as (
    
    select * from {{ ref('dim_tracks') }}

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

accounts_aggregated as (


{% set construct_domains = ["mindset", "thriving", "inspiring", "outcome"] %}


select
    accounts.sfdc_account_id,
    accounts.betterup_segment,
    count(distinct case when members.member_id is not null then members.member_id else null end) as count_activated_members,    
    --the below for loop is based on logic from this pivoted look. 
    --https://betterup.looker.com/explore/assessments/wp_assessments?qid=Nsx5AvskQFKvTRwdl8TKJf&origin_space=8989&toggle=vis
   

    {% for construct_domain in construct_domains %}

    (avg(case when lower(matched_reference_scores_internal.construct_attributes:domain_name::varchar) =  '{{construct_domain}}'
             then construct_scores.scale_score end)
    -
    avg(case when lower(matched_reference_scores_internal.construct_attributes:domain_name::varchar) =  '{{construct_domain}}'
             then matched_reference_scores_internal.scale_score end))
    /
    avg(case when lower(matched_reference_scores_internal.construct_attributes:domain_name::varchar) =  '{{construct_domain}}'
             then matched_reference_scores_internal.scale_score end) as {{construct_domain}}_average_percent_growth_from_reference,

    {% endfor %}

     
    (avg(construct_scores.scale_score) - avg(matched_reference_scores_internal.scale_score))
    / 
    avg(matched_reference_scores_internal.scale_score) as overall_average_percent_growth_from_reference,
    
    case 
        when (accounts.betterup_segment in ('Enterprise','Strategic','Government') and count_activated_members >= 30)
                   or
             (accounts.betterup_segment in ('SMB','MidMarket','Emerging Enterprise') and count_activated_members >= 10)
        then true else false end as has_met_activated_members_threshold,  

    case
        when overall_average_percent_growth_from_reference >= 0.17
            then 'Good'
        when overall_average_percent_growth_from_reference between 0.08 and 0.17
            then 'Okay'
        when overall_average_percent_growth_from_reference <= 0.08
            then 'Poor' else null end as account_health_rubric_behavorial_overall_score

from construct_scores_internal as construct_scores
inner join members 
    on members.member_id = construct_scores.member_id
inner join matched_reference_scores_internal
    on construct_scores.member_id = matched_reference_scores_internal.member_id
    and construct_scores.construct_key = matched_reference_scores_internal.construct_key
    and construct_scores.reference_population_id = matched_reference_scores_internal.reference_population_id
    and matched_reference_scores_internal.submitted_at < construct_scores.submitted_at
left join organizations
    on organizations.organization_id = construct_scores.organization_id
inner join accounts
    on accounts.sfdc_account_id = organizations.sfdc_account_id
inner join eligible_accounts
    on accounts.sfdc_account_id = eligible_accounts.sfdc_account_id

where
    construct_scores.is_primary_reflection_point
    and construct_scores.score_reverse_sequence = 1
    and matched_reference_scores_internal.score_sequence = 1
    and matched_reference_scores_internal.is_onboarding
    and construct_scores.reference_population_id = construct_scores.organization_reference_population_id
    and lower(matched_reference_scores_internal.construct_attributes:domain_name::varchar) in ('mindset', 'thriving', 'inspiring', 'outcome')
    and members.activated_at < dateadd('month', -4, current_timestamp())
    and members.activated_at >= dateadd('month', -16, current_timestamp())

group by accounts.sfdc_account_id, accounts.betterup_segment)

select * from accounts_aggregated where has_met_activated_members_threshold
