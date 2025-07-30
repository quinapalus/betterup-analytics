{%- set decision_point_stages = dbt_utils.get_column_values(table=ref('stg_mapping__decision_points'), column='decision_point_stage_name_cleaned') -%}
{%- set fallout_stage_buckets = ['Rejected','Inactive','On Hold'] -%}
{%- set overflow_on_hold_stage_names = ['FY2023 Early Stage Applicants Hold','Hold for Review',
                                        'PC-Hold for Review-NON SPRINT','Excess Coaches Pending Approval','EN - Hold for Review',
                                        'FY2023 Later Stage Applicants Hold','Academy BUCC Hold','ACADEMY CONTRACT HOLD','WTL - Hold for Review'] -%}
{%- set legacy_inactive_stage_names = ['Inactive Sorting','Inactive Referred'] -%}

{%- set overflow_on_hold_stage_names_string = quoted_list(overflow_on_hold_stage_names) -%}
{%- set decision_point_stages_string = quoted_list(decision_point_stages) -%}
{%- set fallout_stage_buckets_string = quoted_list(fallout_stage_buckets) -%}
{%- set legacy_inactive_stage_names_string = quoted_list(legacy_inactive_stage_names) -%}

with transitions as (

    select * from {{ ref('stg_fountain__transitions') }}

),

stages as (

    select * from {{ ref('int_fountain__stages') }}

),

transitions_with_stages as (
    select
        transitions.fountain_applicant_id,
        transitions.stage_id,
        stages.funnel_id,
        stages.stage_title,
        transitions.stage_name,
        stages.stage_title_cleaned,
        iff(stages.stage_title in ({{ overflow_on_hold_stage_names_string }}) or transitions.stage_name in ({{ overflow_on_hold_stage_names_string }}),true,false) as is_overflow_on_hold_stage,
        transitions.created_at,
        lead(created_at,1) over(partition by transitions.fountain_applicant_id,stages.funnel_id order by created_at asc) as next_transition_timestamp,
        lead(stage_title,1) over(partition by transitions.fountain_applicant_id,stages.funnel_id order by created_at asc) as next_stage_title,
        datediff(day,created_at,coalesce(next_transition_timestamp,current_date())) as days_to_next_transition,

        --for each decision point create a fallout flag that will be true if the next stage they transition to is one of the fallout stages
        {%- for stage in decision_point_stages %}
        iff(stage_title_cleaned = '{{ stage }}' and next_stage_title in ({{ fallout_stage_buckets_string }}),true,false) as is_{{ stage }}_fallout,
        {%- endfor %}
        --for each decision stage get the timestamp of the first time that an application transitioned to the stage
        {%- for stage in decision_point_stages %}
        min(case when stages.stage_title_cleaned = '{{ stage }}' then transitions.created_at else null end) over(partition by fountain_applicant_id,stages.funnel_id) as first_entered_{{ stage }}_timestamp
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
        
    from transitions
    left join stages
        on stages.stage_id = transitions.stage_id
),

total_days_spent_in_stage as (
    --calculate the total days spent in each stage for each application
    select
        fountain_applicant_id,
        funnel_id,
        stage_title_cleaned,
        is_overflow_on_hold_stage,
        sum(days_to_next_transition) as total_days_spent_in_stage
    from transitions_with_stages
    group by fountain_applicant_id,funnel_id,stage_title_cleaned,is_overflow_on_hold_stage
),

total_days_spent_in_stage_pivoted as (
    --pivoting rows into columns for total_days_spent_in_stage_bucket so that grain is one row per application and columns are the total number of days spent in each stage bucket
    select
        fountain_applicant_id,
        funnel_id,
        --getting total days spent in a waitlist stage
        sum(iff(is_overflow_on_hold_stage,total_days_spent_in_stage,0)) as days_spent_in_waitlist,
        --for each stage bucket get the total number of days spent in the bucket
        {%- for stage in decision_point_stages %}
        sum(iff(stage_title_cleaned = '{{ stage }}', total_days_spent_in_stage, 0)) as days_spent_in_{{ stage }}
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    from total_days_spent_in_stage
    where funnel_id is not null 
    group by fountain_applicant_id,funnel_id
),

custom_stage_flags as (
/*
creating a flag for if the application has has ever been put into an overflow on-hold stage
the overflow on hold stages are where we store people from places we are currently overstaffed from so we hold them there until there is a demand to bring them on
for certain metrics we are going to want to filter these out. we will use this flag to do that.
*/
    select
        transitions.fountain_applicant_id,
        max(iff(
            /*
            need to do a check for transitions stage_name and stage_title because
            when stages are deleted in Fountain the name is null out from the stages stage_title field
            but is preserved in the transitions sstage_name field.
            */
            transitions.stage_name in ( {{ overflow_on_hold_stage_names_string}} )
            or stages.stage_title in ( {{overflow_on_hold_stage_names_string }}),true, false)) as has_ever_been_overflow_on_hold,
        max(iff(
            /*
            need to do a check for transitions stage_name and stage_title because
            when stages are deleted in Fountain the name is null out from the stages stage_title field
            but is preserved in the transitions sstage_name field.
            */
            transitions.stage_name in ( {{ legacy_inactive_stage_names_string }} )
            or stages.stage_title in ( {{ legacy_inactive_stage_names_string }}),true, false)) as has_ever_been_legacy_inactive
    from transitions
    left join stages
        on stages.stage_id = transitions.stage_id
    group by transitions.fountain_applicant_id

)

select
    {{ dbt_utils.surrogate_key([' transitions_with_stages.fountain_applicant_id','transitions_with_stages.funnel_id']) }} as primary_key,
    transitions_with_stages.fountain_applicant_id,
    transitions_with_stages.funnel_id,
    total_days_spent_in_stage_pivoted.* exclude(fountain_applicant_id,funnel_id),
    custom_stage_flags.* exclude(fountain_applicant_id),

    {%- for stage in decision_point_stages %}
    max(is_{{ stage }}_fallout) as is_{{ stage }}_fallout,
    {%- endfor %}

    {%- for stage in decision_point_stages %}
    max(first_entered_{{ stage }}_timestamp) as first_entered_{{ stage }}_timestamp
    {%- if not loop.last -%},{% endif %}
    {%- endfor %}

from transitions_with_stages
left join total_days_spent_in_stage_pivoted
    on total_days_spent_in_stage_pivoted.fountain_applicant_id = transitions_with_stages.fountain_applicant_id
    and total_days_spent_in_stage_pivoted.funnel_id = transitions_with_stages.funnel_id
left join custom_stage_flags
    on custom_stage_flags.fountain_applicant_id = transitions_with_stages.fountain_applicant_id
where transitions_with_stages.funnel_id is not null
group by all
