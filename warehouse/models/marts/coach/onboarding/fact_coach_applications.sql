--setting some variables that will be used multiple times. 

{%- set all_stage_buckets = ['application','administration','readiness','finalize','approved','rejected','on_hold','inactive'] -%}
{%- set active_stage_buckets = ['application','administration','readiness','finalize'] -%}
{%- set unsuccessful_stage_buckets = ['rejected','on_hold','inactive'] -%}
{%- set rejected_stage_buckets = ['rejected'] -%}
{%- set fallout_stage_buckets = ['rejected','inactive','on_hold'] -%}
{%- set successful_stage_buckets = ['approved'] -%}
{%- set first_stage_buckets = ['application'] -%}
{%- set active_and_successful_stage_buckets = active_stage_buckets + successful_stage_buckets -%}
{%- set overflow_on_hold_stage_names = ['FY2023 Early Stage Applicants Hold','Hold for Review',
                                        'PC-Hold for Review-NON SPRINT','Excess Coaches Pending Approval','EN - Hold for Review',
                                        'FY2023 Later Stage Applicants Hold','Academy BUCC Hold','ACADEMY CONTRACT HOLD'] -%}
{%- set legacy_inactive_stage_names = ['Inactive Sorting','Inactive Referred'] -%}

--the quoted_list_macro is used to convert a list of strings into a comma separated list of quoted strings for use in SQL "in" statements
{%- set all_stage_buckets_string = quoted_list(all_stage_buckets) -%}
{%- set active_stage_buckets_string = quoted_list(active_stage_buckets) -%}
{%- set unsuccessful_stage_buckets_string = quoted_list(unsuccessful_stage_buckets) -%}
{%- set fallout_stage_buckets_string = quoted_list(fallout_stage_buckets) -%}
{%- set rejected_stage_buckets_string = quoted_list(rejected_stage_buckets) -%}
{%- set successful_stage_buckets_string = quoted_list(successful_stage_buckets) -%}
{%- set first_stage_buckets_string = quoted_list(first_stage_buckets) -%}
{%- set active_and_successful_stage_buckets_string = quoted_list(active_and_successful_stage_buckets) -%}
{%- set overflow_on_hold_stage_names_quoted_list = quoted_list(overflow_on_hold_stage_names) -%}
{%- set legacy_inactive_stage_names_quoted_list = quoted_list(legacy_inactive_stage_names) -%}

--Prototype query of this model can be found here https://app.snowflake.com/pb82673/oj02423/w5QCW8B86U2h#query
--the final grain of this model is one row per coach application per funnel
--application is used interchangeably with applicant in this model

with transitions as (

    select * from {{ ref('stg_fountain__transitions') }}

),

stages as (

    select *,
    row_number() over(partition by funnel_id,bucket_name order by funnel_order) as bucket_index
    from {{ ref('int_fountain__stages') }}

),

app_coaches as (
    --we use this to get first_staffable_at
    select * from {{ ref('dim_coaches') }}

),

forecast_details as (

    select * from {{ ref('stg_mapping__forecasted_staffable_date') }}

),

application_auxillary_details as (
    --some applicaiton calculated fields have been split over to int_coach__applications_auxillary_details to keep this model cleaner
    --anything having to do with standard stage buckets we will do here. 
    --anything more custom having to do with specific fountain stages we will do in this auxillary model
    --we join int_coach__applications_auxillary_details to the end result of this model
    select * from {{ ref('int_coach__applications_auxillary_details') }}

),

target_days_to_staffable as (

    select 
        funnel_id,
        forecast_start_date,
        forecast_end_date,
        sum(expected_days_in_bucket) as target_days_to_staffable
    from forecast_details
    group by funnel_id,forecast_start_date,forecast_end_date

),

expected_days_to_staffable_by_stage as (
    --for each funnel, bucket_name, start_date and end_date calculate the total expected days to get through the rest of the stages in the funnel
    --this will join to the final result of this model to give us the total expected days to complete the application based on the current stage bucket of the applications
    select
        forecast_details.funnel_id,
        forecast_details.bucket_name,
        forecast_details.expected_days_in_bucket,
        forecast_details.forecast_start_date,
        forecast_end_date,
        stages.funnel_order,
        coalesce(sum(expected_days_in_bucket) over (
            partition by forecast_details.funnel_id, forecast_start_date, forecast_end_date 
            order by funnel_order asc 
            rows between current row and unbounded following), 0) as expected_days_to_staffable
    from forecast_details
    left join stages
        on stages.funnel_id = forecast_details.funnel_id
        and stages.bucket_name = forecast_details.bucket_name
    where bucket_index = 1
),

bucket_transitions as (
    /*
    joining transitions with stage_mapping to get the bucket name for each transition, 
    and applying some business logic for use downstream. 
    */

    select
        transitions.fountain_applicant_id,
        transitions.stage_id,
        stages.bucket_name,
        stages.funnel_id,
        stages.funnel_order,
        transitions.created_at, --this is the date that the stage transition ocurred
        lead(created_at,1) over(partition by transitions.fountain_applicant_id,stages.funnel_id order by created_at asc) as next_transition_timestamp,
        --if the next transition is null then we are at the end of the funnel. therefore we use current date as the end date for the date_diff
        datediff(day,created_at,coalesce(next_transition_timestamp,current_date())) as days_to_next_transition,
        row_number() over(partition by transitions.fountain_applicant_id,stages.funnel_id,stages.bucket_name order by created_at asc) as bucket_transition_index,
        first_value(stages.bucket_name) over(partition by transitions.fountain_applicant_id,stages.funnel_id order by transitions.created_at desc) as current_stage_bucket,
        iff(current_stage_bucket in ({{ active_stage_buckets_string }}),true,false) as is_active_application,
        iff(current_stage_bucket in ({{ unsuccessful_stage_buckets_string }}),true,false) as is_unsuccessful_application,
        iff(current_stage_bucket in ({{ successful_stage_buckets_string }}),true,false) as is_successful_application,

        --applications can be put in rejected, on hold or inactive and then become active again. the application length for these tends to be longer 
        --so for some analyses we are going to want to filter those out. This flag allows us to do that
        max(iff(stages.bucket_name in ({{ fallout_stage_buckets_string }}),true,false)) over(partition by transitions.fountain_applicant_id,stages.funnel_id) as has_ever_been_fallout,
        
        --for each unsuccessful stage bucket get the timestamp of the most recent time that an application transitioned to the bucket
        --cleanest to do in this step of the funnel since things get more complicated with windows and where clauses in the next step
        {%- for stage in unsuccessful_stage_buckets %}
        max(case when bucket_name = '{{ stage }}' then transitions.created_at else null end) over(partition by fountain_applicant_id,stages.funnel_id) as most_recent_{{ stage }}_timestamp
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
        from transitions
            left join stages
            on stages.stage_id = transitions.stage_id
        where
            --filtering out null bucket_names so that they do not throw off window functions 
            bucket_name is not null
),

total_days_spent_in_stage_bucket as (
    --before we filter bucket_transitions in the bucket_transitions_filtered cte we need to calculate the total number of days spent in each bucket
    select
        fountain_applicant_id,
        funnel_id,
        bucket_name,
        sum(days_to_next_transition) as total_days_spent_in_bucket
    from bucket_transitions
    group by fountain_applicant_id,funnel_id,bucket_name
),

total_days_spent_in_stage_bucket_pivoted as (
    --pivoting rows into columns for total_days_spent_in_stage_bucket so that grain is one row per application and columns are the total number of days spent in each stage bucket
    select
        fountain_applicant_id,
        funnel_id,
        --for each stage bucket get the total number of days spent in the bucket
        {%- for stage in all_stage_buckets %}
        sum(iff(bucket_name = '{{ stage }}', total_days_spent_in_bucket, 0)) as days_spent_in_{{ stage }}_bucket
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    from total_days_spent_in_stage_bucket
    group by fountain_applicant_id,funnel_id
),

distinct_bucket_transitions as (
    --applying business logic to bucket_transitions_filtered to get the distinct bucket transitions for each application and the "look ahead" bucket names and timestamps

    select 
        fountain_applicant_id,
        bucket_transitions.funnel_id,
        bucket_transitions.stage_id,
        bucket_name,
        created_at,
        funnel_order,
        bucket_transition_index,
        has_ever_been_fallout,
        lead(bucket_transitions.created_at,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as next_bucket_timestamp_by_funnel_order,
        lead(bucket_transitions.created_at,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as next_bucket_timestamp_by_transition_timestamp,
        lead(bucket_transitions.bucket_name,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as next_bucket_name_by_funnel_order,
        lead(bucket_transitions.bucket_name,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as next_bucket_name_by_transition_timestamp,
        lead(bucket_transitions.funnel_order,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as next_bucket_funnel_order_by_funnel_order,
        lead(bucket_transitions.funnel_order,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as next_bucket_funnel_order_by_transition_timestamp,
        lag(bucket_transitions.created_at,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as prior_bucket_timestamp_by_funnel_order,
        lag(bucket_transitions.created_at,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as prior_bucket_timestamp_by_transition_timestamp,
        lag(bucket_transitions.bucket_name,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as prior_bucket_name_by_funnel_order,
        lag(bucket_transitions.bucket_name,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as prior_bucket_name_by_transition_timestamp,
        lag(bucket_transitions.funnel_order,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order asc) as prior_bucket_funnel_order_by_funnel_order,
        lag(bucket_transitions.funnel_order,1) ignore nulls over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by created_at asc) as prior_bucket_funnel_order_by_transition_timestamp,
        max(bucket_transitions.created_at) over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order desc) as max_bucket_timestamp_by_funnel_order,
        max(bucket_transitions.funnel_order) over(partition by bucket_transitions.fountain_applicant_id,bucket_transitions.funnel_id order by funnel_order desc) as max_bucket_funnel_order_by_funnel_order,
        most_recent_rejected_timestamp,
        most_recent_on_hold_timestamp,
        most_recent_inactive_timestamp
    from bucket_transitions
    where bucket_transition_index = 1
        and 
        /*
        if an application is currently active or is already approved we don't want to include rows with transitions into unsuccessful stages
        this is a nuance of the funnel mechanics in fountain. an application can be rejected then become active again.
        the purpose of excluding these rows is to prevent the most recent timestamp of an unsuccessful stage from being used
        as the timestamp of the most recent transition or timestamp of the next transition.
        this gives inaccurate pass through rates and drop out rates. this will become apparent later in the model.
        */
        not ((is_active_application or is_successful_application) and bucket_name in ({{ unsuccessful_stage_buckets_string }}))
),

highest_stage_reached as (

--this cte returns the highest stage that each application has reached. It was cleaner to do this in its own cte rather than in on the prior ctes
    select
        fountain_applicant_id,
        distinct_bucket_transitions.funnel_id,
        first_value(bucket_name) over(partition by fountain_applicant_id,funnel_id order by funnel_order desc) as highest_stage_reached
    from distinct_bucket_transitions
    where bucket_name not in ({{ unsuccessful_stage_buckets_string }})
    qualify row_number() over(partition by fountain_applicant_id,funnel_id order by funnel_order desc) = 1
),

highest_non_successful_stage_reached as (

    --this cte returns the non successful stage that each application has reached. It was cleaner to do this in its own cte rather than in on the prior ctes
    select
        fountain_applicant_id,
        distinct_bucket_transitions.funnel_id,
        first_value(bucket_name) over(partition by fountain_applicant_id,funnel_id order by funnel_order desc) as highest_non_successful_stage_reached,
        first_value(created_at) over(partition by fountain_applicant_id,funnel_id order by funnel_order desc) as highest_non_successful_stage_reached_timestamp
    from distinct_bucket_transitions
    where bucket_name not in ({{ unsuccessful_stage_buckets_string }}) and bucket_name not in ({{ successful_stage_buckets_string }})
    qualify row_number() over(partition by fountain_applicant_id,funnel_id order by funnel_order desc) = 1
    
),

bucket_entry_timestamps as (

    select
        distinct_bucket_transitions.fountain_applicant_id,
        distinct_bucket_transitions.funnel_id,
        /*
        for each active stage bucket get the timestamp of the first time that an application entered into the bucket but using the stage_bucket_transition_timestamp that we defined 
        the distinct_bucket_transitions cte

        The logic in this min call makes sure that stage timestamps are never out of order ie Readiness timestamp is before application timestamp
        */
        {%- for stage in active_and_successful_stage_buckets %} 
        min(case
              when bucket_name = '{{ stage }}' and max_bucket_timestamp_by_funnel_order < created_at
                then max_bucket_timestamp_by_funnel_order
              when bucket_name = '{{ stage }}' and next_bucket_timestamp_by_funnel_order < created_at
                   and next_bucket_name_by_funnel_order not in ({{ unsuccessful_stage_buckets_string }})
                then next_bucket_timestamp_by_funnel_order
              when bucket_name = '{{ stage }}' then created_at end) as first_entered_{{ stage }}_timestamp
        {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    from distinct_bucket_transitions
    left join highest_non_successful_stage_reached
        on distinct_bucket_transitions.fountain_applicant_id = highest_non_successful_stage_reached.fountain_applicant_id
        and distinct_bucket_transitions.funnel_id = highest_non_successful_stage_reached.funnel_id
    where bucket_name not in ({{ unsuccessful_stage_buckets_string }})
    group by distinct_bucket_transitions.fountain_applicant_id,distinct_bucket_transitions.funnel_id
),

bucket_entry_timestamps_cleaned as (
        --the logic in this CTE statement is there to handle situations where an application skips a stage bucket. if that happens, the stage(s) that are 
        --skipped will take the timestamp of the stage skipped to
        --when new stage buckets are added this needs to be updated. 
    select
        bucket_entry_timestamps.fountain_applicant_id,
        bucket_entry_timestamps.funnel_id,
        iff(first_entered_application_timestamp is null, coalesce(first_entered_administration_timestamp,first_entered_readiness_timestamp,first_entered_finalize_timestamp,first_entered_approved_timestamp,app_coaches.first_staffable_at),first_entered_application_timestamp) as first_entered_application_timestamp,
        iff(first_entered_administration_timestamp is null, coalesce(first_entered_readiness_timestamp,first_entered_finalize_timestamp,first_entered_approved_timestamp,app_coaches.first_staffable_at),first_entered_administration_timestamp) as first_entered_administration_timestamp,
        iff(first_entered_readiness_timestamp is null, coalesce(first_entered_finalize_timestamp,first_entered_approved_timestamp,app_coaches.first_staffable_at),first_entered_readiness_timestamp) as first_entered_readiness_timestamp,
        iff(first_entered_finalize_timestamp is null, coalesce(first_entered_approved_timestamp,app_coaches.first_staffable_at), first_entered_finalize_timestamp) as first_entered_finalize_timestamp,
        iff(first_entered_approved_timestamp is null, app_coaches.first_staffable_at,first_entered_approved_timestamp) as first_entered_approved_timestamp
    from bucket_entry_timestamps
    left join app_coaches
        on app_coaches.fountain_applicant_id = bucket_entry_timestamps.fountain_applicant_id
),

application_pass_through_fallout_flags as (

    --This cte is used to flag applications that have passed through or fallen out of each stage bucket
    select
        distinct_bucket_transitions.fountain_applicant_id,
        distinct_bucket_transitions.funnel_id,
        distinct_bucket_transitions.stage_id,
        distinct_bucket_transitions.bucket_name,
        distinct_bucket_transitions.funnel_order,
        distinct_bucket_transitions.created_at,
        distinct_bucket_transitions.bucket_transition_index,
        next_bucket_name_by_transition_timestamp,
        next_bucket_name_by_funnel_order,
        next_bucket_funnel_order_by_funnel_order,
        first_value(distinct_bucket_transitions.bucket_name) over(partition by distinct_bucket_transitions.fountain_applicant_id,distinct_bucket_transitions.funnel_id order by distinct_bucket_transitions.created_at desc) as current_stage_bucket,
        first_value(distinct_bucket_transitions.funnel_order) over(partition by distinct_bucket_transitions.fountain_applicant_id,distinct_bucket_transitions.funnel_id order by distinct_bucket_transitions.created_at desc) as current_stage_bucket_funnel_order,
        first_value(distinct_bucket_transitions.created_at) over(partition by distinct_bucket_transitions.fountain_applicant_id,distinct_bucket_transitions.funnel_id order by distinct_bucket_transitions.created_at desc) as last_stage_bucket_changed_timestamp,
        iff(current_stage_bucket in ({{ active_stage_buckets_string }}),true,false) as is_active_application,
        iff(current_stage_bucket in ({{ unsuccessful_stage_buckets_string }}),true,false) as is_unsuccessful_application,        
        has_ever_been_fallout,
        --fallout flags
        --for each active stage bucket return true if the stage transition represents a funnel exit for the application
        {%- for stage in active_stage_buckets %}
        iff(bucket_name = '{{ stage }}' and next_bucket_name_by_funnel_order in ({{ fallout_stage_buckets_string }}) and is_unsuccessful_application,true,false) as is_{{ stage }}_fallout,
        {%- endfor %}

        --pass through flags
        --for each active stage bucket return true if the stage transition represents a funnel progression for the application
        {%- for stage in active_stage_buckets %}
        iff((bucket_name = '{{ stage }}' or current_stage_bucket = 'approved') and next_bucket_funnel_order_by_funnel_order > funnel_order, true,false) as is_{{ stage }}_pass_through,
        {%- endfor %}
        
        distinct_bucket_transitions.most_recent_rejected_timestamp,
        distinct_bucket_transitions.most_recent_on_hold_timestamp,
        distinct_bucket_transitions.most_recent_inactive_timestamp,
        bucket_entry_timestamps_cleaned.first_entered_application_timestamp,
        bucket_entry_timestamps_cleaned.first_entered_administration_timestamp,
        bucket_entry_timestamps_cleaned.first_entered_readiness_timestamp,
        bucket_entry_timestamps_cleaned.first_entered_finalize_timestamp,
        bucket_entry_timestamps_cleaned.first_entered_approved_timestamp
    
    from distinct_bucket_transitions
    left join bucket_entry_timestamps_cleaned
        on distinct_bucket_transitions.fountain_applicant_id = bucket_entry_timestamps_cleaned.fountain_applicant_id
           and distinct_bucket_transitions.funnel_id = bucket_entry_timestamps_cleaned.funnel_id

),

applications as (

--this cte brings together all of the pieces up to this point and creates a one row per application per funnel dataset. this is the grain of the final model output
--everything after this just builds on top of this cte

    select 
        application_pass_through_fallout_flags.fountain_applicant_id,
        application_pass_through_fallout_flags.funnel_id,
        {{ dbt_utils.surrogate_key([' application_pass_through_fallout_flags.fountain_applicant_id','application_pass_through_fallout_flags.funnel_id']) }} as primary_key,
        current_stage_bucket,
        current_stage_bucket_funnel_order,
        --when we define the pass through rates and drop off rates we exclude on-hold applications. flagging this here 
        iff(current_stage_bucket != 'on_hold', true,false) as is_not_on_hold,
        highest_stage_reached,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_application_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_administration_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_readiness_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_finalize_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_approved_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_on_hold_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_rejected_bucket,
        total_days_spent_in_stage_bucket_pivoted.days_spent_in_inactive_bucket,
        count(distinct application_pass_through_fallout_flags.funnel_id) over(partition by application_pass_through_fallout_flags.fountain_applicant_id) as count_funnels,
        max(is_application_fallout) as is_application_fallout,
        max(is_administration_fallout) as is_administration_fallout,
        max(is_readiness_fallout) as is_readiness_fallout,
        max(is_finalize_fallout) as is_finalize_fallout,
        max(is_application_pass_through) as is_application_pass_through,
        max(is_administration_pass_through) as is_administration_pass_through,
        max(is_readiness_pass_through) as is_readiness_pass_through,
        max(is_finalize_pass_through) as is_finalize_pass_through,
        max(first_entered_application_timestamp) as first_entered_application_timestamp,
        max(first_entered_administration_timestamp) as first_entered_administration_timestamp,
        max(first_entered_readiness_timestamp) as first_entered_readiness_timestamp,
        max(first_entered_finalize_timestamp) as first_entered_finalize_timestamp,
        max(first_entered_approved_timestamp) as first_entered_approved_timestamp,
        max(most_recent_rejected_timestamp) as most_recent_rejected_timestamp,
        max(most_recent_on_hold_timestamp) as most_recent_on_hold_timestamp,
        max(most_recent_inactive_timestamp) as most_recent_inactive_timestamp,
        max(last_stage_bucket_changed_timestamp) as last_stage_bucket_changed_timestamp,
        max(is_active_application) as is_active_application,
        max(is_unsuccessful_application) as is_unsuccessful_application,
        max(has_ever_been_fallout) as has_ever_been_fallout
    from application_pass_through_fallout_flags
    left join highest_stage_reached
        on highest_stage_reached.fountain_applicant_id = application_pass_through_fallout_flags.fountain_applicant_id
        and highest_stage_reached.funnel_id = application_pass_through_fallout_flags.funnel_id
    left join total_days_spent_in_stage_bucket_pivoted
        on total_days_spent_in_stage_bucket_pivoted.fountain_applicant_id = application_pass_through_fallout_flags.fountain_applicant_id
        and total_days_spent_in_stage_bucket_pivoted.funnel_id = application_pass_through_fallout_flags.funnel_id
    {{ dbt_utils.group_by(n=15) }}
),

final as (

    --in this final select statement any additional logic is applied and CTEs are joined in
    select
        applications.fountain_applicant_id as fountain_application_id,
        applications.funnel_id,
        applications.primary_key,
        app_coaches.coach_profile_id,
        coach_profile_uuid,
        current_stage_bucket,
        --this is used to make the stage name look nice in BI layer
        initcap(current_stage_bucket) as current_stage_bucket_uppercase,
        current_stage_bucket_funnel_order,
        is_not_on_hold,
        highest_stage_reached,
        days_spent_in_application_bucket,
        days_spent_in_administration_bucket,
        days_spent_in_readiness_bucket,
        days_spent_in_finalize_bucket,
        days_spent_in_approved_bucket,
        days_spent_in_on_hold_bucket,
        days_spent_in_rejected_bucket,
        days_spent_in_inactive_bucket,
        count_funnels,
        --flagging auto-rejected applications as application_fallout. 
        iff(is_application_fallout or (current_stage_bucket in ({{ fallout_stage_buckets_string }}) and highest_stage_reached is null),true,false) as is_application_fallout,
        is_administration_fallout,
        is_readiness_fallout,
        is_finalize_fallout,
        is_application_pass_through,
        is_administration_pass_through,
        is_readiness_pass_through,
        is_finalize_pass_through,
        coalesce(first_entered_application_timestamp,first_entered_administration_timestamp) as first_entered_application_timestamp,
        first_entered_administration_timestamp,
        first_entered_readiness_timestamp,
        first_entered_finalize_timestamp,
        first_entered_approved_timestamp,
        most_recent_rejected_timestamp,
        most_recent_on_hold_timestamp,
        most_recent_inactive_timestamp,
        last_stage_bucket_changed_timestamp,
        is_active_application,
        is_unsuccessful_application,
        iff(current_stage_bucket in ({{ fallout_stage_buckets_string }}),true,false) as is_currently_fallout_application,
        iff(current_stage_bucket in ({{ fallout_stage_buckets_string }}),highest_stage_reached,null) as highest_stage_reached_before_fallout,
        iff(current_stage_bucket in ({{ fallout_stage_buckets_string }}),current_stage_bucket,null) as fallout_stage,
        iff(current_stage_bucket in ({{ fallout_stage_buckets_string }}),last_stage_bucket_changed_timestamp,null) as fallout_timestamp,
        coalesce(has_ever_been_fallout or application_auxillary_details.has_ever_been_legacy_inactive,false) as has_ever_been_fallout,
        coalesce(application_auxillary_details.has_ever_been_overflow_on_hold,false) as has_ever_been_overflow_on_hold,
        app_coaches.first_staffable_at,
        /*
        Once a coach has an approved application and they are onboarded to BU app they should not use Fountain to re-apply to additional coaching positions.
        There is a seperate process that happens outside of fountain for onboarded coaches to apply to other coaching types (ie Care, specialists).
        The is_reonboard_application flags these applications.
        */
        iff(app_coaches.first_staffable_at < first_entered_application_timestamp,true,false) as is_reonboard_application,

        --first_staffable_at is used to flag a staffable application. this is more "real" to the business than using the fountain approved stage timestamp
        iff(not is_reonboard_application and first_staffable_at is not null, true,false) as is_staffable_application,
        --datediffs
        datediff(day,first_entered_application_timestamp,first_entered_administration_timestamp) as days_between_entering_application_and_entering_administration,
        datediff(day,first_entered_administration_timestamp,first_entered_readiness_timestamp) as days_between_entering_administration_and_entering_readiness,
        datediff(day,first_entered_readiness_timestamp,first_entered_finalize_timestamp) as days_between_entering_readiness_and_entering_finalize,
        datediff(day,first_entered_finalize_timestamp,first_entered_approved_timestamp) as days_between_entering_finalize_and_entering_approved,
        datediff(day,first_entered_application_timestamp,first_entered_approved_timestamp) as days_between_entering_application_and_entering_approved,
        datediff(day,first_entered_approved_timestamp,first_staffable_at) as days_between_entering_approved_and_first_staffable_date,
        datediff(day,first_entered_application_timestamp,most_recent_rejected_timestamp) as days_between_entering_application_and_rejected,
        datediff(day,first_entered_application_timestamp,most_recent_inactive_timestamp) as days_between_entering_application_and_inactive,
        datediff(day,first_entered_application_timestamp,most_recent_on_hold_timestamp) as days_between_entering_application_and_on_hold,
        datediff(day,first_entered_application_timestamp,app_coaches.first_staffable_at) as days_between_entering_application_first_staffable_date,
        datediff(day,last_stage_bucket_changed_timestamp,current_date()) as days_in_current_stage_bucket,
        datediff(day,first_entered_application_timestamp,current_date()) as days_since_application_started,
        /*
        for days to hire we exclude any applicants that have ever been fallout and subtract any days that they spent in waitlist.
        applications that have been fallout and go on to become staffable usually have some unusual circumstances and skew the days to hire average way up
        days spent in waitlist is excluded since it isn't really part of the time that we spend getting an applicant through the application process
        */
        iff(not(has_ever_been_fallout or has_ever_been_legacy_inactive), 
            datediff(day,first_entered_application_timestamp,app_coaches.first_staffable_at) - coalesce(days_spent_in_waitlist,0), null) as days_to_hire,

        --forecast logic
        target_days_to_staffable.target_days_to_staffable,
        dateadd(day, target_days_to_staffable, first_entered_application_timestamp) as target_staffable_date,
        iff(first_staffable_at > target_staffable_date,true,false) as is_past_target_staffable_date,
        datediff(day,first_staffable_at,target_staffable_date) as days_between_actual_staffable_date_and_target_staffable_date,
        expected_days_to_staffable_by_stage.expected_days_in_bucket as target_days_in_current_bucket,
        expected_days_to_staffable_by_stage.expected_days_to_staffable,

        --expected_staffable_dates
        dateadd(day,expected_days_to_staffable,current_date()) as expected_staffable_date_basic,
        iff(target_days_in_current_bucket > days_in_current_stage_bucket,
            --if an applicaiton has been in its current stage bucket for less days than the target for that bucket
            -- we want to subtract this date difference from the expected staffable date
            dateadd(day,-(days_in_current_stage_bucket),expected_staffable_date_basic),
            expected_staffable_date_basic) as expected_staffable_date,
        application_auxillary_details.* exclude(primary_key,fountain_applicant_id,funnel_id,has_ever_been_overflow_on_hold)
            
    from applications
    left join app_coaches
        on app_coaches.fountain_applicant_id = applications.fountain_applicant_id
    left join target_days_to_staffable
        on applications.funnel_id = target_days_to_staffable.funnel_id
        and applications.first_entered_application_timestamp between 
        target_days_to_staffable.forecast_start_date and target_days_to_staffable.forecast_end_date
    left join expected_days_to_staffable_by_stage
        on expected_days_to_staffable_by_stage.funnel_id = applications.funnel_id
        and expected_days_to_staffable_by_stage.bucket_name = applications.current_stage_bucket
        and applications.first_entered_application_timestamp between 
        expected_days_to_staffable_by_stage.forecast_start_date and expected_days_to_staffable_by_stage.forecast_end_date 
    left join application_auxillary_details
        on application_auxillary_details.fountain_applicant_id = applications.fountain_applicant_id
        and application_auxillary_details.funnel_id = applications.funnel_id
    --there are handful duplicate fountain_applicant_ids in app_coaches. using a qualify to dedupe, taking the min staffable date
    where applications.first_entered_application_timestamp is not null
    qualify row_number() over(partition by applications.primary_key order by app_coaches.first_staffable_at asc) = 1)

select * from final
