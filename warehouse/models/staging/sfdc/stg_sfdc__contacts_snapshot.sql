{{
  config(
    tags=['classification.c2_restricted']
  )
}}

with contact_snapshot as (

    select * from {{ ref('snapshot_sfdc_contacts') }}

)

select 
    --ids
    id as sfdc_contact_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
    owner_id as contact_owner_id,
    created_by_id as contact_created_by_id,
    account_id as sfdc_account_id,
    'contact' as sfdc_record_type,

    --categorical and text attributes
    first_name,
    last_name,
    email,
    split_part(email, '@', 2) as email_domain,
    title as job_title,
    mailing_country as person_country,
    mailing_state as person_state,
    contact_status_c as contact_status,
    booking_status_cp_c as booking_status,
    role_level_c as role_level,
    company_size_c as company_size,
    job_function_mkto_c as job_function,
    first_conversion_c as first_conversion,
    recent_conversion_c as recent_conversion,
    prospect_type_c as prospect_type,
    latest_source_c as latest_source,
    latest_source_drill_down_1_c as latest_source_drill_down_1,
    latest_source_drill_down_2_c as latest_source_drill_down_2,
    original_source_c as original_source,
    original_source_drill_down_1_c as original_source_drill_down_1,
    original_source_drill_down_2_c as original_source_drill_down_2,
    content_syndication_asset_download_c as content_syndication_asset_download,

    --outreach fields
    current_active_sequence_c as currently_active_outreach_sequence,
    current_sequence_step_number_c as current_sequence_step,
    current_sequence_step_type_c as current_sequence_step_type,
    current_sequence_status_c as current_sequence_status,
    {{ environment_varchar_to_timestamp('current_sequence_task_due_date_c','current_sequence_task_due_date') }},
    finished_sequences_c as finished_sequences,
    number_of_active_tasks_c as number_of_active_tasks,
    number_of_active_sequences_c as number_of_active_sequences,

    mkto_71_lead_score_c as person_score,
    surge_score_c as mql_score,
    behavioral_score_c as behavioral_score,
    demographic_score_c as demographic_score,

    --quantities
    
    --boooleans
    has_opted_out_of_email,
  
    --dates and timestamps
    {{ environment_varchar_to_timestamp('created_date','created_at') }},
    {{ environment_varchar_to_timestamp('last_modified_date','last_modified_at') }},
    first_conversion_date_c::date as first_conversion_date,
    recent_conversion_date_c::date as recent_conversion_date,

    --other
    is_deleted,
    {{ environment_varchar_to_timestamp('dbt_valid_from','valid_from') }},
    {{ environment_varchar_to_timestamp('dbt_valid_to','valid_to') }},
    dbt_valid_to is null as is_current_version,
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,

    case when
      row_number() over(
        partition by id,date_trunc('day',valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day

from contact_snapshot c 
where coalesce(c.account_id,'Unknown') != '00150000025c8DOAAY' --this is a test account in salesforce
