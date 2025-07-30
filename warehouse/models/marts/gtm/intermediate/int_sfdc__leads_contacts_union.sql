{{
  config(
    tags=['run_test_true']
  )
}}

with leads as (
    select * from {{ ref('stg_sfdc__leads_snapshot') }}
),

contacts as (
    select * from {{ ref('stg_sfdc__contacts_snapshot') }}

),

accounts_current_records as (
    select * from {{ ref('int_sfdc__accounts_snapshot') }} where is_current_version = true
),
--this creates a combined dimension for lead and contact records

final as (
    --leads
    select
        --ids
        sfdc_lead_id as sfdc_person_id,
        lead_owner_id as person_owner_id,
        lead_created_by_id as person_created_by_id,
        sfdc_account_id,
        sfdc_record_type,

        --categorical and text attributes
        first_name,
        last_name,
        company_name,
        email,
        email_domain,
        job_title,
        person_country,
        person_state,
        lead_status as person_status,
        booking_status,
        content_syndication_asset_download,
        role_level,
        company_size,
        job_function,
        first_conversion,
        recent_conversion,
        prospect_type,
        latest_source,
        latest_source_drill_down_1,
        latest_source_drill_down_2,
        original_source,
        original_source_drill_down_1,
        original_source_drill_down_2,
        currently_active_outreach_sequence,
        current_sequence_step,
        current_sequence_step_type,
        current_sequence_status,
        current_sequence_task_due_date,
        finished_sequences,
        number_of_active_tasks,
        number_of_active_sequences,

        person_score,
        mql_score,
        behavioral_score,
        demographic_score,

        --booleans
        has_opted_out_of_email,

        --dates and timestamps
        created_at,
        last_modified_at,
        first_conversion_date,
        recent_conversion_date,

        --other
        leads.is_deleted,
        valid_from,
        valid_to,
        is_current_version,
        version,
        is_last_snapshot_of_day

    from leads
    where
        is_deleted = false
        and is_converted = false
        and coalesce(lower(company_name),'unknown') not like '%betterup%'
        and coalesce(lower(email),'Unknown') not like '%betterup%'

    union all

    --contacts
    select
        --ids
        sfdc_contact_id as sfdc_person_id,
        contact_owner_id as person_owner_id,
        contact_created_by_id as person_created_by_id,
        contacts.sfdc_account_id as sfdc_account_id,
        sfdc_record_type,

        --categorical and text attributes
        first_name,
        last_name,
        a.account_name as company_name,
        email,
        email_domain,
        job_title,
        person_country,
        person_state,
        contact_status as person_status,
        booking_status,
        content_syndication_asset_download,
        role_level,
        contacts.company_size,
        job_function,
        first_conversion,
        recent_conversion,
        prospect_type,
        latest_source,
        latest_source_drill_down_1,
        latest_source_drill_down_2,
        original_source,
        original_source_drill_down_1,
        original_source_drill_down_2,
        currently_active_outreach_sequence,
        current_sequence_step,
        current_sequence_step_type,
        current_sequence_status,
        current_sequence_task_due_date,
        finished_sequences,
        number_of_active_tasks,
        number_of_active_sequences,
        person_score,
        mql_score,
        behavioral_score,
        demographic_score,

        --booleans
        has_opted_out_of_email,

        --dates and timestamps
        contacts.created_at,
        contacts.last_modified_at,
        first_conversion_date,
        recent_conversion_date,

        --other
        contacts.is_deleted,
        contacts.valid_from,
        contacts.valid_to,
        contacts.is_current_version,
        contacts.version,
        contacts.is_last_snapshot_of_day

    from contacts
    inner join accounts_current_records a
        on a.sfdc_account_id = contacts.sfdc_account_id
    where
        contacts.is_deleted = false
        and coalesce(lower(company_name),'unknown') not like '%betterup%'
        and coalesce(lower(email),'unknown') not like '%betterup%'
)

select
    {{ dbt_utils.surrogate_key(['sfdc_person_id', 'valid_to', 'valid_from']) }} AS primary_key,
    *
from final