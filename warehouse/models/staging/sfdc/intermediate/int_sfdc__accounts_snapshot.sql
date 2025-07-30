{%- set zoominfo_technology_fields = [
  "zoominfo_crm_software",
  "zoominfo_customer_experience_systems",
  "zoominfo_customer_feedback_systems",
  "zoominfo_email_hosting_systems",
  "zoominfo_erp_software",
  "zoominfo_filesharing_systems",
  "zoominfo_hr_software",
  "zoominfo_lms_software",
  "zoominfo_operating_systems",
  "zoominfo_other_comm_collab_systems",
  "zoominfo_other_hr_systems",
  "zoominfo_other_it_systems",
  "zoominfo_team_collaboration",
  "zoominfo_business_process_systems"
] -%}

with accounts_history as (

  select * from  {{ ref('stg_sfdc__accounts_snapshot') }}

),

sfdc_users_snapshot_current_version as (

  select * from {{ ref('int_sfdc__users_snapshots') }}
  where is_current_version
  --this brings in the most recent salesforce user snapshot.
  --columns joined in from this table will be suffixed with _current_version

),

sfdc_users_snapshot as (

  select * from {{ ref('int_sfdc__users_snapshots') }}

),

dated_conversion_rates as (

  select * from {{ ref('stg_sfdc__dated_conversion_rates') }}

),

record_types as (
    select * from {{ ref('stg_sfdc__record_types') }}
),

usd_currency_conversions as (

  select 
    a.history_primary_key,
    a.previous_quarter_carr_unconverted / r.conversion_rate as previous_quarter_carr_usd,
    a.annual_account_revenue_zoominfo_unconverted / r.conversion_rate as annual_account_revenue_zoominfo_usd
  from accounts_history a
  left join dated_conversion_rates r
    on r.iso_code = a.currency_iso_code and r.conversion_rate_period_sequence = 1
)

select 
  a.*,

  --concatenating the zoominfo technology fields into a single field
  {% set zoominfo_fields_concatenated = "" %}
  {%- for field in zoominfo_technology_fields -%}
    {%- if not loop.first -%} 
        || (case when {{ field }} is not null then '; ' else '' end) ||
    {%- endif -%}
    coalesce({{ field }},'')
  {%- endfor %}
  as zoominfo_technology_fields_concatenated_raw,

  --converting empty strings to null
  nullif(zoominfo_technology_fields_concatenated_raw,'') as zoominfo_technology_fields_concatenated,
  curr.previous_quarter_carr_usd,
  curr.annual_account_revenue_zoominfo_usd,
  record_types.record_type_name,

  --related user details for most recent user snapshot
  account_owner.name as account_owner_name_current_version, 
  account_owner.user_role_name as account_owner_role_current_version,
  account_csm.name as account_csm_name_current_version,
  account_csm.user_role_name as account_csm_role_current_version,
  betterup_executive_sponsor.name as betterup_executive_sponsor_name,
  betterup_executive_sponsor.user_role_name as betterup_executive_sponsor_role

from 
accounts_history a
left join usd_currency_conversions curr
  on curr.history_primary_key = a.history_primary_key

left join sfdc_users_snapshot_current_version account_owner
  on a.account_owner_id = account_owner.sfdc_user_id

left join sfdc_users_snapshot_current_version account_csm
  on a.account_csm_id = account_csm.sfdc_user_id

left join sfdc_users_snapshot betterup_executive_sponsor
  on a.betterup_executive_sponsor_id = betterup_executive_sponsor.sfdc_user_id
  and(
    (a.valid_from > betterup_executive_sponsor.valid_to
    and a.valid_to < betterup_executive_sponsor.valid_from)
    or a.valid_to is null and betterup_executive_sponsor.valid_to is null)

left join record_types
    on a.sfdc_record_type_id = record_types.sfdc_record_type_id

where a.sfdc_account_id != '00150000025c8DOAAY' --this is a test account in salesforce
