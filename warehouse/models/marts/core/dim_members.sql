{{
  config(
    tags=['eu']
  )
}}

with users as (
  select * from {{ ref('int_app__users') }}
),

users_roles as (
  select * from {{ ref('int_app__users_roles') }}
),

roles as (
  select * from {{ ref('stg_app__roles') }}
),

member_profiles as (
  select * from {{ ref('stg_app__member_profiles') }}
),

assessments as (
  select * from {{ ref('int_app__assessments') }}
),

member_lifetime_tracks_reporting_groups as (
  select * from {{ ref('int_member_lifetime_tracks_reporting_groups') }}
),

------Building Redundacies with DIM_MEMBER -------------------
int_member__legacy as (
  select
    *
  from {{ ref('int_member__legacy')}}
),

joined_attribution as (
    select * from {{ ref('int_user_attribution_attributes')}}
),

{% if env_var('DEPLOYMENT_ENVIRONMENT','') != 'US Gov' %}
members_on_converged_platform as (
  select distinct
    user_id,
    is_on_converged_platform
  from {{ ref('stg_segment_backend__identifies') }}
  where is_on_converged_platform
  and context_library_name is not null
),
{% endif %}

member_users as (
  select u.*
  from users u
  where exists (
    select 1
    from users_roles ur
    inner join roles r on r.role_id = ur.role_id
    where ur.user_id = u.user_id
    and r.name IN ('care', 'member')
  )
),

member_level as (
  select
    member_profile_id,
    people_manager,
    manages_other_managers,
    next_member_nps_at,
      case
        when not PEOPLE_MANAGER then 'individual-contributor'
        when PEOPLE_MANAGER and not MANAGES_OTHER_MANAGERS then 'frontline-manager'
        -- There are some records wherepeople_manager is false but manages_other_managers is true, assuming this is the case of bad user entry
        when MANAGES_OTHER_MANAGERS then 'manager-of-managers'
        end as level
  from member_profiles
),

job_function as (
  -- pull in self-reported job function from most recent onboading assessment for each member
  select
    user_id as member_id,
    responses:"job_function"::varchar as job_function
  from assessments
  where is_onboarding
  and responses:"job_function" is not null
  qualify row_number() over (partition by user_id order by submitted_at desc) = 1
),

has_previously_received_coaching as (
  -- pull in response to previous_coaching from most recent onboading assessment for each member
  select
    user_id as member_id,
    case when responses:"previous_coaching"::int between 0 and 1 
      then responses:"previous_coaching"::boolean
      else FALSE end as has_previously_received_coaching 
      --this somewhat unusual logic was added b/c we had some bad test data in the Fed
      --staging environment.  Ideally, we'd fix the data rather than add code like this to 
      --address it, but we needed to move fast.  We can remove this later when the data 
      --is fixed. Bad data examples are -1 or 2 as a response to "previous_coaching"
  from assessments
  where is_onboarding
  and responses:"previous_coaching" is not null
  qualify row_number() over (partition by user_id order by submitted_at desc) = 1
),

final as (

  select

  --primary key
  m.user_id as member_id,

  --foreign keys
  m.manager_id,
  m.inviter_id,
  m.next_session_id,
  m.organization_id,
  m.member_profile_id,

  --logical data
  m.email,
  m.first_name,
  m.last_name,
  m.first_name || ' ' || m.last_name as name,
  m.title,
  m.motivation,
  m.language,
  m.coaching_language,
  jf.job_function,
  m.state,
  m.time_zone,
  m.tz_iana,
  m.country_code,
  m.country_name,
  m.subregion_m49,
  m.geo,
  ml.level,
  m.upfront_subscription_state,
  l.lifetime_track_ids,
  l.lifetime_track_names,
  l.lifetime_reporting_group_ids,
 
  --booleans
  {% if env_var('DEPLOYMENT_ENVIRONMENT','') != 'US Gov' %}
  coalesce(i.is_on_converged_platform, FALSE) as is_on_converged_platform,
  {% endif %}
  m.is_call_recording_allowed,
  m.confirmed_at is not null as is_activated, -- definition of activated since June 2019
  m.confirmed_at is not null as is_confirmed,
  m.completed_member_onboarding_at is not null as is_onboarded,
  rc.has_previously_received_coaching,
  m.is_sso_preferred,
  
  --timestamps
  m.created_at,
  m.confirmed_at,
  m.confirmed_at as activated_at, -- definition of activated since June 2019
  m.care_confirmed_at,
  m.lead_confirmed_at,
  m.confirmation_sent_at,
  m.completed_member_onboarding_at,
  m.deactivated_at,
  m.last_engaged_at,
  m.last_usage_at,
  m.updated_at,

  -- DIM_USER REDUNDACIES---- 
  -- This allows us to repoint all looker explores and reports to dim_members and away from dim_user.
  ---marketing attribution
  ----first touch
  joined_attribution.first_touch_utm_source,
  joined_attribution.first_touch_utm_campaign,
  joined_attribution.first_touch_utm_medium,
  joined_attribution.first_touch_utm_content,
  joined_attribution.first_touch_channel_attribution,

  ----last touch
  joined_attribution.last_touch_utm_source,
  joined_attribution.last_touch_utm_campaign,
  joined_attribution.last_touch_utm_medium,
  joined_attribution.last_touch_utm_content,
  joined_attribution.last_touch_channel_attribution,
  
  -- DIM_MEMBER REDUNDACIES---- 
  -- This allows us to repoint all looker explores and reports to dim_members and away from dim_member.
  int_member__legacy.member_key,
  int_member__legacy.app_member_id,
  int_member__legacy.member_geo,
  int_member__legacy.member_subregion_m49,
  int_member__legacy.employee_level,
  --16 HCM attributes
  int_member__legacy.member_hcm_attribute_work_location,
  int_member__legacy.member_hcm_attribute_city,
  int_member__legacy.member_hcm_attribute_country,
  int_member__legacy.member_hcm_attribute_region,
  int_member__legacy.member_hcm_attribute_job_family,
  int_member__legacy.member_hcm_attribute_job_code,
  int_member__legacy.member_hcm_attribute_cost_center,
  int_member__legacy.member_hcm_attribute_business_unit,
  int_member__legacy.member_hcm_attribute_group,
  int_member__legacy.member_hcm_attribute_market,
  int_member__legacy.member_hcm_attribute_employee_type,
  int_member__legacy.member_hcm_attribute_department,
  int_member__legacy.member_hcm_attribute_level,
  int_member__legacy.member_hcm_attribute_is_manager,
  int_member__legacy.member_hcm_attribute_brand,
  int_member__legacy.member_hcm_attribute_gender

  from member_users as m
  left join member_level as ml
    on m.member_profile_id = ml.member_profile_id
  left join job_function as jf
    on m.user_id = jf.member_id
  left join has_previously_received_coaching rc
    on m.user_id = rc.member_id
  {% if env_var('DEPLOYMENT_ENVIRONMENT','') != 'US Gov' %}
  left join members_on_converged_platform as i
    on to_varchar(m.user_id) = i.user_id
  {% endif %}
  left join member_lifetime_tracks_reporting_groups l
    on m.user_id = l.member_id
  left join joined_attribution
    on m.user_id = joined_attribution.user_id
  left join int_member__legacy
    on int_member__legacy.app_member_id = m.user_id
)

select * from final