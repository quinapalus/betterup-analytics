{{
  config(
    tags=["eu"]
  )
}}

WITH users AS (
  SELECT * FROM {{ ref('int_app__users') }}
), tracks AS (
  SELECT * FROM {{ ref('dim_tracks') }}
), organizations AS (
  SELECT * FROM {{ ref('stg_app__organizations') }}
), roles AS (
  SELECT * FROM {{ ref('stg_app__roles') }}
), users_roles AS (
  SELECT * FROM {{ ref('stg_app__users_roles') }}
), partner_ids AS (
  SELECT u.user_id FROM users AS u
    JOIN users_roles AS ur ON ur.user_id = u.user_id
    JOIN roles AS r ON r.role_id = ur.role_id
  WHERE r.name = 'partner'
),

final as (

    SELECT u.user_id,
           t.track_id,
           (u.confirmed_at IS NOT NULL AND u.deactivated_at IS NULL) AS is_active,
           (pi.user_id IS NOT NULL) AS is_current_partner
    FROM users AS u
           JOIN users_roles AS ur ON ur.user_id = u.user_id
           JOIN roles AS r ON r.role_id = ur.role_id
           JOIN tracks AS t ON t.track_id = r.resource_id
           LEFT OUTER JOIN partner_ids AS pi ON pi.user_id = u.user_id
    WHERE r.name = 'track_admin'
      AND r.resource_type = 'Track'
      AND t.deployment_type NOT IN ('trial', 'trial_smb', 'trial_care', 'care')

    UNION

    SELECT u.user_id,
           t.track_id,
           (u.confirmed_at IS NOT NULL AND u.deactivated_at IS NULL) AS is_active,
           (pi.user_id IS NOT NULL) AS is_current_partner
    FROM users AS u
           JOIN users_roles AS ur ON ur.user_id = u.user_id
           JOIN roles AS r ON r.role_id = ur.role_id
           JOIN organizations AS o ON o.organization_id = r.resource_id
           JOIN tracks AS t ON t.organization_id = o.organization_id
           LEFT OUTER JOIN partner_ids AS pi ON pi.user_id = u.user_id
    WHERE r.name = 'track_admin'
      AND r.resource_type = 'Organization'
      AND t.deployment_type NOT IN ('trial', 'trial_smb', 'trial_care', 'care')
)

select
  {{ dbt_utils.surrogate_key(['user_id', 'track_id']) }} as primary_key,
  *
from final
