WITH invited AS (

  SELECT * FROM {{ ref('dbt_events__invited') }}

),

invite_flags AS (

  SELECT
    member_id,
    associated_record_id,
    IFF(event_object = 'care_product', TRUE, FALSE) AS is_new_care_invite,
    IFF(event_object = 'coaching_circles_product', TRUE, FALSE) AS is_new_coaching_circles_invite,
    IFF(event_object = 'extended_network_product', TRUE, FALSE) AS is_new_extended_network_invite,
    IFF(event_object = 'foundations_product', TRUE, FALSE) AS is_new_foundations_invite,
    IFF(event_object = 'on_demand_product', TRUE, FALSE) AS is_new_on_demand_invite,
    IFF(event_object = 'primary_coaching_product', TRUE, FALSE) AS is_new_primary_coaching_invite,
    IFF(event_object = 'workshops_product', TRUE, FALSE) AS is_new_workshops_invite
  FROM invited

),

grouped as (
  SELECT
    member_id,
    associated_record_id AS product_subscription_assignment_id,
    BOOLOR_AGG(is_new_care_invite) AS is_new_care_invite,
    BOOLOR_AGG(is_new_coaching_circles_invite) AS is_new_coaching_circles_invite,
    BOOLOR_AGG(is_new_extended_network_invite) AS is_new_extended_network_invite,
    BOOLOR_AGG(is_new_foundations_invite) AS is_new_foundations_invite,
    BOOLOR_AGG(is_new_on_demand_invite) AS is_new_on_demand_invite,
    BOOLOR_AGG(is_new_primary_coaching_invite) AS is_new_primary_coaching_invite,
    BOOLOR_AGG(is_new_workshops_invite) AS is_new_workshops_invite
  FROM invite_flags
  GROUP BY 1,2
),

final as (
  select {{ dbt_utils.surrogate_key(['member_id', 'product_subscription_assignment_id']) }} as invited_pivoted_id,
  *
  from grouped
)

select *
from final
