{{
  config(
    tags=["eu"]
  )
}}

with messages as (

    select * from {{ ref('stg_app__messages') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'Message'
),

coach_assignments as (

    select * from {{ ref('int_app__coach_assignments') }}

),

member_events as (

    select * from {{ ref('fact_member_events') }}

),

final as (

    select
        -- fields from staging
        m.message_id,
        m.created_at,
        m.updated_at,
        m.attachment_status,
        m.body,
        m.client_uuid,
        m.coach_assignment_id,
        m.created_from,
        m.read_by_recipient_at,
        m.recipient_id,
        m.sender_id,
        m.conversation_participant_id,
        m.generated_message_id,

        -- joined fields
        ca.role,
        me.event_object AS message_type
    from messages m
    left join deleted_records dr
        on m.message_id = dr.item_id
    left join coach_assignments ca
        on m.coach_assignment_id = ca.coach_assignment_id
    left join member_events me
        on m.message_id = me.associated_record_id and
            me.associated_record_type = 'Message' and
            me.event_object != 'message'
    where
        -- remove destroyed records
        dr.item_id is null
)

select * from final