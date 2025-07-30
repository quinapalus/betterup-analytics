{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH sfdc_contacts AS (

  SELECT * FROM {{ref('stg_sfdc__contacts')}}

),

dbt_user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

),

dbt_members AS (

  SELECT * FROM {{ref('dbt_members')}}

),

dbt_partners AS (

  SELECT * FROM {{ref('dbt_partners')}}

),

organizations AS (

  SELECT * FROM {{ref('stg_app__organizations')}}

),

sfdc_accounts AS (

  SELECT * FROM {{ref('stg_sfdc__accounts')}}

)

SELECT
      -- Salesforce fields:
      c.sfdc_contact_id,
      u.email AS app_email,
      coalesce(u.first_name,{{ get_first_name_from_email('u.email') }}) AS app_first_name,
      coalesce(u.last_name,{{ get_last_name_from_email('u.email') }}) AS app_last_name,
      c.is_current_member AS sfdc_is_current_member,
      c.is_past_member AS sfdc_is_past_member,
      c.is_program_admin AS sfdc_is_program_admin,
      c.sfdc_account_id,
      a.account_type AS sfdc_account_type,
       -- fetch deleted status of the record to know if contact has to be created in Salesforce
      coalesce(c.is_deleted,false) AS sfdc_is_deleted,
      -- Application fields:
      coalesce(m.is_current_member,false) AS app_is_current_member,
      coalesce(m.is_current_member = false, false) AS app_is_past_member,
      p.partner_id IS NOT NULL AS app_is_program_admin,
      -- mark Contacts where Salesforce field values differ from app
      -- ie. records that need to be updated in Salesforce
      coalesce(((coalesce(m.is_current_member, false)) <> c.is_current_member)
        OR ((coalesce(m.is_current_member = false, false)) <> c.is_past_member)
        OR ((p.partner_id IS NOT NULL) <> c.is_program_admin), true)
        AS values_differ_from_app,
       -- SFDC account ID taken from BUApp Organizations object
       o.sfdc_account_id AS app_sfdc_account_id,
       --- To check if User is deactivated
       u.deactivated_at IS NOT NULL AS app_is_deactivated,
       u.confirmed_at IS NOT NULL AS app_is_confirmed
    FROM dbt_user_info AS u
    -- match Salesforce Contacts to User accounts on email address
    -- also fetch user accounts that are not in SFDC -- used in contacts_updater.py
    LEFT OUTER JOIN sfdc_contacts AS c
      ON c.email = u.email
    -- match Contacts to Member and Partner roles on user_id, if applicable
    LEFT OUTER JOIN dbt_members AS m
      ON u.user_id = m.member_id
    LEFT OUTER JOIN dbt_partners AS p
      ON u.user_id = p.partner_id
    LEFT OUTER JOIN organizations AS o
      ON u.organization_id = o.organization_id
    LEFT OUTER JOIN sfdc_accounts AS a
      ON c.sfdc_account_id = a.sfdc_account_id