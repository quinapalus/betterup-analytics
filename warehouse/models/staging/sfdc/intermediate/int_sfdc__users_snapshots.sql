with users_history as (

  select * from {{ ref('stg_sfdc__users_snapshot') }}

),

user_roles as (

  select * from {{ ref('stg_sfdc__user_roles') }}

),

marketing_team_rollup as (

    select * from {{ ref('marketing_team_rollup') }}

)

select 
    u.*,
    ur.role_name as user_role_name,
    coalesce(marketing_team_rollup.marketing_team_rollup,'Undefined') as marketing_team_rollup,

    manager.name as manager_name,
    manager_user_role.role_name as manager_role_name,

    assigned_sdr.name as assigned_sdr_name,
    assigned_sdr_user_role.role_name as assigned_sdr_role_name,

    vp.name as vp_name,
    vp_user_role.role_name as vp_role_name,

    rvp.name as rvp_name,
    rvp_user_role.role_name as rvp_role_name


from users_history u

left join user_roles ur 
    on ur.sfdc_user_role_id = u.user_role_id 

left join marketing_team_rollup
    on marketing_team_rollup.role_id = ur.sfdc_user_role_id

left join users_history manager
    on manager.sfdc_user_id = u.manager_id
    and(
    (u.valid_from > manager.valid_to
    and u.valid_to < manager.valid_from)
    or u.valid_to is null and manager.valid_to is null)
left join user_roles manager_user_role
    on manager_user_role.sfdc_user_role_id = manager.user_role_id

left join users_history rvp
    on rvp.sfdc_user_id = u.rvp_id
    and(
    (u.valid_from > rvp.valid_to
    and u.valid_to < rvp.valid_from)
    or u.valid_to is null and rvp.valid_to is null)
left join user_roles rvp_user_role
    on rvp_user_role.sfdc_user_role_id = rvp.user_role_id
    

left join users_history assigned_sdr
    on assigned_sdr.sfdc_user_id = u.assigned_sdr_id
    and(
    (u.valid_from > assigned_sdr.valid_to
    and u.valid_to < assigned_sdr.valid_from)
    or u.valid_to is null and assigned_sdr.valid_to is null)
left join user_roles assigned_sdr_user_role
    on assigned_sdr_user_role.sfdc_user_role_id = assigned_sdr.user_role_id

left join users_history vp
    on vp.sfdc_user_id = u.vp_id
    and(
    (u.valid_from > vp.valid_to
    and u.valid_to < vp.valid_from)
    or u.valid_to is null and vp.valid_to is null)
left join user_roles vp_user_role
    on vp_user_role.sfdc_user_role_id = vp.user_role_id
    
