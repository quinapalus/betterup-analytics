with accounts_history as (

  select * from {{ ref('int_sfdc__accounts_snapshot') }}

),

account_formulas as (

/*
risk rating sfdc formula as of 2022-04-20
0.10 * IF( X1st_Session_Completed__c > 0.6, 1, IF( OR(ISPICKVAL( Lifecycle__c , "1 - Charting the Course"),ISPICKVAL( Lifecycle__c , "0 - Assemble Your Crew")) , 1, 10) ) )

+

( 0.30 * IF( of_Programs__c >1, 1, 10) )

+

( 0.15 * IF( ISPICKVAL( Highest_Level_Champion__c , "C-Suite") , 1, IF(ISPICKVAL( Highest_Level_Champion__c , "LOB Exec"), 1, IF(ISPICKVAL( Highest_Level_Champion__c , "VP HR"), 5, 10) ) ) )

+

( 0.25 * IF( ISPICKVAL( Sphere_of_Influence__c , "4+ Champions") , 1, 10) )

+

( 0.2 * IF( ISBLANK( Strategic_Catalyst__c ) , 10, IF( INCLUDES(Strategic_Catalyst__c, "None") , 10, 1) ))

)
*/

/*
Is Ultimate Parent Account formula as of 2022-11-09
Data Type	Formula	 	 
IF(
OR(
DOZISF__ZoomInfo_Id__c = ZoomInfo_Ultimate_parent_id__c,
ISBLANK(DOZISF__ZoomInfo_Id__c),
AND(DOZISF__ZoomInfo_Id__c != ZoomInfo_Ultimate_parent_id__c, Ultimate_Parent__c = "")),
true,
false
)
*/

select 

    history_primary_key,

    (0.10 * case when first_session_completed_rate > 0.6 then 1 
                when (account_lifecycle_stage = '1 - Charting the Course' or account_lifecycle_stage = '0 - Assemble Your Crew') then 1 else 10 end)
    +
    (0.30 * case when number_of_programs > 1 then 1 else 10 end) 
    +
    (0.25 * case when sphere_of_influence = '4+ Champions' then 1 else 10 end)
    +
    (0.2 * case when strategic_catalysts is null or strategic_catalysts like '%None%' then 10 else 1 end)
    as risk_rating,

    case
      when (zoominfo_id = zoominfo_ultimate_parent_id
            or zoominfo_id is null
            or (zoominfo_id != zoominfo_ultimate_parent_id and ultimate_parent is null))
      then true else false end as is_ultimate_parent_account

from accounts_history

)

select 
    accounts_history.history_primary_key,
    account_formulas.risk_rating,
    is_ultimate_parent_account
from accounts_history
left join account_formulas 
    on account_formulas.history_primary_key = accounts_history.history_primary_key
