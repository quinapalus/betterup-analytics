WITH opportunities AS (

  SELECT * FROM {{ref('int_sfdc__opportunities')}}

),

opportunity_products AS (

  SELECT * FROM {{ref('dbt_opportunity_products')}}

),

days as (
    
    select * from {{datespine("day","'2015-02-09'::timestamp","current_date")}}
    
)


SELECT
  o.sfdc_opportunity_id,
  o.type,
  o.amount,
  op.seats_purchased,
  op.hours_purchased,
  days.date_day
FROM opportunities AS o
JOIN opportunity_products AS op
  ON o.sfdc_opportunity_id = op.sfdc_opportunity_id
JOIN days
  ON DATE_TRUNC('DAY', o.start_date_per_agreement) <= days.date_day
  AND DATE_TRUNC('DAY', o.end_date_per_agreement) >= days.date_day
WHERE o.is_closed AND o.is_won AND
      o.start_date_per_agreement IS NOT NULL AND
      o.end_date_per_agreement IS NOT NULL
