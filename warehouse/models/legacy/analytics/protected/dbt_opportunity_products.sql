WITH opportunity_line_items AS (

  SELECT * FROM {{ref('stg_sfdc__opportunity_line_items')}}

),

products AS (

  SELECT * FROM {{ref('stg_sfdc__products')}}

)


SELECT
    li.sfdc_opportunity_id,
    SUM(CASE WHEN p.type = 'Seat' THEN li.quantity ELSE 0 END) AS seats_purchased,
    SUM(CASE WHEN p.type = 'Hour' THEN li.quantity ELSE 0 END) AS hours_purchased
FROM opportunity_line_items AS li
JOIN products AS p
  ON li.sfdc_product_id = p.sfdc_product_id
GROUP BY li.sfdc_opportunity_id
