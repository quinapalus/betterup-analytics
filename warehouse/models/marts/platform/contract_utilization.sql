{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='table'
  )
}}


{%- call statement('get_max_seats', fetch_result=True) -%}
      SELECT coalesce(max(seats),0) as max_seats FROM {{ref('stg_app__contract_line_items')}}
{%- endcall -%}

{%- set max_seats = load_result('get_max_seats')['data'][0][0] -%}


WITH member_platform_calendar AS (
    SELECT * FROM {{ref('member_platform_calendar')}}
),

contract_line_items AS (
    SELECT * FROM {{ref('stg_app__contract_line_items')}}
),

contracts AS (
    SELECT * FROM {{ref('stg_app__contracts')}}
),

dates AS (
    SELECT * FROM {{ref('dim_date')}}
),

products AS (
    SELECT * FROM {{ref('stg_app__products')}}
),

calendar AS (
     SELECT date_key,
            date,
            calendar_year_month,
            is_current_fiscal_quarter,
            is_previous_fiscal_quarter,
            date = LAST_DAY(date) AS is_last_day_of_month
      FROM dates
),

max_number_of_seats AS (
      SELECT max(seats) AS max_seats
        FROM contract_line_items
),

ordered_psa AS (
    SELECT product_id,
           organization_id,
           date AS psa_date,
           product_subscription_id,
           product_subscription_assignment_id,
           v2,
           track_assignment_id,
           track_id,
           member_id,
           ROW_NUMBER() over (PARTITION BY organization_id, date_key, v2, product_id, product_subscription_id ORDER BY psa_starts_at,product_subscription_assignment_id) AS psa_rownum
     FROM member_platform_calendar AS mpc
     --WHERE product_subscription_assignment_id IS NOT NULL
),

ordered_cli AS (
     SELECT cli.contract_line_item_id,
            c.organization_id,
            cli.product_id,
            cli.product_subscription_id,
            cli.seats,
            d.date AS cli_date,
            ROW_NUMBER() over (PARTITION BY c.organization_id, cli.product_id, cli.product_subscription_id,d.date ORDER BY cli.starts_at,cli.contract_line_item_id) AS seat_number
     FROM calendar AS d
     INNER JOIN contract_line_items AS cli on date(convert_timezone('UTC','America/Los_Angeles',cli.starts_at)) <= d.date  and d.date <= coalesce(date(convert_timezone('UTC','America/Los_Angeles',cli.ends_at)),current_date)
     INNER JOIN contracts AS c on c.contract_id = cli.contract_id
     -- generates a row for every seat we have using the number of seats from contract line items (cli.seats)
     INNER JOIN (select row_number() over(order by seq4()) as r from table(generator(rowcount => {{max_seats}})) ) AS v on v.r <=cli.seats
),

currently_active_cli AS (
      SELECT
      cli.contract_line_item_id,
      c.organization_id,
      cli.product_id,
      cli.product_subscription_id,
      c.contract_id,
      ROW_NUMBER() over (PARTITION BY c.organization_id, cli.product_id, cli.product_subscription_id ORDER BY cli.starts_at DESC) AS r
     FROM contract_line_items AS cli
     INNER JOIN contracts AS c
           ON c.contract_id = cli.contract_id
     -- since we show utilization as a point in time resultset
     -- using current_date to fetch currently active CLIs
     WHERE current_date BETWEEN cli.starts_at AND cli.ends_at
     QUALIFY r = 1 -- picking 1 in case there are more than 1 line items with same start date
),

contract_util AS (
    SELECT psa_date AS utilization_date,
           cli_date AS availability_date,
           COALESCE(cli.product_id, psa.product_id) AS product_id,
           COALESCE(cli.organization_id, psa.organization_id) AS organization_id,
           seat_number AS available_seat_number,
           COALESCE(cli.product_subscription_id, psa.product_subscription_id) AS product_subscription_id,
           product_subscription_assignment_id,
           v2,
           track_assignment_id,
           track_id,
           contract_id,
           -- if the CLI for a row is null, that is, there is no available seat for the day for that specific product
           -- we pick up the oldest cli created for that organization for that product and assign the over utilized PSA to it
           COALESCE(cli.contract_line_item_id,ccli.contract_line_item_id) AS contract_line_item_id,
           psa_rownum AS utilized_seat_number,
           psa.member_id,
           CASE WHEN cli.contract_line_item_id IS NULL THEN True ELSE False END AS is_over_utilized
      FROM ordered_psa AS psa
      FULL OUTER JOIN ordered_cli AS cli
          ON cli.organization_id = psa.organization_id
          AND cli.product_id = psa.product_id
          AND (cli.product_subscription_id IS NULL OR cli.product_subscription_id = psa.product_subscription_id)
          AND psa.psa_date = cli.cli_date
          AND seat_number = psa_rownum
      LEFT OUTER JOIN currently_active_cli AS ccli
           ON ccli.organization_id = psa.organization_id
           AND ccli.product_id = psa.product_id
           AND (ccli.product_subscription_id IS NULL OR ccli.product_subscription_id = psa.product_subscription_id)
           AND cli.seat_number IS NULL
)
SELECT
       {{ dbt_utils.surrogate_key(['utilization_date', 'availability_date', 'track_id', 'c.organization_id', 'product_subscription_id', 'member_id',
                                    'contract_line_item_id', 'utilized_seat_number', 'available_seat_number', 'product_subscription_assignment_id',
                                    'v2', 'is_over_utilized', 'contract_id' ]) }} as unique_id,
       c.*,
       p.coaching_cloud,
       p.on_demand,
       p.primary_coaching,
       p.care,
       p.coaching_circles,
       p.workshops,
       p.extended_network
FROM contract_util AS c
LEFT OUTER JOIN products AS p
ON p.product_id = c.product_id
