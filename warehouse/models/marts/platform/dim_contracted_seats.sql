{{
  config(
    materialized='view'
  )
}}

WITH app_contracts AS (
  SELECT * FROM {{ref('stg_app__contracts')}}
),

contract_line_items AS (
    SELECT * FROM {{ref('stg_app__contract_line_items')}}
),

app_tracks AS (
    SELECT * FROM {{ref('dim_tracks')}}
),

contracted_seats AS (
    SELECT
        t.track_id,
        cli.contract_id,
        cli.contract_line_item_id,
        cli.product_id,
        cli.seats,
        cli.starts_at
    FROM app_tracks AS t
    INNER JOIN app_contracts AS c ON t.contract_id = c.contract_id
    INNER JOIN contract_line_items AS cli ON cli.contract_id = c.contract_id
    ORDER BY cli.contract_line_item_id, cli.product_id, cli.starts_at
)

SELECT * FROM contracted_seats
