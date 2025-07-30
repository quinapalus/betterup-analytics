WITH users AS (

  SELECT * FROM {{ ref('stg_wkfw__users') }}

)


SELECT * FROM users
