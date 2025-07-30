

WITH subscriptions AS (

  SELECT * FROM {{ source('salesforce', 'subscriptions') }}

)

SELECT
  id AS sfdc_subscription_id,
  name AS subscription_name,
  sbqq_product_id_c AS product_id,
  sbqq_product_name_c AS product_name,
  sbqq_quantity_c AS quantity,
  sbqq_bundled_c AS is_bundled,
  sbqq_root_id_c AS root_id,
  sbqq_subscription_start_date_c AS subscription_start_date,
  sbqq_subscription_end_date_c AS subscription_end_date,
  sbqq_required_by_id_c AS required_by_product_id,
  sbqq_renewal_quantity_c AS renewal_quantity,
  sbqq_list_price_c AS list_price,
  sbqq_net_price_c AS net_price,
  sbqq_regular_price_c AS regular_price,
  sbqq_customer_price_c AS customer_price,
  sbqq_additional_discount_amount_c AS discount_amount,
  sbqq_billing_frequency_c AS billing_frequency,
  sbqq_prorate_multiplier_c AS prorate_multiplier,
  sbqq_contract_c AS contract_id,
  opportunity_type_c AS opportunity_type,
  sbqq_account_c AS sfdc_account_id
FROM subscriptions
WHERE NOT is_deleted