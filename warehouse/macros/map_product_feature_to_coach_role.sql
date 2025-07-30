{% macro map_product_feature_to_coach_role(product_feature) -%}

-- Owner : Seetha Venkatadri
-- Please tag on future PRs
-- Purpose: get coach assignment role that can map to a specific product feature
CASE
  WHEN {{ product_feature }} = 'PRIMARY_COACHING' THEN 'primary'
  WHEN {{ product_feature }} = 'ON_DEMAND' THEN 'on_demand'
  WHEN {{ product_feature }} = 'EXTENDED_NETWORK' THEN 'secondary'
  WHEN {{ product_feature }} = 'CARE' THEN 'care'
  WHEN {{ product_feature }} IN ('COACHING_CIRCLES','WORKSHOPS') THEN 'group'
  ELSE NULL
END

{%- endmacro %}