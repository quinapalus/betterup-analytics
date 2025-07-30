{% macro sanitize_product_group(on_demand,
                                primary_coaching,
                                care,
                                coaching_circles,
                                workshops,
                                extended_network) -%}

RTRIM(CONCAT (
      IFF(on_demand, 'On-Demand + ', ''),
      IFF(primary_coaching, 'Primary Coaching + ', ''),
      IFF(care, 'Care + ', ''),
      IFF(coaching_circles, 'Coaching Circles + ', ''),
      IFF(workshops, 'Workshops + ', ''),
      IFF(extended_network, 'Extended Network + ', '')
    ),' +')

{%- endmacro %}
