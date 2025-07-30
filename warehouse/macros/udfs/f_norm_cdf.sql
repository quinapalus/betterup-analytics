{% macro f_norm_cdf() %}

  {% set arg, arg_ref = 'input', 'input' %}

  CREATE OR REPLACE FUNCTION {{ target.schema }}.f_norm_cdf({{arg}} float) RETURNS float
  AS $$

    {% set z %}
        {{arg_ref}} / sqrt(2.0)
    {% endset %}

    {% set t %}
        1.0 / (1.0 + 0.5 * abs({{z}}))
    {% endset %}

    {% set erf_z%}
        (1.0 - {{t}} * exp( -{{z}}*{{z}} - 1.26551223 +
              {{t}} * ( 1.00002368 +
              {{t}} * ( 0.37409196 +
              {{t}} * ( 0.09678418 +
              {{t}} * (-0.18628806 +
              {{t}} * ( 0.27886807 +
              {{t}} * (-1.13520398 +
              {{t}} * ( 1.48851587 +
              {{t}} * (-0.82215223 +
              {{t}} * ( 0.17087277))))))))))) * sign({{z}})
    {% endset %}

    {% set final %}
        0.5 * (1.0 + {{erf_z}})
    {% endset %}

    {{ final }} $$;
{% endmacro %}
