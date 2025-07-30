{% macro stub_dbt_coach( exclude_capacity_attributes ) -%}

-- This is a macro to expose relevant fields from dbt_coach
-- while dim_coach is fixed. Once that happens, this macro should be removed.
-- For this macro to work, you must join `dbt_coach` and alias it as "dc"

-- Note: Use "false" for exclude_capacity_attributes if you want those fields to be
-- included in your star models.

dc.email AS coach_email,
dc.first_name AS coach_first_name,
dc.last_name AS coach_last_name,
dc.first_name || ' ' || dc.last_name AS coach_name,
dc.bio AS coach_bio,
dc.is_in_network AS coach_is_currently_in_network,
dc.coach_geo,
dc.coach_subregion_m49,
dc.coach_country_code,
dc.coach_country_name,
dc.coach_geo_country_code,
array_to_string(dc.staffing_languages, ',') AS coach_staffing_languages,
dc.staffing_tier,
dc.coach_state,
dc.pipeline_stage AS coach_pipeline_stage,
dc.staffable_state AS coach_staffable_state,
dc.priority_language AS coach_priority_language,
dc.type_primary AS coach_is_type_primary,
dc.type_extended_network AS coach_is_type_extended_network,
dc.type_on_demand AS coach_is_type_on_demand,
dc.extended_network_specialist_verticals,
{% if exclude_capacity_attributes == false %}
  dc.seats_desired_count,
  dc.seats_occupied_count,
  dc.seats_available_count,
{% endif %}
dc.days_since_application,
dc.days_since_hire

{%- endmacro %}
