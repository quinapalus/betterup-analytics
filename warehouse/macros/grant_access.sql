{% macro grant_access() -%}

  {# always grant access to development user #}
  grant usage on schema {{ this.schema }} to reporter_dev;
  grant select on {{ this }} to reporter_dev;

  {% if target.name == 'prod' %}
    {# in production grant access based on target schemas #}
    {% if this.schema not in ['analytics_app','base','braze_staging','catalyst_service','coach_payments','content',
                              'dbt_project_evaluator','deployments','labs','salesforce_service','snapshots','sources'] %}
      grant usage on schema {{ this.schema }} to reporter;
      grant select on {{ this }} to reporter;
      grant usage on schema {{ this.schema }} to reporter_restricted;
      grant select on {{ this }} to reporter_restricted;
      grant usage on schema {{ this.schema }} to looker_role;
      grant select on {{ this }} to looker_role;

    {% elif this.schema == 'restricted' %}
      grant usage on schema {{ this.schema }} to reporter_restricted;
      grant select on {{ this }} to reporter_restricted;
    {% endif %}

    {% if this.schema == 'carina' %}
      grant usage on schema {{ this.schema }} to reporter_carina;
      grant select on {{ this }} to reporter_carina;
    {% endif %}

    {% if this.schema == 'catalyst_service' %}
      grant usage on schema {{ this.schema }} to catalyst_service;
      grant select on {{ this }} to catalyst_service;
    {% endif %}

    {% if this.schema == 'salesforce_service' %}
      grant usage on schema {{ this.schema }} to salesforce_service;
      grant select on {{ this }} to salesforce_service;
    {% endif %}
  {% endif %}
{%- endmacro %}
