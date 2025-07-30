{# 
  Name: schema_cleanup
  This macro is used to identify and/or delete objects in Snowflake that are not 
  part of our dbt project.  These could be objects that were created in the wrong schema 
  and never cleaned up, objects that were deleted from the dbt project but not removed
  from snowflake, etc.

  You'll likely ALWAYS target prod (--target prod) when running this macro because 
  our dev environments use custom schemas which rarely line up with production.
  Also, MAKE SURE you've pulled the latest from the 'main' branch if running locally so 
  you're not comparing snowflake with an old version of our production 'main' branch.

  Arguments:
  schema = Value can be any schema name or a comma separated list of schemas. The default 
    value is 'all_schemas', which means this will run against all schemas in the target 
    database. You can specify 1 or more schemas if you only want to run this for specific 
    schemas.
  mode = Value can be 'in_dbt' or 'any'. The default value is 'in_dbt', which means it 
    will only check for objects that are currently in the dbt project. This will basically
    identify objects that exist in the wrong schema.  For example, tableA exists in 
    schemaA.tableA and schemaB.tableA but should only exist in one.
    ***BE VERY CAREFUL when setting this argument to 'any', which will identify any 
    object not currently in the dbt project, even those created by other teams/users. We 
    currently do have objects that were created outside of dbt that we don't want to drop.
    The downside of this is that objects that were deleted from dbt but never dropped from 
    Snowflake will not be identified. Safe alternative - run this macro in mode = 'any' 
    and set the drop_objects argument to 'no' to get a print out of everything that would
    have been deleted which you can then manually review/run.
  drop_objects = Value can be true or false. The default value is false which means this 
    macro will not actually drop any objects, it will only print out DROP statements for 
    you to run manually as you wish.
    ***BE VERY CAREFUL setting this argument to true as it will DROP objects in the data
    warehouse.

  Examples:
  dbt run-operation schema_cleanup --target prod
  dbt run-operation schema_cleanup --args '{schema: [analytics]}' --target prod
  dbt run-operation schema_cleanup --args '{schema: [analytics, app, base]}' --target prod
  dbt run-operation schema_cleanup --args '{schema: all_schemas, mode: any, drop_objects: false}' --target prod
 #}

{% macro schema_cleanup(schema='all_schemas', mode='in_dbt', drop_objects=false) %} 
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {{ print('"schema" must be a string or a list') }}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}
  {% call statement('get_outdated_tables', fetch_result=True) %}
    select distinct c.schema_name,
           c.ref_name,
           case c.ref_type when 'BASE TABLE' then 'TABLE' else c.ref_type end as ref_type
    from (
        select table_schema as schema_name, 
            table_name  as ref_name, 
            case when table_type = 'Base Table' then 'Table' else table_type end as ref_type
        from information_schema.tables
        where table_schema <> 'INFORMATION_SCHEMA'
        {% if schema != 'all_schemas' %}
            and table_schema in (
            {%- for s in schema -%}
            UPPER('{{ s }}'){% if not loop.last %},{% endif %}
            {%- endfor -%}
            )
        {% endif %}
        ) as c
        left join (values
        {%- for node in graph['nodes'].values() | selectattr("resource_type", "equalto", "model") | list
                        + graph['nodes'].values() | selectattr("resource_type", "equalto", "seed")  | list 
                        + graph['nodes'].values() | selectattr("resource_type", "equalto", "snapshot")  | list %} 
            (UPPER('{{node.schema}}'), UPPER('{{node.name}}')){% if not loop.last %},{% endif %}
        {%- endfor %}
        ) as desired (schema_name, ref_name) on desired.schema_name = c.schema_name
                                            and desired.ref_name    = c.ref_name
        left join (values
        {%- for node in graph['nodes'].values() | selectattr("resource_type", "equalto", "model") | list
                        + graph['nodes'].values() | selectattr("resource_type", "equalto", "seed")  | list 
                        + graph['nodes'].values() | selectattr("resource_type", "equalto", "snapshot")  | list %} 
            
            (UPPER('{{node.name}}')){% if not loop.last %},{% endif %}
        {%- endfor %}
        ) as in_dbt (ref_name) on in_dbt.ref_name    = c.ref_name
    where desired.ref_name is null
      {% if mode == "any" %}
      --No Filter on in_dbt.ref_name
      {% else %}
      and in_dbt.ref_name is not null --Only want to identify models in dbt but in the wrong schema
      {% endif %}
    order by 1, 2
  {% endcall %}
  {%- for to_delete in load_result('get_outdated_tables')['data'] %} 
    {% set fqn = target.database + '.' + to_delete[0] + '.' + to_delete[1] %}
    {% if drop_objects == false %}
      {{ print("DROP " ~ to_delete[2] ~ " IF EXISTS " ~ fqn ~ ";") }}
    {% elif drop_objects == true %}
      {% call statement() -%}
        {% do log('dropping ' ~ to_delete[2] ~ ': ' ~ fqn, info=true) %}
        {{ print("Running the following DROP statements:") }}
        {{ print("DROP " ~ to_delete[2] ~ " IF EXISTS " ~ fqn ~ ";") }}
        {# drop {{ to_delete[2] }} if exists {{ fqn }};
        commenting out the drop statement b/c we have so many dependency landmines
        this can be changed if/when we want this macro to actually drop objects
        but for we're making it a dud just to be safe. #}
      {%- endcall %}
    {% endif %}
  {%- endfor %}
  {{ print("Number of Objects " ~ load_result('get_outdated_tables')['data']|length) }}
{% endmacro %}