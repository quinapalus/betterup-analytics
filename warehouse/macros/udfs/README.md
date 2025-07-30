# User-Defined Functions (UDFs)

The `sql` files in this `udfs` directory define custom functions for use both within `dbt` and any services that connect to the data warehouse (e.g. Mode). Defining UDFs as macros within `dbt` allows us to:
* increase visibility into UDFs
* incorporate GitHub flow for creating and maintaining UDFs
* and, easily separate development and production environments for UDFs

See this excellent [dbt Discourse thread](https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions-redshift/18) for more information on the setup.

## Adding a new UDF

* Create a new `.sql` file within the `udfs` directory, wrapping the UDF within a jinja `{% macro %}` tag.
* Then add the macro to the list of UDFs in `macros/create_udfs.sql`.

That's it! The `create_udfs` macro is configured to run before every dbt run (using the `on-run-start` hook in [`dbt_project.yml`](https://github.com/betterup/betterup-analytics/blob/master/warehouse/dbt_project.yml)) and target the associated environment.

## Testing UDFs

In order to leverage `dbt`'s built in model testing and documentation features, add a `.sql` model file to `models/analytics/udfs` testing the UDF over a representative domain. Function description, along with argument and output descriptions, and appropriate schema tests should be specified in the associated `.yml` file. Additional data tests can be added to the `tests` directory as needed.
