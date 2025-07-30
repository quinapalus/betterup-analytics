**For review:** tag_reviewer

**For visibility**: tag_user

# Context and Resources

- [BUAPP-#](https://betterup.atlassian.net/browse/BUAPP-#)
- [DATA-#](https://betterup.atlassian.net/browse/DATA-#)

...why it was changed...

# Specific Code Changes

...what was changed...

# Data Tests

...prove you tested the code changes and are convinced it’s working properly, include screenshots...

...run DataDiff to check your dev data against prod, for example:

- `dbt run -m <my_data_model> && data-diff --dbt`
- `dbt run -m stg_coach__coach_profiles+` then `data-diff --dbt -s coach_comp__sessions` 

# Test Coverage Requirements

All new dbt models must have:
- 1 unique test on the primary key of the model
- 1 not_null test on the primary key of the model

If your PR adds a model that does not meet these requirements the DBT PR Test CI check will fail and you will need to add tests in order for it to pass.
For more details on this requirement please read [this Confluence page](https://betterup.atlassian.net/wiki/spaces/ET/pages/3221127190/RFC+dbt+Test+Coverage+Requirements)

# Table Schema Updates

Moving tables between schemas? Creating a new schema? See guidelines on [this Confluence page](https://betterup.atlassian.net/wiki/spaces/DATA/pages/3307798608/Moving+Tables+to+New+Schemas+in+dbt+Guidelines)

# Environment Sync
Please ensure that you have followed the process here. https://betterup.atlassian.net/wiki/spaces/DATA/pages/3137175704/Snowflake+Configuration+Change+Process
Any changes in US should be reflected in EU via betterup-infrastructure and terraform.
