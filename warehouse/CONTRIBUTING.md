# Testing your changes

For convenience, we've created a script to run and test models you are working on.

```
# Run the models and tests for all models and dependencies of models you've changed. Continuous integration runs then.
bin/run-test

# Only run the tests
bin/run-test --no-run

# Limit to only those models directly modified
bin/run-test --aggressive

# Ignore running dbt seed
bin/run-test --without-seed
```

# Documenting `dbt` models

## Add schema.yml files for new models

`dbt` models can be enhanced by creating what `dbt` calls [schema.yml](https://docs.getdbt.com/docs/schemayml-files) files.
They describe the model and fields and define which tests should run.
**When adding a new or modifying an existing dbt model, the associated schema.yml file should be created or updated.**

To see some examples, take a look at:
* [stg_fountain_applicants.yml](models/staging/fountain/stg_fountain__applicants.yml)
* [app_billable_events.yml](models/app/base/app_billable_events.yml)

## Add business classification tags.

Per our Data Classification Policy, data fields need to be classified as *confidential*, *restricted*, or *highly restricted*.
Each model should be tagged with a classification tag to indicate to downstream consumers how the data
must be regarded.

Models can be tagged by adding a configuration section to the top of the model's `.sql` file.

```sql
{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH activities AS (

  SELECT * FROM {{ source('app', 'activities') }}

)


SELECT
  id AS activity_id,
  user_id AS member_id,
  resource_id,
  creator_id,
  viewed_at, -- populated for activities starting Dec 2016: https://github.com/betterup/betterup-app/issues/3768
  completed_at,
  favorited_at, -- member saved to Bookmarked list
  rating, -- rating was re-introduced in Sept 2017
  created_at,
  updated_at
FROM activities
```


<dl>
  <dt><pre>classification.c3_confidential</pre></dt>
  <dd>
      Refers to personal and confidential data available upon approval
      to users that have a clear business need to access and use such
      data as part of their required job duties and responsibilities.
      This data is used in routine business processes and the unauthorized
      disclosure, modification or destruction of this type of data will have
      a moderate impact on individuals and the organization.
  </dd>

  <dt><pre>classification.c2_restricted</pre></dt>
  <dd>
      Refers to personal and confidential data that is made available on
      limited bases to users that require this data as an integral part of
      their responsibilities. This data is used in specific business processes
      and the unauthorized disclosure, modification or destruction of this
      type of data will have a serious impact on individuals and the organization.
      Protecting and maintaining the confidentiality of this classification of
      personal and confidential data is of high priority to BetterUp and additional
      safeguards may be implemented to protect it.
  </dd>

  <dt><pre>classification.c1_highly_restricted</pre></dt>
  <dd>
      Refers to personal and confidential data that is made available on very
      specific and “need-to-know” bases. It is highly sensitive data that,
      similar to Restricted Data, by disclosing, modifying or destroying such
      data it will have a material adverse and very serious effect on the
      individual and the organization. Protecting and maintaining the confidentiality
      of this classification of personal and confidential data is of critical
      priority to BetterUp and high-level safeguards must be implemented to protect it.
  </dd>
</dl>

# Updating Confluence page for Analytics

If you are working in the `carina` layer, please ensure that you are keeping the Analytics team updated on what's released to DOMO, so that we can keep up to date with all the changes. You can use this [page](https://betterup.atlassian.net/wiki/spaces/AN/pages/409208110/Analytics+Release+Notes).
