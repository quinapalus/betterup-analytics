## BetterUp Warehouse ETL Best Practices

Read the [CONTRIBUTING](CONTRIBUTING.md) file for contribution requirements regarding models.

#### FIPS mode

To run dbt on a FIPS compliance mode, ensure that the system enviornment variable `FIPS_MODE` is set to `"true"`. To do this, run `export FIPS_MODE='true'` prior to running dbt.

In FIPS compliance mode, the surrogate_key functionality has been replaced to use SHA256 as a hashing algorithm instead of MD5. As a result, columns that are generated using surrogate_key will appear as a 64 character hex string as opposed to a 32 character one.

A unit test has been written for this functionality. Please see `macros/test_fips_compliant_surrogate_key.sql` and `tests/test_fips_compliant_surrogate_key.sh`

#### Base Layer Models

* Base models are responsible for filtering out invalid or deprecated columns and tables from source data.
* Base models should **not** filter out any data.
* Base models act as the sole access point to raw (source) data tables, providing a clean interface for the rest of the transformation logic.
* All aliasing to rationalize column names into the standard format described below should occur within the base layer.

#### Standard Naming Conventions

1. Use a descriptive name for **all** object keys, including an object's primary key. That is, `assessments.assessment_id` instead of `assessments.id`.
2. References to the `users` object should use role specific names if current and potential future behavior is restricted to a single role. For example, `calls.member_id` instead of `calls.user_id` and `calls.member_connected_at` instead of `calls.user_connected_at`.
3. References to the `users` object that are not restricted to a single role should use action-specific language. For example, `resources.creator_id` instead of `resources.user_id`.
4. Object naming should be consistent with end-user (e.g. member facing) language. For example, `goals` instead of `objectives`, and `sessions` instead of `appointments`.
5. When naming aggregate columns, postfix the name of the measure with the mathematical function (e.g. `_count`, `_sum`, `_mean`). For example, `completed_session_count` instead of `count_completed_sessions` or `num_completed_sessions`. And `completed_session_minutes_mean` instead of `avg_completed_session_length`.

## dbt Resources

- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://github.com/betterup/betterup-analytics#setup)
- Join the [chat](http://ac-slackin.herokuapp.com/) on Slack for live questions and support.
