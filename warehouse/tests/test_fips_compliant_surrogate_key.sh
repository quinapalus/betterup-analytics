OLD_FIPS_MODE_FLAG=$FIPS_MODE
export FIPS_MODE='true'
poetry run dbt run-operation test_patched_surrogate_key
export FIPS_MODE='false'
poetry run dbt run-operation test_surrogate_key
export FIPS_MODE=OLD_FIPS_MODE
