#!/bin/bash
target_database="dev_analytics"
target_schema="DBT_$1"
echo "Target schema: ${target_schema}"
while read -r line; do
if [[ $line == '{"database":'* ]]; then
    data=$(echo "$line" | jq -r '.')
    table_name=$(echo "$data" | jq -r '.name')
    source_database=$(echo "$data" | jq -r '.database')
    source_schema=$(echo "$data" | jq -r '.schema')
    echo "Cloning table: ${table_name} from ${source_database}.${source_schema} to ${target_database}.${target_schema}"
    poetry run dbt run-operation clone_table_for_pr --args "{source_database: ${source_database}, source_schema: ${source_schema}, target_database: ${target_database}, target_schema: ${target_schema}, table_name: ${table_name}}" --target dev_with_pass
fi
done < inc_models.txt