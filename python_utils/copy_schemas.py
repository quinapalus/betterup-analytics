# This script is used to copy over table schemas from the US snowflake to the EU snowflake. 
# It allows for you to copy over multiple schemas quickly without having to manually copy and paste the DDL into snowflake.

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from pprint import pprint

us_creds = URL(user="",
               password="",
               account="oj02423.us-east-1",
               warehouse="",
               database="",
               schema="",
               role='stitch_loader',
               )

eu_creds = URL(user="",
               password="",
               account="hl82943.eu-central-1",
               warehouse="",
               database="",
               schema="",
               role='',
               )


def get_snowflake(sql, creds):
    engine = create_engine(creds)

    with engine.begin() as conn:
        results = list(conn.execute(sql))
    return results
    # return [(table.lower(), column.lower()) for table, column in results if table.upper() == 'DW_PLATE_ATTRS']


sql = """
select table_name from RAW.INFORMATION_SCHEMA.TABLES
where table_schema = 'STITCH_APP'
"""

table_list = get_snowflake(sql, us_creds)

for table, in table_list:
    get_ddl_sql = f"""select get_ddl('table', '{table.lower()}');"""
    try:
        ddl_sql = get_snowflake(get_ddl_sql, us_creds)
        for sql, in ddl_sql:
            execute_ddl = get_snowflake(sql, eu_creds)
            print(execute_ddl)
    except Exception as e:
        print(e)

print("===== DONE ============================")
