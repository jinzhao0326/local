import snowflake.connector
import pandas as pd
import yaml

with open('/Users/jinzhao/Documents/doordash/doordash-etl/development.yaml', 'r') as stream:
    try:
        d = yaml.safe_load(stream)
    except yaml.YAMLError as e:
        print(e)

con = snowflake.connector.connect(
    user = d['snowflake-prod']['username'],
    password = d['snowflake-prod']['password'],
    account = d['snowflake-prod']['account'],
    warehouse = d['snowflake-prod']['warehouse'],
)

q = con.cursor().execute(
"""
SELECT
   1 AS num
"""
)

df = q.fetch_pandas_all()