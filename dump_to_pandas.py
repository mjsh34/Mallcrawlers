import pandas as pd
import psycopg2

import os
from pathlib import Path
import argparse


ap = argparse.ArgumentParser()
ap.add_argument('--output_csv_path', '-o', required=True)
ap.add_argument('--table_name', '-t', required=True)
args = ap.parse_args()

db_hostname = os.getenv('DB_HOSTNAME')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

conn = psycopg2.connect(host=db_hostname, user=db_username, password=db_password, dbname=db_name)
cur = conn.cursor()

q = "SELECT * FROM {}".format(args.table_name)
print("Building dataframe ({})...".format(q))
df = pd.read_sql_query(q, conn)

print("Dataframe ({}, {}):\n{}".format(df.shape, df.columns, df.head(5)))
print()

out_csv_path = Path(args.output_csv_path)
out_csv_path.parent.mkdir(exist_ok=True, parents=True)
print("Saving as csv ({})...".format(out_csv_path))
df.to_csv(out_csv_path)

print("Done")

