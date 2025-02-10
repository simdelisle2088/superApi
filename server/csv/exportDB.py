import pandas as pd
from sqlalchemy import create_engine

# Database connection
engine = create_engine('mysql+pymysql://deliver-prod:AQLDZNY485Xtbqn@localhost/pasuperxyz_superdeliver')

# Query and export to CSV
query = "SELECT * FROM `locations` WHERE level = 1 AND is_archived = false;"
df = pd.read_sql(query, engine)
df.to_csv('C:/Users/simon/export.csv', index=False)
