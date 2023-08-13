import pandas as pd 
from sqlalchemy import create_engine
import os
import argparse
import time

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = "output.parquet"
    
    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df_iter = pd.read_paruet("output.parquet", iterator = True, chunksize = 100000)
    df = next(df_iter)
    df.head(n=0).to_sql(name = table_name, con = engine, if_exists = "replace")
    df.tosql(name = table_name, con=engine, if_exists="append")

    while True:
        t_start = time()
        df = next(df_iter)
        df.to_sql(name = table_name, con=engine, if_exits = True)
        t_end = time()
        print("uploaded another chunk in %.3f seconds"%(t_end-t_start))

if __name__=="__main__":
    parser  = argparse.ArgumentParser(description= "parse arguments for the ingestion part")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--url")

    args = parser.parse_args()
    main(args)
