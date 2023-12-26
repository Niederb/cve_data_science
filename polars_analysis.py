import polars as pl
import glob

def read_parquet():
    path = r'data/*.parquet'
    parquet_files = glob.glob(path)
    df = pl.DataFrame()
    for parquet_file in parquet_files:
        print(parquet_file)
        if df.is_empty():
            df = pl.read_parquet(parquet_file)
        else:
            df_temp = pl.read_parquet(parquet_file)
            #df = df.vstack(df_temp)
            df = pl.concat([df, df_temp], how="diagonal_relaxed")
    return df

df = read_parquet()
by_assigner = df.group_by(by="assignerShortName").count().sort("count", descending=True)
print(by_assigner)
by_state = df.group_by(by="state").count().sort("count", descending=True)
print(by_state)
by_data_version = df.group_by(by="dataVersion").count().sort("count", descending=True)
print(by_data_version)
