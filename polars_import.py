import polars as pl
import glob
import os

CVELIST_CHECKOUT = 'cvelistV5/'
DATA_DIRECTORY = 'data/'

def import_data():
    path = CVELIST_CHECKOUT + r'cves/*/'
    year_directories = glob.glob(path)
    error_files = []

    for year_dir in year_directories:
        year = os.path.basename(os.path.normpath(year_dir))
        if year == "2021" or year == "2022" or year == "2023" or year == "2010":
            continue
        errors_in_year = 0
        print(f"Processing year {year}")
        # absolute path to search all text files inside a specific folder
        path = year_dir + r'*/*.json'
        files = glob.glob(path)

        df = pl.DataFrame()
        
        select_columns = ["dateReserved"]
        for f in files:
            if df.is_empty():
                df = pl.read_json(source=f).unnest("cveMetadata")
                #df = df.select(select_columns)
            else:
                #print(f)
                try:
                    df_temp = pl.read_json(source=f).unnest("cveMetadata")
                    #df_temp = df_temp.select(select_columns)
                    #df = df.vstack(df_temp)
                    df = pl.concat([df, df_temp], how="diagonal_relaxed")
                except pl.PolarsPanicError as e:
                    error_files.append(f)
                    errors_in_year += 1
        df.write_parquet(DATA_DIRECTORY + year + ".parquet")
        print(f"Finished year {year}: {len(files)} Files, {errors_in_year} Errors")
    print(f"Number errors: {len(error_files)}")
    return df

df = import_data()
df.write_parquet("dataframe.parquet")
print(df)
print(df.group_by("dateReserved").count().sort("count", descending=True))