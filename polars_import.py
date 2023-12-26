import polars as pl
import glob
import os

CVELIST_CHECKOUT = 'cvelistV5/'
DATA_DIRECTORY = 'data/'

def import_year(year_dir: str, import_errors_file):
    year = os.path.basename(os.path.normpath(year_dir))
    error_files = []
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
            try:
                df_temp = pl.read_json(source=f).unnest("cveMetadata")
                #df_temp = df_temp.select(select_columns)
                #df = df.vstack(df_temp)
                try:
                    df = pl.concat([df, df_temp], how="diagonal_relaxed")
                except pl.InvalidOperationError:
                    error_string = f"InvalidOperationError: {f}"
                    error_files.append(error_string)
                except pl.ComputeError:
                    error_string = f"ComputeError: {f}"
                    error_files.append(error_string)
                except pl.SchemaError:
                    error_string = f"SchemaError: {f}"
                    error_files.append(error_string)
            except pl.PolarsPanicError as e:
                error_string = f"PolarsPanicError: {f}"
                error_files.append(f)
                import_errors_file.write(error_string)
            
    [ import_errors_file.write(e + '\n') for e in error_files]
    return year, error_files, df

def import_data(skip_years, import_errors_file):
    path = CVELIST_CHECKOUT + r'cves/*/'
    year_directories = glob.glob(path)
    total_errors = []
    for year_dir in year_directories:        
        year, error_files, df = import_year(year_dir, import_errors_file)
        df.write_parquet(DATA_DIRECTORY + year + ".parquet")
        print(f"Finished year {year}: {df.height} Success, {len(error_files)} Errors")
        total_errors.extend(error_files)
    print(f"Number errors: {len(total_errors)}")
    return df

skip_years = ["2010", "2013"]
with open("import-errors.txt", "w") as import_errors_file:
    df = import_data(skip_years, import_errors_file)
df.write_parquet("dataframe.parquet")
print(df)
print(df.group_by("dateReserved").count().sort("count", descending=True))