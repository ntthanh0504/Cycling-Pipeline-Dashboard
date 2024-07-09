import os
import argparse
import subprocess
from urllib.parse import urlparse, urlunparse
from sqlalchemy import create_engine
import pandas as pd
from time import time

# Constants
ROOT = "data/cycling"
YEARS = ["2018", "2019"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
MONTHS = ["(Jan-Mar)", "(Apr-Jun)", "(Jul-Sep)", "(Oct-Dec)"]
SEASONS = ["spring", "autumn"]
POSITION = "Central"


def construct_filename(year, quarter, month, season):
    season_str = f" {season}" if season else ""
    return f"{year} {quarter}{season_str} {month}-{POSITION}.csv"


def construct_directory_path(year, quarter, month):
    return os.path.join(ROOT, year, f"{quarter}-{month}")


def download_data(url, directory_path, filename):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory {directory_path}")
    file_path = os.path.join(directory_path, filename)
    try:
        subprocess.run(['wget', url, '-O', file_path], check=True)
        print("Data downloaded successfully")
    except subprocess.CalledProcessError as e:
        print(f"Failed to download the data: {e}")


def extract_data(args):
    print("Downloading the data")
    parsed_url = urlparse(args.url)
    base_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
    for year in YEARS:
        for quarter, month in zip(QUARTERS, MONTHS):
            season = SEASONS[0] if quarter == "Q2" else SEASONS[1] if quarter == "Q4" else ""
            filename = construct_filename(year, quarter, month, season)
            url = f"{
                base_url}/ActiveTravelCountsProgramme/{filename.replace(' ', '%20')}"
            directory_path = construct_directory_path(year, quarter, month)
            print("\n" + "="*50)
            print(f"Downloading data from {url}")
            download_data(url, directory_path, filename)


def transform_data():
    for year in YEARS:
        for quarter, month in zip(QUARTERS, MONTHS):
            season = SEASONS[0] if quarter == "Q2" else SEASONS[1] if quarter == "Q4" else ""
            file_path = os.path.join(ROOT, year, f"{
                                     quarter}-{month}", construct_filename(year, quarter, month, season))
            df = pd.read_csv(file_path)
            # Duplicate the rows
            df = df.drop_duplicates()
            # Replace the rows with missing values
            df['Path'] = df['Path'].fillna('Unknown')
            df['Path'] = df['Path'].astype(str)
            # Transform the columns
            df['Quarter'] = df['Year'].str.split().str[1]
            df['Year'] = df['Year'].str.split().str[0]
            df['Year'] = pd.to_datetime(df['Year'], format='%Y').dt.year
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.date
            df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.time
            df.to_csv(file_path, index=False)


def load_data(args):
    engine = create_engine(
        f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')
    with engine.connect() as con:
        for year in YEARS:
            for quarter, month in zip(QUARTERS, MONTHS):
                season = SEASONS[0] if quarter == "Q2" else SEASONS[1] if quarter == "Q4" else ""
                file_path = os.path.join(ROOT, year, f"{
                                         quarter}-{month}", construct_filename(year, quarter, month, season))
                table_name = f"{year}_{quarter}_{season}_{month}_{
                    POSITION}".replace(' ', '_').replace('(', '').replace(')', '')
                print(f"{table_name}")
                df_iter = pd.read_csv(file_path, chunksize=10000)
                for df in df_iter:
                    t_start = time()
                    df.to_sql(name=table_name, con=con,
                              if_exists='append', index=False)
                    t_end = time()
                    print(f'Inserted a chunk, took {
                          t_end - t_start:.3f} second')


def etl_pipeline(args):
    print("Extracting data ...")
    extract_data(args)
    print("\n" + "="*50)
    print("Transforming data ...")
    transform_data()
    print("\n" + "="*50)
    print("Loading data to postgres ...")
    load_data(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Download cycling data and save to local directory')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True,
                        help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--url', required=True,
                        help='URL of the data file to download')
    args = parser.parse_args()
    etl_pipeline(args)
