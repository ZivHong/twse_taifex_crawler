import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table
import os
import csv
import re
def create_db_engine(dbname="test"):
    database_filepath = f"archived/{dbname}.db"
    if os.path.exists(database_filepath):
    # If it exists, delete it
        os.remove(database_filepath)
    engine = create_engine(f'sqlite:///{database_filepath}')

    # Create a metadata object that will hold all the information about the database
    metadata = MetaData()

    # Define a simple table
    stock = Table(
        'stock',
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column('stock', String),
        Column('dealer', String),
        Column('price', Float),
        Column('buy', Integer),
        Column('sell', Integer),
        Column('excd', Integer)
    )

    # Create the table in the database
    metadata.create_all(engine)
    return engine

def bshtm_to_db(csv_filename,sql_engine,csv_encoding='big5'):
    stock = None
    with open(csv_filename,encoding=csv_encoding) as file_obj:
        reader_obj = csv.reader(file_obj)
        stock_pattern = r'"(.*?)"'
        skip_next = False
        all_data_list = []
        for row in reader_obj: 
            row_length = len(row)
            if row_length <= 1: # first line
                continue
            elif row_length == 2: # stock line
                stock = re.findall(stock_pattern, row[1])[0]
                skip_next = True
            elif skip_next: # skip header name row
                skip_next = False
                continue
            elif row_length == 6: # excd file
                all_data_list.append([stock]+row[1:5]+[2 if row[5] == "逐筆" else 1])
            elif row_length == 11: # normal stock file
                all_data_list.append([stock]+row[1:5]+[0])
                if len(row[6]) != 0:
                    all_data_list.append([stock]+row[7:11]+[0])
    df = pd.DataFrame(all_data_list)
    df.rename(columns={0: 'stock', 1: 'dealer', 2: 'price', 3: 'buy', 4: 'sell', 5: 'excd'},inplace=True)
    df.iloc[:, 1] = df.iloc[:, 1].str[:4]
    df.to_sql("stock",con=sql_engine,index=False,if_exists="append")

if __name__ == "__main__":
    # os.makedirs("archived",exist_ok=True)
    fail_list = []
    base_path = "/home/user/judong/stock"
    for folder in os.listdir(base_path):
        if folder.isdigit():
            sql_engine = create_db_engine(folder)
            bshtm_path = f"{base_path}/{folder}/bshtm"
            try:
                for csv_file in os.listdir(bshtm_path):
                    try:
                        bshtm_to_db(f"{bshtm_path}/{csv_file}",sql_engine)
                    except:
                        fail_list.append([folder,os.path.splitext(csv_file)[0]])
            except Exception as e:
                fail_list.append([folder,e])
            sql_engine.dispose()
    print(fail_list)
    fail_df = pd.DataFrame(fail_list)
    fail_df.sort_values(by=fail_df.columns[0],inplace=True)
    fail_df.to_csv("archived/fail.csv", mode="a+", header=False, index=False)