'''File Converter application'''

'''importing necessary packages'''
import glob
import json
import re
import os
import sys
import pandas as pd
from dotenv import load_dotenv

'''loading environmet variables'''
load_dotenv()

'''getting column names'''
def get_column_names(schema, tb_name:str, sorting_key='column_position'):
    table= sorted(schema[tb_name],key =lambda  x : x[sorting_key])
    return [col['column_name'] for col in table]

'''reading csv files'''
def read_csv(file_path, schema, table_name):
    column_names = get_column_names(schema,table_name)
    return  pd.read_csv(file_path, header=None, names= column_names)

'''csv to json'''
def csv_to_json(df,filename, target_path):
    os.makedirs(target_path, exist_ok=True)
    df.to_json(f'{target_path}/{filename}', orient= 'records', lines=True)

'''covnverter logic'''
def file_converter(data,src,target,table_name):
    files= glob.glob(f'{src}/{table_name}/part-*')

    if not len(files):
        raise NameError(f'No files found for {table_name}')
        
    for file in files:
        # print(f'processing {file}')
        df= read_csv(file,data,table_name)
        filename= re.split('[/\\\]',file)[2]
        csv_to_json(df,filename,target)

'''main logic'''
def process_files(ds_name=None):
    src_path= os.getenv('src_path')
    target_path= os.getenv('target_path')
    schema= json.load(open(f'{src_path}/schemas.json'))

    if not ds_name:
        ds_name=schema.keys()
    
    for table in ds_name:
        try:
            print(f'processing {table}')
            file_converter(data=schema,target=target_path,src=src_path,table_name=table)
    
        except NameError as e:
            print(e)
            print(f'Error processing: {table}')

if __name__=='__main__':
    if len(sys.argv)==2:
        table_names= json.loads(sys.argv[1])
        process_files(table_names)
    else:
        process_files()
