#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime as dt
import snowflake.connector

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy.dialects import registry
from sqlalchemy import event

# CRED_FILE = "snowflake_credentials.py"
from snowflake_credentials import creds

import bz2
import os
import time
import threading
import snowflake as sf
import zipfile as zf
import math


class SnowConnector():
    def __init__(self, creds):
        try:
            self.user = creds['user']
            self.account = creds['account']
            self.warehouse = creds['warehouse']
            self.database = creds['database']
            self.schema = creds['schema']
            self.infile = creds['infile']
            self.filechunks_exist = False
            self.staging_folder = 'staging'
            self.rows_to_read = 1000000
            self.field_delimiter = ','
            self.field_delimiter = '\n'
            self.file_within_zip = ''
            self.rm_staging_folder = True
            self.rm_staging_folder = True
            
            if 'snow_table' in creds and creds['snow_table']:
                self.snow_table = creds['snow_table']
            elif 'infile' in creds and creds['infile']:
                # create table name from infile name
                remove_list = ['.txt', '.csv', '.zip']
                temp_filename = creds['infile'].split('/')[-1]
                for r in remove_list:
                    temp_filename = temp_filename.replace(r, '')
                    
                temp_filename = temp_filename.replace(' ', '_').replace('.', '_')
                self.snow_table = ''.join(s for s in temp_filename if (s.isalnum() or s == '_'))
            else:
                now = dt.datetime.now()
                self.snow_table = 'python_upload_' + now.replace(':', '-').replace('.', '-').replace(' ', '-')
            
            if '.zip' in self.infile:
                self.filetype =  'zip'
            elif '.txt' in self.infile:
                self.filetype =  'txt'
            elif '.csv' in self.infile:
                self.filetype =  'csv'
            else:
                self.filetype =  'none'
            
            if 'create_new_if_table_exists' in creds and creds['create_new_if_table_exists']:
                self.if_table_exists = 'create_new'
            elif 'replace_if_table_exists' in creds and creds['replace_if_table_exists']:
                self.if_table_exists = 'replace'
            else:
                print("""No selection for how to handle pre-existing Snowflake tables with the same name. 
                         Default is to create a new table""")
                self.if_table_exists = 'create_new'

            if 'filechunks_exist' in creds and creds['filechunks_exist']:
                self.filechunks_exist = True

            if 'staging_folder_for_filechunks' in creds and creds['staging_folder_for_filechunks']:
                self.staging_folder = str(creds['staging_folder_for_filechunks'])

            if 'rows_to_read_for_chunking' in creds and creds['rows_to_read_for_chunking']:
                self.rows_to_read = creds['rows_to_read_for_chunking']

            if 'field_delimiter' in creds and creds['field_delimiter']:
                self.field_delimiter = str(creds['field_delimiter'])

            if 'record_delimiter' in creds and creds['record_delimiter']:
                self.record_delimiter = str(creds['record_delimiter'])

            if 'filename_in_zip' in creds and creds['filename_in_zip']:
                self.filename_in_zip = creds['filename_in_zip']

            if 'delete_staging_folder_after_process' in creds and creds['delete_staging_folder_after_process']:
                self.rm_staging_folder = creds['delete_staging_folder_after_process']

            if 'remove_local_staging_filechunks' in creds and creds['remove_local_staging_filechunks']:
                self.rm_staging_files = creds['remove_local_staging_filechunks']
                
        except Exception as e:
            print(e, '\nPlease check the credential file for missing information')

    def print_conn_info(self):
        print('USER:', self.user)
        print('ACCOUNT:', self.account)
        print('WAREHOUSE:', self.warehouse)
        print('DATABASE:', self.database)
        print('SCHEMA:', self.schema)
        print('DATA INPUT:', self.infile)
        print('INFILE TYPE:', self.filetype)
        print('SNOWFLAKE TABLE:', self.snow_table)
        print()
        
    def get_filetype(self):
        return self.filetype
        
    def get_infile(self):
        return self.infile
    
    def get_table(self):
        return self.snow_table

    def get_rows_to_read(self):
        return self.rows_to_read

    def get_field_delimiter(self):
        return self.field_delimiter

    def get_record_delimiter(self):
        return self.record_delimiter

    def get_staging_folder(self):
        return self.staging_folder

    def get_file_within_zip(self):
        return self.filename_in_zip

    def get_rm_staging_folder(self):
        return self.rm_staging_folder

    def get_rm_staging_files(self):
        return self.rm_staging_files
    
    def snowflake_cursor(self):
        conn = snowflake.connector.connect(
                                            user=self.user,
                                            authenticator='externalbrowser',
                                            account=self.account,
                                            warehouse=self.warehouse,
                                            database=self.database,
                                            schema=self.schema
                                           )
        
        print("Connected to Snowflake")
        return conn
        
    def table_exists(self, conn, table=None):
        
        if table:
            table_name = table
        else:
            table_name = self.snow_table
            
        check = f"SHOW TABLES LIKE '{table_name}'"
        
        cur = conn.cursor()
        cur.execute(check)
        tables = cur.fetchall()
        
        if tables:
            return True
        return False
        
    def create_table(self, col_names_types):
        """
        col_names_types = '(col1 integer, col2 string, col3 date)'
        
        if_exits: create_new or replace
        """
        table_name = self.snow_table
        conn = self.snowflake_cursor()
        
        if self.table_exists(conn):
            if self.if_table_exists == 'create_new':
                old_table_name = table_name
                loop_count = 2
                
                while True:
                    table_name = f'{old_table_name}_{loop_count}'
                    exists = self.table_exists(conn, table_name)
                    
                    if not exists:
                        break
                    
                    loop_count += 1
                self.snow_table = table_name
                print(old_table_name, 'already exists. new table name is', table_name)
            else:
                print(table_name, 'already exists. table will be replaced')

        create = "CREATE OR REPLACE TABLE " + table_name + col_names_types
        conn.cursor().execute(create)
        print(table_name, 'table created or replaced')
        

def file_col_names_types(filename, all_strings=True):    
    # set all col types to string
    if all_strings:
        infile = pd.read_csv(filename, nrows=1)
        temp_cols = infile.columns
        clean_cols = []
        unnamed_count = 1
        
        for col in temp_cols:
            if 'Unnamed:' in col:
                col = f'UNNAMED_COLUMN_{unnamed_count}'
                unnamed_count += 1
            clean_cols.append(col)
        infile_sql_types = "(" + ", ".join([col.replace(' ', '_').strip() + ' string' for col in clean_cols]) + ")"
    
    # attempt to guess col types based on first 1000 rows of infile
    else:
        infile = pd.read_csv(filename, nrows=1000)
        py_to_sql_types = {'int64': 'integer', 'int32': 'integer', 'object': 'string', 
                           'float64': 'bigint', 'float32': 'bigint', 'bool': 'bit', 
                           'datetime64[ns]': 'datetime'}

        infile_types = infile.dtypes.apply(lambda x: x.name).to_dict()
        infile_sql_types = "(" + ", ".join([key.replace(' ', '_').strip() + ' ' + 
                                            (py_to_sql_types[value] if value in py_to_sql_types else 'string') 
                                            for key, value in infile_types.items()]) + ")"

    return infile_sql_types


def file_col_names(filename):
    infile = pd.read_csv(filename, nrows=1)

    infile_col_names = "(" + ", ".join([c.replace(' ', '_').strip() for c in infile.columns]) + ")"

    return infile_col_names


class ZipToGzThread (threading.Thread):
    def __init__(self, threadID, skip, rows_to_read, filename, header, dtypes,
                 staging_folder, zipname=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.skip = skip
        self.rows_to_read = rows_to_read
        self.filename = filename
        self.header = header.columns
        self.dtypes = dtypes
        self.staging_folder = staging_folder
        self.zipname = zipname
        
    def run(self):
        chunk_file(self.skip, self.rows_to_read, self.filename, self.header,
                   self.dtypes, self.threadID, self.staging_folder, self.zipname)

        
def chunk_file(skip, rows_to_read, filename, header, dtypes, threadID, staging_filepath, zipname):
    if zipname:
        temp = pd.read_csv(zipname, skiprows=skip, nrows=rows_to_read, dtype=dtypes,
                           names=header, header=None, engine ='c')
    else:
        temp = pd.read_csv(filename, skiprows=skip, nrows=rows_to_read, dtype=dtypes,
                           names=header, header=None, engine ='c')
    print(f'read {len(temp)} lines starting at row {skip}')
    temp.to_csv(f'{staging_filepath}/{filename}_{threadID}.gz', index=False, header=True,
                compression='gzip', chunksize=1000)
    print(f'sent thread ID {threadID} to csv in staging folder')


# credit: https://interworks.com/blog/2020/03/04/zero-to-snowflake-multi-threaded-bulk-loading-with-python/
class sfExecutionThread (threading.Thread):
    def __init__(self, threadID, sql_query):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.sql_query = sql_query
        
    def run(self):
        print('Starting {0}: {1}'.format(self.threadID, self.sql_query))
        execute_in_snowflake(self.sql_query)
        print('Exiting {0}: {1}'.format(self.threadID, self.sql_query))
        
        
def execute_in_snowflake(sf_query):
    # connect to snowflake
    temp_snow = SnowConnector(creds)
    conn = temp_snow.snowflake_cursor()

    # increase timeout
    conn.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 86400')

    conn.cursor().execute(sf_query)
    conn.close()
    

def main():
    start_time = dt.datetime.now()
    print('Starting main:', start_time)
    
    snow = SnowConnector(creds)
    snow.print_conn_info()
    filetype = snow.get_filetype()
    table_name = snow.get_table()
    
    if filetype in ['zip', 'csv', 'txt']:  
        filename = snow.get_infile()
        
        col_names = file_col_names(filename)
        col_names_types = file_col_names_types(filename, all_strings=True)

        print(dt.datetime.now(), '***', 'Creating table', table_name)
        snow.create_table(col_names_types)

        # create staging folder
        staging_folder = snow.get_staging_folder()
        path = os.path.abspath(os.getcwd())
        staging_filepath = str(path)+'/'+staging_folder
        staging_exists = os.path.isdir(staging_folder)

        print(dt.datetime.now(), '***', 'Creating or clearing staging folder...')
        if staging_exists:
            # remove contents from staging folder
            for root, dirs, files in os.walk(staging_filepath):
                for f in files:
                    os.unlink(os.path.join(root, f))
        else:
            try:  
                os.mkdir(staging_filepath)  
            except OSError as error:  
                print(error)         
        print(dt.datetime.now(), '***', 'Staging folder ready')

        print(dt.datetime.now(), '***', 'Counting rows in file and getting header info...')
        if filetype == 'zip':
            zipname = snow.get_infile()
            filename = snow.get_file_within_zip()

            with zf.ZipFile(zipname) as folder:
                with folder.open(filename) as f:
                    row_count = 0
                    for _ in f:
                        row_count += 1
            header = pd.read_csv(zipname, nrows=1)
        else:
            with open(filename) as f:
                row_count = 0
                for _ in f:
                    row_count += 1
            header = pd.read_csv(filename, nrows=1)
        print(dt.datetime.now(), '***', f'Header info gathered for your {str(row_count)} row file')

        # calculate number of iterations needed for file chunking (recommended to use 1 mill)
        rows_to_read = snow.get_rows_to_read()
        rows_div_mill = row_count / rows_to_read
        iterations = math.ceil(rows_div_mill)
        skip = 1

        # set all types to str/obj for faster read
        dtypes = {k: str for k in header.columns}
        file_chunk_threads = []

        print(dt.datetime.now(), '***', 'Creating thread list for file chunking and GZIP formatting...')
        if filetype == 'zip':
            for i in range(1, iterations+1):
                file_chunk_threads.append(ZipToGzThread(i, skip, rows_to_read, filename, header,
                                                        dtypes, staging_filepath, zipname=zipname))
                skip += rows_to_read
        else:
            for i in range(1, iterations+1):
                file_chunk_threads.append(ZipToGzThread(i, skip, rows_to_read, filename, header,
                                                        dtypes, staging_filepath, zipname=None))
                skip += rows_to_read
        print(dt.datetime.now(), '***', 'Thread list created for file chunking and formatting')

        print(dt.datetime.now(), '***', 'Starting file chunking and formatting threads...')
        for fc_thread in file_chunk_threads:
            fc_thread.start()

        for fc_thread in file_chunk_threads:
            fc_thread.join()
        print(dt.datetime.now(), '***', 'Files chunked and formatted in folder:', staging_folder)
        
        # starting to stage files for transfer
        staging_files = []

        for root, dirs, files in os.walk(f"{staging_filepath}/."):
            for f in files:
                staging_files.append(f)
            
        print(dt.datetime.now(), '***', 'Total chunked files:', len(staging_files))     
        put_statements = []

        print(dt.datetime.now(), '***', 'Creating PUT statements for file chunks...')
        for staging_file in staging_files:
            put_statements.append(f'''
                                    PUT file://{staging_filepath}/{staging_file} @%{table_name} 
                                    SOURCE_COMPRESSION = GZIP
                                    PARALLEL = 20
                                    AUTO_COMPRESS = FALSE
                                   ''')
        print(dt.datetime.now(), '***', 'PUT statements created')                        

        put_threads = []
        put_counter = 0

        # create thread list
        print(dt.datetime.now(), '***', 'Creating thread list for PUT statements...')
        for statement in put_statements:
            put_threads.append(sfExecutionThread(put_counter, statement))
            put_counter += 1
            
        # execute the threads
        print(dt.datetime.now(), '***', 'Starting PUT threads...')
        for thread in put_threads:
            thread.start()

        for thread in put_threads:
            thread.join()
        print(dt.datetime.now(), '***', 'PUT threads complete. Data Staged')
        
        field_delimiter = snow.get_field_delimiter()
        record_delimiter = snow.get_record_delimiter()
     
        print(dt.datetime.now(), '***', 'Starting COPY INTO...')
        copy_into_sql = f"""COPY INTO {table_name} 
                                        FROM @%{table_name} 
                                        FILE_FORMAT = (REPLACE_INVALID_CHARACTERS = TRUE
                                                       SKIP_HEADER = 1
                                                       FIELD_DELIMITER = '{field_delimiter}'
                                                       RECORD_DELIMITER = '{record_delimiter}')
                                        ON_ERROR = CONTINUE"""
        snow.snowflake_cursor().cursor().execute(copy_into_sql)
        print(dt.datetime.now(), '***', 'COPY INTO complete. Data in', table_name)

        # clear staging table
        print(dt.datetime.now(), '***', 'Clearing staging table...')
        remove_staging_sql = f"REMOVE @%{table_name}"
        snow.snowflake_cursor().cursor().execute(remove_staging_sql)
        print(dt.datetime.now(), '***', 'Staging table cleared')

        # clear local staging folder
        if snow.get_rm_staging_files():
            print(dt.datetime.now(), '***', 'Clearing local staging folder...')
            for root, dirs, files in os.walk(staging_filepath):
                for f in files:
                    os.unlink(os.path.join(root, f))
            print(dt.datetime.now(), '***', 'Local staging folder cleared')

        if snow.get_rm_staging_folder():
            print(dt.datetime.now(), '***', 'Deleting staging folder...')
            os.rmdir(staging_filepath)
            print(dt.datetime.now(), '***', 'Local staging folder deleted')
  
    elif filetype == 'sql':
        print('This connector is not equipped to handle SQL transfers at this time')
    elif filetype in ['hive', 'hadoop']:
        print('This connector is not equipped to handle Hive/Hadoop transfers at this time')
    
    end_time = dt.datetime.now()
    print('Total file splitting, formatting, and upload process took', end_time - start_time)


if __name__ == '__main__':
    main()
