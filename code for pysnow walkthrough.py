class SnowConnector():
    def __init__(self, creds):
        # set variables from your creds file
        return
        
    # define variable access functions
    
    def snowflake_cursor(self):
        e_counter = 0
        conn = None

        while e_counter < 5:
            try:
                conn = snowflake.connector.connect(
                                                    user=self.user,
                                                    authenticator='externalbrowser',
                                                    account=self.account,
                                                    warehouse=self.warehouse,
                                                    database=self.database,
                                                    schema=self.schema
                                                   )
                break
            except snowflake.connector.errors.DatabaseError as e:
                print(e)
                print('Connection to Snowflake refused, trying again...')
                e_counter += 1
                time.sleep(5)
        if not conn:
            print("""\n*****\n
                     Connection to Snowflake refused after 5 attempts. 
                     Please check connection credentials and 
                     connection to server/internet.
                     \n*****\n""")
            exit(1)
        print("Connected to Snowflake")
        return conn
       
    # define table checking and creation/replacing functions
    
    
class ZipToGzThread (threading.Thread):
    def __init__(self, thread_id, skip, rows_to_read, 
                 filename, header, dtypes,
                 staging_folder, zipname=None):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.skip = skip
        self.rows_to_read = rows_to_read
        self.filename = filename
        self.header = header.columns
        self.dtypes = dtypes
        self.staging_folder = staging_folder
        self.zipname = zipname
        
    def run(self):
        chunk_file(self.skip, self.rows_to_read, 
                   self.filename, self.header,
                   self.dtypes, self.threadID, 
                   self.staging_folder, self.zipname)
                   
def chunk_file(skip, rows_to_read, filename, 
               header, dtypes, threadID, 
               staging_filepath, zipname=None):
    if zipname:
        temp = pd.read_csv(zipname, skiprows=skip, 
                           nrows=rows_to_read, dtype=dtypes,
                           names=header, header=None, engine='c')
    else:
        temp = pd.read_csv(filename, skiprows=skip, 
                           nrows=rows_to_read, dtype=dtypes,
                           names=header, header=None, engine='c')
    print(f'read {len(temp)} lines starting at row {skip}')
    
    temp.to_csv(f'{staging_filepath}/{filename}_{threadID}.gz', 
                index=False, header=True,
                compression='gzip', chunksize=1000)
    print(f'sent thread ID {threadID} to local staging folder as GZIP')
    
    
class SfExecutionThread (threading.Thread):
    def __init__(self, thread_id, sql_query):
        threading.Thread.__init__(self)
        self.threadID = thread_id
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
    conn.cursor().execute("""ALTER SESSION SET 
                             STATEMENT_TIMEOUT_IN_SECONDS = 86400""")

    conn.cursor().execute(sf_query)
    conn.close()