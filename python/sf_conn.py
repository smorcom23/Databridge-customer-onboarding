import sys
import os
import snowflake.connector
from getpass import getpass
from time import sleep
from pathlib import Path

connectionStr=Path('./connection.txt').read_text()

"""userinput = input("snowflake username: ")
passwordinput = getpass("Enter password: ")"""

STREAM_EXECUTE_FILE = "execute.sql"

print("establishing connection")
ctx = snowflake.connector.connect(user="DATABRIDGE_QA_SVC_USER",  password= connectionStr , account="lifesphere-datahub_preprod")

print("connectrion successfull")
cs = ctx.cursor()

cs.execute("USE ROLE DATABRIDGE_QA_RW_ROLE;")
cs.execute("USE WAREHOUSE DATABRIDGE_QA_WH;") 
cs.execute("USE DATABASE DATABRIDGE_QA_DB;")
cs.execute("USE SCHEMA LSDB_DMS_RPL;")

sql_filepath = "../examples/run_sql/"

for dir_path in sorted(os.listdir(sql_filepath)):
    full_path = os.path.join(sql_filepath, dir_path)
    print('Full path: ' + full_path)
    if full_path.endswith(".sql"):
        print('Executing script : %s' % full_path)
        with open(full_path, 'r', encoding='utf-8') as f:
            for cur in ctx.execute_stream(f):
                for ret in cur:
                    print(ret)

cs.close()
ctx.close()