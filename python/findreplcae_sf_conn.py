import sys
import os
import snowflake.connector
from getpass import getpass
from time import sleep
from pathlib import Path

connectionStr=Path('./connection.txt').read_text()

"""userinput = input("snowflake username: ")
passwordinput = getpass("Enter password: ")"""

target1 = "DATABRIDGE_JNJ_DB"
target2 = "DATABRIDGE_JNJ_SANDBOX_DB"
target1_previous_value = target1

print("establishing connection")
ctx = snowflake.connector.connect(user="DATABRIDGE_JNJ_SVC_USER",  password= connectionStr , account="lifesphere-datahub_preview")

print("connectrion successfull")
cs = ctx.cursor()

cs.execute("USE ROLE DATABRIDGE_JNJ_RW_ROLE;")
cs.execute("USE WAREHOUSE DATABRIDGE_JNJ_XSMALL_WH;") 
cs.execute("USE DATABASE DATABRIDGE_JNJ_DB;")
cs.execute("USE SCHEMA LSDB_TRANSFM;")

sql_filepath = "../snowflake_code/lsdb/object_creation"

for dir_path in sorted(os.listdir(sql_filepath)):
    target_table_nm = os.path.splitext(dir_path)[0]
    if target_table_nm.endswith("_DER"):
        target_table_nm = target_table_nm.replace("_DER","")

    full_path = os.path.join(sql_filepath, dir_path)
    print('Full path: ' + full_path)
    if full_path.endswith(".sql"):
        print('Executing script : %s' % full_path)
        stream_execute_file = "execute.sql"
        if target_table_nm.startswith("01_LSDB_"):
            target1 = target2
        else:
            target1 = target1_previous_value

        with open(full_path, 'r', encoding='utf-8') as s:
            rf = s.read()
            rf = rf.replace("$$TGT_DB_NAME.$$LSDB_TRANSFM." + target_table_nm + " ", target2 + ".$$LSDB_TRANSFM." + target_table_nm + " ")
            rf = rf.replace("$$TGT_DB_NAME.$$LSDB_TRANSFM." + target_table_nm + "\n", target2 + ".$$LSDB_TRANSFM." + target_table_nm + "\n")
            rf = rf.replace("$$LSDB_TRANSFM","LSDB_TRANSFM")
            rf = rf.replace("$$LSDB_RPL","LSDB_RPL")
            rf = rf.replace("$$TGT_DB_NAME",target1)  
            rf = rf.replace("$$STG_DB_NAME",target1)  
            rf = rf.replace("$$TGT_CNTRL_DB_NAME","DATABRIDGE_CNTRL_DICT") 
            rf = rf.replace("$$STG_CNTRL_DB_NAME","DATABRIDGE_CNTRL_DICT") 
            rf = rf.replace("$$LSDB_CNTRL_TRANSFM","LSDB_TRANSFM") 
            with open(stream_execute_file, "w") as wf:
                wf.write(rf)
            with open(stream_execute_file, 'r', encoding='utf-8') as f:
                for cur in ctx.execute_stream(f):
                    for ret in cur:
                     print(ret)
            os.remove(stream_execute_file)

cs.close()
ctx.close()