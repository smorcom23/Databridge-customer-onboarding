import sys
import os
import snowflake.connector
from getpass import getpass
from time import sleep
from pathlib import Path

connectionStr=Path('./connection.txt').read_text()

"""userinput = input("snowflake username: ")
passwordinput = getpass("Enter password: ")"""

target1 = "DATABRIDGE_CRO_DB"
target2 = "DATABRIDGE_CRO_DB"
target1_previous_value = target1

print("establishing connection")
ctx = snowflake.connector.connect(user="DATABRIDGE_CRO_SVC_USER",  password= connectionStr , account="lifesphere-datahub_preprod")

print("connectrion successfull")
cs = ctx.cursor()

cs.execute("USE ROLE DATABRIDGE_CRO_RW_ROLE;")
cs.execute("USE WAREHOUSE DATABRIDGE_CRO_XSMALL_WH;") 
"""cs.execute("USE DATABASE DATABRIDGE_JNJ_DB;")
cs.execute("USE SCHEMA LSDB_TRANSFM;")"""

"""sql_filepath = "../snowflake_code/lsdb_2023.2.0.0/Fresh/lsdb/object_creation"  """
sql_filepath = "../../snowflake_code/temp" 

for dir_path in sorted(os.listdir(sql_filepath)):
    target_table_nm = os.path.splitext(dir_path)[0]
    if target_table_nm.endswith("_DER"):
        target_table_nm = target_table_nm.replace("_DER","")

    full_path = os.path.join(sql_filepath, dir_path)
    print('Full path: ' + full_path)
    if full_path.endswith(".sql"):
        print('Executing script : %s' % full_path)
        stream_execute_file = "execute.sql"
        
        with open(full_path, 'r', encoding='utf-8') as s:
            rf = s.read()
            rf = rf.replace("${stage_db_name}",target1)
            rf = rf.replace("${stage_schema_name}","LSDB_RPL")
            rf = rf.replace("${tenant_transfm_db_name}",target1)  
            rf = rf.replace("${tenant_transfm_schema_name}","LSDB_TRANSFM")  
            rf = rf.replace("${cntrl_transfm_db_name}",target2) 
            rf = rf.replace("${cntrl_transfm_schema_name}","LSDB_CNTRL_TRANSFM")
            rf = rf.replace("${stage_cntrl_db_name}",target2)
            rf = rf.replace("${stage_cntrl_schema_name}","LSDB_CNTRL_RPL") 
            rf = rf.replace("${task_warehouse_name}","DATABRIDGE_CRO_XSMALL_WH")
            rf = rf.replace("${task_schedule_frequency}","200 minute")
            rf = rf.replace("${external_stage_name}","DATABRIDGE_QA_DB.LSDB_DMS_RPL.DMS_SNOWPIPE")
            rf = rf.replace("${lsmv_schema_anme}","lsmv2022_fresh_39_2023111")  
            
            with open(stream_execute_file, "w") as wf:
                wf.write(rf)
            with open(stream_execute_file, 'r', encoding='utf-8') as f:
                for cur in ctx.execute_stream(f):
                    for ret in cur:
                     print(ret)
            os.remove(stream_execute_file)

cs.close()
ctx.close()