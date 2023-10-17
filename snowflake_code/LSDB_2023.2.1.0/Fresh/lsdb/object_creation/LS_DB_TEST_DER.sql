
-- ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST ADD COLUMN DER_LAB_TEST_DATE TEXT;

-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST_DER();


	
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_TEST_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_TEST_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST ADD COLUMN DER_LAB_TEST_DATE TEXT(80);
*/






insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_TEST_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_TEST_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST_DER';


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST_DER'),
	'LSRA','Case','LS_DB_TEST_DER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_TEST_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_TEST_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST)
WHERE TARGET_TABLE_NAME = 'LS_DB_TEST_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_CASE_QFC AS
select distinct RECORD_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_TEST_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_TEST_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_TEST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST   
SET LS_DB_TEST.DER_LAB_TEST_DATE=LS_DB_TEST_TMP.DER_LAB_TEST_DATE
FROM (
select record_id,
		case when TESTDATE_NF is not null then TESTDATE_NF
		when  TESTDATEFMT is null  then UPPER(to_char(TESTDATE,'DD-MON-YYYY'))
   		when  TESTDATEFMT IN (0,204)  then UPPER(to_char(TESTDATE,'DD-MON-YYYY'))
   		when  TESTDATEFMT IN (1,602)  then UPPER(to_char(TESTDATE,'YYYY'))
   		when  TESTDATEFMT IN (2,610)  then UPPER(to_char(TESTDATE,'MON-YYYY'))
   		when  TESTDATEFMT in (3,4,5,6,7,8,9,203,611,102)  then UPPER(to_char(TESTDATE,'DD-MON-YYYY'))
		end DER_LAB_TEST_DATE		
	from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_test where RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_CASE_QFC)
AND LS_DB_TEST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_TEST_TMP
    WHERE LS_DB_TEST.record_id = LS_DB_TEST_TMP.record_id	
	AND LS_DB_TEST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'); 





UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_TEST_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_TEST_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_TEST_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	


 
 
 
 
 





