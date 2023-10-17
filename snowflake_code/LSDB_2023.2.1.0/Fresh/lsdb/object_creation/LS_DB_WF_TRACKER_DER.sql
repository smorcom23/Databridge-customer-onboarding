-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_TRACKER_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_WF_TRACKER_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_WF_TRACKER_DER';


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_WF_TRACKER_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_WF_TRACKER_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_TRACKER_DER'),
	'LSRA','Case','LS_DB_WF_TRACKER_DER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_WF_TRACKER_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER)
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_CASE_QFC AS
select distinct INTEGRATION_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_WF_TRACKER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER   
SET LS_DB_WF_TRACKER.DER_LATE_BY_DAYS=LS_DB_WF_TRACKER_TMP.DER_LATE_BY_DAYS  , 
LS_DB_WF_TRACKER.DER_LATENESS_FLAG=LS_DB_WF_TRACKER_TMP.DER_LATENESS_FLAG
FROM (select INTEGRATION_ID,
	CASE
	WHEN datediff(day, LS_DB_WF_TRACKER.due_date,LS_DB_WF_TRACKER.exit_time ) > 0 THEN CAST(datediff(day,LS_DB_WF_TRACKER.due_date,LS_DB_WF_TRACKER.exit_time) AS VARCHAR)
	WHEN datediff(day, LS_DB_WF_TRACKER.due_date,LS_DB_WF_TRACKER.exit_time ) <= 0 THEN '-'
	ELSE NULL END AS DER_LATE_BY_DAYS
	,
	CASE
	WHEN datediff(day, LS_DB_WF_TRACKER.due_date,LS_DB_WF_TRACKER.exit_time ) > 0 THEN 'Yes'
    WHEN datediff(day, LS_DB_WF_TRACKER.due_date,LS_DB_WF_TRACKER.exit_time ) <= 0 THEN 'No'
    ELSE NULL END DER_LATENESS_FLAG

from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER 
where INTEGRATION_ID in (select INTEGRATION_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_CASE_QFC)
AND LS_DB_WF_TRACKER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_WF_TRACKER_TMP
    WHERE LS_DB_WF_TRACKER.INTEGRATION_ID = LS_DB_WF_TRACKER_TMP.INTEGRATION_ID	
	AND LS_DB_WF_TRACKER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress') ;	


 RETURN 'LS_DB_WF_TRACKER_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

