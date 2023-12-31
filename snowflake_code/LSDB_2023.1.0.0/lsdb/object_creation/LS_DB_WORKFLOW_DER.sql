--ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW ADD COLUMN DER_LATE_BY_DAYS TEXT;
--ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW ADD COLUMN DER_LATENESS_FLAG TEXT;
--delete from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WORKFLOW_DER'
--	select INTEGRATION_ID,DER_LATE_BY_DAYS,DER_LATENESS_FLAG from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW
-- call $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WORKFLOW_DER()
-- -- -- -- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WORKFLOW_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();




insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_WORKFLOW_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+2,'LS_DB_WORKFLOW_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)
;

INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WORKFLOW_DER'),
	'LSRA','Case','LS_DB_WORKFLOW_DER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_WORKFLOW_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_WORKFLOW_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW)
WHERE TARGET_TABLE_NAME = 'LS_DB_WORKFLOW_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW_CASE_QFC;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW_CASE_QFC AS
select distinct INTEGRATION_ID
FROM 
	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_WORKFLOW_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_WORKFLOW_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW   
SET LS_DB_WORKFLOW.DER_LATE_BY_DAYS=LS_DB_WORKFLOW_TMP.DER_LATE_BY_DAYS  , 
LS_DB_WORKFLOW.DER_LATENESS_FLAG=LS_DB_WORKFLOW_TMP.DER_LATENESS_FLAG
FROM (select INTEGRATION_ID,
	CASE
	WHEN datediff(day, LS_DB_WORKFLOW.wftrack_due_date,LS_DB_WORKFLOW.wftrack_exit_time ) > 0 THEN CAST(datediff(day,LS_DB_WORKFLOW.wftrack_due_date,LS_DB_WORKFLOW.wftrack_exit_time) AS VARCHAR)
	WHEN datediff(day, LS_DB_WORKFLOW.wftrack_due_date,LS_DB_WORKFLOW.wftrack_exit_time ) <= 0 THEN '-'
	ELSE NULL END AS DER_LATE_BY_DAYS
	,
	CASE
	WHEN datediff(day, LS_DB_WORKFLOW.wftrack_due_date,LS_DB_WORKFLOW.wftrack_exit_time ) > 0 THEN 'Yes'
    WHEN datediff(day, LS_DB_WORKFLOW.wftrack_due_date,LS_DB_WORKFLOW.wftrack_exit_time ) <= 0 THEN 'No'
    ELSE NULL END DER_LATENESS_FLAG

from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW 
where INTEGRATION_ID in (select INTEGRATION_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW_CASE_QFC)
AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_WORKFLOW_TMP
    WHERE LS_DB_WORKFLOW.INTEGRATION_ID = LS_DB_WORKFLOW_TMP.INTEGRATION_ID	
	AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');




UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WORKFLOW),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WORKFLOW_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WORKFLOW_DER'
and LOAD_STATUS = 'In Progress') ;	


 RETURN 'LS_DB_WORKFLOW_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_WORKFLOW_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

/*

 CREATE or replace TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_WORKFLOW
  WAREHOUSE = EXTRASMALL
  Schedule = '15 minute'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WORKFLOW();
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WORKFLOW_DER();
END;
$$
;

ALTER TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_WORKFLOW RESUME; 
 				*/