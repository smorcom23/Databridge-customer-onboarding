
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WF_ASSIGNMENT()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_ASSIGNMENT'),
	'LSRA','Case','LS_DB_WF_ASSIGNMENT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_WF_ASSIGNMENT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_ASSIGNMENT'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_ASSIGNMENT'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_ASSIGNMENT_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_ASSIGNMENT_DELETION_TMP  As select RECORD_ID,'lsmv_workflow_assignments' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_workflow_assignments WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_DECODE
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_workflow_assignments WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_ASSIGNMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_ASSIGNMENT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_workflow_assignments WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_ASSIGNMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_ASSIGNMENT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_workflow_assignments_SUBSET AS 
(
select * from 
    (SELECT  
    assign_to  assign_to,assign_to_recid  assign_to_recid,condition_script  condition_script,date_created  date_created,date_modified  date_modified,fk_awfa_rec_id  fk_awfa_rec_id,object_id  object_id,record_id  record_id,script_name  script_name,script_record_id  script_record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_workflow_assignments
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_ASSIGNMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_assignments')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_workflow_assignments_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_workflow_assignments_SUBSET.USER_CREATED CREATED_BY,lsmv_workflow_assignments_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_workflow_assignments_SUBSET.user_modified  ,lsmv_workflow_assignments_SUBSET.user_created  ,lsmv_workflow_assignments_SUBSET.spr_id  ,lsmv_workflow_assignments_SUBSET.script_record_id  ,lsmv_workflow_assignments_SUBSET.script_name  ,lsmv_workflow_assignments_SUBSET.record_id  ,lsmv_workflow_assignments_SUBSET.object_id  ,lsmv_workflow_assignments_SUBSET.fk_awfa_rec_id  ,lsmv_workflow_assignments_SUBSET.date_modified  ,lsmv_workflow_assignments_SUBSET.date_created  ,lsmv_workflow_assignments_SUBSET.condition_script  ,lsmv_workflow_assignments_SUBSET.assign_to_recid  ,lsmv_workflow_assignments_SUBSET.assign_to ,CONCAT( NVL(lsmv_workflow_assignments_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_workflow_assignments_SUBSET  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP)
where target_table_name='LS_DB_WF_ASSIGNMENT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_ASSIGNMENT'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT   
SET LS_DB_WF_ASSIGNMENT.user_modified = LS_DB_WF_ASSIGNMENT_TMP.user_modified,LS_DB_WF_ASSIGNMENT.user_created = LS_DB_WF_ASSIGNMENT_TMP.user_created,LS_DB_WF_ASSIGNMENT.spr_id = LS_DB_WF_ASSIGNMENT_TMP.spr_id,LS_DB_WF_ASSIGNMENT.script_record_id = LS_DB_WF_ASSIGNMENT_TMP.script_record_id,LS_DB_WF_ASSIGNMENT.script_name = LS_DB_WF_ASSIGNMENT_TMP.script_name,LS_DB_WF_ASSIGNMENT.record_id = LS_DB_WF_ASSIGNMENT_TMP.record_id,LS_DB_WF_ASSIGNMENT.object_id = LS_DB_WF_ASSIGNMENT_TMP.object_id,LS_DB_WF_ASSIGNMENT.fk_awfa_rec_id = LS_DB_WF_ASSIGNMENT_TMP.fk_awfa_rec_id,LS_DB_WF_ASSIGNMENT.date_modified = LS_DB_WF_ASSIGNMENT_TMP.date_modified,LS_DB_WF_ASSIGNMENT.date_created = LS_DB_WF_ASSIGNMENT_TMP.date_created,LS_DB_WF_ASSIGNMENT.condition_script = LS_DB_WF_ASSIGNMENT_TMP.condition_script,LS_DB_WF_ASSIGNMENT.assign_to_recid = LS_DB_WF_ASSIGNMENT_TMP.assign_to_recid,LS_DB_WF_ASSIGNMENT.assign_to = LS_DB_WF_ASSIGNMENT_TMP.assign_to,
LS_DB_WF_ASSIGNMENT.PROCESSING_DT = LS_DB_WF_ASSIGNMENT_TMP.PROCESSING_DT ,
LS_DB_WF_ASSIGNMENT.expiry_date    =LS_DB_WF_ASSIGNMENT_TMP.expiry_date       ,
LS_DB_WF_ASSIGNMENT.created_by     =LS_DB_WF_ASSIGNMENT_TMP.created_by        ,
LS_DB_WF_ASSIGNMENT.created_dt     =LS_DB_WF_ASSIGNMENT_TMP.created_dt        ,
LS_DB_WF_ASSIGNMENT.load_ts        =LS_DB_WF_ASSIGNMENT_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP 
WHERE 	LS_DB_WF_ASSIGNMENT.INTEGRATION_ID = LS_DB_WF_ASSIGNMENT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_WF_ASSIGNMENT_TMP.PROCESSING_DT = LS_DB_WF_ASSIGNMENT.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
script_record_id,
script_name,
record_id,
object_id,
fk_awfa_rec_id,
date_modified,
date_created,
condition_script,
assign_to_recid,
assign_to)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
script_record_id,
script_name,
record_id,
object_id,
fk_awfa_rec_id,
date_modified,
date_created,
condition_script,
assign_to_recid,
assign_to
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT TGT)
                                                                                ; 
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_ASSIGNMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_assignments')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP 
WHERE 	TO_DATE(LS_DB_WF_ASSIGNMENT.PROCESSING_DT) < TO_DATE(LS_DB_WF_ASSIGNMENT_TMP.PROCESSING_DT)
AND LS_DB_WF_ASSIGNMENT.INTEGRATION_ID = LS_DB_WF_ASSIGNMENT_TMP.INTEGRATION_ID
AND LS_DB_WF_ASSIGNMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_ASSIGNMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_assignments')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_ASSIGNMENT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WF_ASSIGNMENT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_ASSIGNMENT'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_WF_ASSIGNMENT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_WF_ASSIGNMENT'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_WF_ASSIGNMENT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
