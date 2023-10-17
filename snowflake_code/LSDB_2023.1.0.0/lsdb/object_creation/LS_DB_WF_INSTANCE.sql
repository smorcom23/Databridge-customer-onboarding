
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_WF_INSTANCE()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_INSTANCE'),
	'LSRA','Case','LS_DB_WF_INSTANCE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_WF_INSTANCE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_INSTANCE'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_WF_INSTANCE'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_INSTANCE_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_INSTANCE_DELETION_TMP  As select RECORD_ID,'lsmv_wf_instance' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_wf_instance WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_wf_instance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_INSTANCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_INSTANCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_awf_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_wf_instance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_INSTANCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_WF_INSTANCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_wf_instance_SUBSET AS 
(
select * from 
    (SELECT  
    auto_complete_first_activity  auto_complete_first_activity,completion_flag  completion_flag,date_created  date_created,date_modified  date_modified,fk_awf_rec_id  fk_awf_rec_id,fk_awfa_rec_id  fk_awfa_rec_id,fk_jbpm_process_inst_id  fk_jbpm_process_inst_id,last_transition  last_transition,next_transition  next_transition,next_transition_note  next_transition_note,record_id  record_id,sampling_flag  sampling_flag,spr_id  spr_id,status  status,user_created  user_created,user_modified  user_modified,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_wf_instance
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_awf_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_INSTANCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_instance')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_wf_instance_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_wf_instance_SUBSET.USER_CREATED CREATED_BY,lsmv_wf_instance_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_wf_instance_SUBSET.version  ,lsmv_wf_instance_SUBSET.user_modified  ,lsmv_wf_instance_SUBSET.user_created  ,lsmv_wf_instance_SUBSET.status  ,lsmv_wf_instance_SUBSET.spr_id  ,lsmv_wf_instance_SUBSET.sampling_flag  ,lsmv_wf_instance_SUBSET.record_id  ,lsmv_wf_instance_SUBSET.next_transition_note  ,lsmv_wf_instance_SUBSET.next_transition  ,lsmv_wf_instance_SUBSET.last_transition  ,lsmv_wf_instance_SUBSET.fk_jbpm_process_inst_id  ,lsmv_wf_instance_SUBSET.fk_awfa_rec_id  ,lsmv_wf_instance_SUBSET.fk_awf_rec_id  ,lsmv_wf_instance_SUBSET.date_modified  ,lsmv_wf_instance_SUBSET.date_created  ,lsmv_wf_instance_SUBSET.completion_flag  ,lsmv_wf_instance_SUBSET.auto_complete_first_activity ,CONCAT( NVL(lsmv_wf_instance_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_wf_instance_SUBSET  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP)
where target_table_name='LS_DB_WF_INSTANCE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_INSTANCE'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE   
SET LS_DB_WF_INSTANCE.version = LS_DB_WF_INSTANCE_TMP.version,LS_DB_WF_INSTANCE.user_modified = LS_DB_WF_INSTANCE_TMP.user_modified,LS_DB_WF_INSTANCE.user_created = LS_DB_WF_INSTANCE_TMP.user_created,LS_DB_WF_INSTANCE.status = LS_DB_WF_INSTANCE_TMP.status,LS_DB_WF_INSTANCE.spr_id = LS_DB_WF_INSTANCE_TMP.spr_id,LS_DB_WF_INSTANCE.sampling_flag = LS_DB_WF_INSTANCE_TMP.sampling_flag,LS_DB_WF_INSTANCE.record_id = LS_DB_WF_INSTANCE_TMP.record_id,LS_DB_WF_INSTANCE.next_transition_note = LS_DB_WF_INSTANCE_TMP.next_transition_note,LS_DB_WF_INSTANCE.next_transition = LS_DB_WF_INSTANCE_TMP.next_transition,LS_DB_WF_INSTANCE.last_transition = LS_DB_WF_INSTANCE_TMP.last_transition,LS_DB_WF_INSTANCE.fk_jbpm_process_inst_id = LS_DB_WF_INSTANCE_TMP.fk_jbpm_process_inst_id,LS_DB_WF_INSTANCE.fk_awfa_rec_id = LS_DB_WF_INSTANCE_TMP.fk_awfa_rec_id,LS_DB_WF_INSTANCE.fk_awf_rec_id = LS_DB_WF_INSTANCE_TMP.fk_awf_rec_id,LS_DB_WF_INSTANCE.date_modified = LS_DB_WF_INSTANCE_TMP.date_modified,LS_DB_WF_INSTANCE.date_created = LS_DB_WF_INSTANCE_TMP.date_created,LS_DB_WF_INSTANCE.completion_flag = LS_DB_WF_INSTANCE_TMP.completion_flag,LS_DB_WF_INSTANCE.auto_complete_first_activity = LS_DB_WF_INSTANCE_TMP.auto_complete_first_activity,
LS_DB_WF_INSTANCE.PROCESSING_DT = LS_DB_WF_INSTANCE_TMP.PROCESSING_DT ,
LS_DB_WF_INSTANCE.expiry_date    =LS_DB_WF_INSTANCE_TMP.expiry_date       ,
LS_DB_WF_INSTANCE.created_by     =LS_DB_WF_INSTANCE_TMP.created_by        ,
LS_DB_WF_INSTANCE.created_dt     =LS_DB_WF_INSTANCE_TMP.created_dt        ,
LS_DB_WF_INSTANCE.load_ts        =LS_DB_WF_INSTANCE_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP 
WHERE 	LS_DB_WF_INSTANCE.INTEGRATION_ID = LS_DB_WF_INSTANCE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_WF_INSTANCE_TMP.PROCESSING_DT = LS_DB_WF_INSTANCE.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
status,
spr_id,
sampling_flag,
record_id,
next_transition_note,
next_transition,
last_transition,
fk_jbpm_process_inst_id,
fk_awfa_rec_id,
fk_awf_rec_id,
date_modified,
date_created,
completion_flag,
auto_complete_first_activity)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
status,
spr_id,
sampling_flag,
record_id,
next_transition_note,
next_transition,
last_transition,
fk_jbpm_process_inst_id,
fk_awfa_rec_id,
fk_awf_rec_id,
date_modified,
date_created,
completion_flag,
auto_complete_first_activity
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP 
WHERE 	TO_DATE(LS_DB_WF_INSTANCE.PROCESSING_DT) < TO_DATE(LS_DB_WF_INSTANCE_TMP.PROCESSING_DT)
AND LS_DB_WF_INSTANCE.INTEGRATION_ID = LS_DB_WF_INSTANCE_TMP.INTEGRATION_ID
AND LS_DB_WF_INSTANCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_INSTANCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_instance')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_WF_INSTANCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_instance')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_WF_INSTANCE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WF_INSTANCE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_INSTANCE'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_WF_INSTANCE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_WF_INSTANCE'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_WF_INSTANCE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
