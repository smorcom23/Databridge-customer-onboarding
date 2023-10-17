
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ST_BATCH_SUB_QUEUE()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;

CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP (
ROW_WID	NUMBER(38,0),
FUNCTIONAL_AREA	VARCHAR(25),
ENTITY_NAME	VARCHAR(25),
TARGET_TABLE_NAME	VARCHAR(100),
LOAD_TS	TIMESTAMP_NTZ(9),
LOAD_START_TS	TIMESTAMP_NTZ(9),
LOAD_END_TS	TIMESTAMP_NTZ(9),
REC_READ_CNT	NUMBER(38,0),
REC_PROCESSED_CNT	NUMBER(38,0),
ERROR_REC_CNT	NUMBER(38,0),
ERROR_DETAILS	VARCHAR(8000),
LOAD_STATUS	VARCHAR(15),
CHANGED_REC_SET	VARIANT);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ST_BATCH_SUB_QUEUE'),
	'LSDB','Case','LS_DB_ST_BATCH_SUB_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_ST_BATCH_SUB_QUEUE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_ST_BATCH_SUB_QUEUE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_BATCH_SUB_QUEUE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_BATCH_SUB_QUEUE_DELETION_TMP  As select RECORD_ID,'lsmv_st_batch_sub_queue' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_batch_sub_queue WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					), D_MEDDRA_ICD_SUBSET AS 
( select distinct BK_MEDDRA_ICD_WID,LLT_CODE,PT_CODE,PRIMARY_SOC_FG from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_batch_sub_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ST_BATCH_SUB_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_batch_sub_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ST_BATCH_SUB_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_st_batch_sub_queue_SUBSET AS 
(
select * from 
    (SELECT  
    batch_date_completed  batch_date_completed,batch_date_created  batch_date_created,batch_doc_size  batch_doc_size,batch_repo_doc_id  batch_repo_doc_id,batch_search_criteria  batch_search_criteria,batch_submitted_on  batch_submitted_on,batch_type  batch_type,batch_zip_filename  batch_zip_filename,comments  comments,date_created  date_created,date_modified  date_modified,distribution_contact  distribution_contact,distribution_format_name  distribution_format_name,doc_type  doc_type,doctype_name  doctype_name,error_message  error_message,fk_ddc_contact_id  fk_ddc_contact_id,is_complete  is_complete,is_deleted  is_deleted,log_file_json_id  log_file_json_id,machine_name  machine_name,no_of_sub_per_batch  no_of_sub_per_batch,presigned_url  presigned_url,processed_at  processed_at,processing_started_date_at  processing_started_date_at,re_run_count  re_run_count,record_id  record_id,report_format  report_format,retry_after  retry_after,retry_count  retry_count,spr_id  spr_id,stage  stage,status  status,sub_queue_batch_id  sub_queue_batch_id,submission_rec_ids  submission_rec_ids,total_cases_count  total_cases_count,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_batch_sub_queue
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_BATCH_SUB_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_batch_sub_queue')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_st_batch_sub_queue_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_st_batch_sub_queue_SUBSET.USER_CREATED CREATED_BY,lsmv_st_batch_sub_queue_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_batch_sub_queue_SUBSET.user_modified  ,lsmv_st_batch_sub_queue_SUBSET.user_created  ,lsmv_st_batch_sub_queue_SUBSET.total_cases_count  ,lsmv_st_batch_sub_queue_SUBSET.submission_rec_ids  ,lsmv_st_batch_sub_queue_SUBSET.sub_queue_batch_id  ,lsmv_st_batch_sub_queue_SUBSET.status  ,lsmv_st_batch_sub_queue_SUBSET.stage  ,lsmv_st_batch_sub_queue_SUBSET.spr_id  ,lsmv_st_batch_sub_queue_SUBSET.retry_count  ,lsmv_st_batch_sub_queue_SUBSET.retry_after  ,lsmv_st_batch_sub_queue_SUBSET.report_format  ,lsmv_st_batch_sub_queue_SUBSET.record_id  ,lsmv_st_batch_sub_queue_SUBSET.re_run_count  ,lsmv_st_batch_sub_queue_SUBSET.processing_started_date_at  ,lsmv_st_batch_sub_queue_SUBSET.processed_at  ,lsmv_st_batch_sub_queue_SUBSET.presigned_url  ,lsmv_st_batch_sub_queue_SUBSET.no_of_sub_per_batch  ,lsmv_st_batch_sub_queue_SUBSET.machine_name  ,lsmv_st_batch_sub_queue_SUBSET.log_file_json_id  ,lsmv_st_batch_sub_queue_SUBSET.is_deleted  ,lsmv_st_batch_sub_queue_SUBSET.is_complete  ,lsmv_st_batch_sub_queue_SUBSET.fk_ddc_contact_id  ,lsmv_st_batch_sub_queue_SUBSET.error_message  ,lsmv_st_batch_sub_queue_SUBSET.doctype_name  ,lsmv_st_batch_sub_queue_SUBSET.doc_type  ,lsmv_st_batch_sub_queue_SUBSET.distribution_format_name  ,lsmv_st_batch_sub_queue_SUBSET.distribution_contact  ,lsmv_st_batch_sub_queue_SUBSET.date_modified  ,lsmv_st_batch_sub_queue_SUBSET.date_created  ,lsmv_st_batch_sub_queue_SUBSET.comments  ,lsmv_st_batch_sub_queue_SUBSET.batch_zip_filename  ,lsmv_st_batch_sub_queue_SUBSET.batch_type  ,lsmv_st_batch_sub_queue_SUBSET.batch_submitted_on  ,lsmv_st_batch_sub_queue_SUBSET.batch_search_criteria  ,lsmv_st_batch_sub_queue_SUBSET.batch_repo_doc_id  ,lsmv_st_batch_sub_queue_SUBSET.batch_doc_size  ,lsmv_st_batch_sub_queue_SUBSET.batch_date_created  ,lsmv_st_batch_sub_queue_SUBSET.batch_date_completed ,CONCAT( NVL(lsmv_st_batch_sub_queue_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_batch_sub_queue_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP)
where target_table_name='LS_DB_ST_BATCH_SUB_QUEUE'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE   
SET LS_DB_ST_BATCH_SUB_QUEUE.user_modified = LS_DB_ST_BATCH_SUB_QUEUE_TMP.user_modified,LS_DB_ST_BATCH_SUB_QUEUE.user_created = LS_DB_ST_BATCH_SUB_QUEUE_TMP.user_created,LS_DB_ST_BATCH_SUB_QUEUE.total_cases_count = LS_DB_ST_BATCH_SUB_QUEUE_TMP.total_cases_count,LS_DB_ST_BATCH_SUB_QUEUE.submission_rec_ids = LS_DB_ST_BATCH_SUB_QUEUE_TMP.submission_rec_ids,LS_DB_ST_BATCH_SUB_QUEUE.sub_queue_batch_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.sub_queue_batch_id,LS_DB_ST_BATCH_SUB_QUEUE.status = LS_DB_ST_BATCH_SUB_QUEUE_TMP.status,LS_DB_ST_BATCH_SUB_QUEUE.stage = LS_DB_ST_BATCH_SUB_QUEUE_TMP.stage,LS_DB_ST_BATCH_SUB_QUEUE.spr_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.spr_id,LS_DB_ST_BATCH_SUB_QUEUE.retry_count = LS_DB_ST_BATCH_SUB_QUEUE_TMP.retry_count,LS_DB_ST_BATCH_SUB_QUEUE.retry_after = LS_DB_ST_BATCH_SUB_QUEUE_TMP.retry_after,LS_DB_ST_BATCH_SUB_QUEUE.report_format = LS_DB_ST_BATCH_SUB_QUEUE_TMP.report_format,LS_DB_ST_BATCH_SUB_QUEUE.record_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.record_id,LS_DB_ST_BATCH_SUB_QUEUE.re_run_count = LS_DB_ST_BATCH_SUB_QUEUE_TMP.re_run_count,LS_DB_ST_BATCH_SUB_QUEUE.processing_started_date_at = LS_DB_ST_BATCH_SUB_QUEUE_TMP.processing_started_date_at,LS_DB_ST_BATCH_SUB_QUEUE.processed_at = LS_DB_ST_BATCH_SUB_QUEUE_TMP.processed_at,LS_DB_ST_BATCH_SUB_QUEUE.presigned_url = LS_DB_ST_BATCH_SUB_QUEUE_TMP.presigned_url,LS_DB_ST_BATCH_SUB_QUEUE.no_of_sub_per_batch = LS_DB_ST_BATCH_SUB_QUEUE_TMP.no_of_sub_per_batch,LS_DB_ST_BATCH_SUB_QUEUE.machine_name = LS_DB_ST_BATCH_SUB_QUEUE_TMP.machine_name,LS_DB_ST_BATCH_SUB_QUEUE.log_file_json_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.log_file_json_id,LS_DB_ST_BATCH_SUB_QUEUE.is_deleted = LS_DB_ST_BATCH_SUB_QUEUE_TMP.is_deleted,LS_DB_ST_BATCH_SUB_QUEUE.is_complete = LS_DB_ST_BATCH_SUB_QUEUE_TMP.is_complete,LS_DB_ST_BATCH_SUB_QUEUE.fk_ddc_contact_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.fk_ddc_contact_id,LS_DB_ST_BATCH_SUB_QUEUE.error_message = LS_DB_ST_BATCH_SUB_QUEUE_TMP.error_message,LS_DB_ST_BATCH_SUB_QUEUE.doctype_name = LS_DB_ST_BATCH_SUB_QUEUE_TMP.doctype_name,LS_DB_ST_BATCH_SUB_QUEUE.doc_type = LS_DB_ST_BATCH_SUB_QUEUE_TMP.doc_type,LS_DB_ST_BATCH_SUB_QUEUE.distribution_format_name = LS_DB_ST_BATCH_SUB_QUEUE_TMP.distribution_format_name,LS_DB_ST_BATCH_SUB_QUEUE.distribution_contact = LS_DB_ST_BATCH_SUB_QUEUE_TMP.distribution_contact,LS_DB_ST_BATCH_SUB_QUEUE.date_modified = LS_DB_ST_BATCH_SUB_QUEUE_TMP.date_modified,LS_DB_ST_BATCH_SUB_QUEUE.date_created = LS_DB_ST_BATCH_SUB_QUEUE_TMP.date_created,LS_DB_ST_BATCH_SUB_QUEUE.comments = LS_DB_ST_BATCH_SUB_QUEUE_TMP.comments,LS_DB_ST_BATCH_SUB_QUEUE.batch_zip_filename = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_zip_filename,LS_DB_ST_BATCH_SUB_QUEUE.batch_type = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_type,LS_DB_ST_BATCH_SUB_QUEUE.batch_submitted_on = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_submitted_on,LS_DB_ST_BATCH_SUB_QUEUE.batch_search_criteria = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_search_criteria,LS_DB_ST_BATCH_SUB_QUEUE.batch_repo_doc_id = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_repo_doc_id,LS_DB_ST_BATCH_SUB_QUEUE.batch_doc_size = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_doc_size,LS_DB_ST_BATCH_SUB_QUEUE.batch_date_created = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_date_created,LS_DB_ST_BATCH_SUB_QUEUE.batch_date_completed = LS_DB_ST_BATCH_SUB_QUEUE_TMP.batch_date_completed,
LS_DB_ST_BATCH_SUB_QUEUE.PROCESSING_DT = LS_DB_ST_BATCH_SUB_QUEUE_TMP.PROCESSING_DT ,
LS_DB_ST_BATCH_SUB_QUEUE.expiry_date    =LS_DB_ST_BATCH_SUB_QUEUE_TMP.expiry_date       ,
LS_DB_ST_BATCH_SUB_QUEUE.created_by     =LS_DB_ST_BATCH_SUB_QUEUE_TMP.created_by        ,
LS_DB_ST_BATCH_SUB_QUEUE.created_dt     =LS_DB_ST_BATCH_SUB_QUEUE_TMP.created_dt        ,
LS_DB_ST_BATCH_SUB_QUEUE.load_ts        =LS_DB_ST_BATCH_SUB_QUEUE_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP 
WHERE 	LS_DB_ST_BATCH_SUB_QUEUE.INTEGRATION_ID = LS_DB_ST_BATCH_SUB_QUEUE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_ST_BATCH_SUB_QUEUE_TMP.PROCESSING_DT = LS_DB_ST_BATCH_SUB_QUEUE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
total_cases_count,
submission_rec_ids,
sub_queue_batch_id,
status,
stage,
spr_id,
retry_count,
retry_after,
report_format,
record_id,
re_run_count,
processing_started_date_at,
processed_at,
presigned_url,
no_of_sub_per_batch,
machine_name,
log_file_json_id,
is_deleted,
is_complete,
fk_ddc_contact_id,
error_message,
doctype_name,
doc_type,
distribution_format_name,
distribution_contact,
date_modified,
date_created,
comments,
batch_zip_filename,
batch_type,
batch_submitted_on,
batch_search_criteria,
batch_repo_doc_id,
batch_doc_size,
batch_date_created,
batch_date_completed)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
total_cases_count,
submission_rec_ids,
sub_queue_batch_id,
status,
stage,
spr_id,
retry_count,
retry_after,
report_format,
record_id,
re_run_count,
processing_started_date_at,
processed_at,
presigned_url,
no_of_sub_per_batch,
machine_name,
log_file_json_id,
is_deleted,
is_complete,
fk_ddc_contact_id,
error_message,
doctype_name,
doc_type,
distribution_format_name,
distribution_contact,
date_modified,
date_created,
comments,
batch_zip_filename,
batch_type,
batch_submitted_on,
batch_search_criteria,
batch_repo_doc_id,
batch_doc_size,
batch_date_created,
batch_date_completed
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP 
WHERE 	TO_DATE(LS_DB_ST_BATCH_SUB_QUEUE.PROCESSING_DT) < TO_DATE(LS_DB_ST_BATCH_SUB_QUEUE_TMP.PROCESSING_DT)
AND LS_DB_ST_BATCH_SUB_QUEUE.INTEGRATION_ID = LS_DB_ST_BATCH_SUB_QUEUE_TMP.INTEGRATION_ID
AND LS_DB_ST_BATCH_SUB_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_BATCH_SUB_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_batch_sub_queue')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_BATCH_SUB_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_batch_sub_queue')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_BATCH_SUB_QUEUE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_ST_BATCH_SUB_QUEUE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_ST_BATCH_SUB_QUEUE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ST_BATCH_SUB_QUEUE'),
	'LSDB','Case','LS_DB_ST_BATCH_SUB_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_ST_BATCH_SUB_QUEUE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
