
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_TRACKER()
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
ROW_WID           NUMBER(38,0),
FUNCTIONAL_AREA        VARCHAR(25),
ENTITY_NAME   VARCHAR(25),
TARGET_TABLE_NAME   VARCHAR(100),
LOAD_TS              TIMESTAMP_NTZ(9),
LOAD_START_TS               TIMESTAMP_NTZ(9),
LOAD_END_TS   TIMESTAMP_NTZ(9),
REC_READ_CNT NUMBER(38,0),
REC_PROCESSED_CNT    NUMBER(38,0),
ERROR_REC_CNT              NUMBER(38,0),
ERROR_DETAILS VARCHAR(8000),
LOAD_STATUS   VARCHAR(15),
CHANGED_REC_SET        VARIANT);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_TRACKER'),
                'LSDB','Case','LS_DB_WF_TRACKER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_WF_TRACKER' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_WF_TRACKER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_DELETION_TMP  As select RECORD_ID,'lsmv_wf_tracker' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('7077','801')
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

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WF_TRACKER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT wf_activity_record_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WF_TRACKER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
,
                LSMV_COMMON_COLUMN_SUBSET as
(   select RECORD_ID,common_parent_key,ARI_REC_ID,CASE_NO,AER_VERSION_NO,RECEIPT_ID,RECEIPT_NO,VERSION_NO 
              from     (
                                                          select LSMV_CASE_NO_SUBSET.RECORD_ID,LSMV_CASE_NO_SUBSET.common_parent_key,AER_INFO.ARI_REC_ID, AER_INFO.AER_NO CASE_NO, AER_INFO.AER_VERSION_NO, RECPT_ITM.RECORD_ID RECEIPT_ID,
                                                                                      RECPT_ITM.RECEIPT_NO RECEIPT_NO,RECPT_ITM.VERSION VERSION_NO , 
                                                                                      row_number () OVER ( PARTITION BY LSMV_CASE_NO_SUBSET.RECORD_ID,RECPT_ITM.RECORD_ID ORDER BY to_date(GREATEST(
                                                                                                                                                                             NVL(RECPT_ITM.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(AER_INFO.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
                                                                                                                                                                                            )) DESC ) REC_RANK
                                                                                      from ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO AER_INFO,${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_wf_tracker_SUBSET AS 
(
select * from 
    (SELECT  
    action_quality  action_quality,aer_local_version_no  aer_local_version_no,aer_no  aer_no,aer_version_no  aer_version_no,ari_rec_id  ari_rec_id,assign_to  assign_to,assigned_to_name  assigned_to_name,auto_actions_details  auto_actions_details,autocomplete  autocomplete,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(autocomplete AS VARCHAR(100)) )autocomplete_de_ml , autocomplete_failed  autocomplete_failed,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,due_date  due_date,entry_time  entry_time,entry_user  entry_user,exit_time  exit_time,exit_user  exit_user,fk_aim_record_id  fk_aim_record_id,fk_lsm_rec_id  fk_lsm_rec_id,fk_lwaq_rec_id  fk_lwaq_rec_id,inq_rec_id  inq_rec_id,lateness_reason  lateness_reason,lateness_reason_code  lateness_reason_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='801' AND CD=CAST(lateness_reason_code AS VARCHAR(100)) )lateness_reason_code_de_ml , literature_record_id  literature_record_id,no_of_days_spent  no_of_days_spent,out_of_wf  out_of_wf,previous_wf  previous_wf,previous_wf_actvity_id  previous_wf_actvity_id,processing_unit  processing_unit,quality_flag  quality_flag,quality_json  quality_json,quality_review_fail_status  quality_review_fail_status,quality_review_status  quality_review_status,quality_scores  quality_scores,receipt_no  receipt_no,record_id  record_id,sampling_date  sampling_date,sampling_flag  sampling_flag,sampling_recid  sampling_recid,sender_organization_name  sender_organization_name,seq  seq,spr_id  spr_id,trackertype  trackertype,user_created  user_created,user_modified  user_modified,wf_activity_name  wf_activity_name,wf_activity_record_id  wf_activity_record_id,wf_name  wf_name,wf_next_activity_record_id  wf_next_activity_record_id,wf_record_id  wf_record_id,wf_transition_name  wf_transition_name,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
wf_activity_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_tracker')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_wf_tracker_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE                ,lsmv_wf_tracker_SUBSET.USER_CREATED CREATED_BY,lsmv_wf_tracker_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_wf_tracker_SUBSET.wf_transition_name  ,lsmv_wf_tracker_SUBSET.wf_record_id  ,lsmv_wf_tracker_SUBSET.wf_next_activity_record_id  ,lsmv_wf_tracker_SUBSET.wf_name  ,lsmv_wf_tracker_SUBSET.wf_activity_record_id  ,lsmv_wf_tracker_SUBSET.wf_activity_name  ,lsmv_wf_tracker_SUBSET.user_modified  ,lsmv_wf_tracker_SUBSET.user_created  ,lsmv_wf_tracker_SUBSET.trackertype  ,lsmv_wf_tracker_SUBSET.spr_id  ,lsmv_wf_tracker_SUBSET.seq  ,lsmv_wf_tracker_SUBSET.sender_organization_name  ,lsmv_wf_tracker_SUBSET.sampling_recid  ,lsmv_wf_tracker_SUBSET.sampling_flag  ,lsmv_wf_tracker_SUBSET.sampling_date  ,lsmv_wf_tracker_SUBSET.record_id  ,lsmv_wf_tracker_SUBSET.receipt_no  ,lsmv_wf_tracker_SUBSET.quality_scores  ,lsmv_wf_tracker_SUBSET.quality_review_status  ,lsmv_wf_tracker_SUBSET.quality_review_fail_status  ,lsmv_wf_tracker_SUBSET.quality_json  ,lsmv_wf_tracker_SUBSET.quality_flag  ,lsmv_wf_tracker_SUBSET.processing_unit  ,lsmv_wf_tracker_SUBSET.previous_wf_actvity_id  ,lsmv_wf_tracker_SUBSET.previous_wf  ,lsmv_wf_tracker_SUBSET.out_of_wf  ,lsmv_wf_tracker_SUBSET.no_of_days_spent  ,lsmv_wf_tracker_SUBSET.literature_record_id  ,lsmv_wf_tracker_SUBSET.lateness_reason_code_de_ml  ,lsmv_wf_tracker_SUBSET.lateness_reason_code  ,lsmv_wf_tracker_SUBSET.lateness_reason  ,lsmv_wf_tracker_SUBSET.inq_rec_id  ,lsmv_wf_tracker_SUBSET.fk_lwaq_rec_id  ,lsmv_wf_tracker_SUBSET.fk_lsm_rec_id  ,lsmv_wf_tracker_SUBSET.fk_aim_record_id  ,lsmv_wf_tracker_SUBSET.exit_user  ,lsmv_wf_tracker_SUBSET.exit_time  ,lsmv_wf_tracker_SUBSET.entry_user  ,lsmv_wf_tracker_SUBSET.entry_time  ,lsmv_wf_tracker_SUBSET.due_date  ,lsmv_wf_tracker_SUBSET.date_modified  ,lsmv_wf_tracker_SUBSET.date_created  ,lsmv_wf_tracker_SUBSET.comp_rec_id  ,lsmv_wf_tracker_SUBSET.autocomplete_failed  ,lsmv_wf_tracker_SUBSET.autocomplete_de_ml  ,lsmv_wf_tracker_SUBSET.autocomplete  ,lsmv_wf_tracker_SUBSET.auto_actions_details  ,lsmv_wf_tracker_SUBSET.assigned_to_name  ,lsmv_wf_tracker_SUBSET.assign_to  ,lsmv_wf_tracker_SUBSET.ari_rec_id  ,lsmv_wf_tracker_SUBSET.aer_version_no  ,lsmv_wf_tracker_SUBSET.aer_no  ,lsmv_wf_tracker_SUBSET.aer_local_version_no  ,lsmv_wf_tracker_SUBSET.action_quality ,CONCAT(NVL(lsmv_wf_tracker_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_wf_tracker_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_wf_tracker_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP)
where target_table_name='LS_DB_WF_TRACKER'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER   
SET LS_DB_WF_TRACKER.wf_transition_name = LS_DB_WF_TRACKER_TMP.wf_transition_name,LS_DB_WF_TRACKER.wf_record_id = LS_DB_WF_TRACKER_TMP.wf_record_id,LS_DB_WF_TRACKER.wf_next_activity_record_id = LS_DB_WF_TRACKER_TMP.wf_next_activity_record_id,LS_DB_WF_TRACKER.wf_name = LS_DB_WF_TRACKER_TMP.wf_name,LS_DB_WF_TRACKER.wf_activity_record_id = LS_DB_WF_TRACKER_TMP.wf_activity_record_id,LS_DB_WF_TRACKER.wf_activity_name = LS_DB_WF_TRACKER_TMP.wf_activity_name,LS_DB_WF_TRACKER.user_modified = LS_DB_WF_TRACKER_TMP.user_modified,LS_DB_WF_TRACKER.user_created = LS_DB_WF_TRACKER_TMP.user_created,LS_DB_WF_TRACKER.trackertype = LS_DB_WF_TRACKER_TMP.trackertype,LS_DB_WF_TRACKER.spr_id = LS_DB_WF_TRACKER_TMP.spr_id,LS_DB_WF_TRACKER.seq = LS_DB_WF_TRACKER_TMP.seq,LS_DB_WF_TRACKER.sender_organization_name = LS_DB_WF_TRACKER_TMP.sender_organization_name,LS_DB_WF_TRACKER.sampling_recid = LS_DB_WF_TRACKER_TMP.sampling_recid,LS_DB_WF_TRACKER.sampling_flag = LS_DB_WF_TRACKER_TMP.sampling_flag,LS_DB_WF_TRACKER.sampling_date = LS_DB_WF_TRACKER_TMP.sampling_date,LS_DB_WF_TRACKER.record_id = LS_DB_WF_TRACKER_TMP.record_id,LS_DB_WF_TRACKER.receipt_no = LS_DB_WF_TRACKER_TMP.receipt_no,LS_DB_WF_TRACKER.quality_scores = LS_DB_WF_TRACKER_TMP.quality_scores,LS_DB_WF_TRACKER.quality_review_status = LS_DB_WF_TRACKER_TMP.quality_review_status,LS_DB_WF_TRACKER.quality_review_fail_status = LS_DB_WF_TRACKER_TMP.quality_review_fail_status,LS_DB_WF_TRACKER.quality_json = LS_DB_WF_TRACKER_TMP.quality_json,LS_DB_WF_TRACKER.quality_flag = LS_DB_WF_TRACKER_TMP.quality_flag,LS_DB_WF_TRACKER.processing_unit = LS_DB_WF_TRACKER_TMP.processing_unit,LS_DB_WF_TRACKER.previous_wf_actvity_id = LS_DB_WF_TRACKER_TMP.previous_wf_actvity_id,LS_DB_WF_TRACKER.previous_wf = LS_DB_WF_TRACKER_TMP.previous_wf,LS_DB_WF_TRACKER.out_of_wf = LS_DB_WF_TRACKER_TMP.out_of_wf,LS_DB_WF_TRACKER.no_of_days_spent = LS_DB_WF_TRACKER_TMP.no_of_days_spent,LS_DB_WF_TRACKER.literature_record_id = LS_DB_WF_TRACKER_TMP.literature_record_id,LS_DB_WF_TRACKER.lateness_reason_code_de_ml = LS_DB_WF_TRACKER_TMP.lateness_reason_code_de_ml,LS_DB_WF_TRACKER.lateness_reason_code = LS_DB_WF_TRACKER_TMP.lateness_reason_code,LS_DB_WF_TRACKER.lateness_reason = LS_DB_WF_TRACKER_TMP.lateness_reason,LS_DB_WF_TRACKER.inq_rec_id = LS_DB_WF_TRACKER_TMP.inq_rec_id,LS_DB_WF_TRACKER.fk_lwaq_rec_id = LS_DB_WF_TRACKER_TMP.fk_lwaq_rec_id,LS_DB_WF_TRACKER.fk_lsm_rec_id = LS_DB_WF_TRACKER_TMP.fk_lsm_rec_id,LS_DB_WF_TRACKER.fk_aim_record_id = LS_DB_WF_TRACKER_TMP.fk_aim_record_id,LS_DB_WF_TRACKER.exit_user = LS_DB_WF_TRACKER_TMP.exit_user,LS_DB_WF_TRACKER.exit_time = LS_DB_WF_TRACKER_TMP.exit_time,LS_DB_WF_TRACKER.entry_user = LS_DB_WF_TRACKER_TMP.entry_user,LS_DB_WF_TRACKER.entry_time = LS_DB_WF_TRACKER_TMP.entry_time,LS_DB_WF_TRACKER.due_date = LS_DB_WF_TRACKER_TMP.due_date,LS_DB_WF_TRACKER.date_modified = LS_DB_WF_TRACKER_TMP.date_modified,LS_DB_WF_TRACKER.date_created = LS_DB_WF_TRACKER_TMP.date_created,LS_DB_WF_TRACKER.comp_rec_id = LS_DB_WF_TRACKER_TMP.comp_rec_id,LS_DB_WF_TRACKER.autocomplete_failed = LS_DB_WF_TRACKER_TMP.autocomplete_failed,LS_DB_WF_TRACKER.autocomplete_de_ml = LS_DB_WF_TRACKER_TMP.autocomplete_de_ml,LS_DB_WF_TRACKER.autocomplete = LS_DB_WF_TRACKER_TMP.autocomplete,LS_DB_WF_TRACKER.auto_actions_details = LS_DB_WF_TRACKER_TMP.auto_actions_details,LS_DB_WF_TRACKER.assigned_to_name = LS_DB_WF_TRACKER_TMP.assigned_to_name,LS_DB_WF_TRACKER.assign_to = LS_DB_WF_TRACKER_TMP.assign_to,LS_DB_WF_TRACKER.ari_rec_id = LS_DB_WF_TRACKER_TMP.ari_rec_id,LS_DB_WF_TRACKER.aer_version_no = LS_DB_WF_TRACKER_TMP.aer_version_no,LS_DB_WF_TRACKER.aer_no = LS_DB_WF_TRACKER_TMP.aer_no,LS_DB_WF_TRACKER.aer_local_version_no = LS_DB_WF_TRACKER_TMP.aer_local_version_no,LS_DB_WF_TRACKER.action_quality = LS_DB_WF_TRACKER_TMP.action_quality,
LS_DB_WF_TRACKER.PROCESSING_DT = LS_DB_WF_TRACKER_TMP.PROCESSING_DT ,
LS_DB_WF_TRACKER.receipt_id     =LS_DB_WF_TRACKER_TMP.receipt_id        ,
LS_DB_WF_TRACKER.case_no        =LS_DB_WF_TRACKER_TMP.case_no           ,
LS_DB_WF_TRACKER.case_version   =LS_DB_WF_TRACKER_TMP.case_version      ,
LS_DB_WF_TRACKER.version_no     =LS_DB_WF_TRACKER_TMP.version_no        ,
LS_DB_WF_TRACKER.expiry_date    =LS_DB_WF_TRACKER_TMP.expiry_date       ,
LS_DB_WF_TRACKER.load_ts        =LS_DB_WF_TRACKER_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP 
WHERE LS_DB_WF_TRACKER.INTEGRATION_ID = LS_DB_WF_TRACKER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_WF_TRACKER_TMP.PROCESSING_DT = LS_DB_WF_TRACKER.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,wf_transition_name,
wf_record_id,
wf_next_activity_record_id,
wf_name,
wf_activity_record_id,
wf_activity_name,
user_modified,
user_created,
trackertype,
spr_id,
seq,
sender_organization_name,
sampling_recid,
sampling_flag,
sampling_date,
record_id,
receipt_no,
quality_scores,
quality_review_status,
quality_review_fail_status,
quality_json,
quality_flag,
processing_unit,
previous_wf_actvity_id,
previous_wf,
out_of_wf,
no_of_days_spent,
literature_record_id,
lateness_reason_code_de_ml,
lateness_reason_code,
lateness_reason,
inq_rec_id,
fk_lwaq_rec_id,
fk_lsm_rec_id,
fk_aim_record_id,
exit_user,
exit_time,
entry_user,
entry_time,
due_date,
date_modified,
date_created,
comp_rec_id,
autocomplete_failed,
autocomplete_de_ml,
autocomplete,
auto_actions_details,
assigned_to_name,
assign_to,
ari_rec_id,
aer_version_no,
aer_no,
aer_local_version_no,
action_quality)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,wf_transition_name,
wf_record_id,
wf_next_activity_record_id,
wf_name,
wf_activity_record_id,
wf_activity_name,
user_modified,
user_created,
trackertype,
spr_id,
seq,
sender_organization_name,
sampling_recid,
sampling_flag,
sampling_date,
record_id,
receipt_no,
quality_scores,
quality_review_status,
quality_review_fail_status,
quality_json,
quality_flag,
processing_unit,
previous_wf_actvity_id,
previous_wf,
out_of_wf,
no_of_days_spent,
literature_record_id,
lateness_reason_code_de_ml,
lateness_reason_code,
lateness_reason,
inq_rec_id,
fk_lwaq_rec_id,
fk_lsm_rec_id,
fk_aim_record_id,
exit_user,
exit_time,
entry_user,
entry_time,
due_date,
date_modified,
date_created,
comp_rec_id,
autocomplete_failed,
autocomplete_de_ml,
autocomplete,
auto_actions_details,
assigned_to_name,
assign_to,
ari_rec_id,
aer_version_no,
aer_no,
aer_local_version_no,
action_quality
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP 
WHERE TO_DATE(LS_DB_WF_TRACKER.PROCESSING_DT) < TO_DATE(LS_DB_WF_TRACKER_TMP.PROCESSING_DT)
AND LS_DB_WF_TRACKER.INTEGRATION_ID = LS_DB_WF_TRACKER_TMP.INTEGRATION_ID
AND LS_DB_WF_TRACKER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_tracker')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_tracker')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WF_TRACKER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WF_TRACKER'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_WF_TRACKER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WF_TRACKER'),
                'LSDB','Case','LS_DB_WF_TRACKER',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_WF_TRACKER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
