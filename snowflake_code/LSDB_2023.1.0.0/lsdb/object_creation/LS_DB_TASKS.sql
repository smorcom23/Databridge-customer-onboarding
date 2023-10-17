
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_TASKS()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TASKS'),
	'LSRA','Case','LS_DB_TASKS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_TASKS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_TASKS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_TASKS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  As select RECORD_ID,'lsmv_task' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_task_contact_tracking' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_contact_tracking WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_task_documnents' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_documnents WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_task_ntf_tracking' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_ntf_tracking WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_task_tracking' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_tracking WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_contact_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_atsk_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_contact_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_documnents WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_lsmv_task_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_documnents WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_atsk_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_ntf_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_atsk_rec_id record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_task_ntf_tracking WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_TASKS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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
                                                                                      from $$STG_DB_NAME.$$LSDB_RPL.LSMV_AER_INFO AER_INFO,$$STG_DB_NAME.$$LSDB_RPL.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_task_tracking_SUBSET AS 
(
select * from 
    (SELECT  
    alert_status  tasktrac_alert_status,ari_rec_id  tasktrac_ari_rec_id,attachments_name  tasktrac_attachments_name,comp_rec_id  tasktrac_comp_rec_id,date_created  tasktrac_date_created,date_modified  tasktrac_date_modified,email_body  tasktrac_email_body,fk_ag_rec_id  tasktrac_fk_ag_rec_id,fk_atsk_rec_id  tasktrac_fk_atsk_rec_id,fk_au_rec_id  tasktrac_fk_au_rec_id,inq_rec_id  tasktrac_inq_rec_id,next_reminder  tasktrac_next_reminder,record_id  tasktrac_record_id,snooze_period  tasktrac_snooze_period,snooze_unit  tasktrac_snooze_unit,spr_id  tasktrac_spr_id,status  tasktrac_status,status_date  tasktrac_status_date,subject  tasktrac_subject,to_email_id  tasktrac_to_email_id,user_created  tasktrac_user_created,user_id  tasktrac_user_id,user_modified  tasktrac_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_task_tracking
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_atsk_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_tracking')
  ) where REC_RANK=1 )
  , lsmv_task_contact_tracking_SUBSET AS 
(
select * from 
    (SELECT  
    alert_status  taskctrac_alert_status,ari_rec_id  taskctrac_ari_rec_id,attachments_name  taskctrac_attachments_name,contact_id  taskctrac_contact_id,date_created  taskctrac_date_created,date_modified  taskctrac_date_modified,email_body  taskctrac_email_body,email_id  taskctrac_email_id,email_to_or_cc  taskctrac_email_to_or_cc,fk_atsk_rec_id  taskctrac_fk_atsk_rec_id,fk_au_rec_id  taskctrac_fk_au_rec_id,fk_req_rec_id  taskctrac_fk_req_rec_id,record_id  taskctrac_record_id,spr_id  taskctrac_spr_id,subject  taskctrac_subject,user_created  taskctrac_user_created,user_modified  taskctrac_user_modified,user_type  taskctrac_user_type,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_task_contact_tracking
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_atsk_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_contact_tracking')
  ) where REC_RANK=1 )
  , lsmv_task_documnents_SUBSET AS 
(
select * from 
    (SELECT  
    added_by  taskdoc_added_by,ari_rec_id  taskdoc_ari_rec_id,date_created  taskdoc_date_created,date_modified  taskdoc_date_modified,description  taskdoc_description,doc_id  taskdoc_doc_id,doc_size  taskdoc_doc_size,file_name  taskdoc_file_name,fk_lsmv_task_rec_id  taskdoc_fk_lsmv_task_rec_id,record_id  taskdoc_record_id,spr_id  taskdoc_spr_id,user_created  taskdoc_user_created,user_modified  taskdoc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_task_documnents
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_lsmv_task_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_documnents')
  ) where REC_RANK=1 )
  , lsmv_task_SUBSET AS 
(
select * from 
    (SELECT  
    actual_completion_date  task_actual_completion_date,all_contacts  task_all_contacts,all_facility_users_informed  task_all_facility_users_informed,archived_flag  task_archived_flag,ari_rec_id  task_ari_rec_id,authority  task_authority,case_owner  task_case_owner,comments  task_comments,comp_rec_id  task_comp_rec_id,contact  task_contact,date_created  task_date_created,date_modified  task_date_modified,due_date  task_due_date,escalation_count  task_escalation_count,escalation_model  task_escalation_model,field_id  task_field_id,field_label  task_field_label,inform_all_facility_users  task_inform_all_facility_users,inq_rec_id  task_inq_rec_id,language_code  task_language_code,no_of_reminders  task_no_of_reminders,notes  task_notes,owner  task_owner,primary_contact  task_primary_contact,priority  task_priority,reason_for_change  task_reason_for_change,record_id  task_record_id,ref_id_version  task_ref_id_version,reference_rec_id  task_reference_rec_id,related_ref_id  task_related_ref_id,related_to  task_related_to,related_type  task_related_type,reminder_count  task_reminder_count,reminder_date  task_reminder_date,reminder_flag  task_reminder_flag,reminder_interval  task_reminder_interval,reminder_interval_unit  task_reminder_interval_unit,reminder_sent_date  task_reminder_sent_date,reply_date  task_reply_date,reply_status  task_reply_status,single_reminder  task_single_reminder,spr_id  task_spr_id,status  task_status,status_date  task_status_date,status_updated_by  task_status_updated_by,subject  task_subject,task_id  task_task_id,task_time  task_task_time,task_type  task_task_type,task_updated  task_task_updated,user_created  task_user_created,user_modified  task_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_task
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task')
  ) where REC_RANK=1 )
  , lsmv_task_ntf_tracking_SUBSET AS 
(
select * from 
    (SELECT  
    assigned_user_type  taskntftrac_assigned_user_type,attachments_name  taskntftrac_attachments_name,count  taskntftrac_count,date_created  taskntftrac_date_created,date_modified  taskntftrac_date_modified,email_body  taskntftrac_email_body,email_id  taskntftrac_email_id,email_to_or_cc  taskntftrac_email_to_or_cc,fk_atsk_rec_id  taskntftrac_fk_atsk_rec_id,fk_record_id  taskntftrac_fk_record_id,notification_date  taskntftrac_notification_date,recipient_id  taskntftrac_recipient_id,record_id  taskntftrac_record_id,spr_id  taskntftrac_spr_id,subject  taskntftrac_subject,type  taskntftrac_type,user_created  taskntftrac_user_created,user_modified  taskntftrac_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_task_ntf_tracking
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_atsk_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_ntf_tracking')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_task_ntf_tracking_SUBSET.taskntftrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_task_tracking_SUBSET.tasktrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_task_documnents_SUBSET.taskdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_task_SUBSET.task_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_task_contact_tracking_SUBSET.taskctrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_task_SUBSET.task_USER_MODIFIED USER_MODIFIED,lsmv_task_SUBSET.task_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_task_SUBSET.task_USER_CREATED CREATED_BY,lsmv_task_SUBSET.task_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_task_SUBSET.task_user_modified  ,lsmv_task_SUBSET.task_user_created  ,lsmv_task_SUBSET.task_task_updated  ,lsmv_task_SUBSET.task_task_type  ,lsmv_task_SUBSET.task_task_time  ,lsmv_task_SUBSET.task_task_id  ,lsmv_task_SUBSET.task_subject  ,lsmv_task_SUBSET.task_status_updated_by  ,lsmv_task_SUBSET.task_status_date  ,lsmv_task_SUBSET.task_status  ,lsmv_task_SUBSET.task_spr_id  ,lsmv_task_SUBSET.task_single_reminder  ,lsmv_task_SUBSET.task_reply_status  ,lsmv_task_SUBSET.task_reply_date  ,lsmv_task_SUBSET.task_reminder_sent_date  ,lsmv_task_SUBSET.task_reminder_interval_unit  ,lsmv_task_SUBSET.task_reminder_interval  ,lsmv_task_SUBSET.task_reminder_flag  ,lsmv_task_SUBSET.task_reminder_date  ,lsmv_task_SUBSET.task_reminder_count  ,lsmv_task_SUBSET.task_related_type  ,lsmv_task_SUBSET.task_related_to  ,lsmv_task_SUBSET.task_related_ref_id  ,lsmv_task_SUBSET.task_reference_rec_id  ,lsmv_task_SUBSET.task_ref_id_version  ,lsmv_task_SUBSET.task_record_id  ,lsmv_task_SUBSET.task_reason_for_change  ,lsmv_task_SUBSET.task_priority  ,lsmv_task_SUBSET.task_primary_contact  ,lsmv_task_SUBSET.task_owner  ,lsmv_task_SUBSET.task_notes  ,lsmv_task_SUBSET.task_no_of_reminders  ,lsmv_task_SUBSET.task_language_code  ,lsmv_task_SUBSET.task_inq_rec_id  ,lsmv_task_SUBSET.task_inform_all_facility_users  ,lsmv_task_SUBSET.task_field_label  ,lsmv_task_SUBSET.task_field_id  ,lsmv_task_SUBSET.task_escalation_model  ,lsmv_task_SUBSET.task_escalation_count  ,lsmv_task_SUBSET.task_due_date  ,lsmv_task_SUBSET.task_date_modified  ,lsmv_task_SUBSET.task_date_created  ,lsmv_task_SUBSET.task_contact  ,lsmv_task_SUBSET.task_comp_rec_id  ,lsmv_task_SUBSET.task_comments  ,lsmv_task_SUBSET.task_case_owner  ,lsmv_task_SUBSET.task_authority  ,lsmv_task_SUBSET.task_ari_rec_id  ,lsmv_task_SUBSET.task_archived_flag  ,lsmv_task_SUBSET.task_all_facility_users_informed  ,lsmv_task_SUBSET.task_all_contacts  ,lsmv_task_SUBSET.task_actual_completion_date  ,lsmv_task_tracking_SUBSET.tasktrac_user_modified  ,lsmv_task_tracking_SUBSET.tasktrac_user_id  ,lsmv_task_tracking_SUBSET.tasktrac_user_created  ,lsmv_task_tracking_SUBSET.tasktrac_to_email_id  ,lsmv_task_tracking_SUBSET.tasktrac_subject  ,lsmv_task_tracking_SUBSET.tasktrac_status_date  ,lsmv_task_tracking_SUBSET.tasktrac_status  ,lsmv_task_tracking_SUBSET.tasktrac_spr_id  ,lsmv_task_tracking_SUBSET.tasktrac_snooze_unit  ,lsmv_task_tracking_SUBSET.tasktrac_snooze_period  ,lsmv_task_tracking_SUBSET.tasktrac_record_id  ,lsmv_task_tracking_SUBSET.tasktrac_next_reminder  ,lsmv_task_tracking_SUBSET.tasktrac_inq_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_fk_au_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_fk_atsk_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_fk_ag_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_email_body  ,lsmv_task_tracking_SUBSET.tasktrac_date_modified  ,lsmv_task_tracking_SUBSET.tasktrac_date_created  ,lsmv_task_tracking_SUBSET.tasktrac_comp_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_attachments_name  ,lsmv_task_tracking_SUBSET.tasktrac_ari_rec_id  ,lsmv_task_tracking_SUBSET.tasktrac_alert_status  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_user_modified  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_user_created  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_type  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_subject  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_spr_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_record_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_recipient_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_notification_date  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_fk_record_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_fk_atsk_rec_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_email_to_or_cc  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_email_id  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_email_body  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_date_modified  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_date_created  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_count  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_attachments_name  ,lsmv_task_ntf_tracking_SUBSET.taskntftrac_assigned_user_type  ,lsmv_task_documnents_SUBSET.taskdoc_user_modified  ,lsmv_task_documnents_SUBSET.taskdoc_user_created  ,lsmv_task_documnents_SUBSET.taskdoc_spr_id  ,lsmv_task_documnents_SUBSET.taskdoc_record_id  ,lsmv_task_documnents_SUBSET.taskdoc_fk_lsmv_task_rec_id  ,lsmv_task_documnents_SUBSET.taskdoc_file_name  ,lsmv_task_documnents_SUBSET.taskdoc_doc_size  ,lsmv_task_documnents_SUBSET.taskdoc_doc_id  ,lsmv_task_documnents_SUBSET.taskdoc_description  ,lsmv_task_documnents_SUBSET.taskdoc_date_modified  ,lsmv_task_documnents_SUBSET.taskdoc_date_created  ,lsmv_task_documnents_SUBSET.taskdoc_ari_rec_id  ,lsmv_task_documnents_SUBSET.taskdoc_added_by  ,lsmv_task_contact_tracking_SUBSET.taskctrac_user_type  ,lsmv_task_contact_tracking_SUBSET.taskctrac_user_modified  ,lsmv_task_contact_tracking_SUBSET.taskctrac_user_created  ,lsmv_task_contact_tracking_SUBSET.taskctrac_subject  ,lsmv_task_contact_tracking_SUBSET.taskctrac_spr_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_record_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_fk_req_rec_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_fk_au_rec_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_fk_atsk_rec_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_email_to_or_cc  ,lsmv_task_contact_tracking_SUBSET.taskctrac_email_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_email_body  ,lsmv_task_contact_tracking_SUBSET.taskctrac_date_modified  ,lsmv_task_contact_tracking_SUBSET.taskctrac_date_created  ,lsmv_task_contact_tracking_SUBSET.taskctrac_contact_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_attachments_name  ,lsmv_task_contact_tracking_SUBSET.taskctrac_ari_rec_id  ,lsmv_task_contact_tracking_SUBSET.taskctrac_alert_status ,CONCAT( NVL(lsmv_task_ntf_tracking_SUBSET.taskntftrac_RECORD_ID,-1),'||',NVL(lsmv_task_tracking_SUBSET.tasktrac_RECORD_ID,-1),'||',NVL(lsmv_task_documnents_SUBSET.taskdoc_RECORD_ID,-1),'||',NVL(lsmv_task_SUBSET.task_RECORD_ID,-1),'||',NVL(lsmv_task_contact_tracking_SUBSET.taskctrac_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_task_SUBSET  LEFT JOIN lsmv_task_contact_tracking_SUBSET ON lsmv_task_SUBSET.task_RECORD_ID=lsmv_task_contact_tracking_SUBSET.taskctrac_fk_atsk_rec_id
                         LEFT JOIN lsmv_task_documnents_SUBSET ON lsmv_task_SUBSET.task_RECORD_ID=lsmv_task_documnents_SUBSET.taskdoc_fk_lsmv_task_rec_id
                         LEFT JOIN lsmv_task_ntf_tracking_SUBSET ON lsmv_task_SUBSET.task_RECORD_ID=lsmv_task_ntf_tracking_SUBSET.taskntftrac_fk_atsk_rec_id
                         LEFT JOIN lsmv_task_tracking_SUBSET ON lsmv_task_SUBSET.task_RECORD_ID=lsmv_task_tracking_SUBSET.tasktrac_fk_atsk_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_task_ntf_tracking_SUBSET.taskntftrac_RECORD_ID,lsmv_task_tracking_SUBSET.tasktrac_RECORD_ID,lsmv_task_documnents_SUBSET.taskdoc_RECORD_ID,lsmv_task_SUBSET.task_RECORD_ID,lsmv_task_contact_tracking_SUBSET.taskctrac_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP)
where target_table_name='LS_DB_TASKS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TASKS'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS   
SET LS_DB_TASKS.task_user_modified = LS_DB_TASKS_TMP.task_user_modified,LS_DB_TASKS.task_user_created = LS_DB_TASKS_TMP.task_user_created,LS_DB_TASKS.task_task_updated = LS_DB_TASKS_TMP.task_task_updated,LS_DB_TASKS.task_task_type = LS_DB_TASKS_TMP.task_task_type,LS_DB_TASKS.task_task_time = LS_DB_TASKS_TMP.task_task_time,LS_DB_TASKS.task_task_id = LS_DB_TASKS_TMP.task_task_id,LS_DB_TASKS.task_subject = LS_DB_TASKS_TMP.task_subject,LS_DB_TASKS.task_status_updated_by = LS_DB_TASKS_TMP.task_status_updated_by,LS_DB_TASKS.task_status_date = LS_DB_TASKS_TMP.task_status_date,LS_DB_TASKS.task_status = LS_DB_TASKS_TMP.task_status,LS_DB_TASKS.task_spr_id = LS_DB_TASKS_TMP.task_spr_id,LS_DB_TASKS.task_single_reminder = LS_DB_TASKS_TMP.task_single_reminder,LS_DB_TASKS.task_reply_status = LS_DB_TASKS_TMP.task_reply_status,LS_DB_TASKS.task_reply_date = LS_DB_TASKS_TMP.task_reply_date,LS_DB_TASKS.task_reminder_sent_date = LS_DB_TASKS_TMP.task_reminder_sent_date,LS_DB_TASKS.task_reminder_interval_unit = LS_DB_TASKS_TMP.task_reminder_interval_unit,LS_DB_TASKS.task_reminder_interval = LS_DB_TASKS_TMP.task_reminder_interval,LS_DB_TASKS.task_reminder_flag = LS_DB_TASKS_TMP.task_reminder_flag,LS_DB_TASKS.task_reminder_date = LS_DB_TASKS_TMP.task_reminder_date,LS_DB_TASKS.task_reminder_count = LS_DB_TASKS_TMP.task_reminder_count,LS_DB_TASKS.task_related_type = LS_DB_TASKS_TMP.task_related_type,LS_DB_TASKS.task_related_to = LS_DB_TASKS_TMP.task_related_to,LS_DB_TASKS.task_related_ref_id = LS_DB_TASKS_TMP.task_related_ref_id,LS_DB_TASKS.task_reference_rec_id = LS_DB_TASKS_TMP.task_reference_rec_id,LS_DB_TASKS.task_ref_id_version = LS_DB_TASKS_TMP.task_ref_id_version,LS_DB_TASKS.task_record_id = LS_DB_TASKS_TMP.task_record_id,LS_DB_TASKS.task_reason_for_change = LS_DB_TASKS_TMP.task_reason_for_change,LS_DB_TASKS.task_priority = LS_DB_TASKS_TMP.task_priority,LS_DB_TASKS.task_primary_contact = LS_DB_TASKS_TMP.task_primary_contact,LS_DB_TASKS.task_owner = LS_DB_TASKS_TMP.task_owner,LS_DB_TASKS.task_notes = LS_DB_TASKS_TMP.task_notes,LS_DB_TASKS.task_no_of_reminders = LS_DB_TASKS_TMP.task_no_of_reminders,LS_DB_TASKS.task_language_code = LS_DB_TASKS_TMP.task_language_code,LS_DB_TASKS.task_inq_rec_id = LS_DB_TASKS_TMP.task_inq_rec_id,LS_DB_TASKS.task_inform_all_facility_users = LS_DB_TASKS_TMP.task_inform_all_facility_users,LS_DB_TASKS.task_field_label = LS_DB_TASKS_TMP.task_field_label,LS_DB_TASKS.task_field_id = LS_DB_TASKS_TMP.task_field_id,LS_DB_TASKS.task_escalation_model = LS_DB_TASKS_TMP.task_escalation_model,LS_DB_TASKS.task_escalation_count = LS_DB_TASKS_TMP.task_escalation_count,LS_DB_TASKS.task_due_date = LS_DB_TASKS_TMP.task_due_date,LS_DB_TASKS.task_date_modified = LS_DB_TASKS_TMP.task_date_modified,LS_DB_TASKS.task_date_created = LS_DB_TASKS_TMP.task_date_created,LS_DB_TASKS.task_contact = LS_DB_TASKS_TMP.task_contact,LS_DB_TASKS.task_comp_rec_id = LS_DB_TASKS_TMP.task_comp_rec_id,LS_DB_TASKS.task_comments = LS_DB_TASKS_TMP.task_comments,LS_DB_TASKS.task_case_owner = LS_DB_TASKS_TMP.task_case_owner,LS_DB_TASKS.task_authority = LS_DB_TASKS_TMP.task_authority,LS_DB_TASKS.task_ari_rec_id = LS_DB_TASKS_TMP.task_ari_rec_id,LS_DB_TASKS.task_archived_flag = LS_DB_TASKS_TMP.task_archived_flag,LS_DB_TASKS.task_all_facility_users_informed = LS_DB_TASKS_TMP.task_all_facility_users_informed,LS_DB_TASKS.task_all_contacts = LS_DB_TASKS_TMP.task_all_contacts,LS_DB_TASKS.task_actual_completion_date = LS_DB_TASKS_TMP.task_actual_completion_date,LS_DB_TASKS.tasktrac_user_modified = LS_DB_TASKS_TMP.tasktrac_user_modified,LS_DB_TASKS.tasktrac_user_id = LS_DB_TASKS_TMP.tasktrac_user_id,LS_DB_TASKS.tasktrac_user_created = LS_DB_TASKS_TMP.tasktrac_user_created,LS_DB_TASKS.tasktrac_to_email_id = LS_DB_TASKS_TMP.tasktrac_to_email_id,LS_DB_TASKS.tasktrac_subject = LS_DB_TASKS_TMP.tasktrac_subject,LS_DB_TASKS.tasktrac_status_date = LS_DB_TASKS_TMP.tasktrac_status_date,LS_DB_TASKS.tasktrac_status = LS_DB_TASKS_TMP.tasktrac_status,LS_DB_TASKS.tasktrac_spr_id = LS_DB_TASKS_TMP.tasktrac_spr_id,LS_DB_TASKS.tasktrac_snooze_unit = LS_DB_TASKS_TMP.tasktrac_snooze_unit,LS_DB_TASKS.tasktrac_snooze_period = LS_DB_TASKS_TMP.tasktrac_snooze_period,LS_DB_TASKS.tasktrac_record_id = LS_DB_TASKS_TMP.tasktrac_record_id,LS_DB_TASKS.tasktrac_next_reminder = LS_DB_TASKS_TMP.tasktrac_next_reminder,LS_DB_TASKS.tasktrac_inq_rec_id = LS_DB_TASKS_TMP.tasktrac_inq_rec_id,LS_DB_TASKS.tasktrac_fk_au_rec_id = LS_DB_TASKS_TMP.tasktrac_fk_au_rec_id,LS_DB_TASKS.tasktrac_fk_atsk_rec_id = LS_DB_TASKS_TMP.tasktrac_fk_atsk_rec_id,LS_DB_TASKS.tasktrac_fk_ag_rec_id = LS_DB_TASKS_TMP.tasktrac_fk_ag_rec_id,LS_DB_TASKS.tasktrac_email_body = LS_DB_TASKS_TMP.tasktrac_email_body,LS_DB_TASKS.tasktrac_date_modified = LS_DB_TASKS_TMP.tasktrac_date_modified,LS_DB_TASKS.tasktrac_date_created = LS_DB_TASKS_TMP.tasktrac_date_created,LS_DB_TASKS.tasktrac_comp_rec_id = LS_DB_TASKS_TMP.tasktrac_comp_rec_id,LS_DB_TASKS.tasktrac_attachments_name = LS_DB_TASKS_TMP.tasktrac_attachments_name,LS_DB_TASKS.tasktrac_ari_rec_id = LS_DB_TASKS_TMP.tasktrac_ari_rec_id,LS_DB_TASKS.tasktrac_alert_status = LS_DB_TASKS_TMP.tasktrac_alert_status,LS_DB_TASKS.taskntftrac_user_modified = LS_DB_TASKS_TMP.taskntftrac_user_modified,LS_DB_TASKS.taskntftrac_user_created = LS_DB_TASKS_TMP.taskntftrac_user_created,LS_DB_TASKS.taskntftrac_type = LS_DB_TASKS_TMP.taskntftrac_type,LS_DB_TASKS.taskntftrac_subject = LS_DB_TASKS_TMP.taskntftrac_subject,LS_DB_TASKS.taskntftrac_spr_id = LS_DB_TASKS_TMP.taskntftrac_spr_id,LS_DB_TASKS.taskntftrac_record_id = LS_DB_TASKS_TMP.taskntftrac_record_id,LS_DB_TASKS.taskntftrac_recipient_id = LS_DB_TASKS_TMP.taskntftrac_recipient_id,LS_DB_TASKS.taskntftrac_notification_date = LS_DB_TASKS_TMP.taskntftrac_notification_date,LS_DB_TASKS.taskntftrac_fk_record_id = LS_DB_TASKS_TMP.taskntftrac_fk_record_id,LS_DB_TASKS.taskntftrac_fk_atsk_rec_id = LS_DB_TASKS_TMP.taskntftrac_fk_atsk_rec_id,LS_DB_TASKS.taskntftrac_email_to_or_cc = LS_DB_TASKS_TMP.taskntftrac_email_to_or_cc,LS_DB_TASKS.taskntftrac_email_id = LS_DB_TASKS_TMP.taskntftrac_email_id,LS_DB_TASKS.taskntftrac_email_body = LS_DB_TASKS_TMP.taskntftrac_email_body,LS_DB_TASKS.taskntftrac_date_modified = LS_DB_TASKS_TMP.taskntftrac_date_modified,LS_DB_TASKS.taskntftrac_date_created = LS_DB_TASKS_TMP.taskntftrac_date_created,LS_DB_TASKS.taskntftrac_count = LS_DB_TASKS_TMP.taskntftrac_count,LS_DB_TASKS.taskntftrac_attachments_name = LS_DB_TASKS_TMP.taskntftrac_attachments_name,LS_DB_TASKS.taskntftrac_assigned_user_type = LS_DB_TASKS_TMP.taskntftrac_assigned_user_type,LS_DB_TASKS.taskdoc_user_modified = LS_DB_TASKS_TMP.taskdoc_user_modified,LS_DB_TASKS.taskdoc_user_created = LS_DB_TASKS_TMP.taskdoc_user_created,LS_DB_TASKS.taskdoc_spr_id = LS_DB_TASKS_TMP.taskdoc_spr_id,LS_DB_TASKS.taskdoc_record_id = LS_DB_TASKS_TMP.taskdoc_record_id,LS_DB_TASKS.taskdoc_fk_lsmv_task_rec_id = LS_DB_TASKS_TMP.taskdoc_fk_lsmv_task_rec_id,LS_DB_TASKS.taskdoc_file_name = LS_DB_TASKS_TMP.taskdoc_file_name,LS_DB_TASKS.taskdoc_doc_size = LS_DB_TASKS_TMP.taskdoc_doc_size,LS_DB_TASKS.taskdoc_doc_id = LS_DB_TASKS_TMP.taskdoc_doc_id,LS_DB_TASKS.taskdoc_description = LS_DB_TASKS_TMP.taskdoc_description,LS_DB_TASKS.taskdoc_date_modified = LS_DB_TASKS_TMP.taskdoc_date_modified,LS_DB_TASKS.taskdoc_date_created = LS_DB_TASKS_TMP.taskdoc_date_created,LS_DB_TASKS.taskdoc_ari_rec_id = LS_DB_TASKS_TMP.taskdoc_ari_rec_id,LS_DB_TASKS.taskdoc_added_by = LS_DB_TASKS_TMP.taskdoc_added_by,LS_DB_TASKS.taskctrac_user_type = LS_DB_TASKS_TMP.taskctrac_user_type,LS_DB_TASKS.taskctrac_user_modified = LS_DB_TASKS_TMP.taskctrac_user_modified,LS_DB_TASKS.taskctrac_user_created = LS_DB_TASKS_TMP.taskctrac_user_created,LS_DB_TASKS.taskctrac_subject = LS_DB_TASKS_TMP.taskctrac_subject,LS_DB_TASKS.taskctrac_spr_id = LS_DB_TASKS_TMP.taskctrac_spr_id,LS_DB_TASKS.taskctrac_record_id = LS_DB_TASKS_TMP.taskctrac_record_id,LS_DB_TASKS.taskctrac_fk_req_rec_id = LS_DB_TASKS_TMP.taskctrac_fk_req_rec_id,LS_DB_TASKS.taskctrac_fk_au_rec_id = LS_DB_TASKS_TMP.taskctrac_fk_au_rec_id,LS_DB_TASKS.taskctrac_fk_atsk_rec_id = LS_DB_TASKS_TMP.taskctrac_fk_atsk_rec_id,LS_DB_TASKS.taskctrac_email_to_or_cc = LS_DB_TASKS_TMP.taskctrac_email_to_or_cc,LS_DB_TASKS.taskctrac_email_id = LS_DB_TASKS_TMP.taskctrac_email_id,LS_DB_TASKS.taskctrac_email_body = LS_DB_TASKS_TMP.taskctrac_email_body,LS_DB_TASKS.taskctrac_date_modified = LS_DB_TASKS_TMP.taskctrac_date_modified,LS_DB_TASKS.taskctrac_date_created = LS_DB_TASKS_TMP.taskctrac_date_created,LS_DB_TASKS.taskctrac_contact_id = LS_DB_TASKS_TMP.taskctrac_contact_id,LS_DB_TASKS.taskctrac_attachments_name = LS_DB_TASKS_TMP.taskctrac_attachments_name,LS_DB_TASKS.taskctrac_ari_rec_id = LS_DB_TASKS_TMP.taskctrac_ari_rec_id,LS_DB_TASKS.taskctrac_alert_status = LS_DB_TASKS_TMP.taskctrac_alert_status,
LS_DB_TASKS.PROCESSING_DT = LS_DB_TASKS_TMP.PROCESSING_DT,
LS_DB_TASKS.receipt_id     =LS_DB_TASKS_TMP.receipt_id    ,
LS_DB_TASKS.case_no        =LS_DB_TASKS_TMP.case_no           ,
LS_DB_TASKS.case_version   =LS_DB_TASKS_TMP.case_version      ,
LS_DB_TASKS.version_no     =LS_DB_TASKS_TMP.version_no        ,
LS_DB_TASKS.user_modified  =LS_DB_TASKS_TMP.user_modified     ,
LS_DB_TASKS.date_modified  =LS_DB_TASKS_TMP.date_modified     ,
LS_DB_TASKS.expiry_date    =LS_DB_TASKS_TMP.expiry_date       ,
LS_DB_TASKS.created_by     =LS_DB_TASKS_TMP.created_by        ,
LS_DB_TASKS.created_dt     =LS_DB_TASKS_TMP.created_dt        ,
LS_DB_TASKS.load_ts        =LS_DB_TASKS_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP 
WHERE 	LS_DB_TASKS.INTEGRATION_ID = LS_DB_TASKS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_TASKS_TMP.PROCESSING_DT = LS_DB_TASKS.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_TASKS.taskntftrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.tasktrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.taskdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.task_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.taskctrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_TASKS_TMP.taskntftrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.tasktrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.taskdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.task_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.taskctrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_TASKS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_TASKS.task_RECORD_ID ,LS_DB_TASKS.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP 
ON LS_DB_TASKS.task_RECORD_ID=LS_DB_TASKS_TMP.task_RECORD_ID
AND LS_DB_TASKS.INTEGRATION_ID = LS_DB_TASKS_TMP.INTEGRATION_ID 
where LS_DB_TASKS_TMP.INTEGRATION_ID  is null AND LS_DB_TASKS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_TASKS.task_RECORD_ID in (select task_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP )
) TMP where LS_DB_TASKS.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_TASKS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_TASKS.task_RECORD_ID ,LS_DB_TASKS.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP 
    ON LS_DB_TASKS.task_RECORD_ID=LS_DB_TASKS_TMP.task_RECORD_ID
    AND LS_DB_TASKS.INTEGRATION_ID = LS_DB_TASKS_TMP.INTEGRATION_ID 
    where LS_DB_TASKS_TMP.INTEGRATION_ID  is null AND LS_DB_TASKS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_TASKS.task_RECORD_ID in (select task_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS
( receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,task_user_modified,
task_user_created,
task_task_updated,
task_task_type,
task_task_time,
task_task_id,
task_subject,
task_status_updated_by,
task_status_date,
task_status,
task_spr_id,
task_single_reminder,
task_reply_status,
task_reply_date,
task_reminder_sent_date,
task_reminder_interval_unit,
task_reminder_interval,
task_reminder_flag,
task_reminder_date,
task_reminder_count,
task_related_type,
task_related_to,
task_related_ref_id,
task_reference_rec_id,
task_ref_id_version,
task_record_id,
task_reason_for_change,
task_priority,
task_primary_contact,
task_owner,
task_notes,
task_no_of_reminders,
task_language_code,
task_inq_rec_id,
task_inform_all_facility_users,
task_field_label,
task_field_id,
task_escalation_model,
task_escalation_count,
task_due_date,
task_date_modified,
task_date_created,
task_contact,
task_comp_rec_id,
task_comments,
task_case_owner,
task_authority,
task_ari_rec_id,
task_archived_flag,
task_all_facility_users_informed,
task_all_contacts,
task_actual_completion_date,
tasktrac_user_modified,
tasktrac_user_id,
tasktrac_user_created,
tasktrac_to_email_id,
tasktrac_subject,
tasktrac_status_date,
tasktrac_status,
tasktrac_spr_id,
tasktrac_snooze_unit,
tasktrac_snooze_period,
tasktrac_record_id,
tasktrac_next_reminder,
tasktrac_inq_rec_id,
tasktrac_fk_au_rec_id,
tasktrac_fk_atsk_rec_id,
tasktrac_fk_ag_rec_id,
tasktrac_email_body,
tasktrac_date_modified,
tasktrac_date_created,
tasktrac_comp_rec_id,
tasktrac_attachments_name,
tasktrac_ari_rec_id,
tasktrac_alert_status,
taskntftrac_user_modified,
taskntftrac_user_created,
taskntftrac_type,
taskntftrac_subject,
taskntftrac_spr_id,
taskntftrac_record_id,
taskntftrac_recipient_id,
taskntftrac_notification_date,
taskntftrac_fk_record_id,
taskntftrac_fk_atsk_rec_id,
taskntftrac_email_to_or_cc,
taskntftrac_email_id,
taskntftrac_email_body,
taskntftrac_date_modified,
taskntftrac_date_created,
taskntftrac_count,
taskntftrac_attachments_name,
taskntftrac_assigned_user_type,
taskdoc_user_modified,
taskdoc_user_created,
taskdoc_spr_id,
taskdoc_record_id,
taskdoc_fk_lsmv_task_rec_id,
taskdoc_file_name,
taskdoc_doc_size,
taskdoc_doc_id,
taskdoc_description,
taskdoc_date_modified,
taskdoc_date_created,
taskdoc_ari_rec_id,
taskdoc_added_by,
taskctrac_user_type,
taskctrac_user_modified,
taskctrac_user_created,
taskctrac_subject,
taskctrac_spr_id,
taskctrac_record_id,
taskctrac_fk_req_rec_id,
taskctrac_fk_au_rec_id,
taskctrac_fk_atsk_rec_id,
taskctrac_email_to_or_cc,
taskctrac_email_id,
taskctrac_email_body,
taskctrac_date_modified,
taskctrac_date_created,
taskctrac_contact_id,
taskctrac_attachments_name,
taskctrac_ari_rec_id,
taskctrac_alert_status)
SELECT 
 receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,task_user_modified,
task_user_created,
task_task_updated,
task_task_type,
task_task_time,
task_task_id,
task_subject,
task_status_updated_by,
task_status_date,
task_status,
task_spr_id,
task_single_reminder,
task_reply_status,
task_reply_date,
task_reminder_sent_date,
task_reminder_interval_unit,
task_reminder_interval,
task_reminder_flag,
task_reminder_date,
task_reminder_count,
task_related_type,
task_related_to,
task_related_ref_id,
task_reference_rec_id,
task_ref_id_version,
task_record_id,
task_reason_for_change,
task_priority,
task_primary_contact,
task_owner,
task_notes,
task_no_of_reminders,
task_language_code,
task_inq_rec_id,
task_inform_all_facility_users,
task_field_label,
task_field_id,
task_escalation_model,
task_escalation_count,
task_due_date,
task_date_modified,
task_date_created,
task_contact,
task_comp_rec_id,
task_comments,
task_case_owner,
task_authority,
task_ari_rec_id,
task_archived_flag,
task_all_facility_users_informed,
task_all_contacts,
task_actual_completion_date,
tasktrac_user_modified,
tasktrac_user_id,
tasktrac_user_created,
tasktrac_to_email_id,
tasktrac_subject,
tasktrac_status_date,
tasktrac_status,
tasktrac_spr_id,
tasktrac_snooze_unit,
tasktrac_snooze_period,
tasktrac_record_id,
tasktrac_next_reminder,
tasktrac_inq_rec_id,
tasktrac_fk_au_rec_id,
tasktrac_fk_atsk_rec_id,
tasktrac_fk_ag_rec_id,
tasktrac_email_body,
tasktrac_date_modified,
tasktrac_date_created,
tasktrac_comp_rec_id,
tasktrac_attachments_name,
tasktrac_ari_rec_id,
tasktrac_alert_status,
taskntftrac_user_modified,
taskntftrac_user_created,
taskntftrac_type,
taskntftrac_subject,
taskntftrac_spr_id,
taskntftrac_record_id,
taskntftrac_recipient_id,
taskntftrac_notification_date,
taskntftrac_fk_record_id,
taskntftrac_fk_atsk_rec_id,
taskntftrac_email_to_or_cc,
taskntftrac_email_id,
taskntftrac_email_body,
taskntftrac_date_modified,
taskntftrac_date_created,
taskntftrac_count,
taskntftrac_attachments_name,
taskntftrac_assigned_user_type,
taskdoc_user_modified,
taskdoc_user_created,
taskdoc_spr_id,
taskdoc_record_id,
taskdoc_fk_lsmv_task_rec_id,
taskdoc_file_name,
taskdoc_doc_size,
taskdoc_doc_id,
taskdoc_description,
taskdoc_date_modified,
taskdoc_date_created,
taskdoc_ari_rec_id,
taskdoc_added_by,
taskctrac_user_type,
taskctrac_user_modified,
taskctrac_user_created,
taskctrac_subject,
taskctrac_spr_id,
taskctrac_record_id,
taskctrac_fk_req_rec_id,
taskctrac_fk_au_rec_id,
taskctrac_fk_atsk_rec_id,
taskctrac_email_to_or_cc,
taskctrac_email_id,
taskctrac_email_body,
taskctrac_date_modified,
taskctrac_date_created,
taskctrac_contact_id,
taskctrac_attachments_name,
taskctrac_ari_rec_id,
taskctrac_alert_status
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP 
WHERE 	TO_DATE(LS_DB_TASKS.PROCESSING_DT) < TO_DATE(LS_DB_TASKS_TMP.PROCESSING_DT)
AND LS_DB_TASKS.INTEGRATION_ID = LS_DB_TASKS_TMP.INTEGRATION_ID
AND LS_DB_TASKS.task_RECORD_ID = LS_DB_TASKS_TMP.task_RECORD_ID
AND LS_DB_TASKS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_TASKS.taskntftrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.tasktrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.taskdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.task_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_TASKS.taskctrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_TASKS_TMP.taskntftrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.tasktrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.taskdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.task_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_TASKS_TMP.taskctrac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS TGT
WHERE  ( task_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task') OR taskctrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_contact_tracking') OR taskdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_documnents') OR taskntftrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_ntf_tracking') OR tasktrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_tracking')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( task_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task') OR taskctrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_contact_tracking') OR taskdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_documnents') OR taskntftrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_ntf_tracking') OR tasktrac_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_TASKS_DELETION_TMP  WHERE TABLE_NAME='lsmv_task_tracking')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_TASKS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_TASKS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TASKS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_TASKS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_TASKS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_TASKS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
