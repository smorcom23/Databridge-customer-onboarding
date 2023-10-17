
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_CASE_QUES()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_QUES'),
	'LSRA','Case','LS_DB_CASE_QUES',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_QUES' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_QUES'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_QUES'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  As select RECORD_ID,'lsmv_case_ques' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_case_ques_details' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques_details WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_QUES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_case_ques_SUBSET AS 
(
select * from 
    (SELECT  
    allow_future_date  ques_allow_future_date,answer  ques_answer,ari_rec_id  ques_ari_rec_id,case_rec_id  ques_case_rec_id,case_update  ques_case_update,case_value  ques_case_value,context  ques_context,date_created  ques_date_created,date_modified  ques_date_modified,entity_context  ques_entity_context,entity_rec_id  ques_entity_rec_id,field_id  ques_field_id,fk_agx_ques_rec_id  ques_fk_agx_ques_rec_id,imprecise_date_fmt  ques_imprecise_date_fmt,is_answered_by_user  ques_is_answered_by_user,is_privacy_enabled  ques_is_privacy_enabled,manual_created  ques_manual_created,ques_rec_id  ques_ques_rec_id,question  ques_question,question_deleted_edited_manually  ques_question_deleted_edited_manually,question_id  ques_question_id,question_index  ques_question_index,question_type  ques_question_type,record_id  ques_record_id,rejection  ques_rejection,spr_id  ques_spr_id,status_code  ques_status_code,ui_type  ques_ui_type,user_created  ques_user_created,user_modified  ques_user_modified,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques
 WHERE  ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques')
  ) where REC_RANK=1 )
  , lsmv_case_ques_details_SUBSET AS 
(
select * from 
    (SELECT  
    ack_date  quesdet_ack_date,ari_rec_id  quesdet_ari_rec_id,auto_update_fu_data  quesdet_auto_update_fu_data,case_id  quesdet_case_id,case_rec_id  quesdet_case_rec_id,case_update  quesdet_case_update,case_version  quesdet_case_version,config_lang  quesdet_config_lang,correspondence_rec_id  quesdet_correspondence_rec_id,correspondence_status  quesdet_correspondence_status,date_created  quesdet_date_created,date_modified  quesdet_date_modified,date_reply_received  quesdet_date_reply_received,document_tempalte_data  quesdet_document_tempalte_data,follow_up_ack_template  quesdet_follow_up_ack_template,follow_up_alert_type  quesdet_follow_up_alert_type,follow_up_document_template  quesdet_follow_up_document_template,follow_up_email_template  quesdet_follow_up_email_template,follow_up_id  quesdet_follow_up_id,follow_up_rem_template  quesdet_follow_up_rem_template,form_name  quesdet_form_name,form_rec_id  quesdet_form_rec_id,fuq_reason_code  quesdet_fuq_reason_code,fuq_reason_comments  quesdet_fuq_reason_comments,fuq_reason_decline  quesdet_fuq_reason_decline,generated_date  quesdet_generated_date,is_recipient_exist  quesdet_is_recipient_exist,last_alert_triggered  quesdet_last_alert_triggered,latest_corresp_rec_id  quesdet_latest_corresp_rec_id,lrn_no  quesdet_lrn_no,manually_sent  quesdet_manually_sent,module_type  quesdet_module_type,no_of_que_ans  quesdet_no_of_que_ans,no_of_reminder_triggred  quesdet_no_of_reminder_triggred,password_detail  quesdet_password_detail,question_generated  quesdet_question_generated,record_id  quesdet_record_id,rejection  quesdet_rejection,reminder_count  quesdet_reminder_count,reply_count  quesdet_reply_count,rule_rec_ids  quesdet_rule_rec_ids,sent_date  quesdet_sent_date,spr_id  quesdet_spr_id,status  quesdet_status,user_created  quesdet_user_created,user_modified  quesdet_user_modified,wf_activity_rec_id  quesdet_wf_activity_rec_id,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_case_ques_details
 WHERE  ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques_details')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_case_ques_SUBSET.ques_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_case_ques_details_SUBSET.quesdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_case_ques_SUBSET.ques_USER_MODIFIED USER_MODIFIED,lsmv_case_ques_SUBSET.ques_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_case_ques_SUBSET.ques_USER_CREATED CREATED_BY,lsmv_case_ques_SUBSET.ques_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_case_ques_SUBSET.ques_user_modified  ,lsmv_case_ques_SUBSET.ques_user_created  ,lsmv_case_ques_SUBSET.ques_ui_type  ,lsmv_case_ques_SUBSET.ques_status_code  ,lsmv_case_ques_SUBSET.ques_spr_id  ,lsmv_case_ques_SUBSET.ques_rejection  ,lsmv_case_ques_SUBSET.ques_record_id  ,lsmv_case_ques_SUBSET.ques_question_type  ,lsmv_case_ques_SUBSET.ques_question_index  ,lsmv_case_ques_SUBSET.ques_question_id  ,lsmv_case_ques_SUBSET.ques_question_deleted_edited_manually  ,lsmv_case_ques_SUBSET.ques_question  ,lsmv_case_ques_SUBSET.ques_ques_rec_id  ,lsmv_case_ques_SUBSET.ques_manual_created  ,lsmv_case_ques_SUBSET.ques_is_privacy_enabled  ,lsmv_case_ques_SUBSET.ques_is_answered_by_user  ,lsmv_case_ques_SUBSET.ques_imprecise_date_fmt  ,lsmv_case_ques_SUBSET.ques_fk_agx_ques_rec_id  ,lsmv_case_ques_SUBSET.ques_field_id  ,lsmv_case_ques_SUBSET.ques_entity_rec_id  ,lsmv_case_ques_SUBSET.ques_entity_context  ,lsmv_case_ques_SUBSET.ques_date_modified  ,lsmv_case_ques_SUBSET.ques_date_created  ,lsmv_case_ques_SUBSET.ques_context  ,lsmv_case_ques_SUBSET.ques_case_value  ,lsmv_case_ques_SUBSET.ques_case_update  ,lsmv_case_ques_SUBSET.ques_case_rec_id  ,lsmv_case_ques_SUBSET.ques_ari_rec_id  ,lsmv_case_ques_SUBSET.ques_answer  ,lsmv_case_ques_SUBSET.ques_allow_future_date  ,lsmv_case_ques_details_SUBSET.quesdet_wf_activity_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_user_modified  ,lsmv_case_ques_details_SUBSET.quesdet_user_created  ,lsmv_case_ques_details_SUBSET.quesdet_status  ,lsmv_case_ques_details_SUBSET.quesdet_spr_id  ,lsmv_case_ques_details_SUBSET.quesdet_sent_date  ,lsmv_case_ques_details_SUBSET.quesdet_rule_rec_ids  ,lsmv_case_ques_details_SUBSET.quesdet_reply_count  ,lsmv_case_ques_details_SUBSET.quesdet_reminder_count  ,lsmv_case_ques_details_SUBSET.quesdet_rejection  ,lsmv_case_ques_details_SUBSET.quesdet_record_id  ,lsmv_case_ques_details_SUBSET.quesdet_question_generated  ,lsmv_case_ques_details_SUBSET.quesdet_password_detail  ,lsmv_case_ques_details_SUBSET.quesdet_no_of_reminder_triggred  ,lsmv_case_ques_details_SUBSET.quesdet_no_of_que_ans  ,lsmv_case_ques_details_SUBSET.quesdet_module_type  ,lsmv_case_ques_details_SUBSET.quesdet_manually_sent  ,lsmv_case_ques_details_SUBSET.quesdet_lrn_no  ,lsmv_case_ques_details_SUBSET.quesdet_latest_corresp_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_last_alert_triggered  ,lsmv_case_ques_details_SUBSET.quesdet_is_recipient_exist  ,lsmv_case_ques_details_SUBSET.quesdet_generated_date  ,lsmv_case_ques_details_SUBSET.quesdet_fuq_reason_decline  ,lsmv_case_ques_details_SUBSET.quesdet_fuq_reason_comments  ,lsmv_case_ques_details_SUBSET.quesdet_fuq_reason_code  ,lsmv_case_ques_details_SUBSET.quesdet_form_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_form_name  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_rem_template  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_id  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_email_template  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_document_template  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_alert_type  ,lsmv_case_ques_details_SUBSET.quesdet_follow_up_ack_template  ,lsmv_case_ques_details_SUBSET.quesdet_document_tempalte_data  ,lsmv_case_ques_details_SUBSET.quesdet_date_reply_received  ,lsmv_case_ques_details_SUBSET.quesdet_date_modified  ,lsmv_case_ques_details_SUBSET.quesdet_date_created  ,lsmv_case_ques_details_SUBSET.quesdet_correspondence_status  ,lsmv_case_ques_details_SUBSET.quesdet_correspondence_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_config_lang  ,lsmv_case_ques_details_SUBSET.quesdet_case_version  ,lsmv_case_ques_details_SUBSET.quesdet_case_update  ,lsmv_case_ques_details_SUBSET.quesdet_case_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_case_id  ,lsmv_case_ques_details_SUBSET.quesdet_auto_update_fu_data  ,lsmv_case_ques_details_SUBSET.quesdet_ari_rec_id  ,lsmv_case_ques_details_SUBSET.quesdet_ack_date ,CONCAT( NVL(lsmv_case_ques_SUBSET.ques_RECORD_ID,-1),'||',NVL(lsmv_case_ques_details_SUBSET.quesdet_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_case_ques_SUBSET  LEFT JOIN lsmv_case_ques_details_SUBSET ON lsmv_case_ques_SUBSET.ques_fk_agx_ques_rec_id=lsmv_case_ques_details_SUBSET.quesdet_RECORD_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_case_ques_SUBSET.ques_ARI_REC_ID,lsmv_case_ques_details_SUBSET.quesdet_ARI_REC_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP)
where target_table_name='LS_DB_CASE_QUES'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_QUES'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES   
SET LS_DB_CASE_QUES.ques_user_modified = LS_DB_CASE_QUES_TMP.ques_user_modified,LS_DB_CASE_QUES.ques_user_created = LS_DB_CASE_QUES_TMP.ques_user_created,LS_DB_CASE_QUES.ques_ui_type = LS_DB_CASE_QUES_TMP.ques_ui_type,LS_DB_CASE_QUES.ques_status_code = LS_DB_CASE_QUES_TMP.ques_status_code,LS_DB_CASE_QUES.ques_spr_id = LS_DB_CASE_QUES_TMP.ques_spr_id,LS_DB_CASE_QUES.ques_rejection = LS_DB_CASE_QUES_TMP.ques_rejection,LS_DB_CASE_QUES.ques_record_id = LS_DB_CASE_QUES_TMP.ques_record_id,LS_DB_CASE_QUES.ques_question_type = LS_DB_CASE_QUES_TMP.ques_question_type,LS_DB_CASE_QUES.ques_question_index = LS_DB_CASE_QUES_TMP.ques_question_index,LS_DB_CASE_QUES.ques_question_id = LS_DB_CASE_QUES_TMP.ques_question_id,LS_DB_CASE_QUES.ques_question_deleted_edited_manually = LS_DB_CASE_QUES_TMP.ques_question_deleted_edited_manually,LS_DB_CASE_QUES.ques_question = LS_DB_CASE_QUES_TMP.ques_question,LS_DB_CASE_QUES.ques_ques_rec_id = LS_DB_CASE_QUES_TMP.ques_ques_rec_id,LS_DB_CASE_QUES.ques_manual_created = LS_DB_CASE_QUES_TMP.ques_manual_created,LS_DB_CASE_QUES.ques_is_privacy_enabled = LS_DB_CASE_QUES_TMP.ques_is_privacy_enabled,LS_DB_CASE_QUES.ques_is_answered_by_user = LS_DB_CASE_QUES_TMP.ques_is_answered_by_user,LS_DB_CASE_QUES.ques_imprecise_date_fmt = LS_DB_CASE_QUES_TMP.ques_imprecise_date_fmt,LS_DB_CASE_QUES.ques_fk_agx_ques_rec_id = LS_DB_CASE_QUES_TMP.ques_fk_agx_ques_rec_id,LS_DB_CASE_QUES.ques_field_id = LS_DB_CASE_QUES_TMP.ques_field_id,LS_DB_CASE_QUES.ques_entity_rec_id = LS_DB_CASE_QUES_TMP.ques_entity_rec_id,LS_DB_CASE_QUES.ques_entity_context = LS_DB_CASE_QUES_TMP.ques_entity_context,LS_DB_CASE_QUES.ques_date_modified = LS_DB_CASE_QUES_TMP.ques_date_modified,LS_DB_CASE_QUES.ques_date_created = LS_DB_CASE_QUES_TMP.ques_date_created,LS_DB_CASE_QUES.ques_context = LS_DB_CASE_QUES_TMP.ques_context,LS_DB_CASE_QUES.ques_case_value = LS_DB_CASE_QUES_TMP.ques_case_value,LS_DB_CASE_QUES.ques_case_update = LS_DB_CASE_QUES_TMP.ques_case_update,LS_DB_CASE_QUES.ques_case_rec_id = LS_DB_CASE_QUES_TMP.ques_case_rec_id,LS_DB_CASE_QUES.ques_ari_rec_id = LS_DB_CASE_QUES_TMP.ques_ari_rec_id,LS_DB_CASE_QUES.ques_answer = LS_DB_CASE_QUES_TMP.ques_answer,LS_DB_CASE_QUES.ques_allow_future_date = LS_DB_CASE_QUES_TMP.ques_allow_future_date,LS_DB_CASE_QUES.quesdet_wf_activity_rec_id = LS_DB_CASE_QUES_TMP.quesdet_wf_activity_rec_id,LS_DB_CASE_QUES.quesdet_user_modified = LS_DB_CASE_QUES_TMP.quesdet_user_modified,LS_DB_CASE_QUES.quesdet_user_created = LS_DB_CASE_QUES_TMP.quesdet_user_created,LS_DB_CASE_QUES.quesdet_status = LS_DB_CASE_QUES_TMP.quesdet_status,LS_DB_CASE_QUES.quesdet_spr_id = LS_DB_CASE_QUES_TMP.quesdet_spr_id,LS_DB_CASE_QUES.quesdet_sent_date = LS_DB_CASE_QUES_TMP.quesdet_sent_date,LS_DB_CASE_QUES.quesdet_rule_rec_ids = LS_DB_CASE_QUES_TMP.quesdet_rule_rec_ids,LS_DB_CASE_QUES.quesdet_reply_count = LS_DB_CASE_QUES_TMP.quesdet_reply_count,LS_DB_CASE_QUES.quesdet_reminder_count = LS_DB_CASE_QUES_TMP.quesdet_reminder_count,LS_DB_CASE_QUES.quesdet_rejection = LS_DB_CASE_QUES_TMP.quesdet_rejection,LS_DB_CASE_QUES.quesdet_record_id = LS_DB_CASE_QUES_TMP.quesdet_record_id,LS_DB_CASE_QUES.quesdet_question_generated = LS_DB_CASE_QUES_TMP.quesdet_question_generated,LS_DB_CASE_QUES.quesdet_password_detail = LS_DB_CASE_QUES_TMP.quesdet_password_detail,LS_DB_CASE_QUES.quesdet_no_of_reminder_triggred = LS_DB_CASE_QUES_TMP.quesdet_no_of_reminder_triggred,LS_DB_CASE_QUES.quesdet_no_of_que_ans = LS_DB_CASE_QUES_TMP.quesdet_no_of_que_ans,LS_DB_CASE_QUES.quesdet_module_type = LS_DB_CASE_QUES_TMP.quesdet_module_type,LS_DB_CASE_QUES.quesdet_manually_sent = LS_DB_CASE_QUES_TMP.quesdet_manually_sent,LS_DB_CASE_QUES.quesdet_lrn_no = LS_DB_CASE_QUES_TMP.quesdet_lrn_no,LS_DB_CASE_QUES.quesdet_latest_corresp_rec_id = LS_DB_CASE_QUES_TMP.quesdet_latest_corresp_rec_id,LS_DB_CASE_QUES.quesdet_last_alert_triggered = LS_DB_CASE_QUES_TMP.quesdet_last_alert_triggered,LS_DB_CASE_QUES.quesdet_is_recipient_exist = LS_DB_CASE_QUES_TMP.quesdet_is_recipient_exist,LS_DB_CASE_QUES.quesdet_generated_date = LS_DB_CASE_QUES_TMP.quesdet_generated_date,LS_DB_CASE_QUES.quesdet_fuq_reason_decline = LS_DB_CASE_QUES_TMP.quesdet_fuq_reason_decline,LS_DB_CASE_QUES.quesdet_fuq_reason_comments = LS_DB_CASE_QUES_TMP.quesdet_fuq_reason_comments,LS_DB_CASE_QUES.quesdet_fuq_reason_code = LS_DB_CASE_QUES_TMP.quesdet_fuq_reason_code,LS_DB_CASE_QUES.quesdet_form_rec_id = LS_DB_CASE_QUES_TMP.quesdet_form_rec_id,LS_DB_CASE_QUES.quesdet_form_name = LS_DB_CASE_QUES_TMP.quesdet_form_name,LS_DB_CASE_QUES.quesdet_follow_up_rem_template = LS_DB_CASE_QUES_TMP.quesdet_follow_up_rem_template,LS_DB_CASE_QUES.quesdet_follow_up_id = LS_DB_CASE_QUES_TMP.quesdet_follow_up_id,LS_DB_CASE_QUES.quesdet_follow_up_email_template = LS_DB_CASE_QUES_TMP.quesdet_follow_up_email_template,LS_DB_CASE_QUES.quesdet_follow_up_document_template = LS_DB_CASE_QUES_TMP.quesdet_follow_up_document_template,LS_DB_CASE_QUES.quesdet_follow_up_alert_type = LS_DB_CASE_QUES_TMP.quesdet_follow_up_alert_type,LS_DB_CASE_QUES.quesdet_follow_up_ack_template = LS_DB_CASE_QUES_TMP.quesdet_follow_up_ack_template,LS_DB_CASE_QUES.quesdet_document_tempalte_data = LS_DB_CASE_QUES_TMP.quesdet_document_tempalte_data,LS_DB_CASE_QUES.quesdet_date_reply_received = LS_DB_CASE_QUES_TMP.quesdet_date_reply_received,LS_DB_CASE_QUES.quesdet_date_modified = LS_DB_CASE_QUES_TMP.quesdet_date_modified,LS_DB_CASE_QUES.quesdet_date_created = LS_DB_CASE_QUES_TMP.quesdet_date_created,LS_DB_CASE_QUES.quesdet_correspondence_status = LS_DB_CASE_QUES_TMP.quesdet_correspondence_status,LS_DB_CASE_QUES.quesdet_correspondence_rec_id = LS_DB_CASE_QUES_TMP.quesdet_correspondence_rec_id,LS_DB_CASE_QUES.quesdet_config_lang = LS_DB_CASE_QUES_TMP.quesdet_config_lang,LS_DB_CASE_QUES.quesdet_case_version = LS_DB_CASE_QUES_TMP.quesdet_case_version,LS_DB_CASE_QUES.quesdet_case_update = LS_DB_CASE_QUES_TMP.quesdet_case_update,LS_DB_CASE_QUES.quesdet_case_rec_id = LS_DB_CASE_QUES_TMP.quesdet_case_rec_id,LS_DB_CASE_QUES.quesdet_case_id = LS_DB_CASE_QUES_TMP.quesdet_case_id,LS_DB_CASE_QUES.quesdet_auto_update_fu_data = LS_DB_CASE_QUES_TMP.quesdet_auto_update_fu_data,LS_DB_CASE_QUES.quesdet_ari_rec_id = LS_DB_CASE_QUES_TMP.quesdet_ari_rec_id,LS_DB_CASE_QUES.quesdet_ack_date = LS_DB_CASE_QUES_TMP.quesdet_ack_date,
LS_DB_CASE_QUES.PROCESSING_DT = LS_DB_CASE_QUES_TMP.PROCESSING_DT,
LS_DB_CASE_QUES.receipt_id     =LS_DB_CASE_QUES_TMP.receipt_id    ,
LS_DB_CASE_QUES.case_no        =LS_DB_CASE_QUES_TMP.case_no           ,
LS_DB_CASE_QUES.case_version   =LS_DB_CASE_QUES_TMP.case_version      ,
LS_DB_CASE_QUES.version_no     =LS_DB_CASE_QUES_TMP.version_no        ,
LS_DB_CASE_QUES.user_modified  =LS_DB_CASE_QUES_TMP.user_modified     ,
LS_DB_CASE_QUES.date_modified  =LS_DB_CASE_QUES_TMP.date_modified     ,
LS_DB_CASE_QUES.expiry_date    =LS_DB_CASE_QUES_TMP.expiry_date       ,
LS_DB_CASE_QUES.created_by     =LS_DB_CASE_QUES_TMP.created_by        ,
LS_DB_CASE_QUES.created_dt     =LS_DB_CASE_QUES_TMP.created_dt        ,
LS_DB_CASE_QUES.load_ts        =LS_DB_CASE_QUES_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP 
WHERE 	LS_DB_CASE_QUES.INTEGRATION_ID = LS_DB_CASE_QUES_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_QUES_TMP.PROCESSING_DT = LS_DB_CASE_QUES.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_QUES.ques_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CASE_QUES.quesdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CASE_QUES_TMP.ques_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CASE_QUES_TMP.quesdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_QUES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_QUES.ques_ARI_REC_ID ,LS_DB_CASE_QUES.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP 
ON LS_DB_CASE_QUES.ques_ARI_REC_ID=LS_DB_CASE_QUES_TMP.ques_ARI_REC_ID
AND LS_DB_CASE_QUES.INTEGRATION_ID = LS_DB_CASE_QUES_TMP.INTEGRATION_ID 
where LS_DB_CASE_QUES_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_QUES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_QUES.ques_ARI_REC_ID in (select ques_ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP )
) TMP where LS_DB_CASE_QUES.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_QUES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_QUES.ques_ARI_REC_ID ,LS_DB_CASE_QUES.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP 
    ON LS_DB_CASE_QUES.ques_ARI_REC_ID=LS_DB_CASE_QUES_TMP.ques_ARI_REC_ID
    AND LS_DB_CASE_QUES.INTEGRATION_ID = LS_DB_CASE_QUES_TMP.INTEGRATION_ID 
    where LS_DB_CASE_QUES_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_QUES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_QUES.ques_ARI_REC_ID in (select ques_ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES
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
integration_id ,ques_user_modified,
ques_user_created,
ques_ui_type,
ques_status_code,
ques_spr_id,
ques_rejection,
ques_record_id,
ques_question_type,
ques_question_index,
ques_question_id,
ques_question_deleted_edited_manually,
ques_question,
ques_ques_rec_id,
ques_manual_created,
ques_is_privacy_enabled,
ques_is_answered_by_user,
ques_imprecise_date_fmt,
ques_fk_agx_ques_rec_id,
ques_field_id,
ques_entity_rec_id,
ques_entity_context,
ques_date_modified,
ques_date_created,
ques_context,
ques_case_value,
ques_case_update,
ques_case_rec_id,
ques_ari_rec_id,
ques_answer,
ques_allow_future_date,
quesdet_wf_activity_rec_id,
quesdet_user_modified,
quesdet_user_created,
quesdet_status,
quesdet_spr_id,
quesdet_sent_date,
quesdet_rule_rec_ids,
quesdet_reply_count,
quesdet_reminder_count,
quesdet_rejection,
quesdet_record_id,
quesdet_question_generated,
quesdet_password_detail,
quesdet_no_of_reminder_triggred,
quesdet_no_of_que_ans,
quesdet_module_type,
quesdet_manually_sent,
quesdet_lrn_no,
quesdet_latest_corresp_rec_id,
quesdet_last_alert_triggered,
quesdet_is_recipient_exist,
quesdet_generated_date,
quesdet_fuq_reason_decline,
quesdet_fuq_reason_comments,
quesdet_fuq_reason_code,
quesdet_form_rec_id,
quesdet_form_name,
quesdet_follow_up_rem_template,
quesdet_follow_up_id,
quesdet_follow_up_email_template,
quesdet_follow_up_document_template,
quesdet_follow_up_alert_type,
quesdet_follow_up_ack_template,
quesdet_document_tempalte_data,
quesdet_date_reply_received,
quesdet_date_modified,
quesdet_date_created,
quesdet_correspondence_status,
quesdet_correspondence_rec_id,
quesdet_config_lang,
quesdet_case_version,
quesdet_case_update,
quesdet_case_rec_id,
quesdet_case_id,
quesdet_auto_update_fu_data,
quesdet_ari_rec_id,
quesdet_ack_date)
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
integration_id ,ques_user_modified,
ques_user_created,
ques_ui_type,
ques_status_code,
ques_spr_id,
ques_rejection,
ques_record_id,
ques_question_type,
ques_question_index,
ques_question_id,
ques_question_deleted_edited_manually,
ques_question,
ques_ques_rec_id,
ques_manual_created,
ques_is_privacy_enabled,
ques_is_answered_by_user,
ques_imprecise_date_fmt,
ques_fk_agx_ques_rec_id,
ques_field_id,
ques_entity_rec_id,
ques_entity_context,
ques_date_modified,
ques_date_created,
ques_context,
ques_case_value,
ques_case_update,
ques_case_rec_id,
ques_ari_rec_id,
ques_answer,
ques_allow_future_date,
quesdet_wf_activity_rec_id,
quesdet_user_modified,
quesdet_user_created,
quesdet_status,
quesdet_spr_id,
quesdet_sent_date,
quesdet_rule_rec_ids,
quesdet_reply_count,
quesdet_reminder_count,
quesdet_rejection,
quesdet_record_id,
quesdet_question_generated,
quesdet_password_detail,
quesdet_no_of_reminder_triggred,
quesdet_no_of_que_ans,
quesdet_module_type,
quesdet_manually_sent,
quesdet_lrn_no,
quesdet_latest_corresp_rec_id,
quesdet_last_alert_triggered,
quesdet_is_recipient_exist,
quesdet_generated_date,
quesdet_fuq_reason_decline,
quesdet_fuq_reason_comments,
quesdet_fuq_reason_code,
quesdet_form_rec_id,
quesdet_form_name,
quesdet_follow_up_rem_template,
quesdet_follow_up_id,
quesdet_follow_up_email_template,
quesdet_follow_up_document_template,
quesdet_follow_up_alert_type,
quesdet_follow_up_ack_template,
quesdet_document_tempalte_data,
quesdet_date_reply_received,
quesdet_date_modified,
quesdet_date_created,
quesdet_correspondence_status,
quesdet_correspondence_rec_id,
quesdet_config_lang,
quesdet_case_version,
quesdet_case_update,
quesdet_case_rec_id,
quesdet_case_id,
quesdet_auto_update_fu_data,
quesdet_ari_rec_id,
quesdet_ack_date
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP 
WHERE 	TO_DATE(LS_DB_CASE_QUES.PROCESSING_DT) < TO_DATE(LS_DB_CASE_QUES_TMP.PROCESSING_DT)
AND LS_DB_CASE_QUES.INTEGRATION_ID = LS_DB_CASE_QUES_TMP.INTEGRATION_ID
AND LS_DB_CASE_QUES.ques_ARI_REC_ID = LS_DB_CASE_QUES_TMP.ques_ARI_REC_ID
AND LS_DB_CASE_QUES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CASE_QUES.ques_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CASE_QUES.quesdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CASE_QUES_TMP.ques_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CASE_QUES_TMP.quesdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES TGT
WHERE  ( ques_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques') OR quesdet_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques_details')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( ques_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques') OR quesdet_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASE_QUES_DELETION_TMP  WHERE TABLE_NAME='lsmv_case_ques_details')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASE_QUES_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_QUES'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_QUES'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_QUES Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_QUES'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_QUES not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
