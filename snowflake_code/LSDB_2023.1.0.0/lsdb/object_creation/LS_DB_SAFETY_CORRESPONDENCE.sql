
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SAFETY_CORRESPONDENCE()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_CORRESPONDENCE'),
	'LSRA','Case','LS_DB_SAFETY_CORRESPONDENCE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SAFETY_CORRESPONDENCE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_CORRESPONDENCE'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_CORRESPONDENCE'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  As select RECORD_ID,'lsmv_safety_corresp_doc' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_doc WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_safety_corresp_queue' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_queue WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_safety_correspondence' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_correspondence WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT correspondence_ref record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_sfty_corr_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_correspondence WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_correspondence WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_safety_correspondence_SUBSET AS 
(
select * from 
    (SELECT  
    ack_reply  saftycorr_ack_reply,all_facility_users  saftycorr_all_facility_users,ari_rec_id  saftycorr_ari_rec_id,auto_reply_fu_corres  saftycorr_auto_reply_fu_corres,call_duration  saftycorr_call_duration,call_received_from  saftycorr_call_received_from,call_status  saftycorr_call_status,carbon_copy  saftycorr_carbon_copy,category  saftycorr_category,comp_rec_id  saftycorr_comp_rec_id,corr_flag  saftycorr_corr_flag,correspondence_date  saftycorr_correspondence_date,correspondence_mode  saftycorr_correspondence_mode,correspondence_seq  saftycorr_correspondence_seq,cover_page_template_name  saftycorr_cover_page_template_name,cover_page_template_record_id  saftycorr_cover_page_template_record_id,date_created  saftycorr_date_created,date_modified  saftycorr_date_modified,date_transmitted  saftycorr_date_transmitted,delivery_status  saftycorr_delivery_status,direction  saftycorr_direction,dispatch_status  saftycorr_dispatch_status,display_in_portal  saftycorr_display_in_portal,email_notification  saftycorr_email_notification,expected_reply_date  saftycorr_expected_reply_date,fax_id  saftycorr_fax_id,fax_sent_date  saftycorr_fax_sent_date,fax_status  saftycorr_fax_status,fk_aim_ref_rec_id  saftycorr_fk_aim_ref_rec_id,fk_ccm_comm_rec_id  saftycorr_fk_ccm_comm_rec_id,fk_ccm_comm_thread_rec_id  saftycorr_fk_ccm_comm_thread_rec_id,fk_naer_ref_rec_id  saftycorr_fk_naer_ref_rec_id,fk_saf_corr_doc_rec_id  saftycorr_fk_saf_corr_doc_rec_id,followup_type  saftycorr_followup_type,from_email_id  saftycorr_from_email_id,from_fax_no  saftycorr_from_fax_no,include_icsrxml  saftycorr_include_icsrxml,inq_rec_id  saftycorr_inq_rec_id,insert_solution  saftycorr_insert_solution,letter_creation_option  saftycorr_letter_creation_option,letter_received_from  saftycorr_letter_received_from,letter_send_date  saftycorr_letter_send_date,letter_sent_to  saftycorr_letter_sent_to,letter_word_editor_value  saftycorr_letter_word_editor_value,local_correspondence_view  saftycorr_local_correspondence_view,lrn  saftycorr_lrn,lrn_version  saftycorr_lrn_version,message_id  saftycorr_message_id,no_of_reminders  saftycorr_no_of_reminders,phone_area_code  saftycorr_phone_area_code,phone_cntry_code  saftycorr_phone_cntry_code,phone_no  saftycorr_phone_no,priority  saftycorr_priority,ques_rec_id  saftycorr_ques_rec_id,read_correspondence  saftycorr_read_correspondence,receipt_no  saftycorr_receipt_no,record_id  saftycorr_record_id,rem_override  saftycorr_rem_override,rem_template_name  saftycorr_rem_template_name,rem_template_record_id  saftycorr_rem_template_record_id,rem_turn_off  saftycorr_rem_turn_off,reminder_count  saftycorr_reminder_count,reminder_flag  saftycorr_reminder_flag,reminder_interval  saftycorr_reminder_interval,reminder_interval_unit  saftycorr_reminder_interval_unit,reminder_next_date  saftycorr_reminder_next_date,reminder_sent_date  saftycorr_reminder_sent_date,response_corres_notify  saftycorr_response_corres_notify,review_reminder  saftycorr_review_reminder,sent_by  saftycorr_sent_by,spr_id  saftycorr_spr_id,status  saftycorr_status,subject  saftycorr_subject,template_name  saftycorr_template_name,template_record_id  saftycorr_template_record_id,to_email_id  saftycorr_to_email_id,to_fax_no  saftycorr_to_fax_no,user_created  saftycorr_user_created,user_modified  saftycorr_user_modified,who_received_call  saftycorr_who_received_call,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_correspondence
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_correspondence')
  ) where REC_RANK=1 )
  , lsmv_safety_corresp_doc_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  saftycorrdoc_ari_rec_id,category_code  saftycorrdoc_category_code,date_created  saftycorrdoc_date_created,date_modified  saftycorrdoc_date_modified,doc_id  saftycorrdoc_doc_id,doc_name  saftycorrdoc_doc_name,doc_size  saftycorrdoc_doc_size,doc_source  saftycorrdoc_doc_source,doc_template_type  saftycorrdoc_doc_template_type,fk_sfty_corr_rec_id  saftycorrdoc_fk_sfty_corr_rec_id,record_id  saftycorrdoc_record_id,spr_id  saftycorrdoc_spr_id,user_created  saftycorrdoc_user_created,user_modified  saftycorrdoc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_doc
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_sfty_corr_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_doc')
  ) where REC_RANK=1 )
  , lsmv_safety_corresp_queue_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  saftycorrq_ari_rec_id,comp_rec_id  saftycorrq_comp_rec_id,correspondence_ref  saftycorrq_correspondence_ref,date_created  saftycorrq_date_created,date_modified  saftycorrq_date_modified,dispatch_status  saftycorrq_dispatch_status,fax_completion_date  saftycorrq_fax_completion_date,fax_status_code  saftycorrq_fax_status_code,fax_status_desc  saftycorrq_fax_status_desc,fax_transmission_id  saftycorrq_fax_transmission_id,inq_rec_id  saftycorrq_inq_rec_id,locale  saftycorrq_locale,medium_code  saftycorrq_medium_code,record_id  saftycorrq_record_id,spr_id  saftycorrq_spr_id,user_created  saftycorrq_user_created,user_modified  saftycorrq_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_safety_corresp_queue
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
correspondence_ref IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_queue')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_safety_correspondence_SUBSET.saftycorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_safety_corresp_queue_SUBSET.saftycorrq_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_safety_correspondence_SUBSET.saftycorr_USER_MODIFIED USER_MODIFIED,lsmv_safety_correspondence_SUBSET.saftycorr_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_safety_correspondence_SUBSET.saftycorr_USER_CREATED CREATED_BY,lsmv_safety_correspondence_SUBSET.saftycorr_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_safety_correspondence_SUBSET.saftycorr_who_received_call  ,lsmv_safety_correspondence_SUBSET.saftycorr_user_modified  ,lsmv_safety_correspondence_SUBSET.saftycorr_user_created  ,lsmv_safety_correspondence_SUBSET.saftycorr_to_fax_no  ,lsmv_safety_correspondence_SUBSET.saftycorr_to_email_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_template_record_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_template_name  ,lsmv_safety_correspondence_SUBSET.saftycorr_subject  ,lsmv_safety_correspondence_SUBSET.saftycorr_status  ,lsmv_safety_correspondence_SUBSET.saftycorr_spr_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_sent_by  ,lsmv_safety_correspondence_SUBSET.saftycorr_review_reminder  ,lsmv_safety_correspondence_SUBSET.saftycorr_response_corres_notify  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_sent_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_next_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_interval_unit  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_interval  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_flag  ,lsmv_safety_correspondence_SUBSET.saftycorr_reminder_count  ,lsmv_safety_correspondence_SUBSET.saftycorr_rem_turn_off  ,lsmv_safety_correspondence_SUBSET.saftycorr_rem_template_record_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_rem_template_name  ,lsmv_safety_correspondence_SUBSET.saftycorr_rem_override  ,lsmv_safety_correspondence_SUBSET.saftycorr_record_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_receipt_no  ,lsmv_safety_correspondence_SUBSET.saftycorr_read_correspondence  ,lsmv_safety_correspondence_SUBSET.saftycorr_ques_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_priority  ,lsmv_safety_correspondence_SUBSET.saftycorr_phone_no  ,lsmv_safety_correspondence_SUBSET.saftycorr_phone_cntry_code  ,lsmv_safety_correspondence_SUBSET.saftycorr_phone_area_code  ,lsmv_safety_correspondence_SUBSET.saftycorr_no_of_reminders  ,lsmv_safety_correspondence_SUBSET.saftycorr_message_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_lrn_version  ,lsmv_safety_correspondence_SUBSET.saftycorr_lrn  ,lsmv_safety_correspondence_SUBSET.saftycorr_local_correspondence_view  ,lsmv_safety_correspondence_SUBSET.saftycorr_letter_word_editor_value  ,lsmv_safety_correspondence_SUBSET.saftycorr_letter_sent_to  ,lsmv_safety_correspondence_SUBSET.saftycorr_letter_send_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_letter_received_from  ,lsmv_safety_correspondence_SUBSET.saftycorr_letter_creation_option  ,lsmv_safety_correspondence_SUBSET.saftycorr_insert_solution  ,lsmv_safety_correspondence_SUBSET.saftycorr_inq_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_include_icsrxml  ,lsmv_safety_correspondence_SUBSET.saftycorr_from_fax_no  ,lsmv_safety_correspondence_SUBSET.saftycorr_from_email_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_followup_type  ,lsmv_safety_correspondence_SUBSET.saftycorr_fk_saf_corr_doc_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_fk_naer_ref_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_fk_ccm_comm_thread_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_fk_ccm_comm_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_fk_aim_ref_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_fax_status  ,lsmv_safety_correspondence_SUBSET.saftycorr_fax_sent_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_fax_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_expected_reply_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_email_notification  ,lsmv_safety_correspondence_SUBSET.saftycorr_display_in_portal  ,lsmv_safety_correspondence_SUBSET.saftycorr_dispatch_status  ,lsmv_safety_correspondence_SUBSET.saftycorr_direction  ,lsmv_safety_correspondence_SUBSET.saftycorr_delivery_status  ,lsmv_safety_correspondence_SUBSET.saftycorr_date_transmitted  ,lsmv_safety_correspondence_SUBSET.saftycorr_date_modified  ,lsmv_safety_correspondence_SUBSET.saftycorr_date_created  ,lsmv_safety_correspondence_SUBSET.saftycorr_cover_page_template_record_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_cover_page_template_name  ,lsmv_safety_correspondence_SUBSET.saftycorr_correspondence_seq  ,lsmv_safety_correspondence_SUBSET.saftycorr_correspondence_mode  ,lsmv_safety_correspondence_SUBSET.saftycorr_correspondence_date  ,lsmv_safety_correspondence_SUBSET.saftycorr_corr_flag  ,lsmv_safety_correspondence_SUBSET.saftycorr_comp_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_category  ,lsmv_safety_correspondence_SUBSET.saftycorr_carbon_copy  ,lsmv_safety_correspondence_SUBSET.saftycorr_call_status  ,lsmv_safety_correspondence_SUBSET.saftycorr_call_received_from  ,lsmv_safety_correspondence_SUBSET.saftycorr_call_duration  ,lsmv_safety_correspondence_SUBSET.saftycorr_auto_reply_fu_corres  ,lsmv_safety_correspondence_SUBSET.saftycorr_ari_rec_id  ,lsmv_safety_correspondence_SUBSET.saftycorr_all_facility_users  ,lsmv_safety_correspondence_SUBSET.saftycorr_ack_reply  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_user_modified  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_user_created  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_spr_id  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_record_id  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_medium_code  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_locale  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_inq_rec_id  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_fax_transmission_id  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_fax_status_desc  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_fax_status_code  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_fax_completion_date  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_dispatch_status  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_date_modified  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_date_created  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_correspondence_ref  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_comp_rec_id  ,lsmv_safety_corresp_queue_SUBSET.saftycorrq_ari_rec_id  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_user_modified  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_user_created  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_spr_id  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_record_id  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_fk_sfty_corr_rec_id  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_doc_template_type  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_doc_source  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_doc_size  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_doc_name  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_doc_id  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_date_modified  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_date_created  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_category_code  ,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_ari_rec_id ,CONCAT( NVL(lsmv_safety_correspondence_SUBSET.saftycorr_RECORD_ID,-1),'||',NVL(lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_RECORD_ID,-1),'||',NVL(lsmv_safety_corresp_queue_SUBSET.saftycorrq_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_safety_correspondence_SUBSET  LEFT JOIN lsmv_safety_corresp_doc_SUBSET ON lsmv_safety_correspondence_SUBSET.saftycorr_record_id=lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_fk_sfty_corr_rec_id
                         LEFT JOIN lsmv_safety_corresp_queue_SUBSET ON lsmv_safety_correspondence_SUBSET.saftycorr_record_id=lsmv_safety_corresp_queue_SUBSET.saftycorrq_correspondence_ref
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_safety_correspondence_SUBSET.saftycorr_RECORD_ID,lsmv_safety_corresp_doc_SUBSET.saftycorrdoc_RECORD_ID,lsmv_safety_corresp_queue_SUBSET.saftycorrq_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP)
where target_table_name='LS_DB_SAFETY_CORRESPONDENCE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_CORRESPONDENCE'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE   
SET LS_DB_SAFETY_CORRESPONDENCE.saftycorr_who_received_call = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_who_received_call,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_user_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_user_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_user_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_user_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_to_fax_no = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_to_fax_no,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_to_email_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_to_email_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_template_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_template_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_template_name = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_template_name,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_subject = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_subject,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_spr_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_spr_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_sent_by = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_sent_by,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_review_reminder = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_review_reminder,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_response_corres_notify = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_response_corres_notify,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_sent_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_sent_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_next_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_next_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_interval_unit = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_interval_unit,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_interval = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_interval,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_flag = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_flag,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_reminder_count = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_reminder_count,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_rem_turn_off = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_rem_turn_off,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_rem_template_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_rem_template_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_rem_template_name = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_rem_template_name,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_rem_override = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_rem_override,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_receipt_no = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_receipt_no,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_read_correspondence = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_read_correspondence,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_ques_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_ques_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_priority = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_priority,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_phone_no = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_phone_no,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_phone_cntry_code = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_phone_cntry_code,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_phone_area_code = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_phone_area_code,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_no_of_reminders = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_no_of_reminders,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_message_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_message_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_lrn_version = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_lrn_version,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_lrn = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_lrn,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_local_correspondence_view = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_local_correspondence_view,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_letter_word_editor_value = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_letter_word_editor_value,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_letter_sent_to = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_letter_sent_to,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_letter_send_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_letter_send_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_letter_received_from = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_letter_received_from,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_letter_creation_option = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_letter_creation_option,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_insert_solution = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_insert_solution,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_inq_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_inq_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_include_icsrxml = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_include_icsrxml,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_from_fax_no = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_from_fax_no,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_from_email_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_from_email_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_followup_type = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_followup_type,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fk_saf_corr_doc_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fk_saf_corr_doc_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fk_naer_ref_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fk_naer_ref_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fk_ccm_comm_thread_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fk_ccm_comm_thread_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fk_ccm_comm_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fk_ccm_comm_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fk_aim_ref_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fk_aim_ref_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fax_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fax_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fax_sent_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fax_sent_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_fax_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_fax_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_expected_reply_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_expected_reply_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_email_notification = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_email_notification,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_display_in_portal = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_display_in_portal,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_dispatch_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_dispatch_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_direction = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_direction,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_delivery_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_delivery_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_date_transmitted = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_date_transmitted,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_date_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_date_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_date_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_date_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_cover_page_template_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_cover_page_template_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_cover_page_template_name = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_cover_page_template_name,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_correspondence_seq = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_correspondence_seq,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_correspondence_mode = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_correspondence_mode,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_correspondence_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_correspondence_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_corr_flag = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_corr_flag,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_comp_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_comp_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_category = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_category,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_carbon_copy = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_carbon_copy,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_call_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_call_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_call_received_from = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_call_received_from,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_call_duration = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_call_duration,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_auto_reply_fu_corres = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_auto_reply_fu_corres,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_ari_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_ari_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_all_facility_users = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_all_facility_users,LS_DB_SAFETY_CORRESPONDENCE.saftycorr_ack_reply = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_ack_reply,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_user_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_user_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_user_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_user_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_spr_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_spr_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_medium_code = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_medium_code,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_locale = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_locale,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_inq_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_inq_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_fax_transmission_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_fax_transmission_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_fax_status_desc = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_fax_status_desc,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_fax_status_code = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_fax_status_code,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_fax_completion_date = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_fax_completion_date,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_dispatch_status = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_dispatch_status,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_date_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_date_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_date_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_date_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_correspondence_ref = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_correspondence_ref,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_comp_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_comp_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_ari_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_ari_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_user_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_user_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_user_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_user_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_spr_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_spr_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_record_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_record_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_fk_sfty_corr_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_fk_sfty_corr_rec_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_doc_template_type = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_doc_template_type,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_doc_source = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_doc_source,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_doc_size = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_doc_size,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_doc_name = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_doc_name,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_doc_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_doc_id,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_date_modified = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_date_modified,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_date_created = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_date_created,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_category_code = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_category_code,LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_ari_rec_id = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_ari_rec_id,
LS_DB_SAFETY_CORRESPONDENCE.PROCESSING_DT = LS_DB_SAFETY_CORRESPONDENCE_TMP.PROCESSING_DT,
LS_DB_SAFETY_CORRESPONDENCE.receipt_id     =LS_DB_SAFETY_CORRESPONDENCE_TMP.receipt_id    ,
LS_DB_SAFETY_CORRESPONDENCE.case_no        =LS_DB_SAFETY_CORRESPONDENCE_TMP.case_no           ,
LS_DB_SAFETY_CORRESPONDENCE.case_version   =LS_DB_SAFETY_CORRESPONDENCE_TMP.case_version      ,
LS_DB_SAFETY_CORRESPONDENCE.version_no     =LS_DB_SAFETY_CORRESPONDENCE_TMP.version_no        ,
LS_DB_SAFETY_CORRESPONDENCE.user_modified  =LS_DB_SAFETY_CORRESPONDENCE_TMP.user_modified     ,
LS_DB_SAFETY_CORRESPONDENCE.date_modified  =LS_DB_SAFETY_CORRESPONDENCE_TMP.date_modified     ,
LS_DB_SAFETY_CORRESPONDENCE.expiry_date    =LS_DB_SAFETY_CORRESPONDENCE_TMP.expiry_date       ,
LS_DB_SAFETY_CORRESPONDENCE.created_by     =LS_DB_SAFETY_CORRESPONDENCE_TMP.created_by        ,
LS_DB_SAFETY_CORRESPONDENCE.created_dt     =LS_DB_SAFETY_CORRESPONDENCE_TMP.created_dt        ,
LS_DB_SAFETY_CORRESPONDENCE.load_ts        =LS_DB_SAFETY_CORRESPONDENCE_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP 
WHERE 	LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SAFETY_CORRESPONDENCE_TMP.PROCESSING_DT = LS_DB_SAFETY_CORRESPONDENCE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_SAFETY_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID ,LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP 
ON LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID=LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_RECORD_ID
AND LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID 
where LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID  is null AND LS_DB_SAFETY_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID in (select saftycorr_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP )
) TMP where LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_SAFETY_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID ,LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP 
    ON LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID=LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_RECORD_ID
    AND LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID 
    where LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID  is null AND LS_DB_SAFETY_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID in (select saftycorr_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE
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
integration_id ,saftycorr_who_received_call,
saftycorr_user_modified,
saftycorr_user_created,
saftycorr_to_fax_no,
saftycorr_to_email_id,
saftycorr_template_record_id,
saftycorr_template_name,
saftycorr_subject,
saftycorr_status,
saftycorr_spr_id,
saftycorr_sent_by,
saftycorr_review_reminder,
saftycorr_response_corres_notify,
saftycorr_reminder_sent_date,
saftycorr_reminder_next_date,
saftycorr_reminder_interval_unit,
saftycorr_reminder_interval,
saftycorr_reminder_flag,
saftycorr_reminder_count,
saftycorr_rem_turn_off,
saftycorr_rem_template_record_id,
saftycorr_rem_template_name,
saftycorr_rem_override,
saftycorr_record_id,
saftycorr_receipt_no,
saftycorr_read_correspondence,
saftycorr_ques_rec_id,
saftycorr_priority,
saftycorr_phone_no,
saftycorr_phone_cntry_code,
saftycorr_phone_area_code,
saftycorr_no_of_reminders,
saftycorr_message_id,
saftycorr_lrn_version,
saftycorr_lrn,
saftycorr_local_correspondence_view,
saftycorr_letter_word_editor_value,
saftycorr_letter_sent_to,
saftycorr_letter_send_date,
saftycorr_letter_received_from,
saftycorr_letter_creation_option,
saftycorr_insert_solution,
saftycorr_inq_rec_id,
saftycorr_include_icsrxml,
saftycorr_from_fax_no,
saftycorr_from_email_id,
saftycorr_followup_type,
saftycorr_fk_saf_corr_doc_rec_id,
saftycorr_fk_naer_ref_rec_id,
saftycorr_fk_ccm_comm_thread_rec_id,
saftycorr_fk_ccm_comm_rec_id,
saftycorr_fk_aim_ref_rec_id,
saftycorr_fax_status,
saftycorr_fax_sent_date,
saftycorr_fax_id,
saftycorr_expected_reply_date,
saftycorr_email_notification,
saftycorr_display_in_portal,
saftycorr_dispatch_status,
saftycorr_direction,
saftycorr_delivery_status,
saftycorr_date_transmitted,
saftycorr_date_modified,
saftycorr_date_created,
saftycorr_cover_page_template_record_id,
saftycorr_cover_page_template_name,
saftycorr_correspondence_seq,
saftycorr_correspondence_mode,
saftycorr_correspondence_date,
saftycorr_corr_flag,
saftycorr_comp_rec_id,
saftycorr_category,
saftycorr_carbon_copy,
saftycorr_call_status,
saftycorr_call_received_from,
saftycorr_call_duration,
saftycorr_auto_reply_fu_corres,
saftycorr_ari_rec_id,
saftycorr_all_facility_users,
saftycorr_ack_reply,
saftycorrq_user_modified,
saftycorrq_user_created,
saftycorrq_spr_id,
saftycorrq_record_id,
saftycorrq_medium_code,
saftycorrq_locale,
saftycorrq_inq_rec_id,
saftycorrq_fax_transmission_id,
saftycorrq_fax_status_desc,
saftycorrq_fax_status_code,
saftycorrq_fax_completion_date,
saftycorrq_dispatch_status,
saftycorrq_date_modified,
saftycorrq_date_created,
saftycorrq_correspondence_ref,
saftycorrq_comp_rec_id,
saftycorrq_ari_rec_id,
saftycorrdoc_user_modified,
saftycorrdoc_user_created,
saftycorrdoc_spr_id,
saftycorrdoc_record_id,
saftycorrdoc_fk_sfty_corr_rec_id,
saftycorrdoc_doc_template_type,
saftycorrdoc_doc_source,
saftycorrdoc_doc_size,
saftycorrdoc_doc_name,
saftycorrdoc_doc_id,
saftycorrdoc_date_modified,
saftycorrdoc_date_created,
saftycorrdoc_category_code,
saftycorrdoc_ari_rec_id)
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
integration_id ,saftycorr_who_received_call,
saftycorr_user_modified,
saftycorr_user_created,
saftycorr_to_fax_no,
saftycorr_to_email_id,
saftycorr_template_record_id,
saftycorr_template_name,
saftycorr_subject,
saftycorr_status,
saftycorr_spr_id,
saftycorr_sent_by,
saftycorr_review_reminder,
saftycorr_response_corres_notify,
saftycorr_reminder_sent_date,
saftycorr_reminder_next_date,
saftycorr_reminder_interval_unit,
saftycorr_reminder_interval,
saftycorr_reminder_flag,
saftycorr_reminder_count,
saftycorr_rem_turn_off,
saftycorr_rem_template_record_id,
saftycorr_rem_template_name,
saftycorr_rem_override,
saftycorr_record_id,
saftycorr_receipt_no,
saftycorr_read_correspondence,
saftycorr_ques_rec_id,
saftycorr_priority,
saftycorr_phone_no,
saftycorr_phone_cntry_code,
saftycorr_phone_area_code,
saftycorr_no_of_reminders,
saftycorr_message_id,
saftycorr_lrn_version,
saftycorr_lrn,
saftycorr_local_correspondence_view,
saftycorr_letter_word_editor_value,
saftycorr_letter_sent_to,
saftycorr_letter_send_date,
saftycorr_letter_received_from,
saftycorr_letter_creation_option,
saftycorr_insert_solution,
saftycorr_inq_rec_id,
saftycorr_include_icsrxml,
saftycorr_from_fax_no,
saftycorr_from_email_id,
saftycorr_followup_type,
saftycorr_fk_saf_corr_doc_rec_id,
saftycorr_fk_naer_ref_rec_id,
saftycorr_fk_ccm_comm_thread_rec_id,
saftycorr_fk_ccm_comm_rec_id,
saftycorr_fk_aim_ref_rec_id,
saftycorr_fax_status,
saftycorr_fax_sent_date,
saftycorr_fax_id,
saftycorr_expected_reply_date,
saftycorr_email_notification,
saftycorr_display_in_portal,
saftycorr_dispatch_status,
saftycorr_direction,
saftycorr_delivery_status,
saftycorr_date_transmitted,
saftycorr_date_modified,
saftycorr_date_created,
saftycorr_cover_page_template_record_id,
saftycorr_cover_page_template_name,
saftycorr_correspondence_seq,
saftycorr_correspondence_mode,
saftycorr_correspondence_date,
saftycorr_corr_flag,
saftycorr_comp_rec_id,
saftycorr_category,
saftycorr_carbon_copy,
saftycorr_call_status,
saftycorr_call_received_from,
saftycorr_call_duration,
saftycorr_auto_reply_fu_corres,
saftycorr_ari_rec_id,
saftycorr_all_facility_users,
saftycorr_ack_reply,
saftycorrq_user_modified,
saftycorrq_user_created,
saftycorrq_spr_id,
saftycorrq_record_id,
saftycorrq_medium_code,
saftycorrq_locale,
saftycorrq_inq_rec_id,
saftycorrq_fax_transmission_id,
saftycorrq_fax_status_desc,
saftycorrq_fax_status_code,
saftycorrq_fax_completion_date,
saftycorrq_dispatch_status,
saftycorrq_date_modified,
saftycorrq_date_created,
saftycorrq_correspondence_ref,
saftycorrq_comp_rec_id,
saftycorrq_ari_rec_id,
saftycorrdoc_user_modified,
saftycorrdoc_user_created,
saftycorrdoc_spr_id,
saftycorrdoc_record_id,
saftycorrdoc_fk_sfty_corr_rec_id,
saftycorrdoc_doc_template_type,
saftycorrdoc_doc_source,
saftycorrdoc_doc_size,
saftycorrdoc_doc_name,
saftycorrdoc_doc_id,
saftycorrdoc_date_modified,
saftycorrdoc_date_created,
saftycorrdoc_category_code,
saftycorrdoc_ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP 
WHERE 	TO_DATE(LS_DB_SAFETY_CORRESPONDENCE.PROCESSING_DT) < TO_DATE(LS_DB_SAFETY_CORRESPONDENCE_TMP.PROCESSING_DT)
AND LS_DB_SAFETY_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SAFETY_CORRESPONDENCE_TMP.INTEGRATION_ID
AND LS_DB_SAFETY_CORRESPONDENCE.saftycorr_RECORD_ID = LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_RECORD_ID
AND LS_DB_SAFETY_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE.saftycorrq_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_CORRESPONDENCE_TMP.saftycorrq_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE TGT
WHERE  ( saftycorrdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_doc') OR saftycorrq_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_queue') OR saftycorr_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_correspondence')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( saftycorrdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_doc') OR saftycorrq_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_corresp_queue') OR saftycorr_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SAFETY_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_safety_correspondence')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_CORRESPONDENCE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SAFETY_CORRESPONDENCE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_CORRESPONDENCE'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_SAFETY_CORRESPONDENCE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SAFETY_CORRESPONDENCE'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_SAFETY_CORRESPONDENCE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
