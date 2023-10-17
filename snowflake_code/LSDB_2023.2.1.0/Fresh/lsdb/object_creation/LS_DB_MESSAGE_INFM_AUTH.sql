
USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MESSAGE_INFM_AUTH'),
                'LSDB','Case','LS_DB_MESSAGE_INFM_AUTH',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_MESSAGE_INFM_AUTH' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_MESSAGE_INFM_AUTH' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  As select RECORD_ID,'lsmv_informed_authority' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_informed_authority WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_st_message' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1015','139','9601','9602','9602','9623','9625','9638','9643','9647','9652','9652','9939')
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')),
LSMV_CASE_NO_SUBSET as
(

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_MESSAGE_INFM_AUTH' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_MESSAGE_INFM_AUTH' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  (select record_id from (select  record_id, ROW_NUMBER() OVER (ORDER BY CDC_OPERATION_TIME DESC) RANK from ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM ri where RECEIPT_NO=ri.RECEIPT_NO) tmp where tmp.rank=1) ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_informed_authority WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_MESSAGE_INFM_AUTH' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_LSM_REC_ID record_id, 0 common_parent_key,  (select record_id from (select  record_id, ROW_NUMBER() OVER (ORDER BY CDC_OPERATION_TIME DESC) RANK from ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM ri where RECEIPT_NO=ri.RECEIPT_NO) tmp where tmp.rank=1) ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_informed_authority WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_MESSAGE_INFM_AUTH' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_informed_authority_SUBSET AS 
(
select * from 
    (SELECT  
    ack_received  infmauth_ack_received,acknowledgement_number  infmauth_acknowledgement_number,acknowledgement_received_date  infmauth_acknowledgement_received_date,aer_no  infmauth_aer_no,auth_no_comp_no_enable  infmauth_auth_no_comp_no_enable,authority_name  infmauth_authority_name,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9638' AND CD=CAST(authority_name AS VARCHAR(100)) )infmauth_authority_name_de_ml , authority_or_company_no  infmauth_authority_or_company_no,authority_partner_id  infmauth_authority_partner_id,authority_resp_date  infmauth_authority_resp_date,cancellation_flag  infmauth_cancellation_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9939' AND CD=CAST(cancellation_flag AS VARCHAR(100)) )infmauth_cancellation_flag_de_ml , case_sum_and_rep_comments  infmauth_case_sum_and_rep_comments,clinical_drug_code  infmauth_clinical_drug_code,clinical_trial_notificatn  infmauth_clinical_trial_notificatn,comments  infmauth_comments,company_remarks  infmauth_company_remarks,company_remarks_language  infmauth_company_remarks_language,complete_flag  infmauth_complete_flag,contact_type  infmauth_contact_type,date_created  infmauth_date_created,date_informed  infmauth_date_informed,date_informed_precision  infmauth_date_informed_precision,date_informed_to_distributor  infmauth_date_informed_to_distributor,date_modified  infmauth_date_modified,decision_date  infmauth_decision_date,dist_info_id  infmauth_dist_info_id,e2b_message_type  infmauth_e2b_message_type,event_description  infmauth_event_description,event_description_language  infmauth_event_description_language,expected_date_of_next_report  infmauth_expected_date_of_next_report,final_report  infmauth_final_report,fk_lsm_rec_id  infmauth_fk_lsm_rec_id,follow_up_type  infmauth_follow_up_type,followup_response_to_auth_req  infmauth_followup_response_to_auth_req,followup_to_device_eval  infmauth_followup_to_device_eval,followup_type_additional_info  infmauth_followup_type_additional_info,followup_type_correction  infmauth_followup_type_correction,future_approach  infmauth_future_approach,immediate_report_flag  infmauth_immediate_report_flag,jpn_counter_measure  infmauth_jpn_counter_measure,linked_report_id  infmauth_linked_report_id,local_criteria_report_type  infmauth_local_criteria_report_type,locally_expedited  infmauth_locally_expedited,locally_expedited_nf  infmauth_locally_expedited_nf,mdn_received  infmauth_mdn_received,mhlw_device_prev_rep_year  infmauth_mhlw_device_prev_rep_year,mhlw_device_prev_report_id  infmauth_mhlw_device_prev_report_id,mhlw_device_rep_type  infmauth_mhlw_device_rep_type,mhlw_device_rep_year  infmauth_mhlw_device_rep_year,mhlw_regenerative_report_type  infmauth_mhlw_regenerative_report_type,mhlw_report_type  infmauth_mhlw_report_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9652' AND CD=CAST(mhlw_report_type AS VARCHAR(100)) )infmauth_mhlw_report_type_de_ml , native_language  infmauth_native_language,new_drug_classification  infmauth_new_drug_classification,nullify_reason_details  infmauth_nullify_reason_details,other_comments  infmauth_other_comments,other_comments_jpn  infmauth_other_comments_jpn,other_ref_date  infmauth_other_ref_date,other_safety_ref_no  infmauth_other_safety_ref_no,other_safety_ref_ver  infmauth_other_safety_ref_ver,patient_under_treatment  infmauth_patient_under_treatment,pmda_report_id_no  infmauth_pmda_report_id_no,product_rank_order_json  infmauth_product_rank_order_json,reason_for_cancellation  infmauth_reason_for_cancellation,reason_for_incomplete  infmauth_reason_for_incomplete,reason_for_nullifn_or_amend  infmauth_reason_for_nullifn_or_amend,reason_notsubmitted  infmauth_reason_notsubmitted,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9625' AND CD=CAST(reason_notsubmitted AS VARCHAR(100)) )infmauth_reason_notsubmitted_de_ml , receipt_no  infmauth_receipt_no,record_id  infmauth_record_id,reference_no_received_date  infmauth_reference_no_received_date,reference_number  infmauth_reference_number,reg_clock_start_dt_comment  infmauth_reg_clock_start_dt_comment,rep_informed_auth_directly  infmauth_rep_informed_auth_directly,report_date  infmauth_report_date,report_follow_up_no  infmauth_report_follow_up_no,report_for_nullifn_or_amend  infmauth_report_for_nullifn_or_amend,report_format  infmauth_report_format,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9601' AND CD=CAST(report_format AS VARCHAR(100)) )infmauth_report_format_de_ml , report_medium  infmauth_report_medium,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9602' AND CD=CAST(report_medium AS VARCHAR(100)) )infmauth_report_medium_de_ml , report_sent_date_to_fda  infmauth_report_sent_date_to_fda,report_sent_date_to_mfr  infmauth_report_sent_date_to_mfr,report_withdrawal_reason  infmauth_report_withdrawal_reason,reporter_comments  infmauth_reporter_comments,reporter_comments_language  infmauth_reporter_comments_language,reporting_status  infmauth_reporting_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='139' AND CD=CAST(reporting_status AS VARCHAR(100)) )infmauth_reporting_status_de_ml , safety_report_id  infmauth_safety_report_id,safety_report_id_enable  infmauth_safety_report_id_enable,safety_report_version_no  infmauth_safety_report_version_no,source_of_ia  infmauth_source_of_ia,spr_id  infmauth_spr_id,study_phase  infmauth_study_phase,suspect_product  infmauth_suspect_product,suspect_product_rec_id  infmauth_suspect_product_rec_id,targeted_disease  infmauth_targeted_disease,type_of_ack_received  infmauth_type_of_ack_received,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9647' AND CD=CAST(type_of_ack_received AS VARCHAR(100)) )infmauth_type_of_ack_received_de_ml , type_of_report  infmauth_type_of_report,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9623' AND CD=CAST(type_of_report AS VARCHAR(100)) )infmauth_type_of_report_de_ml , uf_dist_dat_sent_to_fda_prec  infmauth_uf_dist_dat_sent_to_fda_prec,uf_dist_dat_sent_to_mfr_prec  infmauth_uf_dist_dat_sent_to_mfr_prec,uf_dist_report_sent_to_fda  infmauth_uf_dist_report_sent_to_fda,uf_dist_report_sent_to_mfr  infmauth_uf_dist_report_sent_to_mfr,user_created  infmauth_user_created,user_id  infmauth_user_id,user_modified  infmauth_user_modified,version_no  infmauth_version_no,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_informed_authority
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_LSM_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_informed_authority')
  ) where REC_RANK=1 )
  , lsmv_st_message_SUBSET AS 
(
select * from 
    (SELECT  
    ack_date  stmsg_ack_date,ack_received  stmsg_ack_received,ack_status  stmsg_ack_status,activity_arrival_date  stmsg_activity_arrival_date,activity_due_date  stmsg_activity_due_date,add_narratives_overflow  stmsg_add_narratives_overflow,aer_company_unit_name  stmsg_aer_company_unit_name,aer_company_unit_rec_id  stmsg_aer_company_unit_rec_id,aer_id  stmsg_aer_id,aer_no  stmsg_aer_no,aer_processing_unit_rec_id  stmsg_aer_processing_unit_rec_id,aer_receipt_date  stmsg_aer_receipt_date,affiliate_submission_due_date  stmsg_affiliate_submission_due_date,agx_aoip_acknowledgement_date  stmsg_agx_aoip_acknowledgement_date,agx_aoip_acknowledgement_num  stmsg_agx_aoip_acknowledgement_num,agx_aoip_authority  stmsg_agx_aoip_authority,agx_aoip_date_informed  stmsg_agx_aoip_date_informed,agx_aoip_followup_number  stmsg_agx_aoip_followup_number,agx_aoip_reason_notsubmitted  stmsg_agx_aoip_reason_notsubmitted,agx_aoip_reference_date  stmsg_agx_aoip_reference_date,agx_aoip_reference_number  stmsg_agx_aoip_reference_number,agx_aoip_reporting_status  stmsg_agx_aoip_reporting_status,aim_rec_id  stmsg_aim_rec_id,alert_sent  stmsg_alert_sent,app_date_distributed  stmsg_app_date_distributed,app_distributed_date  stmsg_app_distributed_date,app_distribution_status  stmsg_app_distribution_status,app_error_message  stmsg_app_error_message,app_lde_language  stmsg_app_lde_language,app_message_type  stmsg_app_message_type,app_nar_include_clinical  stmsg_app_nar_include_clinical,app_req_id  stmsg_app_req_id,app_sender_comments  stmsg_app_sender_comments,archive_process_date  stmsg_archive_process_date,archive_status  stmsg_archive_status,archived_date  stmsg_archived_date,ari_rec_id  stmsg_ari_rec_id,assign_to  stmsg_assign_to,assigned_to  stmsg_assigned_to,att_ack_code  stmsg_att_ack_code,att_icsr_doc_sent  stmsg_att_icsr_doc_sent,att_mdn_ack_received  stmsg_att_mdn_ack_received,authority_numb  stmsg_authority_numb,authority_partner_id  stmsg_authority_partner_id,auto_complete_first_activity  stmsg_auto_complete_first_activity,batch_doc_id  stmsg_batch_doc_id,batch_export  stmsg_batch_export,batch_id  stmsg_batch_id,batch_lde_approved  stmsg_batch_lde_approved,batch_queue_id  stmsg_batch_queue_id,blinded_report_type  stmsg_blinded_report_type,case_archived  stmsg_case_archived,case_blinded  stmsg_case_blinded,case_country  stmsg_case_country,case_due_date  stmsg_case_due_date,case_level_expectedness  stmsg_case_level_expectedness,case_nullified  stmsg_case_nullified,case_report_doc_id  stmsg_case_report_doc_id,case_significance  stmsg_case_significance,case_susar  stmsg_case_susar,case_type  stmsg_case_type,case_version  stmsg_case_version,causality_assessment  stmsg_causality_assessment,cc_email_address  stmsg_cc_email_address,child_wf_id  stmsg_child_wf_id,comments  stmsg_comments,company_causality  stmsg_company_causality,company_numb  stmsg_company_numb,company_unit  stmsg_company_unit,company_unit_name  stmsg_company_unit_name,consider_similar_product  stmsg_consider_similar_product,contact_type  stmsg_contact_type,copied_case_seq  stmsg_copied_case_seq,copied_submission  stmsg_copied_submission,core_labelling  stmsg_core_labelling,correspondence_flag  stmsg_correspondence_flag,correspondence_seq  stmsg_correspondence_seq,cover_letter_desc  stmsg_cover_letter_desc,try_to_number(cover_letter_doc_id,38)  stmsg_cover_letter_doc_id,cover_letter_doc_id  stmsg_cover_letter_doc_id_1 ,cover_letter_holder  stmsg_cover_letter_holder,cover_letter_name  stmsg_cover_letter_name,cover_letter_size  stmsg_cover_letter_size,cover_letter_template_rec_id  stmsg_cover_letter_template_rec_id,cover_letter_type  stmsg_cover_letter_type,created_year  stmsg_created_year,data_exclusion  stmsg_data_exclusion,data_privacy  stmsg_data_privacy,date_created  stmsg_date_created,date_modified  stmsg_date_modified,date_xmit  stmsg_date_xmit,death  stmsg_death,delivery_notification_status  stmsg_delivery_notification_status,dist_aer_info_id  stmsg_dist_aer_info_id,dist_format_rec_id  stmsg_dist_format_rec_id,dist_rule_anchor_rec_id  stmsg_dist_rule_anchor_rec_id,distribution_unit_name  stmsg_distribution_unit_name,do_not_validate_and_extract  stmsg_do_not_validate_and_extract,doctype_code  stmsg_doctype_code,doctype_name  stmsg_doctype_name,downgrade_status  stmsg_downgrade_status,duplicate_ack  stmsg_duplicate_ack,duplicate_numb  stmsg_duplicate_numb,e2b_dtd_version  stmsg_e2b_dtd_version,e2b_message_type  stmsg_e2b_message_type,e2b_msg_queue_status  stmsg_e2b_msg_queue_status,elastic_indexed  stmsg_elastic_indexed,email_address  stmsg_email_address,email_message  stmsg_email_message,email_retry_count  stmsg_email_retry_count,email_sent  stmsg_email_sent,email_subject  stmsg_email_subject,email_template_rec_id  stmsg_email_template_rec_id,error_message  stmsg_error_message,esm_communication_status  stmsg_esm_communication_status,eu_spc_labelling  stmsg_eu_spc_labelling,ext_sourced_case_no  stmsg_ext_sourced_case_no,external_app_cm_seq_id  stmsg_external_app_cm_seq_id,external_app_wfi_id  stmsg_external_app_wfi_id,failed_date  stmsg_failed_date,fax_batch_id  stmsg_fax_batch_id,fax_number  stmsg_fax_number,fax_template_rec_id  stmsg_fax_template_rec_id,fk_ap_rec_id  stmsg_fk_ap_rec_id,fk_aw_rec_id  stmsg_fk_aw_rec_id,fk_awa_rec_id  stmsg_fk_awa_rec_id,fk_ddc_contact_id  stmsg_fk_ddc_contact_id,fk_ldm_rec_id  stmsg_fk_ldm_rec_id,fk_lms_rec_id  stmsg_fk_lms_rec_id,fk_lp_receiver_rec_id  stmsg_fk_lp_receiver_rec_id,fk_lp_sender_rec_id  stmsg_fk_lp_sender_rec_id,fk_lsip_rec_id  stmsg_fk_lsip_rec_id,fl_data_privacy  stmsg_fl_data_privacy,fl_multi_reporting  stmsg_fl_multi_reporting,fl_overall_lateness  stmsg_fl_overall_lateness,fl_submit_trans  stmsg_fl_submit_trans,format_country  stmsg_format_country,format_type  stmsg_format_type,format_version  stmsg_format_version,hp_confirmed  stmsg_hp_confirmed,ib_labelling  stmsg_ib_labelling,icsr_rule_id  stmsg_icsr_rule_id,identification_no  stmsg_identification_no,include_r3_in_r2  stmsg_include_r3_in_r2,informing_unit  stmsg_informing_unit,informing_unit_name  stmsg_informing_unit_name,initial_aer_id  stmsg_initial_aer_id,is_due_dt_recalctd  stmsg_is_due_dt_recalctd,is_expedited  stmsg_is_expedited,language_code  stmsg_language_code,last_alert_notified_time  stmsg_last_alert_notified_time,last_transition  stmsg_last_transition,last_user_modified  stmsg_last_user_modified,late_by  stmsg_late_by,lateness_reason_code  stmsg_lateness_reason_code,lateness_reason_comment  stmsg_lateness_reason_comment,lateness_resp_unit_name  stmsg_lateness_resp_unit_name,latest_receipt_date  stmsg_latest_receipt_date,latest_receive_date_jpn  stmsg_latest_receive_date_jpn,lde_approved  stmsg_lde_approved,lde_language  stmsg_lde_language,lde_lock  stmsg_lde_lock,license_type  stmsg_license_type,life_threatening  stmsg_life_threatening,literature  stmsg_literature,local_version_no  stmsg_local_version_no,manual_ssd  stmsg_manual_ssd,manual_upload  stmsg_manual_upload,manual_upload_file_name  stmsg_manual_upload_file_name,mdn_date  stmsg_mdn_date,mdn_received  stmsg_mdn_received,medically_confirmed  stmsg_medically_confirmed,medium  stmsg_medium,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9602' AND CD=CAST(medium AS VARCHAR(100)) )stmsg_medium_de_ml , message_no  stmsg_message_no,message_state  stmsg_message_state,mfr_number  stmsg_mfr_number,mhlw_report_type  stmsg_mhlw_report_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9652' AND CD=CAST(mhlw_report_type AS VARCHAR(100)) )stmsg_mhlw_report_type_de_ml , narrative_include_clinical  stmsg_narrative_include_clinical,next_transition  stmsg_next_transition,next_transition_note  stmsg_next_transition_note,notes_flag  stmsg_notes_flag,oth_med_imp_condition  stmsg_oth_med_imp_condition,other_ref_date  stmsg_other_ref_date,other_safety_ref_no  stmsg_other_safety_ref_no,other_safety_ref_ver  stmsg_other_safety_ref_ver,outbound_ref1  stmsg_outbound_ref1,outbound_ref2  stmsg_outbound_ref2,outbound_ref3  stmsg_outbound_ref3,outbound_ref4  stmsg_outbound_ref4,overall_due_date  stmsg_overall_due_date,override_due_dt  stmsg_override_due_dt,override_reason  stmsg_override_reason,owner  stmsg_owner,parent_opm_rec_id  stmsg_parent_opm_rec_id,partner_id  stmsg_partner_id,partner_name  stmsg_partner_name,patient_id  stmsg_patient_id,patientinitial_nf  stmsg_patientinitial_nf,previous_submission_status  stmsg_previous_submission_status,primary_reporter  stmsg_primary_reporter,primary_source_country  stmsg_primary_source_country,priority  stmsg_priority,product_selected_level  stmsg_product_selected_level,products_available  stmsg_products_available,prolonged_hospitalization  stmsg_prolonged_hospitalization,protocol_no  stmsg_protocol_no,protocol_no_nf  stmsg_protocol_no_nf,qbe_comments  stmsg_qbe_comments,r3_xml_size  stmsg_r3_xml_size,reactions_available  stmsg_reactions_available,read_unread_correspondence  stmsg_read_unread_correspondence,receipent_receiver_type  stmsg_receipent_receiver_type,receipent_sender_type  stmsg_receipent_sender_type,receipt_no  stmsg_receipt_no,receiver_interchange_id  stmsg_receiver_interchange_id,receiver_unit  stmsg_receiver_unit,recepient_country  stmsg_recepient_country,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(recepient_country AS VARCHAR(100)) )stmsg_recepient_country_de_ml , recipient_receiver_rec_id  stmsg_recipient_receiver_rec_id,recipient_sender_rec_id  stmsg_recipient_sender_rec_id,record_id  stmsg_record_id,record_owner_unit  stmsg_record_owner_unit,record_owner_unit_name  stmsg_record_owner_unit_name,reg_clock_start_date_jpn  stmsg_reg_clock_start_date_jpn,regenerated_doc_id  stmsg_regenerated_doc_id,regional_state  stmsg_regional_state,regional_state_nf  stmsg_regional_state_nf,regulatory_report_id  stmsg_regulatory_report_id,remove_reason_code  stmsg_remove_reason_code,remove_reason_comment  stmsg_remove_reason_comment,report_ack  stmsg_report_ack,report_doc_type  stmsg_report_doc_type,report_name  stmsg_report_name,report_size  stmsg_report_size,report_type  stmsg_report_type,reporter_causality  stmsg_reporter_causality,reporter_comments  stmsg_reporter_comments,reporter_country  stmsg_reporter_country,reportercountry_nf  stmsg_reportercountry_nf,resubmit_flag  stmsg_resubmit_flag,retrieve_comment  stmsg_retrieve_comment,review_lit_doc  stmsg_review_lit_doc,safety_report_id  stmsg_safety_report_id,sender_comments  stmsg_sender_comments,sender_email_id  stmsg_sender_email_id,sender_interchange_id  stmsg_sender_interchange_id,sender_unit  stmsg_sender_unit,seq_infm_auth  stmsg_seq_infm_auth,seq_lateness_info  stmsg_seq_lateness_info,serious_reaction  stmsg_serious_reaction,seriousness  stmsg_seriousness,single_email_submission  stmsg_single_email_submission,source_of_submission  stmsg_source_of_submission,special_project_names  stmsg_special_project_names,spr_id  stmsg_spr_id,status  stmsg_status,study_design  stmsg_study_design,study_type  stmsg_study_type,sub_compliance  stmsg_sub_compliance,sub_due_date_jpn  stmsg_sub_due_date_jpn,submission_attempt_count  stmsg_submission_attempt_count,submission_condition_type  stmsg_submission_condition_type,submission_date  stmsg_submission_date,submission_due_date  stmsg_submission_due_date,submission_query_desc  stmsg_submission_query_desc,submission_query_name  stmsg_submission_query_name,submission_state  stmsg_submission_state,submission_status  stmsg_submission_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9643' AND CD=CAST(submission_status AS VARCHAR(100)) )stmsg_submission_status_de_ml , submission_unit  stmsg_submission_unit,submission_unit_name  stmsg_submission_unit_name,submit_notes  stmsg_submit_notes,submitted_by  stmsg_submitted_by,support_docs_merged  stmsg_support_docs_merged,suspect_product  stmsg_suspect_product,time_line_day  stmsg_time_line_day,timeframe  stmsg_timeframe,to_be_synch_cm  stmsg_to_be_synch_cm,to_be_synch_due_date  stmsg_to_be_synch_due_date,to_be_synch_sub_status  stmsg_to_be_synch_sub_status,to_be_synchronize  stmsg_to_be_synchronize,trade_name  stmsg_trade_name,user_created  stmsg_user_created,user_last_modified  stmsg_user_last_modified,user_modified  stmsg_user_modified,user_submitted_date  stmsg_user_submitted_date,uspi_labelling  stmsg_uspi_labelling,version_no  stmsg_version_no,wf_activity_name  stmsg_wf_activity_name,wf_completion_flag  stmsg_wf_completion_flag,wf_message_state  stmsg_wf_message_state,wf_status  stmsg_wf_status,xml_doc_config_name  stmsg_xml_doc_config_name,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_message
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_message')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_informed_authority_SUBSET.infmauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_st_message_SUBSET.stmsg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO            ,lsmv_st_message_SUBSET.stmsg_USER_MODIFIED USER_MODIFIED,lsmv_st_message_SUBSET.stmsg_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE     ,lsmv_st_message_SUBSET.stmsg_USER_CREATED CREATED_BY,lsmv_st_message_SUBSET.stmsg_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_message_SUBSET.stmsg_xml_doc_config_name  ,lsmv_st_message_SUBSET.stmsg_wf_status  ,lsmv_st_message_SUBSET.stmsg_wf_message_state  ,lsmv_st_message_SUBSET.stmsg_wf_completion_flag  ,lsmv_st_message_SUBSET.stmsg_wf_activity_name  ,lsmv_st_message_SUBSET.stmsg_version_no  ,lsmv_st_message_SUBSET.stmsg_uspi_labelling  ,lsmv_st_message_SUBSET.stmsg_user_submitted_date  ,lsmv_st_message_SUBSET.stmsg_user_modified  ,lsmv_st_message_SUBSET.stmsg_user_last_modified  ,lsmv_st_message_SUBSET.stmsg_user_created  ,lsmv_st_message_SUBSET.stmsg_trade_name  ,lsmv_st_message_SUBSET.stmsg_to_be_synchronize  ,lsmv_st_message_SUBSET.stmsg_to_be_synch_sub_status  ,lsmv_st_message_SUBSET.stmsg_to_be_synch_due_date  ,lsmv_st_message_SUBSET.stmsg_to_be_synch_cm  ,lsmv_st_message_SUBSET.stmsg_timeframe  ,lsmv_st_message_SUBSET.stmsg_time_line_day  ,lsmv_st_message_SUBSET.stmsg_suspect_product  ,lsmv_st_message_SUBSET.stmsg_support_docs_merged  ,lsmv_st_message_SUBSET.stmsg_submitted_by  ,lsmv_st_message_SUBSET.stmsg_submit_notes  ,lsmv_st_message_SUBSET.stmsg_submission_unit_name  ,lsmv_st_message_SUBSET.stmsg_submission_unit  ,lsmv_st_message_SUBSET.stmsg_submission_status_de_ml  ,lsmv_st_message_SUBSET.stmsg_submission_status  ,lsmv_st_message_SUBSET.stmsg_submission_state  ,lsmv_st_message_SUBSET.stmsg_submission_query_name  ,lsmv_st_message_SUBSET.stmsg_submission_query_desc  ,lsmv_st_message_SUBSET.stmsg_submission_due_date  ,lsmv_st_message_SUBSET.stmsg_submission_date  ,lsmv_st_message_SUBSET.stmsg_submission_condition_type  ,lsmv_st_message_SUBSET.stmsg_submission_attempt_count  ,lsmv_st_message_SUBSET.stmsg_sub_due_date_jpn  ,lsmv_st_message_SUBSET.stmsg_sub_compliance  ,lsmv_st_message_SUBSET.stmsg_study_type  ,lsmv_st_message_SUBSET.stmsg_study_design  ,lsmv_st_message_SUBSET.stmsg_status  ,lsmv_st_message_SUBSET.stmsg_spr_id  ,lsmv_st_message_SUBSET.stmsg_special_project_names  ,lsmv_st_message_SUBSET.stmsg_source_of_submission  ,lsmv_st_message_SUBSET.stmsg_single_email_submission  ,lsmv_st_message_SUBSET.stmsg_seriousness  ,lsmv_st_message_SUBSET.stmsg_serious_reaction  ,lsmv_st_message_SUBSET.stmsg_seq_lateness_info  ,lsmv_st_message_SUBSET.stmsg_seq_infm_auth  ,lsmv_st_message_SUBSET.stmsg_sender_unit  ,lsmv_st_message_SUBSET.stmsg_sender_interchange_id  ,lsmv_st_message_SUBSET.stmsg_sender_email_id  ,lsmv_st_message_SUBSET.stmsg_sender_comments  ,lsmv_st_message_SUBSET.stmsg_safety_report_id  ,lsmv_st_message_SUBSET.stmsg_review_lit_doc  ,lsmv_st_message_SUBSET.stmsg_retrieve_comment  ,lsmv_st_message_SUBSET.stmsg_resubmit_flag  ,lsmv_st_message_SUBSET.stmsg_reportercountry_nf  ,lsmv_st_message_SUBSET.stmsg_reporter_country  ,lsmv_st_message_SUBSET.stmsg_reporter_comments  ,lsmv_st_message_SUBSET.stmsg_reporter_causality  ,lsmv_st_message_SUBSET.stmsg_report_type  ,lsmv_st_message_SUBSET.stmsg_report_size  ,lsmv_st_message_SUBSET.stmsg_report_name  ,lsmv_st_message_SUBSET.stmsg_report_doc_type  ,lsmv_st_message_SUBSET.stmsg_report_ack  ,lsmv_st_message_SUBSET.stmsg_remove_reason_comment  ,lsmv_st_message_SUBSET.stmsg_remove_reason_code  ,lsmv_st_message_SUBSET.stmsg_regulatory_report_id  ,lsmv_st_message_SUBSET.stmsg_regional_state_nf  ,lsmv_st_message_SUBSET.stmsg_regional_state  ,lsmv_st_message_SUBSET.stmsg_regenerated_doc_id  ,lsmv_st_message_SUBSET.stmsg_reg_clock_start_date_jpn  ,lsmv_st_message_SUBSET.stmsg_record_owner_unit_name  ,lsmv_st_message_SUBSET.stmsg_record_owner_unit  ,lsmv_st_message_SUBSET.stmsg_record_id  ,lsmv_st_message_SUBSET.stmsg_recipient_sender_rec_id  ,lsmv_st_message_SUBSET.stmsg_recipient_receiver_rec_id  ,lsmv_st_message_SUBSET.stmsg_recepient_country_de_ml  ,lsmv_st_message_SUBSET.stmsg_recepient_country  ,lsmv_st_message_SUBSET.stmsg_receiver_unit  ,lsmv_st_message_SUBSET.stmsg_receiver_interchange_id  ,lsmv_st_message_SUBSET.stmsg_receipt_no  ,lsmv_st_message_SUBSET.stmsg_receipent_sender_type  ,lsmv_st_message_SUBSET.stmsg_receipent_receiver_type  ,lsmv_st_message_SUBSET.stmsg_read_unread_correspondence  ,lsmv_st_message_SUBSET.stmsg_reactions_available  ,lsmv_st_message_SUBSET.stmsg_r3_xml_size  ,lsmv_st_message_SUBSET.stmsg_qbe_comments  ,lsmv_st_message_SUBSET.stmsg_protocol_no_nf  ,lsmv_st_message_SUBSET.stmsg_protocol_no  ,lsmv_st_message_SUBSET.stmsg_prolonged_hospitalization  ,lsmv_st_message_SUBSET.stmsg_products_available  ,lsmv_st_message_SUBSET.stmsg_product_selected_level  ,lsmv_st_message_SUBSET.stmsg_priority  ,lsmv_st_message_SUBSET.stmsg_primary_source_country  ,lsmv_st_message_SUBSET.stmsg_primary_reporter  ,lsmv_st_message_SUBSET.stmsg_previous_submission_status  ,lsmv_st_message_SUBSET.stmsg_patientinitial_nf  ,lsmv_st_message_SUBSET.stmsg_patient_id  ,lsmv_st_message_SUBSET.stmsg_partner_name  ,lsmv_st_message_SUBSET.stmsg_partner_id  ,lsmv_st_message_SUBSET.stmsg_parent_opm_rec_id  ,lsmv_st_message_SUBSET.stmsg_owner  ,lsmv_st_message_SUBSET.stmsg_override_reason  ,lsmv_st_message_SUBSET.stmsg_override_due_dt  ,lsmv_st_message_SUBSET.stmsg_overall_due_date  ,lsmv_st_message_SUBSET.stmsg_outbound_ref4  ,lsmv_st_message_SUBSET.stmsg_outbound_ref3  ,lsmv_st_message_SUBSET.stmsg_outbound_ref2  ,lsmv_st_message_SUBSET.stmsg_outbound_ref1  ,lsmv_st_message_SUBSET.stmsg_other_safety_ref_ver  ,lsmv_st_message_SUBSET.stmsg_other_safety_ref_no  ,lsmv_st_message_SUBSET.stmsg_other_ref_date  ,lsmv_st_message_SUBSET.stmsg_oth_med_imp_condition  ,lsmv_st_message_SUBSET.stmsg_notes_flag  ,lsmv_st_message_SUBSET.stmsg_next_transition_note  ,lsmv_st_message_SUBSET.stmsg_next_transition  ,lsmv_st_message_SUBSET.stmsg_narrative_include_clinical  ,lsmv_st_message_SUBSET.stmsg_mhlw_report_type_de_ml  ,lsmv_st_message_SUBSET.stmsg_mhlw_report_type  ,lsmv_st_message_SUBSET.stmsg_mfr_number  ,lsmv_st_message_SUBSET.stmsg_message_state  ,lsmv_st_message_SUBSET.stmsg_message_no  ,lsmv_st_message_SUBSET.stmsg_medium_de_ml  ,lsmv_st_message_SUBSET.stmsg_medium  ,lsmv_st_message_SUBSET.stmsg_medically_confirmed  ,lsmv_st_message_SUBSET.stmsg_mdn_received  ,lsmv_st_message_SUBSET.stmsg_mdn_date  ,lsmv_st_message_SUBSET.stmsg_manual_upload_file_name  ,lsmv_st_message_SUBSET.stmsg_manual_upload  ,lsmv_st_message_SUBSET.stmsg_manual_ssd  ,lsmv_st_message_SUBSET.stmsg_local_version_no  ,lsmv_st_message_SUBSET.stmsg_literature  ,lsmv_st_message_SUBSET.stmsg_life_threatening  ,lsmv_st_message_SUBSET.stmsg_license_type  ,lsmv_st_message_SUBSET.stmsg_lde_lock  ,lsmv_st_message_SUBSET.stmsg_lde_language  ,lsmv_st_message_SUBSET.stmsg_lde_approved  ,lsmv_st_message_SUBSET.stmsg_latest_receive_date_jpn  ,lsmv_st_message_SUBSET.stmsg_latest_receipt_date  ,lsmv_st_message_SUBSET.stmsg_lateness_resp_unit_name  ,lsmv_st_message_SUBSET.stmsg_lateness_reason_comment  ,lsmv_st_message_SUBSET.stmsg_lateness_reason_code  ,lsmv_st_message_SUBSET.stmsg_late_by  ,lsmv_st_message_SUBSET.stmsg_last_user_modified  ,lsmv_st_message_SUBSET.stmsg_last_transition  ,lsmv_st_message_SUBSET.stmsg_last_alert_notified_time  ,lsmv_st_message_SUBSET.stmsg_language_code  ,lsmv_st_message_SUBSET.stmsg_is_expedited  ,lsmv_st_message_SUBSET.stmsg_is_due_dt_recalctd  ,lsmv_st_message_SUBSET.stmsg_initial_aer_id  ,lsmv_st_message_SUBSET.stmsg_informing_unit_name  ,lsmv_st_message_SUBSET.stmsg_informing_unit  ,lsmv_st_message_SUBSET.stmsg_include_r3_in_r2  ,lsmv_st_message_SUBSET.stmsg_identification_no  ,lsmv_st_message_SUBSET.stmsg_icsr_rule_id  ,lsmv_st_message_SUBSET.stmsg_ib_labelling  ,lsmv_st_message_SUBSET.stmsg_hp_confirmed  ,lsmv_st_message_SUBSET.stmsg_format_version  ,lsmv_st_message_SUBSET.stmsg_format_type  ,lsmv_st_message_SUBSET.stmsg_format_country  ,lsmv_st_message_SUBSET.stmsg_fl_submit_trans  ,lsmv_st_message_SUBSET.stmsg_fl_overall_lateness  ,lsmv_st_message_SUBSET.stmsg_fl_multi_reporting  ,lsmv_st_message_SUBSET.stmsg_fl_data_privacy  ,lsmv_st_message_SUBSET.stmsg_fk_lsip_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_lp_sender_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_lp_receiver_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_lms_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_ldm_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_ddc_contact_id  ,lsmv_st_message_SUBSET.stmsg_fk_awa_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_aw_rec_id  ,lsmv_st_message_SUBSET.stmsg_fk_ap_rec_id  ,lsmv_st_message_SUBSET.stmsg_fax_template_rec_id  ,lsmv_st_message_SUBSET.stmsg_fax_number  ,lsmv_st_message_SUBSET.stmsg_fax_batch_id  ,lsmv_st_message_SUBSET.stmsg_failed_date  ,lsmv_st_message_SUBSET.stmsg_external_app_wfi_id  ,lsmv_st_message_SUBSET.stmsg_external_app_cm_seq_id  ,lsmv_st_message_SUBSET.stmsg_ext_sourced_case_no  ,lsmv_st_message_SUBSET.stmsg_eu_spc_labelling  ,lsmv_st_message_SUBSET.stmsg_esm_communication_status  ,lsmv_st_message_SUBSET.stmsg_error_message  ,lsmv_st_message_SUBSET.stmsg_email_template_rec_id  ,lsmv_st_message_SUBSET.stmsg_email_subject  ,lsmv_st_message_SUBSET.stmsg_email_sent  ,lsmv_st_message_SUBSET.stmsg_email_retry_count  ,lsmv_st_message_SUBSET.stmsg_email_message  ,lsmv_st_message_SUBSET.stmsg_email_address  ,lsmv_st_message_SUBSET.stmsg_elastic_indexed  ,lsmv_st_message_SUBSET.stmsg_e2b_msg_queue_status  ,lsmv_st_message_SUBSET.stmsg_e2b_message_type  ,lsmv_st_message_SUBSET.stmsg_e2b_dtd_version  ,lsmv_st_message_SUBSET.stmsg_duplicate_numb  ,lsmv_st_message_SUBSET.stmsg_duplicate_ack  ,lsmv_st_message_SUBSET.stmsg_downgrade_status  ,lsmv_st_message_SUBSET.stmsg_doctype_name  ,lsmv_st_message_SUBSET.stmsg_doctype_code  ,lsmv_st_message_SUBSET.stmsg_do_not_validate_and_extract  ,lsmv_st_message_SUBSET.stmsg_distribution_unit_name  ,lsmv_st_message_SUBSET.stmsg_dist_rule_anchor_rec_id  ,lsmv_st_message_SUBSET.stmsg_dist_format_rec_id  ,lsmv_st_message_SUBSET.stmsg_dist_aer_info_id  ,lsmv_st_message_SUBSET.stmsg_delivery_notification_status  ,lsmv_st_message_SUBSET.stmsg_death  ,lsmv_st_message_SUBSET.stmsg_date_xmit  ,lsmv_st_message_SUBSET.stmsg_date_modified  ,lsmv_st_message_SUBSET.stmsg_date_created  ,lsmv_st_message_SUBSET.stmsg_data_privacy  ,lsmv_st_message_SUBSET.stmsg_data_exclusion  ,lsmv_st_message_SUBSET.stmsg_created_year  ,lsmv_st_message_SUBSET.stmsg_cover_letter_type  ,lsmv_st_message_SUBSET.stmsg_cover_letter_template_rec_id  ,lsmv_st_message_SUBSET.stmsg_cover_letter_size  ,lsmv_st_message_SUBSET.stmsg_cover_letter_name  ,lsmv_st_message_SUBSET.stmsg_cover_letter_holder  ,lsmv_st_message_SUBSET.stmsg_cover_letter_doc_id ,lsmv_st_message_SUBSET.stmsg_cover_letter_doc_id_1  ,lsmv_st_message_SUBSET.stmsg_cover_letter_desc  ,lsmv_st_message_SUBSET.stmsg_correspondence_seq  ,lsmv_st_message_SUBSET.stmsg_correspondence_flag  ,lsmv_st_message_SUBSET.stmsg_core_labelling  ,lsmv_st_message_SUBSET.stmsg_copied_submission  ,lsmv_st_message_SUBSET.stmsg_copied_case_seq  ,lsmv_st_message_SUBSET.stmsg_contact_type  ,lsmv_st_message_SUBSET.stmsg_consider_similar_product  ,lsmv_st_message_SUBSET.stmsg_company_unit_name  ,lsmv_st_message_SUBSET.stmsg_company_unit  ,lsmv_st_message_SUBSET.stmsg_company_numb  ,lsmv_st_message_SUBSET.stmsg_company_causality  ,lsmv_st_message_SUBSET.stmsg_comments  ,lsmv_st_message_SUBSET.stmsg_child_wf_id  ,lsmv_st_message_SUBSET.stmsg_cc_email_address  ,lsmv_st_message_SUBSET.stmsg_causality_assessment  ,lsmv_st_message_SUBSET.stmsg_case_version  ,lsmv_st_message_SUBSET.stmsg_case_type  ,lsmv_st_message_SUBSET.stmsg_case_susar  ,lsmv_st_message_SUBSET.stmsg_case_significance  ,lsmv_st_message_SUBSET.stmsg_case_report_doc_id  ,lsmv_st_message_SUBSET.stmsg_case_nullified  ,lsmv_st_message_SUBSET.stmsg_case_level_expectedness  ,lsmv_st_message_SUBSET.stmsg_case_due_date  ,lsmv_st_message_SUBSET.stmsg_case_country  ,lsmv_st_message_SUBSET.stmsg_case_blinded  ,lsmv_st_message_SUBSET.stmsg_case_archived  ,lsmv_st_message_SUBSET.stmsg_blinded_report_type  ,lsmv_st_message_SUBSET.stmsg_batch_queue_id  ,lsmv_st_message_SUBSET.stmsg_batch_lde_approved  ,lsmv_st_message_SUBSET.stmsg_batch_id  ,lsmv_st_message_SUBSET.stmsg_batch_export  ,lsmv_st_message_SUBSET.stmsg_batch_doc_id  ,lsmv_st_message_SUBSET.stmsg_auto_complete_first_activity  ,lsmv_st_message_SUBSET.stmsg_authority_partner_id  ,lsmv_st_message_SUBSET.stmsg_authority_numb  ,lsmv_st_message_SUBSET.stmsg_att_mdn_ack_received  ,lsmv_st_message_SUBSET.stmsg_att_icsr_doc_sent  ,lsmv_st_message_SUBSET.stmsg_att_ack_code  ,lsmv_st_message_SUBSET.stmsg_assigned_to  ,lsmv_st_message_SUBSET.stmsg_assign_to  ,lsmv_st_message_SUBSET.stmsg_ari_rec_id  ,lsmv_st_message_SUBSET.stmsg_archived_date  ,lsmv_st_message_SUBSET.stmsg_archive_status  ,lsmv_st_message_SUBSET.stmsg_archive_process_date  ,lsmv_st_message_SUBSET.stmsg_app_sender_comments  ,lsmv_st_message_SUBSET.stmsg_app_req_id  ,lsmv_st_message_SUBSET.stmsg_app_nar_include_clinical  ,lsmv_st_message_SUBSET.stmsg_app_message_type  ,lsmv_st_message_SUBSET.stmsg_app_lde_language  ,lsmv_st_message_SUBSET.stmsg_app_error_message  ,lsmv_st_message_SUBSET.stmsg_app_distribution_status  ,lsmv_st_message_SUBSET.stmsg_app_distributed_date  ,lsmv_st_message_SUBSET.stmsg_app_date_distributed  ,lsmv_st_message_SUBSET.stmsg_alert_sent  ,lsmv_st_message_SUBSET.stmsg_aim_rec_id  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_reporting_status  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_reference_number  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_reference_date  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_reason_notsubmitted  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_followup_number  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_date_informed  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_authority  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_acknowledgement_num  ,lsmv_st_message_SUBSET.stmsg_agx_aoip_acknowledgement_date  ,lsmv_st_message_SUBSET.stmsg_affiliate_submission_due_date  ,lsmv_st_message_SUBSET.stmsg_aer_receipt_date  ,lsmv_st_message_SUBSET.stmsg_aer_processing_unit_rec_id  ,lsmv_st_message_SUBSET.stmsg_aer_no  ,lsmv_st_message_SUBSET.stmsg_aer_id  ,lsmv_st_message_SUBSET.stmsg_aer_company_unit_rec_id  ,lsmv_st_message_SUBSET.stmsg_aer_company_unit_name  ,lsmv_st_message_SUBSET.stmsg_add_narratives_overflow  ,lsmv_st_message_SUBSET.stmsg_activity_due_date  ,lsmv_st_message_SUBSET.stmsg_activity_arrival_date  ,lsmv_st_message_SUBSET.stmsg_ack_status  ,lsmv_st_message_SUBSET.stmsg_ack_received  ,lsmv_st_message_SUBSET.stmsg_ack_date  ,lsmv_informed_authority_SUBSET.infmauth_version_no  ,lsmv_informed_authority_SUBSET.infmauth_user_modified  ,lsmv_informed_authority_SUBSET.infmauth_user_id  ,lsmv_informed_authority_SUBSET.infmauth_user_created  ,lsmv_informed_authority_SUBSET.infmauth_uf_dist_report_sent_to_mfr  ,lsmv_informed_authority_SUBSET.infmauth_uf_dist_report_sent_to_fda  ,lsmv_informed_authority_SUBSET.infmauth_uf_dist_dat_sent_to_mfr_prec  ,lsmv_informed_authority_SUBSET.infmauth_uf_dist_dat_sent_to_fda_prec  ,lsmv_informed_authority_SUBSET.infmauth_type_of_report_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_type_of_report  ,lsmv_informed_authority_SUBSET.infmauth_type_of_ack_received_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_type_of_ack_received  ,lsmv_informed_authority_SUBSET.infmauth_targeted_disease  ,lsmv_informed_authority_SUBSET.infmauth_suspect_product_rec_id  ,lsmv_informed_authority_SUBSET.infmauth_suspect_product  ,lsmv_informed_authority_SUBSET.infmauth_study_phase  ,lsmv_informed_authority_SUBSET.infmauth_spr_id  ,lsmv_informed_authority_SUBSET.infmauth_source_of_ia  ,lsmv_informed_authority_SUBSET.infmauth_safety_report_version_no  ,lsmv_informed_authority_SUBSET.infmauth_safety_report_id_enable  ,lsmv_informed_authority_SUBSET.infmauth_safety_report_id  ,lsmv_informed_authority_SUBSET.infmauth_reporting_status_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_reporting_status  ,lsmv_informed_authority_SUBSET.infmauth_reporter_comments_language  ,lsmv_informed_authority_SUBSET.infmauth_reporter_comments  ,lsmv_informed_authority_SUBSET.infmauth_report_withdrawal_reason  ,lsmv_informed_authority_SUBSET.infmauth_report_sent_date_to_mfr  ,lsmv_informed_authority_SUBSET.infmauth_report_sent_date_to_fda  ,lsmv_informed_authority_SUBSET.infmauth_report_medium_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_report_medium  ,lsmv_informed_authority_SUBSET.infmauth_report_format_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_report_format  ,lsmv_informed_authority_SUBSET.infmauth_report_for_nullifn_or_amend  ,lsmv_informed_authority_SUBSET.infmauth_report_follow_up_no  ,lsmv_informed_authority_SUBSET.infmauth_report_date  ,lsmv_informed_authority_SUBSET.infmauth_rep_informed_auth_directly  ,lsmv_informed_authority_SUBSET.infmauth_reg_clock_start_dt_comment  ,lsmv_informed_authority_SUBSET.infmauth_reference_number  ,lsmv_informed_authority_SUBSET.infmauth_reference_no_received_date  ,lsmv_informed_authority_SUBSET.infmauth_record_id  ,lsmv_informed_authority_SUBSET.infmauth_receipt_no  ,lsmv_informed_authority_SUBSET.infmauth_reason_notsubmitted_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_reason_notsubmitted  ,lsmv_informed_authority_SUBSET.infmauth_reason_for_nullifn_or_amend  ,lsmv_informed_authority_SUBSET.infmauth_reason_for_incomplete  ,lsmv_informed_authority_SUBSET.infmauth_reason_for_cancellation  ,lsmv_informed_authority_SUBSET.infmauth_product_rank_order_json  ,lsmv_informed_authority_SUBSET.infmauth_pmda_report_id_no  ,lsmv_informed_authority_SUBSET.infmauth_patient_under_treatment  ,lsmv_informed_authority_SUBSET.infmauth_other_safety_ref_ver  ,lsmv_informed_authority_SUBSET.infmauth_other_safety_ref_no  ,lsmv_informed_authority_SUBSET.infmauth_other_ref_date  ,lsmv_informed_authority_SUBSET.infmauth_other_comments_jpn  ,lsmv_informed_authority_SUBSET.infmauth_other_comments  ,lsmv_informed_authority_SUBSET.infmauth_nullify_reason_details  ,lsmv_informed_authority_SUBSET.infmauth_new_drug_classification  ,lsmv_informed_authority_SUBSET.infmauth_native_language  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_report_type_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_report_type  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_regenerative_report_type  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_device_rep_year  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_device_rep_type  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_device_prev_report_id  ,lsmv_informed_authority_SUBSET.infmauth_mhlw_device_prev_rep_year  ,lsmv_informed_authority_SUBSET.infmauth_mdn_received  ,lsmv_informed_authority_SUBSET.infmauth_locally_expedited_nf  ,lsmv_informed_authority_SUBSET.infmauth_locally_expedited  ,lsmv_informed_authority_SUBSET.infmauth_local_criteria_report_type  ,lsmv_informed_authority_SUBSET.infmauth_linked_report_id  ,lsmv_informed_authority_SUBSET.infmauth_jpn_counter_measure  ,lsmv_informed_authority_SUBSET.infmauth_immediate_report_flag  ,lsmv_informed_authority_SUBSET.infmauth_future_approach  ,lsmv_informed_authority_SUBSET.infmauth_followup_type_correction  ,lsmv_informed_authority_SUBSET.infmauth_followup_type_additional_info  ,lsmv_informed_authority_SUBSET.infmauth_followup_to_device_eval  ,lsmv_informed_authority_SUBSET.infmauth_followup_response_to_auth_req  ,lsmv_informed_authority_SUBSET.infmauth_follow_up_type  ,lsmv_informed_authority_SUBSET.infmauth_fk_lsm_rec_id  ,lsmv_informed_authority_SUBSET.infmauth_final_report  ,lsmv_informed_authority_SUBSET.infmauth_expected_date_of_next_report  ,lsmv_informed_authority_SUBSET.infmauth_event_description_language  ,lsmv_informed_authority_SUBSET.infmauth_event_description  ,lsmv_informed_authority_SUBSET.infmauth_e2b_message_type  ,lsmv_informed_authority_SUBSET.infmauth_dist_info_id  ,lsmv_informed_authority_SUBSET.infmauth_decision_date  ,lsmv_informed_authority_SUBSET.infmauth_date_modified  ,lsmv_informed_authority_SUBSET.infmauth_date_informed_to_distributor  ,lsmv_informed_authority_SUBSET.infmauth_date_informed_precision  ,lsmv_informed_authority_SUBSET.infmauth_date_informed  ,lsmv_informed_authority_SUBSET.infmauth_date_created  ,lsmv_informed_authority_SUBSET.infmauth_contact_type  ,lsmv_informed_authority_SUBSET.infmauth_complete_flag  ,lsmv_informed_authority_SUBSET.infmauth_company_remarks_language  ,lsmv_informed_authority_SUBSET.infmauth_company_remarks  ,lsmv_informed_authority_SUBSET.infmauth_comments  ,lsmv_informed_authority_SUBSET.infmauth_clinical_trial_notificatn  ,lsmv_informed_authority_SUBSET.infmauth_clinical_drug_code  ,lsmv_informed_authority_SUBSET.infmauth_case_sum_and_rep_comments  ,lsmv_informed_authority_SUBSET.infmauth_cancellation_flag_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_cancellation_flag  ,lsmv_informed_authority_SUBSET.infmauth_authority_resp_date  ,lsmv_informed_authority_SUBSET.infmauth_authority_partner_id  ,lsmv_informed_authority_SUBSET.infmauth_authority_or_company_no  ,lsmv_informed_authority_SUBSET.infmauth_authority_name_de_ml  ,lsmv_informed_authority_SUBSET.infmauth_authority_name  ,lsmv_informed_authority_SUBSET.infmauth_auth_no_comp_no_enable  ,lsmv_informed_authority_SUBSET.infmauth_aer_no  ,lsmv_informed_authority_SUBSET.infmauth_acknowledgement_received_date  ,lsmv_informed_authority_SUBSET.infmauth_acknowledgement_number  ,lsmv_informed_authority_SUBSET.infmauth_ack_received ,CONCAT( NVL(lsmv_informed_authority_SUBSET.infmauth_RECORD_ID,-1),'||',NVL(lsmv_st_message_SUBSET.stmsg_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_message_SUBSET  LEFT JOIN lsmv_informed_authority_SUBSET ON lsmv_st_message_SUBSET.stmsg_record_id=lsmv_informed_authority_SUBSET.infmauth_FK_LSM_REC_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_informed_authority_SUBSET.infmauth_RECORD_ID,lsmv_st_message_SUBSET.stmsg_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP)
where target_table_name='LS_DB_MESSAGE_INFM_AUTH'

; 


        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_st_message' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'cover_letter_doc_id' ,
stmsg_cover_letter_doc_id_1,
stmsg_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on cover_letter_doc_id'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP
WHERE (stmsg_cover_letter_doc_id is null and stmsg_cover_letter_doc_id_1 is not null)
--and stmsg_ARI_REC_ID is not null 
--and CASE_NO is not null
;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP
WHERE (stmsg_cover_letter_doc_id is null and stmsg_cover_letter_doc_id_1 is not null) 
--and CASE_NO is not null
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH   
SET LS_DB_MESSAGE_INFM_AUTH.stmsg_xml_doc_config_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_xml_doc_config_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_wf_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_wf_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_wf_message_state = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_wf_message_state,LS_DB_MESSAGE_INFM_AUTH.stmsg_wf_completion_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_wf_completion_flag,LS_DB_MESSAGE_INFM_AUTH.stmsg_wf_activity_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_wf_activity_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_version_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_version_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_uspi_labelling = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_uspi_labelling,LS_DB_MESSAGE_INFM_AUTH.stmsg_user_submitted_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_user_submitted_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_user_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_user_modified,LS_DB_MESSAGE_INFM_AUTH.stmsg_user_last_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_user_last_modified,LS_DB_MESSAGE_INFM_AUTH.stmsg_user_created = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_user_created,LS_DB_MESSAGE_INFM_AUTH.stmsg_trade_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_trade_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_to_be_synchronize = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_to_be_synchronize,LS_DB_MESSAGE_INFM_AUTH.stmsg_to_be_synch_sub_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_to_be_synch_sub_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_to_be_synch_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_to_be_synch_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_to_be_synch_cm = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_to_be_synch_cm,LS_DB_MESSAGE_INFM_AUTH.stmsg_timeframe = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_timeframe,LS_DB_MESSAGE_INFM_AUTH.stmsg_time_line_day = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_time_line_day,LS_DB_MESSAGE_INFM_AUTH.stmsg_suspect_product = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_suspect_product,LS_DB_MESSAGE_INFM_AUTH.stmsg_support_docs_merged = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_support_docs_merged,LS_DB_MESSAGE_INFM_AUTH.stmsg_submitted_by = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submitted_by,LS_DB_MESSAGE_INFM_AUTH.stmsg_submit_notes = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submit_notes,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_status_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_status_de_ml,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_state = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_state,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_query_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_query_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_query_desc = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_query_desc,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_condition_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_condition_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_submission_attempt_count = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_submission_attempt_count,LS_DB_MESSAGE_INFM_AUTH.stmsg_sub_due_date_jpn = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sub_due_date_jpn,LS_DB_MESSAGE_INFM_AUTH.stmsg_sub_compliance = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sub_compliance,LS_DB_MESSAGE_INFM_AUTH.stmsg_study_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_study_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_study_design = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_study_design,LS_DB_MESSAGE_INFM_AUTH.stmsg_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_spr_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_spr_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_special_project_names = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_special_project_names,LS_DB_MESSAGE_INFM_AUTH.stmsg_source_of_submission = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_source_of_submission,LS_DB_MESSAGE_INFM_AUTH.stmsg_single_email_submission = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_single_email_submission,LS_DB_MESSAGE_INFM_AUTH.stmsg_seriousness = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_seriousness,LS_DB_MESSAGE_INFM_AUTH.stmsg_serious_reaction = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_serious_reaction,LS_DB_MESSAGE_INFM_AUTH.stmsg_seq_lateness_info = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_seq_lateness_info,LS_DB_MESSAGE_INFM_AUTH.stmsg_seq_infm_auth = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_seq_infm_auth,LS_DB_MESSAGE_INFM_AUTH.stmsg_sender_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sender_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_sender_interchange_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sender_interchange_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_sender_email_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sender_email_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_sender_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_sender_comments,LS_DB_MESSAGE_INFM_AUTH.stmsg_safety_report_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_safety_report_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_review_lit_doc = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_review_lit_doc,LS_DB_MESSAGE_INFM_AUTH.stmsg_retrieve_comment = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_retrieve_comment,LS_DB_MESSAGE_INFM_AUTH.stmsg_resubmit_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_resubmit_flag,LS_DB_MESSAGE_INFM_AUTH.stmsg_reportercountry_nf = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reportercountry_nf,LS_DB_MESSAGE_INFM_AUTH.stmsg_reporter_country = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reporter_country,LS_DB_MESSAGE_INFM_AUTH.stmsg_reporter_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reporter_comments,LS_DB_MESSAGE_INFM_AUTH.stmsg_reporter_causality = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reporter_causality,LS_DB_MESSAGE_INFM_AUTH.stmsg_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_report_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_report_size = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_report_size,LS_DB_MESSAGE_INFM_AUTH.stmsg_report_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_report_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_report_doc_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_report_doc_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_report_ack = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_report_ack,LS_DB_MESSAGE_INFM_AUTH.stmsg_remove_reason_comment = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_remove_reason_comment,LS_DB_MESSAGE_INFM_AUTH.stmsg_remove_reason_code = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_remove_reason_code,LS_DB_MESSAGE_INFM_AUTH.stmsg_regulatory_report_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_regulatory_report_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_regional_state_nf = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_regional_state_nf,LS_DB_MESSAGE_INFM_AUTH.stmsg_regional_state = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_regional_state,LS_DB_MESSAGE_INFM_AUTH.stmsg_regenerated_doc_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_regenerated_doc_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_reg_clock_start_date_jpn = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reg_clock_start_date_jpn,LS_DB_MESSAGE_INFM_AUTH.stmsg_record_owner_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_record_owner_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_record_owner_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_record_owner_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_record_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_record_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_recipient_sender_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_recipient_sender_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_recipient_receiver_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_recipient_receiver_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_recepient_country_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_recepient_country_de_ml,LS_DB_MESSAGE_INFM_AUTH.stmsg_recepient_country = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_recepient_country,LS_DB_MESSAGE_INFM_AUTH.stmsg_receiver_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_receiver_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_receiver_interchange_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_receiver_interchange_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_receipt_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_receipt_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_receipent_sender_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_receipent_sender_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_receipent_receiver_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_receipent_receiver_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_read_unread_correspondence = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_read_unread_correspondence,LS_DB_MESSAGE_INFM_AUTH.stmsg_reactions_available = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_reactions_available,LS_DB_MESSAGE_INFM_AUTH.stmsg_r3_xml_size = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_r3_xml_size,LS_DB_MESSAGE_INFM_AUTH.stmsg_qbe_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_qbe_comments,LS_DB_MESSAGE_INFM_AUTH.stmsg_protocol_no_nf = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_protocol_no_nf,LS_DB_MESSAGE_INFM_AUTH.stmsg_protocol_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_protocol_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_prolonged_hospitalization = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_prolonged_hospitalization,LS_DB_MESSAGE_INFM_AUTH.stmsg_products_available = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_products_available,LS_DB_MESSAGE_INFM_AUTH.stmsg_product_selected_level = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_product_selected_level,LS_DB_MESSAGE_INFM_AUTH.stmsg_priority = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_priority,LS_DB_MESSAGE_INFM_AUTH.stmsg_primary_source_country = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_primary_source_country,LS_DB_MESSAGE_INFM_AUTH.stmsg_primary_reporter = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_primary_reporter,LS_DB_MESSAGE_INFM_AUTH.stmsg_previous_submission_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_previous_submission_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_patientinitial_nf = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_patientinitial_nf,LS_DB_MESSAGE_INFM_AUTH.stmsg_patient_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_patient_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_partner_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_partner_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_partner_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_partner_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_parent_opm_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_parent_opm_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_owner = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_owner,LS_DB_MESSAGE_INFM_AUTH.stmsg_override_reason = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_override_reason,LS_DB_MESSAGE_INFM_AUTH.stmsg_override_due_dt = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_override_due_dt,LS_DB_MESSAGE_INFM_AUTH.stmsg_overall_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_overall_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_outbound_ref4 = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_outbound_ref4,LS_DB_MESSAGE_INFM_AUTH.stmsg_outbound_ref3 = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_outbound_ref3,LS_DB_MESSAGE_INFM_AUTH.stmsg_outbound_ref2 = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_outbound_ref2,LS_DB_MESSAGE_INFM_AUTH.stmsg_outbound_ref1 = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_outbound_ref1,LS_DB_MESSAGE_INFM_AUTH.stmsg_other_safety_ref_ver = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_other_safety_ref_ver,LS_DB_MESSAGE_INFM_AUTH.stmsg_other_safety_ref_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_other_safety_ref_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_other_ref_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_other_ref_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_oth_med_imp_condition = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_oth_med_imp_condition,LS_DB_MESSAGE_INFM_AUTH.stmsg_notes_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_notes_flag,LS_DB_MESSAGE_INFM_AUTH.stmsg_next_transition_note = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_next_transition_note,LS_DB_MESSAGE_INFM_AUTH.stmsg_next_transition = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_next_transition,LS_DB_MESSAGE_INFM_AUTH.stmsg_narrative_include_clinical = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_narrative_include_clinical,LS_DB_MESSAGE_INFM_AUTH.stmsg_mhlw_report_type_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_mhlw_report_type_de_ml,LS_DB_MESSAGE_INFM_AUTH.stmsg_mhlw_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_mhlw_report_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_mfr_number = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_mfr_number,LS_DB_MESSAGE_INFM_AUTH.stmsg_message_state = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_message_state,LS_DB_MESSAGE_INFM_AUTH.stmsg_message_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_message_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_medium_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_medium_de_ml,LS_DB_MESSAGE_INFM_AUTH.stmsg_medium = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_medium,LS_DB_MESSAGE_INFM_AUTH.stmsg_medically_confirmed = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_medically_confirmed,LS_DB_MESSAGE_INFM_AUTH.stmsg_mdn_received = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_mdn_received,LS_DB_MESSAGE_INFM_AUTH.stmsg_mdn_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_mdn_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_manual_upload_file_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_manual_upload_file_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_manual_upload = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_manual_upload,LS_DB_MESSAGE_INFM_AUTH.stmsg_manual_ssd = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_manual_ssd,LS_DB_MESSAGE_INFM_AUTH.stmsg_local_version_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_local_version_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_literature = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_literature,LS_DB_MESSAGE_INFM_AUTH.stmsg_life_threatening = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_life_threatening,LS_DB_MESSAGE_INFM_AUTH.stmsg_license_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_license_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_lde_lock = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lde_lock,LS_DB_MESSAGE_INFM_AUTH.stmsg_lde_language = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lde_language,LS_DB_MESSAGE_INFM_AUTH.stmsg_lde_approved = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lde_approved,LS_DB_MESSAGE_INFM_AUTH.stmsg_latest_receive_date_jpn = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_latest_receive_date_jpn,LS_DB_MESSAGE_INFM_AUTH.stmsg_latest_receipt_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_latest_receipt_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_lateness_resp_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lateness_resp_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_lateness_reason_comment = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lateness_reason_comment,LS_DB_MESSAGE_INFM_AUTH.stmsg_lateness_reason_code = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_lateness_reason_code,LS_DB_MESSAGE_INFM_AUTH.stmsg_late_by = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_late_by,LS_DB_MESSAGE_INFM_AUTH.stmsg_last_user_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_last_user_modified,LS_DB_MESSAGE_INFM_AUTH.stmsg_last_transition = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_last_transition,LS_DB_MESSAGE_INFM_AUTH.stmsg_last_alert_notified_time = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_last_alert_notified_time,LS_DB_MESSAGE_INFM_AUTH.stmsg_language_code = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_language_code,LS_DB_MESSAGE_INFM_AUTH.stmsg_is_expedited = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_is_expedited,LS_DB_MESSAGE_INFM_AUTH.stmsg_is_due_dt_recalctd = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_is_due_dt_recalctd,LS_DB_MESSAGE_INFM_AUTH.stmsg_initial_aer_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_initial_aer_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_informing_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_informing_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_informing_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_informing_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_include_r3_in_r2 = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_include_r3_in_r2,LS_DB_MESSAGE_INFM_AUTH.stmsg_identification_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_identification_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_icsr_rule_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_icsr_rule_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_ib_labelling = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ib_labelling,LS_DB_MESSAGE_INFM_AUTH.stmsg_hp_confirmed = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_hp_confirmed,LS_DB_MESSAGE_INFM_AUTH.stmsg_format_version = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_format_version,LS_DB_MESSAGE_INFM_AUTH.stmsg_format_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_format_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_format_country = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_format_country,LS_DB_MESSAGE_INFM_AUTH.stmsg_fl_submit_trans = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fl_submit_trans,LS_DB_MESSAGE_INFM_AUTH.stmsg_fl_overall_lateness = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fl_overall_lateness,LS_DB_MESSAGE_INFM_AUTH.stmsg_fl_multi_reporting = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fl_multi_reporting,LS_DB_MESSAGE_INFM_AUTH.stmsg_fl_data_privacy = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fl_data_privacy,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_lsip_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_lsip_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_lp_sender_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_lp_sender_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_lp_receiver_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_lp_receiver_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_lms_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_lms_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_ldm_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_ldm_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_ddc_contact_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_ddc_contact_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_awa_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_awa_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_aw_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_aw_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fk_ap_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fk_ap_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fax_template_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fax_template_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_fax_number = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fax_number,LS_DB_MESSAGE_INFM_AUTH.stmsg_fax_batch_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_fax_batch_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_failed_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_failed_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_external_app_wfi_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_external_app_wfi_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_external_app_cm_seq_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_external_app_cm_seq_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_ext_sourced_case_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ext_sourced_case_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_eu_spc_labelling = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_eu_spc_labelling,LS_DB_MESSAGE_INFM_AUTH.stmsg_esm_communication_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_esm_communication_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_error_message = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_error_message,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_template_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_template_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_subject = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_subject,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_sent = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_sent,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_retry_count = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_retry_count,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_message = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_message,LS_DB_MESSAGE_INFM_AUTH.stmsg_email_address = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_email_address,LS_DB_MESSAGE_INFM_AUTH.stmsg_elastic_indexed = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_elastic_indexed,LS_DB_MESSAGE_INFM_AUTH.stmsg_e2b_msg_queue_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_e2b_msg_queue_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_e2b_message_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_e2b_message_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_e2b_dtd_version = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_e2b_dtd_version,LS_DB_MESSAGE_INFM_AUTH.stmsg_duplicate_numb = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_duplicate_numb,LS_DB_MESSAGE_INFM_AUTH.stmsg_duplicate_ack = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_duplicate_ack,LS_DB_MESSAGE_INFM_AUTH.stmsg_downgrade_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_downgrade_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_doctype_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_doctype_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_doctype_code = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_doctype_code,LS_DB_MESSAGE_INFM_AUTH.stmsg_do_not_validate_and_extract = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_do_not_validate_and_extract,LS_DB_MESSAGE_INFM_AUTH.stmsg_distribution_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_distribution_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_dist_rule_anchor_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_dist_rule_anchor_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_dist_format_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_dist_format_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_dist_aer_info_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_dist_aer_info_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_delivery_notification_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_delivery_notification_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_death = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_death,LS_DB_MESSAGE_INFM_AUTH.stmsg_date_xmit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_date_xmit,LS_DB_MESSAGE_INFM_AUTH.stmsg_date_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_date_modified,LS_DB_MESSAGE_INFM_AUTH.stmsg_date_created = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_date_created,LS_DB_MESSAGE_INFM_AUTH.stmsg_data_privacy = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_data_privacy,LS_DB_MESSAGE_INFM_AUTH.stmsg_data_exclusion = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_data_exclusion,LS_DB_MESSAGE_INFM_AUTH.stmsg_created_year = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_created_year,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_template_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_template_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_size = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_size,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_holder = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_holder,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_doc_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_doc_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_cover_letter_desc = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cover_letter_desc,LS_DB_MESSAGE_INFM_AUTH.stmsg_correspondence_seq = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_correspondence_seq,LS_DB_MESSAGE_INFM_AUTH.stmsg_correspondence_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_correspondence_flag,LS_DB_MESSAGE_INFM_AUTH.stmsg_core_labelling = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_core_labelling,LS_DB_MESSAGE_INFM_AUTH.stmsg_copied_submission = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_copied_submission,LS_DB_MESSAGE_INFM_AUTH.stmsg_copied_case_seq = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_copied_case_seq,LS_DB_MESSAGE_INFM_AUTH.stmsg_contact_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_contact_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_consider_similar_product = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_consider_similar_product,LS_DB_MESSAGE_INFM_AUTH.stmsg_company_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_company_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_company_unit = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_company_unit,LS_DB_MESSAGE_INFM_AUTH.stmsg_company_numb = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_company_numb,LS_DB_MESSAGE_INFM_AUTH.stmsg_company_causality = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_company_causality,LS_DB_MESSAGE_INFM_AUTH.stmsg_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_comments,LS_DB_MESSAGE_INFM_AUTH.stmsg_child_wf_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_child_wf_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_cc_email_address = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_cc_email_address,LS_DB_MESSAGE_INFM_AUTH.stmsg_causality_assessment = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_causality_assessment,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_version = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_version,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_susar = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_susar,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_significance = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_significance,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_report_doc_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_report_doc_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_nullified = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_nullified,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_level_expectedness = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_level_expectedness,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_country = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_country,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_blinded = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_blinded,LS_DB_MESSAGE_INFM_AUTH.stmsg_case_archived = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_case_archived,LS_DB_MESSAGE_INFM_AUTH.stmsg_blinded_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_blinded_report_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_batch_queue_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_batch_queue_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_batch_lde_approved = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_batch_lde_approved,LS_DB_MESSAGE_INFM_AUTH.stmsg_batch_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_batch_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_batch_export = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_batch_export,LS_DB_MESSAGE_INFM_AUTH.stmsg_batch_doc_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_batch_doc_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_auto_complete_first_activity = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_auto_complete_first_activity,LS_DB_MESSAGE_INFM_AUTH.stmsg_authority_partner_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_authority_partner_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_authority_numb = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_authority_numb,LS_DB_MESSAGE_INFM_AUTH.stmsg_att_mdn_ack_received = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_att_mdn_ack_received,LS_DB_MESSAGE_INFM_AUTH.stmsg_att_icsr_doc_sent = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_att_icsr_doc_sent,LS_DB_MESSAGE_INFM_AUTH.stmsg_att_ack_code = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_att_ack_code,LS_DB_MESSAGE_INFM_AUTH.stmsg_assigned_to = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_assigned_to,LS_DB_MESSAGE_INFM_AUTH.stmsg_assign_to = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_assign_to,LS_DB_MESSAGE_INFM_AUTH.stmsg_ari_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ari_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_archived_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_archived_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_archive_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_archive_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_archive_process_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_archive_process_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_sender_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_sender_comments,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_req_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_req_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_nar_include_clinical = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_nar_include_clinical,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_message_type = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_message_type,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_lde_language = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_lde_language,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_error_message = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_error_message,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_distribution_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_distribution_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_distributed_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_distributed_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_app_date_distributed = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_app_date_distributed,LS_DB_MESSAGE_INFM_AUTH.stmsg_alert_sent = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_alert_sent,LS_DB_MESSAGE_INFM_AUTH.stmsg_aim_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aim_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_reporting_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_reporting_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_reference_number = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_reference_number,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_reference_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_reference_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_reason_notsubmitted = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_reason_notsubmitted,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_followup_number = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_followup_number,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_date_informed = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_date_informed,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_authority = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_authority,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_acknowledgement_num = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_acknowledgement_num,LS_DB_MESSAGE_INFM_AUTH.stmsg_agx_aoip_acknowledgement_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_agx_aoip_acknowledgement_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_affiliate_submission_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_affiliate_submission_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_receipt_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_receipt_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_processing_unit_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_processing_unit_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_no = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_no,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_company_unit_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_company_unit_rec_id,LS_DB_MESSAGE_INFM_AUTH.stmsg_aer_company_unit_name = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_aer_company_unit_name,LS_DB_MESSAGE_INFM_AUTH.stmsg_add_narratives_overflow = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_add_narratives_overflow,LS_DB_MESSAGE_INFM_AUTH.stmsg_activity_due_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_activity_due_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_activity_arrival_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_activity_arrival_date,LS_DB_MESSAGE_INFM_AUTH.stmsg_ack_status = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ack_status,LS_DB_MESSAGE_INFM_AUTH.stmsg_ack_received = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ack_received,LS_DB_MESSAGE_INFM_AUTH.stmsg_ack_date = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_ack_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_version_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_version_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_user_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_user_modified,LS_DB_MESSAGE_INFM_AUTH.infmauth_user_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_user_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_user_created = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_user_created,LS_DB_MESSAGE_INFM_AUTH.infmauth_uf_dist_report_sent_to_mfr = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_uf_dist_report_sent_to_mfr,LS_DB_MESSAGE_INFM_AUTH.infmauth_uf_dist_report_sent_to_fda = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_uf_dist_report_sent_to_fda,LS_DB_MESSAGE_INFM_AUTH.infmauth_uf_dist_dat_sent_to_mfr_prec = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_uf_dist_dat_sent_to_mfr_prec,LS_DB_MESSAGE_INFM_AUTH.infmauth_uf_dist_dat_sent_to_fda_prec = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_uf_dist_dat_sent_to_fda_prec,LS_DB_MESSAGE_INFM_AUTH.infmauth_type_of_report_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_type_of_report_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_type_of_report = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_type_of_report,LS_DB_MESSAGE_INFM_AUTH.infmauth_type_of_ack_received_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_type_of_ack_received_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_type_of_ack_received = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_type_of_ack_received,LS_DB_MESSAGE_INFM_AUTH.infmauth_targeted_disease = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_targeted_disease,LS_DB_MESSAGE_INFM_AUTH.infmauth_suspect_product_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_suspect_product_rec_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_suspect_product = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_suspect_product,LS_DB_MESSAGE_INFM_AUTH.infmauth_study_phase = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_study_phase,LS_DB_MESSAGE_INFM_AUTH.infmauth_spr_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_spr_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_source_of_ia = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_source_of_ia,LS_DB_MESSAGE_INFM_AUTH.infmauth_safety_report_version_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_safety_report_version_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_safety_report_id_enable = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_safety_report_id_enable,LS_DB_MESSAGE_INFM_AUTH.infmauth_safety_report_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_safety_report_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_reporting_status_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reporting_status_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_reporting_status = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reporting_status,LS_DB_MESSAGE_INFM_AUTH.infmauth_reporter_comments_language = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reporter_comments_language,LS_DB_MESSAGE_INFM_AUTH.infmauth_reporter_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reporter_comments,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_withdrawal_reason = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_withdrawal_reason,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_sent_date_to_mfr = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_sent_date_to_mfr,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_sent_date_to_fda = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_sent_date_to_fda,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_medium_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_medium_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_medium = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_medium,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_format_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_format_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_format = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_format,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_for_nullifn_or_amend = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_for_nullifn_or_amend,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_follow_up_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_follow_up_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_report_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_report_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_rep_informed_auth_directly = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_rep_informed_auth_directly,LS_DB_MESSAGE_INFM_AUTH.infmauth_reg_clock_start_dt_comment = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reg_clock_start_dt_comment,LS_DB_MESSAGE_INFM_AUTH.infmauth_reference_number = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reference_number,LS_DB_MESSAGE_INFM_AUTH.infmauth_reference_no_received_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reference_no_received_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_record_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_record_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_receipt_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_receipt_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_reason_notsubmitted_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reason_notsubmitted_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_reason_notsubmitted = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reason_notsubmitted,LS_DB_MESSAGE_INFM_AUTH.infmauth_reason_for_nullifn_or_amend = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reason_for_nullifn_or_amend,LS_DB_MESSAGE_INFM_AUTH.infmauth_reason_for_incomplete = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reason_for_incomplete,LS_DB_MESSAGE_INFM_AUTH.infmauth_reason_for_cancellation = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_reason_for_cancellation,LS_DB_MESSAGE_INFM_AUTH.infmauth_product_rank_order_json = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_product_rank_order_json,LS_DB_MESSAGE_INFM_AUTH.infmauth_pmda_report_id_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_pmda_report_id_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_patient_under_treatment = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_patient_under_treatment,LS_DB_MESSAGE_INFM_AUTH.infmauth_other_safety_ref_ver = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_other_safety_ref_ver,LS_DB_MESSAGE_INFM_AUTH.infmauth_other_safety_ref_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_other_safety_ref_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_other_ref_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_other_ref_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_other_comments_jpn = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_other_comments_jpn,LS_DB_MESSAGE_INFM_AUTH.infmauth_other_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_other_comments,LS_DB_MESSAGE_INFM_AUTH.infmauth_nullify_reason_details = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_nullify_reason_details,LS_DB_MESSAGE_INFM_AUTH.infmauth_new_drug_classification = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_new_drug_classification,LS_DB_MESSAGE_INFM_AUTH.infmauth_native_language = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_native_language,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_report_type_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_report_type_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_report_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_regenerative_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_regenerative_report_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_device_rep_year = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_device_rep_year,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_device_rep_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_device_rep_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_device_prev_report_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_device_prev_report_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_mhlw_device_prev_rep_year = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mhlw_device_prev_rep_year,LS_DB_MESSAGE_INFM_AUTH.infmauth_mdn_received = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_mdn_received,LS_DB_MESSAGE_INFM_AUTH.infmauth_locally_expedited_nf = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_locally_expedited_nf,LS_DB_MESSAGE_INFM_AUTH.infmauth_locally_expedited = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_locally_expedited,LS_DB_MESSAGE_INFM_AUTH.infmauth_local_criteria_report_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_local_criteria_report_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_linked_report_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_linked_report_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_jpn_counter_measure = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_jpn_counter_measure,LS_DB_MESSAGE_INFM_AUTH.infmauth_immediate_report_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_immediate_report_flag,LS_DB_MESSAGE_INFM_AUTH.infmauth_future_approach = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_future_approach,LS_DB_MESSAGE_INFM_AUTH.infmauth_followup_type_correction = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_followup_type_correction,LS_DB_MESSAGE_INFM_AUTH.infmauth_followup_type_additional_info = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_followup_type_additional_info,LS_DB_MESSAGE_INFM_AUTH.infmauth_followup_to_device_eval = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_followup_to_device_eval,LS_DB_MESSAGE_INFM_AUTH.infmauth_followup_response_to_auth_req = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_followup_response_to_auth_req,LS_DB_MESSAGE_INFM_AUTH.infmauth_follow_up_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_follow_up_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_fk_lsm_rec_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_fk_lsm_rec_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_final_report = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_final_report,LS_DB_MESSAGE_INFM_AUTH.infmauth_expected_date_of_next_report = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_expected_date_of_next_report,LS_DB_MESSAGE_INFM_AUTH.infmauth_event_description_language = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_event_description_language,LS_DB_MESSAGE_INFM_AUTH.infmauth_event_description = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_event_description,LS_DB_MESSAGE_INFM_AUTH.infmauth_e2b_message_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_e2b_message_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_dist_info_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_dist_info_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_decision_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_decision_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_date_modified = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_date_modified,LS_DB_MESSAGE_INFM_AUTH.infmauth_date_informed_to_distributor = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_date_informed_to_distributor,LS_DB_MESSAGE_INFM_AUTH.infmauth_date_informed_precision = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_date_informed_precision,LS_DB_MESSAGE_INFM_AUTH.infmauth_date_informed = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_date_informed,LS_DB_MESSAGE_INFM_AUTH.infmauth_date_created = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_date_created,LS_DB_MESSAGE_INFM_AUTH.infmauth_contact_type = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_contact_type,LS_DB_MESSAGE_INFM_AUTH.infmauth_complete_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_complete_flag,LS_DB_MESSAGE_INFM_AUTH.infmauth_company_remarks_language = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_company_remarks_language,LS_DB_MESSAGE_INFM_AUTH.infmauth_company_remarks = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_company_remarks,LS_DB_MESSAGE_INFM_AUTH.infmauth_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_comments,LS_DB_MESSAGE_INFM_AUTH.infmauth_clinical_trial_notificatn = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_clinical_trial_notificatn,LS_DB_MESSAGE_INFM_AUTH.infmauth_clinical_drug_code = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_clinical_drug_code,LS_DB_MESSAGE_INFM_AUTH.infmauth_case_sum_and_rep_comments = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_case_sum_and_rep_comments,LS_DB_MESSAGE_INFM_AUTH.infmauth_cancellation_flag_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_cancellation_flag_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_cancellation_flag = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_cancellation_flag,LS_DB_MESSAGE_INFM_AUTH.infmauth_authority_resp_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_authority_resp_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_authority_partner_id = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_authority_partner_id,LS_DB_MESSAGE_INFM_AUTH.infmauth_authority_or_company_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_authority_or_company_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_authority_name_de_ml = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_authority_name_de_ml,LS_DB_MESSAGE_INFM_AUTH.infmauth_authority_name = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_authority_name,LS_DB_MESSAGE_INFM_AUTH.infmauth_auth_no_comp_no_enable = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_auth_no_comp_no_enable,LS_DB_MESSAGE_INFM_AUTH.infmauth_aer_no = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_aer_no,LS_DB_MESSAGE_INFM_AUTH.infmauth_acknowledgement_received_date = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_acknowledgement_received_date,LS_DB_MESSAGE_INFM_AUTH.infmauth_acknowledgement_number = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_acknowledgement_number,LS_DB_MESSAGE_INFM_AUTH.infmauth_ack_received = LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_ack_received,
LS_DB_MESSAGE_INFM_AUTH.PROCESSING_DT = LS_DB_MESSAGE_INFM_AUTH_TMP.PROCESSING_DT,
LS_DB_MESSAGE_INFM_AUTH.receipt_id     =LS_DB_MESSAGE_INFM_AUTH_TMP.receipt_id    ,
LS_DB_MESSAGE_INFM_AUTH.case_no        =LS_DB_MESSAGE_INFM_AUTH_TMP.case_no           ,
LS_DB_MESSAGE_INFM_AUTH.case_version   =LS_DB_MESSAGE_INFM_AUTH_TMP.case_version      ,
LS_DB_MESSAGE_INFM_AUTH.version_no     =LS_DB_MESSAGE_INFM_AUTH_TMP.version_no        ,
LS_DB_MESSAGE_INFM_AUTH.user_modified  =LS_DB_MESSAGE_INFM_AUTH_TMP.user_modified     ,
LS_DB_MESSAGE_INFM_AUTH.date_modified  =LS_DB_MESSAGE_INFM_AUTH_TMP.date_modified     ,
LS_DB_MESSAGE_INFM_AUTH.expiry_date    =LS_DB_MESSAGE_INFM_AUTH_TMP.expiry_date       ,
LS_DB_MESSAGE_INFM_AUTH.created_by     =LS_DB_MESSAGE_INFM_AUTH_TMP.created_by        ,
LS_DB_MESSAGE_INFM_AUTH.created_dt     =LS_DB_MESSAGE_INFM_AUTH_TMP.created_dt        ,
LS_DB_MESSAGE_INFM_AUTH.load_ts        =LS_DB_MESSAGE_INFM_AUTH_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP 
WHERE LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_MESSAGE_INFM_AUTH_TMP.PROCESSING_DT = LS_DB_MESSAGE_INFM_AUTH.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_MESSAGE_INFM_AUTH.infmauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_MESSAGE_INFM_AUTH.stmsg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID ,LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP 
ON LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID=LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_RECORD_ID
AND LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID 
where LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID  is null AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID in (select stmsg_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP )
) TMP where LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID ,LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP 
    ON LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID=LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_RECORD_ID
    AND LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID 
    where LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID  is null AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID in (select stmsg_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH
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
integration_id ,stmsg_xml_doc_config_name,
stmsg_wf_status,
stmsg_wf_message_state,
stmsg_wf_completion_flag,
stmsg_wf_activity_name,
stmsg_version_no,
stmsg_uspi_labelling,
stmsg_user_submitted_date,
stmsg_user_modified,
stmsg_user_last_modified,
stmsg_user_created,
stmsg_trade_name,
stmsg_to_be_synchronize,
stmsg_to_be_synch_sub_status,
stmsg_to_be_synch_due_date,
stmsg_to_be_synch_cm,
stmsg_timeframe,
stmsg_time_line_day,
stmsg_suspect_product,
stmsg_support_docs_merged,
stmsg_submitted_by,
stmsg_submit_notes,
stmsg_submission_unit_name,
stmsg_submission_unit,
stmsg_submission_status_de_ml,
stmsg_submission_status,
stmsg_submission_state,
stmsg_submission_query_name,
stmsg_submission_query_desc,
stmsg_submission_due_date,
stmsg_submission_date,
stmsg_submission_condition_type,
stmsg_submission_attempt_count,
stmsg_sub_due_date_jpn,
stmsg_sub_compliance,
stmsg_study_type,
stmsg_study_design,
stmsg_status,
stmsg_spr_id,
stmsg_special_project_names,
stmsg_source_of_submission,
stmsg_single_email_submission,
stmsg_seriousness,
stmsg_serious_reaction,
stmsg_seq_lateness_info,
stmsg_seq_infm_auth,
stmsg_sender_unit,
stmsg_sender_interchange_id,
stmsg_sender_email_id,
stmsg_sender_comments,
stmsg_safety_report_id,
stmsg_review_lit_doc,
stmsg_retrieve_comment,
stmsg_resubmit_flag,
stmsg_reportercountry_nf,
stmsg_reporter_country,
stmsg_reporter_comments,
stmsg_reporter_causality,
stmsg_report_type,
stmsg_report_size,
stmsg_report_name,
stmsg_report_doc_type,
stmsg_report_ack,
stmsg_remove_reason_comment,
stmsg_remove_reason_code,
stmsg_regulatory_report_id,
stmsg_regional_state_nf,
stmsg_regional_state,
stmsg_regenerated_doc_id,
stmsg_reg_clock_start_date_jpn,
stmsg_record_owner_unit_name,
stmsg_record_owner_unit,
stmsg_record_id,
stmsg_recipient_sender_rec_id,
stmsg_recipient_receiver_rec_id,
stmsg_recepient_country_de_ml,
stmsg_recepient_country,
stmsg_receiver_unit,
stmsg_receiver_interchange_id,
stmsg_receipt_no,
stmsg_receipent_sender_type,
stmsg_receipent_receiver_type,
stmsg_read_unread_correspondence,
stmsg_reactions_available,
stmsg_r3_xml_size,
stmsg_qbe_comments,
stmsg_protocol_no_nf,
stmsg_protocol_no,
stmsg_prolonged_hospitalization,
stmsg_products_available,
stmsg_product_selected_level,
stmsg_priority,
stmsg_primary_source_country,
stmsg_primary_reporter,
stmsg_previous_submission_status,
stmsg_patientinitial_nf,
stmsg_patient_id,
stmsg_partner_name,
stmsg_partner_id,
stmsg_parent_opm_rec_id,
stmsg_owner,
stmsg_override_reason,
stmsg_override_due_dt,
stmsg_overall_due_date,
stmsg_outbound_ref4,
stmsg_outbound_ref3,
stmsg_outbound_ref2,
stmsg_outbound_ref1,
stmsg_other_safety_ref_ver,
stmsg_other_safety_ref_no,
stmsg_other_ref_date,
stmsg_oth_med_imp_condition,
stmsg_notes_flag,
stmsg_next_transition_note,
stmsg_next_transition,
stmsg_narrative_include_clinical,
stmsg_mhlw_report_type_de_ml,
stmsg_mhlw_report_type,
stmsg_mfr_number,
stmsg_message_state,
stmsg_message_no,
stmsg_medium_de_ml,
stmsg_medium,
stmsg_medically_confirmed,
stmsg_mdn_received,
stmsg_mdn_date,
stmsg_manual_upload_file_name,
stmsg_manual_upload,
stmsg_manual_ssd,
stmsg_local_version_no,
stmsg_literature,
stmsg_life_threatening,
stmsg_license_type,
stmsg_lde_lock,
stmsg_lde_language,
stmsg_lde_approved,
stmsg_latest_receive_date_jpn,
stmsg_latest_receipt_date,
stmsg_lateness_resp_unit_name,
stmsg_lateness_reason_comment,
stmsg_lateness_reason_code,
stmsg_late_by,
stmsg_last_user_modified,
stmsg_last_transition,
stmsg_last_alert_notified_time,
stmsg_language_code,
stmsg_is_expedited,
stmsg_is_due_dt_recalctd,
stmsg_initial_aer_id,
stmsg_informing_unit_name,
stmsg_informing_unit,
stmsg_include_r3_in_r2,
stmsg_identification_no,
stmsg_icsr_rule_id,
stmsg_ib_labelling,
stmsg_hp_confirmed,
stmsg_format_version,
stmsg_format_type,
stmsg_format_country,
stmsg_fl_submit_trans,
stmsg_fl_overall_lateness,
stmsg_fl_multi_reporting,
stmsg_fl_data_privacy,
stmsg_fk_lsip_rec_id,
stmsg_fk_lp_sender_rec_id,
stmsg_fk_lp_receiver_rec_id,
stmsg_fk_lms_rec_id,
stmsg_fk_ldm_rec_id,
stmsg_fk_ddc_contact_id,
stmsg_fk_awa_rec_id,
stmsg_fk_aw_rec_id,
stmsg_fk_ap_rec_id,
stmsg_fax_template_rec_id,
stmsg_fax_number,
stmsg_fax_batch_id,
stmsg_failed_date,
stmsg_external_app_wfi_id,
stmsg_external_app_cm_seq_id,
stmsg_ext_sourced_case_no,
stmsg_eu_spc_labelling,
stmsg_esm_communication_status,
stmsg_error_message,
stmsg_email_template_rec_id,
stmsg_email_subject,
stmsg_email_sent,
stmsg_email_retry_count,
stmsg_email_message,
stmsg_email_address,
stmsg_elastic_indexed,
stmsg_e2b_msg_queue_status,
stmsg_e2b_message_type,
stmsg_e2b_dtd_version,
stmsg_duplicate_numb,
stmsg_duplicate_ack,
stmsg_downgrade_status,
stmsg_doctype_name,
stmsg_doctype_code,
stmsg_do_not_validate_and_extract,
stmsg_distribution_unit_name,
stmsg_dist_rule_anchor_rec_id,
stmsg_dist_format_rec_id,
stmsg_dist_aer_info_id,
stmsg_delivery_notification_status,
stmsg_death,
stmsg_date_xmit,
stmsg_date_modified,
stmsg_date_created,
stmsg_data_privacy,
stmsg_data_exclusion,
stmsg_created_year,
stmsg_cover_letter_type,
stmsg_cover_letter_template_rec_id,
stmsg_cover_letter_size,
stmsg_cover_letter_name,
stmsg_cover_letter_holder,
stmsg_cover_letter_doc_id,
stmsg_cover_letter_desc,
stmsg_correspondence_seq,
stmsg_correspondence_flag,
stmsg_core_labelling,
stmsg_copied_submission,
stmsg_copied_case_seq,
stmsg_contact_type,
stmsg_consider_similar_product,
stmsg_company_unit_name,
stmsg_company_unit,
stmsg_company_numb,
stmsg_company_causality,
stmsg_comments,
stmsg_child_wf_id,
stmsg_cc_email_address,
stmsg_causality_assessment,
stmsg_case_version,
stmsg_case_type,
stmsg_case_susar,
stmsg_case_significance,
stmsg_case_report_doc_id,
stmsg_case_nullified,
stmsg_case_level_expectedness,
stmsg_case_due_date,
stmsg_case_country,
stmsg_case_blinded,
stmsg_case_archived,
stmsg_blinded_report_type,
stmsg_batch_queue_id,
stmsg_batch_lde_approved,
stmsg_batch_id,
stmsg_batch_export,
stmsg_batch_doc_id,
stmsg_auto_complete_first_activity,
stmsg_authority_partner_id,
stmsg_authority_numb,
stmsg_att_mdn_ack_received,
stmsg_att_icsr_doc_sent,
stmsg_att_ack_code,
stmsg_assigned_to,
stmsg_assign_to,
stmsg_ari_rec_id,
stmsg_archived_date,
stmsg_archive_status,
stmsg_archive_process_date,
stmsg_app_sender_comments,
stmsg_app_req_id,
stmsg_app_nar_include_clinical,
stmsg_app_message_type,
stmsg_app_lde_language,
stmsg_app_error_message,
stmsg_app_distribution_status,
stmsg_app_distributed_date,
stmsg_app_date_distributed,
stmsg_alert_sent,
stmsg_aim_rec_id,
stmsg_agx_aoip_reporting_status,
stmsg_agx_aoip_reference_number,
stmsg_agx_aoip_reference_date,
stmsg_agx_aoip_reason_notsubmitted,
stmsg_agx_aoip_followup_number,
stmsg_agx_aoip_date_informed,
stmsg_agx_aoip_authority,
stmsg_agx_aoip_acknowledgement_num,
stmsg_agx_aoip_acknowledgement_date,
stmsg_affiliate_submission_due_date,
stmsg_aer_receipt_date,
stmsg_aer_processing_unit_rec_id,
stmsg_aer_no,
stmsg_aer_id,
stmsg_aer_company_unit_rec_id,
stmsg_aer_company_unit_name,
stmsg_add_narratives_overflow,
stmsg_activity_due_date,
stmsg_activity_arrival_date,
stmsg_ack_status,
stmsg_ack_received,
stmsg_ack_date,
infmauth_version_no,
infmauth_user_modified,
infmauth_user_id,
infmauth_user_created,
infmauth_uf_dist_report_sent_to_mfr,
infmauth_uf_dist_report_sent_to_fda,
infmauth_uf_dist_dat_sent_to_mfr_prec,
infmauth_uf_dist_dat_sent_to_fda_prec,
infmauth_type_of_report_de_ml,
infmauth_type_of_report,
infmauth_type_of_ack_received_de_ml,
infmauth_type_of_ack_received,
infmauth_targeted_disease,
infmauth_suspect_product_rec_id,
infmauth_suspect_product,
infmauth_study_phase,
infmauth_spr_id,
infmauth_source_of_ia,
infmauth_safety_report_version_no,
infmauth_safety_report_id_enable,
infmauth_safety_report_id,
infmauth_reporting_status_de_ml,
infmauth_reporting_status,
infmauth_reporter_comments_language,
infmauth_reporter_comments,
infmauth_report_withdrawal_reason,
infmauth_report_sent_date_to_mfr,
infmauth_report_sent_date_to_fda,
infmauth_report_medium_de_ml,
infmauth_report_medium,
infmauth_report_format_de_ml,
infmauth_report_format,
infmauth_report_for_nullifn_or_amend,
infmauth_report_follow_up_no,
infmauth_report_date,
infmauth_rep_informed_auth_directly,
infmauth_reg_clock_start_dt_comment,
infmauth_reference_number,
infmauth_reference_no_received_date,
infmauth_record_id,
infmauth_receipt_no,
infmauth_reason_notsubmitted_de_ml,
infmauth_reason_notsubmitted,
infmauth_reason_for_nullifn_or_amend,
infmauth_reason_for_incomplete,
infmauth_reason_for_cancellation,
infmauth_product_rank_order_json,
infmauth_pmda_report_id_no,
infmauth_patient_under_treatment,
infmauth_other_safety_ref_ver,
infmauth_other_safety_ref_no,
infmauth_other_ref_date,
infmauth_other_comments_jpn,
infmauth_other_comments,
infmauth_nullify_reason_details,
infmauth_new_drug_classification,
infmauth_native_language,
infmauth_mhlw_report_type_de_ml,
infmauth_mhlw_report_type,
infmauth_mhlw_regenerative_report_type,
infmauth_mhlw_device_rep_year,
infmauth_mhlw_device_rep_type,
infmauth_mhlw_device_prev_report_id,
infmauth_mhlw_device_prev_rep_year,
infmauth_mdn_received,
infmauth_locally_expedited_nf,
infmauth_locally_expedited,
infmauth_local_criteria_report_type,
infmauth_linked_report_id,
infmauth_jpn_counter_measure,
infmauth_immediate_report_flag,
infmauth_future_approach,
infmauth_followup_type_correction,
infmauth_followup_type_additional_info,
infmauth_followup_to_device_eval,
infmauth_followup_response_to_auth_req,
infmauth_follow_up_type,
infmauth_fk_lsm_rec_id,
infmauth_final_report,
infmauth_expected_date_of_next_report,
infmauth_event_description_language,
infmauth_event_description,
infmauth_e2b_message_type,
infmauth_dist_info_id,
infmauth_decision_date,
infmauth_date_modified,
infmauth_date_informed_to_distributor,
infmauth_date_informed_precision,
infmauth_date_informed,
infmauth_date_created,
infmauth_contact_type,
infmauth_complete_flag,
infmauth_company_remarks_language,
infmauth_company_remarks,
infmauth_comments,
infmauth_clinical_trial_notificatn,
infmauth_clinical_drug_code,
infmauth_case_sum_and_rep_comments,
infmauth_cancellation_flag_de_ml,
infmauth_cancellation_flag,
infmauth_authority_resp_date,
infmauth_authority_partner_id,
infmauth_authority_or_company_no,
infmauth_authority_name_de_ml,
infmauth_authority_name,
infmauth_auth_no_comp_no_enable,
infmauth_aer_no,
infmauth_acknowledgement_received_date,
infmauth_acknowledgement_number,
infmauth_ack_received)
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
integration_id ,stmsg_xml_doc_config_name,
stmsg_wf_status,
stmsg_wf_message_state,
stmsg_wf_completion_flag,
stmsg_wf_activity_name,
stmsg_version_no,
stmsg_uspi_labelling,
stmsg_user_submitted_date,
stmsg_user_modified,
stmsg_user_last_modified,
stmsg_user_created,
stmsg_trade_name,
stmsg_to_be_synchronize,
stmsg_to_be_synch_sub_status,
stmsg_to_be_synch_due_date,
stmsg_to_be_synch_cm,
stmsg_timeframe,
stmsg_time_line_day,
stmsg_suspect_product,
stmsg_support_docs_merged,
stmsg_submitted_by,
stmsg_submit_notes,
stmsg_submission_unit_name,
stmsg_submission_unit,
stmsg_submission_status_de_ml,
stmsg_submission_status,
stmsg_submission_state,
stmsg_submission_query_name,
stmsg_submission_query_desc,
stmsg_submission_due_date,
stmsg_submission_date,
stmsg_submission_condition_type,
stmsg_submission_attempt_count,
stmsg_sub_due_date_jpn,
stmsg_sub_compliance,
stmsg_study_type,
stmsg_study_design,
stmsg_status,
stmsg_spr_id,
stmsg_special_project_names,
stmsg_source_of_submission,
stmsg_single_email_submission,
stmsg_seriousness,
stmsg_serious_reaction,
stmsg_seq_lateness_info,
stmsg_seq_infm_auth,
stmsg_sender_unit,
stmsg_sender_interchange_id,
stmsg_sender_email_id,
stmsg_sender_comments,
stmsg_safety_report_id,
stmsg_review_lit_doc,
stmsg_retrieve_comment,
stmsg_resubmit_flag,
stmsg_reportercountry_nf,
stmsg_reporter_country,
stmsg_reporter_comments,
stmsg_reporter_causality,
stmsg_report_type,
stmsg_report_size,
stmsg_report_name,
stmsg_report_doc_type,
stmsg_report_ack,
stmsg_remove_reason_comment,
stmsg_remove_reason_code,
stmsg_regulatory_report_id,
stmsg_regional_state_nf,
stmsg_regional_state,
stmsg_regenerated_doc_id,
stmsg_reg_clock_start_date_jpn,
stmsg_record_owner_unit_name,
stmsg_record_owner_unit,
stmsg_record_id,
stmsg_recipient_sender_rec_id,
stmsg_recipient_receiver_rec_id,
stmsg_recepient_country_de_ml,
stmsg_recepient_country,
stmsg_receiver_unit,
stmsg_receiver_interchange_id,
stmsg_receipt_no,
stmsg_receipent_sender_type,
stmsg_receipent_receiver_type,
stmsg_read_unread_correspondence,
stmsg_reactions_available,
stmsg_r3_xml_size,
stmsg_qbe_comments,
stmsg_protocol_no_nf,
stmsg_protocol_no,
stmsg_prolonged_hospitalization,
stmsg_products_available,
stmsg_product_selected_level,
stmsg_priority,
stmsg_primary_source_country,
stmsg_primary_reporter,
stmsg_previous_submission_status,
stmsg_patientinitial_nf,
stmsg_patient_id,
stmsg_partner_name,
stmsg_partner_id,
stmsg_parent_opm_rec_id,
stmsg_owner,
stmsg_override_reason,
stmsg_override_due_dt,
stmsg_overall_due_date,
stmsg_outbound_ref4,
stmsg_outbound_ref3,
stmsg_outbound_ref2,
stmsg_outbound_ref1,
stmsg_other_safety_ref_ver,
stmsg_other_safety_ref_no,
stmsg_other_ref_date,
stmsg_oth_med_imp_condition,
stmsg_notes_flag,
stmsg_next_transition_note,
stmsg_next_transition,
stmsg_narrative_include_clinical,
stmsg_mhlw_report_type_de_ml,
stmsg_mhlw_report_type,
stmsg_mfr_number,
stmsg_message_state,
stmsg_message_no,
stmsg_medium_de_ml,
stmsg_medium,
stmsg_medically_confirmed,
stmsg_mdn_received,
stmsg_mdn_date,
stmsg_manual_upload_file_name,
stmsg_manual_upload,
stmsg_manual_ssd,
stmsg_local_version_no,
stmsg_literature,
stmsg_life_threatening,
stmsg_license_type,
stmsg_lde_lock,
stmsg_lde_language,
stmsg_lde_approved,
stmsg_latest_receive_date_jpn,
stmsg_latest_receipt_date,
stmsg_lateness_resp_unit_name,
stmsg_lateness_reason_comment,
stmsg_lateness_reason_code,
stmsg_late_by,
stmsg_last_user_modified,
stmsg_last_transition,
stmsg_last_alert_notified_time,
stmsg_language_code,
stmsg_is_expedited,
stmsg_is_due_dt_recalctd,
stmsg_initial_aer_id,
stmsg_informing_unit_name,
stmsg_informing_unit,
stmsg_include_r3_in_r2,
stmsg_identification_no,
stmsg_icsr_rule_id,
stmsg_ib_labelling,
stmsg_hp_confirmed,
stmsg_format_version,
stmsg_format_type,
stmsg_format_country,
stmsg_fl_submit_trans,
stmsg_fl_overall_lateness,
stmsg_fl_multi_reporting,
stmsg_fl_data_privacy,
stmsg_fk_lsip_rec_id,
stmsg_fk_lp_sender_rec_id,
stmsg_fk_lp_receiver_rec_id,
stmsg_fk_lms_rec_id,
stmsg_fk_ldm_rec_id,
stmsg_fk_ddc_contact_id,
stmsg_fk_awa_rec_id,
stmsg_fk_aw_rec_id,
stmsg_fk_ap_rec_id,
stmsg_fax_template_rec_id,
stmsg_fax_number,
stmsg_fax_batch_id,
stmsg_failed_date,
stmsg_external_app_wfi_id,
stmsg_external_app_cm_seq_id,
stmsg_ext_sourced_case_no,
stmsg_eu_spc_labelling,
stmsg_esm_communication_status,
stmsg_error_message,
stmsg_email_template_rec_id,
stmsg_email_subject,
stmsg_email_sent,
stmsg_email_retry_count,
stmsg_email_message,
stmsg_email_address,
stmsg_elastic_indexed,
stmsg_e2b_msg_queue_status,
stmsg_e2b_message_type,
stmsg_e2b_dtd_version,
stmsg_duplicate_numb,
stmsg_duplicate_ack,
stmsg_downgrade_status,
stmsg_doctype_name,
stmsg_doctype_code,
stmsg_do_not_validate_and_extract,
stmsg_distribution_unit_name,
stmsg_dist_rule_anchor_rec_id,
stmsg_dist_format_rec_id,
stmsg_dist_aer_info_id,
stmsg_delivery_notification_status,
stmsg_death,
stmsg_date_xmit,
stmsg_date_modified,
stmsg_date_created,
stmsg_data_privacy,
stmsg_data_exclusion,
stmsg_created_year,
stmsg_cover_letter_type,
stmsg_cover_letter_template_rec_id,
stmsg_cover_letter_size,
stmsg_cover_letter_name,
stmsg_cover_letter_holder,
stmsg_cover_letter_doc_id,
stmsg_cover_letter_desc,
stmsg_correspondence_seq,
stmsg_correspondence_flag,
stmsg_core_labelling,
stmsg_copied_submission,
stmsg_copied_case_seq,
stmsg_contact_type,
stmsg_consider_similar_product,
stmsg_company_unit_name,
stmsg_company_unit,
stmsg_company_numb,
stmsg_company_causality,
stmsg_comments,
stmsg_child_wf_id,
stmsg_cc_email_address,
stmsg_causality_assessment,
stmsg_case_version,
stmsg_case_type,
stmsg_case_susar,
stmsg_case_significance,
stmsg_case_report_doc_id,
stmsg_case_nullified,
stmsg_case_level_expectedness,
stmsg_case_due_date,
stmsg_case_country,
stmsg_case_blinded,
stmsg_case_archived,
stmsg_blinded_report_type,
stmsg_batch_queue_id,
stmsg_batch_lde_approved,
stmsg_batch_id,
stmsg_batch_export,
stmsg_batch_doc_id,
stmsg_auto_complete_first_activity,
stmsg_authority_partner_id,
stmsg_authority_numb,
stmsg_att_mdn_ack_received,
stmsg_att_icsr_doc_sent,
stmsg_att_ack_code,
stmsg_assigned_to,
stmsg_assign_to,
stmsg_ari_rec_id,
stmsg_archived_date,
stmsg_archive_status,
stmsg_archive_process_date,
stmsg_app_sender_comments,
stmsg_app_req_id,
stmsg_app_nar_include_clinical,
stmsg_app_message_type,
stmsg_app_lde_language,
stmsg_app_error_message,
stmsg_app_distribution_status,
stmsg_app_distributed_date,
stmsg_app_date_distributed,
stmsg_alert_sent,
stmsg_aim_rec_id,
stmsg_agx_aoip_reporting_status,
stmsg_agx_aoip_reference_number,
stmsg_agx_aoip_reference_date,
stmsg_agx_aoip_reason_notsubmitted,
stmsg_agx_aoip_followup_number,
stmsg_agx_aoip_date_informed,
stmsg_agx_aoip_authority,
stmsg_agx_aoip_acknowledgement_num,
stmsg_agx_aoip_acknowledgement_date,
stmsg_affiliate_submission_due_date,
stmsg_aer_receipt_date,
stmsg_aer_processing_unit_rec_id,
stmsg_aer_no,
stmsg_aer_id,
stmsg_aer_company_unit_rec_id,
stmsg_aer_company_unit_name,
stmsg_add_narratives_overflow,
stmsg_activity_due_date,
stmsg_activity_arrival_date,
stmsg_ack_status,
stmsg_ack_received,
stmsg_ack_date,
infmauth_version_no,
infmauth_user_modified,
infmauth_user_id,
infmauth_user_created,
infmauth_uf_dist_report_sent_to_mfr,
infmauth_uf_dist_report_sent_to_fda,
infmauth_uf_dist_dat_sent_to_mfr_prec,
infmauth_uf_dist_dat_sent_to_fda_prec,
infmauth_type_of_report_de_ml,
infmauth_type_of_report,
infmauth_type_of_ack_received_de_ml,
infmauth_type_of_ack_received,
infmauth_targeted_disease,
infmauth_suspect_product_rec_id,
infmauth_suspect_product,
infmauth_study_phase,
infmauth_spr_id,
infmauth_source_of_ia,
infmauth_safety_report_version_no,
infmauth_safety_report_id_enable,
infmauth_safety_report_id,
infmauth_reporting_status_de_ml,
infmauth_reporting_status,
infmauth_reporter_comments_language,
infmauth_reporter_comments,
infmauth_report_withdrawal_reason,
infmauth_report_sent_date_to_mfr,
infmauth_report_sent_date_to_fda,
infmauth_report_medium_de_ml,
infmauth_report_medium,
infmauth_report_format_de_ml,
infmauth_report_format,
infmauth_report_for_nullifn_or_amend,
infmauth_report_follow_up_no,
infmauth_report_date,
infmauth_rep_informed_auth_directly,
infmauth_reg_clock_start_dt_comment,
infmauth_reference_number,
infmauth_reference_no_received_date,
infmauth_record_id,
infmauth_receipt_no,
infmauth_reason_notsubmitted_de_ml,
infmauth_reason_notsubmitted,
infmauth_reason_for_nullifn_or_amend,
infmauth_reason_for_incomplete,
infmauth_reason_for_cancellation,
infmauth_product_rank_order_json,
infmauth_pmda_report_id_no,
infmauth_patient_under_treatment,
infmauth_other_safety_ref_ver,
infmauth_other_safety_ref_no,
infmauth_other_ref_date,
infmauth_other_comments_jpn,
infmauth_other_comments,
infmauth_nullify_reason_details,
infmauth_new_drug_classification,
infmauth_native_language,
infmauth_mhlw_report_type_de_ml,
infmauth_mhlw_report_type,
infmauth_mhlw_regenerative_report_type,
infmauth_mhlw_device_rep_year,
infmauth_mhlw_device_rep_type,
infmauth_mhlw_device_prev_report_id,
infmauth_mhlw_device_prev_rep_year,
infmauth_mdn_received,
infmauth_locally_expedited_nf,
infmauth_locally_expedited,
infmauth_local_criteria_report_type,
infmauth_linked_report_id,
infmauth_jpn_counter_measure,
infmauth_immediate_report_flag,
infmauth_future_approach,
infmauth_followup_type_correction,
infmauth_followup_type_additional_info,
infmauth_followup_to_device_eval,
infmauth_followup_response_to_auth_req,
infmauth_follow_up_type,
infmauth_fk_lsm_rec_id,
infmauth_final_report,
infmauth_expected_date_of_next_report,
infmauth_event_description_language,
infmauth_event_description,
infmauth_e2b_message_type,
infmauth_dist_info_id,
infmauth_decision_date,
infmauth_date_modified,
infmauth_date_informed_to_distributor,
infmauth_date_informed_precision,
infmauth_date_informed,
infmauth_date_created,
infmauth_contact_type,
infmauth_complete_flag,
infmauth_company_remarks_language,
infmauth_company_remarks,
infmauth_comments,
infmauth_clinical_trial_notificatn,
infmauth_clinical_drug_code,
infmauth_case_sum_and_rep_comments,
infmauth_cancellation_flag_de_ml,
infmauth_cancellation_flag,
infmauth_authority_resp_date,
infmauth_authority_partner_id,
infmauth_authority_or_company_no,
infmauth_authority_name_de_ml,
infmauth_authority_name,
infmauth_auth_no_comp_no_enable,
infmauth_aer_no,
infmauth_acknowledgement_received_date,
infmauth_acknowledgement_number,
infmauth_ack_received
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP 
WHERE TO_DATE(LS_DB_MESSAGE_INFM_AUTH.PROCESSING_DT) < TO_DATE(LS_DB_MESSAGE_INFM_AUTH_TMP.PROCESSING_DT)
AND LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID
AND LS_DB_MESSAGE_INFM_AUTH.stmsg_RECORD_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_RECORD_ID
AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_MESSAGE_INFM_AUTH.infmauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_MESSAGE_INFM_AUTH.stmsg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_MESSAGE_INFM_AUTH_TMP.infmauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_MESSAGE_INFM_AUTH_TMP.stmsg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH TGT
WHERE  ( infmauth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_informed_authority') OR stmsg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_message')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( infmauth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_informed_authority') OR stmsg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_INFM_AUTH_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_message')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_MESSAGE_INFM_AUTH'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_MESSAGE_INFM_AUTH Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MESSAGE_INFM_AUTH'),
                'LSDB','Case','LS_DB_MESSAGE_INFM_AUTH',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_MESSAGE_INFM_AUTH not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
