
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_DISTRIBUTION_FORMAT()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DISTRIBUTION_FORMAT'),
	'LSRA','Case','LS_DB_DISTRIBUTION_FORMAT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DISTRIBUTION_FORMAT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DISTRIBUTION_FORMAT'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DISTRIBUTION_FORMAT'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  As select RECORD_ID,'lsmv_distribution_format' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_format WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_distribution_unit' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_unit WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_format WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_DISTRIBUTION_UNIT record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_format WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DISTRIBUTION_FORMAT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_distribution_format_SUBSET AS 
(
select * from 
    (SELECT  
    active  distfmt_active,allow_batch_export  distfmt_allow_batch_export,allow_email_batch_export  distfmt_allow_email_batch_export,archived  distfmt_archived,auto_redistribute  distfmt_auto_redistribute,blinded_report  distfmt_blinded_report,cc_email_id  distfmt_cc_email_id,data_exclusion  distfmt_data_exclusion,data_privacy_param_map  distfmt_data_privacy_param_map,data_privacy_rule_name  distfmt_data_privacy_rule_name,data_rule_param_map  distfmt_data_rule_param_map,date_created  distfmt_date_created,date_modified  distfmt_date_modified,description  distfmt_description,display_name  distfmt_display_name,drop_olt  distfmt_drop_olt,dtd_schema  distfmt_dtd_schema,dtd_type  distfmt_dtd_type,exclude_data_rule  distfmt_exclude_data_rule,exclude_pivot_rule  distfmt_exclude_pivot_rule,expedited_timeline  distfmt_expedited_timeline,export_term_id  distfmt_export_term_id,fk_cover_letter_template  distfmt_fk_cover_letter_template,fk_data_privacy_rule  distfmt_fk_data_privacy_rule,fk_data_rule  distfmt_fk_data_rule,fk_distribution_unit  distfmt_fk_distribution_unit,fk_email_template  distfmt_fk_email_template,fk_fax_template  distfmt_fk_fax_template,fk_follow_up_no_rule  distfmt_fk_follow_up_no_rule,fk_pivot_rule  distfmt_fk_pivot_rule,fk_receiver_contact  distfmt_fk_receiver_contact,fk_sender_contact  distfmt_fk_sender_contact,fk_submission_wf_name  distfmt_fk_submission_wf_name,fk_submission_workflow  distfmt_fk_submission_workflow,format_type  distfmt_format_type,generate_xml  distfmt_generate_xml,icsr_rule_id  distfmt_icsr_rule_id,icsr_rule_name  distfmt_icsr_rule_name,import_comments  distfmt_import_comments,inc_lit_doc_in_submission  distfmt_inc_lit_doc_in_submission,include_date_of_report  distfmt_include_date_of_report,include_r3_data_in_r2  distfmt_include_r3_data_in_r2,include_unblinded_statement  distfmt_include_unblinded_statement,local_data_entry_req  distfmt_local_data_entry_req,medium  distfmt_medium,medium_details  distfmt_medium_details,message_type  distfmt_message_type,multi_report  distfmt_multi_report,olt_significance  distfmt_olt_significance,param_map  distfmt_param_map,privacy  distfmt_privacy,record_id  distfmt_record_id,reporter_cmt_nrt_lang  distfmt_reporter_cmt_nrt_lang,route_via_submission_module  distfmt_route_via_submission_module,sbt_if_pre_ver_is_distributed  distfmt_sbt_if_pre_ver_is_distributed,send_final_report  distfmt_send_final_report,send_olt  distfmt_send_olt,sender_contact_cu_partner_name  distfmt_sender_contact_cu_partner_name,sender_contact_cu_rec_id  distfmt_sender_contact_cu_rec_id,similar_products  distfmt_similar_products,spr_id  distfmt_spr_id,trusted_partner  distfmt_trusted_partner,user_created  distfmt_user_created,user_modified  distfmt_user_modified,version  distfmt_version,xml_config_name  distfmt_xml_config_name,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_format
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_DISTRIBUTION_UNIT IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_format')
  ) where REC_RANK=1 )
  , lsmv_distribution_unit_SUBSET AS 
(
select * from 
    (SELECT  
    access_partners  distunit_access_partners,active  distunit_active,allow_icsr_attachments  distunit_allow_icsr_attachments,archived  distunit_archived,auto_translation  distunit_auto_translation,causality_access  distunit_causality_access,cc_email_id  distunit_cc_email_id,correspondence_medium  distunit_correspondence_medium,correspondence_medium_details  distunit_correspondence_medium_details,date_created  distunit_date_created,date_modified  distunit_date_modified,description  distunit_description,deviated  distunit_deviated,deviated_param_map  distunit_deviated_param_map,distribution_unit_name  distunit_distribution_unit_name,due_date_calculation_factor  distunit_due_date_calculation_factor,edit_saftey_report  distunit_edit_saftey_report,enable_back_reporting  distunit_enable_back_reporting,event_access  distunit_event_access,event_description_access  distunit_event_description_access,exclude_deviated_rule  distunit_exclude_deviated_rule,exclude_pivot_rule  distunit_exclude_pivot_rule,expedited_timeline  distunit_expedited_timeline,fk_account  distunit_fk_account,fk_company_unit  distunit_fk_company_unit,fk_deviated_rule  distunit_fk_deviated_rule,fk_deviated_workflow  distunit_fk_deviated_workflow,fk_deviated_workflow_name  distunit_fk_deviated_workflow_name,fk_mail_server  distunit_fk_mail_server,fk_pivot_rule  distunit_fk_pivot_rule,fk_receiver_contact  distunit_fk_receiver_contact,fk_sender_contact  distunit_fk_sender_contact,fk_submission_wf_name  distunit_fk_submission_wf_name,fk_submission_workflow  distunit_fk_submission_workflow,import_comments  distunit_import_comments,is_account  distunit_is_account,is_company_unit  distunit_is_company_unit,local_approval_access  distunit_local_approval_access,local_labelling_access  distunit_local_labelling_access,negative_nullification_for_ack  distunit_negative_nullification_for_ack,param_map  distunit_param_map,recal_affiliate_submission  distunit_recal_affiliate_submission,record_id  distunit_record_id,reply_to_email_address  distunit_reply_to_email_address,reporter_comments_access  distunit_reporter_comments_access,selected_language  distunit_selected_language,send_olt  distunit_send_olt,sender_comments_access  distunit_sender_comments_access,sender_contact_cu_partner_id  distunit_sender_contact_cu_partner_id,sender_contact_cu_rec_id  distunit_sender_contact_cu_rec_id,spain_state_access  distunit_spain_state_access,spr_id  distunit_spr_id,store_local_data_in_lsmv  distunit_store_local_data_in_lsmv,summary_reporter_cmt_access  distunit_summary_reporter_cmt_access,trusted_partner  distunit_trusted_partner,user_created  distunit_user_created,user_modified  distunit_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_distribution_unit
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_unit')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_distribution_format_SUBSET.distfmt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_distribution_unit_SUBSET.distunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_distribution_unit_SUBSET.distunit_USER_MODIFIED USER_MODIFIED,lsmv_distribution_unit_SUBSET.distunit_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_distribution_unit_SUBSET.distunit_USER_CREATED CREATED_BY,lsmv_distribution_unit_SUBSET.distunit_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_distribution_unit_SUBSET.distunit_user_modified  ,lsmv_distribution_unit_SUBSET.distunit_user_created  ,lsmv_distribution_unit_SUBSET.distunit_trusted_partner  ,lsmv_distribution_unit_SUBSET.distunit_summary_reporter_cmt_access  ,lsmv_distribution_unit_SUBSET.distunit_store_local_data_in_lsmv  ,lsmv_distribution_unit_SUBSET.distunit_spr_id  ,lsmv_distribution_unit_SUBSET.distunit_spain_state_access  ,lsmv_distribution_unit_SUBSET.distunit_sender_contact_cu_rec_id  ,lsmv_distribution_unit_SUBSET.distunit_sender_contact_cu_partner_id  ,lsmv_distribution_unit_SUBSET.distunit_sender_comments_access  ,lsmv_distribution_unit_SUBSET.distunit_send_olt  ,lsmv_distribution_unit_SUBSET.distunit_selected_language  ,lsmv_distribution_unit_SUBSET.distunit_reporter_comments_access  ,lsmv_distribution_unit_SUBSET.distunit_reply_to_email_address  ,lsmv_distribution_unit_SUBSET.distunit_record_id  ,lsmv_distribution_unit_SUBSET.distunit_recal_affiliate_submission  ,lsmv_distribution_unit_SUBSET.distunit_param_map  ,lsmv_distribution_unit_SUBSET.distunit_negative_nullification_for_ack  ,lsmv_distribution_unit_SUBSET.distunit_local_labelling_access  ,lsmv_distribution_unit_SUBSET.distunit_local_approval_access  ,lsmv_distribution_unit_SUBSET.distunit_is_company_unit  ,lsmv_distribution_unit_SUBSET.distunit_is_account  ,lsmv_distribution_unit_SUBSET.distunit_import_comments  ,lsmv_distribution_unit_SUBSET.distunit_fk_submission_workflow  ,lsmv_distribution_unit_SUBSET.distunit_fk_submission_wf_name  ,lsmv_distribution_unit_SUBSET.distunit_fk_sender_contact  ,lsmv_distribution_unit_SUBSET.distunit_fk_receiver_contact  ,lsmv_distribution_unit_SUBSET.distunit_fk_pivot_rule  ,lsmv_distribution_unit_SUBSET.distunit_fk_mail_server  ,lsmv_distribution_unit_SUBSET.distunit_fk_deviated_workflow_name  ,lsmv_distribution_unit_SUBSET.distunit_fk_deviated_workflow  ,lsmv_distribution_unit_SUBSET.distunit_fk_deviated_rule  ,lsmv_distribution_unit_SUBSET.distunit_fk_company_unit  ,lsmv_distribution_unit_SUBSET.distunit_fk_account  ,lsmv_distribution_unit_SUBSET.distunit_expedited_timeline  ,lsmv_distribution_unit_SUBSET.distunit_exclude_pivot_rule  ,lsmv_distribution_unit_SUBSET.distunit_exclude_deviated_rule  ,lsmv_distribution_unit_SUBSET.distunit_event_description_access  ,lsmv_distribution_unit_SUBSET.distunit_event_access  ,lsmv_distribution_unit_SUBSET.distunit_enable_back_reporting  ,lsmv_distribution_unit_SUBSET.distunit_edit_saftey_report  ,lsmv_distribution_unit_SUBSET.distunit_due_date_calculation_factor  ,lsmv_distribution_unit_SUBSET.distunit_distribution_unit_name  ,lsmv_distribution_unit_SUBSET.distunit_deviated_param_map  ,lsmv_distribution_unit_SUBSET.distunit_deviated  ,lsmv_distribution_unit_SUBSET.distunit_description  ,lsmv_distribution_unit_SUBSET.distunit_date_modified  ,lsmv_distribution_unit_SUBSET.distunit_date_created  ,lsmv_distribution_unit_SUBSET.distunit_correspondence_medium_details  ,lsmv_distribution_unit_SUBSET.distunit_correspondence_medium  ,lsmv_distribution_unit_SUBSET.distunit_cc_email_id  ,lsmv_distribution_unit_SUBSET.distunit_causality_access  ,lsmv_distribution_unit_SUBSET.distunit_auto_translation  ,lsmv_distribution_unit_SUBSET.distunit_archived  ,lsmv_distribution_unit_SUBSET.distunit_allow_icsr_attachments  ,lsmv_distribution_unit_SUBSET.distunit_active  ,lsmv_distribution_unit_SUBSET.distunit_access_partners  ,lsmv_distribution_format_SUBSET.distfmt_xml_config_name  ,lsmv_distribution_format_SUBSET.distfmt_version  ,lsmv_distribution_format_SUBSET.distfmt_user_modified  ,lsmv_distribution_format_SUBSET.distfmt_user_created  ,lsmv_distribution_format_SUBSET.distfmt_trusted_partner  ,lsmv_distribution_format_SUBSET.distfmt_spr_id  ,lsmv_distribution_format_SUBSET.distfmt_similar_products  ,lsmv_distribution_format_SUBSET.distfmt_sender_contact_cu_rec_id  ,lsmv_distribution_format_SUBSET.distfmt_sender_contact_cu_partner_name  ,lsmv_distribution_format_SUBSET.distfmt_send_olt  ,lsmv_distribution_format_SUBSET.distfmt_send_final_report  ,lsmv_distribution_format_SUBSET.distfmt_sbt_if_pre_ver_is_distributed  ,lsmv_distribution_format_SUBSET.distfmt_route_via_submission_module  ,lsmv_distribution_format_SUBSET.distfmt_reporter_cmt_nrt_lang  ,lsmv_distribution_format_SUBSET.distfmt_record_id  ,lsmv_distribution_format_SUBSET.distfmt_privacy  ,lsmv_distribution_format_SUBSET.distfmt_param_map  ,lsmv_distribution_format_SUBSET.distfmt_olt_significance  ,lsmv_distribution_format_SUBSET.distfmt_multi_report  ,lsmv_distribution_format_SUBSET.distfmt_message_type  ,lsmv_distribution_format_SUBSET.distfmt_medium_details  ,lsmv_distribution_format_SUBSET.distfmt_medium  ,lsmv_distribution_format_SUBSET.distfmt_local_data_entry_req  ,lsmv_distribution_format_SUBSET.distfmt_include_unblinded_statement  ,lsmv_distribution_format_SUBSET.distfmt_include_r3_data_in_r2  ,lsmv_distribution_format_SUBSET.distfmt_include_date_of_report  ,lsmv_distribution_format_SUBSET.distfmt_inc_lit_doc_in_submission  ,lsmv_distribution_format_SUBSET.distfmt_import_comments  ,lsmv_distribution_format_SUBSET.distfmt_icsr_rule_name  ,lsmv_distribution_format_SUBSET.distfmt_icsr_rule_id  ,lsmv_distribution_format_SUBSET.distfmt_generate_xml  ,lsmv_distribution_format_SUBSET.distfmt_format_type  ,lsmv_distribution_format_SUBSET.distfmt_fk_submission_workflow  ,lsmv_distribution_format_SUBSET.distfmt_fk_submission_wf_name  ,lsmv_distribution_format_SUBSET.distfmt_fk_sender_contact  ,lsmv_distribution_format_SUBSET.distfmt_fk_receiver_contact  ,lsmv_distribution_format_SUBSET.distfmt_fk_pivot_rule  ,lsmv_distribution_format_SUBSET.distfmt_fk_follow_up_no_rule  ,lsmv_distribution_format_SUBSET.distfmt_fk_fax_template  ,lsmv_distribution_format_SUBSET.distfmt_fk_email_template  ,lsmv_distribution_format_SUBSET.distfmt_fk_distribution_unit  ,lsmv_distribution_format_SUBSET.distfmt_fk_data_rule  ,lsmv_distribution_format_SUBSET.distfmt_fk_data_privacy_rule  ,lsmv_distribution_format_SUBSET.distfmt_fk_cover_letter_template  ,lsmv_distribution_format_SUBSET.distfmt_export_term_id  ,lsmv_distribution_format_SUBSET.distfmt_expedited_timeline  ,lsmv_distribution_format_SUBSET.distfmt_exclude_pivot_rule  ,lsmv_distribution_format_SUBSET.distfmt_exclude_data_rule  ,lsmv_distribution_format_SUBSET.distfmt_dtd_type  ,lsmv_distribution_format_SUBSET.distfmt_dtd_schema  ,lsmv_distribution_format_SUBSET.distfmt_drop_olt  ,lsmv_distribution_format_SUBSET.distfmt_display_name  ,lsmv_distribution_format_SUBSET.distfmt_description  ,lsmv_distribution_format_SUBSET.distfmt_date_modified  ,lsmv_distribution_format_SUBSET.distfmt_date_created  ,lsmv_distribution_format_SUBSET.distfmt_data_rule_param_map  ,lsmv_distribution_format_SUBSET.distfmt_data_privacy_rule_name  ,lsmv_distribution_format_SUBSET.distfmt_data_privacy_param_map  ,lsmv_distribution_format_SUBSET.distfmt_data_exclusion  ,lsmv_distribution_format_SUBSET.distfmt_cc_email_id  ,lsmv_distribution_format_SUBSET.distfmt_blinded_report  ,lsmv_distribution_format_SUBSET.distfmt_auto_redistribute  ,lsmv_distribution_format_SUBSET.distfmt_archived  ,lsmv_distribution_format_SUBSET.distfmt_allow_email_batch_export  ,lsmv_distribution_format_SUBSET.distfmt_allow_batch_export  ,lsmv_distribution_format_SUBSET.distfmt_active ,CONCAT(NVL(lsmv_distribution_format_SUBSET.distfmt_RECORD_ID,-1),'||',NVL(lsmv_distribution_unit_SUBSET.distunit_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_distribution_unit_SUBSET  LEFT JOIN lsmv_distribution_format_SUBSET ON lsmv_distribution_unit_SUBSET.distunit_record_id=lsmv_distribution_format_SUBSET.distfmt_FK_DISTRIBUTION_UNIT
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP)
where target_table_name='LS_DB_DISTRIBUTION_FORMAT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DISTRIBUTION_FORMAT'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT   
SET LS_DB_DISTRIBUTION_FORMAT.distunit_user_modified = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_user_modified,LS_DB_DISTRIBUTION_FORMAT.distunit_user_created = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_user_created,LS_DB_DISTRIBUTION_FORMAT.distunit_trusted_partner = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_trusted_partner,LS_DB_DISTRIBUTION_FORMAT.distunit_summary_reporter_cmt_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_summary_reporter_cmt_access,LS_DB_DISTRIBUTION_FORMAT.distunit_store_local_data_in_lsmv = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_store_local_data_in_lsmv,LS_DB_DISTRIBUTION_FORMAT.distunit_spr_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_spr_id,LS_DB_DISTRIBUTION_FORMAT.distunit_spain_state_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_spain_state_access,LS_DB_DISTRIBUTION_FORMAT.distunit_sender_contact_cu_rec_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_sender_contact_cu_rec_id,LS_DB_DISTRIBUTION_FORMAT.distunit_sender_contact_cu_partner_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_sender_contact_cu_partner_id,LS_DB_DISTRIBUTION_FORMAT.distunit_sender_comments_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_sender_comments_access,LS_DB_DISTRIBUTION_FORMAT.distunit_send_olt = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_send_olt,LS_DB_DISTRIBUTION_FORMAT.distunit_selected_language = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_selected_language,LS_DB_DISTRIBUTION_FORMAT.distunit_reporter_comments_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_reporter_comments_access,LS_DB_DISTRIBUTION_FORMAT.distunit_reply_to_email_address = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_reply_to_email_address,LS_DB_DISTRIBUTION_FORMAT.distunit_record_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_record_id,LS_DB_DISTRIBUTION_FORMAT.distunit_recal_affiliate_submission = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_recal_affiliate_submission,LS_DB_DISTRIBUTION_FORMAT.distunit_param_map = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_param_map,LS_DB_DISTRIBUTION_FORMAT.distunit_negative_nullification_for_ack = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_negative_nullification_for_ack,LS_DB_DISTRIBUTION_FORMAT.distunit_local_labelling_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_local_labelling_access,LS_DB_DISTRIBUTION_FORMAT.distunit_local_approval_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_local_approval_access,LS_DB_DISTRIBUTION_FORMAT.distunit_is_company_unit = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_is_company_unit,LS_DB_DISTRIBUTION_FORMAT.distunit_is_account = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_is_account,LS_DB_DISTRIBUTION_FORMAT.distunit_import_comments = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_import_comments,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_submission_workflow = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_submission_workflow,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_submission_wf_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_submission_wf_name,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_sender_contact = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_sender_contact,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_receiver_contact = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_receiver_contact,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_pivot_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_pivot_rule,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_mail_server = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_mail_server,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_deviated_workflow_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_deviated_workflow_name,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_deviated_workflow = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_deviated_workflow,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_deviated_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_deviated_rule,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_company_unit = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_company_unit,LS_DB_DISTRIBUTION_FORMAT.distunit_fk_account = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_fk_account,LS_DB_DISTRIBUTION_FORMAT.distunit_expedited_timeline = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_expedited_timeline,LS_DB_DISTRIBUTION_FORMAT.distunit_exclude_pivot_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_exclude_pivot_rule,LS_DB_DISTRIBUTION_FORMAT.distunit_exclude_deviated_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_exclude_deviated_rule,LS_DB_DISTRIBUTION_FORMAT.distunit_event_description_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_event_description_access,LS_DB_DISTRIBUTION_FORMAT.distunit_event_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_event_access,LS_DB_DISTRIBUTION_FORMAT.distunit_enable_back_reporting = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_enable_back_reporting,LS_DB_DISTRIBUTION_FORMAT.distunit_edit_saftey_report = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_edit_saftey_report,LS_DB_DISTRIBUTION_FORMAT.distunit_due_date_calculation_factor = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_due_date_calculation_factor,LS_DB_DISTRIBUTION_FORMAT.distunit_distribution_unit_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_distribution_unit_name,LS_DB_DISTRIBUTION_FORMAT.distunit_deviated_param_map = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_deviated_param_map,LS_DB_DISTRIBUTION_FORMAT.distunit_deviated = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_deviated,LS_DB_DISTRIBUTION_FORMAT.distunit_description = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_description,LS_DB_DISTRIBUTION_FORMAT.distunit_date_modified = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_date_modified,LS_DB_DISTRIBUTION_FORMAT.distunit_date_created = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_date_created,LS_DB_DISTRIBUTION_FORMAT.distunit_correspondence_medium_details = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_correspondence_medium_details,LS_DB_DISTRIBUTION_FORMAT.distunit_correspondence_medium = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_correspondence_medium,LS_DB_DISTRIBUTION_FORMAT.distunit_cc_email_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_cc_email_id,LS_DB_DISTRIBUTION_FORMAT.distunit_causality_access = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_causality_access,LS_DB_DISTRIBUTION_FORMAT.distunit_auto_translation = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_auto_translation,LS_DB_DISTRIBUTION_FORMAT.distunit_archived = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_archived,LS_DB_DISTRIBUTION_FORMAT.distunit_allow_icsr_attachments = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_allow_icsr_attachments,LS_DB_DISTRIBUTION_FORMAT.distunit_active = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_active,LS_DB_DISTRIBUTION_FORMAT.distunit_access_partners = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_access_partners,LS_DB_DISTRIBUTION_FORMAT.distfmt_xml_config_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_xml_config_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_version = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_version,LS_DB_DISTRIBUTION_FORMAT.distfmt_user_modified = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_user_modified,LS_DB_DISTRIBUTION_FORMAT.distfmt_user_created = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_user_created,LS_DB_DISTRIBUTION_FORMAT.distfmt_trusted_partner = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_trusted_partner,LS_DB_DISTRIBUTION_FORMAT.distfmt_spr_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_spr_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_similar_products = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_similar_products,LS_DB_DISTRIBUTION_FORMAT.distfmt_sender_contact_cu_rec_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_sender_contact_cu_rec_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_sender_contact_cu_partner_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_sender_contact_cu_partner_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_send_olt = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_send_olt,LS_DB_DISTRIBUTION_FORMAT.distfmt_send_final_report = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_send_final_report,LS_DB_DISTRIBUTION_FORMAT.distfmt_sbt_if_pre_ver_is_distributed = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_sbt_if_pre_ver_is_distributed,LS_DB_DISTRIBUTION_FORMAT.distfmt_route_via_submission_module = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_route_via_submission_module,LS_DB_DISTRIBUTION_FORMAT.distfmt_reporter_cmt_nrt_lang = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_reporter_cmt_nrt_lang,LS_DB_DISTRIBUTION_FORMAT.distfmt_record_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_record_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_privacy = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_privacy,LS_DB_DISTRIBUTION_FORMAT.distfmt_param_map = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_param_map,LS_DB_DISTRIBUTION_FORMAT.distfmt_olt_significance = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_olt_significance,LS_DB_DISTRIBUTION_FORMAT.distfmt_multi_report = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_multi_report,LS_DB_DISTRIBUTION_FORMAT.distfmt_message_type = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_message_type,LS_DB_DISTRIBUTION_FORMAT.distfmt_medium_details = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_medium_details,LS_DB_DISTRIBUTION_FORMAT.distfmt_medium = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_medium,LS_DB_DISTRIBUTION_FORMAT.distfmt_local_data_entry_req = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_local_data_entry_req,LS_DB_DISTRIBUTION_FORMAT.distfmt_include_unblinded_statement = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_include_unblinded_statement,LS_DB_DISTRIBUTION_FORMAT.distfmt_include_r3_data_in_r2 = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_include_r3_data_in_r2,LS_DB_DISTRIBUTION_FORMAT.distfmt_include_date_of_report = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_include_date_of_report,LS_DB_DISTRIBUTION_FORMAT.distfmt_inc_lit_doc_in_submission = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_inc_lit_doc_in_submission,LS_DB_DISTRIBUTION_FORMAT.distfmt_import_comments = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_import_comments,LS_DB_DISTRIBUTION_FORMAT.distfmt_icsr_rule_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_icsr_rule_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_icsr_rule_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_icsr_rule_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_generate_xml = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_generate_xml,LS_DB_DISTRIBUTION_FORMAT.distfmt_format_type = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_format_type,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_submission_workflow = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_submission_workflow,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_submission_wf_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_submission_wf_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_sender_contact = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_sender_contact,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_receiver_contact = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_receiver_contact,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_pivot_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_pivot_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_follow_up_no_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_follow_up_no_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_fax_template = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_fax_template,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_email_template = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_email_template,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_distribution_unit = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_distribution_unit,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_data_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_data_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_data_privacy_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_data_privacy_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_fk_cover_letter_template = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_fk_cover_letter_template,LS_DB_DISTRIBUTION_FORMAT.distfmt_export_term_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_export_term_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_expedited_timeline = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_expedited_timeline,LS_DB_DISTRIBUTION_FORMAT.distfmt_exclude_pivot_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_exclude_pivot_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_exclude_data_rule = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_exclude_data_rule,LS_DB_DISTRIBUTION_FORMAT.distfmt_dtd_type = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_dtd_type,LS_DB_DISTRIBUTION_FORMAT.distfmt_dtd_schema = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_dtd_schema,LS_DB_DISTRIBUTION_FORMAT.distfmt_drop_olt = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_drop_olt,LS_DB_DISTRIBUTION_FORMAT.distfmt_display_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_display_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_description = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_description,LS_DB_DISTRIBUTION_FORMAT.distfmt_date_modified = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_date_modified,LS_DB_DISTRIBUTION_FORMAT.distfmt_date_created = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_date_created,LS_DB_DISTRIBUTION_FORMAT.distfmt_data_rule_param_map = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_data_rule_param_map,LS_DB_DISTRIBUTION_FORMAT.distfmt_data_privacy_rule_name = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_data_privacy_rule_name,LS_DB_DISTRIBUTION_FORMAT.distfmt_data_privacy_param_map = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_data_privacy_param_map,LS_DB_DISTRIBUTION_FORMAT.distfmt_data_exclusion = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_data_exclusion,LS_DB_DISTRIBUTION_FORMAT.distfmt_cc_email_id = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_cc_email_id,LS_DB_DISTRIBUTION_FORMAT.distfmt_blinded_report = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_blinded_report,LS_DB_DISTRIBUTION_FORMAT.distfmt_auto_redistribute = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_auto_redistribute,LS_DB_DISTRIBUTION_FORMAT.distfmt_archived = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_archived,LS_DB_DISTRIBUTION_FORMAT.distfmt_allow_email_batch_export = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_allow_email_batch_export,LS_DB_DISTRIBUTION_FORMAT.distfmt_allow_batch_export = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_allow_batch_export,LS_DB_DISTRIBUTION_FORMAT.distfmt_active = LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_active,
LS_DB_DISTRIBUTION_FORMAT.PROCESSING_DT = LS_DB_DISTRIBUTION_FORMAT_TMP.PROCESSING_DT,
LS_DB_DISTRIBUTION_FORMAT.user_modified  =LS_DB_DISTRIBUTION_FORMAT_TMP.user_modified     ,
LS_DB_DISTRIBUTION_FORMAT.date_modified  =LS_DB_DISTRIBUTION_FORMAT_TMP.date_modified     ,
LS_DB_DISTRIBUTION_FORMAT.expiry_date    =LS_DB_DISTRIBUTION_FORMAT_TMP.expiry_date       ,
LS_DB_DISTRIBUTION_FORMAT.created_by     =LS_DB_DISTRIBUTION_FORMAT_TMP.created_by        ,
LS_DB_DISTRIBUTION_FORMAT.created_dt     =LS_DB_DISTRIBUTION_FORMAT_TMP.created_dt        ,
LS_DB_DISTRIBUTION_FORMAT.load_ts        =LS_DB_DISTRIBUTION_FORMAT_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP 
WHERE 	LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID = LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DISTRIBUTION_FORMAT_TMP.PROCESSING_DT = LS_DB_DISTRIBUTION_FORMAT.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DISTRIBUTION_FORMAT.distfmt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DISTRIBUTION_FORMAT.distunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DISTRIBUTION_FORMAT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID ,LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP 
ON LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID=LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_RECORD_ID
AND LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID = LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID 
where LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID  is null AND LS_DB_DISTRIBUTION_FORMAT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID in (select distunit_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP )
) TMP where LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DISTRIBUTION_FORMAT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID ,LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP 
    ON LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID=LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_RECORD_ID
    AND LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID = LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID 
    where LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID  is null AND LS_DB_DISTRIBUTION_FORMAT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID in (select distunit_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,distunit_user_modified,
distunit_user_created,
distunit_trusted_partner,
distunit_summary_reporter_cmt_access,
distunit_store_local_data_in_lsmv,
distunit_spr_id,
distunit_spain_state_access,
distunit_sender_contact_cu_rec_id,
distunit_sender_contact_cu_partner_id,
distunit_sender_comments_access,
distunit_send_olt,
distunit_selected_language,
distunit_reporter_comments_access,
distunit_reply_to_email_address,
distunit_record_id,
distunit_recal_affiliate_submission,
distunit_param_map,
distunit_negative_nullification_for_ack,
distunit_local_labelling_access,
distunit_local_approval_access,
distunit_is_company_unit,
distunit_is_account,
distunit_import_comments,
distunit_fk_submission_workflow,
distunit_fk_submission_wf_name,
distunit_fk_sender_contact,
distunit_fk_receiver_contact,
distunit_fk_pivot_rule,
distunit_fk_mail_server,
distunit_fk_deviated_workflow_name,
distunit_fk_deviated_workflow,
distunit_fk_deviated_rule,
distunit_fk_company_unit,
distunit_fk_account,
distunit_expedited_timeline,
distunit_exclude_pivot_rule,
distunit_exclude_deviated_rule,
distunit_event_description_access,
distunit_event_access,
distunit_enable_back_reporting,
distunit_edit_saftey_report,
distunit_due_date_calculation_factor,
distunit_distribution_unit_name,
distunit_deviated_param_map,
distunit_deviated,
distunit_description,
distunit_date_modified,
distunit_date_created,
distunit_correspondence_medium_details,
distunit_correspondence_medium,
distunit_cc_email_id,
distunit_causality_access,
distunit_auto_translation,
distunit_archived,
distunit_allow_icsr_attachments,
distunit_active,
distunit_access_partners,
distfmt_xml_config_name,
distfmt_version,
distfmt_user_modified,
distfmt_user_created,
distfmt_trusted_partner,
distfmt_spr_id,
distfmt_similar_products,
distfmt_sender_contact_cu_rec_id,
distfmt_sender_contact_cu_partner_name,
distfmt_send_olt,
distfmt_send_final_report,
distfmt_sbt_if_pre_ver_is_distributed,
distfmt_route_via_submission_module,
distfmt_reporter_cmt_nrt_lang,
distfmt_record_id,
distfmt_privacy,
distfmt_param_map,
distfmt_olt_significance,
distfmt_multi_report,
distfmt_message_type,
distfmt_medium_details,
distfmt_medium,
distfmt_local_data_entry_req,
distfmt_include_unblinded_statement,
distfmt_include_r3_data_in_r2,
distfmt_include_date_of_report,
distfmt_inc_lit_doc_in_submission,
distfmt_import_comments,
distfmt_icsr_rule_name,
distfmt_icsr_rule_id,
distfmt_generate_xml,
distfmt_format_type,
distfmt_fk_submission_workflow,
distfmt_fk_submission_wf_name,
distfmt_fk_sender_contact,
distfmt_fk_receiver_contact,
distfmt_fk_pivot_rule,
distfmt_fk_follow_up_no_rule,
distfmt_fk_fax_template,
distfmt_fk_email_template,
distfmt_fk_distribution_unit,
distfmt_fk_data_rule,
distfmt_fk_data_privacy_rule,
distfmt_fk_cover_letter_template,
distfmt_export_term_id,
distfmt_expedited_timeline,
distfmt_exclude_pivot_rule,
distfmt_exclude_data_rule,
distfmt_dtd_type,
distfmt_dtd_schema,
distfmt_drop_olt,
distfmt_display_name,
distfmt_description,
distfmt_date_modified,
distfmt_date_created,
distfmt_data_rule_param_map,
distfmt_data_privacy_rule_name,
distfmt_data_privacy_param_map,
distfmt_data_exclusion,
distfmt_cc_email_id,
distfmt_blinded_report,
distfmt_auto_redistribute,
distfmt_archived,
distfmt_allow_email_batch_export,
distfmt_allow_batch_export,
distfmt_active)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,distunit_user_modified,
distunit_user_created,
distunit_trusted_partner,
distunit_summary_reporter_cmt_access,
distunit_store_local_data_in_lsmv,
distunit_spr_id,
distunit_spain_state_access,
distunit_sender_contact_cu_rec_id,
distunit_sender_contact_cu_partner_id,
distunit_sender_comments_access,
distunit_send_olt,
distunit_selected_language,
distunit_reporter_comments_access,
distunit_reply_to_email_address,
distunit_record_id,
distunit_recal_affiliate_submission,
distunit_param_map,
distunit_negative_nullification_for_ack,
distunit_local_labelling_access,
distunit_local_approval_access,
distunit_is_company_unit,
distunit_is_account,
distunit_import_comments,
distunit_fk_submission_workflow,
distunit_fk_submission_wf_name,
distunit_fk_sender_contact,
distunit_fk_receiver_contact,
distunit_fk_pivot_rule,
distunit_fk_mail_server,
distunit_fk_deviated_workflow_name,
distunit_fk_deviated_workflow,
distunit_fk_deviated_rule,
distunit_fk_company_unit,
distunit_fk_account,
distunit_expedited_timeline,
distunit_exclude_pivot_rule,
distunit_exclude_deviated_rule,
distunit_event_description_access,
distunit_event_access,
distunit_enable_back_reporting,
distunit_edit_saftey_report,
distunit_due_date_calculation_factor,
distunit_distribution_unit_name,
distunit_deviated_param_map,
distunit_deviated,
distunit_description,
distunit_date_modified,
distunit_date_created,
distunit_correspondence_medium_details,
distunit_correspondence_medium,
distunit_cc_email_id,
distunit_causality_access,
distunit_auto_translation,
distunit_archived,
distunit_allow_icsr_attachments,
distunit_active,
distunit_access_partners,
distfmt_xml_config_name,
distfmt_version,
distfmt_user_modified,
distfmt_user_created,
distfmt_trusted_partner,
distfmt_spr_id,
distfmt_similar_products,
distfmt_sender_contact_cu_rec_id,
distfmt_sender_contact_cu_partner_name,
distfmt_send_olt,
distfmt_send_final_report,
distfmt_sbt_if_pre_ver_is_distributed,
distfmt_route_via_submission_module,
distfmt_reporter_cmt_nrt_lang,
distfmt_record_id,
distfmt_privacy,
distfmt_param_map,
distfmt_olt_significance,
distfmt_multi_report,
distfmt_message_type,
distfmt_medium_details,
distfmt_medium,
distfmt_local_data_entry_req,
distfmt_include_unblinded_statement,
distfmt_include_r3_data_in_r2,
distfmt_include_date_of_report,
distfmt_inc_lit_doc_in_submission,
distfmt_import_comments,
distfmt_icsr_rule_name,
distfmt_icsr_rule_id,
distfmt_generate_xml,
distfmt_format_type,
distfmt_fk_submission_workflow,
distfmt_fk_submission_wf_name,
distfmt_fk_sender_contact,
distfmt_fk_receiver_contact,
distfmt_fk_pivot_rule,
distfmt_fk_follow_up_no_rule,
distfmt_fk_fax_template,
distfmt_fk_email_template,
distfmt_fk_distribution_unit,
distfmt_fk_data_rule,
distfmt_fk_data_privacy_rule,
distfmt_fk_cover_letter_template,
distfmt_export_term_id,
distfmt_expedited_timeline,
distfmt_exclude_pivot_rule,
distfmt_exclude_data_rule,
distfmt_dtd_type,
distfmt_dtd_schema,
distfmt_drop_olt,
distfmt_display_name,
distfmt_description,
distfmt_date_modified,
distfmt_date_created,
distfmt_data_rule_param_map,
distfmt_data_privacy_rule_name,
distfmt_data_privacy_param_map,
distfmt_data_exclusion,
distfmt_cc_email_id,
distfmt_blinded_report,
distfmt_auto_redistribute,
distfmt_archived,
distfmt_allow_email_batch_export,
distfmt_allow_batch_export,
distfmt_active
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT TGT
WHERE  ( distfmt_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_format') OR distunit_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_unit')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP 
WHERE 	TO_DATE(LS_DB_DISTRIBUTION_FORMAT.PROCESSING_DT) < TO_DATE(LS_DB_DISTRIBUTION_FORMAT_TMP.PROCESSING_DT)
AND LS_DB_DISTRIBUTION_FORMAT.INTEGRATION_ID = LS_DB_DISTRIBUTION_FORMAT_TMP.INTEGRATION_ID
AND LS_DB_DISTRIBUTION_FORMAT.distunit_RECORD_ID = LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_RECORD_ID
AND LS_DB_DISTRIBUTION_FORMAT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DISTRIBUTION_FORMAT.distfmt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DISTRIBUTION_FORMAT.distunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DISTRIBUTION_FORMAT_TMP.distfmt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DISTRIBUTION_FORMAT_TMP.distunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( distfmt_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_format') OR distunit_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DISTRIBUTION_FORMAT_DELETION_TMP  WHERE TABLE_NAME='lsmv_distribution_unit')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DISTRIBUTION_FORMAT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DISTRIBUTION_FORMAT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DISTRIBUTION_FORMAT'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DISTRIBUTION_FORMAT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DISTRIBUTION_FORMAT'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DISTRIBUTION_FORMAT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
