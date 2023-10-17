
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_E2B_IMPORT_QUEUE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_IMPORT_QUEUE'),
	'LSDB','Case','LS_DB_E2B_IMPORT_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_E2B_IMPORT_QUEUE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_E2B_IMPORT_QUEUE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  As select RECORD_ID,'lsmv_e2b_mes_queue' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_mes_queue WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_e2b_message_ack' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_message_ack WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_e2b_msg_rpt_queue' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_msg_rpt_queue WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_e2b_report_ack' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_report_ack WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_mes_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_mes_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_msg_rpt_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_msg_queue record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_msg_rpt_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_report_ack WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_lema_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_report_ack WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_message_ack WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_export_queue_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_message_ack WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_IMPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_e2b_mes_queue_SUBSET AS 
(
select * from 
    (SELECT  
    ack_code  e2bmsgque_ack_code,ack_imp_config  e2bmsgque_ack_imp_config,ack_to_be_processed  e2bmsgque_ack_to_be_processed,acknowledgement_generated  e2bmsgque_acknowledgement_generated,archive_status  e2bmsgque_archive_status,batch_or_single  e2bmsgque_batch_or_single,case_id  e2bmsgque_case_id,case_info_json  e2bmsgque_case_info_json,case_record_ids  e2bmsgque_case_record_ids,date_created  e2bmsgque_date_created,date_modified  e2bmsgque_date_modified,date_submitted  e2bmsgque_date_submitted,dtd_type  e2bmsgque_dtd_type,email_intake_queue_recid  e2bmsgque_email_intake_queue_recid,email_name  e2bmsgque_email_name,email_subject  e2bmsgque_email_subject,error_detail  e2bmsgque_error_detail,external_file_path  e2bmsgque_external_file_path,file_name_without_ext  e2bmsgque_file_name_without_ext,first_name  e2bmsgque_first_name,fk_node_rec_id  e2bmsgque_fk_node_rec_id,folder_path  e2bmsgque_folder_path,gateway_ack_date  e2bmsgque_gateway_ack_date,ichr_message_number  e2bmsgque_ichr_message_number,import_status  e2bmsgque_import_status,imported_e2b_xml_filedata  e2bmsgque_imported_e2b_xml_filedata,inbound_e2b_xml_encoding_type  e2bmsgque_inbound_e2b_xml_encoding_type,inbound_email_message_uid  e2bmsgque_inbound_email_message_uid,last_name  e2bmsgque_last_name,local_icsr_number  e2bmsgque_local_icsr_number,mdn_date  e2bmsgque_mdn_date,mdn_message  e2bmsgque_mdn_message,mdn_status  e2bmsgque_mdn_status,mdn_update_attempts  e2bmsgque_mdn_update_attempts,message_file_name  e2bmsgque_message_file_name,message_number  e2bmsgque_message_number,message_type  e2bmsgque_message_type,module_id  e2bmsgque_module_id,processing_status  e2bmsgque_processing_status,receiver_institution  e2bmsgque_receiver_institution,record_id  e2bmsgque_record_id,repo_docid  e2bmsgque_repo_docid,rpt_queues_filled  e2bmsgque_rpt_queues_filled,sender_institution  e2bmsgque_sender_institution,spr_id  e2bmsgque_spr_id,status  e2bmsgque_status,status_last_updated  e2bmsgque_status_last_updated,thread_name  e2bmsgque_thread_name,trans_queue_flag  e2bmsgque_trans_queue_flag,transformed_docid  e2bmsgque_transformed_docid,transmission_date  e2bmsgque_transmission_date,user_created  e2bmsgque_user_created,user_id  e2bmsgque_user_id,user_modified  e2bmsgque_user_modified,vaer_no  e2bmsgque_vaer_no,xml_source  e2bmsgque_xml_source,xml_type  e2bmsgque_xml_type,xml_uploaded_date  e2bmsgque_xml_uploaded_date,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_mes_queue
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_mes_queue')
  ) where REC_RANK=1 )
  , lsmv_e2b_msg_rpt_queue_SUBSET AS 
(
select * from 
    (SELECT  
    ack_code  e2bmsgrpt_ack_code,aer_no  e2bmsgrpt_aer_no,aer_ver_no  e2bmsgrpt_aer_ver_no,archive_status  e2bmsgrpt_archive_status,authority_numb  e2bmsgrpt_authority_numb,bulk_status  e2bmsgrpt_bulk_status,case_process_thread  e2bmsgrpt_case_process_thread,comments  e2bmsgrpt_comments,company_numb  e2bmsgrpt_company_numb,date_created  e2bmsgrpt_date_created,date_modified  e2bmsgrpt_date_modified,email_intake_queue_rpt_recid  e2bmsgrpt_email_intake_queue_rpt_recid,fk_msg_queue  e2bmsgrpt_fk_msg_queue,fk_node_rec_id  e2bmsgrpt_fk_node_rec_id,icsr_report_no  e2bmsgrpt_icsr_report_no,icsr_report_ver_no  e2bmsgrpt_icsr_report_ver_no,import_status  e2bmsgrpt_import_status,is_validation_exsits  e2bmsgrpt_is_validation_exsits,json_string  e2bmsgrpt_json_string,mhlw_device_rpt_type  e2bmsgrpt_mhlw_device_rpt_type,msg_date_created  e2bmsgrpt_msg_date_created,msg_date_created_format  e2bmsgrpt_msg_date_created_format,processing_status  e2bmsgrpt_processing_status,receipt_item_rec_id  e2bmsgrpt_receipt_item_rec_id,receipt_no  e2bmsgrpt_receipt_no,record_id  e2bmsgrpt_record_id,seq_no  e2bmsgrpt_seq_no,spr_id  e2bmsgrpt_spr_id,thread_name  e2bmsgrpt_thread_name,thread_name_old  e2bmsgrpt_thread_name_old,user_created  e2bmsgrpt_user_created,user_modified  e2bmsgrpt_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_msg_rpt_queue
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_msg_queue IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_msg_rpt_queue')
  ) where REC_RANK=1 )
  , lsmv_e2b_message_ack_SUBSET AS 
(
select * from 
    (SELECT  
    ack_file_size  e2bmsgack_ack_file_size,ack_message_date  e2bmsgack_ack_message_date,agx_communication_status  e2bmsgack_agx_communication_status,authority_number  e2bmsgack_authority_number,date_created  e2bmsgack_date_created,date_modified  e2bmsgack_date_modified,fk_export_queue_rec_id  e2bmsgack_fk_export_queue_rec_id,fk_node_rec_id  e2bmsgack_fk_node_rec_id,icsr_message_number  e2bmsgack_icsr_message_number,imp_exp_status  e2bmsgack_imp_exp_status,json_s3_path  e2bmsgack_json_s3_path,local_message_number  e2bmsgack_local_message_number,mdn_date  e2bmsgack_mdn_date,mdn_message  e2bmsgack_mdn_message,message_ack_file  e2bmsgack_message_ack_file,message_date  e2bmsgack_message_date,message_file_name  e2bmsgack_message_file_name,message_number  e2bmsgack_message_number,message_receiver_id  e2bmsgack_message_receiver_id,message_sender_id  e2bmsgack_message_sender_id,module_id  e2bmsgack_module_id,parsing_error_message  e2bmsgack_parsing_error_message,record_id  e2bmsgack_record_id,repo_docid  e2bmsgack_repo_docid,spr_id  e2bmsgack_spr_id,status_code  e2bmsgack_status_code,transmission_ack_code  e2bmsgack_transmission_ack_code,user_created  e2bmsgack_user_created,user_modified  e2bmsgack_user_modified,xml_source  e2bmsgack_xml_source,xml_uploaded_date  e2bmsgack_xml_uploaded_date,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_message_ack
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_export_queue_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_message_ack')
  ) where REC_RANK=1 )
  , lsmv_e2b_report_ack_SUBSET AS 
(
select * from 
    (SELECT  
    authority_number  e2brptack_authority_number,company_number  e2brptack_company_number,date_created  e2brptack_date_created,date_modified  e2brptack_date_modified,error_message_comment  e2brptack_error_message_comment,fk_lema_rec_id  e2brptack_fk_lema_rec_id,is_warning  e2brptack_is_warning,local_report_number  e2brptack_local_report_number,message_number  e2brptack_message_number,mhlw_device_rep_type  e2brptack_mhlw_device_rep_type,mhlw_device_rep_year  e2brptack_mhlw_device_rep_year,mhlw_no_of_times_reported  e2brptack_mhlw_no_of_times_reported,mhlw_rep_ref_no  e2brptack_mhlw_rep_ref_no,other_number  e2brptack_other_number,receipt_date  e2brptack_receipt_date,record_id  e2brptack_record_id,rep_seq  e2brptack_rep_seq,report_ack_code  e2brptack_report_ack_code,safety_report_id  e2brptack_safety_report_id,safety_report_ver_number  e2brptack_safety_report_ver_number,spr_id  e2brptack_spr_id,user_created  e2brptack_user_created,user_modified  e2brptack_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_report_ack
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_lema_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_report_ack')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_e2b_message_ack_SUBSET.e2bmsgack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_e2b_report_ack_SUBSET.e2brptack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_e2b_mes_queue_SUBSET.e2bmsgque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_USER_MODIFIED USER_MODIFIED,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_USER_CREATED CREATED_BY,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_e2b_report_ack_SUBSET.e2brptack_user_modified  ,lsmv_e2b_report_ack_SUBSET.e2brptack_user_created  ,lsmv_e2b_report_ack_SUBSET.e2brptack_spr_id  ,lsmv_e2b_report_ack_SUBSET.e2brptack_safety_report_ver_number  ,lsmv_e2b_report_ack_SUBSET.e2brptack_safety_report_id  ,lsmv_e2b_report_ack_SUBSET.e2brptack_report_ack_code  ,lsmv_e2b_report_ack_SUBSET.e2brptack_rep_seq  ,lsmv_e2b_report_ack_SUBSET.e2brptack_record_id  ,lsmv_e2b_report_ack_SUBSET.e2brptack_receipt_date  ,lsmv_e2b_report_ack_SUBSET.e2brptack_other_number  ,lsmv_e2b_report_ack_SUBSET.e2brptack_mhlw_rep_ref_no  ,lsmv_e2b_report_ack_SUBSET.e2brptack_mhlw_no_of_times_reported  ,lsmv_e2b_report_ack_SUBSET.e2brptack_mhlw_device_rep_year  ,lsmv_e2b_report_ack_SUBSET.e2brptack_mhlw_device_rep_type  ,lsmv_e2b_report_ack_SUBSET.e2brptack_message_number  ,lsmv_e2b_report_ack_SUBSET.e2brptack_local_report_number  ,lsmv_e2b_report_ack_SUBSET.e2brptack_is_warning  ,lsmv_e2b_report_ack_SUBSET.e2brptack_fk_lema_rec_id  ,lsmv_e2b_report_ack_SUBSET.e2brptack_error_message_comment  ,lsmv_e2b_report_ack_SUBSET.e2brptack_date_modified  ,lsmv_e2b_report_ack_SUBSET.e2brptack_date_created  ,lsmv_e2b_report_ack_SUBSET.e2brptack_company_number  ,lsmv_e2b_report_ack_SUBSET.e2brptack_authority_number  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_user_modified  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_user_created  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_thread_name_old  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_thread_name  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_spr_id  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_seq_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_record_id  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_receipt_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_receipt_item_rec_id  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_processing_status  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_msg_date_created_format  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_msg_date_created  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_mhlw_device_rpt_type  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_json_string  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_is_validation_exsits  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_import_status  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_icsr_report_ver_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_icsr_report_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_fk_node_rec_id  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_fk_msg_queue  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_email_intake_queue_rpt_recid  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_date_modified  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_date_created  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_company_numb  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_comments  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_case_process_thread  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_bulk_status  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_authority_numb  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_archive_status  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_aer_ver_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_aer_no  ,lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_ack_code  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_xml_uploaded_date  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_xml_source  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_user_modified  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_user_created  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_transmission_ack_code  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_status_code  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_spr_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_repo_docid  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_record_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_parsing_error_message  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_module_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_sender_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_receiver_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_number  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_file_name  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_date  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_message_ack_file  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_mdn_message  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_mdn_date  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_local_message_number  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_json_s3_path  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_imp_exp_status  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_icsr_message_number  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_fk_node_rec_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_fk_export_queue_rec_id  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_date_modified  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_date_created  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_authority_number  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_agx_communication_status  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_ack_message_date  ,lsmv_e2b_message_ack_SUBSET.e2bmsgack_ack_file_size  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_xml_uploaded_date  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_xml_type  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_xml_source  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_vaer_no  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_user_modified  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_user_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_user_created  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_transmission_date  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_transformed_docid  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_trans_queue_flag  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_thread_name  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_status_last_updated  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_status  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_spr_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_sender_institution  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_rpt_queues_filled  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_repo_docid  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_record_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_receiver_institution  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_processing_status  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_module_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_message_type  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_message_number  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_message_file_name  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_mdn_update_attempts  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_mdn_status  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_mdn_message  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_mdn_date  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_local_icsr_number  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_last_name  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_inbound_email_message_uid  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_inbound_e2b_xml_encoding_type  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_imported_e2b_xml_filedata  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_import_status  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_ichr_message_number  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_gateway_ack_date  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_folder_path  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_fk_node_rec_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_first_name  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_file_name_without_ext  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_external_file_path  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_error_detail  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_email_subject  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_email_name  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_email_intake_queue_recid  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_dtd_type  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_date_submitted  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_date_modified  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_date_created  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_case_record_ids  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_case_info_json  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_case_id  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_batch_or_single  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_archive_status  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_acknowledgement_generated  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_ack_to_be_processed  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_ack_imp_config  ,lsmv_e2b_mes_queue_SUBSET.e2bmsgque_ack_code ,CONCAT(NVL(lsmv_e2b_message_ack_SUBSET.e2bmsgack_RECORD_ID,-1),'||',NVL(lsmv_e2b_report_ack_SUBSET.e2brptack_RECORD_ID,-1),'||',NVL(lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_RECORD_ID,-1),'||',NVL(lsmv_e2b_mes_queue_SUBSET.e2bmsgque_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_e2b_mes_queue_SUBSET  LEFT JOIN lsmv_e2b_msg_rpt_queue_SUBSET ON lsmv_e2b_mes_queue_SUBSET.e2bmsgque_RECORD_ID=lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_fk_msg_queue
                         LEFT JOIN lsmv_e2b_message_ack_SUBSET ON lsmv_e2b_msg_rpt_queue_SUBSET.e2bmsgrpt_RECORD_ID=lsmv_e2b_message_ack_SUBSET.e2bmsgack_fk_export_queue_rec_id
                         LEFT JOIN lsmv_e2b_report_ack_SUBSET ON lsmv_e2b_message_ack_SUBSET.e2bmsgack_RECORD_ID=lsmv_e2b_report_ack_SUBSET.e2brptack_fk_lema_rec_id
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP)
where target_table_name='LS_DB_E2B_IMPORT_QUEUE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE   
SET LS_DB_E2B_IMPORT_QUEUE.e2brptack_user_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_user_modified,LS_DB_E2B_IMPORT_QUEUE.e2brptack_user_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_user_created,LS_DB_E2B_IMPORT_QUEUE.e2brptack_spr_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_spr_id,LS_DB_E2B_IMPORT_QUEUE.e2brptack_safety_report_ver_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_safety_report_ver_number,LS_DB_E2B_IMPORT_QUEUE.e2brptack_safety_report_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_safety_report_id,LS_DB_E2B_IMPORT_QUEUE.e2brptack_report_ack_code = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_report_ack_code,LS_DB_E2B_IMPORT_QUEUE.e2brptack_rep_seq = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_rep_seq,LS_DB_E2B_IMPORT_QUEUE.e2brptack_record_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_record_id,LS_DB_E2B_IMPORT_QUEUE.e2brptack_receipt_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_receipt_date,LS_DB_E2B_IMPORT_QUEUE.e2brptack_other_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_other_number,LS_DB_E2B_IMPORT_QUEUE.e2brptack_mhlw_rep_ref_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_mhlw_rep_ref_no,LS_DB_E2B_IMPORT_QUEUE.e2brptack_mhlw_no_of_times_reported = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_mhlw_no_of_times_reported,LS_DB_E2B_IMPORT_QUEUE.e2brptack_mhlw_device_rep_year = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_mhlw_device_rep_year,LS_DB_E2B_IMPORT_QUEUE.e2brptack_mhlw_device_rep_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_mhlw_device_rep_type,LS_DB_E2B_IMPORT_QUEUE.e2brptack_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_message_number,LS_DB_E2B_IMPORT_QUEUE.e2brptack_local_report_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_local_report_number,LS_DB_E2B_IMPORT_QUEUE.e2brptack_is_warning = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_is_warning,LS_DB_E2B_IMPORT_QUEUE.e2brptack_fk_lema_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_fk_lema_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2brptack_error_message_comment = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_error_message_comment,LS_DB_E2B_IMPORT_QUEUE.e2brptack_date_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_date_modified,LS_DB_E2B_IMPORT_QUEUE.e2brptack_date_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_date_created,LS_DB_E2B_IMPORT_QUEUE.e2brptack_company_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_company_number,LS_DB_E2B_IMPORT_QUEUE.e2brptack_authority_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_authority_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_user_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_user_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_user_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_user_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_thread_name_old = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_thread_name_old,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_thread_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_thread_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_spr_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_spr_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_seq_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_seq_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_record_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_record_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_receipt_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_receipt_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_receipt_item_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_receipt_item_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_processing_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_processing_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_msg_date_created_format = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_msg_date_created_format,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_msg_date_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_msg_date_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_mhlw_device_rpt_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_mhlw_device_rpt_type,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_json_string = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_json_string,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_is_validation_exsits = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_is_validation_exsits,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_import_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_import_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_icsr_report_ver_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_icsr_report_ver_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_icsr_report_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_icsr_report_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_fk_node_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_fk_node_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_fk_msg_queue = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_fk_msg_queue,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_email_intake_queue_rpt_recid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_email_intake_queue_rpt_recid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_date_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_date_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_date_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_date_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_company_numb = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_company_numb,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_comments = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_comments,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_case_process_thread = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_case_process_thread,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_bulk_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_bulk_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_authority_numb = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_authority_numb,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_archive_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_archive_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_aer_ver_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_aer_ver_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_aer_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_aer_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_ack_code = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_ack_code,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_xml_uploaded_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_xml_uploaded_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_xml_source = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_xml_source,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_user_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_user_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_user_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_user_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_transmission_ack_code = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_transmission_ack_code,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_status_code = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_status_code,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_spr_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_spr_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_repo_docid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_repo_docid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_record_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_record_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_parsing_error_message = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_parsing_error_message,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_module_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_module_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_sender_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_sender_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_receiver_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_receiver_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_file_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_file_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_message_ack_file = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_message_ack_file,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_mdn_message = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_mdn_message,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_mdn_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_mdn_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_local_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_local_message_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_json_s3_path = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_json_s3_path,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_imp_exp_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_imp_exp_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_icsr_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_icsr_message_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_fk_node_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_fk_node_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_fk_export_queue_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_fk_export_queue_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_date_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_date_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_date_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_date_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_authority_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_authority_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_agx_communication_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_agx_communication_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_ack_message_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_ack_message_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_ack_file_size = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_ack_file_size,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_xml_uploaded_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_xml_uploaded_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_xml_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_xml_type,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_xml_source = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_xml_source,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_vaer_no = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_vaer_no,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_user_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_user_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_user_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_user_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_user_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_user_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_transmission_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_transmission_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_transformed_docid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_transformed_docid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_trans_queue_flag = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_trans_queue_flag,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_thread_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_thread_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_status_last_updated = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_status_last_updated,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_spr_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_spr_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_sender_institution = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_sender_institution,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_rpt_queues_filled = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_rpt_queues_filled,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_repo_docid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_repo_docid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_record_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_record_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_receiver_institution = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_receiver_institution,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_processing_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_processing_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_module_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_module_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_message_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_message_type,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_message_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_message_file_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_message_file_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_mdn_update_attempts = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_mdn_update_attempts,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_mdn_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_mdn_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_mdn_message = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_mdn_message,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_mdn_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_mdn_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_local_icsr_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_local_icsr_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_last_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_last_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_inbound_email_message_uid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_inbound_email_message_uid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_inbound_e2b_xml_encoding_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_inbound_e2b_xml_encoding_type,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_imported_e2b_xml_filedata = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_imported_e2b_xml_filedata,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_import_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_import_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_ichr_message_number = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_ichr_message_number,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_gateway_ack_date = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_gateway_ack_date,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_folder_path = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_folder_path,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_fk_node_rec_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_fk_node_rec_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_first_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_first_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_file_name_without_ext = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_file_name_without_ext,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_external_file_path = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_external_file_path,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_error_detail = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_error_detail,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_email_subject = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_email_subject,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_email_name = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_email_name,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_email_intake_queue_recid = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_email_intake_queue_recid,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_dtd_type = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_dtd_type,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_date_submitted = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_date_submitted,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_date_modified = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_date_modified,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_date_created = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_date_created,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_case_record_ids = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_case_record_ids,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_case_info_json = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_case_info_json,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_case_id = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_case_id,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_batch_or_single = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_batch_or_single,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_archive_status = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_archive_status,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_acknowledgement_generated = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_acknowledgement_generated,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_ack_to_be_processed = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_ack_to_be_processed,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_ack_imp_config = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_ack_imp_config,LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_ack_code = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_ack_code,
LS_DB_E2B_IMPORT_QUEUE.PROCESSING_DT = LS_DB_E2B_IMPORT_QUEUE_TMP.PROCESSING_DT,
LS_DB_E2B_IMPORT_QUEUE.user_modified  =LS_DB_E2B_IMPORT_QUEUE_TMP.user_modified     ,
LS_DB_E2B_IMPORT_QUEUE.date_modified  =LS_DB_E2B_IMPORT_QUEUE_TMP.date_modified     ,
LS_DB_E2B_IMPORT_QUEUE.expiry_date    =LS_DB_E2B_IMPORT_QUEUE_TMP.expiry_date       ,
LS_DB_E2B_IMPORT_QUEUE.created_by     =LS_DB_E2B_IMPORT_QUEUE_TMP.created_by        ,
LS_DB_E2B_IMPORT_QUEUE.created_dt     =LS_DB_E2B_IMPORT_QUEUE_TMP.created_dt        ,
LS_DB_E2B_IMPORT_QUEUE.load_ts        =LS_DB_E2B_IMPORT_QUEUE_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP 
WHERE 	LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_E2B_IMPORT_QUEUE_TMP.PROCESSING_DT = LS_DB_E2B_IMPORT_QUEUE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2brptack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_E2B_IMPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID ,LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP 
ON LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID=LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_RECORD_ID
AND LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID 
where LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID  is null AND LS_DB_E2B_IMPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID in (select e2bmsgque_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP )
) TMP where LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_E2B_IMPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID ,LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP 
    ON LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID=LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_RECORD_ID
    AND LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID 
    where LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID  is null AND LS_DB_E2B_IMPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID in (select e2bmsgque_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,e2brptack_user_modified,
e2brptack_user_created,
e2brptack_spr_id,
e2brptack_safety_report_ver_number,
e2brptack_safety_report_id,
e2brptack_report_ack_code,
e2brptack_rep_seq,
e2brptack_record_id,
e2brptack_receipt_date,
e2brptack_other_number,
e2brptack_mhlw_rep_ref_no,
e2brptack_mhlw_no_of_times_reported,
e2brptack_mhlw_device_rep_year,
e2brptack_mhlw_device_rep_type,
e2brptack_message_number,
e2brptack_local_report_number,
e2brptack_is_warning,
e2brptack_fk_lema_rec_id,
e2brptack_error_message_comment,
e2brptack_date_modified,
e2brptack_date_created,
e2brptack_company_number,
e2brptack_authority_number,
e2bmsgrpt_user_modified,
e2bmsgrpt_user_created,
e2bmsgrpt_thread_name_old,
e2bmsgrpt_thread_name,
e2bmsgrpt_spr_id,
e2bmsgrpt_seq_no,
e2bmsgrpt_record_id,
e2bmsgrpt_receipt_no,
e2bmsgrpt_receipt_item_rec_id,
e2bmsgrpt_processing_status,
e2bmsgrpt_msg_date_created_format,
e2bmsgrpt_msg_date_created,
e2bmsgrpt_mhlw_device_rpt_type,
e2bmsgrpt_json_string,
e2bmsgrpt_is_validation_exsits,
e2bmsgrpt_import_status,
e2bmsgrpt_icsr_report_ver_no,
e2bmsgrpt_icsr_report_no,
e2bmsgrpt_fk_node_rec_id,
e2bmsgrpt_fk_msg_queue,
e2bmsgrpt_email_intake_queue_rpt_recid,
e2bmsgrpt_date_modified,
e2bmsgrpt_date_created,
e2bmsgrpt_company_numb,
e2bmsgrpt_comments,
e2bmsgrpt_case_process_thread,
e2bmsgrpt_bulk_status,
e2bmsgrpt_authority_numb,
e2bmsgrpt_archive_status,
e2bmsgrpt_aer_ver_no,
e2bmsgrpt_aer_no,
e2bmsgrpt_ack_code,
e2bmsgack_xml_uploaded_date,
e2bmsgack_xml_source,
e2bmsgack_user_modified,
e2bmsgack_user_created,
e2bmsgack_transmission_ack_code,
e2bmsgack_status_code,
e2bmsgack_spr_id,
e2bmsgack_repo_docid,
e2bmsgack_record_id,
e2bmsgack_parsing_error_message,
e2bmsgack_module_id,
e2bmsgack_message_sender_id,
e2bmsgack_message_receiver_id,
e2bmsgack_message_number,
e2bmsgack_message_file_name,
e2bmsgack_message_date,
e2bmsgack_message_ack_file,
e2bmsgack_mdn_message,
e2bmsgack_mdn_date,
e2bmsgack_local_message_number,
e2bmsgack_json_s3_path,
e2bmsgack_imp_exp_status,
e2bmsgack_icsr_message_number,
e2bmsgack_fk_node_rec_id,
e2bmsgack_fk_export_queue_rec_id,
e2bmsgack_date_modified,
e2bmsgack_date_created,
e2bmsgack_authority_number,
e2bmsgack_agx_communication_status,
e2bmsgack_ack_message_date,
e2bmsgack_ack_file_size,
e2bmsgque_xml_uploaded_date,
e2bmsgque_xml_type,
e2bmsgque_xml_source,
e2bmsgque_vaer_no,
e2bmsgque_user_modified,
e2bmsgque_user_id,
e2bmsgque_user_created,
e2bmsgque_transmission_date,
e2bmsgque_transformed_docid,
e2bmsgque_trans_queue_flag,
e2bmsgque_thread_name,
e2bmsgque_status_last_updated,
e2bmsgque_status,
e2bmsgque_spr_id,
e2bmsgque_sender_institution,
e2bmsgque_rpt_queues_filled,
e2bmsgque_repo_docid,
e2bmsgque_record_id,
e2bmsgque_receiver_institution,
e2bmsgque_processing_status,
e2bmsgque_module_id,
e2bmsgque_message_type,
e2bmsgque_message_number,
e2bmsgque_message_file_name,
e2bmsgque_mdn_update_attempts,
e2bmsgque_mdn_status,
e2bmsgque_mdn_message,
e2bmsgque_mdn_date,
e2bmsgque_local_icsr_number,
e2bmsgque_last_name,
e2bmsgque_inbound_email_message_uid,
e2bmsgque_inbound_e2b_xml_encoding_type,
e2bmsgque_imported_e2b_xml_filedata,
e2bmsgque_import_status,
e2bmsgque_ichr_message_number,
e2bmsgque_gateway_ack_date,
e2bmsgque_folder_path,
e2bmsgque_fk_node_rec_id,
e2bmsgque_first_name,
e2bmsgque_file_name_without_ext,
e2bmsgque_external_file_path,
e2bmsgque_error_detail,
e2bmsgque_email_subject,
e2bmsgque_email_name,
e2bmsgque_email_intake_queue_recid,
e2bmsgque_dtd_type,
e2bmsgque_date_submitted,
e2bmsgque_date_modified,
e2bmsgque_date_created,
e2bmsgque_case_record_ids,
e2bmsgque_case_info_json,
e2bmsgque_case_id,
e2bmsgque_batch_or_single,
e2bmsgque_archive_status,
e2bmsgque_acknowledgement_generated,
e2bmsgque_ack_to_be_processed,
e2bmsgque_ack_imp_config,
e2bmsgque_ack_code)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,e2brptack_user_modified,
e2brptack_user_created,
e2brptack_spr_id,
e2brptack_safety_report_ver_number,
e2brptack_safety_report_id,
e2brptack_report_ack_code,
e2brptack_rep_seq,
e2brptack_record_id,
e2brptack_receipt_date,
e2brptack_other_number,
e2brptack_mhlw_rep_ref_no,
e2brptack_mhlw_no_of_times_reported,
e2brptack_mhlw_device_rep_year,
e2brptack_mhlw_device_rep_type,
e2brptack_message_number,
e2brptack_local_report_number,
e2brptack_is_warning,
e2brptack_fk_lema_rec_id,
e2brptack_error_message_comment,
e2brptack_date_modified,
e2brptack_date_created,
e2brptack_company_number,
e2brptack_authority_number,
e2bmsgrpt_user_modified,
e2bmsgrpt_user_created,
e2bmsgrpt_thread_name_old,
e2bmsgrpt_thread_name,
e2bmsgrpt_spr_id,
e2bmsgrpt_seq_no,
e2bmsgrpt_record_id,
e2bmsgrpt_receipt_no,
e2bmsgrpt_receipt_item_rec_id,
e2bmsgrpt_processing_status,
e2bmsgrpt_msg_date_created_format,
e2bmsgrpt_msg_date_created,
e2bmsgrpt_mhlw_device_rpt_type,
e2bmsgrpt_json_string,
e2bmsgrpt_is_validation_exsits,
e2bmsgrpt_import_status,
e2bmsgrpt_icsr_report_ver_no,
e2bmsgrpt_icsr_report_no,
e2bmsgrpt_fk_node_rec_id,
e2bmsgrpt_fk_msg_queue,
e2bmsgrpt_email_intake_queue_rpt_recid,
e2bmsgrpt_date_modified,
e2bmsgrpt_date_created,
e2bmsgrpt_company_numb,
e2bmsgrpt_comments,
e2bmsgrpt_case_process_thread,
e2bmsgrpt_bulk_status,
e2bmsgrpt_authority_numb,
e2bmsgrpt_archive_status,
e2bmsgrpt_aer_ver_no,
e2bmsgrpt_aer_no,
e2bmsgrpt_ack_code,
e2bmsgack_xml_uploaded_date,
e2bmsgack_xml_source,
e2bmsgack_user_modified,
e2bmsgack_user_created,
e2bmsgack_transmission_ack_code,
e2bmsgack_status_code,
e2bmsgack_spr_id,
e2bmsgack_repo_docid,
e2bmsgack_record_id,
e2bmsgack_parsing_error_message,
e2bmsgack_module_id,
e2bmsgack_message_sender_id,
e2bmsgack_message_receiver_id,
e2bmsgack_message_number,
e2bmsgack_message_file_name,
e2bmsgack_message_date,
e2bmsgack_message_ack_file,
e2bmsgack_mdn_message,
e2bmsgack_mdn_date,
e2bmsgack_local_message_number,
e2bmsgack_json_s3_path,
e2bmsgack_imp_exp_status,
e2bmsgack_icsr_message_number,
e2bmsgack_fk_node_rec_id,
e2bmsgack_fk_export_queue_rec_id,
e2bmsgack_date_modified,
e2bmsgack_date_created,
e2bmsgack_authority_number,
e2bmsgack_agx_communication_status,
e2bmsgack_ack_message_date,
e2bmsgack_ack_file_size,
e2bmsgque_xml_uploaded_date,
e2bmsgque_xml_type,
e2bmsgque_xml_source,
e2bmsgque_vaer_no,
e2bmsgque_user_modified,
e2bmsgque_user_id,
e2bmsgque_user_created,
e2bmsgque_transmission_date,
e2bmsgque_transformed_docid,
e2bmsgque_trans_queue_flag,
e2bmsgque_thread_name,
e2bmsgque_status_last_updated,
e2bmsgque_status,
e2bmsgque_spr_id,
e2bmsgque_sender_institution,
e2bmsgque_rpt_queues_filled,
e2bmsgque_repo_docid,
e2bmsgque_record_id,
e2bmsgque_receiver_institution,
e2bmsgque_processing_status,
e2bmsgque_module_id,
e2bmsgque_message_type,
e2bmsgque_message_number,
e2bmsgque_message_file_name,
e2bmsgque_mdn_update_attempts,
e2bmsgque_mdn_status,
e2bmsgque_mdn_message,
e2bmsgque_mdn_date,
e2bmsgque_local_icsr_number,
e2bmsgque_last_name,
e2bmsgque_inbound_email_message_uid,
e2bmsgque_inbound_e2b_xml_encoding_type,
e2bmsgque_imported_e2b_xml_filedata,
e2bmsgque_import_status,
e2bmsgque_ichr_message_number,
e2bmsgque_gateway_ack_date,
e2bmsgque_folder_path,
e2bmsgque_fk_node_rec_id,
e2bmsgque_first_name,
e2bmsgque_file_name_without_ext,
e2bmsgque_external_file_path,
e2bmsgque_error_detail,
e2bmsgque_email_subject,
e2bmsgque_email_name,
e2bmsgque_email_intake_queue_recid,
e2bmsgque_dtd_type,
e2bmsgque_date_submitted,
e2bmsgque_date_modified,
e2bmsgque_date_created,
e2bmsgque_case_record_ids,
e2bmsgque_case_info_json,
e2bmsgque_case_id,
e2bmsgque_batch_or_single,
e2bmsgque_archive_status,
e2bmsgque_acknowledgement_generated,
e2bmsgque_ack_to_be_processed,
e2bmsgque_ack_imp_config,
e2bmsgque_ack_code
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE TGT
WHERE  ( e2bmsgque_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_mes_queue') OR e2bmsgack_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_message_ack') OR e2bmsgrpt_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_msg_rpt_queue') OR e2brptack_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_report_ack')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP 
WHERE 	TO_DATE(LS_DB_E2B_IMPORT_QUEUE.PROCESSING_DT) < TO_DATE(LS_DB_E2B_IMPORT_QUEUE_TMP.PROCESSING_DT)
AND LS_DB_E2B_IMPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_IMPORT_QUEUE_TMP.INTEGRATION_ID
AND LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_RECORD_ID = LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_RECORD_ID
AND LS_DB_E2B_IMPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2brptack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE.e2bmsgque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2brptack_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_IMPORT_QUEUE_TMP.e2bmsgque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( e2bmsgque_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_mes_queue') OR e2bmsgack_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_message_ack') OR e2bmsgrpt_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_msg_rpt_queue') OR e2brptack_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_IMPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_report_ack')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_IMPORT_QUEUE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_E2B_IMPORT_QUEUE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_E2B_IMPORT_QUEUE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_IMPORT_QUEUE'),
	'LSDB','Case','LS_DB_E2B_IMPORT_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_E2B_IMPORT_QUEUE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
