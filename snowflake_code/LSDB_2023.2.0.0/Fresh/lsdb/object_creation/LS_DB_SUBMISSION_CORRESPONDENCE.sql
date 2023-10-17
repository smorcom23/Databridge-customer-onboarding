
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CORRESPONDENCE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_CORRESPONDENCE'),
	'LSDB','Case','LS_DB_SUBMISSION_CORRESPONDENCE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_SUBMISSION_CORRESPONDENCE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SUBMISSION_CORRESPONDENCE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  As select RECORD_ID,'lsmv_st_corresp_doc' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_corresp_doc WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_st_correspondence' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_correspondence WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_correspondence WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_correspondence WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_corresp_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_lscr_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_corresp_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CORRESPONDENCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_st_correspondence_SUBSET AS 
(
select * from 
    (SELECT  
    ack_reply  stcorr_ack_reply,ari_rec_id  stcorr_ari_rec_id,call_duration  stcorr_call_duration,call_received_from  stcorr_call_received_from,call_status  stcorr_call_status,carbon_copy  stcorr_carbon_copy,category  stcorr_category,comp_rec_id  stcorr_comp_rec_id,correspondence_date  stcorr_correspondence_date,correspondence_mode  stcorr_correspondence_mode,correspondence_seq  stcorr_correspondence_seq,created_year  stcorr_created_year,date_created  stcorr_date_created,date_modified  stcorr_date_modified,date_transmitted  stcorr_date_transmitted,delivery_status  stcorr_delivery_status,direction  stcorr_direction,dispatch_status  stcorr_dispatch_status,display_in_portal  stcorr_display_in_portal,expected_reply_date  stcorr_expected_reply_date,fax_id  stcorr_fax_id,fax_status  stcorr_fax_status,fk_ccm_comm_rec_id  stcorr_fk_ccm_comm_rec_id,fk_ccm_comm_thread_rec_id  stcorr_fk_ccm_comm_thread_rec_id,fk_lsm_rec_id  stcorr_fk_lsm_rec_id,fk_naer_ref_rec_id  stcorr_fk_naer_ref_rec_id,fk_saf_corr_doc_rec_id  stcorr_fk_saf_corr_doc_rec_id,from_email_id  stcorr_from_email_id,include_icsrxml  stcorr_include_icsrxml,inq_rec_id  stcorr_inq_rec_id,insert_solution  stcorr_insert_solution,letter_creation_option  stcorr_letter_creation_option,letter_received_from  stcorr_letter_received_from,letter_send_date  stcorr_letter_send_date,letter_sent_to  stcorr_letter_sent_to,letter_word_editor_value  stcorr_letter_word_editor_value,lrn  stcorr_lrn,phone_area_code  stcorr_phone_area_code,phone_cntry_code  stcorr_phone_cntry_code,phone_no  stcorr_phone_no,priority  stcorr_priority,read_correspondence  stcorr_read_correspondence,receipt_no  stcorr_receipt_no,record_id  stcorr_record_id,sent_by  stcorr_sent_by,spr_id  stcorr_spr_id,status  stcorr_status,subject  stcorr_subject,submission_no  stcorr_submission_no,submission_state  stcorr_submission_state,template_name  stcorr_template_name,template_record_id  stcorr_template_record_id,to_email_id  stcorr_to_email_id,user_created  stcorr_user_created,user_modified  stcorr_user_modified,who_received_call  stcorr_who_received_call,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_correspondence
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_correspondence')
  ) where REC_RANK=1 )
  , lsmv_st_corresp_doc_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  stcorrdoc_ari_rec_id,date_created  stcorrdoc_date_created,date_modified  stcorrdoc_date_modified,doc_id  stcorrdoc_doc_id,doc_name  stcorrdoc_doc_name,doc_size  stcorrdoc_doc_size,doc_source  stcorrdoc_doc_source,fk_lscr_rec_id  stcorrdoc_fk_lscr_rec_id,record_id  stcorrdoc_record_id,spr_id  stcorrdoc_spr_id,user_created  stcorrdoc_user_created,user_modified  stcorrdoc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_corresp_doc
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_lscr_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_corresp_doc')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_st_corresp_doc_SUBSET.stcorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_st_correspondence_SUBSET.stcorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_st_correspondence_SUBSET.stcorr_USER_MODIFIED USER_MODIFIED,lsmv_st_correspondence_SUBSET.stcorr_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_st_correspondence_SUBSET.stcorr_USER_CREATED CREATED_BY,lsmv_st_correspondence_SUBSET.stcorr_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_correspondence_SUBSET.stcorr_who_received_call  ,lsmv_st_correspondence_SUBSET.stcorr_user_modified  ,lsmv_st_correspondence_SUBSET.stcorr_user_created  ,lsmv_st_correspondence_SUBSET.stcorr_to_email_id  ,lsmv_st_correspondence_SUBSET.stcorr_template_record_id  ,lsmv_st_correspondence_SUBSET.stcorr_template_name  ,lsmv_st_correspondence_SUBSET.stcorr_submission_state  ,lsmv_st_correspondence_SUBSET.stcorr_submission_no  ,lsmv_st_correspondence_SUBSET.stcorr_subject  ,lsmv_st_correspondence_SUBSET.stcorr_status  ,lsmv_st_correspondence_SUBSET.stcorr_spr_id  ,lsmv_st_correspondence_SUBSET.stcorr_sent_by  ,lsmv_st_correspondence_SUBSET.stcorr_record_id  ,lsmv_st_correspondence_SUBSET.stcorr_receipt_no  ,lsmv_st_correspondence_SUBSET.stcorr_read_correspondence  ,lsmv_st_correspondence_SUBSET.stcorr_priority  ,lsmv_st_correspondence_SUBSET.stcorr_phone_no  ,lsmv_st_correspondence_SUBSET.stcorr_phone_cntry_code  ,lsmv_st_correspondence_SUBSET.stcorr_phone_area_code  ,lsmv_st_correspondence_SUBSET.stcorr_lrn  ,lsmv_st_correspondence_SUBSET.stcorr_letter_word_editor_value  ,lsmv_st_correspondence_SUBSET.stcorr_letter_sent_to  ,lsmv_st_correspondence_SUBSET.stcorr_letter_send_date  ,lsmv_st_correspondence_SUBSET.stcorr_letter_received_from  ,lsmv_st_correspondence_SUBSET.stcorr_letter_creation_option  ,lsmv_st_correspondence_SUBSET.stcorr_insert_solution  ,lsmv_st_correspondence_SUBSET.stcorr_inq_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_include_icsrxml  ,lsmv_st_correspondence_SUBSET.stcorr_from_email_id  ,lsmv_st_correspondence_SUBSET.stcorr_fk_saf_corr_doc_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_fk_naer_ref_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_fk_lsm_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_fk_ccm_comm_thread_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_fk_ccm_comm_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_fax_status  ,lsmv_st_correspondence_SUBSET.stcorr_fax_id  ,lsmv_st_correspondence_SUBSET.stcorr_expected_reply_date  ,lsmv_st_correspondence_SUBSET.stcorr_display_in_portal  ,lsmv_st_correspondence_SUBSET.stcorr_dispatch_status  ,lsmv_st_correspondence_SUBSET.stcorr_direction  ,lsmv_st_correspondence_SUBSET.stcorr_delivery_status  ,lsmv_st_correspondence_SUBSET.stcorr_date_transmitted  ,lsmv_st_correspondence_SUBSET.stcorr_date_modified  ,lsmv_st_correspondence_SUBSET.stcorr_date_created  ,lsmv_st_correspondence_SUBSET.stcorr_created_year  ,lsmv_st_correspondence_SUBSET.stcorr_correspondence_seq  ,lsmv_st_correspondence_SUBSET.stcorr_correspondence_mode  ,lsmv_st_correspondence_SUBSET.stcorr_correspondence_date  ,lsmv_st_correspondence_SUBSET.stcorr_comp_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_category  ,lsmv_st_correspondence_SUBSET.stcorr_carbon_copy  ,lsmv_st_correspondence_SUBSET.stcorr_call_status  ,lsmv_st_correspondence_SUBSET.stcorr_call_received_from  ,lsmv_st_correspondence_SUBSET.stcorr_call_duration  ,lsmv_st_correspondence_SUBSET.stcorr_ari_rec_id  ,lsmv_st_correspondence_SUBSET.stcorr_ack_reply  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_user_modified  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_user_created  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_spr_id  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_record_id  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_fk_lscr_rec_id  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_doc_source  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_doc_size  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_doc_name  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_doc_id  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_date_modified  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_date_created  ,lsmv_st_corresp_doc_SUBSET.stcorrdoc_ari_rec_id ,CONCAT( NVL(lsmv_st_corresp_doc_SUBSET.stcorrdoc_RECORD_ID,-1),'||',NVL(lsmv_st_correspondence_SUBSET.stcorr_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_correspondence_SUBSET  LEFT JOIN lsmv_st_corresp_doc_SUBSET ON lsmv_st_correspondence_SUBSET.stcorr_record_id=lsmv_st_corresp_doc_SUBSET.stcorrdoc_fk_lscr_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_st_corresp_doc_SUBSET.stcorrdoc_RECORD_ID,lsmv_st_correspondence_SUBSET.stcorr_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP)
where target_table_name='LS_DB_SUBMISSION_CORRESPONDENCE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE   
SET LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_who_received_call = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_who_received_call,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_user_modified = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_user_modified,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_user_created = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_user_created,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_to_email_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_to_email_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_template_record_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_template_record_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_template_name = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_template_name,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_submission_state = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_submission_state,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_submission_no = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_submission_no,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_subject = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_subject,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_status = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_status,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_spr_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_spr_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_sent_by = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_sent_by,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_record_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_record_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_receipt_no = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_receipt_no,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_read_correspondence = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_read_correspondence,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_priority = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_priority,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_phone_no = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_phone_no,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_phone_cntry_code = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_phone_cntry_code,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_phone_area_code = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_phone_area_code,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_lrn = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_lrn,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_letter_word_editor_value = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_letter_word_editor_value,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_letter_sent_to = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_letter_sent_to,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_letter_send_date = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_letter_send_date,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_letter_received_from = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_letter_received_from,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_letter_creation_option = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_letter_creation_option,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_insert_solution = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_insert_solution,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_inq_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_inq_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_include_icsrxml = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_include_icsrxml,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_from_email_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_from_email_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fk_saf_corr_doc_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fk_saf_corr_doc_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fk_naer_ref_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fk_naer_ref_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fk_lsm_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fk_lsm_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fk_ccm_comm_thread_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fk_ccm_comm_thread_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fk_ccm_comm_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fk_ccm_comm_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fax_status = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fax_status,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_fax_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_fax_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_expected_reply_date = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_expected_reply_date,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_display_in_portal = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_display_in_portal,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_dispatch_status = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_dispatch_status,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_direction = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_direction,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_delivery_status = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_delivery_status,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_date_transmitted = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_date_transmitted,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_date_modified = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_date_modified,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_date_created = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_date_created,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_created_year = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_created_year,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_correspondence_seq = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_correspondence_seq,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_correspondence_mode = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_correspondence_mode,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_correspondence_date = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_correspondence_date,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_comp_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_comp_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_category = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_category,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_carbon_copy = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_carbon_copy,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_call_status = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_call_status,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_call_received_from = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_call_received_from,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_call_duration = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_call_duration,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_ari_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_ari_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_ack_reply = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_ack_reply,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_user_modified = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_user_modified,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_user_created = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_user_created,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_spr_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_spr_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_record_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_record_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_fk_lscr_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_fk_lscr_rec_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_doc_source = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_doc_source,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_doc_size = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_doc_size,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_doc_name = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_doc_name,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_doc_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_doc_id,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_date_modified = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_date_modified,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_date_created = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_date_created,LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_ari_rec_id = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_ari_rec_id,
LS_DB_SUBMISSION_CORRESPONDENCE.PROCESSING_DT = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.PROCESSING_DT,
LS_DB_SUBMISSION_CORRESPONDENCE.receipt_id     =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.receipt_id    ,
LS_DB_SUBMISSION_CORRESPONDENCE.case_no        =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.case_no           ,
LS_DB_SUBMISSION_CORRESPONDENCE.case_version   =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.case_version      ,
LS_DB_SUBMISSION_CORRESPONDENCE.version_no     =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.version_no        ,
LS_DB_SUBMISSION_CORRESPONDENCE.user_modified  =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.user_modified     ,
LS_DB_SUBMISSION_CORRESPONDENCE.date_modified  =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.date_modified     ,
LS_DB_SUBMISSION_CORRESPONDENCE.expiry_date    =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.expiry_date       ,
LS_DB_SUBMISSION_CORRESPONDENCE.created_by     =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.created_by        ,
LS_DB_SUBMISSION_CORRESPONDENCE.created_dt     =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.created_dt        ,
LS_DB_SUBMISSION_CORRESPONDENCE.load_ts        =LS_DB_SUBMISSION_CORRESPONDENCE_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP 
WHERE 	LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SUBMISSION_CORRESPONDENCE_TMP.PROCESSING_DT = LS_DB_SUBMISSION_CORRESPONDENCE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_SUBMISSION_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID ,LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP 
ON LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID=LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_RECORD_ID
AND LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID 
where LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID  is null AND LS_DB_SUBMISSION_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID in (select stcorr_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP )
) TMP where LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_SUBMISSION_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID ,LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP 
    ON LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID=LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_RECORD_ID
    AND LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID 
    where LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID  is null AND LS_DB_SUBMISSION_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID in (select stcorr_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE
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
integration_id ,stcorr_who_received_call,
stcorr_user_modified,
stcorr_user_created,
stcorr_to_email_id,
stcorr_template_record_id,
stcorr_template_name,
stcorr_submission_state,
stcorr_submission_no,
stcorr_subject,
stcorr_status,
stcorr_spr_id,
stcorr_sent_by,
stcorr_record_id,
stcorr_receipt_no,
stcorr_read_correspondence,
stcorr_priority,
stcorr_phone_no,
stcorr_phone_cntry_code,
stcorr_phone_area_code,
stcorr_lrn,
stcorr_letter_word_editor_value,
stcorr_letter_sent_to,
stcorr_letter_send_date,
stcorr_letter_received_from,
stcorr_letter_creation_option,
stcorr_insert_solution,
stcorr_inq_rec_id,
stcorr_include_icsrxml,
stcorr_from_email_id,
stcorr_fk_saf_corr_doc_rec_id,
stcorr_fk_naer_ref_rec_id,
stcorr_fk_lsm_rec_id,
stcorr_fk_ccm_comm_thread_rec_id,
stcorr_fk_ccm_comm_rec_id,
stcorr_fax_status,
stcorr_fax_id,
stcorr_expected_reply_date,
stcorr_display_in_portal,
stcorr_dispatch_status,
stcorr_direction,
stcorr_delivery_status,
stcorr_date_transmitted,
stcorr_date_modified,
stcorr_date_created,
stcorr_created_year,
stcorr_correspondence_seq,
stcorr_correspondence_mode,
stcorr_correspondence_date,
stcorr_comp_rec_id,
stcorr_category,
stcorr_carbon_copy,
stcorr_call_status,
stcorr_call_received_from,
stcorr_call_duration,
stcorr_ari_rec_id,
stcorr_ack_reply,
stcorrdoc_user_modified,
stcorrdoc_user_created,
stcorrdoc_spr_id,
stcorrdoc_record_id,
stcorrdoc_fk_lscr_rec_id,
stcorrdoc_doc_source,
stcorrdoc_doc_size,
stcorrdoc_doc_name,
stcorrdoc_doc_id,
stcorrdoc_date_modified,
stcorrdoc_date_created,
stcorrdoc_ari_rec_id)
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
integration_id ,stcorr_who_received_call,
stcorr_user_modified,
stcorr_user_created,
stcorr_to_email_id,
stcorr_template_record_id,
stcorr_template_name,
stcorr_submission_state,
stcorr_submission_no,
stcorr_subject,
stcorr_status,
stcorr_spr_id,
stcorr_sent_by,
stcorr_record_id,
stcorr_receipt_no,
stcorr_read_correspondence,
stcorr_priority,
stcorr_phone_no,
stcorr_phone_cntry_code,
stcorr_phone_area_code,
stcorr_lrn,
stcorr_letter_word_editor_value,
stcorr_letter_sent_to,
stcorr_letter_send_date,
stcorr_letter_received_from,
stcorr_letter_creation_option,
stcorr_insert_solution,
stcorr_inq_rec_id,
stcorr_include_icsrxml,
stcorr_from_email_id,
stcorr_fk_saf_corr_doc_rec_id,
stcorr_fk_naer_ref_rec_id,
stcorr_fk_lsm_rec_id,
stcorr_fk_ccm_comm_thread_rec_id,
stcorr_fk_ccm_comm_rec_id,
stcorr_fax_status,
stcorr_fax_id,
stcorr_expected_reply_date,
stcorr_display_in_portal,
stcorr_dispatch_status,
stcorr_direction,
stcorr_delivery_status,
stcorr_date_transmitted,
stcorr_date_modified,
stcorr_date_created,
stcorr_created_year,
stcorr_correspondence_seq,
stcorr_correspondence_mode,
stcorr_correspondence_date,
stcorr_comp_rec_id,
stcorr_category,
stcorr_carbon_copy,
stcorr_call_status,
stcorr_call_received_from,
stcorr_call_duration,
stcorr_ari_rec_id,
stcorr_ack_reply,
stcorrdoc_user_modified,
stcorrdoc_user_created,
stcorrdoc_spr_id,
stcorrdoc_record_id,
stcorrdoc_fk_lscr_rec_id,
stcorrdoc_doc_source,
stcorrdoc_doc_size,
stcorrdoc_doc_name,
stcorrdoc_doc_id,
stcorrdoc_date_modified,
stcorrdoc_date_created,
stcorrdoc_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP 
WHERE 	TO_DATE(LS_DB_SUBMISSION_CORRESPONDENCE.PROCESSING_DT) < TO_DATE(LS_DB_SUBMISSION_CORRESPONDENCE_TMP.PROCESSING_DT)
AND LS_DB_SUBMISSION_CORRESPONDENCE.INTEGRATION_ID = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.INTEGRATION_ID
AND LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_RECORD_ID = LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_RECORD_ID
AND LS_DB_SUBMISSION_CORRESPONDENCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_SUBMISSION_CORRESPONDENCE.stcorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SUBMISSION_CORRESPONDENCE.stcorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorrdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SUBMISSION_CORRESPONDENCE_TMP.stcorr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE TGT
WHERE  ( stcorrdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_corresp_doc') OR stcorr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_correspondence')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( stcorrdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_corresp_doc') OR stcorr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CORRESPONDENCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_correspondence')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CORRESPONDENCE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SUBMISSION_CORRESPONDENCE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_SUBMISSION_CORRESPONDENCE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_CORRESPONDENCE'),
	'LSDB','Case','LS_DB_SUBMISSION_CORRESPONDENCE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_SUBMISSION_CORRESPONDENCE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
