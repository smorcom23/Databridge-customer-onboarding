
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_E2B_CASE_EXPORT_QUEUE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_CASE_EXPORT_QUEUE'),
                'LSDB','Case','LS_DB_E2B_CASE_EXPORT_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_E2B_CASE_EXPORT_QUEUE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_E2B_CASE_EXPORT_QUEUE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  As select RECORD_ID,'lsmv_e2b_case_export_doc_info' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_doc_info WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_e2b_case_export_queue' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_queue WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9905')
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

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_CASE_EXPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_CASE_EXPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_doc_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_CASE_EXPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_eceq_rec_id record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_doc_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_E2B_CASE_EXPORT_QUEUE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_e2b_case_export_queue_SUBSET AS 
(
select * from 
    (SELECT  
    additional_comments  expque_additional_comments,batch_id  expque_batch_id,blinded  expque_blinded,case_identifier_no  expque_case_identifier_no,case_type  expque_case_type,consider_submission  expque_consider_submission,count_of_submission  expque_count_of_submission,date_created  expque_date_created,date_exported  expque_date_exported,date_modified  expque_date_modified,description  expque_description,doc_id  expque_doc_id,doc_size  expque_doc_size,export_status  expque_export_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9905' AND CD=CAST(export_status AS VARCHAR(100)) )expque_export_status_de_ml , export_term_id  expque_export_term_id,fk_aer_rec_id  expque_fk_aer_rec_id,fk_ari_rec_id  expque_fk_ari_rec_id,fk_inq_rec_id  expque_fk_inq_rec_id,fk_node_rec_id  expque_fk_node_rec_id,generate_xml  expque_generate_xml,inserted_date  expque_inserted_date,is_ack_req  expque_is_ack_req,is_do_not_gen  expque_is_do_not_gen,is_mdn_req  expque_is_mdn_req,language  expque_language,masked  expque_masked,module  expque_module,receiver  expque_receiver,record_id  expque_record_id,related_sub_rec_ids  expque_related_sub_rec_ids,spr_id  expque_spr_id,submission_rec_id  expque_submission_rec_id,thread_name  expque_thread_name,user_created  expque_user_created,user_modified  expque_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_queue
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_queue')
  ) where REC_RANK=1 )
  , lsmv_e2b_case_export_doc_info_SUBSET AS 
(
select * from 
    (SELECT  
    ack_doc_id  expdocinfo_ack_doc_id,ack_mdn_date  expdocinfo_ack_mdn_date,ack_mdn_message  expdocinfo_ack_mdn_message,case_identifier_no  expdocinfo_case_identifier_no,category_code  expdocinfo_category_code,date_created  expdocinfo_date_created,date_doc_exported  expdocinfo_date_doc_exported,date_modified  expdocinfo_date_modified,doc_ack_mdn_status  expdocinfo_doc_ack_mdn_status,doc_ack_source  expdocinfo_doc_ack_source,doc_ack_status  expdocinfo_doc_ack_status,doc_data  expdocinfo_doc_data,doc_exported_status  expdocinfo_doc_exported_status,doc_ext  expdocinfo_doc_ext,doc_id  expdocinfo_doc_id,doc_mdn_status  expdocinfo_doc_mdn_status,doc_name  expdocinfo_doc_name,doc_rec_id  expdocinfo_doc_rec_id,doc_size  expdocinfo_doc_size,doc_type  expdocinfo_doc_type,docs_ack_attach_file_name  expdocinfo_docs_ack_attach_file_name,docs_ack_code  expdocinfo_docs_ack_code,docs_ack_msg  expdocinfo_docs_ack_msg,docs_ack_rcv_date  expdocinfo_docs_ack_rcv_date,docs_ack_subject  expdocinfo_docs_ack_subject,docs_xml_ack_data  expdocinfo_docs_xml_ack_data,exported_doc_name  expdocinfo_exported_doc_name,fk_aer_rec_id  expdocinfo_fk_aer_rec_id,fk_ari_rec_id  expdocinfo_fk_ari_rec_id,fk_e2b_sub_rec_id  expdocinfo_fk_e2b_sub_rec_id,fk_eceq_rec_id  expdocinfo_fk_eceq_rec_id,is_additional_doc  expdocinfo_is_additional_doc,latest_doc_version  expdocinfo_latest_doc_version,mdn_date  expdocinfo_mdn_date,mdn_message  expdocinfo_mdn_message,merge_doc_names  expdocinfo_merge_doc_names,merge_doc_record_ids  expdocinfo_merge_doc_record_ids,receiver  expdocinfo_receiver,record_id  expdocinfo_record_id,safetyreportid  expdocinfo_safetyreportid,sender  expdocinfo_sender,spr_id  expdocinfo_spr_id,user_created  expdocinfo_user_created,user_modified  expdocinfo_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_case_export_doc_info
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_eceq_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_doc_info')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_e2b_case_export_queue_SUBSET.expque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO            ,lsmv_e2b_case_export_queue_SUBSET.expque_USER_MODIFIED USER_MODIFIED,lsmv_e2b_case_export_queue_SUBSET.expque_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE     ,lsmv_e2b_case_export_queue_SUBSET.expque_USER_CREATED CREATED_BY,lsmv_e2b_case_export_queue_SUBSET.expque_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_e2b_case_export_queue_SUBSET.expque_user_modified  ,lsmv_e2b_case_export_queue_SUBSET.expque_user_created  ,lsmv_e2b_case_export_queue_SUBSET.expque_thread_name  ,lsmv_e2b_case_export_queue_SUBSET.expque_submission_rec_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_spr_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_related_sub_rec_ids  ,lsmv_e2b_case_export_queue_SUBSET.expque_record_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_receiver  ,lsmv_e2b_case_export_queue_SUBSET.expque_module  ,lsmv_e2b_case_export_queue_SUBSET.expque_masked  ,lsmv_e2b_case_export_queue_SUBSET.expque_language  ,lsmv_e2b_case_export_queue_SUBSET.expque_is_mdn_req  ,lsmv_e2b_case_export_queue_SUBSET.expque_is_do_not_gen  ,lsmv_e2b_case_export_queue_SUBSET.expque_is_ack_req  ,lsmv_e2b_case_export_queue_SUBSET.expque_inserted_date  ,lsmv_e2b_case_export_queue_SUBSET.expque_generate_xml  ,lsmv_e2b_case_export_queue_SUBSET.expque_fk_node_rec_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_fk_inq_rec_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_fk_ari_rec_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_fk_aer_rec_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_export_term_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_export_status_de_ml  ,lsmv_e2b_case_export_queue_SUBSET.expque_export_status  ,lsmv_e2b_case_export_queue_SUBSET.expque_doc_size  ,lsmv_e2b_case_export_queue_SUBSET.expque_doc_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_description  ,lsmv_e2b_case_export_queue_SUBSET.expque_date_modified  ,lsmv_e2b_case_export_queue_SUBSET.expque_date_exported  ,lsmv_e2b_case_export_queue_SUBSET.expque_date_created  ,lsmv_e2b_case_export_queue_SUBSET.expque_count_of_submission  ,lsmv_e2b_case_export_queue_SUBSET.expque_consider_submission  ,lsmv_e2b_case_export_queue_SUBSET.expque_case_type  ,lsmv_e2b_case_export_queue_SUBSET.expque_case_identifier_no  ,lsmv_e2b_case_export_queue_SUBSET.expque_blinded  ,lsmv_e2b_case_export_queue_SUBSET.expque_batch_id  ,lsmv_e2b_case_export_queue_SUBSET.expque_additional_comments  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_user_modified  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_user_created  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_spr_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_sender  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_safetyreportid  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_record_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_receiver  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_merge_doc_record_ids  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_merge_doc_names  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_mdn_message  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_mdn_date  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_latest_doc_version  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_is_additional_doc  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_fk_eceq_rec_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_fk_e2b_sub_rec_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_fk_ari_rec_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_fk_aer_rec_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_exported_doc_name  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_xml_ack_data  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_ack_subject  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_ack_rcv_date  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_ack_msg  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_ack_code  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_docs_ack_attach_file_name  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_type  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_size  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_rec_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_name  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_mdn_status  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_id  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_ext  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_exported_status  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_data  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_ack_status  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_ack_source  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_doc_ack_mdn_status  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_date_modified  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_date_doc_exported  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_date_created  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_category_code  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_case_identifier_no  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_ack_mdn_message  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_ack_mdn_date  ,lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_ack_doc_id ,CONCAT( NVL(lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_RECORD_ID,-1),'||',NVL(lsmv_e2b_case_export_queue_SUBSET.expque_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_e2b_case_export_queue_SUBSET  LEFT JOIN lsmv_e2b_case_export_doc_info_SUBSET ON lsmv_e2b_case_export_queue_SUBSET.expque_record_id=lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_fk_eceq_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_e2b_case_export_doc_info_SUBSET.expdocinfo_RECORD_ID,lsmv_e2b_case_export_queue_SUBSET.expque_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP)
where target_table_name='LS_DB_E2B_CASE_EXPORT_QUEUE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE   
SET LS_DB_E2B_CASE_EXPORT_QUEUE.expque_user_modified = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_user_modified,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_user_created = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_user_created,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_thread_name = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_thread_name,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_submission_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_submission_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_spr_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_spr_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_related_sub_rec_ids = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_related_sub_rec_ids,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_record_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_record_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_receiver = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_receiver,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_module = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_module,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_masked = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_masked,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_language = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_language,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_is_mdn_req = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_is_mdn_req,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_is_do_not_gen = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_is_do_not_gen,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_is_ack_req = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_is_ack_req,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_inserted_date = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_inserted_date,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_generate_xml = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_generate_xml,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_fk_node_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_fk_node_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_fk_inq_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_fk_inq_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_fk_ari_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_fk_ari_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_fk_aer_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_fk_aer_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_export_term_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_export_term_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_export_status_de_ml = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_export_status_de_ml,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_export_status = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_export_status,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_doc_size = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_doc_size,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_doc_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_doc_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_description = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_description,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_date_modified = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_date_modified,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_date_exported = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_date_exported,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_date_created = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_date_created,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_count_of_submission = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_count_of_submission,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_consider_submission = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_consider_submission,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_case_type = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_case_type,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_case_identifier_no = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_case_identifier_no,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_blinded = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_blinded,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_batch_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_batch_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expque_additional_comments = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_additional_comments,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_user_modified = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_user_modified,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_user_created = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_user_created,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_spr_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_spr_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_sender = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_sender,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_safetyreportid = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_safetyreportid,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_record_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_record_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_receiver = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_receiver,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_merge_doc_record_ids = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_merge_doc_record_ids,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_merge_doc_names = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_merge_doc_names,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_mdn_message = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_mdn_message,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_mdn_date = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_mdn_date,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_latest_doc_version = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_latest_doc_version,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_is_additional_doc = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_is_additional_doc,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_fk_eceq_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_fk_eceq_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_fk_e2b_sub_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_fk_e2b_sub_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_fk_ari_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_fk_ari_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_fk_aer_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_fk_aer_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_exported_doc_name = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_exported_doc_name,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_xml_ack_data = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_xml_ack_data,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_ack_subject = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_ack_subject,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_ack_rcv_date = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_ack_rcv_date,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_ack_msg = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_ack_msg,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_ack_code = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_ack_code,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_docs_ack_attach_file_name = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_docs_ack_attach_file_name,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_type = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_type,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_size = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_size,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_rec_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_rec_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_name = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_name,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_mdn_status = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_mdn_status,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_id,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_ext = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_ext,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_exported_status = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_exported_status,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_data = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_data,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_ack_status = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_ack_status,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_ack_source = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_ack_source,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_doc_ack_mdn_status = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_doc_ack_mdn_status,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_date_modified = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_date_modified,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_date_doc_exported = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_date_doc_exported,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_date_created = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_date_created,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_category_code = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_category_code,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_case_identifier_no = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_case_identifier_no,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_ack_mdn_message = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_ack_mdn_message,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_ack_mdn_date = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_ack_mdn_date,LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_ack_doc_id = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_ack_doc_id,
LS_DB_E2B_CASE_EXPORT_QUEUE.PROCESSING_DT = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.PROCESSING_DT,
LS_DB_E2B_CASE_EXPORT_QUEUE.receipt_id     =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.receipt_id    ,
LS_DB_E2B_CASE_EXPORT_QUEUE.case_no        =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.case_no           ,
LS_DB_E2B_CASE_EXPORT_QUEUE.case_version   =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.case_version      ,
LS_DB_E2B_CASE_EXPORT_QUEUE.version_no     =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.version_no        ,
LS_DB_E2B_CASE_EXPORT_QUEUE.user_modified  =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.user_modified     ,
LS_DB_E2B_CASE_EXPORT_QUEUE.date_modified  =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.date_modified     ,
LS_DB_E2B_CASE_EXPORT_QUEUE.expiry_date    =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expiry_date       ,
LS_DB_E2B_CASE_EXPORT_QUEUE.created_by     =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.created_by        ,
LS_DB_E2B_CASE_EXPORT_QUEUE.created_dt     =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.created_dt        ,
LS_DB_E2B_CASE_EXPORT_QUEUE.load_ts        =LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP 
WHERE LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.PROCESSING_DT = LS_DB_E2B_CASE_EXPORT_QUEUE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_CASE_EXPORT_QUEUE.expque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_E2B_CASE_EXPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID ,LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP 
ON LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID=LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_RECORD_ID
AND LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID 
where LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID  is null AND LS_DB_E2B_CASE_EXPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID in (select expque_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP )
) TMP where LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_E2B_CASE_EXPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID ,LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP 
    ON LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID=LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_RECORD_ID
    AND LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID 
    where LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID  is null AND LS_DB_E2B_CASE_EXPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID in (select expque_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE
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
integration_id ,expque_user_modified,
expque_user_created,
expque_thread_name,
expque_submission_rec_id,
expque_spr_id,
expque_related_sub_rec_ids,
expque_record_id,
expque_receiver,
expque_module,
expque_masked,
expque_language,
expque_is_mdn_req,
expque_is_do_not_gen,
expque_is_ack_req,
expque_inserted_date,
expque_generate_xml,
expque_fk_node_rec_id,
expque_fk_inq_rec_id,
expque_fk_ari_rec_id,
expque_fk_aer_rec_id,
expque_export_term_id,
expque_export_status_de_ml,
expque_export_status,
expque_doc_size,
expque_doc_id,
expque_description,
expque_date_modified,
expque_date_exported,
expque_date_created,
expque_count_of_submission,
expque_consider_submission,
expque_case_type,
expque_case_identifier_no,
expque_blinded,
expque_batch_id,
expque_additional_comments,
expdocinfo_user_modified,
expdocinfo_user_created,
expdocinfo_spr_id,
expdocinfo_sender,
expdocinfo_safetyreportid,
expdocinfo_record_id,
expdocinfo_receiver,
expdocinfo_merge_doc_record_ids,
expdocinfo_merge_doc_names,
expdocinfo_mdn_message,
expdocinfo_mdn_date,
expdocinfo_latest_doc_version,
expdocinfo_is_additional_doc,
expdocinfo_fk_eceq_rec_id,
expdocinfo_fk_e2b_sub_rec_id,
expdocinfo_fk_ari_rec_id,
expdocinfo_fk_aer_rec_id,
expdocinfo_exported_doc_name,
expdocinfo_docs_xml_ack_data,
expdocinfo_docs_ack_subject,
expdocinfo_docs_ack_rcv_date,
expdocinfo_docs_ack_msg,
expdocinfo_docs_ack_code,
expdocinfo_docs_ack_attach_file_name,
expdocinfo_doc_type,
expdocinfo_doc_size,
expdocinfo_doc_rec_id,
expdocinfo_doc_name,
expdocinfo_doc_mdn_status,
expdocinfo_doc_id,
expdocinfo_doc_ext,
expdocinfo_doc_exported_status,
expdocinfo_doc_data,
expdocinfo_doc_ack_status,
expdocinfo_doc_ack_source,
expdocinfo_doc_ack_mdn_status,
expdocinfo_date_modified,
expdocinfo_date_doc_exported,
expdocinfo_date_created,
expdocinfo_category_code,
expdocinfo_case_identifier_no,
expdocinfo_ack_mdn_message,
expdocinfo_ack_mdn_date,
expdocinfo_ack_doc_id)
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
integration_id ,expque_user_modified,
expque_user_created,
expque_thread_name,
expque_submission_rec_id,
expque_spr_id,
expque_related_sub_rec_ids,
expque_record_id,
expque_receiver,
expque_module,
expque_masked,
expque_language,
expque_is_mdn_req,
expque_is_do_not_gen,
expque_is_ack_req,
expque_inserted_date,
expque_generate_xml,
expque_fk_node_rec_id,
expque_fk_inq_rec_id,
expque_fk_ari_rec_id,
expque_fk_aer_rec_id,
expque_export_term_id,
expque_export_status_de_ml,
expque_export_status,
expque_doc_size,
expque_doc_id,
expque_description,
expque_date_modified,
expque_date_exported,
expque_date_created,
expque_count_of_submission,
expque_consider_submission,
expque_case_type,
expque_case_identifier_no,
expque_blinded,
expque_batch_id,
expque_additional_comments,
expdocinfo_user_modified,
expdocinfo_user_created,
expdocinfo_spr_id,
expdocinfo_sender,
expdocinfo_safetyreportid,
expdocinfo_record_id,
expdocinfo_receiver,
expdocinfo_merge_doc_record_ids,
expdocinfo_merge_doc_names,
expdocinfo_mdn_message,
expdocinfo_mdn_date,
expdocinfo_latest_doc_version,
expdocinfo_is_additional_doc,
expdocinfo_fk_eceq_rec_id,
expdocinfo_fk_e2b_sub_rec_id,
expdocinfo_fk_ari_rec_id,
expdocinfo_fk_aer_rec_id,
expdocinfo_exported_doc_name,
expdocinfo_docs_xml_ack_data,
expdocinfo_docs_ack_subject,
expdocinfo_docs_ack_rcv_date,
expdocinfo_docs_ack_msg,
expdocinfo_docs_ack_code,
expdocinfo_docs_ack_attach_file_name,
expdocinfo_doc_type,
expdocinfo_doc_size,
expdocinfo_doc_rec_id,
expdocinfo_doc_name,
expdocinfo_doc_mdn_status,
expdocinfo_doc_id,
expdocinfo_doc_ext,
expdocinfo_doc_exported_status,
expdocinfo_doc_data,
expdocinfo_doc_ack_status,
expdocinfo_doc_ack_source,
expdocinfo_doc_ack_mdn_status,
expdocinfo_date_modified,
expdocinfo_date_doc_exported,
expdocinfo_date_created,
expdocinfo_category_code,
expdocinfo_case_identifier_no,
expdocinfo_ack_mdn_message,
expdocinfo_ack_mdn_date,
expdocinfo_ack_doc_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP 
WHERE TO_DATE(LS_DB_E2B_CASE_EXPORT_QUEUE.PROCESSING_DT) < TO_DATE(LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.PROCESSING_DT)
AND LS_DB_E2B_CASE_EXPORT_QUEUE.INTEGRATION_ID = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.INTEGRATION_ID
AND LS_DB_E2B_CASE_EXPORT_QUEUE.expque_RECORD_ID = LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_RECORD_ID
AND LS_DB_E2B_CASE_EXPORT_QUEUE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_E2B_CASE_EXPORT_QUEUE.expdocinfo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_E2B_CASE_EXPORT_QUEUE.expque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expdocinfo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_E2B_CASE_EXPORT_QUEUE_TMP.expque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE TGT
WHERE  ( expdocinfo_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_doc_info') OR expque_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_queue')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( expdocinfo_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_doc_info') OR expque_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_E2B_CASE_EXPORT_QUEUE_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_case_export_queue')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_E2B_CASE_EXPORT_QUEUE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_E2B_CASE_EXPORT_QUEUE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_E2B_CASE_EXPORT_QUEUE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_CASE_EXPORT_QUEUE'),
                'LSDB','Case','LS_DB_E2B_CASE_EXPORT_QUEUE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_E2B_CASE_EXPORT_QUEUE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
