
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CAPA()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CAPA'),
	'LSDB','Case','LS_DB_CAPA',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_CAPA' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CAPA' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  As select RECORD_ID,'lsmv_capa_lateness_linked_aer' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_linked_aer WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_capa_lateness_records' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_records WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_corrective_capa_doc' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_corrective_capa_doc WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_preventive_capa_doc' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_preventive_capa_doc WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_rootcause_capa_doc' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_rootcause_capa_doc WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10102','10103')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_records WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_records WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_linked_aer WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_CAPA_LATENESS_RECORD_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_linked_aer WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_corrective_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_CAPA_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_corrective_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_rootcause_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_CAPA_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_rootcause_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_preventive_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_CAPA_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_preventive_capa_doc WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CAPA' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_preventive_capa_doc_SUBSET AS 
(
select * from 
    (SELECT  
    comments  prevcapa_comments,date_created  prevcapa_date_created,date_modified  prevcapa_date_modified,doc_id  prevcapa_doc_id,doc_name  prevcapa_doc_name,doc_size  prevcapa_doc_size,doc_source  prevcapa_doc_source,fk_capa_rec_id  prevcapa_fk_capa_rec_id,record_id  prevcapa_record_id,spr_id  prevcapa_spr_id,user_created  prevcapa_user_created,user_modified  prevcapa_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_preventive_capa_doc
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_CAPA_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_preventive_capa_doc')
  ) where REC_RANK=1 )
  , lsmv_capa_lateness_linked_aer_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  caplatelink_date_created,date_modified  caplatelink_date_modified,delay  caplatelink_delay,fk_capa_lateness_record_id  caplatelink_fk_capa_lateness_record_id,is_linked  caplatelink_is_linked,linked_aer_no  caplatelink_linked_aer_no,linked_aer_version  caplatelink_linked_aer_version,linked_receipt_no  caplatelink_linked_receipt_no,record_id  caplatelink_record_id,spr_id  caplatelink_spr_id,user_created  caplatelink_user_created,user_modified  caplatelink_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_linked_aer
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_CAPA_LATENESS_RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_linked_aer')
  ) where REC_RANK=1 )
  , lsmv_corrective_capa_doc_SUBSET AS 
(
select * from 
    (SELECT  
    comments  corrcap_comments,date_created  corrcap_date_created,date_modified  corrcap_date_modified,doc_id  corrcap_doc_id,doc_name  corrcap_doc_name,doc_size  corrcap_doc_size,doc_source  corrcap_doc_source,fk_capa_rec_id  corrcap_fk_capa_rec_id,record_id  corrcap_record_id,spr_id  corrcap_spr_id,user_created  corrcap_user_created,user_modified  corrcap_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_corrective_capa_doc
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_CAPA_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_corrective_capa_doc')
  ) where REC_RANK=1 )
  , lsmv_rootcause_capa_doc_SUBSET AS 
(
select * from 
    (SELECT  
    comments  rootcap_comments,date_created  rootcap_date_created,date_modified  rootcap_date_modified,doc_id  rootcap_doc_id,doc_name  rootcap_doc_name,doc_size  rootcap_doc_size,doc_source  rootcap_doc_source,fk_capa_rec_id  rootcap_fk_capa_rec_id,record_id  rootcap_record_id,spr_id  rootcap_spr_id,user_created  rootcap_user_created,user_modified  rootcap_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_rootcause_capa_doc
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_CAPA_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_rootcause_capa_doc')
  ) where REC_RANK=1 )
  , lsmv_capa_lateness_records_SUBSET AS 
(
select * from 
    (SELECT  
    accountable  caplate_accountable,aer_no  caplate_aer_no,aer_version_no  caplate_aer_version_no,assignee  caplate_assignee,capa_completion_date  caplate_capa_completion_date,capa_creation_date  caplate_capa_creation_date,capa_number  caplate_capa_number,category  caplate_category,comments  caplate_comments,corrective_action  caplate_corrective_action,date_archived  caplate_date_archived,date_created  caplate_date_created,date_modified  caplate_date_modified,delay  caplate_delay,desc_of_non_compliance  caplate_desc_of_non_compliance,due_date  caplate_due_date,im_rec_id  caplate_im_rec_id,is_archived  caplate_is_archived,late_submission_contact  caplate_late_submission_contact,lateness_reason  caplate_lateness_reason,module  caplate_module,monitoring_end_date  caplate_monitoring_end_date,monitoring_start_date  caplate_monitoring_start_date,monitoring_timeline  caplate_monitoring_timeline,monitoring_type  caplate_monitoring_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10103' AND CD=CAST(monitoring_type AS VARCHAR(100)) )caplate_monitoring_type_de_ml , non_compliance_iden_date  caplate_non_compliance_iden_date,preventive_action  caplate_preventive_action,rca  caplate_rca,record_id  caplate_record_id,reference_number  caplate_reference_number,spr_id  caplate_spr_id,status  caplate_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10102' AND CD=CAST(status AS VARCHAR(100)) )caplate_status_de_ml , user_created  caplate_user_created,user_modified  caplate_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_capa_lateness_records
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_records')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_preventive_capa_doc_SUBSET.prevcapa_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_rootcause_capa_doc_SUBSET.rootcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_corrective_capa_doc_SUBSET.corrcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_capa_lateness_records_SUBSET.caplate_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_capa_lateness_records_SUBSET.caplate_USER_MODIFIED USER_MODIFIED,lsmv_capa_lateness_records_SUBSET.caplate_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_capa_lateness_records_SUBSET.caplate_USER_CREATED CREATED_BY,lsmv_capa_lateness_records_SUBSET.caplate_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_rootcause_capa_doc_SUBSET.rootcap_user_modified  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_user_created  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_spr_id  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_record_id  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_fk_capa_rec_id  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_doc_source  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_doc_size  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_doc_name  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_doc_id  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_date_modified  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_date_created  ,lsmv_rootcause_capa_doc_SUBSET.rootcap_comments  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_user_modified  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_user_created  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_spr_id  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_record_id  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_fk_capa_rec_id  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_doc_source  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_doc_size  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_doc_name  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_doc_id  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_date_modified  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_date_created  ,lsmv_preventive_capa_doc_SUBSET.prevcapa_comments  ,lsmv_corrective_capa_doc_SUBSET.corrcap_user_modified  ,lsmv_corrective_capa_doc_SUBSET.corrcap_user_created  ,lsmv_corrective_capa_doc_SUBSET.corrcap_spr_id  ,lsmv_corrective_capa_doc_SUBSET.corrcap_record_id  ,lsmv_corrective_capa_doc_SUBSET.corrcap_fk_capa_rec_id  ,lsmv_corrective_capa_doc_SUBSET.corrcap_doc_source  ,lsmv_corrective_capa_doc_SUBSET.corrcap_doc_size  ,lsmv_corrective_capa_doc_SUBSET.corrcap_doc_name  ,lsmv_corrective_capa_doc_SUBSET.corrcap_doc_id  ,lsmv_corrective_capa_doc_SUBSET.corrcap_date_modified  ,lsmv_corrective_capa_doc_SUBSET.corrcap_date_created  ,lsmv_corrective_capa_doc_SUBSET.corrcap_comments  ,lsmv_capa_lateness_records_SUBSET.caplate_user_modified  ,lsmv_capa_lateness_records_SUBSET.caplate_user_created  ,lsmv_capa_lateness_records_SUBSET.caplate_status_de_ml  ,lsmv_capa_lateness_records_SUBSET.caplate_status  ,lsmv_capa_lateness_records_SUBSET.caplate_spr_id  ,lsmv_capa_lateness_records_SUBSET.caplate_reference_number  ,lsmv_capa_lateness_records_SUBSET.caplate_record_id  ,lsmv_capa_lateness_records_SUBSET.caplate_rca  ,lsmv_capa_lateness_records_SUBSET.caplate_preventive_action  ,lsmv_capa_lateness_records_SUBSET.caplate_non_compliance_iden_date  ,lsmv_capa_lateness_records_SUBSET.caplate_monitoring_type_de_ml  ,lsmv_capa_lateness_records_SUBSET.caplate_monitoring_type  ,lsmv_capa_lateness_records_SUBSET.caplate_monitoring_timeline  ,lsmv_capa_lateness_records_SUBSET.caplate_monitoring_start_date  ,lsmv_capa_lateness_records_SUBSET.caplate_monitoring_end_date  ,lsmv_capa_lateness_records_SUBSET.caplate_module  ,lsmv_capa_lateness_records_SUBSET.caplate_lateness_reason  ,lsmv_capa_lateness_records_SUBSET.caplate_late_submission_contact  ,lsmv_capa_lateness_records_SUBSET.caplate_is_archived  ,lsmv_capa_lateness_records_SUBSET.caplate_im_rec_id  ,lsmv_capa_lateness_records_SUBSET.caplate_due_date  ,lsmv_capa_lateness_records_SUBSET.caplate_desc_of_non_compliance  ,lsmv_capa_lateness_records_SUBSET.caplate_delay  ,lsmv_capa_lateness_records_SUBSET.caplate_date_modified  ,lsmv_capa_lateness_records_SUBSET.caplate_date_created  ,lsmv_capa_lateness_records_SUBSET.caplate_date_archived  ,lsmv_capa_lateness_records_SUBSET.caplate_corrective_action  ,lsmv_capa_lateness_records_SUBSET.caplate_comments  ,lsmv_capa_lateness_records_SUBSET.caplate_category  ,lsmv_capa_lateness_records_SUBSET.caplate_capa_number  ,lsmv_capa_lateness_records_SUBSET.caplate_capa_creation_date  ,lsmv_capa_lateness_records_SUBSET.caplate_capa_completion_date  ,lsmv_capa_lateness_records_SUBSET.caplate_assignee  ,lsmv_capa_lateness_records_SUBSET.caplate_aer_version_no  ,lsmv_capa_lateness_records_SUBSET.caplate_aer_no  ,lsmv_capa_lateness_records_SUBSET.caplate_accountable  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_user_modified  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_user_created  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_spr_id  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_record_id  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_linked_receipt_no  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_linked_aer_version  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_linked_aer_no  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_is_linked  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_fk_capa_lateness_record_id  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_delay  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_date_modified  ,lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_date_created ,CONCAT(NVL(lsmv_preventive_capa_doc_SUBSET.prevcapa_RECORD_ID,-1),'||',NVL(lsmv_rootcause_capa_doc_SUBSET.rootcap_RECORD_ID,-1),'||',NVL(lsmv_corrective_capa_doc_SUBSET.corrcap_RECORD_ID,-1),'||',NVL(lsmv_capa_lateness_linked_aer_SUBSET.caplatelink_RECORD_ID,-1),'||',NVL(lsmv_capa_lateness_records_SUBSET.caplate_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_capa_lateness_records_SUBSET  LEFT JOIN LSMV_CAPA_LATENESS_LINKED_AER_SUBSET ON LSMV_CAPA_LATENESS_RECORDS_SUBSET.caplate_RECORD_ID=LSMV_CAPA_LATENESS_LINKED_AER_SUBSET.caplatelink_FK_CAPA_LATENESS_RECORD_ID
                         LEFT JOIN LSMV_CORRECTIVE_CAPA_DOC_SUBSET ON LSMV_CAPA_LATENESS_RECORDS_SUBSET.caplate_RECORD_ID=LSMV_CORRECTIVE_CAPA_DOC_SUBSET.corrcap_FK_CAPA_REC_ID
                         LEFT JOIN LSMV_PREVENTIVE_CAPA_DOC_SUBSET ON LSMV_CAPA_LATENESS_RECORDS_SUBSET.caplate_RECORD_ID=LSMV_PREVENTIVE_CAPA_DOC_SUBSET.prevcapa_FK_CAPA_REC_ID
                         LEFT JOIN LSMV_ROOTCAUSE_CAPA_DOC_SUBSET ON LSMV_CAPA_LATENESS_RECORDS_SUBSET.caplate_RECORD_ID=LSMV_ROOTCAUSE_CAPA_DOC_SUBSET.rootcap_FK_CAPA_REC_ID
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP)
where target_table_name='LS_DB_CAPA'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA   
SET LS_DB_CAPA.rootcap_user_modified = LS_DB_CAPA_TMP.rootcap_user_modified,LS_DB_CAPA.rootcap_user_created = LS_DB_CAPA_TMP.rootcap_user_created,LS_DB_CAPA.rootcap_spr_id = LS_DB_CAPA_TMP.rootcap_spr_id,LS_DB_CAPA.rootcap_record_id = LS_DB_CAPA_TMP.rootcap_record_id,LS_DB_CAPA.rootcap_fk_capa_rec_id = LS_DB_CAPA_TMP.rootcap_fk_capa_rec_id,LS_DB_CAPA.rootcap_doc_source = LS_DB_CAPA_TMP.rootcap_doc_source,LS_DB_CAPA.rootcap_doc_size = LS_DB_CAPA_TMP.rootcap_doc_size,LS_DB_CAPA.rootcap_doc_name = LS_DB_CAPA_TMP.rootcap_doc_name,LS_DB_CAPA.rootcap_doc_id = LS_DB_CAPA_TMP.rootcap_doc_id,LS_DB_CAPA.rootcap_date_modified = LS_DB_CAPA_TMP.rootcap_date_modified,LS_DB_CAPA.rootcap_date_created = LS_DB_CAPA_TMP.rootcap_date_created,LS_DB_CAPA.rootcap_comments = LS_DB_CAPA_TMP.rootcap_comments,LS_DB_CAPA.prevcapa_user_modified = LS_DB_CAPA_TMP.prevcapa_user_modified,LS_DB_CAPA.prevcapa_user_created = LS_DB_CAPA_TMP.prevcapa_user_created,LS_DB_CAPA.prevcapa_spr_id = LS_DB_CAPA_TMP.prevcapa_spr_id,LS_DB_CAPA.prevcapa_record_id = LS_DB_CAPA_TMP.prevcapa_record_id,LS_DB_CAPA.prevcapa_fk_capa_rec_id = LS_DB_CAPA_TMP.prevcapa_fk_capa_rec_id,LS_DB_CAPA.prevcapa_doc_source = LS_DB_CAPA_TMP.prevcapa_doc_source,LS_DB_CAPA.prevcapa_doc_size = LS_DB_CAPA_TMP.prevcapa_doc_size,LS_DB_CAPA.prevcapa_doc_name = LS_DB_CAPA_TMP.prevcapa_doc_name,LS_DB_CAPA.prevcapa_doc_id = LS_DB_CAPA_TMP.prevcapa_doc_id,LS_DB_CAPA.prevcapa_date_modified = LS_DB_CAPA_TMP.prevcapa_date_modified,LS_DB_CAPA.prevcapa_date_created = LS_DB_CAPA_TMP.prevcapa_date_created,LS_DB_CAPA.prevcapa_comments = LS_DB_CAPA_TMP.prevcapa_comments,LS_DB_CAPA.corrcap_user_modified = LS_DB_CAPA_TMP.corrcap_user_modified,LS_DB_CAPA.corrcap_user_created = LS_DB_CAPA_TMP.corrcap_user_created,LS_DB_CAPA.corrcap_spr_id = LS_DB_CAPA_TMP.corrcap_spr_id,LS_DB_CAPA.corrcap_record_id = LS_DB_CAPA_TMP.corrcap_record_id,LS_DB_CAPA.corrcap_fk_capa_rec_id = LS_DB_CAPA_TMP.corrcap_fk_capa_rec_id,LS_DB_CAPA.corrcap_doc_source = LS_DB_CAPA_TMP.corrcap_doc_source,LS_DB_CAPA.corrcap_doc_size = LS_DB_CAPA_TMP.corrcap_doc_size,LS_DB_CAPA.corrcap_doc_name = LS_DB_CAPA_TMP.corrcap_doc_name,LS_DB_CAPA.corrcap_doc_id = LS_DB_CAPA_TMP.corrcap_doc_id,LS_DB_CAPA.corrcap_date_modified = LS_DB_CAPA_TMP.corrcap_date_modified,LS_DB_CAPA.corrcap_date_created = LS_DB_CAPA_TMP.corrcap_date_created,LS_DB_CAPA.corrcap_comments = LS_DB_CAPA_TMP.corrcap_comments,LS_DB_CAPA.caplate_user_modified = LS_DB_CAPA_TMP.caplate_user_modified,LS_DB_CAPA.caplate_user_created = LS_DB_CAPA_TMP.caplate_user_created,LS_DB_CAPA.caplate_status_de_ml = LS_DB_CAPA_TMP.caplate_status_de_ml,LS_DB_CAPA.caplate_status = LS_DB_CAPA_TMP.caplate_status,LS_DB_CAPA.caplate_spr_id = LS_DB_CAPA_TMP.caplate_spr_id,LS_DB_CAPA.caplate_reference_number = LS_DB_CAPA_TMP.caplate_reference_number,LS_DB_CAPA.caplate_record_id = LS_DB_CAPA_TMP.caplate_record_id,LS_DB_CAPA.caplate_rca = LS_DB_CAPA_TMP.caplate_rca,LS_DB_CAPA.caplate_preventive_action = LS_DB_CAPA_TMP.caplate_preventive_action,LS_DB_CAPA.caplate_non_compliance_iden_date = LS_DB_CAPA_TMP.caplate_non_compliance_iden_date,LS_DB_CAPA.caplate_monitoring_type_de_ml = LS_DB_CAPA_TMP.caplate_monitoring_type_de_ml,LS_DB_CAPA.caplate_monitoring_type = LS_DB_CAPA_TMP.caplate_monitoring_type,LS_DB_CAPA.caplate_monitoring_timeline = LS_DB_CAPA_TMP.caplate_monitoring_timeline,LS_DB_CAPA.caplate_monitoring_start_date = LS_DB_CAPA_TMP.caplate_monitoring_start_date,LS_DB_CAPA.caplate_monitoring_end_date = LS_DB_CAPA_TMP.caplate_monitoring_end_date,LS_DB_CAPA.caplate_module = LS_DB_CAPA_TMP.caplate_module,LS_DB_CAPA.caplate_lateness_reason = LS_DB_CAPA_TMP.caplate_lateness_reason,LS_DB_CAPA.caplate_late_submission_contact = LS_DB_CAPA_TMP.caplate_late_submission_contact,LS_DB_CAPA.caplate_is_archived = LS_DB_CAPA_TMP.caplate_is_archived,LS_DB_CAPA.caplate_im_rec_id = LS_DB_CAPA_TMP.caplate_im_rec_id,LS_DB_CAPA.caplate_due_date = LS_DB_CAPA_TMP.caplate_due_date,LS_DB_CAPA.caplate_desc_of_non_compliance = LS_DB_CAPA_TMP.caplate_desc_of_non_compliance,LS_DB_CAPA.caplate_delay = LS_DB_CAPA_TMP.caplate_delay,LS_DB_CAPA.caplate_date_modified = LS_DB_CAPA_TMP.caplate_date_modified,LS_DB_CAPA.caplate_date_created = LS_DB_CAPA_TMP.caplate_date_created,LS_DB_CAPA.caplate_date_archived = LS_DB_CAPA_TMP.caplate_date_archived,LS_DB_CAPA.caplate_corrective_action = LS_DB_CAPA_TMP.caplate_corrective_action,LS_DB_CAPA.caplate_comments = LS_DB_CAPA_TMP.caplate_comments,LS_DB_CAPA.caplate_category = LS_DB_CAPA_TMP.caplate_category,LS_DB_CAPA.caplate_capa_number = LS_DB_CAPA_TMP.caplate_capa_number,LS_DB_CAPA.caplate_capa_creation_date = LS_DB_CAPA_TMP.caplate_capa_creation_date,LS_DB_CAPA.caplate_capa_completion_date = LS_DB_CAPA_TMP.caplate_capa_completion_date,LS_DB_CAPA.caplate_assignee = LS_DB_CAPA_TMP.caplate_assignee,LS_DB_CAPA.caplate_aer_version_no = LS_DB_CAPA_TMP.caplate_aer_version_no,LS_DB_CAPA.caplate_aer_no = LS_DB_CAPA_TMP.caplate_aer_no,LS_DB_CAPA.caplate_accountable = LS_DB_CAPA_TMP.caplate_accountable,LS_DB_CAPA.caplatelink_user_modified = LS_DB_CAPA_TMP.caplatelink_user_modified,LS_DB_CAPA.caplatelink_user_created = LS_DB_CAPA_TMP.caplatelink_user_created,LS_DB_CAPA.caplatelink_spr_id = LS_DB_CAPA_TMP.caplatelink_spr_id,LS_DB_CAPA.caplatelink_record_id = LS_DB_CAPA_TMP.caplatelink_record_id,LS_DB_CAPA.caplatelink_linked_receipt_no = LS_DB_CAPA_TMP.caplatelink_linked_receipt_no,LS_DB_CAPA.caplatelink_linked_aer_version = LS_DB_CAPA_TMP.caplatelink_linked_aer_version,LS_DB_CAPA.caplatelink_linked_aer_no = LS_DB_CAPA_TMP.caplatelink_linked_aer_no,LS_DB_CAPA.caplatelink_is_linked = LS_DB_CAPA_TMP.caplatelink_is_linked,LS_DB_CAPA.caplatelink_fk_capa_lateness_record_id = LS_DB_CAPA_TMP.caplatelink_fk_capa_lateness_record_id,LS_DB_CAPA.caplatelink_delay = LS_DB_CAPA_TMP.caplatelink_delay,LS_DB_CAPA.caplatelink_date_modified = LS_DB_CAPA_TMP.caplatelink_date_modified,LS_DB_CAPA.caplatelink_date_created = LS_DB_CAPA_TMP.caplatelink_date_created,
LS_DB_CAPA.PROCESSING_DT = LS_DB_CAPA_TMP.PROCESSING_DT,
LS_DB_CAPA.user_modified  =LS_DB_CAPA_TMP.user_modified     ,
LS_DB_CAPA.date_modified  =LS_DB_CAPA_TMP.date_modified     ,
LS_DB_CAPA.expiry_date    =LS_DB_CAPA_TMP.expiry_date       ,
LS_DB_CAPA.created_by     =LS_DB_CAPA_TMP.created_by        ,
LS_DB_CAPA.created_dt     =LS_DB_CAPA_TMP.created_dt        ,
LS_DB_CAPA.load_ts        =LS_DB_CAPA_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP 
WHERE 	LS_DB_CAPA.INTEGRATION_ID = LS_DB_CAPA_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CAPA_TMP.PROCESSING_DT = LS_DB_CAPA.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CAPA.prevcapa_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.rootcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.corrcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.caplatelink_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.caplate_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CAPA_TMP.prevcapa_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.rootcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.corrcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.caplatelink_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.caplate_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CAPA.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CAPA.caplate_RECORD_ID ,LS_DB_CAPA.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP 
ON LS_DB_CAPA.caplate_RECORD_ID=LS_DB_CAPA_TMP.caplate_RECORD_ID
AND LS_DB_CAPA.INTEGRATION_ID = LS_DB_CAPA_TMP.INTEGRATION_ID 
where LS_DB_CAPA_TMP.INTEGRATION_ID  is null AND LS_DB_CAPA.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CAPA.caplate_RECORD_ID in (select caplate_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP )
) TMP where LS_DB_CAPA.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CAPA.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CAPA.caplate_RECORD_ID ,LS_DB_CAPA.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP 
    ON LS_DB_CAPA.caplate_RECORD_ID=LS_DB_CAPA_TMP.caplate_RECORD_ID
    AND LS_DB_CAPA.INTEGRATION_ID = LS_DB_CAPA_TMP.INTEGRATION_ID 
    where LS_DB_CAPA_TMP.INTEGRATION_ID  is null AND LS_DB_CAPA.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CAPA.caplate_RECORD_ID in (select caplate_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,rootcap_user_modified,
rootcap_user_created,
rootcap_spr_id,
rootcap_record_id,
rootcap_fk_capa_rec_id,
rootcap_doc_source,
rootcap_doc_size,
rootcap_doc_name,
rootcap_doc_id,
rootcap_date_modified,
rootcap_date_created,
rootcap_comments,
prevcapa_user_modified,
prevcapa_user_created,
prevcapa_spr_id,
prevcapa_record_id,
prevcapa_fk_capa_rec_id,
prevcapa_doc_source,
prevcapa_doc_size,
prevcapa_doc_name,
prevcapa_doc_id,
prevcapa_date_modified,
prevcapa_date_created,
prevcapa_comments,
corrcap_user_modified,
corrcap_user_created,
corrcap_spr_id,
corrcap_record_id,
corrcap_fk_capa_rec_id,
corrcap_doc_source,
corrcap_doc_size,
corrcap_doc_name,
corrcap_doc_id,
corrcap_date_modified,
corrcap_date_created,
corrcap_comments,
caplate_user_modified,
caplate_user_created,
caplate_status_de_ml,
caplate_status,
caplate_spr_id,
caplate_reference_number,
caplate_record_id,
caplate_rca,
caplate_preventive_action,
caplate_non_compliance_iden_date,
caplate_monitoring_type_de_ml,
caplate_monitoring_type,
caplate_monitoring_timeline,
caplate_monitoring_start_date,
caplate_monitoring_end_date,
caplate_module,
caplate_lateness_reason,
caplate_late_submission_contact,
caplate_is_archived,
caplate_im_rec_id,
caplate_due_date,
caplate_desc_of_non_compliance,
caplate_delay,
caplate_date_modified,
caplate_date_created,
caplate_date_archived,
caplate_corrective_action,
caplate_comments,
caplate_category,
caplate_capa_number,
caplate_capa_creation_date,
caplate_capa_completion_date,
caplate_assignee,
caplate_aer_version_no,
caplate_aer_no,
caplate_accountable,
caplatelink_user_modified,
caplatelink_user_created,
caplatelink_spr_id,
caplatelink_record_id,
caplatelink_linked_receipt_no,
caplatelink_linked_aer_version,
caplatelink_linked_aer_no,
caplatelink_is_linked,
caplatelink_fk_capa_lateness_record_id,
caplatelink_delay,
caplatelink_date_modified,
caplatelink_date_created)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,rootcap_user_modified,
rootcap_user_created,
rootcap_spr_id,
rootcap_record_id,
rootcap_fk_capa_rec_id,
rootcap_doc_source,
rootcap_doc_size,
rootcap_doc_name,
rootcap_doc_id,
rootcap_date_modified,
rootcap_date_created,
rootcap_comments,
prevcapa_user_modified,
prevcapa_user_created,
prevcapa_spr_id,
prevcapa_record_id,
prevcapa_fk_capa_rec_id,
prevcapa_doc_source,
prevcapa_doc_size,
prevcapa_doc_name,
prevcapa_doc_id,
prevcapa_date_modified,
prevcapa_date_created,
prevcapa_comments,
corrcap_user_modified,
corrcap_user_created,
corrcap_spr_id,
corrcap_record_id,
corrcap_fk_capa_rec_id,
corrcap_doc_source,
corrcap_doc_size,
corrcap_doc_name,
corrcap_doc_id,
corrcap_date_modified,
corrcap_date_created,
corrcap_comments,
caplate_user_modified,
caplate_user_created,
caplate_status_de_ml,
caplate_status,
caplate_spr_id,
caplate_reference_number,
caplate_record_id,
caplate_rca,
caplate_preventive_action,
caplate_non_compliance_iden_date,
caplate_monitoring_type_de_ml,
caplate_monitoring_type,
caplate_monitoring_timeline,
caplate_monitoring_start_date,
caplate_monitoring_end_date,
caplate_module,
caplate_lateness_reason,
caplate_late_submission_contact,
caplate_is_archived,
caplate_im_rec_id,
caplate_due_date,
caplate_desc_of_non_compliance,
caplate_delay,
caplate_date_modified,
caplate_date_created,
caplate_date_archived,
caplate_corrective_action,
caplate_comments,
caplate_category,
caplate_capa_number,
caplate_capa_creation_date,
caplate_capa_completion_date,
caplate_assignee,
caplate_aer_version_no,
caplate_aer_no,
caplate_accountable,
caplatelink_user_modified,
caplatelink_user_created,
caplatelink_spr_id,
caplatelink_record_id,
caplatelink_linked_receipt_no,
caplatelink_linked_aer_version,
caplatelink_linked_aer_no,
caplatelink_is_linked,
caplatelink_fk_capa_lateness_record_id,
caplatelink_delay,
caplatelink_date_modified,
caplatelink_date_created
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP 
WHERE 	TO_DATE(LS_DB_CAPA.PROCESSING_DT) < TO_DATE(LS_DB_CAPA_TMP.PROCESSING_DT)
AND LS_DB_CAPA.INTEGRATION_ID = LS_DB_CAPA_TMP.INTEGRATION_ID
AND LS_DB_CAPA.caplate_RECORD_ID = LS_DB_CAPA_TMP.caplate_RECORD_ID
AND LS_DB_CAPA.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CAPA.prevcapa_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.rootcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.corrcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.caplatelink_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CAPA.caplate_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CAPA_TMP.prevcapa_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.rootcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.corrcap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.caplatelink_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CAPA_TMP.caplate_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA TGT
WHERE  ( caplatelink_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_linked_aer') OR caplate_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_records') OR corrcap_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_corrective_capa_doc') OR prevcapa_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_preventive_capa_doc') OR rootcap_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_rootcause_capa_doc')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( caplatelink_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_linked_aer') OR caplate_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_capa_lateness_records') OR corrcap_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_corrective_capa_doc') OR prevcapa_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_preventive_capa_doc') OR rootcap_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CAPA_DELETION_TMP  WHERE TABLE_NAME='lsmv_rootcause_capa_doc')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CAPA_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CAPA'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_CAPA Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CAPA'),
	'LSDB','Case','LS_DB_CAPA',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_CAPA not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
