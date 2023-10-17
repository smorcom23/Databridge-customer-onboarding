
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CASE_DETAILS()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_CASE_DETAILS'),
	'LSDB','Case','LS_DB_SUBMISSION_CASE_DETAILS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_SUBMISSION_CASE_DETAILS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SUBMISSION_CASE_DETAILS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CASE_DETAILS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CASE_DETAILS_DELETION_TMP  As select RECORD_ID,'lsmv_st_case_details' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_case_details WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_case_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CASE_DETAILS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_case_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_CASE_DETAILS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_st_case_details_SUBSET AS 
(
select * from 
    (SELECT  
    aer_id  aer_id,aer_no  aer_no,aer_owner_unit  aer_owner_unit,aggregate_report_doc_id  aggregate_report_doc_id,aggregate_report_doc_type  aggregate_report_doc_type,aggregate_report_name  aggregate_report_name,aggregate_report_size  aggregate_report_size,aggregate_report_type  aggregate_report_type,app_distributed_date  app_distributed_date,app_req_id  app_req_id,ari_rec_id  ari_rec_id,case_blinded  case_blinded,case_country  case_country,case_significance  case_significance,case_type  case_type,comments  comments,core_labelling  core_labelling,created_year  created_year,date_created  date_created,date_distributed  date_distributed,date_modified  date_modified,eu_spc_labelling  eu_spc_labelling,external_app_cm_seq_id  external_app_cm_seq_id,fk_lsip_rec_id  fk_lsip_rec_id,fl_all_submissions  fl_all_submissions,ib_labelling  ib_labelling,initial_aer_id  initial_aer_id,latest_receipt_date  latest_receipt_date,literature  literature,local_version_no  local_version_no,manual_upload  manual_upload,manual_upload_file_name  manual_upload_file_name,medically_confirmed  medically_confirmed,nullified  nullified,outbound_ref1  outbound_ref1,outbound_ref2  outbound_ref2,outbound_ref3  outbound_ref3,outbound_ref4  outbound_ref4,package_id  package_id,patient_id  patient_id,primary_reporter  primary_reporter,primary_source_country  primary_source_country,priority  priority,products_available  products_available,protocol_no  protocol_no,reactions_available  reactions_available,record_id  record_id,report_upload_date  report_upload_date,reporters_country  reporters_country,serious_reaction  serious_reaction,seriousness  seriousness,spr_id  spr_id,submission_state  submission_state,susar  susar,suspect_product  suspect_product,time_period  time_period,user_created  user_created,user_distributed  user_distributed,user_modified  user_modified,uspi_labelling  uspi_labelling,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_case_details
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CASE_DETAILS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_case_details')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_st_case_details_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_st_case_details_SUBSET.USER_CREATED CREATED_BY,lsmv_st_case_details_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_case_details_SUBSET.uspi_labelling  ,lsmv_st_case_details_SUBSET.user_modified  ,lsmv_st_case_details_SUBSET.user_distributed  ,lsmv_st_case_details_SUBSET.user_created  ,lsmv_st_case_details_SUBSET.time_period  ,lsmv_st_case_details_SUBSET.suspect_product  ,lsmv_st_case_details_SUBSET.susar  ,lsmv_st_case_details_SUBSET.submission_state  ,lsmv_st_case_details_SUBSET.spr_id  ,lsmv_st_case_details_SUBSET.seriousness  ,lsmv_st_case_details_SUBSET.serious_reaction  ,lsmv_st_case_details_SUBSET.reporters_country  ,lsmv_st_case_details_SUBSET.report_upload_date  ,lsmv_st_case_details_SUBSET.record_id  ,lsmv_st_case_details_SUBSET.reactions_available  ,lsmv_st_case_details_SUBSET.protocol_no  ,lsmv_st_case_details_SUBSET.products_available  ,lsmv_st_case_details_SUBSET.priority  ,lsmv_st_case_details_SUBSET.primary_source_country  ,lsmv_st_case_details_SUBSET.primary_reporter  ,lsmv_st_case_details_SUBSET.patient_id  ,lsmv_st_case_details_SUBSET.package_id  ,lsmv_st_case_details_SUBSET.outbound_ref4  ,lsmv_st_case_details_SUBSET.outbound_ref3  ,lsmv_st_case_details_SUBSET.outbound_ref2  ,lsmv_st_case_details_SUBSET.outbound_ref1  ,lsmv_st_case_details_SUBSET.nullified  ,lsmv_st_case_details_SUBSET.medically_confirmed  ,lsmv_st_case_details_SUBSET.manual_upload_file_name  ,lsmv_st_case_details_SUBSET.manual_upload  ,lsmv_st_case_details_SUBSET.local_version_no  ,lsmv_st_case_details_SUBSET.literature  ,lsmv_st_case_details_SUBSET.latest_receipt_date  ,lsmv_st_case_details_SUBSET.initial_aer_id  ,lsmv_st_case_details_SUBSET.ib_labelling  ,lsmv_st_case_details_SUBSET.fl_all_submissions  ,lsmv_st_case_details_SUBSET.fk_lsip_rec_id  ,lsmv_st_case_details_SUBSET.external_app_cm_seq_id  ,lsmv_st_case_details_SUBSET.eu_spc_labelling  ,lsmv_st_case_details_SUBSET.date_modified  ,lsmv_st_case_details_SUBSET.date_distributed  ,lsmv_st_case_details_SUBSET.date_created  ,lsmv_st_case_details_SUBSET.created_year  ,lsmv_st_case_details_SUBSET.core_labelling  ,lsmv_st_case_details_SUBSET.comments  ,lsmv_st_case_details_SUBSET.case_type  ,lsmv_st_case_details_SUBSET.case_significance  ,lsmv_st_case_details_SUBSET.case_country  ,lsmv_st_case_details_SUBSET.case_blinded  ,lsmv_st_case_details_SUBSET.ari_rec_id  ,lsmv_st_case_details_SUBSET.app_req_id  ,lsmv_st_case_details_SUBSET.app_distributed_date  ,lsmv_st_case_details_SUBSET.aggregate_report_type  ,lsmv_st_case_details_SUBSET.aggregate_report_size  ,lsmv_st_case_details_SUBSET.aggregate_report_name  ,lsmv_st_case_details_SUBSET.aggregate_report_doc_type  ,lsmv_st_case_details_SUBSET.aggregate_report_doc_id  ,lsmv_st_case_details_SUBSET.aer_owner_unit  ,lsmv_st_case_details_SUBSET.aer_no  ,lsmv_st_case_details_SUBSET.aer_id ,CONCAT(NVL(lsmv_st_case_details_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_case_details_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_st_case_details_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP)
where target_table_name='LS_DB_SUBMISSION_CASE_DETAILS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS   
SET LS_DB_SUBMISSION_CASE_DETAILS.uspi_labelling = LS_DB_SUBMISSION_CASE_DETAILS_TMP.uspi_labelling,LS_DB_SUBMISSION_CASE_DETAILS.user_modified = LS_DB_SUBMISSION_CASE_DETAILS_TMP.user_modified,LS_DB_SUBMISSION_CASE_DETAILS.user_distributed = LS_DB_SUBMISSION_CASE_DETAILS_TMP.user_distributed,LS_DB_SUBMISSION_CASE_DETAILS.user_created = LS_DB_SUBMISSION_CASE_DETAILS_TMP.user_created,LS_DB_SUBMISSION_CASE_DETAILS.time_period = LS_DB_SUBMISSION_CASE_DETAILS_TMP.time_period,LS_DB_SUBMISSION_CASE_DETAILS.suspect_product = LS_DB_SUBMISSION_CASE_DETAILS_TMP.suspect_product,LS_DB_SUBMISSION_CASE_DETAILS.susar = LS_DB_SUBMISSION_CASE_DETAILS_TMP.susar,LS_DB_SUBMISSION_CASE_DETAILS.submission_state = LS_DB_SUBMISSION_CASE_DETAILS_TMP.submission_state,LS_DB_SUBMISSION_CASE_DETAILS.spr_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.spr_id,LS_DB_SUBMISSION_CASE_DETAILS.seriousness = LS_DB_SUBMISSION_CASE_DETAILS_TMP.seriousness,LS_DB_SUBMISSION_CASE_DETAILS.serious_reaction = LS_DB_SUBMISSION_CASE_DETAILS_TMP.serious_reaction,LS_DB_SUBMISSION_CASE_DETAILS.reporters_country = LS_DB_SUBMISSION_CASE_DETAILS_TMP.reporters_country,LS_DB_SUBMISSION_CASE_DETAILS.report_upload_date = LS_DB_SUBMISSION_CASE_DETAILS_TMP.report_upload_date,LS_DB_SUBMISSION_CASE_DETAILS.record_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.record_id,LS_DB_SUBMISSION_CASE_DETAILS.reactions_available = LS_DB_SUBMISSION_CASE_DETAILS_TMP.reactions_available,LS_DB_SUBMISSION_CASE_DETAILS.protocol_no = LS_DB_SUBMISSION_CASE_DETAILS_TMP.protocol_no,LS_DB_SUBMISSION_CASE_DETAILS.products_available = LS_DB_SUBMISSION_CASE_DETAILS_TMP.products_available,LS_DB_SUBMISSION_CASE_DETAILS.priority = LS_DB_SUBMISSION_CASE_DETAILS_TMP.priority,LS_DB_SUBMISSION_CASE_DETAILS.primary_source_country = LS_DB_SUBMISSION_CASE_DETAILS_TMP.primary_source_country,LS_DB_SUBMISSION_CASE_DETAILS.primary_reporter = LS_DB_SUBMISSION_CASE_DETAILS_TMP.primary_reporter,LS_DB_SUBMISSION_CASE_DETAILS.patient_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.patient_id,LS_DB_SUBMISSION_CASE_DETAILS.package_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.package_id,LS_DB_SUBMISSION_CASE_DETAILS.outbound_ref4 = LS_DB_SUBMISSION_CASE_DETAILS_TMP.outbound_ref4,LS_DB_SUBMISSION_CASE_DETAILS.outbound_ref3 = LS_DB_SUBMISSION_CASE_DETAILS_TMP.outbound_ref3,LS_DB_SUBMISSION_CASE_DETAILS.outbound_ref2 = LS_DB_SUBMISSION_CASE_DETAILS_TMP.outbound_ref2,LS_DB_SUBMISSION_CASE_DETAILS.outbound_ref1 = LS_DB_SUBMISSION_CASE_DETAILS_TMP.outbound_ref1,LS_DB_SUBMISSION_CASE_DETAILS.nullified = LS_DB_SUBMISSION_CASE_DETAILS_TMP.nullified,LS_DB_SUBMISSION_CASE_DETAILS.medically_confirmed = LS_DB_SUBMISSION_CASE_DETAILS_TMP.medically_confirmed,LS_DB_SUBMISSION_CASE_DETAILS.manual_upload_file_name = LS_DB_SUBMISSION_CASE_DETAILS_TMP.manual_upload_file_name,LS_DB_SUBMISSION_CASE_DETAILS.manual_upload = LS_DB_SUBMISSION_CASE_DETAILS_TMP.manual_upload,LS_DB_SUBMISSION_CASE_DETAILS.local_version_no = LS_DB_SUBMISSION_CASE_DETAILS_TMP.local_version_no,LS_DB_SUBMISSION_CASE_DETAILS.literature = LS_DB_SUBMISSION_CASE_DETAILS_TMP.literature,LS_DB_SUBMISSION_CASE_DETAILS.latest_receipt_date = LS_DB_SUBMISSION_CASE_DETAILS_TMP.latest_receipt_date,LS_DB_SUBMISSION_CASE_DETAILS.initial_aer_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.initial_aer_id,LS_DB_SUBMISSION_CASE_DETAILS.ib_labelling = LS_DB_SUBMISSION_CASE_DETAILS_TMP.ib_labelling,LS_DB_SUBMISSION_CASE_DETAILS.fl_all_submissions = LS_DB_SUBMISSION_CASE_DETAILS_TMP.fl_all_submissions,LS_DB_SUBMISSION_CASE_DETAILS.fk_lsip_rec_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.fk_lsip_rec_id,LS_DB_SUBMISSION_CASE_DETAILS.external_app_cm_seq_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.external_app_cm_seq_id,LS_DB_SUBMISSION_CASE_DETAILS.eu_spc_labelling = LS_DB_SUBMISSION_CASE_DETAILS_TMP.eu_spc_labelling,LS_DB_SUBMISSION_CASE_DETAILS.date_modified = LS_DB_SUBMISSION_CASE_DETAILS_TMP.date_modified,LS_DB_SUBMISSION_CASE_DETAILS.date_distributed = LS_DB_SUBMISSION_CASE_DETAILS_TMP.date_distributed,LS_DB_SUBMISSION_CASE_DETAILS.date_created = LS_DB_SUBMISSION_CASE_DETAILS_TMP.date_created,LS_DB_SUBMISSION_CASE_DETAILS.created_year = LS_DB_SUBMISSION_CASE_DETAILS_TMP.created_year,LS_DB_SUBMISSION_CASE_DETAILS.core_labelling = LS_DB_SUBMISSION_CASE_DETAILS_TMP.core_labelling,LS_DB_SUBMISSION_CASE_DETAILS.comments = LS_DB_SUBMISSION_CASE_DETAILS_TMP.comments,LS_DB_SUBMISSION_CASE_DETAILS.case_type = LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_type,LS_DB_SUBMISSION_CASE_DETAILS.case_significance = LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_significance,LS_DB_SUBMISSION_CASE_DETAILS.case_country = LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_country,LS_DB_SUBMISSION_CASE_DETAILS.case_blinded = LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_blinded,LS_DB_SUBMISSION_CASE_DETAILS.ari_rec_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.ari_rec_id,LS_DB_SUBMISSION_CASE_DETAILS.app_req_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.app_req_id,LS_DB_SUBMISSION_CASE_DETAILS.app_distributed_date = LS_DB_SUBMISSION_CASE_DETAILS_TMP.app_distributed_date,LS_DB_SUBMISSION_CASE_DETAILS.aggregate_report_type = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aggregate_report_type,LS_DB_SUBMISSION_CASE_DETAILS.aggregate_report_size = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aggregate_report_size,LS_DB_SUBMISSION_CASE_DETAILS.aggregate_report_name = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aggregate_report_name,LS_DB_SUBMISSION_CASE_DETAILS.aggregate_report_doc_type = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aggregate_report_doc_type,LS_DB_SUBMISSION_CASE_DETAILS.aggregate_report_doc_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aggregate_report_doc_id,LS_DB_SUBMISSION_CASE_DETAILS.aer_owner_unit = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aer_owner_unit,LS_DB_SUBMISSION_CASE_DETAILS.aer_no = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aer_no,LS_DB_SUBMISSION_CASE_DETAILS.aer_id = LS_DB_SUBMISSION_CASE_DETAILS_TMP.aer_id,
LS_DB_SUBMISSION_CASE_DETAILS.PROCESSING_DT = LS_DB_SUBMISSION_CASE_DETAILS_TMP.PROCESSING_DT ,
LS_DB_SUBMISSION_CASE_DETAILS.receipt_id     =LS_DB_SUBMISSION_CASE_DETAILS_TMP.receipt_id        ,
LS_DB_SUBMISSION_CASE_DETAILS.case_no        =LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_no           ,
LS_DB_SUBMISSION_CASE_DETAILS.case_version   =LS_DB_SUBMISSION_CASE_DETAILS_TMP.case_version      ,
LS_DB_SUBMISSION_CASE_DETAILS.version_no     =LS_DB_SUBMISSION_CASE_DETAILS_TMP.version_no        ,
LS_DB_SUBMISSION_CASE_DETAILS.expiry_date    =LS_DB_SUBMISSION_CASE_DETAILS_TMP.expiry_date       ,
LS_DB_SUBMISSION_CASE_DETAILS.load_ts        =LS_DB_SUBMISSION_CASE_DETAILS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP 
WHERE 	LS_DB_SUBMISSION_CASE_DETAILS.INTEGRATION_ID = LS_DB_SUBMISSION_CASE_DETAILS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SUBMISSION_CASE_DETAILS_TMP.PROCESSING_DT = LS_DB_SUBMISSION_CASE_DETAILS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uspi_labelling,
user_modified,
user_distributed,
user_created,
time_period,
suspect_product,
susar,
submission_state,
spr_id,
seriousness,
serious_reaction,
reporters_country,
report_upload_date,
record_id,
reactions_available,
protocol_no,
products_available,
priority,
primary_source_country,
primary_reporter,
patient_id,
package_id,
outbound_ref4,
outbound_ref3,
outbound_ref2,
outbound_ref1,
nullified,
medically_confirmed,
manual_upload_file_name,
manual_upload,
local_version_no,
literature,
latest_receipt_date,
initial_aer_id,
ib_labelling,
fl_all_submissions,
fk_lsip_rec_id,
external_app_cm_seq_id,
eu_spc_labelling,
date_modified,
date_distributed,
date_created,
created_year,
core_labelling,
comments,
case_type,
case_significance,
case_country,
case_blinded,
ari_rec_id,
app_req_id,
app_distributed_date,
aggregate_report_type,
aggregate_report_size,
aggregate_report_name,
aggregate_report_doc_type,
aggregate_report_doc_id,
aer_owner_unit,
aer_no,
aer_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uspi_labelling,
user_modified,
user_distributed,
user_created,
time_period,
suspect_product,
susar,
submission_state,
spr_id,
seriousness,
serious_reaction,
reporters_country,
report_upload_date,
record_id,
reactions_available,
protocol_no,
products_available,
priority,
primary_source_country,
primary_reporter,
patient_id,
package_id,
outbound_ref4,
outbound_ref3,
outbound_ref2,
outbound_ref1,
nullified,
medically_confirmed,
manual_upload_file_name,
manual_upload,
local_version_no,
literature,
latest_receipt_date,
initial_aer_id,
ib_labelling,
fl_all_submissions,
fk_lsip_rec_id,
external_app_cm_seq_id,
eu_spc_labelling,
date_modified,
date_distributed,
date_created,
created_year,
core_labelling,
comments,
case_type,
case_significance,
case_country,
case_blinded,
ari_rec_id,
app_req_id,
app_distributed_date,
aggregate_report_type,
aggregate_report_size,
aggregate_report_name,
aggregate_report_doc_type,
aggregate_report_doc_id,
aer_owner_unit,
aer_no,
aer_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS TGT)
                                                                                ; 
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CASE_DETAILS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_case_details')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP 
WHERE 	TO_DATE(LS_DB_SUBMISSION_CASE_DETAILS.PROCESSING_DT) < TO_DATE(LS_DB_SUBMISSION_CASE_DETAILS_TMP.PROCESSING_DT)
AND LS_DB_SUBMISSION_CASE_DETAILS.INTEGRATION_ID = LS_DB_SUBMISSION_CASE_DETAILS_TMP.INTEGRATION_ID
AND LS_DB_SUBMISSION_CASE_DETAILS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SUBMISSION_CASE_DETAILS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_case_details')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SUBMISSION_CASE_DETAILS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SUBMISSION_CASE_DETAILS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_SUBMISSION_CASE_DETAILS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_CASE_DETAILS'),
	'LSDB','Case','LS_DB_SUBMISSION_CASE_DETAILS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_SUBMISSION_CASE_DETAILS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
