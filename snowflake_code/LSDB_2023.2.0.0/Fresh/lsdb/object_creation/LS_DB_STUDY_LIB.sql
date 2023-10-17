
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_STUDY_LIB()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_STUDY_LIB'),
	'LSDB','Case','LS_DB_STUDY_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_STUDY_LIB' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_STUDY_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_study_account_library' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_account_library WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_cross_ref_inds' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_cross_ref_inds WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_event_library' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_event_library WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_library' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_library WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_product_library' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_product_library WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_registration' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_registration WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_product_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_study_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_product_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_account_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_study_record_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_account_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_event_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_study_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_event_library WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_cross_ref_inds WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_study_record_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_cross_ref_inds WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_registration WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_study_record_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_study_registration WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_STUDY_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_study_account_library_SUBSET AS 
(
select * from 
    (SELECT  
    account_domain  studyacclib_account_domain,account_email  studyacclib_account_email,account_industry  studyacclib_account_industry,account_name  studyacclib_account_name,account_website  studyacclib_account_website,date_created  studyacclib_date_created,date_modified  studyacclib_date_modified,fk_study_account_record_id  studyacclib_fk_study_account_record_id,fk_study_record_id  studyacclib_fk_study_record_id,record_id  studyacclib_record_id,spr_id  studyacclib_spr_id,user_created  studyacclib_user_created,user_modified  studyacclib_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_account_library
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_study_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_account_library')
  ) where REC_RANK=1 )
  , lsmv_study_registration_SUBSET AS 
(
select * from 
    (SELECT  
    approval_no  stdyreg_approval_no,ari_rec_id  stdyreg_ari_rec_id,authority  stdyreg_authority,company_unit_id  stdyreg_company_unit_id,company_unit_name  stdyreg_company_unit_name,country  stdyreg_country,date_created  stdyreg_date_created,date_modified  stdyreg_date_modified,ec_reporting_flag  stdyreg_ec_reporting_flag,fk_study_record_id  stdyreg_fk_study_record_id,no_of_medicinalproduct  stdyreg_no_of_medicinalproduct,no_of_patientsenrolled  stdyreg_no_of_patientsenrolled,project_no  stdyreg_project_no,protocol_no  stdyreg_protocol_no,record_id  stdyreg_record_id,reference_or_coordinate_member_state  stdyreg_reference_or_coordinate_member_state,registration_no  stdyreg_registration_no,spr_id  stdyreg_spr_id,status_of_trial  stdyreg_status_of_trial,study_registration_date  stdyreg_study_registration_date,user_created  stdyreg_user_created,user_id  stdyreg_user_id,user_modified  stdyreg_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_registration
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_study_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration')
  ) where REC_RANK=1 )
  , lsmv_study_cross_ref_inds_SUBSET AS 
(
select * from 
    (SELECT  
    cross_ref_ind  crossrefind_cross_ref_ind,date_created  crossrefind_date_created,date_modified  crossrefind_date_modified,fk_study_record_id  crossrefind_fk_study_record_id,ps_uuid  crossrefind_ps_uuid,record_id  crossrefind_record_id,spr_id  crossrefind_spr_id,study_type  crossrefind_study_type,user_created  crossrefind_user_created,user_modified  crossrefind_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_cross_ref_inds
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_study_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_cross_ref_inds')
  ) where REC_RANK=1 )
  , lsmv_study_library_SUBSET AS 
(
select * from 
    (SELECT  
    account_record_id  stdylib_account_record_id,add_to_case  stdylib_add_to_case,all_company_unit  stdylib_all_company_unit,blinded_study  stdylib_blinded_study,blinding_technique  stdylib_blinding_technique,bu_impacted_case_rule  stdylib_bu_impacted_case_rule,bu_rule  stdylib_bu_rule,bu_wf_activity_id  stdylib_bu_wf_activity_id,bu_workflow  stdylib_bu_workflow,clinical_trial_id  stdylib_clinical_trial_id,code_broken  stdylib_code_broken,code_broken_on  stdylib_code_broken_on,ctd_to_ctr_transition_date  stdylib_ctd_to_ctr_transition_date,date_created  stdylib_date_created,date_modified  stdylib_date_modified,doc_id  stdylib_doc_id,doc_name  stdylib_doc_name,document_flag  stdylib_document_flag,euct_regulation  stdylib_euct_regulation,eudract_no  stdylib_eudract_no,global_study_enrollment_count  stdylib_global_study_enrollment_count,iis  stdylib_iis,panda_num  stdylib_panda_num,partner_record_id  stdylib_partner_record_id,primary_ind  stdylib_primary_ind,primary_test_compound  stdylib_primary_test_compound,project_no  stdylib_project_no,protocol_description  stdylib_protocol_description,protocol_details  stdylib_protocol_details,protocol_no  stdylib_protocol_no,protocol_title  stdylib_protocol_title,query_contact  stdylib_query_contact,record_id  stdylib_record_id,registration_number  stdylib_registration_number,safety_reporting_responsibility  stdylib_safety_reporting_responsibility,spr_id  stdylib_spr_id,study_acronym  stdylib_study_acronym,study_active  stdylib_study_active,study_design  stdylib_study_design,study_design_description  stdylib_study_design_description,study_end_date  stdylib_study_end_date,study_phase  stdylib_study_phase,study_source  stdylib_study_source,study_sponser  stdylib_study_sponser,study_sponser_type  stdylib_study_sponser_type,study_start_date  stdylib_study_start_date,study_type  stdylib_study_type,user_created  stdylib_user_created,user_modified  stdylib_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_library
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_library')
  ) where REC_RANK=1 )
  , lsmv_study_event_library_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  stdyevntlib_date_created,date_modified  stdyevntlib_date_modified,event  stdyevntlib_event,event_code  stdyevntlib_event_code,event_type  stdyevntlib_event_type,fk_study_rec_id  stdyevntlib_fk_study_rec_id,meddra_version  stdyevntlib_meddra_version,pt_code  stdyevntlib_pt_code,record_id  stdyevntlib_record_id,spr_id  stdyevntlib_spr_id,user_created  stdyevntlib_user_created,user_modified  stdyevntlib_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_event_library
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_study_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_event_library')
  ) where REC_RANK=1 )
  , lsmv_study_product_library_SUBSET AS 
(
select * from 
    (SELECT  
    approval_no  stdyprdlib_approval_no,approval_type  stdyprdlib_approval_type,arisg_product_id  stdyprdlib_arisg_product_id,country_code  stdyprdlib_country_code,date_created  stdyprdlib_date_created,date_modified  stdyprdlib_date_modified,fk_study_product_record_id  stdyprdlib_fk_study_product_record_id,fk_study_rec_id  stdyprdlib_fk_study_rec_id,product_name  stdyprdlib_product_name,protocol_product_type  stdyprdlib_protocol_product_type,record_id  stdyprdlib_record_id,spr_id  stdyprdlib_spr_id,study_local_tradename  stdyprdlib_study_local_tradename,study_trade_rec_id  stdyprdlib_study_trade_rec_id,user_created  stdyprdlib_user_created,user_modified  stdyprdlib_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_study_product_library
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_study_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_product_library')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_study_registration_SUBSET.stdyreg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_cross_ref_inds_SUBSET.crossrefind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_event_library_SUBSET.stdyevntlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_account_library_SUBSET.studyacclib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_product_library_SUBSET.stdyprdlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_library_SUBSET.stdylib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_study_library_SUBSET.stdylib_USER_MODIFIED USER_MODIFIED,lsmv_study_library_SUBSET.stdylib_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_study_library_SUBSET.stdylib_USER_CREATED CREATED_BY,lsmv_study_library_SUBSET.stdylib_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_study_registration_SUBSET.stdyreg_user_modified  ,lsmv_study_registration_SUBSET.stdyreg_user_id  ,lsmv_study_registration_SUBSET.stdyreg_user_created  ,lsmv_study_registration_SUBSET.stdyreg_study_registration_date  ,lsmv_study_registration_SUBSET.stdyreg_status_of_trial  ,lsmv_study_registration_SUBSET.stdyreg_spr_id  ,lsmv_study_registration_SUBSET.stdyreg_registration_no  ,lsmv_study_registration_SUBSET.stdyreg_reference_or_coordinate_member_state  ,lsmv_study_registration_SUBSET.stdyreg_record_id  ,lsmv_study_registration_SUBSET.stdyreg_protocol_no  ,lsmv_study_registration_SUBSET.stdyreg_project_no  ,lsmv_study_registration_SUBSET.stdyreg_no_of_patientsenrolled  ,lsmv_study_registration_SUBSET.stdyreg_no_of_medicinalproduct  ,lsmv_study_registration_SUBSET.stdyreg_fk_study_record_id  ,lsmv_study_registration_SUBSET.stdyreg_ec_reporting_flag  ,lsmv_study_registration_SUBSET.stdyreg_date_modified  ,lsmv_study_registration_SUBSET.stdyreg_date_created  ,lsmv_study_registration_SUBSET.stdyreg_country  ,lsmv_study_registration_SUBSET.stdyreg_company_unit_name  ,lsmv_study_registration_SUBSET.stdyreg_company_unit_id  ,lsmv_study_registration_SUBSET.stdyreg_authority  ,lsmv_study_registration_SUBSET.stdyreg_ari_rec_id  ,lsmv_study_registration_SUBSET.stdyreg_approval_no  ,lsmv_study_product_library_SUBSET.stdyprdlib_user_modified  ,lsmv_study_product_library_SUBSET.stdyprdlib_user_created  ,lsmv_study_product_library_SUBSET.stdyprdlib_study_trade_rec_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_study_local_tradename  ,lsmv_study_product_library_SUBSET.stdyprdlib_spr_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_record_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_protocol_product_type  ,lsmv_study_product_library_SUBSET.stdyprdlib_product_name  ,lsmv_study_product_library_SUBSET.stdyprdlib_fk_study_rec_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_fk_study_product_record_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_date_modified  ,lsmv_study_product_library_SUBSET.stdyprdlib_date_created  ,lsmv_study_product_library_SUBSET.stdyprdlib_country_code  ,lsmv_study_product_library_SUBSET.stdyprdlib_arisg_product_id  ,lsmv_study_product_library_SUBSET.stdyprdlib_approval_type  ,lsmv_study_product_library_SUBSET.stdyprdlib_approval_no  ,lsmv_study_library_SUBSET.stdylib_user_modified  ,lsmv_study_library_SUBSET.stdylib_user_created  ,lsmv_study_library_SUBSET.stdylib_study_type  ,lsmv_study_library_SUBSET.stdylib_study_start_date  ,lsmv_study_library_SUBSET.stdylib_study_sponser_type  ,lsmv_study_library_SUBSET.stdylib_study_sponser  ,lsmv_study_library_SUBSET.stdylib_study_source  ,lsmv_study_library_SUBSET.stdylib_study_phase  ,lsmv_study_library_SUBSET.stdylib_study_end_date  ,lsmv_study_library_SUBSET.stdylib_study_design_description  ,lsmv_study_library_SUBSET.stdylib_study_design  ,lsmv_study_library_SUBSET.stdylib_study_active  ,lsmv_study_library_SUBSET.stdylib_study_acronym  ,lsmv_study_library_SUBSET.stdylib_spr_id  ,lsmv_study_library_SUBSET.stdylib_safety_reporting_responsibility  ,lsmv_study_library_SUBSET.stdylib_registration_number  ,lsmv_study_library_SUBSET.stdylib_record_id  ,lsmv_study_library_SUBSET.stdylib_query_contact  ,lsmv_study_library_SUBSET.stdylib_protocol_title  ,lsmv_study_library_SUBSET.stdylib_protocol_no  ,lsmv_study_library_SUBSET.stdylib_protocol_details  ,lsmv_study_library_SUBSET.stdylib_protocol_description  ,lsmv_study_library_SUBSET.stdylib_project_no  ,lsmv_study_library_SUBSET.stdylib_primary_test_compound  ,lsmv_study_library_SUBSET.stdylib_primary_ind  ,lsmv_study_library_SUBSET.stdylib_partner_record_id  ,lsmv_study_library_SUBSET.stdylib_panda_num  ,lsmv_study_library_SUBSET.stdylib_iis  ,lsmv_study_library_SUBSET.stdylib_global_study_enrollment_count  ,lsmv_study_library_SUBSET.stdylib_eudract_no  ,lsmv_study_library_SUBSET.stdylib_euct_regulation  ,lsmv_study_library_SUBSET.stdylib_document_flag  ,lsmv_study_library_SUBSET.stdylib_doc_name  ,lsmv_study_library_SUBSET.stdylib_doc_id  ,lsmv_study_library_SUBSET.stdylib_date_modified  ,lsmv_study_library_SUBSET.stdylib_date_created  ,lsmv_study_library_SUBSET.stdylib_ctd_to_ctr_transition_date  ,lsmv_study_library_SUBSET.stdylib_code_broken_on  ,lsmv_study_library_SUBSET.stdylib_code_broken  ,lsmv_study_library_SUBSET.stdylib_clinical_trial_id  ,lsmv_study_library_SUBSET.stdylib_bu_workflow  ,lsmv_study_library_SUBSET.stdylib_bu_wf_activity_id  ,lsmv_study_library_SUBSET.stdylib_bu_rule  ,lsmv_study_library_SUBSET.stdylib_bu_impacted_case_rule  ,lsmv_study_library_SUBSET.stdylib_blinding_technique  ,lsmv_study_library_SUBSET.stdylib_blinded_study  ,lsmv_study_library_SUBSET.stdylib_all_company_unit  ,lsmv_study_library_SUBSET.stdylib_add_to_case  ,lsmv_study_library_SUBSET.stdylib_account_record_id  ,lsmv_study_event_library_SUBSET.stdyevntlib_user_modified  ,lsmv_study_event_library_SUBSET.stdyevntlib_user_created  ,lsmv_study_event_library_SUBSET.stdyevntlib_spr_id  ,lsmv_study_event_library_SUBSET.stdyevntlib_record_id  ,lsmv_study_event_library_SUBSET.stdyevntlib_pt_code  ,lsmv_study_event_library_SUBSET.stdyevntlib_meddra_version  ,lsmv_study_event_library_SUBSET.stdyevntlib_fk_study_rec_id  ,lsmv_study_event_library_SUBSET.stdyevntlib_event_type  ,lsmv_study_event_library_SUBSET.stdyevntlib_event_code  ,lsmv_study_event_library_SUBSET.stdyevntlib_event  ,lsmv_study_event_library_SUBSET.stdyevntlib_date_modified  ,lsmv_study_event_library_SUBSET.stdyevntlib_date_created  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_user_modified  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_user_created  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_study_type  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_spr_id  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_record_id  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_ps_uuid  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_fk_study_record_id  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_date_modified  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_date_created  ,lsmv_study_cross_ref_inds_SUBSET.crossrefind_cross_ref_ind  ,lsmv_study_account_library_SUBSET.studyacclib_user_modified  ,lsmv_study_account_library_SUBSET.studyacclib_user_created  ,lsmv_study_account_library_SUBSET.studyacclib_spr_id  ,lsmv_study_account_library_SUBSET.studyacclib_record_id  ,lsmv_study_account_library_SUBSET.studyacclib_fk_study_record_id  ,lsmv_study_account_library_SUBSET.studyacclib_fk_study_account_record_id  ,lsmv_study_account_library_SUBSET.studyacclib_date_modified  ,lsmv_study_account_library_SUBSET.studyacclib_date_created  ,lsmv_study_account_library_SUBSET.studyacclib_account_website  ,lsmv_study_account_library_SUBSET.studyacclib_account_name  ,lsmv_study_account_library_SUBSET.studyacclib_account_industry  ,lsmv_study_account_library_SUBSET.studyacclib_account_email  ,lsmv_study_account_library_SUBSET.studyacclib_account_domain ,CONCAT(NVL(lsmv_study_registration_SUBSET.stdyreg_RECORD_ID,-1),'||',NVL(lsmv_study_cross_ref_inds_SUBSET.crossrefind_RECORD_ID,-1),'||',NVL(lsmv_study_event_library_SUBSET.stdyevntlib_RECORD_ID,-1),'||',NVL(lsmv_study_account_library_SUBSET.studyacclib_RECORD_ID,-1),'||',NVL(lsmv_study_product_library_SUBSET.stdyprdlib_RECORD_ID,-1),'||',NVL(lsmv_study_library_SUBSET.stdylib_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_study_library_SUBSET  LEFT JOIN lsmv_study_registration_SUBSET ON lsmv_study_library_SUBSET.stdylib_record_id=lsmv_study_registration_SUBSET.stdyreg_fk_study_record_id
                         LEFT JOIN lsmv_study_account_library_SUBSET ON lsmv_study_library_SUBSET.stdylib_RECORD_ID=lsmv_study_account_library_SUBSET.studyacclib_fk_study_record_id
                         LEFT JOIN lsmv_study_cross_ref_inds_SUBSET ON lsmv_study_library_SUBSET.stdylib_RECORD_ID=lsmv_study_cross_ref_inds_SUBSET.crossrefind_fk_study_record_id
                         LEFT JOIN lsmv_study_event_library_SUBSET ON lsmv_study_library_SUBSET.stdylib_RECORD_ID=lsmv_study_event_library_SUBSET.stdyevntlib_fk_study_rec_id
                         LEFT JOIN lsmv_study_product_library_SUBSET ON lsmv_study_library_SUBSET.stdylib_RECORD_ID=lsmv_study_product_library_SUBSET.stdyprdlib_fk_study_rec_id
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP)
where target_table_name='LS_DB_STUDY_LIB'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB   
SET LS_DB_STUDY_LIB.stdyreg_user_modified = LS_DB_STUDY_LIB_TMP.stdyreg_user_modified,LS_DB_STUDY_LIB.stdyreg_user_id = LS_DB_STUDY_LIB_TMP.stdyreg_user_id,LS_DB_STUDY_LIB.stdyreg_user_created = LS_DB_STUDY_LIB_TMP.stdyreg_user_created,LS_DB_STUDY_LIB.stdyreg_study_registration_date = LS_DB_STUDY_LIB_TMP.stdyreg_study_registration_date,LS_DB_STUDY_LIB.stdyreg_status_of_trial = LS_DB_STUDY_LIB_TMP.stdyreg_status_of_trial,LS_DB_STUDY_LIB.stdyreg_spr_id = LS_DB_STUDY_LIB_TMP.stdyreg_spr_id,LS_DB_STUDY_LIB.stdyreg_registration_no = LS_DB_STUDY_LIB_TMP.stdyreg_registration_no,LS_DB_STUDY_LIB.stdyreg_reference_or_coordinate_member_state = LS_DB_STUDY_LIB_TMP.stdyreg_reference_or_coordinate_member_state,LS_DB_STUDY_LIB.stdyreg_record_id = LS_DB_STUDY_LIB_TMP.stdyreg_record_id,LS_DB_STUDY_LIB.stdyreg_protocol_no = LS_DB_STUDY_LIB_TMP.stdyreg_protocol_no,LS_DB_STUDY_LIB.stdyreg_project_no = LS_DB_STUDY_LIB_TMP.stdyreg_project_no,LS_DB_STUDY_LIB.stdyreg_no_of_patientsenrolled = LS_DB_STUDY_LIB_TMP.stdyreg_no_of_patientsenrolled,LS_DB_STUDY_LIB.stdyreg_no_of_medicinalproduct = LS_DB_STUDY_LIB_TMP.stdyreg_no_of_medicinalproduct,LS_DB_STUDY_LIB.stdyreg_fk_study_record_id = LS_DB_STUDY_LIB_TMP.stdyreg_fk_study_record_id,LS_DB_STUDY_LIB.stdyreg_ec_reporting_flag = LS_DB_STUDY_LIB_TMP.stdyreg_ec_reporting_flag,LS_DB_STUDY_LIB.stdyreg_date_modified = LS_DB_STUDY_LIB_TMP.stdyreg_date_modified,LS_DB_STUDY_LIB.stdyreg_date_created = LS_DB_STUDY_LIB_TMP.stdyreg_date_created,LS_DB_STUDY_LIB.stdyreg_country = LS_DB_STUDY_LIB_TMP.stdyreg_country,LS_DB_STUDY_LIB.stdyreg_company_unit_name = LS_DB_STUDY_LIB_TMP.stdyreg_company_unit_name,LS_DB_STUDY_LIB.stdyreg_company_unit_id = LS_DB_STUDY_LIB_TMP.stdyreg_company_unit_id,LS_DB_STUDY_LIB.stdyreg_authority = LS_DB_STUDY_LIB_TMP.stdyreg_authority,LS_DB_STUDY_LIB.stdyreg_ari_rec_id = LS_DB_STUDY_LIB_TMP.stdyreg_ari_rec_id,LS_DB_STUDY_LIB.stdyreg_approval_no = LS_DB_STUDY_LIB_TMP.stdyreg_approval_no,LS_DB_STUDY_LIB.stdyprdlib_user_modified = LS_DB_STUDY_LIB_TMP.stdyprdlib_user_modified,LS_DB_STUDY_LIB.stdyprdlib_user_created = LS_DB_STUDY_LIB_TMP.stdyprdlib_user_created,LS_DB_STUDY_LIB.stdyprdlib_study_trade_rec_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_study_trade_rec_id,LS_DB_STUDY_LIB.stdyprdlib_study_local_tradename = LS_DB_STUDY_LIB_TMP.stdyprdlib_study_local_tradename,LS_DB_STUDY_LIB.stdyprdlib_spr_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_spr_id,LS_DB_STUDY_LIB.stdyprdlib_record_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_record_id,LS_DB_STUDY_LIB.stdyprdlib_protocol_product_type = LS_DB_STUDY_LIB_TMP.stdyprdlib_protocol_product_type,LS_DB_STUDY_LIB.stdyprdlib_product_name = LS_DB_STUDY_LIB_TMP.stdyprdlib_product_name,LS_DB_STUDY_LIB.stdyprdlib_fk_study_rec_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_fk_study_rec_id,LS_DB_STUDY_LIB.stdyprdlib_fk_study_product_record_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_fk_study_product_record_id,LS_DB_STUDY_LIB.stdyprdlib_date_modified = LS_DB_STUDY_LIB_TMP.stdyprdlib_date_modified,LS_DB_STUDY_LIB.stdyprdlib_date_created = LS_DB_STUDY_LIB_TMP.stdyprdlib_date_created,LS_DB_STUDY_LIB.stdyprdlib_country_code = LS_DB_STUDY_LIB_TMP.stdyprdlib_country_code,LS_DB_STUDY_LIB.stdyprdlib_arisg_product_id = LS_DB_STUDY_LIB_TMP.stdyprdlib_arisg_product_id,LS_DB_STUDY_LIB.stdyprdlib_approval_type = LS_DB_STUDY_LIB_TMP.stdyprdlib_approval_type,LS_DB_STUDY_LIB.stdyprdlib_approval_no = LS_DB_STUDY_LIB_TMP.stdyprdlib_approval_no,LS_DB_STUDY_LIB.stdylib_user_modified = LS_DB_STUDY_LIB_TMP.stdylib_user_modified,LS_DB_STUDY_LIB.stdylib_user_created = LS_DB_STUDY_LIB_TMP.stdylib_user_created,LS_DB_STUDY_LIB.stdylib_study_type = LS_DB_STUDY_LIB_TMP.stdylib_study_type,LS_DB_STUDY_LIB.stdylib_study_start_date = LS_DB_STUDY_LIB_TMP.stdylib_study_start_date,LS_DB_STUDY_LIB.stdylib_study_sponser_type = LS_DB_STUDY_LIB_TMP.stdylib_study_sponser_type,LS_DB_STUDY_LIB.stdylib_study_sponser = LS_DB_STUDY_LIB_TMP.stdylib_study_sponser,LS_DB_STUDY_LIB.stdylib_study_source = LS_DB_STUDY_LIB_TMP.stdylib_study_source,LS_DB_STUDY_LIB.stdylib_study_phase = LS_DB_STUDY_LIB_TMP.stdylib_study_phase,LS_DB_STUDY_LIB.stdylib_study_end_date = LS_DB_STUDY_LIB_TMP.stdylib_study_end_date,LS_DB_STUDY_LIB.stdylib_study_design_description = LS_DB_STUDY_LIB_TMP.stdylib_study_design_description,LS_DB_STUDY_LIB.stdylib_study_design = LS_DB_STUDY_LIB_TMP.stdylib_study_design,LS_DB_STUDY_LIB.stdylib_study_active = LS_DB_STUDY_LIB_TMP.stdylib_study_active,LS_DB_STUDY_LIB.stdylib_study_acronym = LS_DB_STUDY_LIB_TMP.stdylib_study_acronym,LS_DB_STUDY_LIB.stdylib_spr_id = LS_DB_STUDY_LIB_TMP.stdylib_spr_id,LS_DB_STUDY_LIB.stdylib_safety_reporting_responsibility = LS_DB_STUDY_LIB_TMP.stdylib_safety_reporting_responsibility,LS_DB_STUDY_LIB.stdylib_registration_number = LS_DB_STUDY_LIB_TMP.stdylib_registration_number,LS_DB_STUDY_LIB.stdylib_record_id = LS_DB_STUDY_LIB_TMP.stdylib_record_id,LS_DB_STUDY_LIB.stdylib_query_contact = LS_DB_STUDY_LIB_TMP.stdylib_query_contact,LS_DB_STUDY_LIB.stdylib_protocol_title = LS_DB_STUDY_LIB_TMP.stdylib_protocol_title,LS_DB_STUDY_LIB.stdylib_protocol_no = LS_DB_STUDY_LIB_TMP.stdylib_protocol_no,LS_DB_STUDY_LIB.stdylib_protocol_details = LS_DB_STUDY_LIB_TMP.stdylib_protocol_details,LS_DB_STUDY_LIB.stdylib_protocol_description = LS_DB_STUDY_LIB_TMP.stdylib_protocol_description,LS_DB_STUDY_LIB.stdylib_project_no = LS_DB_STUDY_LIB_TMP.stdylib_project_no,LS_DB_STUDY_LIB.stdylib_primary_test_compound = LS_DB_STUDY_LIB_TMP.stdylib_primary_test_compound,LS_DB_STUDY_LIB.stdylib_primary_ind = LS_DB_STUDY_LIB_TMP.stdylib_primary_ind,LS_DB_STUDY_LIB.stdylib_partner_record_id = LS_DB_STUDY_LIB_TMP.stdylib_partner_record_id,LS_DB_STUDY_LIB.stdylib_panda_num = LS_DB_STUDY_LIB_TMP.stdylib_panda_num,LS_DB_STUDY_LIB.stdylib_iis = LS_DB_STUDY_LIB_TMP.stdylib_iis,LS_DB_STUDY_LIB.stdylib_global_study_enrollment_count = LS_DB_STUDY_LIB_TMP.stdylib_global_study_enrollment_count,LS_DB_STUDY_LIB.stdylib_eudract_no = LS_DB_STUDY_LIB_TMP.stdylib_eudract_no,LS_DB_STUDY_LIB.stdylib_euct_regulation = LS_DB_STUDY_LIB_TMP.stdylib_euct_regulation,LS_DB_STUDY_LIB.stdylib_document_flag = LS_DB_STUDY_LIB_TMP.stdylib_document_flag,LS_DB_STUDY_LIB.stdylib_doc_name = LS_DB_STUDY_LIB_TMP.stdylib_doc_name,LS_DB_STUDY_LIB.stdylib_doc_id = LS_DB_STUDY_LIB_TMP.stdylib_doc_id,LS_DB_STUDY_LIB.stdylib_date_modified = LS_DB_STUDY_LIB_TMP.stdylib_date_modified,LS_DB_STUDY_LIB.stdylib_date_created = LS_DB_STUDY_LIB_TMP.stdylib_date_created,LS_DB_STUDY_LIB.stdylib_ctd_to_ctr_transition_date = LS_DB_STUDY_LIB_TMP.stdylib_ctd_to_ctr_transition_date,LS_DB_STUDY_LIB.stdylib_code_broken_on = LS_DB_STUDY_LIB_TMP.stdylib_code_broken_on,LS_DB_STUDY_LIB.stdylib_code_broken = LS_DB_STUDY_LIB_TMP.stdylib_code_broken,LS_DB_STUDY_LIB.stdylib_clinical_trial_id = LS_DB_STUDY_LIB_TMP.stdylib_clinical_trial_id,LS_DB_STUDY_LIB.stdylib_bu_workflow = LS_DB_STUDY_LIB_TMP.stdylib_bu_workflow,LS_DB_STUDY_LIB.stdylib_bu_wf_activity_id = LS_DB_STUDY_LIB_TMP.stdylib_bu_wf_activity_id,LS_DB_STUDY_LIB.stdylib_bu_rule = LS_DB_STUDY_LIB_TMP.stdylib_bu_rule,LS_DB_STUDY_LIB.stdylib_bu_impacted_case_rule = LS_DB_STUDY_LIB_TMP.stdylib_bu_impacted_case_rule,LS_DB_STUDY_LIB.stdylib_blinding_technique = LS_DB_STUDY_LIB_TMP.stdylib_blinding_technique,LS_DB_STUDY_LIB.stdylib_blinded_study = LS_DB_STUDY_LIB_TMP.stdylib_blinded_study,LS_DB_STUDY_LIB.stdylib_all_company_unit = LS_DB_STUDY_LIB_TMP.stdylib_all_company_unit,LS_DB_STUDY_LIB.stdylib_add_to_case = LS_DB_STUDY_LIB_TMP.stdylib_add_to_case,LS_DB_STUDY_LIB.stdylib_account_record_id = LS_DB_STUDY_LIB_TMP.stdylib_account_record_id,LS_DB_STUDY_LIB.stdyevntlib_user_modified = LS_DB_STUDY_LIB_TMP.stdyevntlib_user_modified,LS_DB_STUDY_LIB.stdyevntlib_user_created = LS_DB_STUDY_LIB_TMP.stdyevntlib_user_created,LS_DB_STUDY_LIB.stdyevntlib_spr_id = LS_DB_STUDY_LIB_TMP.stdyevntlib_spr_id,LS_DB_STUDY_LIB.stdyevntlib_record_id = LS_DB_STUDY_LIB_TMP.stdyevntlib_record_id,LS_DB_STUDY_LIB.stdyevntlib_pt_code = LS_DB_STUDY_LIB_TMP.stdyevntlib_pt_code,LS_DB_STUDY_LIB.stdyevntlib_meddra_version = LS_DB_STUDY_LIB_TMP.stdyevntlib_meddra_version,LS_DB_STUDY_LIB.stdyevntlib_fk_study_rec_id = LS_DB_STUDY_LIB_TMP.stdyevntlib_fk_study_rec_id,LS_DB_STUDY_LIB.stdyevntlib_event_type = LS_DB_STUDY_LIB_TMP.stdyevntlib_event_type,LS_DB_STUDY_LIB.stdyevntlib_event_code = LS_DB_STUDY_LIB_TMP.stdyevntlib_event_code,LS_DB_STUDY_LIB.stdyevntlib_event = LS_DB_STUDY_LIB_TMP.stdyevntlib_event,LS_DB_STUDY_LIB.stdyevntlib_date_modified = LS_DB_STUDY_LIB_TMP.stdyevntlib_date_modified,LS_DB_STUDY_LIB.stdyevntlib_date_created = LS_DB_STUDY_LIB_TMP.stdyevntlib_date_created,LS_DB_STUDY_LIB.crossrefind_user_modified = LS_DB_STUDY_LIB_TMP.crossrefind_user_modified,LS_DB_STUDY_LIB.crossrefind_user_created = LS_DB_STUDY_LIB_TMP.crossrefind_user_created,LS_DB_STUDY_LIB.crossrefind_study_type = LS_DB_STUDY_LIB_TMP.crossrefind_study_type,LS_DB_STUDY_LIB.crossrefind_spr_id = LS_DB_STUDY_LIB_TMP.crossrefind_spr_id,LS_DB_STUDY_LIB.crossrefind_record_id = LS_DB_STUDY_LIB_TMP.crossrefind_record_id,LS_DB_STUDY_LIB.crossrefind_ps_uuid = LS_DB_STUDY_LIB_TMP.crossrefind_ps_uuid,LS_DB_STUDY_LIB.crossrefind_fk_study_record_id = LS_DB_STUDY_LIB_TMP.crossrefind_fk_study_record_id,LS_DB_STUDY_LIB.crossrefind_date_modified = LS_DB_STUDY_LIB_TMP.crossrefind_date_modified,LS_DB_STUDY_LIB.crossrefind_date_created = LS_DB_STUDY_LIB_TMP.crossrefind_date_created,LS_DB_STUDY_LIB.crossrefind_cross_ref_ind = LS_DB_STUDY_LIB_TMP.crossrefind_cross_ref_ind,LS_DB_STUDY_LIB.studyacclib_user_modified = LS_DB_STUDY_LIB_TMP.studyacclib_user_modified,LS_DB_STUDY_LIB.studyacclib_user_created = LS_DB_STUDY_LIB_TMP.studyacclib_user_created,LS_DB_STUDY_LIB.studyacclib_spr_id = LS_DB_STUDY_LIB_TMP.studyacclib_spr_id,LS_DB_STUDY_LIB.studyacclib_record_id = LS_DB_STUDY_LIB_TMP.studyacclib_record_id,LS_DB_STUDY_LIB.studyacclib_fk_study_record_id = LS_DB_STUDY_LIB_TMP.studyacclib_fk_study_record_id,LS_DB_STUDY_LIB.studyacclib_fk_study_account_record_id = LS_DB_STUDY_LIB_TMP.studyacclib_fk_study_account_record_id,LS_DB_STUDY_LIB.studyacclib_date_modified = LS_DB_STUDY_LIB_TMP.studyacclib_date_modified,LS_DB_STUDY_LIB.studyacclib_date_created = LS_DB_STUDY_LIB_TMP.studyacclib_date_created,LS_DB_STUDY_LIB.studyacclib_account_website = LS_DB_STUDY_LIB_TMP.studyacclib_account_website,LS_DB_STUDY_LIB.studyacclib_account_name = LS_DB_STUDY_LIB_TMP.studyacclib_account_name,LS_DB_STUDY_LIB.studyacclib_account_industry = LS_DB_STUDY_LIB_TMP.studyacclib_account_industry,LS_DB_STUDY_LIB.studyacclib_account_email = LS_DB_STUDY_LIB_TMP.studyacclib_account_email,LS_DB_STUDY_LIB.studyacclib_account_domain = LS_DB_STUDY_LIB_TMP.studyacclib_account_domain,
LS_DB_STUDY_LIB.PROCESSING_DT = LS_DB_STUDY_LIB_TMP.PROCESSING_DT,
LS_DB_STUDY_LIB.user_modified  =LS_DB_STUDY_LIB_TMP.user_modified     ,
LS_DB_STUDY_LIB.date_modified  =LS_DB_STUDY_LIB_TMP.date_modified     ,
LS_DB_STUDY_LIB.expiry_date    =LS_DB_STUDY_LIB_TMP.expiry_date       ,
LS_DB_STUDY_LIB.created_by     =LS_DB_STUDY_LIB_TMP.created_by        ,
LS_DB_STUDY_LIB.created_dt     =LS_DB_STUDY_LIB_TMP.created_dt        ,
LS_DB_STUDY_LIB.load_ts        =LS_DB_STUDY_LIB_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP 
WHERE 	LS_DB_STUDY_LIB.INTEGRATION_ID = LS_DB_STUDY_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_STUDY_LIB_TMP.PROCESSING_DT = LS_DB_STUDY_LIB.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_STUDY_LIB.stdyreg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.crossrefind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdyevntlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.studyacclib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdyprdlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdylib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_STUDY_LIB_TMP.stdyreg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.crossrefind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdyevntlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.studyacclib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdyprdlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdylib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_STUDY_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_STUDY_LIB.stdylib_RECORD_ID ,LS_DB_STUDY_LIB.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP 
ON LS_DB_STUDY_LIB.stdylib_RECORD_ID=LS_DB_STUDY_LIB_TMP.stdylib_RECORD_ID
AND LS_DB_STUDY_LIB.INTEGRATION_ID = LS_DB_STUDY_LIB_TMP.INTEGRATION_ID 
where LS_DB_STUDY_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_STUDY_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_STUDY_LIB.stdylib_RECORD_ID in (select stdylib_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP )
) TMP where LS_DB_STUDY_LIB.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_STUDY_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_STUDY_LIB.stdylib_RECORD_ID ,LS_DB_STUDY_LIB.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP 
    ON LS_DB_STUDY_LIB.stdylib_RECORD_ID=LS_DB_STUDY_LIB_TMP.stdylib_RECORD_ID
    AND LS_DB_STUDY_LIB.INTEGRATION_ID = LS_DB_STUDY_LIB_TMP.INTEGRATION_ID 
    where LS_DB_STUDY_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_STUDY_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_STUDY_LIB.stdylib_RECORD_ID in (select stdylib_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,stdyreg_user_modified,
stdyreg_user_id,
stdyreg_user_created,
stdyreg_study_registration_date,
stdyreg_status_of_trial,
stdyreg_spr_id,
stdyreg_registration_no,
stdyreg_reference_or_coordinate_member_state,
stdyreg_record_id,
stdyreg_protocol_no,
stdyreg_project_no,
stdyreg_no_of_patientsenrolled,
stdyreg_no_of_medicinalproduct,
stdyreg_fk_study_record_id,
stdyreg_ec_reporting_flag,
stdyreg_date_modified,
stdyreg_date_created,
stdyreg_country,
stdyreg_company_unit_name,
stdyreg_company_unit_id,
stdyreg_authority,
stdyreg_ari_rec_id,
stdyreg_approval_no,
stdyprdlib_user_modified,
stdyprdlib_user_created,
stdyprdlib_study_trade_rec_id,
stdyprdlib_study_local_tradename,
stdyprdlib_spr_id,
stdyprdlib_record_id,
stdyprdlib_protocol_product_type,
stdyprdlib_product_name,
stdyprdlib_fk_study_rec_id,
stdyprdlib_fk_study_product_record_id,
stdyprdlib_date_modified,
stdyprdlib_date_created,
stdyprdlib_country_code,
stdyprdlib_arisg_product_id,
stdyprdlib_approval_type,
stdyprdlib_approval_no,
stdylib_user_modified,
stdylib_user_created,
stdylib_study_type,
stdylib_study_start_date,
stdylib_study_sponser_type,
stdylib_study_sponser,
stdylib_study_source,
stdylib_study_phase,
stdylib_study_end_date,
stdylib_study_design_description,
stdylib_study_design,
stdylib_study_active,
stdylib_study_acronym,
stdylib_spr_id,
stdylib_safety_reporting_responsibility,
stdylib_registration_number,
stdylib_record_id,
stdylib_query_contact,
stdylib_protocol_title,
stdylib_protocol_no,
stdylib_protocol_details,
stdylib_protocol_description,
stdylib_project_no,
stdylib_primary_test_compound,
stdylib_primary_ind,
stdylib_partner_record_id,
stdylib_panda_num,
stdylib_iis,
stdylib_global_study_enrollment_count,
stdylib_eudract_no,
stdylib_euct_regulation,
stdylib_document_flag,
stdylib_doc_name,
stdylib_doc_id,
stdylib_date_modified,
stdylib_date_created,
stdylib_ctd_to_ctr_transition_date,
stdylib_code_broken_on,
stdylib_code_broken,
stdylib_clinical_trial_id,
stdylib_bu_workflow,
stdylib_bu_wf_activity_id,
stdylib_bu_rule,
stdylib_bu_impacted_case_rule,
stdylib_blinding_technique,
stdylib_blinded_study,
stdylib_all_company_unit,
stdylib_add_to_case,
stdylib_account_record_id,
stdyevntlib_user_modified,
stdyevntlib_user_created,
stdyevntlib_spr_id,
stdyevntlib_record_id,
stdyevntlib_pt_code,
stdyevntlib_meddra_version,
stdyevntlib_fk_study_rec_id,
stdyevntlib_event_type,
stdyevntlib_event_code,
stdyevntlib_event,
stdyevntlib_date_modified,
stdyevntlib_date_created,
crossrefind_user_modified,
crossrefind_user_created,
crossrefind_study_type,
crossrefind_spr_id,
crossrefind_record_id,
crossrefind_ps_uuid,
crossrefind_fk_study_record_id,
crossrefind_date_modified,
crossrefind_date_created,
crossrefind_cross_ref_ind,
studyacclib_user_modified,
studyacclib_user_created,
studyacclib_spr_id,
studyacclib_record_id,
studyacclib_fk_study_record_id,
studyacclib_fk_study_account_record_id,
studyacclib_date_modified,
studyacclib_date_created,
studyacclib_account_website,
studyacclib_account_name,
studyacclib_account_industry,
studyacclib_account_email,
studyacclib_account_domain)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,stdyreg_user_modified,
stdyreg_user_id,
stdyreg_user_created,
stdyreg_study_registration_date,
stdyreg_status_of_trial,
stdyreg_spr_id,
stdyreg_registration_no,
stdyreg_reference_or_coordinate_member_state,
stdyreg_record_id,
stdyreg_protocol_no,
stdyreg_project_no,
stdyreg_no_of_patientsenrolled,
stdyreg_no_of_medicinalproduct,
stdyreg_fk_study_record_id,
stdyreg_ec_reporting_flag,
stdyreg_date_modified,
stdyreg_date_created,
stdyreg_country,
stdyreg_company_unit_name,
stdyreg_company_unit_id,
stdyreg_authority,
stdyreg_ari_rec_id,
stdyreg_approval_no,
stdyprdlib_user_modified,
stdyprdlib_user_created,
stdyprdlib_study_trade_rec_id,
stdyprdlib_study_local_tradename,
stdyprdlib_spr_id,
stdyprdlib_record_id,
stdyprdlib_protocol_product_type,
stdyprdlib_product_name,
stdyprdlib_fk_study_rec_id,
stdyprdlib_fk_study_product_record_id,
stdyprdlib_date_modified,
stdyprdlib_date_created,
stdyprdlib_country_code,
stdyprdlib_arisg_product_id,
stdyprdlib_approval_type,
stdyprdlib_approval_no,
stdylib_user_modified,
stdylib_user_created,
stdylib_study_type,
stdylib_study_start_date,
stdylib_study_sponser_type,
stdylib_study_sponser,
stdylib_study_source,
stdylib_study_phase,
stdylib_study_end_date,
stdylib_study_design_description,
stdylib_study_design,
stdylib_study_active,
stdylib_study_acronym,
stdylib_spr_id,
stdylib_safety_reporting_responsibility,
stdylib_registration_number,
stdylib_record_id,
stdylib_query_contact,
stdylib_protocol_title,
stdylib_protocol_no,
stdylib_protocol_details,
stdylib_protocol_description,
stdylib_project_no,
stdylib_primary_test_compound,
stdylib_primary_ind,
stdylib_partner_record_id,
stdylib_panda_num,
stdylib_iis,
stdylib_global_study_enrollment_count,
stdylib_eudract_no,
stdylib_euct_regulation,
stdylib_document_flag,
stdylib_doc_name,
stdylib_doc_id,
stdylib_date_modified,
stdylib_date_created,
stdylib_ctd_to_ctr_transition_date,
stdylib_code_broken_on,
stdylib_code_broken,
stdylib_clinical_trial_id,
stdylib_bu_workflow,
stdylib_bu_wf_activity_id,
stdylib_bu_rule,
stdylib_bu_impacted_case_rule,
stdylib_blinding_technique,
stdylib_blinded_study,
stdylib_all_company_unit,
stdylib_add_to_case,
stdylib_account_record_id,
stdyevntlib_user_modified,
stdyevntlib_user_created,
stdyevntlib_spr_id,
stdyevntlib_record_id,
stdyevntlib_pt_code,
stdyevntlib_meddra_version,
stdyevntlib_fk_study_rec_id,
stdyevntlib_event_type,
stdyevntlib_event_code,
stdyevntlib_event,
stdyevntlib_date_modified,
stdyevntlib_date_created,
crossrefind_user_modified,
crossrefind_user_created,
crossrefind_study_type,
crossrefind_spr_id,
crossrefind_record_id,
crossrefind_ps_uuid,
crossrefind_fk_study_record_id,
crossrefind_date_modified,
crossrefind_date_created,
crossrefind_cross_ref_ind,
studyacclib_user_modified,
studyacclib_user_created,
studyacclib_spr_id,
studyacclib_record_id,
studyacclib_fk_study_record_id,
studyacclib_fk_study_account_record_id,
studyacclib_date_modified,
studyacclib_date_created,
studyacclib_account_website,
studyacclib_account_name,
studyacclib_account_industry,
studyacclib_account_email,
studyacclib_account_domain
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB TGT
WHERE  ( studyacclib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_account_library') OR crossrefind_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_cross_ref_inds') OR stdyevntlib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_event_library') OR stdylib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_library') OR stdyprdlib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_product_library') OR stdyreg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP 
WHERE 	TO_DATE(LS_DB_STUDY_LIB.PROCESSING_DT) < TO_DATE(LS_DB_STUDY_LIB_TMP.PROCESSING_DT)
AND LS_DB_STUDY_LIB.INTEGRATION_ID = LS_DB_STUDY_LIB_TMP.INTEGRATION_ID
AND LS_DB_STUDY_LIB.stdylib_RECORD_ID = LS_DB_STUDY_LIB_TMP.stdylib_RECORD_ID
AND LS_DB_STUDY_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_STUDY_LIB.stdyreg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.crossrefind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdyevntlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.studyacclib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdyprdlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY_LIB.stdylib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_STUDY_LIB_TMP.stdyreg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.crossrefind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdyevntlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.studyacclib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdyprdlib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_LIB_TMP.stdylib_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( studyacclib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_account_library') OR crossrefind_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_cross_ref_inds') OR stdyevntlib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_event_library') OR stdylib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_library') OR stdyprdlib_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_product_library') OR stdyreg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_STUDY_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_STUDY_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_STUDY_LIB'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_STUDY_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_STUDY_LIB'),
	'LSDB','Case','LS_DB_STUDY_LIB',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_STUDY_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
