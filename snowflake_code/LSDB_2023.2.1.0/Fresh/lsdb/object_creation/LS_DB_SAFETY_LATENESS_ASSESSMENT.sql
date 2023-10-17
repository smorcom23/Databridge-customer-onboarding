
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_LATENESS_ASSESSMENT()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_LATENESS_ASSESSMENT'),
	'LSDB','Case','LS_DB_SAFETY_LATENESS_ASSESSMENT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_SAFETY_LATENESS_ASSESSMENT' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  As select RECORD_ID,'lsmv_late_case_activity_info' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_late_case_activity_info WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_lateness_info_inbound' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_info_inbound WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_lateness_sub_compliance' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_sub_compliance WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1002','10100','10144','10144','801','801','9662')
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
 
select DISTINCT RECORD_ID record_id, im_rec_id common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_sub_compliance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT im_rec_id record_id, im_rec_id common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_sub_compliance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, im_rec_id common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_late_case_activity_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT im_rec_id record_id, im_rec_id common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_late_case_activity_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, im_rec_id common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_info_inbound WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT im_rec_id record_id, im_rec_id common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_lateness_info_inbound WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SAFETY_LATENESS_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_lateness_info_inbound_SUBSET AS 
(
select * from 
    (SELECT  
    account_name  lateinfoinbo_account_name,account_type  lateinfoinbo_account_type,capa_number  lateinfoinbo_capa_number,case_expected_received_on  lateinfoinbo_case_expected_received_on,case_received_on  lateinfoinbo_case_received_on,date_created  lateinfoinbo_date_created,date_modified  lateinfoinbo_date_modified,im_rec_id  lateinfoinbo_im_rec_id,is_edited  lateinfoinbo_is_edited,late_by_days  lateinfoinbo_late_by_days,lateness_comments  lateinfoinbo_lateness_comments,lateness_reason  lateinfoinbo_lateness_reason,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10100' AND CD=CAST(lateness_reason AS VARCHAR(100)) )lateinfoinbo_lateness_reason_de_ml , manual_capa_id  lateinfoinbo_manual_capa_id,rca_capa_required  lateinfoinbo_rca_capa_required,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(rca_capa_required AS VARCHAR(100)) )lateinfoinbo_rca_capa_required_de_ml , reason_for_re_assesment  lateinfoinbo_reason_for_re_assesment,record_id  lateinfoinbo_record_id,sender_organization_name  lateinfoinbo_sender_organization_name,spr_id  lateinfoinbo_spr_id,status  lateinfoinbo_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10144' AND CD=CAST(status AS VARCHAR(100)) )lateinfoinbo_status_de_ml , user_created  lateinfoinbo_user_created,user_modified  lateinfoinbo_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_lateness_info_inbound
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
im_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_info_inbound')
  ) where REC_RANK=1 )
  , lsmv_late_case_activity_info_SUBSET AS 
(
select * from 
    (SELECT  
    capa_auto_generated  latecaseact_capa_auto_generated,capa_number  latecaseact_capa_number,date_created  latecaseact_date_created,date_modified  latecaseact_date_modified,due_date  latecaseact_due_date,entry_date  latecaseact_entry_date,exit_date  latecaseact_exit_date,im_rec_id  latecaseact_im_rec_id,is_edited  latecaseact_is_edited,late_by_days  latecaseact_late_by_days,lateness_comments  latecaseact_lateness_comments,lateness_reason  latecaseact_lateness_reason,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='801' AND CD=CAST(lateness_reason AS VARCHAR(100)) )latecaseact_lateness_reason_de_ml , no_of_days_spent  latecaseact_no_of_days_spent,processing_unit  latecaseact_processing_unit,rca_capa_required  latecaseact_rca_capa_required,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(rca_capa_required AS VARCHAR(100)) )latecaseact_rca_capa_required_de_ml , reason_for_re_assesment  latecaseact_reason_for_re_assesment,record_id  latecaseact_record_id,sender_organization_name  latecaseact_sender_organization_name,spr_id  latecaseact_spr_id,status  latecaseact_status,user_created  latecaseact_user_created,user_modified  latecaseact_user_modified,user_name  latecaseact_user_name,wf_activity_name  latecaseact_wf_activity_name,wf_tracker_rec_id  latecaseact_wf_tracker_rec_id,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_late_case_activity_info
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
im_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_late_case_activity_info')
  ) where REC_RANK=1 )
  , lsmv_lateness_sub_compliance_SUBSET AS 
(
select * from 
    (SELECT  
    capa_auto_generated  latesubcomp_capa_auto_generated,capa_number  latesubcomp_capa_number,comments  latesubcomp_comments,contact_name  latesubcomp_contact_name,date_created  latesubcomp_date_created,date_modified  latesubcomp_date_modified,dist_format_rec_id  latesubcomp_dist_format_rec_id,format_name  latesubcomp_format_name,im_rec_id  latesubcomp_im_rec_id,informed_date  latesubcomp_informed_date,is_edited  latesubcomp_is_edited,late_by_days  latesubcomp_late_by_days,lateness_comments  latesubcomp_lateness_comments,lateness_reason  latesubcomp_lateness_reason,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='801' AND CD=CAST(lateness_reason AS VARCHAR(100)) )latesubcomp_lateness_reason_de_ml , lsm_rec_id  latesubcomp_lsm_rec_id,rca_capa_required  latesubcomp_rca_capa_required,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(rca_capa_required AS VARCHAR(100)) )latesubcomp_rca_capa_required_de_ml , reason_for_re_assesment  latesubcomp_reason_for_re_assesment,reason_for_submission_delay  latesubcomp_reason_for_submission_delay,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9662' AND CD=CAST(reason_for_submission_delay AS VARCHAR(100)) )latesubcomp_reason_for_submission_delay_de_ml , record_id  latesubcomp_record_id,sender_organization_name  latesubcomp_sender_organization_name,spr_id  latesubcomp_spr_id,status  latesubcomp_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10144' AND CD=CAST(status AS VARCHAR(100)) )latesubcomp_status_de_ml , submission_due_date  latesubcomp_submission_due_date,submission_unit  latesubcomp_submission_unit,submitted_date  latesubcomp_submitted_date,submitted_user  latesubcomp_submitted_user,user_created  latesubcomp_user_created,user_modified  latesubcomp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_lateness_sub_compliance
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
im_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_sub_compliance')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_late_case_activity_info_SUBSET.latecaseact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_lateness_sub_compliance_SUBSET.latesubcomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_USER_MODIFIED USER_MODIFIED,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_USER_CREATED CREATED_BY,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_user_modified  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_user_created  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_submitted_user  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_submitted_date  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_submission_unit  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_submission_due_date  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_status_de_ml  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_status  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_spr_id  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_sender_organization_name  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_record_id  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_reason_for_submission_delay_de_ml  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_reason_for_submission_delay  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_reason_for_re_assesment  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_rca_capa_required_de_ml  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_rca_capa_required  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_lsm_rec_id  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_lateness_reason_de_ml  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_lateness_reason  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_lateness_comments  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_late_by_days  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_is_edited  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_informed_date  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_im_rec_id  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_format_name  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_dist_format_rec_id  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_date_modified  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_date_created  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_contact_name  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_comments  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_capa_number  ,lsmv_lateness_sub_compliance_SUBSET.latesubcomp_capa_auto_generated  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_user_modified  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_user_created  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_status_de_ml  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_status  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_spr_id  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_sender_organization_name  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_record_id  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_reason_for_re_assesment  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_rca_capa_required_de_ml  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_rca_capa_required  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_manual_capa_id  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_lateness_reason_de_ml  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_lateness_reason  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_lateness_comments  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_late_by_days  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_is_edited  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_im_rec_id  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_date_modified  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_date_created  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_case_received_on  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_case_expected_received_on  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_capa_number  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_account_type  ,lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_account_name  ,lsmv_late_case_activity_info_SUBSET.latecaseact_wf_tracker_rec_id  ,lsmv_late_case_activity_info_SUBSET.latecaseact_wf_activity_name  ,lsmv_late_case_activity_info_SUBSET.latecaseact_user_name  ,lsmv_late_case_activity_info_SUBSET.latecaseact_user_modified  ,lsmv_late_case_activity_info_SUBSET.latecaseact_user_created  ,lsmv_late_case_activity_info_SUBSET.latecaseact_status  ,lsmv_late_case_activity_info_SUBSET.latecaseact_spr_id  ,lsmv_late_case_activity_info_SUBSET.latecaseact_sender_organization_name  ,lsmv_late_case_activity_info_SUBSET.latecaseact_record_id  ,lsmv_late_case_activity_info_SUBSET.latecaseact_reason_for_re_assesment  ,lsmv_late_case_activity_info_SUBSET.latecaseact_rca_capa_required_de_ml  ,lsmv_late_case_activity_info_SUBSET.latecaseact_rca_capa_required  ,lsmv_late_case_activity_info_SUBSET.latecaseact_processing_unit  ,lsmv_late_case_activity_info_SUBSET.latecaseact_no_of_days_spent  ,lsmv_late_case_activity_info_SUBSET.latecaseact_lateness_reason_de_ml  ,lsmv_late_case_activity_info_SUBSET.latecaseact_lateness_reason  ,lsmv_late_case_activity_info_SUBSET.latecaseact_lateness_comments  ,lsmv_late_case_activity_info_SUBSET.latecaseact_late_by_days  ,lsmv_late_case_activity_info_SUBSET.latecaseact_is_edited  ,lsmv_late_case_activity_info_SUBSET.latecaseact_im_rec_id  ,lsmv_late_case_activity_info_SUBSET.latecaseact_exit_date  ,lsmv_late_case_activity_info_SUBSET.latecaseact_entry_date  ,lsmv_late_case_activity_info_SUBSET.latecaseact_due_date  ,lsmv_late_case_activity_info_SUBSET.latecaseact_date_modified  ,lsmv_late_case_activity_info_SUBSET.latecaseact_date_created  ,lsmv_late_case_activity_info_SUBSET.latecaseact_capa_number  ,lsmv_late_case_activity_info_SUBSET.latecaseact_capa_auto_generated ,CONCAT(NVL(lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_RECORD_ID,-1),'||',NVL(lsmv_late_case_activity_info_SUBSET.latecaseact_RECORD_ID,-1),'||',NVL(lsmv_lateness_sub_compliance_SUBSET.latesubcomp_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_lateness_sub_compliance_SUBSET  FULL OUTER JOIN lsmv_late_case_activity_info_SUBSET ON lsmv_lateness_sub_compliance_SUBSET.latesubcomp_im_rec_id=lsmv_late_case_activity_info_SUBSET.latecaseact_im_rec_id
                         FULL OUTER JOIN lsmv_lateness_info_inbound_SUBSET ON lsmv_lateness_sub_compliance_SUBSET.latesubcomp_im_rec_id=lsmv_lateness_info_inbound_SUBSET.lateinfoinbo_im_rec_id
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP)
where target_table_name='LS_DB_SAFETY_LATENESS_ASSESSMENT'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT   
SET LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_user_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_user_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_user_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_user_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_submitted_user = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_submitted_user,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_submitted_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_submitted_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_submission_unit = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_submission_unit,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_submission_due_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_submission_due_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_status_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_status_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_status = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_status,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_spr_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_spr_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_sender_organization_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_sender_organization_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_record_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_record_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_reason_for_submission_delay_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_reason_for_submission_delay_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_reason_for_submission_delay = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_reason_for_submission_delay,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_reason_for_re_assesment = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_reason_for_re_assesment,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_rca_capa_required_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_rca_capa_required_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_rca_capa_required = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_rca_capa_required,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_lsm_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_lsm_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_lateness_reason_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_lateness_reason_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_lateness_reason = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_lateness_reason,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_lateness_comments = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_lateness_comments,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_late_by_days = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_late_by_days,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_is_edited = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_is_edited,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_informed_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_informed_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_im_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_im_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_format_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_format_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_dist_format_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_dist_format_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_date_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_date_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_date_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_date_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_contact_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_contact_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_comments = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_comments,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_capa_number = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_capa_number,LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_capa_auto_generated = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_capa_auto_generated,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_user_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_user_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_user_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_user_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_status_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_status_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_status = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_status,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_spr_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_spr_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_sender_organization_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_sender_organization_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_record_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_record_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_reason_for_re_assesment = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_reason_for_re_assesment,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_rca_capa_required_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_rca_capa_required_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_rca_capa_required = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_rca_capa_required,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_manual_capa_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_manual_capa_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_lateness_reason_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_lateness_reason_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_lateness_reason = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_lateness_reason,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_lateness_comments = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_lateness_comments,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_late_by_days = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_late_by_days,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_is_edited = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_is_edited,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_im_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_im_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_date_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_date_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_date_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_date_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_case_received_on = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_case_received_on,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_case_expected_received_on = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_case_expected_received_on,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_capa_number = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_capa_number,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_account_type = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_account_type,LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_account_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_account_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_wf_tracker_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_wf_tracker_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_wf_activity_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_wf_activity_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_user_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_user_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_user_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_user_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_user_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_user_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_status = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_status,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_spr_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_spr_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_sender_organization_name = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_sender_organization_name,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_record_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_record_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_reason_for_re_assesment = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_reason_for_re_assesment,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_rca_capa_required_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_rca_capa_required_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_rca_capa_required = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_rca_capa_required,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_processing_unit = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_processing_unit,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_no_of_days_spent = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_no_of_days_spent,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_lateness_reason_de_ml = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_lateness_reason_de_ml,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_lateness_reason = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_lateness_reason,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_lateness_comments = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_lateness_comments,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_late_by_days = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_late_by_days,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_is_edited = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_is_edited,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_im_rec_id = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_im_rec_id,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_exit_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_exit_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_entry_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_entry_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_due_date = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_due_date,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_date_modified = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_date_modified,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_date_created = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_date_created,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_capa_number = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_capa_number,LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_capa_auto_generated = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_capa_auto_generated,
LS_DB_SAFETY_LATENESS_ASSESSMENT.PROCESSING_DT = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.PROCESSING_DT,
LS_DB_SAFETY_LATENESS_ASSESSMENT.user_modified  =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.user_modified     ,
LS_DB_SAFETY_LATENESS_ASSESSMENT.date_modified  =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.date_modified     ,
LS_DB_SAFETY_LATENESS_ASSESSMENT.expiry_date    =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.expiry_date       ,
LS_DB_SAFETY_LATENESS_ASSESSMENT.created_by     =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.created_by        ,
LS_DB_SAFETY_LATENESS_ASSESSMENT.created_dt     =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.created_dt        ,
LS_DB_SAFETY_LATENESS_ASSESSMENT.load_ts        =LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP 
WHERE 	LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.PROCESSING_DT = LS_DB_SAFETY_LATENESS_ASSESSMENT.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_SAFETY_LATENESS_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID ,LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP 
ON LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID=LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_RECORD_ID
AND LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID 
where LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID  is null AND LS_DB_SAFETY_LATENESS_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID in (select latesubcomp_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP )
) TMP where LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_SAFETY_LATENESS_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID ,LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP 
    ON LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID=LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_RECORD_ID
    AND LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID 
    where LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID  is null AND LS_DB_SAFETY_LATENESS_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID in (select latesubcomp_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,latesubcomp_user_modified,
latesubcomp_user_created,
latesubcomp_submitted_user,
latesubcomp_submitted_date,
latesubcomp_submission_unit,
latesubcomp_submission_due_date,
latesubcomp_status_de_ml,
latesubcomp_status,
latesubcomp_spr_id,
latesubcomp_sender_organization_name,
latesubcomp_record_id,
latesubcomp_reason_for_submission_delay_de_ml,
latesubcomp_reason_for_submission_delay,
latesubcomp_reason_for_re_assesment,
latesubcomp_rca_capa_required_de_ml,
latesubcomp_rca_capa_required,
latesubcomp_lsm_rec_id,
latesubcomp_lateness_reason_de_ml,
latesubcomp_lateness_reason,
latesubcomp_lateness_comments,
latesubcomp_late_by_days,
latesubcomp_is_edited,
latesubcomp_informed_date,
latesubcomp_im_rec_id,
latesubcomp_format_name,
latesubcomp_dist_format_rec_id,
latesubcomp_date_modified,
latesubcomp_date_created,
latesubcomp_contact_name,
latesubcomp_comments,
latesubcomp_capa_number,
latesubcomp_capa_auto_generated,
lateinfoinbo_user_modified,
lateinfoinbo_user_created,
lateinfoinbo_status_de_ml,
lateinfoinbo_status,
lateinfoinbo_spr_id,
lateinfoinbo_sender_organization_name,
lateinfoinbo_record_id,
lateinfoinbo_reason_for_re_assesment,
lateinfoinbo_rca_capa_required_de_ml,
lateinfoinbo_rca_capa_required,
lateinfoinbo_manual_capa_id,
lateinfoinbo_lateness_reason_de_ml,
lateinfoinbo_lateness_reason,
lateinfoinbo_lateness_comments,
lateinfoinbo_late_by_days,
lateinfoinbo_is_edited,
lateinfoinbo_im_rec_id,
lateinfoinbo_date_modified,
lateinfoinbo_date_created,
lateinfoinbo_case_received_on,
lateinfoinbo_case_expected_received_on,
lateinfoinbo_capa_number,
lateinfoinbo_account_type,
lateinfoinbo_account_name,
latecaseact_wf_tracker_rec_id,
latecaseact_wf_activity_name,
latecaseact_user_name,
latecaseact_user_modified,
latecaseact_user_created,
latecaseact_status,
latecaseact_spr_id,
latecaseact_sender_organization_name,
latecaseact_record_id,
latecaseact_reason_for_re_assesment,
latecaseact_rca_capa_required_de_ml,
latecaseact_rca_capa_required,
latecaseact_processing_unit,
latecaseact_no_of_days_spent,
latecaseact_lateness_reason_de_ml,
latecaseact_lateness_reason,
latecaseact_lateness_comments,
latecaseact_late_by_days,
latecaseact_is_edited,
latecaseact_im_rec_id,
latecaseact_exit_date,
latecaseact_entry_date,
latecaseact_due_date,
latecaseact_date_modified,
latecaseact_date_created,
latecaseact_capa_number,
latecaseact_capa_auto_generated)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,latesubcomp_user_modified,
latesubcomp_user_created,
latesubcomp_submitted_user,
latesubcomp_submitted_date,
latesubcomp_submission_unit,
latesubcomp_submission_due_date,
latesubcomp_status_de_ml,
latesubcomp_status,
latesubcomp_spr_id,
latesubcomp_sender_organization_name,
latesubcomp_record_id,
latesubcomp_reason_for_submission_delay_de_ml,
latesubcomp_reason_for_submission_delay,
latesubcomp_reason_for_re_assesment,
latesubcomp_rca_capa_required_de_ml,
latesubcomp_rca_capa_required,
latesubcomp_lsm_rec_id,
latesubcomp_lateness_reason_de_ml,
latesubcomp_lateness_reason,
latesubcomp_lateness_comments,
latesubcomp_late_by_days,
latesubcomp_is_edited,
latesubcomp_informed_date,
latesubcomp_im_rec_id,
latesubcomp_format_name,
latesubcomp_dist_format_rec_id,
latesubcomp_date_modified,
latesubcomp_date_created,
latesubcomp_contact_name,
latesubcomp_comments,
latesubcomp_capa_number,
latesubcomp_capa_auto_generated,
lateinfoinbo_user_modified,
lateinfoinbo_user_created,
lateinfoinbo_status_de_ml,
lateinfoinbo_status,
lateinfoinbo_spr_id,
lateinfoinbo_sender_organization_name,
lateinfoinbo_record_id,
lateinfoinbo_reason_for_re_assesment,
lateinfoinbo_rca_capa_required_de_ml,
lateinfoinbo_rca_capa_required,
lateinfoinbo_manual_capa_id,
lateinfoinbo_lateness_reason_de_ml,
lateinfoinbo_lateness_reason,
lateinfoinbo_lateness_comments,
lateinfoinbo_late_by_days,
lateinfoinbo_is_edited,
lateinfoinbo_im_rec_id,
lateinfoinbo_date_modified,
lateinfoinbo_date_created,
lateinfoinbo_case_received_on,
lateinfoinbo_case_expected_received_on,
lateinfoinbo_capa_number,
lateinfoinbo_account_type,
lateinfoinbo_account_name,
latecaseact_wf_tracker_rec_id,
latecaseact_wf_activity_name,
latecaseact_user_name,
latecaseact_user_modified,
latecaseact_user_created,
latecaseact_status,
latecaseact_spr_id,
latecaseact_sender_organization_name,
latecaseact_record_id,
latecaseact_reason_for_re_assesment,
latecaseact_rca_capa_required_de_ml,
latecaseact_rca_capa_required,
latecaseact_processing_unit,
latecaseact_no_of_days_spent,
latecaseact_lateness_reason_de_ml,
latecaseact_lateness_reason,
latecaseact_lateness_comments,
latecaseact_late_by_days,
latecaseact_is_edited,
latecaseact_im_rec_id,
latecaseact_exit_date,
latecaseact_entry_date,
latecaseact_due_date,
latecaseact_date_modified,
latecaseact_date_created,
latecaseact_capa_number,
latecaseact_capa_auto_generated
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP 
WHERE 	TO_DATE(LS_DB_SAFETY_LATENESS_ASSESSMENT.PROCESSING_DT) < TO_DATE(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.PROCESSING_DT)
AND LS_DB_SAFETY_LATENESS_ASSESSMENT.INTEGRATION_ID = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.INTEGRATION_ID
AND LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_RECORD_ID = LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_RECORD_ID
AND LS_DB_SAFETY_LATENESS_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.lateinfoinbo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.latecaseact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT.latesubcomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.lateinfoinbo_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latecaseact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP.latesubcomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT TGT
WHERE  ( latecaseact_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_late_case_activity_info') OR lateinfoinbo_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_info_inbound') OR latesubcomp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_sub_compliance')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( latecaseact_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_late_case_activity_info') OR lateinfoinbo_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_info_inbound') OR latesubcomp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SAFETY_LATENESS_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_lateness_sub_compliance')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_LATENESS_ASSESSMENT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SAFETY_LATENESS_ASSESSMENT'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_SAFETY_LATENESS_ASSESSMENT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_LATENESS_ASSESSMENT'),
	'LSDB','Case','LS_DB_SAFETY_LATENESS_ASSESSMENT',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_SAFETY_LATENESS_ASSESSMENT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
