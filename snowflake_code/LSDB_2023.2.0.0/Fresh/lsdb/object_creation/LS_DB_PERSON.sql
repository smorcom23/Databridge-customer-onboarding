
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PERSON()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PERSON'),
	'LSDB','Case','LS_DB_PERSON',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PERSON' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PERSON' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  As select RECORD_ID,'lsmv_person' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_person WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_person_account' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_account WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_person_company_unit' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_company_unit WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_person WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_person WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_person_record_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_account WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_person_record_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_person_account WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PERSON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_person_company_unit_SUBSET AS 
(
select * from 
    (SELECT  
    company_unit_name  prsncomp_company_unit_name,company_unit_record_id  prsncomp_company_unit_record_id,date_created  prsncomp_date_created,date_modified  prsncomp_date_modified,fk_person_record_id  prsncomp_fk_person_record_id,record_id  prsncomp_record_id,spr_id  prsncomp_spr_id,user_created  prsncomp_user_created,user_modified  prsncomp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_person_company_unit
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_person_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_company_unit')
  ) where REC_RANK=1 )
  , lsmv_person_account_SUBSET AS 
(
select * from 
    (SELECT  
    account_name  prsnacc_account_name,account_type  prsnacc_account_type,date_created  prsnacc_date_created,date_modified  prsnacc_date_modified,fk_account_record_id  prsnacc_fk_account_record_id,fk_person_record_id  prsnacc_fk_person_record_id,primary_account  prsnacc_primary_account,record_id  prsnacc_record_id,spr_id  prsnacc_spr_id,user_created  prsnacc_user_created,user_modified  prsnacc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_person_account
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_person_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_account')
  ) where REC_RANK=1 )
  , lsmv_person_SUBSET AS 
(
select * from 
    (SELECT  
    account_id  prsn_account_id,account_name  prsn_account_name,active  prsn_active,address  prsn_address,all_account_assign  prsn_all_account_assign,all_company_unit  prsn_all_company_unit,app_id  prsn_app_id,app_rec_id  prsn_app_rec_id,archive_based_on_date  prsn_archive_based_on_date,city  prsn_city,comm_medium_others  prsn_comm_medium_others,company_unit_rec_id  prsn_company_unit_rec_id,consent  prsn_consent,consent_collect  prsn_consent_collect,consent_share  prsn_consent_share,contact_ins_from_schedler  prsn_contact_ins_from_schedler,contact_source  prsn_contact_source,correspondence_flag  prsn_correspondence_flag,correspondence_seq  prsn_correspondence_seq,country  prsn_country,crm_last_modified_date  prsn_crm_last_modified_date,crm_record_id  prsn_crm_record_id,crm_source  prsn_crm_source,crm_source_flag  prsn_crm_source_flag,dataprivacy_present  prsn_dataprivacy_present,date_created  prsn_date_created,date_informed_receiver  prsn_date_informed_receiver,date_modified  prsn_date_modified,degree  prsn_degree,dept_name  prsn_dept_name,dept_name_in_upper  prsn_dept_name_in_upper,dept_record_id  prsn_dept_record_id,designation  prsn_designation,do_not_contact  prsn_do_not_contact,doc_name_kana  prsn_doc_name_kana,doc_name_kanji  prsn_doc_name_kanji,e2b_flag  prsn_e2b_flag,email_id  prsn_email_id,email_id_in_upper  prsn_email_id_in_upper,emirf_account_id  prsn_emirf_account_id,ext_no  prsn_ext_no,external_app_rec_id  prsn_external_app_rec_id,external_app_updated_date  prsn_external_app_updated_date,fax_country_code  prsn_fax_country_code,fax_extension_number  prsn_fax_extension_number,fax_no  prsn_fax_no,fax_office_extn_no  prsn_fax_office_extn_no,first_name  prsn_first_name,first_name_in_upper  prsn_first_name_in_upper,fk_acu_rec_id  prsn_fk_acu_rec_id,hospital_name  prsn_hospital_name,icsr_ack  prsn_icsr_ack,interchange_id  prsn_interchange_id,is_audit_required  prsn_is_audit_required,is_distribution  prsn_is_distribution,is_e2b_contact  prsn_is_e2b_contact,is_exported  prsn_is_exported,is_health_professional  prsn_is_health_professional,is_inquiry_contact  prsn_is_inquiry_contact,is_mdn_req  prsn_is_mdn_req,last_name  prsn_last_name,last_name_in_upper  prsn_last_name_in_upper,lastuseddate  prsn_lastuseddate,medical_interest  prsn_medical_interest,middle_name  prsn_middle_name,middle_name_in_upper  prsn_middle_name_in_upper,mobile_number  prsn_mobile_number,notes_flag  prsn_notes_flag,organization  prsn_organization,organization_id  prsn_organization_id,organization_name  prsn_organization_name,organization_name_in_upper  prsn_organization_name_in_upper,other  prsn_other,other_in_upper  prsn_other_in_upper,person_id  prsn_person_id,person_primary_id  prsn_person_primary_id,person_type  prsn_person_type,phone_country_code  prsn_phone_country_code,phone_no_office  prsn_phone_no_office,phone_no_office_extn_no  prsn_phone_no_office_extn_no,phone_no_residence_no  prsn_phone_no_residence_no,postal_code  prsn_postal_code,pref_comm_medium  prsn_pref_comm_medium,primary_postal_code  prsn_primary_postal_code,privacy_info  prsn_privacy_info,profession  prsn_profession,public_contact  prsn_public_contact,read_unread_correspondence  prsn_read_unread_correspondence,record_id  prsn_record_id,reducted  prsn_reducted,reporter_type  prsn_reporter_type,request_id  prsn_request_id,requester_category  prsn_requester_category,requester_category_vet  prsn_requester_category_vet,requester_type  prsn_requester_type,sales_territory_id  prsn_sales_territory_id,sex  prsn_sex,sir_flag  prsn_sir_flag,specialization  prsn_specialization,spr_id  prsn_spr_id,sso_token_id  prsn_sso_token_id,state  prsn_state,suffix  prsn_suffix,task_flag  prsn_task_flag,territory_rec_id  prsn_territory_rec_id,title  prsn_title,user_created  prsn_user_created,user_modified  prsn_user_modified,validity_from  prsn_validity_from,validity_to  prsn_validity_to,version  prsn_version,vet_department  prsn_vet_department,zip_code  prsn_zip_code,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_person
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_person_account_SUBSET.prsnacc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_person_company_unit_SUBSET.prsncomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_person_SUBSET.prsn_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_person_SUBSET.prsn_USER_MODIFIED USER_MODIFIED,lsmv_person_SUBSET.prsn_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_person_SUBSET.prsn_USER_CREATED CREATED_BY,lsmv_person_SUBSET.prsn_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_person_SUBSET.prsn_zip_code  ,lsmv_person_SUBSET.prsn_vet_department  ,lsmv_person_SUBSET.prsn_version  ,lsmv_person_SUBSET.prsn_validity_to  ,lsmv_person_SUBSET.prsn_validity_from  ,lsmv_person_SUBSET.prsn_user_modified  ,lsmv_person_SUBSET.prsn_user_created  ,lsmv_person_SUBSET.prsn_title  ,lsmv_person_SUBSET.prsn_territory_rec_id  ,lsmv_person_SUBSET.prsn_task_flag  ,lsmv_person_SUBSET.prsn_suffix  ,lsmv_person_SUBSET.prsn_state  ,lsmv_person_SUBSET.prsn_sso_token_id  ,lsmv_person_SUBSET.prsn_spr_id  ,lsmv_person_SUBSET.prsn_specialization  ,lsmv_person_SUBSET.prsn_sir_flag  ,lsmv_person_SUBSET.prsn_sex  ,lsmv_person_SUBSET.prsn_sales_territory_id  ,lsmv_person_SUBSET.prsn_requester_type  ,lsmv_person_SUBSET.prsn_requester_category_vet  ,lsmv_person_SUBSET.prsn_requester_category  ,lsmv_person_SUBSET.prsn_request_id  ,lsmv_person_SUBSET.prsn_reporter_type  ,lsmv_person_SUBSET.prsn_reducted  ,lsmv_person_SUBSET.prsn_record_id  ,lsmv_person_SUBSET.prsn_read_unread_correspondence  ,lsmv_person_SUBSET.prsn_public_contact  ,lsmv_person_SUBSET.prsn_profession  ,lsmv_person_SUBSET.prsn_privacy_info  ,lsmv_person_SUBSET.prsn_primary_postal_code  ,lsmv_person_SUBSET.prsn_pref_comm_medium  ,lsmv_person_SUBSET.prsn_postal_code  ,lsmv_person_SUBSET.prsn_phone_no_residence_no  ,lsmv_person_SUBSET.prsn_phone_no_office_extn_no  ,lsmv_person_SUBSET.prsn_phone_no_office  ,lsmv_person_SUBSET.prsn_phone_country_code  ,lsmv_person_SUBSET.prsn_person_type  ,lsmv_person_SUBSET.prsn_person_primary_id  ,lsmv_person_SUBSET.prsn_person_id  ,lsmv_person_SUBSET.prsn_other_in_upper  ,lsmv_person_SUBSET.prsn_other  ,lsmv_person_SUBSET.prsn_organization_name_in_upper  ,lsmv_person_SUBSET.prsn_organization_name  ,lsmv_person_SUBSET.prsn_organization_id  ,lsmv_person_SUBSET.prsn_organization  ,lsmv_person_SUBSET.prsn_notes_flag  ,lsmv_person_SUBSET.prsn_mobile_number  ,lsmv_person_SUBSET.prsn_middle_name_in_upper  ,lsmv_person_SUBSET.prsn_middle_name  ,lsmv_person_SUBSET.prsn_medical_interest  ,lsmv_person_SUBSET.prsn_lastuseddate  ,lsmv_person_SUBSET.prsn_last_name_in_upper  ,lsmv_person_SUBSET.prsn_last_name  ,lsmv_person_SUBSET.prsn_is_mdn_req  ,lsmv_person_SUBSET.prsn_is_inquiry_contact  ,lsmv_person_SUBSET.prsn_is_health_professional  ,lsmv_person_SUBSET.prsn_is_exported  ,lsmv_person_SUBSET.prsn_is_e2b_contact  ,lsmv_person_SUBSET.prsn_is_distribution  ,lsmv_person_SUBSET.prsn_is_audit_required  ,lsmv_person_SUBSET.prsn_interchange_id  ,lsmv_person_SUBSET.prsn_icsr_ack  ,lsmv_person_SUBSET.prsn_hospital_name  ,lsmv_person_SUBSET.prsn_fk_acu_rec_id  ,lsmv_person_SUBSET.prsn_first_name_in_upper  ,lsmv_person_SUBSET.prsn_first_name  ,lsmv_person_SUBSET.prsn_fax_office_extn_no  ,lsmv_person_SUBSET.prsn_fax_no  ,lsmv_person_SUBSET.prsn_fax_extension_number  ,lsmv_person_SUBSET.prsn_fax_country_code  ,lsmv_person_SUBSET.prsn_external_app_updated_date  ,lsmv_person_SUBSET.prsn_external_app_rec_id  ,lsmv_person_SUBSET.prsn_ext_no  ,lsmv_person_SUBSET.prsn_emirf_account_id  ,lsmv_person_SUBSET.prsn_email_id_in_upper  ,lsmv_person_SUBSET.prsn_email_id  ,lsmv_person_SUBSET.prsn_e2b_flag  ,lsmv_person_SUBSET.prsn_doc_name_kanji  ,lsmv_person_SUBSET.prsn_doc_name_kana  ,lsmv_person_SUBSET.prsn_do_not_contact  ,lsmv_person_SUBSET.prsn_designation  ,lsmv_person_SUBSET.prsn_dept_record_id  ,lsmv_person_SUBSET.prsn_dept_name_in_upper  ,lsmv_person_SUBSET.prsn_dept_name  ,lsmv_person_SUBSET.prsn_degree  ,lsmv_person_SUBSET.prsn_date_modified  ,lsmv_person_SUBSET.prsn_date_informed_receiver  ,lsmv_person_SUBSET.prsn_date_created  ,lsmv_person_SUBSET.prsn_dataprivacy_present  ,lsmv_person_SUBSET.prsn_crm_source_flag  ,lsmv_person_SUBSET.prsn_crm_source  ,lsmv_person_SUBSET.prsn_crm_record_id  ,lsmv_person_SUBSET.prsn_crm_last_modified_date  ,lsmv_person_SUBSET.prsn_country  ,lsmv_person_SUBSET.prsn_correspondence_seq  ,lsmv_person_SUBSET.prsn_correspondence_flag  ,lsmv_person_SUBSET.prsn_contact_source  ,lsmv_person_SUBSET.prsn_contact_ins_from_schedler  ,lsmv_person_SUBSET.prsn_consent_share  ,lsmv_person_SUBSET.prsn_consent_collect  ,lsmv_person_SUBSET.prsn_consent  ,lsmv_person_SUBSET.prsn_company_unit_rec_id  ,lsmv_person_SUBSET.prsn_comm_medium_others  ,lsmv_person_SUBSET.prsn_city  ,lsmv_person_SUBSET.prsn_archive_based_on_date  ,lsmv_person_SUBSET.prsn_app_rec_id  ,lsmv_person_SUBSET.prsn_app_id  ,lsmv_person_SUBSET.prsn_all_company_unit  ,lsmv_person_SUBSET.prsn_all_account_assign  ,lsmv_person_SUBSET.prsn_address  ,lsmv_person_SUBSET.prsn_active  ,lsmv_person_SUBSET.prsn_account_name  ,lsmv_person_SUBSET.prsn_account_id  ,lsmv_person_company_unit_SUBSET.prsncomp_user_modified  ,lsmv_person_company_unit_SUBSET.prsncomp_user_created  ,lsmv_person_company_unit_SUBSET.prsncomp_spr_id  ,lsmv_person_company_unit_SUBSET.prsncomp_record_id  ,lsmv_person_company_unit_SUBSET.prsncomp_fk_person_record_id  ,lsmv_person_company_unit_SUBSET.prsncomp_date_modified  ,lsmv_person_company_unit_SUBSET.prsncomp_date_created  ,lsmv_person_company_unit_SUBSET.prsncomp_company_unit_record_id  ,lsmv_person_company_unit_SUBSET.prsncomp_company_unit_name  ,lsmv_person_account_SUBSET.prsnacc_user_modified  ,lsmv_person_account_SUBSET.prsnacc_user_created  ,lsmv_person_account_SUBSET.prsnacc_spr_id  ,lsmv_person_account_SUBSET.prsnacc_record_id  ,lsmv_person_account_SUBSET.prsnacc_primary_account  ,lsmv_person_account_SUBSET.prsnacc_fk_person_record_id  ,lsmv_person_account_SUBSET.prsnacc_fk_account_record_id  ,lsmv_person_account_SUBSET.prsnacc_date_modified  ,lsmv_person_account_SUBSET.prsnacc_date_created  ,lsmv_person_account_SUBSET.prsnacc_account_type  ,lsmv_person_account_SUBSET.prsnacc_account_name ,CONCAT(NVL(lsmv_person_account_SUBSET.prsnacc_RECORD_ID,-1),'||',NVL(lsmv_person_company_unit_SUBSET.prsncomp_RECORD_ID,-1),'||',NVL(lsmv_person_SUBSET.prsn_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_person_SUBSET  LEFT JOIN lsmv_person_account_SUBSET ON lsmv_person_SUBSET.prsn_record_id=lsmv_person_account_SUBSET.prsnacc_fk_person_record_id
                         LEFT JOIN lsmv_person_company_unit_SUBSET ON lsmv_person_SUBSET.prsn_record_id=lsmv_person_company_unit_SUBSET.prsncomp_fk_person_record_id
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP)
where target_table_name='LS_DB_PERSON'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON   
SET LS_DB_PERSON.prsn_zip_code = LS_DB_PERSON_TMP.prsn_zip_code,LS_DB_PERSON.prsn_vet_department = LS_DB_PERSON_TMP.prsn_vet_department,LS_DB_PERSON.prsn_version = LS_DB_PERSON_TMP.prsn_version,LS_DB_PERSON.prsn_validity_to = LS_DB_PERSON_TMP.prsn_validity_to,LS_DB_PERSON.prsn_validity_from = LS_DB_PERSON_TMP.prsn_validity_from,LS_DB_PERSON.prsn_user_modified = LS_DB_PERSON_TMP.prsn_user_modified,LS_DB_PERSON.prsn_user_created = LS_DB_PERSON_TMP.prsn_user_created,LS_DB_PERSON.prsn_title = LS_DB_PERSON_TMP.prsn_title,LS_DB_PERSON.prsn_territory_rec_id = LS_DB_PERSON_TMP.prsn_territory_rec_id,LS_DB_PERSON.prsn_task_flag = LS_DB_PERSON_TMP.prsn_task_flag,LS_DB_PERSON.prsn_suffix = LS_DB_PERSON_TMP.prsn_suffix,LS_DB_PERSON.prsn_state = LS_DB_PERSON_TMP.prsn_state,LS_DB_PERSON.prsn_sso_token_id = LS_DB_PERSON_TMP.prsn_sso_token_id,LS_DB_PERSON.prsn_spr_id = LS_DB_PERSON_TMP.prsn_spr_id,LS_DB_PERSON.prsn_specialization = LS_DB_PERSON_TMP.prsn_specialization,LS_DB_PERSON.prsn_sir_flag = LS_DB_PERSON_TMP.prsn_sir_flag,LS_DB_PERSON.prsn_sex = LS_DB_PERSON_TMP.prsn_sex,LS_DB_PERSON.prsn_sales_territory_id = LS_DB_PERSON_TMP.prsn_sales_territory_id,LS_DB_PERSON.prsn_requester_type = LS_DB_PERSON_TMP.prsn_requester_type,LS_DB_PERSON.prsn_requester_category_vet = LS_DB_PERSON_TMP.prsn_requester_category_vet,LS_DB_PERSON.prsn_requester_category = LS_DB_PERSON_TMP.prsn_requester_category,LS_DB_PERSON.prsn_request_id = LS_DB_PERSON_TMP.prsn_request_id,LS_DB_PERSON.prsn_reporter_type = LS_DB_PERSON_TMP.prsn_reporter_type,LS_DB_PERSON.prsn_reducted = LS_DB_PERSON_TMP.prsn_reducted,LS_DB_PERSON.prsn_record_id = LS_DB_PERSON_TMP.prsn_record_id,LS_DB_PERSON.prsn_read_unread_correspondence = LS_DB_PERSON_TMP.prsn_read_unread_correspondence,LS_DB_PERSON.prsn_public_contact = LS_DB_PERSON_TMP.prsn_public_contact,LS_DB_PERSON.prsn_profession = LS_DB_PERSON_TMP.prsn_profession,LS_DB_PERSON.prsn_privacy_info = LS_DB_PERSON_TMP.prsn_privacy_info,LS_DB_PERSON.prsn_primary_postal_code = LS_DB_PERSON_TMP.prsn_primary_postal_code,LS_DB_PERSON.prsn_pref_comm_medium = LS_DB_PERSON_TMP.prsn_pref_comm_medium,LS_DB_PERSON.prsn_postal_code = LS_DB_PERSON_TMP.prsn_postal_code,LS_DB_PERSON.prsn_phone_no_residence_no = LS_DB_PERSON_TMP.prsn_phone_no_residence_no,LS_DB_PERSON.prsn_phone_no_office_extn_no = LS_DB_PERSON_TMP.prsn_phone_no_office_extn_no,LS_DB_PERSON.prsn_phone_no_office = LS_DB_PERSON_TMP.prsn_phone_no_office,LS_DB_PERSON.prsn_phone_country_code = LS_DB_PERSON_TMP.prsn_phone_country_code,LS_DB_PERSON.prsn_person_type = LS_DB_PERSON_TMP.prsn_person_type,LS_DB_PERSON.prsn_person_primary_id = LS_DB_PERSON_TMP.prsn_person_primary_id,LS_DB_PERSON.prsn_person_id = LS_DB_PERSON_TMP.prsn_person_id,LS_DB_PERSON.prsn_other_in_upper = LS_DB_PERSON_TMP.prsn_other_in_upper,LS_DB_PERSON.prsn_other = LS_DB_PERSON_TMP.prsn_other,LS_DB_PERSON.prsn_organization_name_in_upper = LS_DB_PERSON_TMP.prsn_organization_name_in_upper,LS_DB_PERSON.prsn_organization_name = LS_DB_PERSON_TMP.prsn_organization_name,LS_DB_PERSON.prsn_organization_id = LS_DB_PERSON_TMP.prsn_organization_id,LS_DB_PERSON.prsn_organization = LS_DB_PERSON_TMP.prsn_organization,LS_DB_PERSON.prsn_notes_flag = LS_DB_PERSON_TMP.prsn_notes_flag,LS_DB_PERSON.prsn_mobile_number = LS_DB_PERSON_TMP.prsn_mobile_number,LS_DB_PERSON.prsn_middle_name_in_upper = LS_DB_PERSON_TMP.prsn_middle_name_in_upper,LS_DB_PERSON.prsn_middle_name = LS_DB_PERSON_TMP.prsn_middle_name,LS_DB_PERSON.prsn_medical_interest = LS_DB_PERSON_TMP.prsn_medical_interest,LS_DB_PERSON.prsn_lastuseddate = LS_DB_PERSON_TMP.prsn_lastuseddate,LS_DB_PERSON.prsn_last_name_in_upper = LS_DB_PERSON_TMP.prsn_last_name_in_upper,LS_DB_PERSON.prsn_last_name = LS_DB_PERSON_TMP.prsn_last_name,LS_DB_PERSON.prsn_is_mdn_req = LS_DB_PERSON_TMP.prsn_is_mdn_req,LS_DB_PERSON.prsn_is_inquiry_contact = LS_DB_PERSON_TMP.prsn_is_inquiry_contact,LS_DB_PERSON.prsn_is_health_professional = LS_DB_PERSON_TMP.prsn_is_health_professional,LS_DB_PERSON.prsn_is_exported = LS_DB_PERSON_TMP.prsn_is_exported,LS_DB_PERSON.prsn_is_e2b_contact = LS_DB_PERSON_TMP.prsn_is_e2b_contact,LS_DB_PERSON.prsn_is_distribution = LS_DB_PERSON_TMP.prsn_is_distribution,LS_DB_PERSON.prsn_is_audit_required = LS_DB_PERSON_TMP.prsn_is_audit_required,LS_DB_PERSON.prsn_interchange_id = LS_DB_PERSON_TMP.prsn_interchange_id,LS_DB_PERSON.prsn_icsr_ack = LS_DB_PERSON_TMP.prsn_icsr_ack,LS_DB_PERSON.prsn_hospital_name = LS_DB_PERSON_TMP.prsn_hospital_name,LS_DB_PERSON.prsn_fk_acu_rec_id = LS_DB_PERSON_TMP.prsn_fk_acu_rec_id,LS_DB_PERSON.prsn_first_name_in_upper = LS_DB_PERSON_TMP.prsn_first_name_in_upper,LS_DB_PERSON.prsn_first_name = LS_DB_PERSON_TMP.prsn_first_name,LS_DB_PERSON.prsn_fax_office_extn_no = LS_DB_PERSON_TMP.prsn_fax_office_extn_no,LS_DB_PERSON.prsn_fax_no = LS_DB_PERSON_TMP.prsn_fax_no,LS_DB_PERSON.prsn_fax_extension_number = LS_DB_PERSON_TMP.prsn_fax_extension_number,LS_DB_PERSON.prsn_fax_country_code = LS_DB_PERSON_TMP.prsn_fax_country_code,LS_DB_PERSON.prsn_external_app_updated_date = LS_DB_PERSON_TMP.prsn_external_app_updated_date,LS_DB_PERSON.prsn_external_app_rec_id = LS_DB_PERSON_TMP.prsn_external_app_rec_id,LS_DB_PERSON.prsn_ext_no = LS_DB_PERSON_TMP.prsn_ext_no,LS_DB_PERSON.prsn_emirf_account_id = LS_DB_PERSON_TMP.prsn_emirf_account_id,LS_DB_PERSON.prsn_email_id_in_upper = LS_DB_PERSON_TMP.prsn_email_id_in_upper,LS_DB_PERSON.prsn_email_id = LS_DB_PERSON_TMP.prsn_email_id,LS_DB_PERSON.prsn_e2b_flag = LS_DB_PERSON_TMP.prsn_e2b_flag,LS_DB_PERSON.prsn_doc_name_kanji = LS_DB_PERSON_TMP.prsn_doc_name_kanji,LS_DB_PERSON.prsn_doc_name_kana = LS_DB_PERSON_TMP.prsn_doc_name_kana,LS_DB_PERSON.prsn_do_not_contact = LS_DB_PERSON_TMP.prsn_do_not_contact,LS_DB_PERSON.prsn_designation = LS_DB_PERSON_TMP.prsn_designation,LS_DB_PERSON.prsn_dept_record_id = LS_DB_PERSON_TMP.prsn_dept_record_id,LS_DB_PERSON.prsn_dept_name_in_upper = LS_DB_PERSON_TMP.prsn_dept_name_in_upper,LS_DB_PERSON.prsn_dept_name = LS_DB_PERSON_TMP.prsn_dept_name,LS_DB_PERSON.prsn_degree = LS_DB_PERSON_TMP.prsn_degree,LS_DB_PERSON.prsn_date_modified = LS_DB_PERSON_TMP.prsn_date_modified,LS_DB_PERSON.prsn_date_informed_receiver = LS_DB_PERSON_TMP.prsn_date_informed_receiver,LS_DB_PERSON.prsn_date_created = LS_DB_PERSON_TMP.prsn_date_created,LS_DB_PERSON.prsn_dataprivacy_present = LS_DB_PERSON_TMP.prsn_dataprivacy_present,LS_DB_PERSON.prsn_crm_source_flag = LS_DB_PERSON_TMP.prsn_crm_source_flag,LS_DB_PERSON.prsn_crm_source = LS_DB_PERSON_TMP.prsn_crm_source,LS_DB_PERSON.prsn_crm_record_id = LS_DB_PERSON_TMP.prsn_crm_record_id,LS_DB_PERSON.prsn_crm_last_modified_date = LS_DB_PERSON_TMP.prsn_crm_last_modified_date,LS_DB_PERSON.prsn_country = LS_DB_PERSON_TMP.prsn_country,LS_DB_PERSON.prsn_correspondence_seq = LS_DB_PERSON_TMP.prsn_correspondence_seq,LS_DB_PERSON.prsn_correspondence_flag = LS_DB_PERSON_TMP.prsn_correspondence_flag,LS_DB_PERSON.prsn_contact_source = LS_DB_PERSON_TMP.prsn_contact_source,LS_DB_PERSON.prsn_contact_ins_from_schedler = LS_DB_PERSON_TMP.prsn_contact_ins_from_schedler,LS_DB_PERSON.prsn_consent_share = LS_DB_PERSON_TMP.prsn_consent_share,LS_DB_PERSON.prsn_consent_collect = LS_DB_PERSON_TMP.prsn_consent_collect,LS_DB_PERSON.prsn_consent = LS_DB_PERSON_TMP.prsn_consent,LS_DB_PERSON.prsn_company_unit_rec_id = LS_DB_PERSON_TMP.prsn_company_unit_rec_id,LS_DB_PERSON.prsn_comm_medium_others = LS_DB_PERSON_TMP.prsn_comm_medium_others,LS_DB_PERSON.prsn_city = LS_DB_PERSON_TMP.prsn_city,LS_DB_PERSON.prsn_archive_based_on_date = LS_DB_PERSON_TMP.prsn_archive_based_on_date,LS_DB_PERSON.prsn_app_rec_id = LS_DB_PERSON_TMP.prsn_app_rec_id,LS_DB_PERSON.prsn_app_id = LS_DB_PERSON_TMP.prsn_app_id,LS_DB_PERSON.prsn_all_company_unit = LS_DB_PERSON_TMP.prsn_all_company_unit,LS_DB_PERSON.prsn_all_account_assign = LS_DB_PERSON_TMP.prsn_all_account_assign,LS_DB_PERSON.prsn_address = LS_DB_PERSON_TMP.prsn_address,LS_DB_PERSON.prsn_active = LS_DB_PERSON_TMP.prsn_active,LS_DB_PERSON.prsn_account_name = LS_DB_PERSON_TMP.prsn_account_name,LS_DB_PERSON.prsn_account_id = LS_DB_PERSON_TMP.prsn_account_id,LS_DB_PERSON.prsncomp_user_modified = LS_DB_PERSON_TMP.prsncomp_user_modified,LS_DB_PERSON.prsncomp_user_created = LS_DB_PERSON_TMP.prsncomp_user_created,LS_DB_PERSON.prsncomp_spr_id = LS_DB_PERSON_TMP.prsncomp_spr_id,LS_DB_PERSON.prsncomp_record_id = LS_DB_PERSON_TMP.prsncomp_record_id,LS_DB_PERSON.prsncomp_fk_person_record_id = LS_DB_PERSON_TMP.prsncomp_fk_person_record_id,LS_DB_PERSON.prsncomp_date_modified = LS_DB_PERSON_TMP.prsncomp_date_modified,LS_DB_PERSON.prsncomp_date_created = LS_DB_PERSON_TMP.prsncomp_date_created,LS_DB_PERSON.prsncomp_company_unit_record_id = LS_DB_PERSON_TMP.prsncomp_company_unit_record_id,LS_DB_PERSON.prsncomp_company_unit_name = LS_DB_PERSON_TMP.prsncomp_company_unit_name,LS_DB_PERSON.prsnacc_user_modified = LS_DB_PERSON_TMP.prsnacc_user_modified,LS_DB_PERSON.prsnacc_user_created = LS_DB_PERSON_TMP.prsnacc_user_created,LS_DB_PERSON.prsnacc_spr_id = LS_DB_PERSON_TMP.prsnacc_spr_id,LS_DB_PERSON.prsnacc_record_id = LS_DB_PERSON_TMP.prsnacc_record_id,LS_DB_PERSON.prsnacc_primary_account = LS_DB_PERSON_TMP.prsnacc_primary_account,LS_DB_PERSON.prsnacc_fk_person_record_id = LS_DB_PERSON_TMP.prsnacc_fk_person_record_id,LS_DB_PERSON.prsnacc_fk_account_record_id = LS_DB_PERSON_TMP.prsnacc_fk_account_record_id,LS_DB_PERSON.prsnacc_date_modified = LS_DB_PERSON_TMP.prsnacc_date_modified,LS_DB_PERSON.prsnacc_date_created = LS_DB_PERSON_TMP.prsnacc_date_created,LS_DB_PERSON.prsnacc_account_type = LS_DB_PERSON_TMP.prsnacc_account_type,LS_DB_PERSON.prsnacc_account_name = LS_DB_PERSON_TMP.prsnacc_account_name,
LS_DB_PERSON.PROCESSING_DT = LS_DB_PERSON_TMP.PROCESSING_DT,
LS_DB_PERSON.user_modified  =LS_DB_PERSON_TMP.user_modified     ,
LS_DB_PERSON.date_modified  =LS_DB_PERSON_TMP.date_modified     ,
LS_DB_PERSON.expiry_date    =LS_DB_PERSON_TMP.expiry_date       ,
LS_DB_PERSON.created_by     =LS_DB_PERSON_TMP.created_by        ,
LS_DB_PERSON.created_dt     =LS_DB_PERSON_TMP.created_dt        ,
LS_DB_PERSON.load_ts        =LS_DB_PERSON_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP 
WHERE 	LS_DB_PERSON.INTEGRATION_ID = LS_DB_PERSON_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PERSON_TMP.PROCESSING_DT = LS_DB_PERSON.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PERSON.prsnacc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PERSON.prsncomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PERSON.prsn_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PERSON_TMP.prsnacc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PERSON_TMP.prsncomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PERSON_TMP.prsn_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PERSON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PERSON.prsn_RECORD_ID ,LS_DB_PERSON.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP 
ON LS_DB_PERSON.prsn_RECORD_ID=LS_DB_PERSON_TMP.prsn_RECORD_ID
AND LS_DB_PERSON.INTEGRATION_ID = LS_DB_PERSON_TMP.INTEGRATION_ID 
where LS_DB_PERSON_TMP.INTEGRATION_ID  is null AND LS_DB_PERSON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PERSON.prsn_RECORD_ID in (select prsn_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP )
) TMP where LS_DB_PERSON.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PERSON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PERSON.prsn_RECORD_ID ,LS_DB_PERSON.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP 
    ON LS_DB_PERSON.prsn_RECORD_ID=LS_DB_PERSON_TMP.prsn_RECORD_ID
    AND LS_DB_PERSON.INTEGRATION_ID = LS_DB_PERSON_TMP.INTEGRATION_ID 
    where LS_DB_PERSON_TMP.INTEGRATION_ID  is null AND LS_DB_PERSON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PERSON.prsn_RECORD_ID in (select prsn_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prsn_zip_code,
prsn_vet_department,
prsn_version,
prsn_validity_to,
prsn_validity_from,
prsn_user_modified,
prsn_user_created,
prsn_title,
prsn_territory_rec_id,
prsn_task_flag,
prsn_suffix,
prsn_state,
prsn_sso_token_id,
prsn_spr_id,
prsn_specialization,
prsn_sir_flag,
prsn_sex,
prsn_sales_territory_id,
prsn_requester_type,
prsn_requester_category_vet,
prsn_requester_category,
prsn_request_id,
prsn_reporter_type,
prsn_reducted,
prsn_record_id,
prsn_read_unread_correspondence,
prsn_public_contact,
prsn_profession,
prsn_privacy_info,
prsn_primary_postal_code,
prsn_pref_comm_medium,
prsn_postal_code,
prsn_phone_no_residence_no,
prsn_phone_no_office_extn_no,
prsn_phone_no_office,
prsn_phone_country_code,
prsn_person_type,
prsn_person_primary_id,
prsn_person_id,
prsn_other_in_upper,
prsn_other,
prsn_organization_name_in_upper,
prsn_organization_name,
prsn_organization_id,
prsn_organization,
prsn_notes_flag,
prsn_mobile_number,
prsn_middle_name_in_upper,
prsn_middle_name,
prsn_medical_interest,
prsn_lastuseddate,
prsn_last_name_in_upper,
prsn_last_name,
prsn_is_mdn_req,
prsn_is_inquiry_contact,
prsn_is_health_professional,
prsn_is_exported,
prsn_is_e2b_contact,
prsn_is_distribution,
prsn_is_audit_required,
prsn_interchange_id,
prsn_icsr_ack,
prsn_hospital_name,
prsn_fk_acu_rec_id,
prsn_first_name_in_upper,
prsn_first_name,
prsn_fax_office_extn_no,
prsn_fax_no,
prsn_fax_extension_number,
prsn_fax_country_code,
prsn_external_app_updated_date,
prsn_external_app_rec_id,
prsn_ext_no,
prsn_emirf_account_id,
prsn_email_id_in_upper,
prsn_email_id,
prsn_e2b_flag,
prsn_doc_name_kanji,
prsn_doc_name_kana,
prsn_do_not_contact,
prsn_designation,
prsn_dept_record_id,
prsn_dept_name_in_upper,
prsn_dept_name,
prsn_degree,
prsn_date_modified,
prsn_date_informed_receiver,
prsn_date_created,
prsn_dataprivacy_present,
prsn_crm_source_flag,
prsn_crm_source,
prsn_crm_record_id,
prsn_crm_last_modified_date,
prsn_country,
prsn_correspondence_seq,
prsn_correspondence_flag,
prsn_contact_source,
prsn_contact_ins_from_schedler,
prsn_consent_share,
prsn_consent_collect,
prsn_consent,
prsn_company_unit_rec_id,
prsn_comm_medium_others,
prsn_city,
prsn_archive_based_on_date,
prsn_app_rec_id,
prsn_app_id,
prsn_all_company_unit,
prsn_all_account_assign,
prsn_address,
prsn_active,
prsn_account_name,
prsn_account_id,
prsncomp_user_modified,
prsncomp_user_created,
prsncomp_spr_id,
prsncomp_record_id,
prsncomp_fk_person_record_id,
prsncomp_date_modified,
prsncomp_date_created,
prsncomp_company_unit_record_id,
prsncomp_company_unit_name,
prsnacc_user_modified,
prsnacc_user_created,
prsnacc_spr_id,
prsnacc_record_id,
prsnacc_primary_account,
prsnacc_fk_person_record_id,
prsnacc_fk_account_record_id,
prsnacc_date_modified,
prsnacc_date_created,
prsnacc_account_type,
prsnacc_account_name)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prsn_zip_code,
prsn_vet_department,
prsn_version,
prsn_validity_to,
prsn_validity_from,
prsn_user_modified,
prsn_user_created,
prsn_title,
prsn_territory_rec_id,
prsn_task_flag,
prsn_suffix,
prsn_state,
prsn_sso_token_id,
prsn_spr_id,
prsn_specialization,
prsn_sir_flag,
prsn_sex,
prsn_sales_territory_id,
prsn_requester_type,
prsn_requester_category_vet,
prsn_requester_category,
prsn_request_id,
prsn_reporter_type,
prsn_reducted,
prsn_record_id,
prsn_read_unread_correspondence,
prsn_public_contact,
prsn_profession,
prsn_privacy_info,
prsn_primary_postal_code,
prsn_pref_comm_medium,
prsn_postal_code,
prsn_phone_no_residence_no,
prsn_phone_no_office_extn_no,
prsn_phone_no_office,
prsn_phone_country_code,
prsn_person_type,
prsn_person_primary_id,
prsn_person_id,
prsn_other_in_upper,
prsn_other,
prsn_organization_name_in_upper,
prsn_organization_name,
prsn_organization_id,
prsn_organization,
prsn_notes_flag,
prsn_mobile_number,
prsn_middle_name_in_upper,
prsn_middle_name,
prsn_medical_interest,
prsn_lastuseddate,
prsn_last_name_in_upper,
prsn_last_name,
prsn_is_mdn_req,
prsn_is_inquiry_contact,
prsn_is_health_professional,
prsn_is_exported,
prsn_is_e2b_contact,
prsn_is_distribution,
prsn_is_audit_required,
prsn_interchange_id,
prsn_icsr_ack,
prsn_hospital_name,
prsn_fk_acu_rec_id,
prsn_first_name_in_upper,
prsn_first_name,
prsn_fax_office_extn_no,
prsn_fax_no,
prsn_fax_extension_number,
prsn_fax_country_code,
prsn_external_app_updated_date,
prsn_external_app_rec_id,
prsn_ext_no,
prsn_emirf_account_id,
prsn_email_id_in_upper,
prsn_email_id,
prsn_e2b_flag,
prsn_doc_name_kanji,
prsn_doc_name_kana,
prsn_do_not_contact,
prsn_designation,
prsn_dept_record_id,
prsn_dept_name_in_upper,
prsn_dept_name,
prsn_degree,
prsn_date_modified,
prsn_date_informed_receiver,
prsn_date_created,
prsn_dataprivacy_present,
prsn_crm_source_flag,
prsn_crm_source,
prsn_crm_record_id,
prsn_crm_last_modified_date,
prsn_country,
prsn_correspondence_seq,
prsn_correspondence_flag,
prsn_contact_source,
prsn_contact_ins_from_schedler,
prsn_consent_share,
prsn_consent_collect,
prsn_consent,
prsn_company_unit_rec_id,
prsn_comm_medium_others,
prsn_city,
prsn_archive_based_on_date,
prsn_app_rec_id,
prsn_app_id,
prsn_all_company_unit,
prsn_all_account_assign,
prsn_address,
prsn_active,
prsn_account_name,
prsn_account_id,
prsncomp_user_modified,
prsncomp_user_created,
prsncomp_spr_id,
prsncomp_record_id,
prsncomp_fk_person_record_id,
prsncomp_date_modified,
prsncomp_date_created,
prsncomp_company_unit_record_id,
prsncomp_company_unit_name,
prsnacc_user_modified,
prsnacc_user_created,
prsnacc_spr_id,
prsnacc_record_id,
prsnacc_primary_account,
prsnacc_fk_person_record_id,
prsnacc_fk_account_record_id,
prsnacc_date_modified,
prsnacc_date_created,
prsnacc_account_type,
prsnacc_account_name
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON TGT
WHERE  ( prsn_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person') OR prsnacc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_account') OR prsncomp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_company_unit')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP 
WHERE 	TO_DATE(LS_DB_PERSON.PROCESSING_DT) < TO_DATE(LS_DB_PERSON_TMP.PROCESSING_DT)
AND LS_DB_PERSON.INTEGRATION_ID = LS_DB_PERSON_TMP.INTEGRATION_ID
AND LS_DB_PERSON.prsn_RECORD_ID = LS_DB_PERSON_TMP.prsn_RECORD_ID
AND LS_DB_PERSON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PERSON.prsnacc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PERSON.prsncomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PERSON.prsn_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PERSON_TMP.prsnacc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PERSON_TMP.prsncomp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PERSON_TMP.prsn_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( prsn_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person') OR prsnacc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_account') OR prsncomp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PERSON_DELETION_TMP  WHERE TABLE_NAME='lsmv_person_company_unit')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PERSON_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PERSON'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PERSON Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PERSON'),
	'LSDB','Case','LS_DB_PERSON',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_PERSON not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
