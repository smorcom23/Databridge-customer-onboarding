
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_STUDY()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_STUDY'),
	'LSRA','Case','LS_DB_STUDY',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_STUDY' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_STUDY'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_STUDY'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  As select RECORD_ID,'lsmv_study' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_crossrfind_detail' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_crossrfind_detail WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_study_registration_detail' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_registration_detail WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1004','10125','112','132','133','134','136','15','305','340','341','4','54','54','7077','9929')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_crossrfind_detail WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_STUDY_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_crossrfind_detail WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_registration_detail WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_STUDY_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study_registration_detail WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_STUDY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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
                                                                                      from $$STG_DB_NAME.$$LSDB_RPL.LSMV_AER_INFO AER_INFO,$$STG_DB_NAME.$$LSDB_RPL.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_study_registration_detail_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  stdyregdtl_ari_rec_id,country  stdyregdtl_country,country_nf  stdyregdtl_country_nf,date_created  stdyregdtl_date_created,date_modified  stdyregdtl_date_modified,fk_ari_rec_id  stdyregdtl_fk_ari_rec_id,fk_calllog_id  stdyregdtl_fk_calllog_id,fk_study_rec_id  stdyregdtl_fk_study_rec_id,inq_rec_id  stdyregdtl_inq_rec_id,no_of_medicinalproduct  stdyregdtl_no_of_medicinalproduct,no_of_patientsenrolled  stdyregdtl_no_of_patientsenrolled,record_id  stdyregdtl_record_id,reg_no  stdyregdtl_reg_no,reg_no_nf  stdyregdtl_reg_no_nf,spr_id  stdyregdtl_spr_id,study_registration_date  stdyregdtl_study_registration_date,user_created  stdyregdtl_user_created,user_modified  stdyregdtl_user_modified,uuid  stdyregdtl_uuid,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_study_registration_detail
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_STUDY_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration_detail')
  ) where REC_RANK=1 )
  , lsmv_study_crossrfind_detail_SUBSET AS 
(
select * from 
    (SELECT  
    airi_record_id  stdycfdtl_airi_record_id,ari_rec_id  stdycfdtl_ari_rec_id,date_created  stdycfdtl_date_created,date_modified  stdycfdtl_date_modified,fk_study_rec_id  stdycfdtl_fk_study_rec_id,ps_uuid  stdycfdtl_ps_uuid,record_id  stdycfdtl_record_id,reported_spsr_study_no  stdycfdtl_reported_spsr_study_no,spr_id  stdycfdtl_spr_id,study_cr_type  stdycfdtl_study_cr_type,study_cross_ref_ind  stdycfdtl_study_cross_ref_ind,study_cross_ref_ind_nf  stdycfdtl_study_cross_ref_ind_nf,study_name  stdycfdtl_study_name,user_created  stdycfdtl_user_created,user_modified  stdycfdtl_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_study_crossrfind_detail
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_STUDY_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_crossrfind_detail')
  ) where REC_RANK=1 )
  , lsmv_study_SUBSET AS 
(
select * from 
    (SELECT  
    actual_end_date  stdy_actual_end_date,actual_end_date_fmt  stdy_actual_end_date_fmt,actual_start_date  stdy_actual_start_date,actual_start_date_fmt  stdy_actual_start_date_fmt,ae_occur_during_study  stdy_ae_occur_during_study,any_other_key  stdy_any_other_key,approval  stdy_approval,ari_rec_id  stdy_ari_rec_id,blinding_technique  stdy_blinding_technique,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='134' AND CD=CAST(blinding_technique AS VARCHAR(100)) )stdy_blinding_technique_de_ml , center_no  stdy_center_no,clinical_trial  stdy_clinical_trial,code_broken  stdy_code_broken,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='54' AND CD=CAST(code_broken AS VARCHAR(100)) )stdy_code_broken_de_ml , comp_rec_id  stdy_comp_rec_id,comparator_drug  stdy_comparator_drug,copy_to_safety  stdy_copy_to_safety,ct_notify_sub_times  stdy_ct_notify_sub_times,ctd_to_ctr_transition_date  stdy_ctd_to_ctr_transition_date,date_code_broken  stdy_date_code_broken,date_created  stdy_date_created,date_modified  stdy_date_modified,date_recv_monitor  stdy_date_recv_monitor,description  stdy_description,e2b_r3_registration_country  stdy_e2b_r3_registration_country,e2b_r3_registration_country_nf  stdy_e2b_r3_registration_country_nf,e2b_r3_registration_no  stdy_e2b_r3_registration_no,e2b_r3_registration_no_nf  stdy_e2b_r3_registration_no_nf,enrolment_date  stdy_enrolment_date,enrolment_status  stdy_enrolment_status,entity_updated  stdy_entity_updated,euct_regulation  stdy_euct_regulation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(euct_regulation AS VARCHAR(100)) )stdy_euct_regulation_de_ml , eudract_no  stdy_eudract_no,ext_clob_fld  stdy_ext_clob_fld,global_study_enrollment_count  stdy_global_study_enrollment_count,iis  stdy_iis,inq_rec_id  stdy_inq_rec_id,invest_product_blinded  stdy_invest_product_blinded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(invest_product_blinded AS VARCHAR(100)) )stdy_invest_product_blinded_de_ml , investigation_site  stdy_investigation_site,investigation_site_record_id  stdy_investigation_site_record_id,investigator_no  stdy_investigator_no,lsmv_study_title  stdy_lsmv_study_title,nda_sign  stdy_nda_sign,other_report1  stdy_other_report1,other_report2  stdy_other_report2,other_report3  stdy_other_report3,other_report4  stdy_other_report4,other_study  stdy_other_study,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9929' AND CD=CAST(other_study AS VARCHAR(100)) )stdy_other_study_de_ml , panda_no  stdy_panda_no,panda_no_nf  stdy_panda_no_nf,patient_exposure  stdy_patient_exposure,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='136' AND CD=CAST(patient_exposure AS VARCHAR(100)) )stdy_patient_exposure_de_ml , primary_ind  stdy_primary_ind,primary_ind_nf  stdy_primary_ind_nf,primary_test_compound  stdy_primary_test_compound,prior_patient_no  stdy_prior_patient_no,prior_patient_study  stdy_prior_patient_study,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(prior_patient_study AS VARCHAR(100)) )stdy_prior_patient_study_de_ml , prior_protocol_no  stdy_prior_protocol_no,prior_type_of_drug  stdy_prior_type_of_drug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='341' AND CD=CAST(prior_type_of_drug AS VARCHAR(100)) )stdy_prior_type_of_drug_de_ml , project_no  stdy_project_no,protocol_details  stdy_protocol_details,protocol_no  stdy_protocol_no,protocol_no_nf  stdy_protocol_no_nf,protocol_title  stdy_protocol_title,ps_uuid  stdy_ps_uuid,query_contact  stdy_query_contact,randomization_number  stdy_randomization_number,record_id  stdy_record_id,reported_primary_ind  stdy_reported_primary_ind,safety_reporting_responsibility  stdy_safety_reporting_responsibility,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10125' AND CD=CAST(safety_reporting_responsibility AS VARCHAR(100)) )stdy_safety_reporting_responsibility_de_ml , safetyreportid  stdy_safetyreportid,short_protocol_desc  stdy_short_protocol_desc,site_number  stdy_site_number,site_number_old  stdy_site_number_old,sponsor  stdy_sponsor,sponsor_nf  stdy_sponsor_nf,spr_id  stdy_spr_id,study_acronym  stdy_study_acronym,study_code_broken  stdy_study_code_broken,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='54' AND CD=CAST(study_code_broken AS VARCHAR(100)) )stdy_study_code_broken_de_ml , study_completion_status  stdy_study_completion_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='305' AND CD=CAST(study_completion_status AS VARCHAR(100)) )stdy_study_completion_status_de_ml , study_design  stdy_study_design,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='132' AND CD=CAST(study_design AS VARCHAR(100)) )stdy_study_design_de_ml , study_discont_reason_code  stdy_study_discont_reason_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='112' AND CD=CAST(study_discont_reason_code AS VARCHAR(100)) )stdy_study_discont_reason_code_de_ml , study_drug  stdy_study_drug,study_end_date  stdy_study_end_date,study_library_record_id  stdy_study_library_record_id,study_patient_no  stdy_study_patient_no,study_phase  stdy_study_phase,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='133' AND CD=CAST(study_phase AS VARCHAR(100)) )stdy_study_phase_de_ml , study_phase_number  stdy_study_phase_number,study_serial_number  stdy_study_serial_number,study_start_date  stdy_study_start_date,study_subject_father  stdy_study_subject_father,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(study_subject_father AS VARCHAR(100)) )stdy_study_subject_father_de_ml , study_subject_mother  stdy_study_subject_mother,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(study_subject_mother AS VARCHAR(100)) )stdy_study_subject_mother_de_ml , study_title  stdy_study_title,study_title_nf  stdy_study_title_nf,study_type  stdy_study_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1004' AND CD=CAST(study_type AS VARCHAR(100)) )stdy_study_type_de_ml , subject_id  stdy_subject_id,targeted_disease  stdy_targeted_disease,treatment_group_no  stdy_treatment_group_no,treatmentarm_name  stdy_treatmentarm_name,type_of_drug  stdy_type_of_drug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='340' AND CD=CAST(type_of_drug AS VARCHAR(100)) )stdy_type_of_drug_de_ml , type_of_report  stdy_type_of_report,unblinded_information  stdy_unblinded_information,user_created  stdy_user_created,user_modified  stdy_user_modified,version  stdy_version,withdrawn_date  stdy_withdrawn_date,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_study
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_study_registration_detail_SUBSET.stdyregdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_study_SUBSET.stdy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_study_SUBSET.stdy_USER_MODIFIED USER_MODIFIED,lsmv_study_SUBSET.stdy_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_study_SUBSET.stdy_USER_CREATED CREATED_BY,lsmv_study_SUBSET.stdy_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_study_SUBSET.stdy_withdrawn_date  ,lsmv_study_SUBSET.stdy_version  ,lsmv_study_SUBSET.stdy_user_modified  ,lsmv_study_SUBSET.stdy_user_created  ,lsmv_study_SUBSET.stdy_unblinded_information  ,lsmv_study_SUBSET.stdy_type_of_report  ,lsmv_study_SUBSET.stdy_type_of_drug_de_ml  ,lsmv_study_SUBSET.stdy_type_of_drug  ,lsmv_study_SUBSET.stdy_treatmentarm_name  ,lsmv_study_SUBSET.stdy_treatment_group_no  ,lsmv_study_SUBSET.stdy_targeted_disease  ,lsmv_study_SUBSET.stdy_subject_id  ,lsmv_study_SUBSET.stdy_study_type_de_ml  ,lsmv_study_SUBSET.stdy_study_type  ,lsmv_study_SUBSET.stdy_study_title_nf  ,lsmv_study_SUBSET.stdy_study_title  ,lsmv_study_SUBSET.stdy_study_subject_mother_de_ml  ,lsmv_study_SUBSET.stdy_study_subject_mother  ,lsmv_study_SUBSET.stdy_study_subject_father_de_ml  ,lsmv_study_SUBSET.stdy_study_subject_father  ,lsmv_study_SUBSET.stdy_study_start_date  ,lsmv_study_SUBSET.stdy_study_serial_number  ,lsmv_study_SUBSET.stdy_study_phase_number  ,lsmv_study_SUBSET.stdy_study_phase_de_ml  ,lsmv_study_SUBSET.stdy_study_phase  ,lsmv_study_SUBSET.stdy_study_patient_no  ,lsmv_study_SUBSET.stdy_study_library_record_id  ,lsmv_study_SUBSET.stdy_study_end_date  ,lsmv_study_SUBSET.stdy_study_drug  ,lsmv_study_SUBSET.stdy_study_discont_reason_code_de_ml  ,lsmv_study_SUBSET.stdy_study_discont_reason_code  ,lsmv_study_SUBSET.stdy_study_design_de_ml  ,lsmv_study_SUBSET.stdy_study_design  ,lsmv_study_SUBSET.stdy_study_completion_status_de_ml  ,lsmv_study_SUBSET.stdy_study_completion_status  ,lsmv_study_SUBSET.stdy_study_code_broken_de_ml  ,lsmv_study_SUBSET.stdy_study_code_broken  ,lsmv_study_SUBSET.stdy_study_acronym  ,lsmv_study_SUBSET.stdy_spr_id  ,lsmv_study_SUBSET.stdy_sponsor_nf  ,lsmv_study_SUBSET.stdy_sponsor  ,lsmv_study_SUBSET.stdy_site_number_old  ,lsmv_study_SUBSET.stdy_site_number  ,lsmv_study_SUBSET.stdy_short_protocol_desc  ,lsmv_study_SUBSET.stdy_safetyreportid  ,lsmv_study_SUBSET.stdy_safety_reporting_responsibility_de_ml  ,lsmv_study_SUBSET.stdy_safety_reporting_responsibility  ,lsmv_study_SUBSET.stdy_reported_primary_ind  ,lsmv_study_SUBSET.stdy_record_id  ,lsmv_study_SUBSET.stdy_randomization_number  ,lsmv_study_SUBSET.stdy_query_contact  ,lsmv_study_SUBSET.stdy_ps_uuid  ,lsmv_study_SUBSET.stdy_protocol_title  ,lsmv_study_SUBSET.stdy_protocol_no_nf  ,lsmv_study_SUBSET.stdy_protocol_no  ,lsmv_study_SUBSET.stdy_protocol_details  ,lsmv_study_SUBSET.stdy_project_no  ,lsmv_study_SUBSET.stdy_prior_type_of_drug_de_ml  ,lsmv_study_SUBSET.stdy_prior_type_of_drug  ,lsmv_study_SUBSET.stdy_prior_protocol_no  ,lsmv_study_SUBSET.stdy_prior_patient_study_de_ml  ,lsmv_study_SUBSET.stdy_prior_patient_study  ,lsmv_study_SUBSET.stdy_prior_patient_no  ,lsmv_study_SUBSET.stdy_primary_test_compound  ,lsmv_study_SUBSET.stdy_primary_ind_nf  ,lsmv_study_SUBSET.stdy_primary_ind  ,lsmv_study_SUBSET.stdy_patient_exposure_de_ml  ,lsmv_study_SUBSET.stdy_patient_exposure  ,lsmv_study_SUBSET.stdy_panda_no_nf  ,lsmv_study_SUBSET.stdy_panda_no  ,lsmv_study_SUBSET.stdy_other_study_de_ml  ,lsmv_study_SUBSET.stdy_other_study  ,lsmv_study_SUBSET.stdy_other_report4  ,lsmv_study_SUBSET.stdy_other_report3  ,lsmv_study_SUBSET.stdy_other_report2  ,lsmv_study_SUBSET.stdy_other_report1  ,lsmv_study_SUBSET.stdy_nda_sign  ,lsmv_study_SUBSET.stdy_lsmv_study_title  ,lsmv_study_SUBSET.stdy_investigator_no  ,lsmv_study_SUBSET.stdy_investigation_site_record_id  ,lsmv_study_SUBSET.stdy_investigation_site  ,lsmv_study_SUBSET.stdy_invest_product_blinded_de_ml  ,lsmv_study_SUBSET.stdy_invest_product_blinded  ,lsmv_study_SUBSET.stdy_inq_rec_id  ,lsmv_study_SUBSET.stdy_iis  ,lsmv_study_SUBSET.stdy_global_study_enrollment_count  ,lsmv_study_SUBSET.stdy_ext_clob_fld  ,lsmv_study_SUBSET.stdy_eudract_no  ,lsmv_study_SUBSET.stdy_euct_regulation_de_ml  ,lsmv_study_SUBSET.stdy_euct_regulation  ,lsmv_study_SUBSET.stdy_entity_updated  ,lsmv_study_SUBSET.stdy_enrolment_status  ,lsmv_study_SUBSET.stdy_enrolment_date  ,lsmv_study_SUBSET.stdy_e2b_r3_registration_no_nf  ,lsmv_study_SUBSET.stdy_e2b_r3_registration_no  ,lsmv_study_SUBSET.stdy_e2b_r3_registration_country_nf  ,lsmv_study_SUBSET.stdy_e2b_r3_registration_country  ,lsmv_study_SUBSET.stdy_description  ,lsmv_study_SUBSET.stdy_date_recv_monitor  ,lsmv_study_SUBSET.stdy_date_modified  ,lsmv_study_SUBSET.stdy_date_created  ,lsmv_study_SUBSET.stdy_date_code_broken  ,lsmv_study_SUBSET.stdy_ctd_to_ctr_transition_date  ,lsmv_study_SUBSET.stdy_ct_notify_sub_times  ,lsmv_study_SUBSET.stdy_copy_to_safety  ,lsmv_study_SUBSET.stdy_comparator_drug  ,lsmv_study_SUBSET.stdy_comp_rec_id  ,lsmv_study_SUBSET.stdy_code_broken_de_ml  ,lsmv_study_SUBSET.stdy_code_broken  ,lsmv_study_SUBSET.stdy_clinical_trial  ,lsmv_study_SUBSET.stdy_center_no  ,lsmv_study_SUBSET.stdy_blinding_technique_de_ml  ,lsmv_study_SUBSET.stdy_blinding_technique  ,lsmv_study_SUBSET.stdy_ari_rec_id  ,lsmv_study_SUBSET.stdy_approval  ,lsmv_study_SUBSET.stdy_any_other_key  ,lsmv_study_SUBSET.stdy_ae_occur_during_study  ,lsmv_study_SUBSET.stdy_actual_start_date_fmt  ,lsmv_study_SUBSET.stdy_actual_start_date  ,lsmv_study_SUBSET.stdy_actual_end_date_fmt  ,lsmv_study_SUBSET.stdy_actual_end_date  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_uuid  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_user_modified  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_user_created  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_study_registration_date  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_spr_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_reg_no_nf  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_reg_no  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_record_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_no_of_patientsenrolled  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_no_of_medicinalproduct  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_inq_rec_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_fk_study_rec_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_fk_calllog_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_fk_ari_rec_id  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_date_modified  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_date_created  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_country_nf  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_country  ,lsmv_study_registration_detail_SUBSET.stdyregdtl_ari_rec_id  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_user_modified  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_user_created  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_study_name  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_study_cross_ref_ind_nf  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_study_cross_ref_ind  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_study_cr_type  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_spr_id  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_reported_spsr_study_no  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_record_id  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_ps_uuid  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_fk_study_rec_id  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_date_modified  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_date_created  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_ari_rec_id  ,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_airi_record_id ,CONCAT( NVL(lsmv_study_registration_detail_SUBSET.stdyregdtl_RECORD_ID,-1),'||',NVL(lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_RECORD_ID,-1),'||',NVL(lsmv_study_SUBSET.stdy_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_study_SUBSET  LEFT JOIN lsmv_study_crossrfind_detail_SUBSET ON lsmv_study_SUBSET.stdy_record_id=lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_fk_study_rec_id
                         LEFT JOIN lsmv_study_registration_detail_SUBSET ON lsmv_study_SUBSET.stdy_record_id=lsmv_study_registration_detail_SUBSET.stdyregdtl_fk_study_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_study_registration_detail_SUBSET.stdyregdtl_RECORD_ID,lsmv_study_crossrfind_detail_SUBSET.stdycfdtl_RECORD_ID,lsmv_study_SUBSET.stdy_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP)
where target_table_name='LS_DB_STUDY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_STUDY'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY   
SET LS_DB_STUDY.stdy_withdrawn_date = LS_DB_STUDY_TMP.stdy_withdrawn_date,LS_DB_STUDY.stdy_version = LS_DB_STUDY_TMP.stdy_version,LS_DB_STUDY.stdy_user_modified = LS_DB_STUDY_TMP.stdy_user_modified,LS_DB_STUDY.stdy_user_created = LS_DB_STUDY_TMP.stdy_user_created,LS_DB_STUDY.stdy_unblinded_information = LS_DB_STUDY_TMP.stdy_unblinded_information,LS_DB_STUDY.stdy_type_of_report = LS_DB_STUDY_TMP.stdy_type_of_report,LS_DB_STUDY.stdy_type_of_drug_de_ml = LS_DB_STUDY_TMP.stdy_type_of_drug_de_ml,LS_DB_STUDY.stdy_type_of_drug = LS_DB_STUDY_TMP.stdy_type_of_drug,LS_DB_STUDY.stdy_treatmentarm_name = LS_DB_STUDY_TMP.stdy_treatmentarm_name,LS_DB_STUDY.stdy_treatment_group_no = LS_DB_STUDY_TMP.stdy_treatment_group_no,LS_DB_STUDY.stdy_targeted_disease = LS_DB_STUDY_TMP.stdy_targeted_disease,LS_DB_STUDY.stdy_subject_id = LS_DB_STUDY_TMP.stdy_subject_id,LS_DB_STUDY.stdy_study_type_de_ml = LS_DB_STUDY_TMP.stdy_study_type_de_ml,LS_DB_STUDY.stdy_study_type = LS_DB_STUDY_TMP.stdy_study_type,LS_DB_STUDY.stdy_study_title_nf = LS_DB_STUDY_TMP.stdy_study_title_nf,LS_DB_STUDY.stdy_study_title = LS_DB_STUDY_TMP.stdy_study_title,LS_DB_STUDY.stdy_study_subject_mother_de_ml = LS_DB_STUDY_TMP.stdy_study_subject_mother_de_ml,LS_DB_STUDY.stdy_study_subject_mother = LS_DB_STUDY_TMP.stdy_study_subject_mother,LS_DB_STUDY.stdy_study_subject_father_de_ml = LS_DB_STUDY_TMP.stdy_study_subject_father_de_ml,LS_DB_STUDY.stdy_study_subject_father = LS_DB_STUDY_TMP.stdy_study_subject_father,LS_DB_STUDY.stdy_study_start_date = LS_DB_STUDY_TMP.stdy_study_start_date,LS_DB_STUDY.stdy_study_serial_number = LS_DB_STUDY_TMP.stdy_study_serial_number,LS_DB_STUDY.stdy_study_phase_number = LS_DB_STUDY_TMP.stdy_study_phase_number,LS_DB_STUDY.stdy_study_phase_de_ml = LS_DB_STUDY_TMP.stdy_study_phase_de_ml,LS_DB_STUDY.stdy_study_phase = LS_DB_STUDY_TMP.stdy_study_phase,LS_DB_STUDY.stdy_study_patient_no = LS_DB_STUDY_TMP.stdy_study_patient_no,LS_DB_STUDY.stdy_study_library_record_id = LS_DB_STUDY_TMP.stdy_study_library_record_id,LS_DB_STUDY.stdy_study_end_date = LS_DB_STUDY_TMP.stdy_study_end_date,LS_DB_STUDY.stdy_study_drug = LS_DB_STUDY_TMP.stdy_study_drug,LS_DB_STUDY.stdy_study_discont_reason_code_de_ml = LS_DB_STUDY_TMP.stdy_study_discont_reason_code_de_ml,LS_DB_STUDY.stdy_study_discont_reason_code = LS_DB_STUDY_TMP.stdy_study_discont_reason_code,LS_DB_STUDY.stdy_study_design_de_ml = LS_DB_STUDY_TMP.stdy_study_design_de_ml,LS_DB_STUDY.stdy_study_design = LS_DB_STUDY_TMP.stdy_study_design,LS_DB_STUDY.stdy_study_completion_status_de_ml = LS_DB_STUDY_TMP.stdy_study_completion_status_de_ml,LS_DB_STUDY.stdy_study_completion_status = LS_DB_STUDY_TMP.stdy_study_completion_status,LS_DB_STUDY.stdy_study_code_broken_de_ml = LS_DB_STUDY_TMP.stdy_study_code_broken_de_ml,LS_DB_STUDY.stdy_study_code_broken = LS_DB_STUDY_TMP.stdy_study_code_broken,LS_DB_STUDY.stdy_study_acronym = LS_DB_STUDY_TMP.stdy_study_acronym,LS_DB_STUDY.stdy_spr_id = LS_DB_STUDY_TMP.stdy_spr_id,LS_DB_STUDY.stdy_sponsor_nf = LS_DB_STUDY_TMP.stdy_sponsor_nf,LS_DB_STUDY.stdy_sponsor = LS_DB_STUDY_TMP.stdy_sponsor,LS_DB_STUDY.stdy_site_number_old = LS_DB_STUDY_TMP.stdy_site_number_old,LS_DB_STUDY.stdy_site_number = LS_DB_STUDY_TMP.stdy_site_number,LS_DB_STUDY.stdy_short_protocol_desc = LS_DB_STUDY_TMP.stdy_short_protocol_desc,LS_DB_STUDY.stdy_safetyreportid = LS_DB_STUDY_TMP.stdy_safetyreportid,LS_DB_STUDY.stdy_safety_reporting_responsibility_de_ml = LS_DB_STUDY_TMP.stdy_safety_reporting_responsibility_de_ml,LS_DB_STUDY.stdy_safety_reporting_responsibility = LS_DB_STUDY_TMP.stdy_safety_reporting_responsibility,LS_DB_STUDY.stdy_reported_primary_ind = LS_DB_STUDY_TMP.stdy_reported_primary_ind,LS_DB_STUDY.stdy_record_id = LS_DB_STUDY_TMP.stdy_record_id,LS_DB_STUDY.stdy_randomization_number = LS_DB_STUDY_TMP.stdy_randomization_number,LS_DB_STUDY.stdy_query_contact = LS_DB_STUDY_TMP.stdy_query_contact,LS_DB_STUDY.stdy_ps_uuid = LS_DB_STUDY_TMP.stdy_ps_uuid,LS_DB_STUDY.stdy_protocol_title = LS_DB_STUDY_TMP.stdy_protocol_title,LS_DB_STUDY.stdy_protocol_no_nf = LS_DB_STUDY_TMP.stdy_protocol_no_nf,LS_DB_STUDY.stdy_protocol_no = LS_DB_STUDY_TMP.stdy_protocol_no,LS_DB_STUDY.stdy_protocol_details = LS_DB_STUDY_TMP.stdy_protocol_details,LS_DB_STUDY.stdy_project_no = LS_DB_STUDY_TMP.stdy_project_no,LS_DB_STUDY.stdy_prior_type_of_drug_de_ml = LS_DB_STUDY_TMP.stdy_prior_type_of_drug_de_ml,LS_DB_STUDY.stdy_prior_type_of_drug = LS_DB_STUDY_TMP.stdy_prior_type_of_drug,LS_DB_STUDY.stdy_prior_protocol_no = LS_DB_STUDY_TMP.stdy_prior_protocol_no,LS_DB_STUDY.stdy_prior_patient_study_de_ml = LS_DB_STUDY_TMP.stdy_prior_patient_study_de_ml,LS_DB_STUDY.stdy_prior_patient_study = LS_DB_STUDY_TMP.stdy_prior_patient_study,LS_DB_STUDY.stdy_prior_patient_no = LS_DB_STUDY_TMP.stdy_prior_patient_no,LS_DB_STUDY.stdy_primary_test_compound = LS_DB_STUDY_TMP.stdy_primary_test_compound,LS_DB_STUDY.stdy_primary_ind_nf = LS_DB_STUDY_TMP.stdy_primary_ind_nf,LS_DB_STUDY.stdy_primary_ind = LS_DB_STUDY_TMP.stdy_primary_ind,LS_DB_STUDY.stdy_patient_exposure_de_ml = LS_DB_STUDY_TMP.stdy_patient_exposure_de_ml,LS_DB_STUDY.stdy_patient_exposure = LS_DB_STUDY_TMP.stdy_patient_exposure,LS_DB_STUDY.stdy_panda_no_nf = LS_DB_STUDY_TMP.stdy_panda_no_nf,LS_DB_STUDY.stdy_panda_no = LS_DB_STUDY_TMP.stdy_panda_no,LS_DB_STUDY.stdy_other_study_de_ml = LS_DB_STUDY_TMP.stdy_other_study_de_ml,LS_DB_STUDY.stdy_other_study = LS_DB_STUDY_TMP.stdy_other_study,LS_DB_STUDY.stdy_other_report4 = LS_DB_STUDY_TMP.stdy_other_report4,LS_DB_STUDY.stdy_other_report3 = LS_DB_STUDY_TMP.stdy_other_report3,LS_DB_STUDY.stdy_other_report2 = LS_DB_STUDY_TMP.stdy_other_report2,LS_DB_STUDY.stdy_other_report1 = LS_DB_STUDY_TMP.stdy_other_report1,LS_DB_STUDY.stdy_nda_sign = LS_DB_STUDY_TMP.stdy_nda_sign,LS_DB_STUDY.stdy_lsmv_study_title = LS_DB_STUDY_TMP.stdy_lsmv_study_title,LS_DB_STUDY.stdy_investigator_no = LS_DB_STUDY_TMP.stdy_investigator_no,LS_DB_STUDY.stdy_investigation_site_record_id = LS_DB_STUDY_TMP.stdy_investigation_site_record_id,LS_DB_STUDY.stdy_investigation_site = LS_DB_STUDY_TMP.stdy_investigation_site,LS_DB_STUDY.stdy_invest_product_blinded_de_ml = LS_DB_STUDY_TMP.stdy_invest_product_blinded_de_ml,LS_DB_STUDY.stdy_invest_product_blinded = LS_DB_STUDY_TMP.stdy_invest_product_blinded,LS_DB_STUDY.stdy_inq_rec_id = LS_DB_STUDY_TMP.stdy_inq_rec_id,LS_DB_STUDY.stdy_iis = LS_DB_STUDY_TMP.stdy_iis,LS_DB_STUDY.stdy_global_study_enrollment_count = LS_DB_STUDY_TMP.stdy_global_study_enrollment_count,LS_DB_STUDY.stdy_ext_clob_fld = LS_DB_STUDY_TMP.stdy_ext_clob_fld,LS_DB_STUDY.stdy_eudract_no = LS_DB_STUDY_TMP.stdy_eudract_no,LS_DB_STUDY.stdy_euct_regulation_de_ml = LS_DB_STUDY_TMP.stdy_euct_regulation_de_ml,LS_DB_STUDY.stdy_euct_regulation = LS_DB_STUDY_TMP.stdy_euct_regulation,LS_DB_STUDY.stdy_entity_updated = LS_DB_STUDY_TMP.stdy_entity_updated,LS_DB_STUDY.stdy_enrolment_status = LS_DB_STUDY_TMP.stdy_enrolment_status,LS_DB_STUDY.stdy_enrolment_date = LS_DB_STUDY_TMP.stdy_enrolment_date,LS_DB_STUDY.stdy_e2b_r3_registration_no_nf = LS_DB_STUDY_TMP.stdy_e2b_r3_registration_no_nf,LS_DB_STUDY.stdy_e2b_r3_registration_no = LS_DB_STUDY_TMP.stdy_e2b_r3_registration_no,LS_DB_STUDY.stdy_e2b_r3_registration_country_nf = LS_DB_STUDY_TMP.stdy_e2b_r3_registration_country_nf,LS_DB_STUDY.stdy_e2b_r3_registration_country = LS_DB_STUDY_TMP.stdy_e2b_r3_registration_country,LS_DB_STUDY.stdy_description = LS_DB_STUDY_TMP.stdy_description,LS_DB_STUDY.stdy_date_recv_monitor = LS_DB_STUDY_TMP.stdy_date_recv_monitor,LS_DB_STUDY.stdy_date_modified = LS_DB_STUDY_TMP.stdy_date_modified,LS_DB_STUDY.stdy_date_created = LS_DB_STUDY_TMP.stdy_date_created,LS_DB_STUDY.stdy_date_code_broken = LS_DB_STUDY_TMP.stdy_date_code_broken,LS_DB_STUDY.stdy_ctd_to_ctr_transition_date = LS_DB_STUDY_TMP.stdy_ctd_to_ctr_transition_date,LS_DB_STUDY.stdy_ct_notify_sub_times = LS_DB_STUDY_TMP.stdy_ct_notify_sub_times,LS_DB_STUDY.stdy_copy_to_safety = LS_DB_STUDY_TMP.stdy_copy_to_safety,LS_DB_STUDY.stdy_comparator_drug = LS_DB_STUDY_TMP.stdy_comparator_drug,LS_DB_STUDY.stdy_comp_rec_id = LS_DB_STUDY_TMP.stdy_comp_rec_id,LS_DB_STUDY.stdy_code_broken_de_ml = LS_DB_STUDY_TMP.stdy_code_broken_de_ml,LS_DB_STUDY.stdy_code_broken = LS_DB_STUDY_TMP.stdy_code_broken,LS_DB_STUDY.stdy_clinical_trial = LS_DB_STUDY_TMP.stdy_clinical_trial,LS_DB_STUDY.stdy_center_no = LS_DB_STUDY_TMP.stdy_center_no,LS_DB_STUDY.stdy_blinding_technique_de_ml = LS_DB_STUDY_TMP.stdy_blinding_technique_de_ml,LS_DB_STUDY.stdy_blinding_technique = LS_DB_STUDY_TMP.stdy_blinding_technique,LS_DB_STUDY.stdy_ari_rec_id = LS_DB_STUDY_TMP.stdy_ari_rec_id,LS_DB_STUDY.stdy_approval = LS_DB_STUDY_TMP.stdy_approval,LS_DB_STUDY.stdy_any_other_key = LS_DB_STUDY_TMP.stdy_any_other_key,LS_DB_STUDY.stdy_ae_occur_during_study = LS_DB_STUDY_TMP.stdy_ae_occur_during_study,LS_DB_STUDY.stdy_actual_start_date_fmt = LS_DB_STUDY_TMP.stdy_actual_start_date_fmt,LS_DB_STUDY.stdy_actual_start_date = LS_DB_STUDY_TMP.stdy_actual_start_date,LS_DB_STUDY.stdy_actual_end_date_fmt = LS_DB_STUDY_TMP.stdy_actual_end_date_fmt,LS_DB_STUDY.stdy_actual_end_date = LS_DB_STUDY_TMP.stdy_actual_end_date,LS_DB_STUDY.stdyregdtl_uuid = LS_DB_STUDY_TMP.stdyregdtl_uuid,LS_DB_STUDY.stdyregdtl_user_modified = LS_DB_STUDY_TMP.stdyregdtl_user_modified,LS_DB_STUDY.stdyregdtl_user_created = LS_DB_STUDY_TMP.stdyregdtl_user_created,LS_DB_STUDY.stdyregdtl_study_registration_date = LS_DB_STUDY_TMP.stdyregdtl_study_registration_date,LS_DB_STUDY.stdyregdtl_spr_id = LS_DB_STUDY_TMP.stdyregdtl_spr_id,LS_DB_STUDY.stdyregdtl_reg_no_nf = LS_DB_STUDY_TMP.stdyregdtl_reg_no_nf,LS_DB_STUDY.stdyregdtl_reg_no = LS_DB_STUDY_TMP.stdyregdtl_reg_no,LS_DB_STUDY.stdyregdtl_record_id = LS_DB_STUDY_TMP.stdyregdtl_record_id,LS_DB_STUDY.stdyregdtl_no_of_patientsenrolled = LS_DB_STUDY_TMP.stdyregdtl_no_of_patientsenrolled,LS_DB_STUDY.stdyregdtl_no_of_medicinalproduct = LS_DB_STUDY_TMP.stdyregdtl_no_of_medicinalproduct,LS_DB_STUDY.stdyregdtl_inq_rec_id = LS_DB_STUDY_TMP.stdyregdtl_inq_rec_id,LS_DB_STUDY.stdyregdtl_fk_study_rec_id = LS_DB_STUDY_TMP.stdyregdtl_fk_study_rec_id,LS_DB_STUDY.stdyregdtl_fk_calllog_id = LS_DB_STUDY_TMP.stdyregdtl_fk_calllog_id,LS_DB_STUDY.stdyregdtl_fk_ari_rec_id = LS_DB_STUDY_TMP.stdyregdtl_fk_ari_rec_id,LS_DB_STUDY.stdyregdtl_date_modified = LS_DB_STUDY_TMP.stdyregdtl_date_modified,LS_DB_STUDY.stdyregdtl_date_created = LS_DB_STUDY_TMP.stdyregdtl_date_created,LS_DB_STUDY.stdyregdtl_country_nf = LS_DB_STUDY_TMP.stdyregdtl_country_nf,LS_DB_STUDY.stdyregdtl_country = LS_DB_STUDY_TMP.stdyregdtl_country,LS_DB_STUDY.stdyregdtl_ari_rec_id = LS_DB_STUDY_TMP.stdyregdtl_ari_rec_id,LS_DB_STUDY.stdycfdtl_user_modified = LS_DB_STUDY_TMP.stdycfdtl_user_modified,LS_DB_STUDY.stdycfdtl_user_created = LS_DB_STUDY_TMP.stdycfdtl_user_created,LS_DB_STUDY.stdycfdtl_study_name = LS_DB_STUDY_TMP.stdycfdtl_study_name,LS_DB_STUDY.stdycfdtl_study_cross_ref_ind_nf = LS_DB_STUDY_TMP.stdycfdtl_study_cross_ref_ind_nf,LS_DB_STUDY.stdycfdtl_study_cross_ref_ind = LS_DB_STUDY_TMP.stdycfdtl_study_cross_ref_ind,LS_DB_STUDY.stdycfdtl_study_cr_type = LS_DB_STUDY_TMP.stdycfdtl_study_cr_type,LS_DB_STUDY.stdycfdtl_spr_id = LS_DB_STUDY_TMP.stdycfdtl_spr_id,LS_DB_STUDY.stdycfdtl_reported_spsr_study_no = LS_DB_STUDY_TMP.stdycfdtl_reported_spsr_study_no,LS_DB_STUDY.stdycfdtl_record_id = LS_DB_STUDY_TMP.stdycfdtl_record_id,LS_DB_STUDY.stdycfdtl_ps_uuid = LS_DB_STUDY_TMP.stdycfdtl_ps_uuid,LS_DB_STUDY.stdycfdtl_fk_study_rec_id = LS_DB_STUDY_TMP.stdycfdtl_fk_study_rec_id,LS_DB_STUDY.stdycfdtl_date_modified = LS_DB_STUDY_TMP.stdycfdtl_date_modified,LS_DB_STUDY.stdycfdtl_date_created = LS_DB_STUDY_TMP.stdycfdtl_date_created,LS_DB_STUDY.stdycfdtl_ari_rec_id = LS_DB_STUDY_TMP.stdycfdtl_ari_rec_id,LS_DB_STUDY.stdycfdtl_airi_record_id = LS_DB_STUDY_TMP.stdycfdtl_airi_record_id,
LS_DB_STUDY.PROCESSING_DT = LS_DB_STUDY_TMP.PROCESSING_DT,
LS_DB_STUDY.receipt_id     =LS_DB_STUDY_TMP.receipt_id    ,
LS_DB_STUDY.case_no        =LS_DB_STUDY_TMP.case_no           ,
LS_DB_STUDY.case_version   =LS_DB_STUDY_TMP.case_version      ,
LS_DB_STUDY.version_no     =LS_DB_STUDY_TMP.version_no        ,
LS_DB_STUDY.user_modified  =LS_DB_STUDY_TMP.user_modified     ,
LS_DB_STUDY.date_modified  =LS_DB_STUDY_TMP.date_modified     ,
LS_DB_STUDY.expiry_date    =LS_DB_STUDY_TMP.expiry_date       ,
LS_DB_STUDY.created_by     =LS_DB_STUDY_TMP.created_by        ,
LS_DB_STUDY.created_dt     =LS_DB_STUDY_TMP.created_dt        ,
LS_DB_STUDY.load_ts        =LS_DB_STUDY_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP 
WHERE 	LS_DB_STUDY.INTEGRATION_ID = LS_DB_STUDY_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_STUDY_TMP.PROCESSING_DT = LS_DB_STUDY.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_STUDY.stdyregdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY.stdycfdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY.stdy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_STUDY_TMP.stdyregdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_TMP.stdycfdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_TMP.stdy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_STUDY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_STUDY.stdy_RECORD_ID ,LS_DB_STUDY.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP 
ON LS_DB_STUDY.stdy_RECORD_ID=LS_DB_STUDY_TMP.stdy_RECORD_ID
AND LS_DB_STUDY.INTEGRATION_ID = LS_DB_STUDY_TMP.INTEGRATION_ID 
where LS_DB_STUDY_TMP.INTEGRATION_ID  is null AND LS_DB_STUDY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_STUDY.stdy_RECORD_ID in (select stdy_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP )
) TMP where LS_DB_STUDY.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_STUDY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_STUDY.stdy_RECORD_ID ,LS_DB_STUDY.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP 
    ON LS_DB_STUDY.stdy_RECORD_ID=LS_DB_STUDY_TMP.stdy_RECORD_ID
    AND LS_DB_STUDY.INTEGRATION_ID = LS_DB_STUDY_TMP.INTEGRATION_ID 
    where LS_DB_STUDY_TMP.INTEGRATION_ID  is null AND LS_DB_STUDY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_STUDY.stdy_RECORD_ID in (select stdy_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY
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
integration_id ,stdy_withdrawn_date,
stdy_version,
stdy_user_modified,
stdy_user_created,
stdy_unblinded_information,
stdy_type_of_report,
stdy_type_of_drug_de_ml,
stdy_type_of_drug,
stdy_treatmentarm_name,
stdy_treatment_group_no,
stdy_targeted_disease,
stdy_subject_id,
stdy_study_type_de_ml,
stdy_study_type,
stdy_study_title_nf,
stdy_study_title,
stdy_study_subject_mother_de_ml,
stdy_study_subject_mother,
stdy_study_subject_father_de_ml,
stdy_study_subject_father,
stdy_study_start_date,
stdy_study_serial_number,
stdy_study_phase_number,
stdy_study_phase_de_ml,
stdy_study_phase,
stdy_study_patient_no,
stdy_study_library_record_id,
stdy_study_end_date,
stdy_study_drug,
stdy_study_discont_reason_code_de_ml,
stdy_study_discont_reason_code,
stdy_study_design_de_ml,
stdy_study_design,
stdy_study_completion_status_de_ml,
stdy_study_completion_status,
stdy_study_code_broken_de_ml,
stdy_study_code_broken,
stdy_study_acronym,
stdy_spr_id,
stdy_sponsor_nf,
stdy_sponsor,
stdy_site_number_old,
stdy_site_number,
stdy_short_protocol_desc,
stdy_safetyreportid,
stdy_safety_reporting_responsibility_de_ml,
stdy_safety_reporting_responsibility,
stdy_reported_primary_ind,
stdy_record_id,
stdy_randomization_number,
stdy_query_contact,
stdy_ps_uuid,
stdy_protocol_title,
stdy_protocol_no_nf,
stdy_protocol_no,
stdy_protocol_details,
stdy_project_no,
stdy_prior_type_of_drug_de_ml,
stdy_prior_type_of_drug,
stdy_prior_protocol_no,
stdy_prior_patient_study_de_ml,
stdy_prior_patient_study,
stdy_prior_patient_no,
stdy_primary_test_compound,
stdy_primary_ind_nf,
stdy_primary_ind,
stdy_patient_exposure_de_ml,
stdy_patient_exposure,
stdy_panda_no_nf,
stdy_panda_no,
stdy_other_study_de_ml,
stdy_other_study,
stdy_other_report4,
stdy_other_report3,
stdy_other_report2,
stdy_other_report1,
stdy_nda_sign,
stdy_lsmv_study_title,
stdy_investigator_no,
stdy_investigation_site_record_id,
stdy_investigation_site,
stdy_invest_product_blinded_de_ml,
stdy_invest_product_blinded,
stdy_inq_rec_id,
stdy_iis,
stdy_global_study_enrollment_count,
stdy_ext_clob_fld,
stdy_eudract_no,
stdy_euct_regulation_de_ml,
stdy_euct_regulation,
stdy_entity_updated,
stdy_enrolment_status,
stdy_enrolment_date,
stdy_e2b_r3_registration_no_nf,
stdy_e2b_r3_registration_no,
stdy_e2b_r3_registration_country_nf,
stdy_e2b_r3_registration_country,
stdy_description,
stdy_date_recv_monitor,
stdy_date_modified,
stdy_date_created,
stdy_date_code_broken,
stdy_ctd_to_ctr_transition_date,
stdy_ct_notify_sub_times,
stdy_copy_to_safety,
stdy_comparator_drug,
stdy_comp_rec_id,
stdy_code_broken_de_ml,
stdy_code_broken,
stdy_clinical_trial,
stdy_center_no,
stdy_blinding_technique_de_ml,
stdy_blinding_technique,
stdy_ari_rec_id,
stdy_approval,
stdy_any_other_key,
stdy_ae_occur_during_study,
stdy_actual_start_date_fmt,
stdy_actual_start_date,
stdy_actual_end_date_fmt,
stdy_actual_end_date,
stdyregdtl_uuid,
stdyregdtl_user_modified,
stdyregdtl_user_created,
stdyregdtl_study_registration_date,
stdyregdtl_spr_id,
stdyregdtl_reg_no_nf,
stdyregdtl_reg_no,
stdyregdtl_record_id,
stdyregdtl_no_of_patientsenrolled,
stdyregdtl_no_of_medicinalproduct,
stdyregdtl_inq_rec_id,
stdyregdtl_fk_study_rec_id,
stdyregdtl_fk_calllog_id,
stdyregdtl_fk_ari_rec_id,
stdyregdtl_date_modified,
stdyregdtl_date_created,
stdyregdtl_country_nf,
stdyregdtl_country,
stdyregdtl_ari_rec_id,
stdycfdtl_user_modified,
stdycfdtl_user_created,
stdycfdtl_study_name,
stdycfdtl_study_cross_ref_ind_nf,
stdycfdtl_study_cross_ref_ind,
stdycfdtl_study_cr_type,
stdycfdtl_spr_id,
stdycfdtl_reported_spsr_study_no,
stdycfdtl_record_id,
stdycfdtl_ps_uuid,
stdycfdtl_fk_study_rec_id,
stdycfdtl_date_modified,
stdycfdtl_date_created,
stdycfdtl_ari_rec_id,
stdycfdtl_airi_record_id)
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
integration_id ,stdy_withdrawn_date,
stdy_version,
stdy_user_modified,
stdy_user_created,
stdy_unblinded_information,
stdy_type_of_report,
stdy_type_of_drug_de_ml,
stdy_type_of_drug,
stdy_treatmentarm_name,
stdy_treatment_group_no,
stdy_targeted_disease,
stdy_subject_id,
stdy_study_type_de_ml,
stdy_study_type,
stdy_study_title_nf,
stdy_study_title,
stdy_study_subject_mother_de_ml,
stdy_study_subject_mother,
stdy_study_subject_father_de_ml,
stdy_study_subject_father,
stdy_study_start_date,
stdy_study_serial_number,
stdy_study_phase_number,
stdy_study_phase_de_ml,
stdy_study_phase,
stdy_study_patient_no,
stdy_study_library_record_id,
stdy_study_end_date,
stdy_study_drug,
stdy_study_discont_reason_code_de_ml,
stdy_study_discont_reason_code,
stdy_study_design_de_ml,
stdy_study_design,
stdy_study_completion_status_de_ml,
stdy_study_completion_status,
stdy_study_code_broken_de_ml,
stdy_study_code_broken,
stdy_study_acronym,
stdy_spr_id,
stdy_sponsor_nf,
stdy_sponsor,
stdy_site_number_old,
stdy_site_number,
stdy_short_protocol_desc,
stdy_safetyreportid,
stdy_safety_reporting_responsibility_de_ml,
stdy_safety_reporting_responsibility,
stdy_reported_primary_ind,
stdy_record_id,
stdy_randomization_number,
stdy_query_contact,
stdy_ps_uuid,
stdy_protocol_title,
stdy_protocol_no_nf,
stdy_protocol_no,
stdy_protocol_details,
stdy_project_no,
stdy_prior_type_of_drug_de_ml,
stdy_prior_type_of_drug,
stdy_prior_protocol_no,
stdy_prior_patient_study_de_ml,
stdy_prior_patient_study,
stdy_prior_patient_no,
stdy_primary_test_compound,
stdy_primary_ind_nf,
stdy_primary_ind,
stdy_patient_exposure_de_ml,
stdy_patient_exposure,
stdy_panda_no_nf,
stdy_panda_no,
stdy_other_study_de_ml,
stdy_other_study,
stdy_other_report4,
stdy_other_report3,
stdy_other_report2,
stdy_other_report1,
stdy_nda_sign,
stdy_lsmv_study_title,
stdy_investigator_no,
stdy_investigation_site_record_id,
stdy_investigation_site,
stdy_invest_product_blinded_de_ml,
stdy_invest_product_blinded,
stdy_inq_rec_id,
stdy_iis,
stdy_global_study_enrollment_count,
stdy_ext_clob_fld,
stdy_eudract_no,
stdy_euct_regulation_de_ml,
stdy_euct_regulation,
stdy_entity_updated,
stdy_enrolment_status,
stdy_enrolment_date,
stdy_e2b_r3_registration_no_nf,
stdy_e2b_r3_registration_no,
stdy_e2b_r3_registration_country_nf,
stdy_e2b_r3_registration_country,
stdy_description,
stdy_date_recv_monitor,
stdy_date_modified,
stdy_date_created,
stdy_date_code_broken,
stdy_ctd_to_ctr_transition_date,
stdy_ct_notify_sub_times,
stdy_copy_to_safety,
stdy_comparator_drug,
stdy_comp_rec_id,
stdy_code_broken_de_ml,
stdy_code_broken,
stdy_clinical_trial,
stdy_center_no,
stdy_blinding_technique_de_ml,
stdy_blinding_technique,
stdy_ari_rec_id,
stdy_approval,
stdy_any_other_key,
stdy_ae_occur_during_study,
stdy_actual_start_date_fmt,
stdy_actual_start_date,
stdy_actual_end_date_fmt,
stdy_actual_end_date,
stdyregdtl_uuid,
stdyregdtl_user_modified,
stdyregdtl_user_created,
stdyregdtl_study_registration_date,
stdyregdtl_spr_id,
stdyregdtl_reg_no_nf,
stdyregdtl_reg_no,
stdyregdtl_record_id,
stdyregdtl_no_of_patientsenrolled,
stdyregdtl_no_of_medicinalproduct,
stdyregdtl_inq_rec_id,
stdyregdtl_fk_study_rec_id,
stdyregdtl_fk_calllog_id,
stdyregdtl_fk_ari_rec_id,
stdyregdtl_date_modified,
stdyregdtl_date_created,
stdyregdtl_country_nf,
stdyregdtl_country,
stdyregdtl_ari_rec_id,
stdycfdtl_user_modified,
stdycfdtl_user_created,
stdycfdtl_study_name,
stdycfdtl_study_cross_ref_ind_nf,
stdycfdtl_study_cross_ref_ind,
stdycfdtl_study_cr_type,
stdycfdtl_spr_id,
stdycfdtl_reported_spsr_study_no,
stdycfdtl_record_id,
stdycfdtl_ps_uuid,
stdycfdtl_fk_study_rec_id,
stdycfdtl_date_modified,
stdycfdtl_date_created,
stdycfdtl_ari_rec_id,
stdycfdtl_airi_record_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP 
WHERE 	TO_DATE(LS_DB_STUDY.PROCESSING_DT) < TO_DATE(LS_DB_STUDY_TMP.PROCESSING_DT)
AND LS_DB_STUDY.INTEGRATION_ID = LS_DB_STUDY_TMP.INTEGRATION_ID
AND LS_DB_STUDY.stdy_RECORD_ID = LS_DB_STUDY_TMP.stdy_RECORD_ID
AND LS_DB_STUDY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_STUDY.stdyregdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY.stdycfdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_STUDY.stdy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_STUDY_TMP.stdyregdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_TMP.stdycfdtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_STUDY_TMP.stdy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY TGT
WHERE  ( stdy_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study') OR stdycfdtl_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_crossrfind_detail') OR stdyregdtl_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration_detail')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( stdy_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study') OR stdycfdtl_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_crossrfind_detail') OR stdyregdtl_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_STUDY_DELETION_TMP  WHERE TABLE_NAME='lsmv_study_registration_detail')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_STUDY_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_STUDY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_STUDY'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_STUDY Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_STUDY'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_STUDY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
