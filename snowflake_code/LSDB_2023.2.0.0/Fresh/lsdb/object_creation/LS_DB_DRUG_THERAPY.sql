
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_THERAPY'),
	'LSDB','Case','LS_DB_DRUG_THERAPY',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_THERAPY' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_THERAPY' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  As select RECORD_ID,'lsmv_drug_therapy' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_drug_therapy_best_doctor' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_best_doctor WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_drug_therapy_vaccine' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_vaccine WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1002','1014','1017','1018','1018','1018','1020','1020','17','17','21','4','5014','805','9031','9162','9163','9171','9992')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_vaccine WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_adt_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_vaccine WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_best_doctor WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_best_doctor WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_ADTBD_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_THERAPY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_drug_therapy_vaccine_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  dgthvac_ari_rec_id,date_created  dgthvac_date_created,date_modified  dgthvac_date_modified,fk_adt_rec_id  dgthvac_fk_adt_rec_id,record_id  dgthvac_record_id,spr_id  dgthvac_spr_id,user_created  dgthvac_user_created,user_modified  dgthvac_user_modified,vaccination_address1  dgthvac_vaccination_address1,vaccination_address1_nf  dgthvac_vaccination_address1_nf,vaccination_address2  dgthvac_vaccination_address2,vaccination_address2_nf  dgthvac_vaccination_address2_nf,vaccination_city  dgthvac_vaccination_city,vaccination_city_nf  dgthvac_vaccination_city_nf,vaccination_country  dgthvac_vaccination_country,vaccination_country_nf  dgthvac_vaccination_country_nf,vaccination_fax  dgthvac_vaccination_fax,vaccination_fax_nf  dgthvac_vaccination_fax_nf,vaccination_postal_code  dgthvac_vaccination_postal_code,vaccination_postal_code_nf  dgthvac_vaccination_postal_code_nf,vaccination_state  dgthvac_vaccination_state,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9171' AND CD=CAST(vaccination_state AS VARCHAR(100)) )dgthvac_vaccination_state_de_ml , vaccination_state_nf  dgthvac_vaccination_state_nf,vaccination_state_sf  dgthvac_vaccination_state_sf,vaccination_telephone  dgthvac_vaccination_telephone,vaccination_telephone_nf  dgthvac_vaccination_telephone_nf,vaccination_type  dgthvac_vaccination_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9163' AND CD=CAST(vaccination_type AS VARCHAR(100)) )dgthvac_vaccination_type_de_ml , vaccination_type_nf  dgthvac_vaccination_type_nf,vaccine_name  dgthvac_vaccine_name,vaccine_name_nf  dgthvac_vaccine_name_nf,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_vaccine
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_adt_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
  ) where REC_RANK=1 )
  , lsmv_drug_therapy_best_doctor_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  dgthbstdoc_ari_rec_id,date_created  dgthbstdoc_date_created,date_modified  dgthbstdoc_date_modified,email_id  dgthbstdoc_email_id,first_name  dgthbstdoc_first_name,last_name  dgthbstdoc_last_name,middle_name  dgthbstdoc_middle_name,record_id  dgthbstdoc_record_id,spr_id  dgthbstdoc_spr_id,telephone  dgthbstdoc_telephone,title  dgthbstdoc_title,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5014' AND CD=CAST(title AS VARCHAR(100)) )dgthbstdoc_title_de_ml , user_created  dgthbstdoc_user_created,user_modified  dgthbstdoc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy_best_doctor
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor')
  ) where REC_RANK=1 )
  , lsmv_drug_therapy_SUBSET AS 
(
select * from 
    (SELECT  
    actiondrug  dgth_actiondrug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='21' AND CD=CAST(actiondrug AS VARCHAR(100)) )dgth_actiondrug_de_ml , anatomical_approach_site  dgth_anatomical_approach_site,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9162' AND CD=CAST(anatomical_approach_site AS VARCHAR(100)) )dgth_anatomical_approach_site_de_ml , anatomical_approach_site_nf  dgth_anatomical_approach_site_nf,ari_rec_id  dgth_ari_rec_id,collection_date  dgth_collection_date,comp_rec_id  dgth_comp_rec_id,daily_dose  dgth_daily_dose,daily_dose_manual  dgth_daily_dose_manual,daily_dose_unit  dgth_daily_dose_unit,date_created  dgth_date_created,date_modified  dgth_date_modified,date_overdose_occurred  dgth_date_overdose_occurred,dates_of_use_ocr  dgth_dates_of_use_ocr,dose_num_series  dgth_dose_num_series,dose_prior_pregnancy_numb  dgth_dose_prior_pregnancy_numb,dose_prior_pregnancy_unit  dgth_dose_prior_pregnancy_unit,dosposition  dgth_dosposition,drug_daily_dose  dgth_drug_daily_dose,drug_para_administration_ft  dgth_drug_para_administration_ft,drug_therapy_on_going  dgth_drug_therapy_on_going,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(drug_therapy_on_going AS VARCHAR(100)) )dgth_drug_therapy_on_going_de_ml , drug_theropy_batch_size  dgth_drug_theropy_batch_size,drug_theropy_package_size  dgth_drug_theropy_package_size,drug_therpy_duration  dgth_drug_therpy_duration,drugadmin_route_ft  dgth_drugadmin_route_ft,drugadminduration  dgth_drugadminduration,drugadmindurationunit  dgth_drugadmindurationunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(drugadmindurationunit AS VARCHAR(100)) )dgth_drugadmindurationunit_de_ml , drugadministrationroute  dgth_drugadministrationroute,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1020' AND CD=CAST(drugadministrationroute AS VARCHAR(100)) )dgth_drugadministrationroute_de_ml , drugadministrationroute_id  dgth_drugadministrationroute_id,drugadministrationroute_id_ver  dgth_drugadministrationroute_id_ver,drugadministrationroute_lang  dgth_drugadministrationroute_lang,drugadministrationroute_nf  dgth_drugadministrationroute_nf,drugadministrationroute_ocr  dgth_drugadministrationroute_ocr,drugadministrationroute_sf  dgth_drugadministrationroute_sf,drugcumulativedosagenumb  dgth_drugcumulativedosagenumb,drugcumulativedosagenumb_lang  dgth_drugcumulativedosagenumb_lang,drugcumulativedosageunit  dgth_drugcumulativedosageunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1018' AND CD=CAST(drugcumulativedosageunit AS VARCHAR(100)) )dgth_drugcumulativedosageunit_de_ml , drugcumulativedosageunit_lang  dgth_drugcumulativedosageunit_lang,drugdosageform  dgth_drugdosageform,drugdosageform_as_coded  dgth_drugdosageform_as_coded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='805' AND CD=CAST(drugdosageform AS VARCHAR(100)) )dgth_drugdosageform_de_ml , drugdosageform_lang  dgth_drugdosageform_lang,drugdosageform_nf  dgth_drugdosageform_nf,drugdosageform_sf  dgth_drugdosageform_sf,drugdosageform_termid  dgth_drugdosageform_termid,drugdosageform_termid_version  dgth_drugdosageform_termid_version,drugdosageform_text  dgth_drugdosageform_text,drugdosagetext  dgth_drugdosagetext,drugdosagetext_lang  dgth_drugdosagetext_lang,drugenddate  dgth_drugenddate,drugenddate_nf  dgth_drugenddate_nf,drugenddate_tz  dgth_drugenddate_tz,drugenddatefmt  dgth_drugenddatefmt,drugintdosageunitdef  dgth_drugintdosageunitdef,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1014' AND CD=CAST(drugintdosageunitdef AS VARCHAR(100)) )dgth_drugintdosageunitdef_de_ml , drugintdosageunitdef_lang  dgth_drugintdosageunitdef_lang,drugintdosageunitnumb  dgth_drugintdosageunitnumb,drugintdosageunitnumb_lang  dgth_drugintdosageunitnumb_lang,druglastperiod  dgth_druglastperiod,druglastperiod_lang  dgth_druglastperiod_lang,druglastperiodunit  dgth_druglastperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='17' AND CD=CAST(druglastperiodunit AS VARCHAR(100)) )dgth_druglastperiodunit_de_ml , drugparadministration  dgth_drugparadministration,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1020' AND CD=CAST(drugparadministration AS VARCHAR(100)) )dgth_drugparadministration_de_ml , drugparadministration_id  dgth_drugparadministration_id,drugparadministration_id_ver  dgth_drugparadministration_id_ver,drugparadministration_lang  dgth_drugparadministration_lang,drugparadministration_nf  dgth_drugparadministration_nf,drugparadministration_sf  dgth_drugparadministration_sf,drugseparatedosagenumb  dgth_drugseparatedosagenumb,drugseparatedosagenumb_lang  dgth_drugseparatedosagenumb_lang,drugseparatedosagenumb_ocr  dgth_drugseparatedosagenumb_ocr,drugstartdate  dgth_drugstartdate,drugstartdate_nf  dgth_drugstartdate_nf,drugstartdate_tz  dgth_drugstartdate_tz,drugstartdatefmt  dgth_drugstartdatefmt,drugstartperiod  dgth_drugstartperiod,drugstartperiod_lang  dgth_drugstartperiod_lang,drugstartperiodunit  dgth_drugstartperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='17' AND CD=CAST(drugstartperiodunit AS VARCHAR(100)) )dgth_drugstartperiodunit_de_ml , drugstructuredosagenumb  dgth_drugstructuredosagenumb,drugstructuredosagenumb_lang  dgth_drugstructuredosagenumb_lang,drugstructuredosagenumb_ocr  dgth_drugstructuredosagenumb_ocr,drugstructuredosageunit  dgth_drugstructuredosageunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1018' AND CD=CAST(drugstructuredosageunit AS VARCHAR(100)) )dgth_drugstructuredosageunit_de_ml , drugstructuredosageunit_lang  dgth_drugstructuredosageunit_lang,drugstructuredquantitynumb  dgth_drugstructuredquantitynumb,drugstructuredquantityunit  dgth_drugstructuredquantityunit,drugtreatdurationunit  dgth_drugtreatdurationunit,duration_text  dgth_duration_text,entity_updated  dgth_entity_updated,expiration_date  dgth_expiration_date,expiration_date_fmt  dgth_expiration_date_fmt,fk_ad_rec_id  dgth_fk_ad_rec_id,fk_adtbd_rec_id  dgth_fk_adtbd_rec_id,form_strength  dgth_form_strength,form_strength_unit  dgth_form_strength_unit,form_strength_unit_value  dgth_form_strength_unit_value,frequency  dgth_frequency,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9031' AND CD=CAST(frequency AS VARCHAR(100)) )dgth_frequency_de_ml , gestationperiod  dgth_gestationperiod,gestationperiodunit  dgth_gestationperiodunit,inq_rec_id  dgth_inq_rec_id,is_sample_available  dgth_is_sample_available,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(is_sample_available AS VARCHAR(100)) )dgth_is_sample_available_de_ml , lot_number  dgth_lot_number,lot_number_nf  dgth_lot_number_nf,lotnumberstore  dgth_lotnumberstore,manual_therapy_duration_type  dgth_manual_therapy_duration_type,no_of_complaints_on_the_batch  dgth_no_of_complaints_on_the_batch,notifictaion  dgth_notifictaion,overdose_result_sae  dgth_overdose_result_sae,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(overdose_result_sae AS VARCHAR(100)) )dgth_overdose_result_sae_de_ml , par_route_admin_termid  dgth_par_route_admin_termid,par_route_admin_termid_ver  dgth_par_route_admin_termid_ver,pharmaceutical_termid  dgth_pharmaceutical_termid,pharmaceutical_termid_ver  dgth_pharmaceutical_termid_ver,product_code  dgth_product_code,product_type  dgth_product_type,quantity_mode  dgth_quantity_mode,quantity_ocr  dgth_quantity_ocr,record_id  dgth_record_id,route_admin_termid  dgth_route_admin_termid,route_admin_termid_ver  dgth_route_admin_termid_ver,spr_id  dgth_spr_id,theraphy_site  dgth_theraphy_site,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9992' AND CD=CAST(theraphy_site AS VARCHAR(100)) )dgth_theraphy_site_de_ml , therapy_continued  dgth_therapy_continued,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(therapy_continued AS VARCHAR(100)) )dgth_therapy_continued_de_ml , therapy_dose_reduced_date  dgth_therapy_dose_reduced_date,therapy_end_date_text  dgth_therapy_end_date_text,therapy_lang  dgth_therapy_lang,therapy_start_date_text  dgth_therapy_start_date_text,try_to_number(total_dose,38)  dgth_total_dose,total_dose  dgth_total_dose_1 ,total_dose_manual  dgth_total_dose_manual,total_dose_unit  dgth_total_dose_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1018' AND CD=CAST(total_dose_unit AS VARCHAR(100)) )dgth_total_dose_unit_de_ml , txt_frequency  dgth_txt_frequency,txt_start_ther_date  dgth_txt_start_ther_date,user_created  dgth_user_created,user_modified  dgth_user_modified,version  dgth_version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_ADTBD_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_therapy_SUBSET.dgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_drug_therapy_vaccine_SUBSET.dgthvac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_drug_therapy_SUBSET.dgth_USER_MODIFIED USER_MODIFIED,lsmv_drug_therapy_SUBSET.dgth_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_therapy_SUBSET.dgth_USER_CREATED CREATED_BY,lsmv_drug_therapy_SUBSET.dgth_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_therapy_SUBSET.dgth_version  ,lsmv_drug_therapy_SUBSET.dgth_user_modified  ,lsmv_drug_therapy_SUBSET.dgth_user_created  ,lsmv_drug_therapy_SUBSET.dgth_txt_start_ther_date  ,lsmv_drug_therapy_SUBSET.dgth_txt_frequency  ,lsmv_drug_therapy_SUBSET.dgth_total_dose_unit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_total_dose_unit  ,lsmv_drug_therapy_SUBSET.dgth_total_dose_manual  ,lsmv_drug_therapy_SUBSET.dgth_total_dose ,lsmv_drug_therapy_SUBSET.dgth_total_dose_1  ,lsmv_drug_therapy_SUBSET.dgth_therapy_start_date_text  ,lsmv_drug_therapy_SUBSET.dgth_therapy_lang  ,lsmv_drug_therapy_SUBSET.dgth_therapy_end_date_text  ,lsmv_drug_therapy_SUBSET.dgth_therapy_dose_reduced_date  ,lsmv_drug_therapy_SUBSET.dgth_therapy_continued_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_therapy_continued  ,lsmv_drug_therapy_SUBSET.dgth_theraphy_site_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_theraphy_site  ,lsmv_drug_therapy_SUBSET.dgth_spr_id  ,lsmv_drug_therapy_SUBSET.dgth_route_admin_termid_ver  ,lsmv_drug_therapy_SUBSET.dgth_route_admin_termid  ,lsmv_drug_therapy_SUBSET.dgth_record_id  ,lsmv_drug_therapy_SUBSET.dgth_quantity_ocr  ,lsmv_drug_therapy_SUBSET.dgth_quantity_mode  ,lsmv_drug_therapy_SUBSET.dgth_product_type  ,lsmv_drug_therapy_SUBSET.dgth_product_code  ,lsmv_drug_therapy_SUBSET.dgth_pharmaceutical_termid_ver  ,lsmv_drug_therapy_SUBSET.dgth_pharmaceutical_termid  ,lsmv_drug_therapy_SUBSET.dgth_par_route_admin_termid_ver  ,lsmv_drug_therapy_SUBSET.dgth_par_route_admin_termid  ,lsmv_drug_therapy_SUBSET.dgth_overdose_result_sae_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_overdose_result_sae  ,lsmv_drug_therapy_SUBSET.dgth_notifictaion  ,lsmv_drug_therapy_SUBSET.dgth_no_of_complaints_on_the_batch  ,lsmv_drug_therapy_SUBSET.dgth_manual_therapy_duration_type  ,lsmv_drug_therapy_SUBSET.dgth_lotnumberstore  ,lsmv_drug_therapy_SUBSET.dgth_lot_number_nf  ,lsmv_drug_therapy_SUBSET.dgth_lot_number  ,lsmv_drug_therapy_SUBSET.dgth_is_sample_available_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_is_sample_available  ,lsmv_drug_therapy_SUBSET.dgth_inq_rec_id  ,lsmv_drug_therapy_SUBSET.dgth_gestationperiodunit  ,lsmv_drug_therapy_SUBSET.dgth_gestationperiod  ,lsmv_drug_therapy_SUBSET.dgth_frequency_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_frequency  ,lsmv_drug_therapy_SUBSET.dgth_form_strength_unit_value  ,lsmv_drug_therapy_SUBSET.dgth_form_strength_unit  ,lsmv_drug_therapy_SUBSET.dgth_form_strength  ,lsmv_drug_therapy_SUBSET.dgth_fk_adtbd_rec_id  ,lsmv_drug_therapy_SUBSET.dgth_fk_ad_rec_id  ,lsmv_drug_therapy_SUBSET.dgth_expiration_date_fmt  ,lsmv_drug_therapy_SUBSET.dgth_expiration_date  ,lsmv_drug_therapy_SUBSET.dgth_entity_updated  ,lsmv_drug_therapy_SUBSET.dgth_duration_text  ,lsmv_drug_therapy_SUBSET.dgth_drugtreatdurationunit  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredquantityunit  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredquantitynumb  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosageunit_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosageunit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosageunit  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosagenumb_ocr  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosagenumb_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugstructuredosagenumb  ,lsmv_drug_therapy_SUBSET.dgth_drugstartperiodunit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugstartperiodunit  ,lsmv_drug_therapy_SUBSET.dgth_drugstartperiod_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugstartperiod  ,lsmv_drug_therapy_SUBSET.dgth_drugstartdatefmt  ,lsmv_drug_therapy_SUBSET.dgth_drugstartdate_tz  ,lsmv_drug_therapy_SUBSET.dgth_drugstartdate_nf  ,lsmv_drug_therapy_SUBSET.dgth_drugstartdate  ,lsmv_drug_therapy_SUBSET.dgth_drugseparatedosagenumb_ocr  ,lsmv_drug_therapy_SUBSET.dgth_drugseparatedosagenumb_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugseparatedosagenumb  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_sf  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_nf  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_id_ver  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_id  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugparadministration  ,lsmv_drug_therapy_SUBSET.dgth_druglastperiodunit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_druglastperiodunit  ,lsmv_drug_therapy_SUBSET.dgth_druglastperiod_lang  ,lsmv_drug_therapy_SUBSET.dgth_druglastperiod  ,lsmv_drug_therapy_SUBSET.dgth_drugintdosageunitnumb_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugintdosageunitnumb  ,lsmv_drug_therapy_SUBSET.dgth_drugintdosageunitdef_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugintdosageunitdef_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugintdosageunitdef  ,lsmv_drug_therapy_SUBSET.dgth_drugenddatefmt  ,lsmv_drug_therapy_SUBSET.dgth_drugenddate_tz  ,lsmv_drug_therapy_SUBSET.dgth_drugenddate_nf  ,lsmv_drug_therapy_SUBSET.dgth_drugenddate  ,lsmv_drug_therapy_SUBSET.dgth_drugdosagetext_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugdosagetext  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_text  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_termid_version  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_termid  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_sf  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_nf  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform_as_coded  ,lsmv_drug_therapy_SUBSET.dgth_drugdosageform  ,lsmv_drug_therapy_SUBSET.dgth_drugcumulativedosageunit_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugcumulativedosageunit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugcumulativedosageunit  ,lsmv_drug_therapy_SUBSET.dgth_drugcumulativedosagenumb_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugcumulativedosagenumb  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_sf  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_ocr  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_nf  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_lang  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_id_ver  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_id  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugadministrationroute  ,lsmv_drug_therapy_SUBSET.dgth_drugadmindurationunit_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drugadmindurationunit  ,lsmv_drug_therapy_SUBSET.dgth_drugadminduration  ,lsmv_drug_therapy_SUBSET.dgth_drugadmin_route_ft  ,lsmv_drug_therapy_SUBSET.dgth_drug_therpy_duration  ,lsmv_drug_therapy_SUBSET.dgth_drug_theropy_package_size  ,lsmv_drug_therapy_SUBSET.dgth_drug_theropy_batch_size  ,lsmv_drug_therapy_SUBSET.dgth_drug_therapy_on_going_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_drug_therapy_on_going  ,lsmv_drug_therapy_SUBSET.dgth_drug_para_administration_ft  ,lsmv_drug_therapy_SUBSET.dgth_drug_daily_dose  ,lsmv_drug_therapy_SUBSET.dgth_dosposition  ,lsmv_drug_therapy_SUBSET.dgth_dose_prior_pregnancy_unit  ,lsmv_drug_therapy_SUBSET.dgth_dose_prior_pregnancy_numb  ,lsmv_drug_therapy_SUBSET.dgth_dose_num_series  ,lsmv_drug_therapy_SUBSET.dgth_dates_of_use_ocr  ,lsmv_drug_therapy_SUBSET.dgth_date_overdose_occurred  ,lsmv_drug_therapy_SUBSET.dgth_date_modified  ,lsmv_drug_therapy_SUBSET.dgth_date_created  ,lsmv_drug_therapy_SUBSET.dgth_daily_dose_unit  ,lsmv_drug_therapy_SUBSET.dgth_daily_dose_manual  ,lsmv_drug_therapy_SUBSET.dgth_daily_dose  ,lsmv_drug_therapy_SUBSET.dgth_comp_rec_id  ,lsmv_drug_therapy_SUBSET.dgth_collection_date  ,lsmv_drug_therapy_SUBSET.dgth_ari_rec_id  ,lsmv_drug_therapy_SUBSET.dgth_anatomical_approach_site_nf  ,lsmv_drug_therapy_SUBSET.dgth_anatomical_approach_site_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_anatomical_approach_site  ,lsmv_drug_therapy_SUBSET.dgth_actiondrug_de_ml  ,lsmv_drug_therapy_SUBSET.dgth_actiondrug  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccine_name_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccine_name  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_type_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_type_de_ml  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_type  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_telephone_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_telephone  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_state_sf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_state_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_state_de_ml  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_state  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_postal_code_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_postal_code  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_fax_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_fax  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_country_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_country  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_city_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_city  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_address2_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_address2  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_address1_nf  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_vaccination_address1  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_user_modified  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_user_created  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_spr_id  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_record_id  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_fk_adt_rec_id  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_date_modified  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_date_created  ,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_ari_rec_id  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_user_modified  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_user_created  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_title_de_ml  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_title  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_telephone  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_spr_id  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_record_id  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_middle_name  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_last_name  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_first_name  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_email_id  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_date_modified  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_date_created  ,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_ari_rec_id ,CONCAT( NVL(lsmv_drug_therapy_SUBSET.dgth_RECORD_ID,-1),'||',NVL(lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_RECORD_ID,-1),'||',NVL(lsmv_drug_therapy_vaccine_SUBSET.dgthvac_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_therapy_SUBSET  LEFT JOIN lsmv_drug_therapy_vaccine_SUBSET ON lsmv_drug_therapy_SUBSET.dgth_record_id=lsmv_drug_therapy_vaccine_SUBSET.dgthvac_fk_adt_rec_id
                         LEFT JOIN LSMV_DRUG_THERAPY_BEST_DOCTOR_SUBSET ON lsmv_drug_therapy_SUBSET.dgth_FK_ADTBD_REC_ID=LSMV_DRUG_THERAPY_BEST_DOCTOR_SUBSET.dgthbstdoc_record_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_drug_therapy_SUBSET.dgth_RECORD_ID,lsmv_drug_therapy_best_doctor_SUBSET.dgthbstdoc_RECORD_ID,lsmv_drug_therapy_vaccine_SUBSET.dgthvac_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP)
where target_table_name='LS_DB_DRUG_THERAPY'

; 


        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_drug_therapy' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'total_dose' ,
dgth_total_dose_1,
dgth_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on total_dose'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP
WHERE (dgth_total_dose is null and dgth_total_dose_1 is not null)
and dgth_ARI_REC_ID is not null 
and CASE_NO is not null;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP
WHERE (dgth_total_dose is null and dgth_total_dose_1 is not null) 
and dgth_ARI_REC_ID is not null 
and CASE_NO is not null;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY   
SET LS_DB_DRUG_THERAPY.dgth_version = LS_DB_DRUG_THERAPY_TMP.dgth_version,LS_DB_DRUG_THERAPY.dgth_user_modified = LS_DB_DRUG_THERAPY_TMP.dgth_user_modified,LS_DB_DRUG_THERAPY.dgth_user_created = LS_DB_DRUG_THERAPY_TMP.dgth_user_created,LS_DB_DRUG_THERAPY.dgth_txt_start_ther_date = LS_DB_DRUG_THERAPY_TMP.dgth_txt_start_ther_date,LS_DB_DRUG_THERAPY.dgth_txt_frequency = LS_DB_DRUG_THERAPY_TMP.dgth_txt_frequency,LS_DB_DRUG_THERAPY.dgth_total_dose_unit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_total_dose_unit_de_ml,LS_DB_DRUG_THERAPY.dgth_total_dose_unit = LS_DB_DRUG_THERAPY_TMP.dgth_total_dose_unit,LS_DB_DRUG_THERAPY.dgth_total_dose_manual = LS_DB_DRUG_THERAPY_TMP.dgth_total_dose_manual,LS_DB_DRUG_THERAPY.dgth_total_dose = LS_DB_DRUG_THERAPY_TMP.dgth_total_dose,LS_DB_DRUG_THERAPY.dgth_therapy_start_date_text = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_start_date_text,LS_DB_DRUG_THERAPY.dgth_therapy_lang = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_lang,LS_DB_DRUG_THERAPY.dgth_therapy_end_date_text = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_end_date_text,LS_DB_DRUG_THERAPY.dgth_therapy_dose_reduced_date = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_dose_reduced_date,LS_DB_DRUG_THERAPY.dgth_therapy_continued_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_continued_de_ml,LS_DB_DRUG_THERAPY.dgth_therapy_continued = LS_DB_DRUG_THERAPY_TMP.dgth_therapy_continued,LS_DB_DRUG_THERAPY.dgth_theraphy_site_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_theraphy_site_de_ml,LS_DB_DRUG_THERAPY.dgth_theraphy_site = LS_DB_DRUG_THERAPY_TMP.dgth_theraphy_site,LS_DB_DRUG_THERAPY.dgth_spr_id = LS_DB_DRUG_THERAPY_TMP.dgth_spr_id,LS_DB_DRUG_THERAPY.dgth_route_admin_termid_ver = LS_DB_DRUG_THERAPY_TMP.dgth_route_admin_termid_ver,LS_DB_DRUG_THERAPY.dgth_route_admin_termid = LS_DB_DRUG_THERAPY_TMP.dgth_route_admin_termid,LS_DB_DRUG_THERAPY.dgth_record_id = LS_DB_DRUG_THERAPY_TMP.dgth_record_id,LS_DB_DRUG_THERAPY.dgth_quantity_ocr = LS_DB_DRUG_THERAPY_TMP.dgth_quantity_ocr,LS_DB_DRUG_THERAPY.dgth_quantity_mode = LS_DB_DRUG_THERAPY_TMP.dgth_quantity_mode,LS_DB_DRUG_THERAPY.dgth_product_type = LS_DB_DRUG_THERAPY_TMP.dgth_product_type,LS_DB_DRUG_THERAPY.dgth_product_code = LS_DB_DRUG_THERAPY_TMP.dgth_product_code,LS_DB_DRUG_THERAPY.dgth_pharmaceutical_termid_ver = LS_DB_DRUG_THERAPY_TMP.dgth_pharmaceutical_termid_ver,LS_DB_DRUG_THERAPY.dgth_pharmaceutical_termid = LS_DB_DRUG_THERAPY_TMP.dgth_pharmaceutical_termid,LS_DB_DRUG_THERAPY.dgth_par_route_admin_termid_ver = LS_DB_DRUG_THERAPY_TMP.dgth_par_route_admin_termid_ver,LS_DB_DRUG_THERAPY.dgth_par_route_admin_termid = LS_DB_DRUG_THERAPY_TMP.dgth_par_route_admin_termid,LS_DB_DRUG_THERAPY.dgth_overdose_result_sae_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_overdose_result_sae_de_ml,LS_DB_DRUG_THERAPY.dgth_overdose_result_sae = LS_DB_DRUG_THERAPY_TMP.dgth_overdose_result_sae,LS_DB_DRUG_THERAPY.dgth_notifictaion = LS_DB_DRUG_THERAPY_TMP.dgth_notifictaion,LS_DB_DRUG_THERAPY.dgth_no_of_complaints_on_the_batch = LS_DB_DRUG_THERAPY_TMP.dgth_no_of_complaints_on_the_batch,LS_DB_DRUG_THERAPY.dgth_manual_therapy_duration_type = LS_DB_DRUG_THERAPY_TMP.dgth_manual_therapy_duration_type,LS_DB_DRUG_THERAPY.dgth_lotnumberstore = LS_DB_DRUG_THERAPY_TMP.dgth_lotnumberstore,LS_DB_DRUG_THERAPY.dgth_lot_number_nf = LS_DB_DRUG_THERAPY_TMP.dgth_lot_number_nf,LS_DB_DRUG_THERAPY.dgth_lot_number = LS_DB_DRUG_THERAPY_TMP.dgth_lot_number,LS_DB_DRUG_THERAPY.dgth_is_sample_available_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_is_sample_available_de_ml,LS_DB_DRUG_THERAPY.dgth_is_sample_available = LS_DB_DRUG_THERAPY_TMP.dgth_is_sample_available,LS_DB_DRUG_THERAPY.dgth_inq_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_inq_rec_id,LS_DB_DRUG_THERAPY.dgth_gestationperiodunit = LS_DB_DRUG_THERAPY_TMP.dgth_gestationperiodunit,LS_DB_DRUG_THERAPY.dgth_gestationperiod = LS_DB_DRUG_THERAPY_TMP.dgth_gestationperiod,LS_DB_DRUG_THERAPY.dgth_frequency_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_frequency_de_ml,LS_DB_DRUG_THERAPY.dgth_frequency = LS_DB_DRUG_THERAPY_TMP.dgth_frequency,LS_DB_DRUG_THERAPY.dgth_form_strength_unit_value = LS_DB_DRUG_THERAPY_TMP.dgth_form_strength_unit_value,LS_DB_DRUG_THERAPY.dgth_form_strength_unit = LS_DB_DRUG_THERAPY_TMP.dgth_form_strength_unit,LS_DB_DRUG_THERAPY.dgth_form_strength = LS_DB_DRUG_THERAPY_TMP.dgth_form_strength,LS_DB_DRUG_THERAPY.dgth_fk_adtbd_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_fk_adtbd_rec_id,LS_DB_DRUG_THERAPY.dgth_fk_ad_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_fk_ad_rec_id,LS_DB_DRUG_THERAPY.dgth_expiration_date_fmt = LS_DB_DRUG_THERAPY_TMP.dgth_expiration_date_fmt,LS_DB_DRUG_THERAPY.dgth_expiration_date = LS_DB_DRUG_THERAPY_TMP.dgth_expiration_date,LS_DB_DRUG_THERAPY.dgth_entity_updated = LS_DB_DRUG_THERAPY_TMP.dgth_entity_updated,LS_DB_DRUG_THERAPY.dgth_duration_text = LS_DB_DRUG_THERAPY_TMP.dgth_duration_text,LS_DB_DRUG_THERAPY.dgth_drugtreatdurationunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugtreatdurationunit,LS_DB_DRUG_THERAPY.dgth_drugstructuredquantityunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredquantityunit,LS_DB_DRUG_THERAPY.dgth_drugstructuredquantitynumb = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredquantitynumb,LS_DB_DRUG_THERAPY.dgth_drugstructuredosageunit_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosageunit_lang,LS_DB_DRUG_THERAPY.dgth_drugstructuredosageunit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosageunit_de_ml,LS_DB_DRUG_THERAPY.dgth_drugstructuredosageunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosageunit,LS_DB_DRUG_THERAPY.dgth_drugstructuredosagenumb_ocr = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosagenumb_ocr,LS_DB_DRUG_THERAPY.dgth_drugstructuredosagenumb_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosagenumb_lang,LS_DB_DRUG_THERAPY.dgth_drugstructuredosagenumb = LS_DB_DRUG_THERAPY_TMP.dgth_drugstructuredosagenumb,LS_DB_DRUG_THERAPY.dgth_drugstartperiodunit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartperiodunit_de_ml,LS_DB_DRUG_THERAPY.dgth_drugstartperiodunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartperiodunit,LS_DB_DRUG_THERAPY.dgth_drugstartperiod_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartperiod_lang,LS_DB_DRUG_THERAPY.dgth_drugstartperiod = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartperiod,LS_DB_DRUG_THERAPY.dgth_drugstartdatefmt = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartdatefmt,LS_DB_DRUG_THERAPY.dgth_drugstartdate_tz = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartdate_tz,LS_DB_DRUG_THERAPY.dgth_drugstartdate_nf = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartdate_nf,LS_DB_DRUG_THERAPY.dgth_drugstartdate = LS_DB_DRUG_THERAPY_TMP.dgth_drugstartdate,LS_DB_DRUG_THERAPY.dgth_drugseparatedosagenumb_ocr = LS_DB_DRUG_THERAPY_TMP.dgth_drugseparatedosagenumb_ocr,LS_DB_DRUG_THERAPY.dgth_drugseparatedosagenumb_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugseparatedosagenumb_lang,LS_DB_DRUG_THERAPY.dgth_drugseparatedosagenumb = LS_DB_DRUG_THERAPY_TMP.dgth_drugseparatedosagenumb,LS_DB_DRUG_THERAPY.dgth_drugparadministration_sf = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_sf,LS_DB_DRUG_THERAPY.dgth_drugparadministration_nf = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_nf,LS_DB_DRUG_THERAPY.dgth_drugparadministration_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_lang,LS_DB_DRUG_THERAPY.dgth_drugparadministration_id_ver = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_id_ver,LS_DB_DRUG_THERAPY.dgth_drugparadministration_id = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_id,LS_DB_DRUG_THERAPY.dgth_drugparadministration_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration_de_ml,LS_DB_DRUG_THERAPY.dgth_drugparadministration = LS_DB_DRUG_THERAPY_TMP.dgth_drugparadministration,LS_DB_DRUG_THERAPY.dgth_druglastperiodunit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_druglastperiodunit_de_ml,LS_DB_DRUG_THERAPY.dgth_druglastperiodunit = LS_DB_DRUG_THERAPY_TMP.dgth_druglastperiodunit,LS_DB_DRUG_THERAPY.dgth_druglastperiod_lang = LS_DB_DRUG_THERAPY_TMP.dgth_druglastperiod_lang,LS_DB_DRUG_THERAPY.dgth_druglastperiod = LS_DB_DRUG_THERAPY_TMP.dgth_druglastperiod,LS_DB_DRUG_THERAPY.dgth_drugintdosageunitnumb_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugintdosageunitnumb_lang,LS_DB_DRUG_THERAPY.dgth_drugintdosageunitnumb = LS_DB_DRUG_THERAPY_TMP.dgth_drugintdosageunitnumb,LS_DB_DRUG_THERAPY.dgth_drugintdosageunitdef_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugintdosageunitdef_lang,LS_DB_DRUG_THERAPY.dgth_drugintdosageunitdef_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugintdosageunitdef_de_ml,LS_DB_DRUG_THERAPY.dgth_drugintdosageunitdef = LS_DB_DRUG_THERAPY_TMP.dgth_drugintdosageunitdef,LS_DB_DRUG_THERAPY.dgth_drugenddatefmt = LS_DB_DRUG_THERAPY_TMP.dgth_drugenddatefmt,LS_DB_DRUG_THERAPY.dgth_drugenddate_tz = LS_DB_DRUG_THERAPY_TMP.dgth_drugenddate_tz,LS_DB_DRUG_THERAPY.dgth_drugenddate_nf = LS_DB_DRUG_THERAPY_TMP.dgth_drugenddate_nf,LS_DB_DRUG_THERAPY.dgth_drugenddate = LS_DB_DRUG_THERAPY_TMP.dgth_drugenddate,LS_DB_DRUG_THERAPY.dgth_drugdosagetext_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosagetext_lang,LS_DB_DRUG_THERAPY.dgth_drugdosagetext = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosagetext,LS_DB_DRUG_THERAPY.dgth_drugdosageform_text = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_text,LS_DB_DRUG_THERAPY.dgth_drugdosageform_termid_version = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_termid_version,LS_DB_DRUG_THERAPY.dgth_drugdosageform_termid = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_termid,LS_DB_DRUG_THERAPY.dgth_drugdosageform_sf = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_sf,LS_DB_DRUG_THERAPY.dgth_drugdosageform_nf = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_nf,LS_DB_DRUG_THERAPY.dgth_drugdosageform_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_lang,LS_DB_DRUG_THERAPY.dgth_drugdosageform_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_de_ml,LS_DB_DRUG_THERAPY.dgth_drugdosageform_as_coded = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform_as_coded,LS_DB_DRUG_THERAPY.dgth_drugdosageform = LS_DB_DRUG_THERAPY_TMP.dgth_drugdosageform,LS_DB_DRUG_THERAPY.dgth_drugcumulativedosageunit_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugcumulativedosageunit_lang,LS_DB_DRUG_THERAPY.dgth_drugcumulativedosageunit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugcumulativedosageunit_de_ml,LS_DB_DRUG_THERAPY.dgth_drugcumulativedosageunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugcumulativedosageunit,LS_DB_DRUG_THERAPY.dgth_drugcumulativedosagenumb_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugcumulativedosagenumb_lang,LS_DB_DRUG_THERAPY.dgth_drugcumulativedosagenumb = LS_DB_DRUG_THERAPY_TMP.dgth_drugcumulativedosagenumb,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_sf = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_sf,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_ocr = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_ocr,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_nf = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_nf,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_lang = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_lang,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_id_ver = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_id_ver,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_id = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_id,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute_de_ml,LS_DB_DRUG_THERAPY.dgth_drugadministrationroute = LS_DB_DRUG_THERAPY_TMP.dgth_drugadministrationroute,LS_DB_DRUG_THERAPY.dgth_drugadmindurationunit_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drugadmindurationunit_de_ml,LS_DB_DRUG_THERAPY.dgth_drugadmindurationunit = LS_DB_DRUG_THERAPY_TMP.dgth_drugadmindurationunit,LS_DB_DRUG_THERAPY.dgth_drugadminduration = LS_DB_DRUG_THERAPY_TMP.dgth_drugadminduration,LS_DB_DRUG_THERAPY.dgth_drugadmin_route_ft = LS_DB_DRUG_THERAPY_TMP.dgth_drugadmin_route_ft,LS_DB_DRUG_THERAPY.dgth_drug_therpy_duration = LS_DB_DRUG_THERAPY_TMP.dgth_drug_therpy_duration,LS_DB_DRUG_THERAPY.dgth_drug_theropy_package_size = LS_DB_DRUG_THERAPY_TMP.dgth_drug_theropy_package_size,LS_DB_DRUG_THERAPY.dgth_drug_theropy_batch_size = LS_DB_DRUG_THERAPY_TMP.dgth_drug_theropy_batch_size,LS_DB_DRUG_THERAPY.dgth_drug_therapy_on_going_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_drug_therapy_on_going_de_ml,LS_DB_DRUG_THERAPY.dgth_drug_therapy_on_going = LS_DB_DRUG_THERAPY_TMP.dgth_drug_therapy_on_going,LS_DB_DRUG_THERAPY.dgth_drug_para_administration_ft = LS_DB_DRUG_THERAPY_TMP.dgth_drug_para_administration_ft,LS_DB_DRUG_THERAPY.dgth_drug_daily_dose = LS_DB_DRUG_THERAPY_TMP.dgth_drug_daily_dose,LS_DB_DRUG_THERAPY.dgth_dosposition = LS_DB_DRUG_THERAPY_TMP.dgth_dosposition,LS_DB_DRUG_THERAPY.dgth_dose_prior_pregnancy_unit = LS_DB_DRUG_THERAPY_TMP.dgth_dose_prior_pregnancy_unit,LS_DB_DRUG_THERAPY.dgth_dose_prior_pregnancy_numb = LS_DB_DRUG_THERAPY_TMP.dgth_dose_prior_pregnancy_numb,LS_DB_DRUG_THERAPY.dgth_dose_num_series = LS_DB_DRUG_THERAPY_TMP.dgth_dose_num_series,LS_DB_DRUG_THERAPY.dgth_dates_of_use_ocr = LS_DB_DRUG_THERAPY_TMP.dgth_dates_of_use_ocr,LS_DB_DRUG_THERAPY.dgth_date_overdose_occurred = LS_DB_DRUG_THERAPY_TMP.dgth_date_overdose_occurred,LS_DB_DRUG_THERAPY.dgth_date_modified = LS_DB_DRUG_THERAPY_TMP.dgth_date_modified,LS_DB_DRUG_THERAPY.dgth_date_created = LS_DB_DRUG_THERAPY_TMP.dgth_date_created,LS_DB_DRUG_THERAPY.dgth_daily_dose_unit = LS_DB_DRUG_THERAPY_TMP.dgth_daily_dose_unit,LS_DB_DRUG_THERAPY.dgth_daily_dose_manual = LS_DB_DRUG_THERAPY_TMP.dgth_daily_dose_manual,LS_DB_DRUG_THERAPY.dgth_daily_dose = LS_DB_DRUG_THERAPY_TMP.dgth_daily_dose,LS_DB_DRUG_THERAPY.dgth_comp_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_comp_rec_id,LS_DB_DRUG_THERAPY.dgth_collection_date = LS_DB_DRUG_THERAPY_TMP.dgth_collection_date,LS_DB_DRUG_THERAPY.dgth_ari_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_ari_rec_id,LS_DB_DRUG_THERAPY.dgth_anatomical_approach_site_nf = LS_DB_DRUG_THERAPY_TMP.dgth_anatomical_approach_site_nf,LS_DB_DRUG_THERAPY.dgth_anatomical_approach_site_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_anatomical_approach_site_de_ml,LS_DB_DRUG_THERAPY.dgth_anatomical_approach_site = LS_DB_DRUG_THERAPY_TMP.dgth_anatomical_approach_site,LS_DB_DRUG_THERAPY.dgth_actiondrug_de_ml = LS_DB_DRUG_THERAPY_TMP.dgth_actiondrug_de_ml,LS_DB_DRUG_THERAPY.dgth_actiondrug = LS_DB_DRUG_THERAPY_TMP.dgth_actiondrug,LS_DB_DRUG_THERAPY.dgthvac_vaccine_name_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccine_name_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccine_name = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccine_name,LS_DB_DRUG_THERAPY.dgthvac_vaccination_type_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_type_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_type_de_ml = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_type_de_ml,LS_DB_DRUG_THERAPY.dgthvac_vaccination_type = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_type,LS_DB_DRUG_THERAPY.dgthvac_vaccination_telephone_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_telephone_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_telephone = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_telephone,LS_DB_DRUG_THERAPY.dgthvac_vaccination_state_sf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_state_sf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_state_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_state_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_state_de_ml = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_state_de_ml,LS_DB_DRUG_THERAPY.dgthvac_vaccination_state = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_state,LS_DB_DRUG_THERAPY.dgthvac_vaccination_postal_code_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_postal_code_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_postal_code = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_postal_code,LS_DB_DRUG_THERAPY.dgthvac_vaccination_fax_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_fax_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_fax = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_fax,LS_DB_DRUG_THERAPY.dgthvac_vaccination_country_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_country_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_country = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_country,LS_DB_DRUG_THERAPY.dgthvac_vaccination_city_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_city_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_city = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_city,LS_DB_DRUG_THERAPY.dgthvac_vaccination_address2_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_address2_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_address2 = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_address2,LS_DB_DRUG_THERAPY.dgthvac_vaccination_address1_nf = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_address1_nf,LS_DB_DRUG_THERAPY.dgthvac_vaccination_address1 = LS_DB_DRUG_THERAPY_TMP.dgthvac_vaccination_address1,LS_DB_DRUG_THERAPY.dgthvac_user_modified = LS_DB_DRUG_THERAPY_TMP.dgthvac_user_modified,LS_DB_DRUG_THERAPY.dgthvac_user_created = LS_DB_DRUG_THERAPY_TMP.dgthvac_user_created,LS_DB_DRUG_THERAPY.dgthvac_spr_id = LS_DB_DRUG_THERAPY_TMP.dgthvac_spr_id,LS_DB_DRUG_THERAPY.dgthvac_record_id = LS_DB_DRUG_THERAPY_TMP.dgthvac_record_id,LS_DB_DRUG_THERAPY.dgthvac_fk_adt_rec_id = LS_DB_DRUG_THERAPY_TMP.dgthvac_fk_adt_rec_id,LS_DB_DRUG_THERAPY.dgthvac_date_modified = LS_DB_DRUG_THERAPY_TMP.dgthvac_date_modified,LS_DB_DRUG_THERAPY.dgthvac_date_created = LS_DB_DRUG_THERAPY_TMP.dgthvac_date_created,LS_DB_DRUG_THERAPY.dgthvac_ari_rec_id = LS_DB_DRUG_THERAPY_TMP.dgthvac_ari_rec_id,LS_DB_DRUG_THERAPY.dgthbstdoc_user_modified = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_user_modified,LS_DB_DRUG_THERAPY.dgthbstdoc_user_created = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_user_created,LS_DB_DRUG_THERAPY.dgthbstdoc_title_de_ml = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_title_de_ml,LS_DB_DRUG_THERAPY.dgthbstdoc_title = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_title,LS_DB_DRUG_THERAPY.dgthbstdoc_telephone = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_telephone,LS_DB_DRUG_THERAPY.dgthbstdoc_spr_id = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_spr_id,LS_DB_DRUG_THERAPY.dgthbstdoc_record_id = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_record_id,LS_DB_DRUG_THERAPY.dgthbstdoc_middle_name = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_middle_name,LS_DB_DRUG_THERAPY.dgthbstdoc_last_name = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_last_name,LS_DB_DRUG_THERAPY.dgthbstdoc_first_name = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_first_name,LS_DB_DRUG_THERAPY.dgthbstdoc_email_id = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_email_id,LS_DB_DRUG_THERAPY.dgthbstdoc_date_modified = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_date_modified,LS_DB_DRUG_THERAPY.dgthbstdoc_date_created = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_date_created,LS_DB_DRUG_THERAPY.dgthbstdoc_ari_rec_id = LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_ari_rec_id,
LS_DB_DRUG_THERAPY.PROCESSING_DT = LS_DB_DRUG_THERAPY_TMP.PROCESSING_DT,
LS_DB_DRUG_THERAPY.receipt_id     =LS_DB_DRUG_THERAPY_TMP.receipt_id    ,
LS_DB_DRUG_THERAPY.case_no        =LS_DB_DRUG_THERAPY_TMP.case_no           ,
LS_DB_DRUG_THERAPY.case_version   =LS_DB_DRUG_THERAPY_TMP.case_version      ,
LS_DB_DRUG_THERAPY.version_no     =LS_DB_DRUG_THERAPY_TMP.version_no        ,
LS_DB_DRUG_THERAPY.user_modified  =LS_DB_DRUG_THERAPY_TMP.user_modified     ,
LS_DB_DRUG_THERAPY.date_modified  =LS_DB_DRUG_THERAPY_TMP.date_modified     ,
LS_DB_DRUG_THERAPY.expiry_date    =LS_DB_DRUG_THERAPY_TMP.expiry_date       ,
LS_DB_DRUG_THERAPY.created_by     =LS_DB_DRUG_THERAPY_TMP.created_by        ,
LS_DB_DRUG_THERAPY.created_dt     =LS_DB_DRUG_THERAPY_TMP.created_dt        ,
LS_DB_DRUG_THERAPY.load_ts        =LS_DB_DRUG_THERAPY_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP 
WHERE 	LS_DB_DRUG_THERAPY.INTEGRATION_ID = LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_THERAPY_TMP.PROCESSING_DT = LS_DB_DRUG_THERAPY.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DRUG_THERAPY.dgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_THERAPY.dgthbstdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_THERAPY.dgthvac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DRUG_THERAPY_TMP.dgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_THERAPY_TMP.dgthvac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DRUG_THERAPY.dgth_RECORD_ID ,LS_DB_DRUG_THERAPY.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP 
ON LS_DB_DRUG_THERAPY.dgth_RECORD_ID=LS_DB_DRUG_THERAPY_TMP.dgth_RECORD_ID
AND LS_DB_DRUG_THERAPY.INTEGRATION_ID = LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID 
where LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID  is null AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DRUG_THERAPY.dgth_RECORD_ID in (select dgth_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP )
) TMP where LS_DB_DRUG_THERAPY.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DRUG_THERAPY.dgth_RECORD_ID ,LS_DB_DRUG_THERAPY.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP 
    ON LS_DB_DRUG_THERAPY.dgth_RECORD_ID=LS_DB_DRUG_THERAPY_TMP.dgth_RECORD_ID
    AND LS_DB_DRUG_THERAPY.INTEGRATION_ID = LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID 
    where LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID  is null AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DRUG_THERAPY.dgth_RECORD_ID in (select dgth_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY
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
integration_id ,dgth_version,
dgth_user_modified,
dgth_user_created,
dgth_txt_start_ther_date,
dgth_txt_frequency,
dgth_total_dose_unit_de_ml,
dgth_total_dose_unit,
dgth_total_dose_manual,
dgth_total_dose,
dgth_therapy_start_date_text,
dgth_therapy_lang,
dgth_therapy_end_date_text,
dgth_therapy_dose_reduced_date,
dgth_therapy_continued_de_ml,
dgth_therapy_continued,
dgth_theraphy_site_de_ml,
dgth_theraphy_site,
dgth_spr_id,
dgth_route_admin_termid_ver,
dgth_route_admin_termid,
dgth_record_id,
dgth_quantity_ocr,
dgth_quantity_mode,
dgth_product_type,
dgth_product_code,
dgth_pharmaceutical_termid_ver,
dgth_pharmaceutical_termid,
dgth_par_route_admin_termid_ver,
dgth_par_route_admin_termid,
dgth_overdose_result_sae_de_ml,
dgth_overdose_result_sae,
dgth_notifictaion,
dgth_no_of_complaints_on_the_batch,
dgth_manual_therapy_duration_type,
dgth_lotnumberstore,
dgth_lot_number_nf,
dgth_lot_number,
dgth_is_sample_available_de_ml,
dgth_is_sample_available,
dgth_inq_rec_id,
dgth_gestationperiodunit,
dgth_gestationperiod,
dgth_frequency_de_ml,
dgth_frequency,
dgth_form_strength_unit_value,
dgth_form_strength_unit,
dgth_form_strength,
dgth_fk_adtbd_rec_id,
dgth_fk_ad_rec_id,
dgth_expiration_date_fmt,
dgth_expiration_date,
dgth_entity_updated,
dgth_duration_text,
dgth_drugtreatdurationunit,
dgth_drugstructuredquantityunit,
dgth_drugstructuredquantitynumb,
dgth_drugstructuredosageunit_lang,
dgth_drugstructuredosageunit_de_ml,
dgth_drugstructuredosageunit,
dgth_drugstructuredosagenumb_ocr,
dgth_drugstructuredosagenumb_lang,
dgth_drugstructuredosagenumb,
dgth_drugstartperiodunit_de_ml,
dgth_drugstartperiodunit,
dgth_drugstartperiod_lang,
dgth_drugstartperiod,
dgth_drugstartdatefmt,
dgth_drugstartdate_tz,
dgth_drugstartdate_nf,
dgth_drugstartdate,
dgth_drugseparatedosagenumb_ocr,
dgth_drugseparatedosagenumb_lang,
dgth_drugseparatedosagenumb,
dgth_drugparadministration_sf,
dgth_drugparadministration_nf,
dgth_drugparadministration_lang,
dgth_drugparadministration_id_ver,
dgth_drugparadministration_id,
dgth_drugparadministration_de_ml,
dgth_drugparadministration,
dgth_druglastperiodunit_de_ml,
dgth_druglastperiodunit,
dgth_druglastperiod_lang,
dgth_druglastperiod,
dgth_drugintdosageunitnumb_lang,
dgth_drugintdosageunitnumb,
dgth_drugintdosageunitdef_lang,
dgth_drugintdosageunitdef_de_ml,
dgth_drugintdosageunitdef,
dgth_drugenddatefmt,
dgth_drugenddate_tz,
dgth_drugenddate_nf,
dgth_drugenddate,
dgth_drugdosagetext_lang,
dgth_drugdosagetext,
dgth_drugdosageform_text,
dgth_drugdosageform_termid_version,
dgth_drugdosageform_termid,
dgth_drugdosageform_sf,
dgth_drugdosageform_nf,
dgth_drugdosageform_lang,
dgth_drugdosageform_de_ml,
dgth_drugdosageform_as_coded,
dgth_drugdosageform,
dgth_drugcumulativedosageunit_lang,
dgth_drugcumulativedosageunit_de_ml,
dgth_drugcumulativedosageunit,
dgth_drugcumulativedosagenumb_lang,
dgth_drugcumulativedosagenumb,
dgth_drugadministrationroute_sf,
dgth_drugadministrationroute_ocr,
dgth_drugadministrationroute_nf,
dgth_drugadministrationroute_lang,
dgth_drugadministrationroute_id_ver,
dgth_drugadministrationroute_id,
dgth_drugadministrationroute_de_ml,
dgth_drugadministrationroute,
dgth_drugadmindurationunit_de_ml,
dgth_drugadmindurationunit,
dgth_drugadminduration,
dgth_drugadmin_route_ft,
dgth_drug_therpy_duration,
dgth_drug_theropy_package_size,
dgth_drug_theropy_batch_size,
dgth_drug_therapy_on_going_de_ml,
dgth_drug_therapy_on_going,
dgth_drug_para_administration_ft,
dgth_drug_daily_dose,
dgth_dosposition,
dgth_dose_prior_pregnancy_unit,
dgth_dose_prior_pregnancy_numb,
dgth_dose_num_series,
dgth_dates_of_use_ocr,
dgth_date_overdose_occurred,
dgth_date_modified,
dgth_date_created,
dgth_daily_dose_unit,
dgth_daily_dose_manual,
dgth_daily_dose,
dgth_comp_rec_id,
dgth_collection_date,
dgth_ari_rec_id,
dgth_anatomical_approach_site_nf,
dgth_anatomical_approach_site_de_ml,
dgth_anatomical_approach_site,
dgth_actiondrug_de_ml,
dgth_actiondrug,
dgthvac_vaccine_name_nf,
dgthvac_vaccine_name,
dgthvac_vaccination_type_nf,
dgthvac_vaccination_type_de_ml,
dgthvac_vaccination_type,
dgthvac_vaccination_telephone_nf,
dgthvac_vaccination_telephone,
dgthvac_vaccination_state_sf,
dgthvac_vaccination_state_nf,
dgthvac_vaccination_state_de_ml,
dgthvac_vaccination_state,
dgthvac_vaccination_postal_code_nf,
dgthvac_vaccination_postal_code,
dgthvac_vaccination_fax_nf,
dgthvac_vaccination_fax,
dgthvac_vaccination_country_nf,
dgthvac_vaccination_country,
dgthvac_vaccination_city_nf,
dgthvac_vaccination_city,
dgthvac_vaccination_address2_nf,
dgthvac_vaccination_address2,
dgthvac_vaccination_address1_nf,
dgthvac_vaccination_address1,
dgthvac_user_modified,
dgthvac_user_created,
dgthvac_spr_id,
dgthvac_record_id,
dgthvac_fk_adt_rec_id,
dgthvac_date_modified,
dgthvac_date_created,
dgthvac_ari_rec_id,
dgthbstdoc_user_modified,
dgthbstdoc_user_created,
dgthbstdoc_title_de_ml,
dgthbstdoc_title,
dgthbstdoc_telephone,
dgthbstdoc_spr_id,
dgthbstdoc_record_id,
dgthbstdoc_middle_name,
dgthbstdoc_last_name,
dgthbstdoc_first_name,
dgthbstdoc_email_id,
dgthbstdoc_date_modified,
dgthbstdoc_date_created,
dgthbstdoc_ari_rec_id)
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
integration_id ,dgth_version,
dgth_user_modified,
dgth_user_created,
dgth_txt_start_ther_date,
dgth_txt_frequency,
dgth_total_dose_unit_de_ml,
dgth_total_dose_unit,
dgth_total_dose_manual,
dgth_total_dose,
dgth_therapy_start_date_text,
dgth_therapy_lang,
dgth_therapy_end_date_text,
dgth_therapy_dose_reduced_date,
dgth_therapy_continued_de_ml,
dgth_therapy_continued,
dgth_theraphy_site_de_ml,
dgth_theraphy_site,
dgth_spr_id,
dgth_route_admin_termid_ver,
dgth_route_admin_termid,
dgth_record_id,
dgth_quantity_ocr,
dgth_quantity_mode,
dgth_product_type,
dgth_product_code,
dgth_pharmaceutical_termid_ver,
dgth_pharmaceutical_termid,
dgth_par_route_admin_termid_ver,
dgth_par_route_admin_termid,
dgth_overdose_result_sae_de_ml,
dgth_overdose_result_sae,
dgth_notifictaion,
dgth_no_of_complaints_on_the_batch,
dgth_manual_therapy_duration_type,
dgth_lotnumberstore,
dgth_lot_number_nf,
dgth_lot_number,
dgth_is_sample_available_de_ml,
dgth_is_sample_available,
dgth_inq_rec_id,
dgth_gestationperiodunit,
dgth_gestationperiod,
dgth_frequency_de_ml,
dgth_frequency,
dgth_form_strength_unit_value,
dgth_form_strength_unit,
dgth_form_strength,
dgth_fk_adtbd_rec_id,
dgth_fk_ad_rec_id,
dgth_expiration_date_fmt,
dgth_expiration_date,
dgth_entity_updated,
dgth_duration_text,
dgth_drugtreatdurationunit,
dgth_drugstructuredquantityunit,
dgth_drugstructuredquantitynumb,
dgth_drugstructuredosageunit_lang,
dgth_drugstructuredosageunit_de_ml,
dgth_drugstructuredosageunit,
dgth_drugstructuredosagenumb_ocr,
dgth_drugstructuredosagenumb_lang,
dgth_drugstructuredosagenumb,
dgth_drugstartperiodunit_de_ml,
dgth_drugstartperiodunit,
dgth_drugstartperiod_lang,
dgth_drugstartperiod,
dgth_drugstartdatefmt,
dgth_drugstartdate_tz,
dgth_drugstartdate_nf,
dgth_drugstartdate,
dgth_drugseparatedosagenumb_ocr,
dgth_drugseparatedosagenumb_lang,
dgth_drugseparatedosagenumb,
dgth_drugparadministration_sf,
dgth_drugparadministration_nf,
dgth_drugparadministration_lang,
dgth_drugparadministration_id_ver,
dgth_drugparadministration_id,
dgth_drugparadministration_de_ml,
dgth_drugparadministration,
dgth_druglastperiodunit_de_ml,
dgth_druglastperiodunit,
dgth_druglastperiod_lang,
dgth_druglastperiod,
dgth_drugintdosageunitnumb_lang,
dgth_drugintdosageunitnumb,
dgth_drugintdosageunitdef_lang,
dgth_drugintdosageunitdef_de_ml,
dgth_drugintdosageunitdef,
dgth_drugenddatefmt,
dgth_drugenddate_tz,
dgth_drugenddate_nf,
dgth_drugenddate,
dgth_drugdosagetext_lang,
dgth_drugdosagetext,
dgth_drugdosageform_text,
dgth_drugdosageform_termid_version,
dgth_drugdosageform_termid,
dgth_drugdosageform_sf,
dgth_drugdosageform_nf,
dgth_drugdosageform_lang,
dgth_drugdosageform_de_ml,
dgth_drugdosageform_as_coded,
dgth_drugdosageform,
dgth_drugcumulativedosageunit_lang,
dgth_drugcumulativedosageunit_de_ml,
dgth_drugcumulativedosageunit,
dgth_drugcumulativedosagenumb_lang,
dgth_drugcumulativedosagenumb,
dgth_drugadministrationroute_sf,
dgth_drugadministrationroute_ocr,
dgth_drugadministrationroute_nf,
dgth_drugadministrationroute_lang,
dgth_drugadministrationroute_id_ver,
dgth_drugadministrationroute_id,
dgth_drugadministrationroute_de_ml,
dgth_drugadministrationroute,
dgth_drugadmindurationunit_de_ml,
dgth_drugadmindurationunit,
dgth_drugadminduration,
dgth_drugadmin_route_ft,
dgth_drug_therpy_duration,
dgth_drug_theropy_package_size,
dgth_drug_theropy_batch_size,
dgth_drug_therapy_on_going_de_ml,
dgth_drug_therapy_on_going,
dgth_drug_para_administration_ft,
dgth_drug_daily_dose,
dgth_dosposition,
dgth_dose_prior_pregnancy_unit,
dgth_dose_prior_pregnancy_numb,
dgth_dose_num_series,
dgth_dates_of_use_ocr,
dgth_date_overdose_occurred,
dgth_date_modified,
dgth_date_created,
dgth_daily_dose_unit,
dgth_daily_dose_manual,
dgth_daily_dose,
dgth_comp_rec_id,
dgth_collection_date,
dgth_ari_rec_id,
dgth_anatomical_approach_site_nf,
dgth_anatomical_approach_site_de_ml,
dgth_anatomical_approach_site,
dgth_actiondrug_de_ml,
dgth_actiondrug,
dgthvac_vaccine_name_nf,
dgthvac_vaccine_name,
dgthvac_vaccination_type_nf,
dgthvac_vaccination_type_de_ml,
dgthvac_vaccination_type,
dgthvac_vaccination_telephone_nf,
dgthvac_vaccination_telephone,
dgthvac_vaccination_state_sf,
dgthvac_vaccination_state_nf,
dgthvac_vaccination_state_de_ml,
dgthvac_vaccination_state,
dgthvac_vaccination_postal_code_nf,
dgthvac_vaccination_postal_code,
dgthvac_vaccination_fax_nf,
dgthvac_vaccination_fax,
dgthvac_vaccination_country_nf,
dgthvac_vaccination_country,
dgthvac_vaccination_city_nf,
dgthvac_vaccination_city,
dgthvac_vaccination_address2_nf,
dgthvac_vaccination_address2,
dgthvac_vaccination_address1_nf,
dgthvac_vaccination_address1,
dgthvac_user_modified,
dgthvac_user_created,
dgthvac_spr_id,
dgthvac_record_id,
dgthvac_fk_adt_rec_id,
dgthvac_date_modified,
dgthvac_date_created,
dgthvac_ari_rec_id,
dgthbstdoc_user_modified,
dgthbstdoc_user_created,
dgthbstdoc_title_de_ml,
dgthbstdoc_title,
dgthbstdoc_telephone,
dgthbstdoc_spr_id,
dgthbstdoc_record_id,
dgthbstdoc_middle_name,
dgthbstdoc_last_name,
dgthbstdoc_first_name,
dgthbstdoc_email_id,
dgthbstdoc_date_modified,
dgthbstdoc_date_created,
dgthbstdoc_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY TGT
WHERE  ( dgth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_THERAPY.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_THERAPY_TMP.PROCESSING_DT)
AND LS_DB_DRUG_THERAPY.INTEGRATION_ID = LS_DB_DRUG_THERAPY_TMP.INTEGRATION_ID
AND LS_DB_DRUG_THERAPY.dgth_RECORD_ID = LS_DB_DRUG_THERAPY_TMP.dgth_RECORD_ID
AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DRUG_THERAPY.dgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_THERAPY.dgthbstdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_THERAPY.dgthvac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DRUG_THERAPY_TMP.dgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_THERAPY_TMP.dgthbstdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_THERAPY_TMP.dgthvac_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( dgth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_THERAPY'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_THERAPY Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_THERAPY'),
	'LSDB','Case','LS_DB_DRUG_THERAPY',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_THERAPY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
