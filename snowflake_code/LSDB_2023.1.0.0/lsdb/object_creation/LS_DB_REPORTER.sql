
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_REPORTER()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTER'),
	'LSRA','Case','LS_DB_REPORTER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_REPORTER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTER_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTER_DELETION_TMP  As select RECORD_ID,'lsmv_primarysource' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_primarysource WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10003','1002','1002','1003','1008','1015','1021','1028','122','15','345','4','4','6','8112','8147','9740','9927','9928','9941')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_primarysource WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_primarysource WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_primarysource_SUBSET AS 
(
select * from 
    (SELECT  
    address1  address1,address2  address2,also_reported_to  also_reported_to,ari_rec_id  ari_rec_id,comp_rec_id  comp_rec_id,confidential  confidential,consent_contact  consent_contact,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(consent_contact AS VARCHAR(100)) )consent_contact_de_ml , consent_to_contact_hcp  consent_to_contact_hcp,consent_to_contact_reporter  consent_to_contact_reporter,contact_method  contact_method,contact_person  contact_person,contact_reason  contact_reason,contact_rec_id  contact_rec_id,county  county,county_nf  county_nf,cra_name  cra_name,data_encrypted  data_encrypted,dataprivacy_present  dataprivacy_present,date_created  date_created,date_modified  date_modified,designation  designation,do_not_report_name_code  do_not_report_name_code,doctor_given_family_name  doctor_given_family_name,doctor_given_name  doctor_given_name,donotreportname  donotreportname,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(donotreportname AS VARCHAR(100)) )donotreportname_de_ml , duns_number  duns_number,duns_number_as_coded  duns_number_as_coded,duns_number_checked  duns_number_checked,e2b_r3_regulatory_purpose  e2b_r3_regulatory_purpose,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(e2b_r3_regulatory_purpose AS VARCHAR(100)) )e2b_r3_regulatory_purpose_de_ml , email  email,email_nf  email_nf,entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fax  fax,fei_number_checked  fei_number_checked,feinumber  feinumber,feinumber_as_coded  feinumber_as_coded,fk_ap_recid_investigator  fk_ap_recid_investigator,fk_asr_rec_id  fk_asr_rec_id,fl_health_prof_type  fl_health_prof_type,hcp_classification_kr  hcp_classification_kr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9928' AND CD=CAST(hcp_classification_kr AS VARCHAR(100)) )hcp_classification_kr_de_ml , health_prof_manual_check  health_prof_manual_check,hospital  hospital,hospital_address  hospital_address,hospital_code  hospital_code,hospital_name  hospital_name,identity_disclosed  identity_disclosed,initial_report_sent_to_fda  initial_report_sent_to_fda,inq_rec_id  inq_rec_id,institution  institution,is_coordinator  is_coordinator,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(is_coordinator AS VARCHAR(100)) )is_coordinator_de_ml , is_health_prof  is_health_prof,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(is_health_prof AS VARCHAR(100)) )is_health_prof_de_ml , is_investigator  is_investigator,is_pat_reporter  is_pat_reporter,is_person_author  is_person_author,is_person_investigator  is_person_investigator,is_primary_source  is_primary_source,is_primaryreporter_from_portal  is_primaryreporter_from_portal,literaturereference  literaturereference,literaturereference_lang  literaturereference_lang,literaturereference_nf  literaturereference_nf,observestudytype  observestudytype,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='122' AND CD=CAST(observestudytype AS VARCHAR(100)) )observestudytype_de_ml , occupation  occupation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1028' AND CD=CAST(occupation AS VARCHAR(100)) )occupation_de_ml , occupation_ocr  occupation_ocr,occupation_sf  occupation_sf,other_specify  other_specify,otherhcp_kr  otherhcp_kr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9927' AND CD=CAST(otherhcp_kr AS VARCHAR(100)) )otherhcp_kr_de_ml , person_type  person_type,pharmacy_name  pharmacy_name,phone  phone,phone_nf  phone_nf,physicion_code  physicion_code,po_box  po_box,prescriber_code  prescriber_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(prescriber_code AS VARCHAR(100)) )prescriber_code_de_ml , primary_rep_reg_purpose  primary_rep_reg_purpose,primary_reporter  primary_reporter,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(primary_reporter AS VARCHAR(100)) )primary_reporter_de_ml , primary_source_type  primary_source_type,primary_src_lit_ref_num  primary_src_lit_ref_num,primary_uiidnumber  primary_uiidnumber,primarysource_lang  primarysource_lang,qualification  qualification,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='6' AND CD=CAST(qualification AS VARCHAR(100)) )qualification_de_ml , qualification_nf  qualification_nf,record_id  record_id,reported_to_distributor  reported_to_distributor,reported_to_user_facility  reported_to_user_facility,reporter_fullname  reporter_fullname,reporter_informed_to_mfr  reporter_informed_to_mfr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10003' AND CD=CAST(reporter_informed_to_mfr AS VARCHAR(100)) )reporter_informed_to_mfr_de_ml , reporter_is_patient  reporter_is_patient,reporter_or_contact  reporter_or_contact,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9740' AND CD=CAST(reporter_or_contact AS VARCHAR(100)) )reporter_or_contact_de_ml , reporter_sent_to_fda  reporter_sent_to_fda,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1008' AND CD=CAST(reporter_sent_to_fda AS VARCHAR(100)) )reporter_sent_to_fda_de_ml , reporter_tel_country_code  reporter_tel_country_code,reporter_tel_extension  reporter_tel_extension,reporter_type  reporter_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1003' AND CD=CAST(reporter_type AS VARCHAR(100)) )reporter_type_de_ml , reporter_type_nf  reporter_type_nf,reportercity  reportercity,reportercity_as_coded  reportercity_as_coded,reportercity_lang  reportercity_lang,reportercity_nf  reportercity_nf,reportercountry  reportercountry,reportercountry_as_coded  reportercountry_as_coded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(reportercountry AS VARCHAR(100)) )reportercountry_de_ml , reportercountry_free_text  reportercountry_free_text,reportercountry_lang  reportercountry_lang,reportercountry_nf  reportercountry_nf,reportercountry_ocr  reportercountry_ocr,reporterdeptartment  reporterdeptartment,reporterdeptartment_lang  reporterdeptartment_lang,reporterdeptartment_nf  reporterdeptartment_nf,reporterfamilyname  reporterfamilyname,reporterfamilyname_lang  reporterfamilyname_lang,reporterfamilyname_nf  reporterfamilyname_nf,reportergivenname  reportergivenname,reportergivenname_lang  reportergivenname_lang,reportergivenname_nf  reportergivenname_nf,reportermiddlename  reportermiddlename,reportermiddlename_lang  reportermiddlename_lang,reportermiddlename_nf  reportermiddlename_nf,reporterorganization  reporterorganization,reporterorganization_lang  reporterorganization_lang,reporterorganization_nf  reporterorganization_nf,reporterpostcode  reporterpostcode,reporterpostcode_lang  reporterpostcode_lang,reporterpostcode_nf  reporterpostcode_nf,reporterpstcode_as_coded  reporterpstcode_as_coded,reporterstate  reporterstate,reporterstate_as_coded  reporterstate_as_coded,reporterstate_lang  reporterstate_lang,reporterstate_nf  reporterstate_nf,reporterstreet  reporterstreet,reporterstreet1  reporterstreet1,reporterstreet2  reporterstreet2,reporterstreet_as_coded1  reporterstreet_as_coded1,reporterstreet_as_coded2  reporterstreet_as_coded2,reporterstreet_lang  reporterstreet_lang,reporterstreet_nf  reporterstreet_nf,reportertitle  reportertitle,reportertitle_ft  reportertitle_ft,reportertitle_lang  reportertitle_lang,reportertitle_nf  reportertitle_nf,reportertitle_sf  reportertitle_sf,reportertype_ft  reportertype_ft,requester_category  requester_category,root_form  root_form,site  site,spanish_state  spanish_state,spanish_state_code  spanish_state_code,spanish_state_code_nf  spanish_state_code_nf,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8147' AND CD=CAST(spanish_state AS VARCHAR(100)) )spanish_state_de_ml , specialty  specialty,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='345' AND CD=CAST(specialty AS VARCHAR(100)) )specialty_de_ml , specialty_sf  specialty_sf,sponsorstudynumb  sponsorstudynumb,sponsorstudynumb_lang  sponsorstudynumb_lang,spr_id  spr_id,street_address  street_address,studyname  studyname,studyname_lang  studyname_lang,survay_name  survay_name,telephone  telephone,theraputicarea  theraputicarea,type  type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8112' AND CD=CAST(type AS VARCHAR(100)) )type_de_ml , user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,zip_code  zip_code,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_primarysource
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTER_DELETION_TMP  WHERE TABLE_NAME='lsmv_primarysource')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_primarysource_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_primarysource_SUBSET.USER_CREATED CREATED_BY,lsmv_primarysource_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_primarysource_SUBSET.zip_code  ,lsmv_primarysource_SUBSET.version  ,lsmv_primarysource_SUBSET.uuid  ,lsmv_primarysource_SUBSET.user_modified  ,lsmv_primarysource_SUBSET.user_created  ,lsmv_primarysource_SUBSET.type_de_ml  ,lsmv_primarysource_SUBSET.type  ,lsmv_primarysource_SUBSET.theraputicarea  ,lsmv_primarysource_SUBSET.telephone  ,lsmv_primarysource_SUBSET.survay_name  ,lsmv_primarysource_SUBSET.studyname_lang  ,lsmv_primarysource_SUBSET.studyname  ,lsmv_primarysource_SUBSET.street_address  ,lsmv_primarysource_SUBSET.spr_id  ,lsmv_primarysource_SUBSET.sponsorstudynumb_lang  ,lsmv_primarysource_SUBSET.sponsorstudynumb  ,lsmv_primarysource_SUBSET.specialty_sf  ,lsmv_primarysource_SUBSET.specialty_de_ml  ,lsmv_primarysource_SUBSET.specialty  ,lsmv_primarysource_SUBSET.spanish_state_de_ml  ,lsmv_primarysource_SUBSET.spanish_state_code_nf  ,lsmv_primarysource_SUBSET.spanish_state_code  ,lsmv_primarysource_SUBSET.spanish_state  ,lsmv_primarysource_SUBSET.site  ,lsmv_primarysource_SUBSET.root_form  ,lsmv_primarysource_SUBSET.requester_category  ,lsmv_primarysource_SUBSET.reportertype_ft  ,lsmv_primarysource_SUBSET.reportertitle_sf  ,lsmv_primarysource_SUBSET.reportertitle_nf  ,lsmv_primarysource_SUBSET.reportertitle_lang  ,lsmv_primarysource_SUBSET.reportertitle_ft  ,lsmv_primarysource_SUBSET.reportertitle  ,lsmv_primarysource_SUBSET.reporterstreet_nf  ,lsmv_primarysource_SUBSET.reporterstreet_lang  ,lsmv_primarysource_SUBSET.reporterstreet_as_coded2  ,lsmv_primarysource_SUBSET.reporterstreet_as_coded1  ,lsmv_primarysource_SUBSET.reporterstreet2  ,lsmv_primarysource_SUBSET.reporterstreet1  ,lsmv_primarysource_SUBSET.reporterstreet  ,lsmv_primarysource_SUBSET.reporterstate_nf  ,lsmv_primarysource_SUBSET.reporterstate_lang  ,lsmv_primarysource_SUBSET.reporterstate_as_coded  ,lsmv_primarysource_SUBSET.reporterstate  ,lsmv_primarysource_SUBSET.reporterpstcode_as_coded  ,lsmv_primarysource_SUBSET.reporterpostcode_nf  ,lsmv_primarysource_SUBSET.reporterpostcode_lang  ,lsmv_primarysource_SUBSET.reporterpostcode  ,lsmv_primarysource_SUBSET.reporterorganization_nf  ,lsmv_primarysource_SUBSET.reporterorganization_lang  ,lsmv_primarysource_SUBSET.reporterorganization  ,lsmv_primarysource_SUBSET.reportermiddlename_nf  ,lsmv_primarysource_SUBSET.reportermiddlename_lang  ,lsmv_primarysource_SUBSET.reportermiddlename  ,lsmv_primarysource_SUBSET.reportergivenname_nf  ,lsmv_primarysource_SUBSET.reportergivenname_lang  ,lsmv_primarysource_SUBSET.reportergivenname  ,lsmv_primarysource_SUBSET.reporterfamilyname_nf  ,lsmv_primarysource_SUBSET.reporterfamilyname_lang  ,lsmv_primarysource_SUBSET.reporterfamilyname  ,lsmv_primarysource_SUBSET.reporterdeptartment_nf  ,lsmv_primarysource_SUBSET.reporterdeptartment_lang  ,lsmv_primarysource_SUBSET.reporterdeptartment  ,lsmv_primarysource_SUBSET.reportercountry_ocr  ,lsmv_primarysource_SUBSET.reportercountry_nf  ,lsmv_primarysource_SUBSET.reportercountry_lang  ,lsmv_primarysource_SUBSET.reportercountry_free_text  ,lsmv_primarysource_SUBSET.reportercountry_de_ml  ,lsmv_primarysource_SUBSET.reportercountry_as_coded  ,lsmv_primarysource_SUBSET.reportercountry  ,lsmv_primarysource_SUBSET.reportercity_nf  ,lsmv_primarysource_SUBSET.reportercity_lang  ,lsmv_primarysource_SUBSET.reportercity_as_coded  ,lsmv_primarysource_SUBSET.reportercity  ,lsmv_primarysource_SUBSET.reporter_type_nf  ,lsmv_primarysource_SUBSET.reporter_type_de_ml  ,lsmv_primarysource_SUBSET.reporter_type  ,lsmv_primarysource_SUBSET.reporter_tel_extension  ,lsmv_primarysource_SUBSET.reporter_tel_country_code  ,lsmv_primarysource_SUBSET.reporter_sent_to_fda_de_ml  ,lsmv_primarysource_SUBSET.reporter_sent_to_fda  ,lsmv_primarysource_SUBSET.reporter_or_contact_de_ml  ,lsmv_primarysource_SUBSET.reporter_or_contact  ,lsmv_primarysource_SUBSET.reporter_is_patient  ,lsmv_primarysource_SUBSET.reporter_informed_to_mfr_de_ml  ,lsmv_primarysource_SUBSET.reporter_informed_to_mfr  ,lsmv_primarysource_SUBSET.reporter_fullname  ,lsmv_primarysource_SUBSET.reported_to_user_facility  ,lsmv_primarysource_SUBSET.reported_to_distributor  ,lsmv_primarysource_SUBSET.record_id  ,lsmv_primarysource_SUBSET.qualification_nf  ,lsmv_primarysource_SUBSET.qualification_de_ml  ,lsmv_primarysource_SUBSET.qualification  ,lsmv_primarysource_SUBSET.primarysource_lang  ,lsmv_primarysource_SUBSET.primary_uiidnumber  ,lsmv_primarysource_SUBSET.primary_src_lit_ref_num  ,lsmv_primarysource_SUBSET.primary_source_type  ,lsmv_primarysource_SUBSET.primary_reporter_de_ml  ,lsmv_primarysource_SUBSET.primary_reporter  ,lsmv_primarysource_SUBSET.primary_rep_reg_purpose  ,lsmv_primarysource_SUBSET.prescriber_code_de_ml  ,lsmv_primarysource_SUBSET.prescriber_code  ,lsmv_primarysource_SUBSET.po_box  ,lsmv_primarysource_SUBSET.physicion_code  ,lsmv_primarysource_SUBSET.phone_nf  ,lsmv_primarysource_SUBSET.phone  ,lsmv_primarysource_SUBSET.pharmacy_name  ,lsmv_primarysource_SUBSET.person_type  ,lsmv_primarysource_SUBSET.otherhcp_kr_de_ml  ,lsmv_primarysource_SUBSET.otherhcp_kr  ,lsmv_primarysource_SUBSET.other_specify  ,lsmv_primarysource_SUBSET.occupation_sf  ,lsmv_primarysource_SUBSET.occupation_ocr  ,lsmv_primarysource_SUBSET.occupation_de_ml  ,lsmv_primarysource_SUBSET.occupation  ,lsmv_primarysource_SUBSET.observestudytype_de_ml  ,lsmv_primarysource_SUBSET.observestudytype  ,lsmv_primarysource_SUBSET.literaturereference_nf  ,lsmv_primarysource_SUBSET.literaturereference_lang  ,lsmv_primarysource_SUBSET.literaturereference  ,lsmv_primarysource_SUBSET.is_primaryreporter_from_portal  ,lsmv_primarysource_SUBSET.is_primary_source  ,lsmv_primarysource_SUBSET.is_person_investigator  ,lsmv_primarysource_SUBSET.is_person_author  ,lsmv_primarysource_SUBSET.is_pat_reporter  ,lsmv_primarysource_SUBSET.is_investigator  ,lsmv_primarysource_SUBSET.is_health_prof_de_ml  ,lsmv_primarysource_SUBSET.is_health_prof  ,lsmv_primarysource_SUBSET.is_coordinator_de_ml  ,lsmv_primarysource_SUBSET.is_coordinator  ,lsmv_primarysource_SUBSET.institution  ,lsmv_primarysource_SUBSET.inq_rec_id  ,lsmv_primarysource_SUBSET.initial_report_sent_to_fda  ,lsmv_primarysource_SUBSET.identity_disclosed  ,lsmv_primarysource_SUBSET.hospital_name  ,lsmv_primarysource_SUBSET.hospital_code  ,lsmv_primarysource_SUBSET.hospital_address  ,lsmv_primarysource_SUBSET.hospital  ,lsmv_primarysource_SUBSET.health_prof_manual_check  ,lsmv_primarysource_SUBSET.hcp_classification_kr_de_ml  ,lsmv_primarysource_SUBSET.hcp_classification_kr  ,lsmv_primarysource_SUBSET.fl_health_prof_type  ,lsmv_primarysource_SUBSET.fk_asr_rec_id  ,lsmv_primarysource_SUBSET.fk_ap_recid_investigator  ,lsmv_primarysource_SUBSET.feinumber_as_coded  ,lsmv_primarysource_SUBSET.feinumber  ,lsmv_primarysource_SUBSET.fei_number_checked  ,lsmv_primarysource_SUBSET.fax  ,lsmv_primarysource_SUBSET.ext_clob_fld  ,lsmv_primarysource_SUBSET.entity_updated  ,lsmv_primarysource_SUBSET.email_nf  ,lsmv_primarysource_SUBSET.email  ,lsmv_primarysource_SUBSET.e2b_r3_regulatory_purpose_de_ml  ,lsmv_primarysource_SUBSET.e2b_r3_regulatory_purpose  ,lsmv_primarysource_SUBSET.duns_number_checked  ,lsmv_primarysource_SUBSET.duns_number_as_coded  ,lsmv_primarysource_SUBSET.duns_number  ,lsmv_primarysource_SUBSET.donotreportname_de_ml  ,lsmv_primarysource_SUBSET.donotreportname  ,lsmv_primarysource_SUBSET.doctor_given_name  ,lsmv_primarysource_SUBSET.doctor_given_family_name  ,lsmv_primarysource_SUBSET.do_not_report_name_code  ,lsmv_primarysource_SUBSET.designation  ,lsmv_primarysource_SUBSET.date_modified  ,lsmv_primarysource_SUBSET.date_created  ,lsmv_primarysource_SUBSET.dataprivacy_present  ,lsmv_primarysource_SUBSET.data_encrypted  ,lsmv_primarysource_SUBSET.cra_name  ,lsmv_primarysource_SUBSET.county_nf  ,lsmv_primarysource_SUBSET.county  ,lsmv_primarysource_SUBSET.contact_rec_id  ,lsmv_primarysource_SUBSET.contact_reason  ,lsmv_primarysource_SUBSET.contact_person  ,lsmv_primarysource_SUBSET.contact_method  ,lsmv_primarysource_SUBSET.consent_to_contact_reporter  ,lsmv_primarysource_SUBSET.consent_to_contact_hcp  ,lsmv_primarysource_SUBSET.consent_contact_de_ml  ,lsmv_primarysource_SUBSET.consent_contact  ,lsmv_primarysource_SUBSET.confidential  ,lsmv_primarysource_SUBSET.comp_rec_id  ,lsmv_primarysource_SUBSET.ari_rec_id  ,lsmv_primarysource_SUBSET.also_reported_to  ,lsmv_primarysource_SUBSET.address2  ,lsmv_primarysource_SUBSET.address1 ,CONCAT(NVL(lsmv_primarysource_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_primarysource_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_primarysource_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP)
where target_table_name='LS_DB_REPORTER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTER'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER   
SET LS_DB_REPORTER.zip_code = LS_DB_REPORTER_TMP.zip_code,LS_DB_REPORTER.version = LS_DB_REPORTER_TMP.version,LS_DB_REPORTER.uuid = LS_DB_REPORTER_TMP.uuid,LS_DB_REPORTER.user_modified = LS_DB_REPORTER_TMP.user_modified,LS_DB_REPORTER.user_created = LS_DB_REPORTER_TMP.user_created,LS_DB_REPORTER.type_de_ml = LS_DB_REPORTER_TMP.type_de_ml,LS_DB_REPORTER.type = LS_DB_REPORTER_TMP.type,LS_DB_REPORTER.theraputicarea = LS_DB_REPORTER_TMP.theraputicarea,LS_DB_REPORTER.telephone = LS_DB_REPORTER_TMP.telephone,LS_DB_REPORTER.survay_name = LS_DB_REPORTER_TMP.survay_name,LS_DB_REPORTER.studyname_lang = LS_DB_REPORTER_TMP.studyname_lang,LS_DB_REPORTER.studyname = LS_DB_REPORTER_TMP.studyname,LS_DB_REPORTER.street_address = LS_DB_REPORTER_TMP.street_address,LS_DB_REPORTER.spr_id = LS_DB_REPORTER_TMP.spr_id,LS_DB_REPORTER.sponsorstudynumb_lang = LS_DB_REPORTER_TMP.sponsorstudynumb_lang,LS_DB_REPORTER.sponsorstudynumb = LS_DB_REPORTER_TMP.sponsorstudynumb,LS_DB_REPORTER.specialty_sf = LS_DB_REPORTER_TMP.specialty_sf,LS_DB_REPORTER.specialty_de_ml = LS_DB_REPORTER_TMP.specialty_de_ml,LS_DB_REPORTER.specialty = LS_DB_REPORTER_TMP.specialty,LS_DB_REPORTER.spanish_state_de_ml = LS_DB_REPORTER_TMP.spanish_state_de_ml,LS_DB_REPORTER.spanish_state_code_nf = LS_DB_REPORTER_TMP.spanish_state_code_nf,LS_DB_REPORTER.spanish_state_code = LS_DB_REPORTER_TMP.spanish_state_code,LS_DB_REPORTER.spanish_state = LS_DB_REPORTER_TMP.spanish_state,LS_DB_REPORTER.site = LS_DB_REPORTER_TMP.site,LS_DB_REPORTER.root_form = LS_DB_REPORTER_TMP.root_form,LS_DB_REPORTER.requester_category = LS_DB_REPORTER_TMP.requester_category,LS_DB_REPORTER.reportertype_ft = LS_DB_REPORTER_TMP.reportertype_ft,LS_DB_REPORTER.reportertitle_sf = LS_DB_REPORTER_TMP.reportertitle_sf,LS_DB_REPORTER.reportertitle_nf = LS_DB_REPORTER_TMP.reportertitle_nf,LS_DB_REPORTER.reportertitle_lang = LS_DB_REPORTER_TMP.reportertitle_lang,LS_DB_REPORTER.reportertitle_ft = LS_DB_REPORTER_TMP.reportertitle_ft,LS_DB_REPORTER.reportertitle = LS_DB_REPORTER_TMP.reportertitle,LS_DB_REPORTER.reporterstreet_nf = LS_DB_REPORTER_TMP.reporterstreet_nf,LS_DB_REPORTER.reporterstreet_lang = LS_DB_REPORTER_TMP.reporterstreet_lang,LS_DB_REPORTER.reporterstreet_as_coded2 = LS_DB_REPORTER_TMP.reporterstreet_as_coded2,LS_DB_REPORTER.reporterstreet_as_coded1 = LS_DB_REPORTER_TMP.reporterstreet_as_coded1,LS_DB_REPORTER.reporterstreet2 = LS_DB_REPORTER_TMP.reporterstreet2,LS_DB_REPORTER.reporterstreet1 = LS_DB_REPORTER_TMP.reporterstreet1,LS_DB_REPORTER.reporterstreet = LS_DB_REPORTER_TMP.reporterstreet,LS_DB_REPORTER.reporterstate_nf = LS_DB_REPORTER_TMP.reporterstate_nf,LS_DB_REPORTER.reporterstate_lang = LS_DB_REPORTER_TMP.reporterstate_lang,LS_DB_REPORTER.reporterstate_as_coded = LS_DB_REPORTER_TMP.reporterstate_as_coded,LS_DB_REPORTER.reporterstate = LS_DB_REPORTER_TMP.reporterstate,LS_DB_REPORTER.reporterpstcode_as_coded = LS_DB_REPORTER_TMP.reporterpstcode_as_coded,LS_DB_REPORTER.reporterpostcode_nf = LS_DB_REPORTER_TMP.reporterpostcode_nf,LS_DB_REPORTER.reporterpostcode_lang = LS_DB_REPORTER_TMP.reporterpostcode_lang,LS_DB_REPORTER.reporterpostcode = LS_DB_REPORTER_TMP.reporterpostcode,LS_DB_REPORTER.reporterorganization_nf = LS_DB_REPORTER_TMP.reporterorganization_nf,LS_DB_REPORTER.reporterorganization_lang = LS_DB_REPORTER_TMP.reporterorganization_lang,LS_DB_REPORTER.reporterorganization = LS_DB_REPORTER_TMP.reporterorganization,LS_DB_REPORTER.reportermiddlename_nf = LS_DB_REPORTER_TMP.reportermiddlename_nf,LS_DB_REPORTER.reportermiddlename_lang = LS_DB_REPORTER_TMP.reportermiddlename_lang,LS_DB_REPORTER.reportermiddlename = LS_DB_REPORTER_TMP.reportermiddlename,LS_DB_REPORTER.reportergivenname_nf = LS_DB_REPORTER_TMP.reportergivenname_nf,LS_DB_REPORTER.reportergivenname_lang = LS_DB_REPORTER_TMP.reportergivenname_lang,LS_DB_REPORTER.reportergivenname = LS_DB_REPORTER_TMP.reportergivenname,LS_DB_REPORTER.reporterfamilyname_nf = LS_DB_REPORTER_TMP.reporterfamilyname_nf,LS_DB_REPORTER.reporterfamilyname_lang = LS_DB_REPORTER_TMP.reporterfamilyname_lang,LS_DB_REPORTER.reporterfamilyname = LS_DB_REPORTER_TMP.reporterfamilyname,LS_DB_REPORTER.reporterdeptartment_nf = LS_DB_REPORTER_TMP.reporterdeptartment_nf,LS_DB_REPORTER.reporterdeptartment_lang = LS_DB_REPORTER_TMP.reporterdeptartment_lang,LS_DB_REPORTER.reporterdeptartment = LS_DB_REPORTER_TMP.reporterdeptartment,LS_DB_REPORTER.reportercountry_ocr = LS_DB_REPORTER_TMP.reportercountry_ocr,LS_DB_REPORTER.reportercountry_nf = LS_DB_REPORTER_TMP.reportercountry_nf,LS_DB_REPORTER.reportercountry_lang = LS_DB_REPORTER_TMP.reportercountry_lang,LS_DB_REPORTER.reportercountry_free_text = LS_DB_REPORTER_TMP.reportercountry_free_text,LS_DB_REPORTER.reportercountry_de_ml = LS_DB_REPORTER_TMP.reportercountry_de_ml,LS_DB_REPORTER.reportercountry_as_coded = LS_DB_REPORTER_TMP.reportercountry_as_coded,LS_DB_REPORTER.reportercountry = LS_DB_REPORTER_TMP.reportercountry,LS_DB_REPORTER.reportercity_nf = LS_DB_REPORTER_TMP.reportercity_nf,LS_DB_REPORTER.reportercity_lang = LS_DB_REPORTER_TMP.reportercity_lang,LS_DB_REPORTER.reportercity_as_coded = LS_DB_REPORTER_TMP.reportercity_as_coded,LS_DB_REPORTER.reportercity = LS_DB_REPORTER_TMP.reportercity,LS_DB_REPORTER.reporter_type_nf = LS_DB_REPORTER_TMP.reporter_type_nf,LS_DB_REPORTER.reporter_type_de_ml = LS_DB_REPORTER_TMP.reporter_type_de_ml,LS_DB_REPORTER.reporter_type = LS_DB_REPORTER_TMP.reporter_type,LS_DB_REPORTER.reporter_tel_extension = LS_DB_REPORTER_TMP.reporter_tel_extension,LS_DB_REPORTER.reporter_tel_country_code = LS_DB_REPORTER_TMP.reporter_tel_country_code,LS_DB_REPORTER.reporter_sent_to_fda_de_ml = LS_DB_REPORTER_TMP.reporter_sent_to_fda_de_ml,LS_DB_REPORTER.reporter_sent_to_fda = LS_DB_REPORTER_TMP.reporter_sent_to_fda,LS_DB_REPORTER.reporter_or_contact_de_ml = LS_DB_REPORTER_TMP.reporter_or_contact_de_ml,LS_DB_REPORTER.reporter_or_contact = LS_DB_REPORTER_TMP.reporter_or_contact,LS_DB_REPORTER.reporter_is_patient = LS_DB_REPORTER_TMP.reporter_is_patient,LS_DB_REPORTER.reporter_informed_to_mfr_de_ml = LS_DB_REPORTER_TMP.reporter_informed_to_mfr_de_ml,LS_DB_REPORTER.reporter_informed_to_mfr = LS_DB_REPORTER_TMP.reporter_informed_to_mfr,LS_DB_REPORTER.reporter_fullname = LS_DB_REPORTER_TMP.reporter_fullname,LS_DB_REPORTER.reported_to_user_facility = LS_DB_REPORTER_TMP.reported_to_user_facility,LS_DB_REPORTER.reported_to_distributor = LS_DB_REPORTER_TMP.reported_to_distributor,LS_DB_REPORTER.record_id = LS_DB_REPORTER_TMP.record_id,LS_DB_REPORTER.qualification_nf = LS_DB_REPORTER_TMP.qualification_nf,LS_DB_REPORTER.qualification_de_ml = LS_DB_REPORTER_TMP.qualification_de_ml,LS_DB_REPORTER.qualification = LS_DB_REPORTER_TMP.qualification,LS_DB_REPORTER.primarysource_lang = LS_DB_REPORTER_TMP.primarysource_lang,LS_DB_REPORTER.primary_uiidnumber = LS_DB_REPORTER_TMP.primary_uiidnumber,LS_DB_REPORTER.primary_src_lit_ref_num = LS_DB_REPORTER_TMP.primary_src_lit_ref_num,LS_DB_REPORTER.primary_source_type = LS_DB_REPORTER_TMP.primary_source_type,LS_DB_REPORTER.primary_reporter_de_ml = LS_DB_REPORTER_TMP.primary_reporter_de_ml,LS_DB_REPORTER.primary_reporter = LS_DB_REPORTER_TMP.primary_reporter,LS_DB_REPORTER.primary_rep_reg_purpose = LS_DB_REPORTER_TMP.primary_rep_reg_purpose,LS_DB_REPORTER.prescriber_code_de_ml = LS_DB_REPORTER_TMP.prescriber_code_de_ml,LS_DB_REPORTER.prescriber_code = LS_DB_REPORTER_TMP.prescriber_code,LS_DB_REPORTER.po_box = LS_DB_REPORTER_TMP.po_box,LS_DB_REPORTER.physicion_code = LS_DB_REPORTER_TMP.physicion_code,LS_DB_REPORTER.phone_nf = LS_DB_REPORTER_TMP.phone_nf,LS_DB_REPORTER.phone = LS_DB_REPORTER_TMP.phone,LS_DB_REPORTER.pharmacy_name = LS_DB_REPORTER_TMP.pharmacy_name,LS_DB_REPORTER.person_type = LS_DB_REPORTER_TMP.person_type,LS_DB_REPORTER.otherhcp_kr_de_ml = LS_DB_REPORTER_TMP.otherhcp_kr_de_ml,LS_DB_REPORTER.otherhcp_kr = LS_DB_REPORTER_TMP.otherhcp_kr,LS_DB_REPORTER.other_specify = LS_DB_REPORTER_TMP.other_specify,LS_DB_REPORTER.occupation_sf = LS_DB_REPORTER_TMP.occupation_sf,LS_DB_REPORTER.occupation_ocr = LS_DB_REPORTER_TMP.occupation_ocr,LS_DB_REPORTER.occupation_de_ml = LS_DB_REPORTER_TMP.occupation_de_ml,LS_DB_REPORTER.occupation = LS_DB_REPORTER_TMP.occupation,LS_DB_REPORTER.observestudytype_de_ml = LS_DB_REPORTER_TMP.observestudytype_de_ml,LS_DB_REPORTER.observestudytype = LS_DB_REPORTER_TMP.observestudytype,LS_DB_REPORTER.literaturereference_nf = LS_DB_REPORTER_TMP.literaturereference_nf,LS_DB_REPORTER.literaturereference_lang = LS_DB_REPORTER_TMP.literaturereference_lang,LS_DB_REPORTER.literaturereference = LS_DB_REPORTER_TMP.literaturereference,LS_DB_REPORTER.is_primaryreporter_from_portal = LS_DB_REPORTER_TMP.is_primaryreporter_from_portal,LS_DB_REPORTER.is_primary_source = LS_DB_REPORTER_TMP.is_primary_source,LS_DB_REPORTER.is_person_investigator = LS_DB_REPORTER_TMP.is_person_investigator,LS_DB_REPORTER.is_person_author = LS_DB_REPORTER_TMP.is_person_author,LS_DB_REPORTER.is_pat_reporter = LS_DB_REPORTER_TMP.is_pat_reporter,LS_DB_REPORTER.is_investigator = LS_DB_REPORTER_TMP.is_investigator,LS_DB_REPORTER.is_health_prof_de_ml = LS_DB_REPORTER_TMP.is_health_prof_de_ml,LS_DB_REPORTER.is_health_prof = LS_DB_REPORTER_TMP.is_health_prof,LS_DB_REPORTER.is_coordinator_de_ml = LS_DB_REPORTER_TMP.is_coordinator_de_ml,LS_DB_REPORTER.is_coordinator = LS_DB_REPORTER_TMP.is_coordinator,LS_DB_REPORTER.institution = LS_DB_REPORTER_TMP.institution,LS_DB_REPORTER.inq_rec_id = LS_DB_REPORTER_TMP.inq_rec_id,LS_DB_REPORTER.initial_report_sent_to_fda = LS_DB_REPORTER_TMP.initial_report_sent_to_fda,LS_DB_REPORTER.identity_disclosed = LS_DB_REPORTER_TMP.identity_disclosed,LS_DB_REPORTER.hospital_name = LS_DB_REPORTER_TMP.hospital_name,LS_DB_REPORTER.hospital_code = LS_DB_REPORTER_TMP.hospital_code,LS_DB_REPORTER.hospital_address = LS_DB_REPORTER_TMP.hospital_address,LS_DB_REPORTER.hospital = LS_DB_REPORTER_TMP.hospital,LS_DB_REPORTER.health_prof_manual_check = LS_DB_REPORTER_TMP.health_prof_manual_check,LS_DB_REPORTER.hcp_classification_kr_de_ml = LS_DB_REPORTER_TMP.hcp_classification_kr_de_ml,LS_DB_REPORTER.hcp_classification_kr = LS_DB_REPORTER_TMP.hcp_classification_kr,LS_DB_REPORTER.fl_health_prof_type = LS_DB_REPORTER_TMP.fl_health_prof_type,LS_DB_REPORTER.fk_asr_rec_id = LS_DB_REPORTER_TMP.fk_asr_rec_id,LS_DB_REPORTER.fk_ap_recid_investigator = LS_DB_REPORTER_TMP.fk_ap_recid_investigator,LS_DB_REPORTER.feinumber_as_coded = LS_DB_REPORTER_TMP.feinumber_as_coded,LS_DB_REPORTER.feinumber = LS_DB_REPORTER_TMP.feinumber,LS_DB_REPORTER.fei_number_checked = LS_DB_REPORTER_TMP.fei_number_checked,LS_DB_REPORTER.fax = LS_DB_REPORTER_TMP.fax,LS_DB_REPORTER.ext_clob_fld = LS_DB_REPORTER_TMP.ext_clob_fld,LS_DB_REPORTER.entity_updated = LS_DB_REPORTER_TMP.entity_updated,LS_DB_REPORTER.email_nf = LS_DB_REPORTER_TMP.email_nf,LS_DB_REPORTER.email = LS_DB_REPORTER_TMP.email,LS_DB_REPORTER.e2b_r3_regulatory_purpose_de_ml = LS_DB_REPORTER_TMP.e2b_r3_regulatory_purpose_de_ml,LS_DB_REPORTER.e2b_r3_regulatory_purpose = LS_DB_REPORTER_TMP.e2b_r3_regulatory_purpose,LS_DB_REPORTER.duns_number_checked = LS_DB_REPORTER_TMP.duns_number_checked,LS_DB_REPORTER.duns_number_as_coded = LS_DB_REPORTER_TMP.duns_number_as_coded,LS_DB_REPORTER.duns_number = LS_DB_REPORTER_TMP.duns_number,LS_DB_REPORTER.donotreportname_de_ml = LS_DB_REPORTER_TMP.donotreportname_de_ml,LS_DB_REPORTER.donotreportname = LS_DB_REPORTER_TMP.donotreportname,LS_DB_REPORTER.doctor_given_name = LS_DB_REPORTER_TMP.doctor_given_name,LS_DB_REPORTER.doctor_given_family_name = LS_DB_REPORTER_TMP.doctor_given_family_name,LS_DB_REPORTER.do_not_report_name_code = LS_DB_REPORTER_TMP.do_not_report_name_code,LS_DB_REPORTER.designation = LS_DB_REPORTER_TMP.designation,LS_DB_REPORTER.date_modified = LS_DB_REPORTER_TMP.date_modified,LS_DB_REPORTER.date_created = LS_DB_REPORTER_TMP.date_created,LS_DB_REPORTER.dataprivacy_present = LS_DB_REPORTER_TMP.dataprivacy_present,LS_DB_REPORTER.data_encrypted = LS_DB_REPORTER_TMP.data_encrypted,LS_DB_REPORTER.cra_name = LS_DB_REPORTER_TMP.cra_name,LS_DB_REPORTER.county_nf = LS_DB_REPORTER_TMP.county_nf,LS_DB_REPORTER.county = LS_DB_REPORTER_TMP.county,LS_DB_REPORTER.contact_rec_id = LS_DB_REPORTER_TMP.contact_rec_id,LS_DB_REPORTER.contact_reason = LS_DB_REPORTER_TMP.contact_reason,LS_DB_REPORTER.contact_person = LS_DB_REPORTER_TMP.contact_person,LS_DB_REPORTER.contact_method = LS_DB_REPORTER_TMP.contact_method,LS_DB_REPORTER.consent_to_contact_reporter = LS_DB_REPORTER_TMP.consent_to_contact_reporter,LS_DB_REPORTER.consent_to_contact_hcp = LS_DB_REPORTER_TMP.consent_to_contact_hcp,LS_DB_REPORTER.consent_contact_de_ml = LS_DB_REPORTER_TMP.consent_contact_de_ml,LS_DB_REPORTER.consent_contact = LS_DB_REPORTER_TMP.consent_contact,LS_DB_REPORTER.confidential = LS_DB_REPORTER_TMP.confidential,LS_DB_REPORTER.comp_rec_id = LS_DB_REPORTER_TMP.comp_rec_id,LS_DB_REPORTER.ari_rec_id = LS_DB_REPORTER_TMP.ari_rec_id,LS_DB_REPORTER.also_reported_to = LS_DB_REPORTER_TMP.also_reported_to,LS_DB_REPORTER.address2 = LS_DB_REPORTER_TMP.address2,LS_DB_REPORTER.address1 = LS_DB_REPORTER_TMP.address1,
LS_DB_REPORTER.PROCESSING_DT = LS_DB_REPORTER_TMP.PROCESSING_DT ,
LS_DB_REPORTER.receipt_id     =LS_DB_REPORTER_TMP.receipt_id        ,
LS_DB_REPORTER.case_no        =LS_DB_REPORTER_TMP.case_no           ,
LS_DB_REPORTER.case_version   =LS_DB_REPORTER_TMP.case_version      ,
LS_DB_REPORTER.version_no     =LS_DB_REPORTER_TMP.version_no        ,
LS_DB_REPORTER.expiry_date    =LS_DB_REPORTER_TMP.expiry_date       ,
LS_DB_REPORTER.load_ts        =LS_DB_REPORTER_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP 
WHERE 	LS_DB_REPORTER.INTEGRATION_ID = LS_DB_REPORTER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_REPORTER_TMP.PROCESSING_DT = LS_DB_REPORTER.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,zip_code,
version,
uuid,
user_modified,
user_created,
type_de_ml,
type,
theraputicarea,
telephone,
survay_name,
studyname_lang,
studyname,
street_address,
spr_id,
sponsorstudynumb_lang,
sponsorstudynumb,
specialty_sf,
specialty_de_ml,
specialty,
spanish_state_de_ml,
spanish_state_code_nf,
spanish_state_code,
spanish_state,
site,
root_form,
requester_category,
reportertype_ft,
reportertitle_sf,
reportertitle_nf,
reportertitle_lang,
reportertitle_ft,
reportertitle,
reporterstreet_nf,
reporterstreet_lang,
reporterstreet_as_coded2,
reporterstreet_as_coded1,
reporterstreet2,
reporterstreet1,
reporterstreet,
reporterstate_nf,
reporterstate_lang,
reporterstate_as_coded,
reporterstate,
reporterpstcode_as_coded,
reporterpostcode_nf,
reporterpostcode_lang,
reporterpostcode,
reporterorganization_nf,
reporterorganization_lang,
reporterorganization,
reportermiddlename_nf,
reportermiddlename_lang,
reportermiddlename,
reportergivenname_nf,
reportergivenname_lang,
reportergivenname,
reporterfamilyname_nf,
reporterfamilyname_lang,
reporterfamilyname,
reporterdeptartment_nf,
reporterdeptartment_lang,
reporterdeptartment,
reportercountry_ocr,
reportercountry_nf,
reportercountry_lang,
reportercountry_free_text,
reportercountry_de_ml,
reportercountry_as_coded,
reportercountry,
reportercity_nf,
reportercity_lang,
reportercity_as_coded,
reportercity,
reporter_type_nf,
reporter_type_de_ml,
reporter_type,
reporter_tel_extension,
reporter_tel_country_code,
reporter_sent_to_fda_de_ml,
reporter_sent_to_fda,
reporter_or_contact_de_ml,
reporter_or_contact,
reporter_is_patient,
reporter_informed_to_mfr_de_ml,
reporter_informed_to_mfr,
reporter_fullname,
reported_to_user_facility,
reported_to_distributor,
record_id,
qualification_nf,
qualification_de_ml,
qualification,
primarysource_lang,
primary_uiidnumber,
primary_src_lit_ref_num,
primary_source_type,
primary_reporter_de_ml,
primary_reporter,
primary_rep_reg_purpose,
prescriber_code_de_ml,
prescriber_code,
po_box,
physicion_code,
phone_nf,
phone,
pharmacy_name,
person_type,
otherhcp_kr_de_ml,
otherhcp_kr,
other_specify,
occupation_sf,
occupation_ocr,
occupation_de_ml,
occupation,
observestudytype_de_ml,
observestudytype,
literaturereference_nf,
literaturereference_lang,
literaturereference,
is_primaryreporter_from_portal,
is_primary_source,
is_person_investigator,
is_person_author,
is_pat_reporter,
is_investigator,
is_health_prof_de_ml,
is_health_prof,
is_coordinator_de_ml,
is_coordinator,
institution,
inq_rec_id,
initial_report_sent_to_fda,
identity_disclosed,
hospital_name,
hospital_code,
hospital_address,
hospital,
health_prof_manual_check,
hcp_classification_kr_de_ml,
hcp_classification_kr,
fl_health_prof_type,
fk_asr_rec_id,
fk_ap_recid_investigator,
feinumber_as_coded,
feinumber,
fei_number_checked,
fax,
ext_clob_fld,
entity_updated,
email_nf,
email,
e2b_r3_regulatory_purpose_de_ml,
e2b_r3_regulatory_purpose,
duns_number_checked,
duns_number_as_coded,
duns_number,
donotreportname_de_ml,
donotreportname,
doctor_given_name,
doctor_given_family_name,
do_not_report_name_code,
designation,
date_modified,
date_created,
dataprivacy_present,
data_encrypted,
cra_name,
county_nf,
county,
contact_rec_id,
contact_reason,
contact_person,
contact_method,
consent_to_contact_reporter,
consent_to_contact_hcp,
consent_contact_de_ml,
consent_contact,
confidential,
comp_rec_id,
ari_rec_id,
also_reported_to,
address2,
address1)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,zip_code,
version,
uuid,
user_modified,
user_created,
type_de_ml,
type,
theraputicarea,
telephone,
survay_name,
studyname_lang,
studyname,
street_address,
spr_id,
sponsorstudynumb_lang,
sponsorstudynumb,
specialty_sf,
specialty_de_ml,
specialty,
spanish_state_de_ml,
spanish_state_code_nf,
spanish_state_code,
spanish_state,
site,
root_form,
requester_category,
reportertype_ft,
reportertitle_sf,
reportertitle_nf,
reportertitle_lang,
reportertitle_ft,
reportertitle,
reporterstreet_nf,
reporterstreet_lang,
reporterstreet_as_coded2,
reporterstreet_as_coded1,
reporterstreet2,
reporterstreet1,
reporterstreet,
reporterstate_nf,
reporterstate_lang,
reporterstate_as_coded,
reporterstate,
reporterpstcode_as_coded,
reporterpostcode_nf,
reporterpostcode_lang,
reporterpostcode,
reporterorganization_nf,
reporterorganization_lang,
reporterorganization,
reportermiddlename_nf,
reportermiddlename_lang,
reportermiddlename,
reportergivenname_nf,
reportergivenname_lang,
reportergivenname,
reporterfamilyname_nf,
reporterfamilyname_lang,
reporterfamilyname,
reporterdeptartment_nf,
reporterdeptartment_lang,
reporterdeptartment,
reportercountry_ocr,
reportercountry_nf,
reportercountry_lang,
reportercountry_free_text,
reportercountry_de_ml,
reportercountry_as_coded,
reportercountry,
reportercity_nf,
reportercity_lang,
reportercity_as_coded,
reportercity,
reporter_type_nf,
reporter_type_de_ml,
reporter_type,
reporter_tel_extension,
reporter_tel_country_code,
reporter_sent_to_fda_de_ml,
reporter_sent_to_fda,
reporter_or_contact_de_ml,
reporter_or_contact,
reporter_is_patient,
reporter_informed_to_mfr_de_ml,
reporter_informed_to_mfr,
reporter_fullname,
reported_to_user_facility,
reported_to_distributor,
record_id,
qualification_nf,
qualification_de_ml,
qualification,
primarysource_lang,
primary_uiidnumber,
primary_src_lit_ref_num,
primary_source_type,
primary_reporter_de_ml,
primary_reporter,
primary_rep_reg_purpose,
prescriber_code_de_ml,
prescriber_code,
po_box,
physicion_code,
phone_nf,
phone,
pharmacy_name,
person_type,
otherhcp_kr_de_ml,
otherhcp_kr,
other_specify,
occupation_sf,
occupation_ocr,
occupation_de_ml,
occupation,
observestudytype_de_ml,
observestudytype,
literaturereference_nf,
literaturereference_lang,
literaturereference,
is_primaryreporter_from_portal,
is_primary_source,
is_person_investigator,
is_person_author,
is_pat_reporter,
is_investigator,
is_health_prof_de_ml,
is_health_prof,
is_coordinator_de_ml,
is_coordinator,
institution,
inq_rec_id,
initial_report_sent_to_fda,
identity_disclosed,
hospital_name,
hospital_code,
hospital_address,
hospital,
health_prof_manual_check,
hcp_classification_kr_de_ml,
hcp_classification_kr,
fl_health_prof_type,
fk_asr_rec_id,
fk_ap_recid_investigator,
feinumber_as_coded,
feinumber,
fei_number_checked,
fax,
ext_clob_fld,
entity_updated,
email_nf,
email,
e2b_r3_regulatory_purpose_de_ml,
e2b_r3_regulatory_purpose,
duns_number_checked,
duns_number_as_coded,
duns_number,
donotreportname_de_ml,
donotreportname,
doctor_given_name,
doctor_given_family_name,
do_not_report_name_code,
designation,
date_modified,
date_created,
dataprivacy_present,
data_encrypted,
cra_name,
county_nf,
county,
contact_rec_id,
contact_reason,
contact_person,
contact_method,
consent_to_contact_reporter,
consent_to_contact_hcp,
consent_contact_de_ml,
consent_contact,
confidential,
comp_rec_id,
ari_rec_id,
also_reported_to,
address2,
address1
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER TGT)
                                                                                ; 
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTER_DELETION_TMP  WHERE TABLE_NAME='lsmv_primarysource')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP 
WHERE 	TO_DATE(LS_DB_REPORTER.PROCESSING_DT) < TO_DATE(LS_DB_REPORTER_TMP.PROCESSING_DT)
AND LS_DB_REPORTER.INTEGRATION_ID = LS_DB_REPORTER_TMP.INTEGRATION_ID
AND LS_DB_REPORTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTER_DELETION_TMP  WHERE TABLE_NAME='lsmv_primarysource')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REPORTER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_REPORTER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REPORTER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_REPORTER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
