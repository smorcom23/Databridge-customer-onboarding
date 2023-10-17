
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_DRUG()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG'),
	'LSRA','Case','LS_DB_DRUG',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_DELETION_TMP  As select RECORD_ID,'lsmv_drug' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','10020','10046','10069','1013','1015','1015','1017','1018','1020','1024','1027','11','17','21','4','5015','7077','7077','7077','7077','7077','8008','8008','815','9029','9070','9120','9164','9165','9861','9941','9970')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_drug WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_drug WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_drug_SUBSET AS 
(
select * from 
    (SELECT  
    access_to_otc  access_to_otc,account_record_id  account_record_id,actiondrug  actiondrug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='21' AND CD=CAST(actiondrug AS VARCHAR(100)) )actiondrug_de_ml , active_ing_record_id  active_ing_record_id,active_substance_coded_flag  active_substance_coded_flag,address  address,address_as_coded  address_as_coded,adr_trade_rec_id  adr_trade_rec_id,ari_rec_id  ari_rec_id,arisg_approval_id  arisg_approval_id,arisg_product_id  arisg_product_id,arisg_trade_id  arisg_trade_id,bio_father_drug  bio_father_drug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(bio_father_drug AS VARCHAR(100)) )bio_father_drug_de_ml , biosimilar  biosimilar,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(biosimilar AS VARCHAR(100)) )biosimilar_de_ml , blinded_product_rec_id  blinded_product_rec_id,causality_required  causality_required,caused_by_lo_effect  caused_by_lo_effect,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(caused_by_lo_effect AS VARCHAR(100)) )caused_by_lo_effect_de_ml , city  city,city_as_coded  city_as_coded,classify_product  classify_product,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9970' AND CD=CAST(classify_product AS VARCHAR(100)) )classify_product_de_ml , clinical_drug_code  clinical_drug_code,clinical_drug_code_jpn  clinical_drug_code_jpn,coded_product  coded_product,coding_class  coding_class,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9120' AND CD=CAST(coding_class AS VARCHAR(100)) )coding_class_de_ml , coding_comments  coding_comments,coding_type  coding_type,common_device_code  common_device_code,comp_rec_id  comp_rec_id,company_product  company_product,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(company_product AS VARCHAR(100)) )company_product_de_ml , company_unit_code  company_unit_code,component_type  component_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10046' AND CD=CAST(component_type AS VARCHAR(100)) )component_type_de_ml , concomitant_product  concomitant_product,container_name  container_name,copied_blinded  copied_blinded,country  country,country_as_coded  country_as_coded,date_created  date_created,date_discontinued  date_discontinued,date_modified  date_modified,dechallenge  dechallenge,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1027' AND CD=CAST(dechallenge AS VARCHAR(100)) )dechallenge_de_ml , default_product  default_product,device_avail_eval  device_avail_eval,device_name  device_name,device_reprocessed_and_reused  device_reprocessed_and_reused,device_reprocessor_unit  device_reprocessor_unit,device_returned_to_mfr  device_returned_to_mfr,did_ae_abate  did_ae_abate,drlprefterm  drlprefterm,drug_admin_at_ae  drug_admin_at_ae,drug_auth_as_coded  drug_auth_as_coded,drug_coded_flag  drug_coded_flag,drug_hash_code  drug_hash_code,drug_json_text  drug_json_text,drug_lang  drug_lang,drug_mfg_coded_flag  drug_mfg_coded_flag,drug_sequence_id  drug_sequence_id,drug_type  drug_type,drug_vigilancetype  drug_vigilancetype,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10020' AND CD=CAST(drug_vigilancetype AS VARCHAR(100)) )drug_vigilancetype_de_ml , drugadditional  drugadditional,drugadditional_code  drugadditional_code,drugadditional_lang  drugadditional_lang,drugadditionalinfo  drugadditionalinfo,drugadministrationroute  drugadministrationroute,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1020' AND CD=CAST(drugadministrationroute AS VARCHAR(100)) )drugadministrationroute_de_ml , drugadministrationroute_lang  drugadministrationroute_lang,drugadministrationroute_ocr  drugadministrationroute_ocr,drugauthnumbascoded  drugauthnumbascoded,drugauthorizationcountry  drugauthorizationcountry,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(drugauthorizationcountry AS VARCHAR(100)) )drugauthorizationcountry_de_ml , drugauthorizationcountry_lang  drugauthorizationcountry_lang,drugauthorizationholder  drugauthorizationholder,drugauthorizationholder_as_coded  drugauthorizationholder_as_coded,drugauthorizationholder_lang  drugauthorizationholder_lang,drugauthorizationnumb  drugauthorizationnumb,drugauthorizationnumb_lang  drugauthorizationnumb_lang,drugauthorizationtype  drugauthorizationtype,drugbatchnumb  drugbatchnumb,drugbatchnumb_lang  drugbatchnumb_lang,drugcharacterization  drugcharacterization,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1013' AND CD=CAST(drugcharacterization AS VARCHAR(100)) )drugcharacterization_de_ml , drugcumulativedosagenumb  drugcumulativedosagenumb,drugcumulativedosagenumb_lang  drugcumulativedosagenumb_lang,drugcumulativedosageunit  drugcumulativedosageunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1018' AND CD=CAST(drugcumulativedosageunit AS VARCHAR(100)) )drugcumulativedosageunit_de_ml , drugcumulativedosageunit_lang  drugcumulativedosageunit_lang,drugdosageform  drugdosageform,drugdosageform_lang  drugdosageform_lang,drugdosagetext  drugdosagetext,drugdosagetext_lang  drugdosagetext_lang,drugenddate  drugenddate,drugenddatefmt  drugenddatefmt,drugindication  drugindication,drugindication_lang  drugindication_lang,drugindicationmeddraver  drugindicationmeddraver,drugindicationmeddraver_lang  drugindicationmeddraver_lang,drugintdosageunitdef  drugintdosageunitdef,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9029' AND CD=CAST(drugintdosageunitdef AS VARCHAR(100)) )drugintdosageunitdef_de_ml , drugintdosageunitdef_lang  drugintdosageunitdef_lang,drugintdosageunitnumb  drugintdosageunitnumb,drugintdosageunitnumb_lang  drugintdosageunitnumb_lang,druglastperiod  druglastperiod,druglastperiod_lang  druglastperiod_lang,druglastperiodunit  druglastperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(druglastperiodunit AS VARCHAR(100)) )druglastperiodunit_de_ml , drugparadministration  drugparadministration,drugparadministration_lang  drugparadministration_lang,drugrecurreadministration  drugrecurreadministration,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1024' AND CD=CAST(drugrecurreadministration AS VARCHAR(100)) )drugrecurreadministration_de_ml , drugseparatedosagenumb  drugseparatedosagenumb,drugseparatedosagenumb_lang  drugseparatedosagenumb_lang,drugseparatedosageoth  drugseparatedosageoth,drugstartdate  drugstartdate,drugstartdatefmt  drugstartdatefmt,drugstartperiod  drugstartperiod,drugstartperiod_lang  drugstartperiod_lang,drugstartperiodunit  drugstartperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='17' AND CD=CAST(drugstartperiodunit AS VARCHAR(100)) )drugstartperiodunit_de_ml , drugstructuredosagenumb  drugstructuredosagenumb,drugstructuredosagenumb_lang  drugstructuredosagenumb_lang,drugstructuredosageunit  drugstructuredosageunit,drugstructuredosageunit_lang  drugstructuredosageunit_lang,drugtreatdurationunit  drugtreatdurationunit,drugtreatmentduration  drugtreatmentduration,drugtreatmentduration_lang  drugtreatmentduration_lang,duns_number  duns_number,duns_number_as_coded  duns_number_as_coded,duns_number_checked  duns_number_checked,e2b_r3_med_prodid  e2b_r3_med_prodid,e2b_r3_medprodid_datenumber  e2b_r3_medprodid_datenumber,e2b_r3_pharma_prodid  e2b_r3_pharma_prodid,e2b_r3_pharmaprodid_datenumber  e2b_r3_pharmaprodid_datenumber,entity_updated  entity_updated,equivalent_drug  equivalent_drug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(equivalent_drug AS VARCHAR(100)) )equivalent_drug_de_ml , exclude_for_qde  exclude_for_qde,expiration_date  expiration_date,expiration_date_fmt  expiration_date_fmt,expiration_date_text  expiration_date_text,ext_clob_fld  ext_clob_fld,fei_number  fei_number,fei_number_as_coded  fei_number_as_coded,fei_number_checked  fei_number_checked,fk_apat_rec_id  fk_apat_rec_id,food_cosm_related  food_cosm_related,form_name  form_name,form_strength  form_strength,form_strength_unit  form_strength_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9070' AND CD=CAST(form_strength_unit AS VARCHAR(100)) )form_strength_unit_de_ml , frequency  frequency,generic  generic,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(generic AS VARCHAR(100)) )generic_de_ml , generic_name  generic_name,generic_name_as_coded  generic_name_as_coded,generic_name_cn  generic_name_cn,inq_rec_id  inq_rec_id,intended_use_name  intended_use_name,internal_drug_code  internal_drug_code,invented_name  invented_name,investigational_prod_blinded  investigational_prod_blinded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(investigational_prod_blinded AS VARCHAR(100)) )investigational_prod_blinded_de_ml , is_added_from_krjp  is_added_from_krjp,is_coded  is_coded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(is_coded AS VARCHAR(100)) )is_coded_de_ml , is_lot_batch_num_enabled  is_lot_batch_num_enabled,is_pqc_event  is_pqc_event,is_whood_ing  is_whood_ing,jpn_drug_code  jpn_drug_code,kdd_code  kdd_code,mah_as_coded  mah_as_coded,manufacturer  manufacturer,manufacturer_group  manufacturer_group,manufacturer_name  manufacturer_name,medicinal_prod_id  medicinal_prod_id,medicinal_prod_nf  medicinal_prod_nf,medicinalproduct  medicinalproduct,medicinalproduct_lang  medicinalproduct_lang,most_suspect_product  most_suspect_product,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(most_suspect_product AS VARCHAR(100)) )most_suspect_product_de_ml , ndc  ndc,ndc_no  ndc_no,ndc_text  ndc_text,new_drug_classifn  new_drug_classifn,not_reportable_jpn  not_reportable_jpn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(not_reportable_jpn AS VARCHAR(100)) )not_reportable_jpn_de_ml , obtaindrugcountry  obtaindrugcountry,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(obtaindrugcountry AS VARCHAR(100)) )obtaindrugcountry_de_ml , obtaindrugcountry_lang  obtaindrugcountry_lang,otc_risk_classfn  otc_risk_classfn,paternal_exposure  paternal_exposure,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(paternal_exposure AS VARCHAR(100)) )paternal_exposure_de_ml , pmda_medicine_name  pmda_medicine_name,pqc_number  pqc_number,pre_1938  pre_1938,prefered_code  prefered_code,prefered_product_description  prefered_product_description,prev_rec_id  prev_rec_id,previous_rec_id  previous_rec_id,primary_product_from_reporter  primary_product_from_reporter,primary_suspect_ctdrug  primary_suspect_ctdrug,primary_suspect_drug  primary_suspect_drug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(primary_suspect_drug AS VARCHAR(100)) )primary_suspect_drug_de_ml , prior_use  prior_use,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(prior_use AS VARCHAR(100)) )prior_use_de_ml , prod_addition_info  prod_addition_info,prod_pqms  prod_pqms,prod_ref_comments  prod_ref_comments,prod_ref_required  prod_ref_required,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(prod_ref_required AS VARCHAR(100)) )prod_ref_required_de_ml , prodasrepted  prodasrepted,product_active_ingredient  product_active_ingredient,product_available_flag  product_available_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(product_available_flag AS VARCHAR(100)) )product_available_flag_de_ml , product_availfor_eval  product_availfor_eval,product_compounded  product_compounded,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(product_compounded AS VARCHAR(100)) )product_compounded_de_ml , product_family  product_family,product_formulation  product_formulation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='815' AND CD=CAST(product_formulation AS VARCHAR(100)) )product_formulation_de_ml , product_group  product_group,product_groupname_id  product_groupname_id,product_level  product_level,product_name  product_name,product_name_as_reported_cn  product_name_as_reported_cn,product_over_counter  product_over_counter,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9861' AND CD=CAST(product_over_counter AS VARCHAR(100)) )product_over_counter_de_ml , product_record_id  product_record_id,product_returned_date  product_returned_date,product_type  product_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5015' AND CD=CAST(product_type AS VARCHAR(100)) )product_type_de_ml , productseqgen  productseqgen,protocol_product_type  protocol_product_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8008' AND CD=CAST(protocol_product_type AS VARCHAR(100)) )protocol_product_type_de_ml , pv_product_id  pv_product_id,rank_order  rank_order,reactgestationperiod  reactgestationperiod,reactgestationperiod_lang  reactgestationperiod_lang,reactgestationperiodunit  reactgestationperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='11' AND CD=CAST(reactgestationperiodunit AS VARCHAR(100)) )reactgestationperiodunit_de_ml , record_id  record_id,related_device_cn  related_device_cn,repo_drug_id  repo_drug_id,reportedmedicinalproduct  reportedmedicinalproduct,returned_to_mfr_date  returned_to_mfr_date,returned_to_mfr_date_fmt  returned_to_mfr_date_fmt,scientific_name  scientific_name,serial_numb  serial_numb,specialized_product_category  specialized_product_category,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9164' AND CD=CAST(specialized_product_category AS VARCHAR(100)) )specialized_product_category_de_ml , spr_id  spr_id,state  state,state_as_coded  state_as_coded,street_addrs1  street_addrs1,street_addrs2  street_addrs2,street_addrs_as_coded1  street_addrs_as_coded1,street_addrs_as_coded2  street_addrs_as_coded2,strength_name  strength_name,strength_ocr  strength_ocr,study_product_type  study_product_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8008' AND CD=CAST(study_product_type AS VARCHAR(100)) )study_product_type_de_ml , study_relation_desc  study_relation_desc,synonym_drug  synonym_drug,tolerated  tolerated,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(tolerated AS VARCHAR(100)) )tolerated_de_ml , total_number_of_lots  total_number_of_lots,trademark_name  trademark_name,un_blinded  un_blinded,user_created  user_created,user_modified  user_modified,uuid  uuid,vaccination_military_flag  vaccination_military_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(vaccination_military_flag AS VARCHAR(100)) )vaccination_military_flag_de_ml , vaccination_military_flag_nf  vaccination_military_flag_nf,vaccine_given  vaccine_given,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9165' AND CD=CAST(vaccine_given AS VARCHAR(100)) )vaccine_given_de_ml , vaccine_type  vaccine_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10069' AND CD=CAST(vaccine_type AS VARCHAR(100)) )vaccine_type_de_ml , vaccinetype_nf  vaccinetype_nf,vaccinetype_sf  vaccinetype_sf,version  version,was_prd_restarted  was_prd_restarted,was_prd_stopped  was_prd_stopped,whodd_decode  whodd_decode,whoddcode  whoddcode,whoddcodeflag  whoddcodeflag,whoddversion  whoddversion,ziporpostalcode  ziporpostalcode,ziporpostalcode_as_coded  ziporpostalcode_as_coded,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_drug
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_SUBSET.ziporpostalcode_as_coded  ,lsmv_drug_SUBSET.ziporpostalcode  ,lsmv_drug_SUBSET.whoddversion  ,lsmv_drug_SUBSET.whoddcodeflag  ,lsmv_drug_SUBSET.whoddcode  ,lsmv_drug_SUBSET.whodd_decode  ,lsmv_drug_SUBSET.was_prd_stopped  ,lsmv_drug_SUBSET.was_prd_restarted  ,lsmv_drug_SUBSET.version  ,lsmv_drug_SUBSET.vaccinetype_sf  ,lsmv_drug_SUBSET.vaccinetype_nf  ,lsmv_drug_SUBSET.vaccine_type_de_ml  ,lsmv_drug_SUBSET.vaccine_type  ,lsmv_drug_SUBSET.vaccine_given_de_ml  ,lsmv_drug_SUBSET.vaccine_given  ,lsmv_drug_SUBSET.vaccination_military_flag_nf  ,lsmv_drug_SUBSET.vaccination_military_flag_de_ml  ,lsmv_drug_SUBSET.vaccination_military_flag  ,lsmv_drug_SUBSET.uuid  ,lsmv_drug_SUBSET.user_modified  ,lsmv_drug_SUBSET.user_created  ,lsmv_drug_SUBSET.un_blinded  ,lsmv_drug_SUBSET.trademark_name  ,lsmv_drug_SUBSET.total_number_of_lots  ,lsmv_drug_SUBSET.tolerated_de_ml  ,lsmv_drug_SUBSET.tolerated  ,lsmv_drug_SUBSET.synonym_drug  ,lsmv_drug_SUBSET.study_relation_desc  ,lsmv_drug_SUBSET.study_product_type_de_ml  ,lsmv_drug_SUBSET.study_product_type  ,lsmv_drug_SUBSET.strength_ocr  ,lsmv_drug_SUBSET.strength_name  ,lsmv_drug_SUBSET.street_addrs_as_coded2  ,lsmv_drug_SUBSET.street_addrs_as_coded1  ,lsmv_drug_SUBSET.street_addrs2  ,lsmv_drug_SUBSET.street_addrs1  ,lsmv_drug_SUBSET.state_as_coded  ,lsmv_drug_SUBSET.state  ,lsmv_drug_SUBSET.spr_id  ,lsmv_drug_SUBSET.specialized_product_category_de_ml  ,lsmv_drug_SUBSET.specialized_product_category  ,lsmv_drug_SUBSET.serial_numb  ,lsmv_drug_SUBSET.scientific_name  ,lsmv_drug_SUBSET.returned_to_mfr_date_fmt  ,lsmv_drug_SUBSET.returned_to_mfr_date  ,lsmv_drug_SUBSET.reportedmedicinalproduct  ,lsmv_drug_SUBSET.repo_drug_id  ,lsmv_drug_SUBSET.related_device_cn  ,lsmv_drug_SUBSET.record_id  ,lsmv_drug_SUBSET.reactgestationperiodunit_de_ml  ,lsmv_drug_SUBSET.reactgestationperiodunit  ,lsmv_drug_SUBSET.reactgestationperiod_lang  ,lsmv_drug_SUBSET.reactgestationperiod  ,lsmv_drug_SUBSET.rank_order  ,lsmv_drug_SUBSET.pv_product_id  ,lsmv_drug_SUBSET.protocol_product_type_de_ml  ,lsmv_drug_SUBSET.protocol_product_type  ,lsmv_drug_SUBSET.productseqgen  ,lsmv_drug_SUBSET.product_type_de_ml  ,lsmv_drug_SUBSET.product_type  ,lsmv_drug_SUBSET.product_returned_date  ,lsmv_drug_SUBSET.product_record_id  ,lsmv_drug_SUBSET.product_over_counter_de_ml  ,lsmv_drug_SUBSET.product_over_counter  ,lsmv_drug_SUBSET.product_name_as_reported_cn  ,lsmv_drug_SUBSET.product_name  ,lsmv_drug_SUBSET.product_level  ,lsmv_drug_SUBSET.product_groupname_id  ,lsmv_drug_SUBSET.product_group  ,lsmv_drug_SUBSET.product_formulation_de_ml  ,lsmv_drug_SUBSET.product_formulation  ,lsmv_drug_SUBSET.product_family  ,lsmv_drug_SUBSET.product_compounded_de_ml  ,lsmv_drug_SUBSET.product_compounded  ,lsmv_drug_SUBSET.product_availfor_eval  ,lsmv_drug_SUBSET.product_available_flag_de_ml  ,lsmv_drug_SUBSET.product_available_flag  ,lsmv_drug_SUBSET.product_active_ingredient  ,lsmv_drug_SUBSET.prodasrepted  ,lsmv_drug_SUBSET.prod_ref_required_de_ml  ,lsmv_drug_SUBSET.prod_ref_required  ,lsmv_drug_SUBSET.prod_ref_comments  ,lsmv_drug_SUBSET.prod_pqms  ,lsmv_drug_SUBSET.prod_addition_info  ,lsmv_drug_SUBSET.prior_use_de_ml  ,lsmv_drug_SUBSET.prior_use  ,lsmv_drug_SUBSET.primary_suspect_drug_de_ml  ,lsmv_drug_SUBSET.primary_suspect_drug  ,lsmv_drug_SUBSET.primary_suspect_ctdrug  ,lsmv_drug_SUBSET.primary_product_from_reporter  ,lsmv_drug_SUBSET.previous_rec_id  ,lsmv_drug_SUBSET.prev_rec_id  ,lsmv_drug_SUBSET.prefered_product_description  ,lsmv_drug_SUBSET.prefered_code  ,lsmv_drug_SUBSET.pre_1938  ,lsmv_drug_SUBSET.pqc_number  ,lsmv_drug_SUBSET.pmda_medicine_name  ,lsmv_drug_SUBSET.paternal_exposure_de_ml  ,lsmv_drug_SUBSET.paternal_exposure  ,lsmv_drug_SUBSET.otc_risk_classfn  ,lsmv_drug_SUBSET.obtaindrugcountry_lang  ,lsmv_drug_SUBSET.obtaindrugcountry_de_ml  ,lsmv_drug_SUBSET.obtaindrugcountry  ,lsmv_drug_SUBSET.not_reportable_jpn_de_ml  ,lsmv_drug_SUBSET.not_reportable_jpn  ,lsmv_drug_SUBSET.new_drug_classifn  ,lsmv_drug_SUBSET.ndc_text  ,lsmv_drug_SUBSET.ndc_no  ,lsmv_drug_SUBSET.ndc  ,lsmv_drug_SUBSET.most_suspect_product_de_ml  ,lsmv_drug_SUBSET.most_suspect_product  ,lsmv_drug_SUBSET.medicinalproduct_lang  ,lsmv_drug_SUBSET.medicinalproduct  ,lsmv_drug_SUBSET.medicinal_prod_nf  ,lsmv_drug_SUBSET.medicinal_prod_id  ,lsmv_drug_SUBSET.manufacturer_name  ,lsmv_drug_SUBSET.manufacturer_group  ,lsmv_drug_SUBSET.manufacturer  ,lsmv_drug_SUBSET.mah_as_coded  ,lsmv_drug_SUBSET.kdd_code  ,lsmv_drug_SUBSET.jpn_drug_code  ,lsmv_drug_SUBSET.is_whood_ing  ,lsmv_drug_SUBSET.is_pqc_event  ,lsmv_drug_SUBSET.is_lot_batch_num_enabled  ,lsmv_drug_SUBSET.is_coded_de_ml  ,lsmv_drug_SUBSET.is_coded  ,lsmv_drug_SUBSET.is_added_from_krjp  ,lsmv_drug_SUBSET.investigational_prod_blinded_de_ml  ,lsmv_drug_SUBSET.investigational_prod_blinded  ,lsmv_drug_SUBSET.invented_name  ,lsmv_drug_SUBSET.internal_drug_code  ,lsmv_drug_SUBSET.intended_use_name  ,lsmv_drug_SUBSET.inq_rec_id  ,lsmv_drug_SUBSET.generic_name_cn  ,lsmv_drug_SUBSET.generic_name_as_coded  ,lsmv_drug_SUBSET.generic_name  ,lsmv_drug_SUBSET.generic_de_ml  ,lsmv_drug_SUBSET.generic  ,lsmv_drug_SUBSET.frequency  ,lsmv_drug_SUBSET.form_strength_unit_de_ml  ,lsmv_drug_SUBSET.form_strength_unit  ,lsmv_drug_SUBSET.form_strength  ,lsmv_drug_SUBSET.form_name  ,lsmv_drug_SUBSET.food_cosm_related  ,lsmv_drug_SUBSET.fk_apat_rec_id  ,lsmv_drug_SUBSET.fei_number_checked  ,lsmv_drug_SUBSET.fei_number_as_coded  ,lsmv_drug_SUBSET.fei_number  ,lsmv_drug_SUBSET.ext_clob_fld  ,lsmv_drug_SUBSET.expiration_date_text  ,lsmv_drug_SUBSET.expiration_date_fmt  ,lsmv_drug_SUBSET.expiration_date  ,lsmv_drug_SUBSET.exclude_for_qde  ,lsmv_drug_SUBSET.equivalent_drug_de_ml  ,lsmv_drug_SUBSET.equivalent_drug  ,lsmv_drug_SUBSET.entity_updated  ,lsmv_drug_SUBSET.e2b_r3_pharmaprodid_datenumber  ,lsmv_drug_SUBSET.e2b_r3_pharma_prodid  ,lsmv_drug_SUBSET.e2b_r3_medprodid_datenumber  ,lsmv_drug_SUBSET.e2b_r3_med_prodid  ,lsmv_drug_SUBSET.duns_number_checked  ,lsmv_drug_SUBSET.duns_number_as_coded  ,lsmv_drug_SUBSET.duns_number  ,lsmv_drug_SUBSET.drugtreatmentduration_lang  ,lsmv_drug_SUBSET.drugtreatmentduration  ,lsmv_drug_SUBSET.drugtreatdurationunit  ,lsmv_drug_SUBSET.drugstructuredosageunit_lang  ,lsmv_drug_SUBSET.drugstructuredosageunit  ,lsmv_drug_SUBSET.drugstructuredosagenumb_lang  ,lsmv_drug_SUBSET.drugstructuredosagenumb  ,lsmv_drug_SUBSET.drugstartperiodunit_de_ml  ,lsmv_drug_SUBSET.drugstartperiodunit  ,lsmv_drug_SUBSET.drugstartperiod_lang  ,lsmv_drug_SUBSET.drugstartperiod  ,lsmv_drug_SUBSET.drugstartdatefmt  ,lsmv_drug_SUBSET.drugstartdate  ,lsmv_drug_SUBSET.drugseparatedosageoth  ,lsmv_drug_SUBSET.drugseparatedosagenumb_lang  ,lsmv_drug_SUBSET.drugseparatedosagenumb  ,lsmv_drug_SUBSET.drugrecurreadministration_de_ml  ,lsmv_drug_SUBSET.drugrecurreadministration  ,lsmv_drug_SUBSET.drugparadministration_lang  ,lsmv_drug_SUBSET.drugparadministration  ,lsmv_drug_SUBSET.druglastperiodunit_de_ml  ,lsmv_drug_SUBSET.druglastperiodunit  ,lsmv_drug_SUBSET.druglastperiod_lang  ,lsmv_drug_SUBSET.druglastperiod  ,lsmv_drug_SUBSET.drugintdosageunitnumb_lang  ,lsmv_drug_SUBSET.drugintdosageunitnumb  ,lsmv_drug_SUBSET.drugintdosageunitdef_lang  ,lsmv_drug_SUBSET.drugintdosageunitdef_de_ml  ,lsmv_drug_SUBSET.drugintdosageunitdef  ,lsmv_drug_SUBSET.drugindicationmeddraver_lang  ,lsmv_drug_SUBSET.drugindicationmeddraver  ,lsmv_drug_SUBSET.drugindication_lang  ,lsmv_drug_SUBSET.drugindication  ,lsmv_drug_SUBSET.drugenddatefmt  ,lsmv_drug_SUBSET.drugenddate  ,lsmv_drug_SUBSET.drugdosagetext_lang  ,lsmv_drug_SUBSET.drugdosagetext  ,lsmv_drug_SUBSET.drugdosageform_lang  ,lsmv_drug_SUBSET.drugdosageform  ,lsmv_drug_SUBSET.drugcumulativedosageunit_lang  ,lsmv_drug_SUBSET.drugcumulativedosageunit_de_ml  ,lsmv_drug_SUBSET.drugcumulativedosageunit  ,lsmv_drug_SUBSET.drugcumulativedosagenumb_lang  ,lsmv_drug_SUBSET.drugcumulativedosagenumb  ,lsmv_drug_SUBSET.drugcharacterization_de_ml  ,lsmv_drug_SUBSET.drugcharacterization  ,lsmv_drug_SUBSET.drugbatchnumb_lang  ,lsmv_drug_SUBSET.drugbatchnumb  ,lsmv_drug_SUBSET.drugauthorizationtype  ,lsmv_drug_SUBSET.drugauthorizationnumb_lang  ,lsmv_drug_SUBSET.drugauthorizationnumb  ,lsmv_drug_SUBSET.drugauthorizationholder_lang  ,lsmv_drug_SUBSET.drugauthorizationholder_as_coded  ,lsmv_drug_SUBSET.drugauthorizationholder  ,lsmv_drug_SUBSET.drugauthorizationcountry_lang  ,lsmv_drug_SUBSET.drugauthorizationcountry_de_ml  ,lsmv_drug_SUBSET.drugauthorizationcountry  ,lsmv_drug_SUBSET.drugauthnumbascoded  ,lsmv_drug_SUBSET.drugadministrationroute_ocr  ,lsmv_drug_SUBSET.drugadministrationroute_lang  ,lsmv_drug_SUBSET.drugadministrationroute_de_ml  ,lsmv_drug_SUBSET.drugadministrationroute  ,lsmv_drug_SUBSET.drugadditionalinfo  ,lsmv_drug_SUBSET.drugadditional_lang  ,lsmv_drug_SUBSET.drugadditional_code  ,lsmv_drug_SUBSET.drugadditional  ,lsmv_drug_SUBSET.drug_vigilancetype_de_ml  ,lsmv_drug_SUBSET.drug_vigilancetype  ,lsmv_drug_SUBSET.drug_type  ,lsmv_drug_SUBSET.drug_sequence_id  ,lsmv_drug_SUBSET.drug_mfg_coded_flag  ,lsmv_drug_SUBSET.drug_lang  ,lsmv_drug_SUBSET.drug_json_text  ,lsmv_drug_SUBSET.drug_hash_code  ,lsmv_drug_SUBSET.drug_coded_flag  ,lsmv_drug_SUBSET.drug_auth_as_coded  ,lsmv_drug_SUBSET.drug_admin_at_ae  ,lsmv_drug_SUBSET.drlprefterm  ,lsmv_drug_SUBSET.did_ae_abate  ,lsmv_drug_SUBSET.device_returned_to_mfr  ,lsmv_drug_SUBSET.device_reprocessor_unit  ,lsmv_drug_SUBSET.device_reprocessed_and_reused  ,lsmv_drug_SUBSET.device_name  ,lsmv_drug_SUBSET.device_avail_eval  ,lsmv_drug_SUBSET.default_product  ,lsmv_drug_SUBSET.dechallenge_de_ml  ,lsmv_drug_SUBSET.dechallenge  ,lsmv_drug_SUBSET.date_modified  ,lsmv_drug_SUBSET.date_discontinued  ,lsmv_drug_SUBSET.date_created  ,lsmv_drug_SUBSET.country_as_coded  ,lsmv_drug_SUBSET.country  ,lsmv_drug_SUBSET.copied_blinded  ,lsmv_drug_SUBSET.container_name  ,lsmv_drug_SUBSET.concomitant_product  ,lsmv_drug_SUBSET.component_type_de_ml  ,lsmv_drug_SUBSET.component_type  ,lsmv_drug_SUBSET.company_unit_code  ,lsmv_drug_SUBSET.company_product_de_ml  ,lsmv_drug_SUBSET.company_product  ,lsmv_drug_SUBSET.comp_rec_id  ,lsmv_drug_SUBSET.common_device_code  ,lsmv_drug_SUBSET.coding_type  ,lsmv_drug_SUBSET.coding_comments  ,lsmv_drug_SUBSET.coding_class_de_ml  ,lsmv_drug_SUBSET.coding_class  ,lsmv_drug_SUBSET.coded_product  ,lsmv_drug_SUBSET.clinical_drug_code_jpn  ,lsmv_drug_SUBSET.clinical_drug_code  ,lsmv_drug_SUBSET.classify_product_de_ml  ,lsmv_drug_SUBSET.classify_product  ,lsmv_drug_SUBSET.city_as_coded  ,lsmv_drug_SUBSET.city  ,lsmv_drug_SUBSET.caused_by_lo_effect_de_ml  ,lsmv_drug_SUBSET.caused_by_lo_effect  ,lsmv_drug_SUBSET.causality_required  ,lsmv_drug_SUBSET.blinded_product_rec_id  ,lsmv_drug_SUBSET.biosimilar_de_ml  ,lsmv_drug_SUBSET.biosimilar  ,lsmv_drug_SUBSET.bio_father_drug_de_ml  ,lsmv_drug_SUBSET.bio_father_drug  ,lsmv_drug_SUBSET.arisg_trade_id  ,lsmv_drug_SUBSET.arisg_product_id  ,lsmv_drug_SUBSET.arisg_approval_id  ,lsmv_drug_SUBSET.ari_rec_id  ,lsmv_drug_SUBSET.adr_trade_rec_id  ,lsmv_drug_SUBSET.address_as_coded  ,lsmv_drug_SUBSET.address  ,lsmv_drug_SUBSET.active_substance_coded_flag  ,lsmv_drug_SUBSET.active_ing_record_id  ,lsmv_drug_SUBSET.actiondrug_de_ml  ,lsmv_drug_SUBSET.actiondrug  ,lsmv_drug_SUBSET.account_record_id  ,lsmv_drug_SUBSET.access_to_otc ,CONCAT(NVL(lsmv_drug_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP)
where target_table_name='LS_DB_DRUG'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG   
SET LS_DB_DRUG.ziporpostalcode_as_coded = LS_DB_DRUG_TMP.ziporpostalcode_as_coded,LS_DB_DRUG.ziporpostalcode = LS_DB_DRUG_TMP.ziporpostalcode,LS_DB_DRUG.whoddversion = LS_DB_DRUG_TMP.whoddversion,LS_DB_DRUG.whoddcodeflag = LS_DB_DRUG_TMP.whoddcodeflag,LS_DB_DRUG.whoddcode = LS_DB_DRUG_TMP.whoddcode,LS_DB_DRUG.whodd_decode = LS_DB_DRUG_TMP.whodd_decode,LS_DB_DRUG.was_prd_stopped = LS_DB_DRUG_TMP.was_prd_stopped,LS_DB_DRUG.was_prd_restarted = LS_DB_DRUG_TMP.was_prd_restarted,LS_DB_DRUG.version = LS_DB_DRUG_TMP.version,LS_DB_DRUG.vaccinetype_sf = LS_DB_DRUG_TMP.vaccinetype_sf,LS_DB_DRUG.vaccinetype_nf = LS_DB_DRUG_TMP.vaccinetype_nf,LS_DB_DRUG.vaccine_type_de_ml = LS_DB_DRUG_TMP.vaccine_type_de_ml,LS_DB_DRUG.vaccine_type = LS_DB_DRUG_TMP.vaccine_type,LS_DB_DRUG.vaccine_given_de_ml = LS_DB_DRUG_TMP.vaccine_given_de_ml,LS_DB_DRUG.vaccine_given = LS_DB_DRUG_TMP.vaccine_given,LS_DB_DRUG.vaccination_military_flag_nf = LS_DB_DRUG_TMP.vaccination_military_flag_nf,LS_DB_DRUG.vaccination_military_flag_de_ml = LS_DB_DRUG_TMP.vaccination_military_flag_de_ml,LS_DB_DRUG.vaccination_military_flag = LS_DB_DRUG_TMP.vaccination_military_flag,LS_DB_DRUG.uuid = LS_DB_DRUG_TMP.uuid,LS_DB_DRUG.user_modified = LS_DB_DRUG_TMP.user_modified,LS_DB_DRUG.user_created = LS_DB_DRUG_TMP.user_created,LS_DB_DRUG.un_blinded = LS_DB_DRUG_TMP.un_blinded,LS_DB_DRUG.trademark_name = LS_DB_DRUG_TMP.trademark_name,LS_DB_DRUG.total_number_of_lots = LS_DB_DRUG_TMP.total_number_of_lots,LS_DB_DRUG.tolerated_de_ml = LS_DB_DRUG_TMP.tolerated_de_ml,LS_DB_DRUG.tolerated = LS_DB_DRUG_TMP.tolerated,LS_DB_DRUG.synonym_drug = LS_DB_DRUG_TMP.synonym_drug,LS_DB_DRUG.study_relation_desc = LS_DB_DRUG_TMP.study_relation_desc,LS_DB_DRUG.study_product_type_de_ml = LS_DB_DRUG_TMP.study_product_type_de_ml,LS_DB_DRUG.study_product_type = LS_DB_DRUG_TMP.study_product_type,LS_DB_DRUG.strength_ocr = LS_DB_DRUG_TMP.strength_ocr,LS_DB_DRUG.strength_name = LS_DB_DRUG_TMP.strength_name,LS_DB_DRUG.street_addrs_as_coded2 = LS_DB_DRUG_TMP.street_addrs_as_coded2,LS_DB_DRUG.street_addrs_as_coded1 = LS_DB_DRUG_TMP.street_addrs_as_coded1,LS_DB_DRUG.street_addrs2 = LS_DB_DRUG_TMP.street_addrs2,LS_DB_DRUG.street_addrs1 = LS_DB_DRUG_TMP.street_addrs1,LS_DB_DRUG.state_as_coded = LS_DB_DRUG_TMP.state_as_coded,LS_DB_DRUG.state = LS_DB_DRUG_TMP.state,LS_DB_DRUG.spr_id = LS_DB_DRUG_TMP.spr_id,LS_DB_DRUG.specialized_product_category_de_ml = LS_DB_DRUG_TMP.specialized_product_category_de_ml,LS_DB_DRUG.specialized_product_category = LS_DB_DRUG_TMP.specialized_product_category,LS_DB_DRUG.serial_numb = LS_DB_DRUG_TMP.serial_numb,LS_DB_DRUG.scientific_name = LS_DB_DRUG_TMP.scientific_name,LS_DB_DRUG.returned_to_mfr_date_fmt = LS_DB_DRUG_TMP.returned_to_mfr_date_fmt,LS_DB_DRUG.returned_to_mfr_date = LS_DB_DRUG_TMP.returned_to_mfr_date,LS_DB_DRUG.reportedmedicinalproduct = LS_DB_DRUG_TMP.reportedmedicinalproduct,LS_DB_DRUG.repo_drug_id = LS_DB_DRUG_TMP.repo_drug_id,LS_DB_DRUG.related_device_cn = LS_DB_DRUG_TMP.related_device_cn,LS_DB_DRUG.record_id = LS_DB_DRUG_TMP.record_id,LS_DB_DRUG.reactgestationperiodunit_de_ml = LS_DB_DRUG_TMP.reactgestationperiodunit_de_ml,LS_DB_DRUG.reactgestationperiodunit = LS_DB_DRUG_TMP.reactgestationperiodunit,LS_DB_DRUG.reactgestationperiod_lang = LS_DB_DRUG_TMP.reactgestationperiod_lang,LS_DB_DRUG.reactgestationperiod = LS_DB_DRUG_TMP.reactgestationperiod,LS_DB_DRUG.rank_order = LS_DB_DRUG_TMP.rank_order,LS_DB_DRUG.pv_product_id = LS_DB_DRUG_TMP.pv_product_id,LS_DB_DRUG.protocol_product_type_de_ml = LS_DB_DRUG_TMP.protocol_product_type_de_ml,LS_DB_DRUG.protocol_product_type = LS_DB_DRUG_TMP.protocol_product_type,LS_DB_DRUG.productseqgen = LS_DB_DRUG_TMP.productseqgen,LS_DB_DRUG.product_type_de_ml = LS_DB_DRUG_TMP.product_type_de_ml,LS_DB_DRUG.product_type = LS_DB_DRUG_TMP.product_type,LS_DB_DRUG.product_returned_date = LS_DB_DRUG_TMP.product_returned_date,LS_DB_DRUG.product_record_id = LS_DB_DRUG_TMP.product_record_id,LS_DB_DRUG.product_over_counter_de_ml = LS_DB_DRUG_TMP.product_over_counter_de_ml,LS_DB_DRUG.product_over_counter = LS_DB_DRUG_TMP.product_over_counter,LS_DB_DRUG.product_name_as_reported_cn = LS_DB_DRUG_TMP.product_name_as_reported_cn,LS_DB_DRUG.product_name = LS_DB_DRUG_TMP.product_name,LS_DB_DRUG.product_level = LS_DB_DRUG_TMP.product_level,LS_DB_DRUG.product_groupname_id = LS_DB_DRUG_TMP.product_groupname_id,LS_DB_DRUG.product_group = LS_DB_DRUG_TMP.product_group,LS_DB_DRUG.product_formulation_de_ml = LS_DB_DRUG_TMP.product_formulation_de_ml,LS_DB_DRUG.product_formulation = LS_DB_DRUG_TMP.product_formulation,LS_DB_DRUG.product_family = LS_DB_DRUG_TMP.product_family,LS_DB_DRUG.product_compounded_de_ml = LS_DB_DRUG_TMP.product_compounded_de_ml,LS_DB_DRUG.product_compounded = LS_DB_DRUG_TMP.product_compounded,LS_DB_DRUG.product_availfor_eval = LS_DB_DRUG_TMP.product_availfor_eval,LS_DB_DRUG.product_available_flag_de_ml = LS_DB_DRUG_TMP.product_available_flag_de_ml,LS_DB_DRUG.product_available_flag = LS_DB_DRUG_TMP.product_available_flag,LS_DB_DRUG.product_active_ingredient = LS_DB_DRUG_TMP.product_active_ingredient,LS_DB_DRUG.prodasrepted = LS_DB_DRUG_TMP.prodasrepted,LS_DB_DRUG.prod_ref_required_de_ml = LS_DB_DRUG_TMP.prod_ref_required_de_ml,LS_DB_DRUG.prod_ref_required = LS_DB_DRUG_TMP.prod_ref_required,LS_DB_DRUG.prod_ref_comments = LS_DB_DRUG_TMP.prod_ref_comments,LS_DB_DRUG.prod_pqms = LS_DB_DRUG_TMP.prod_pqms,LS_DB_DRUG.prod_addition_info = LS_DB_DRUG_TMP.prod_addition_info,LS_DB_DRUG.prior_use_de_ml = LS_DB_DRUG_TMP.prior_use_de_ml,LS_DB_DRUG.prior_use = LS_DB_DRUG_TMP.prior_use,LS_DB_DRUG.primary_suspect_drug_de_ml = LS_DB_DRUG_TMP.primary_suspect_drug_de_ml,LS_DB_DRUG.primary_suspect_drug = LS_DB_DRUG_TMP.primary_suspect_drug,LS_DB_DRUG.primary_suspect_ctdrug = LS_DB_DRUG_TMP.primary_suspect_ctdrug,LS_DB_DRUG.primary_product_from_reporter = LS_DB_DRUG_TMP.primary_product_from_reporter,LS_DB_DRUG.previous_rec_id = LS_DB_DRUG_TMP.previous_rec_id,LS_DB_DRUG.prev_rec_id = LS_DB_DRUG_TMP.prev_rec_id,LS_DB_DRUG.prefered_product_description = LS_DB_DRUG_TMP.prefered_product_description,LS_DB_DRUG.prefered_code = LS_DB_DRUG_TMP.prefered_code,LS_DB_DRUG.pre_1938 = LS_DB_DRUG_TMP.pre_1938,LS_DB_DRUG.pqc_number = LS_DB_DRUG_TMP.pqc_number,LS_DB_DRUG.pmda_medicine_name = LS_DB_DRUG_TMP.pmda_medicine_name,LS_DB_DRUG.paternal_exposure_de_ml = LS_DB_DRUG_TMP.paternal_exposure_de_ml,LS_DB_DRUG.paternal_exposure = LS_DB_DRUG_TMP.paternal_exposure,LS_DB_DRUG.otc_risk_classfn = LS_DB_DRUG_TMP.otc_risk_classfn,LS_DB_DRUG.obtaindrugcountry_lang = LS_DB_DRUG_TMP.obtaindrugcountry_lang,LS_DB_DRUG.obtaindrugcountry_de_ml = LS_DB_DRUG_TMP.obtaindrugcountry_de_ml,LS_DB_DRUG.obtaindrugcountry = LS_DB_DRUG_TMP.obtaindrugcountry,LS_DB_DRUG.not_reportable_jpn_de_ml = LS_DB_DRUG_TMP.not_reportable_jpn_de_ml,LS_DB_DRUG.not_reportable_jpn = LS_DB_DRUG_TMP.not_reportable_jpn,LS_DB_DRUG.new_drug_classifn = LS_DB_DRUG_TMP.new_drug_classifn,LS_DB_DRUG.ndc_text = LS_DB_DRUG_TMP.ndc_text,LS_DB_DRUG.ndc_no = LS_DB_DRUG_TMP.ndc_no,LS_DB_DRUG.ndc = LS_DB_DRUG_TMP.ndc,LS_DB_DRUG.most_suspect_product_de_ml = LS_DB_DRUG_TMP.most_suspect_product_de_ml,LS_DB_DRUG.most_suspect_product = LS_DB_DRUG_TMP.most_suspect_product,LS_DB_DRUG.medicinalproduct_lang = LS_DB_DRUG_TMP.medicinalproduct_lang,LS_DB_DRUG.medicinalproduct = LS_DB_DRUG_TMP.medicinalproduct,LS_DB_DRUG.medicinal_prod_nf = LS_DB_DRUG_TMP.medicinal_prod_nf,LS_DB_DRUG.medicinal_prod_id = LS_DB_DRUG_TMP.medicinal_prod_id,LS_DB_DRUG.manufacturer_name = LS_DB_DRUG_TMP.manufacturer_name,LS_DB_DRUG.manufacturer_group = LS_DB_DRUG_TMP.manufacturer_group,LS_DB_DRUG.manufacturer = LS_DB_DRUG_TMP.manufacturer,LS_DB_DRUG.mah_as_coded = LS_DB_DRUG_TMP.mah_as_coded,LS_DB_DRUG.kdd_code = LS_DB_DRUG_TMP.kdd_code,LS_DB_DRUG.jpn_drug_code = LS_DB_DRUG_TMP.jpn_drug_code,LS_DB_DRUG.is_whood_ing = LS_DB_DRUG_TMP.is_whood_ing,LS_DB_DRUG.is_pqc_event = LS_DB_DRUG_TMP.is_pqc_event,LS_DB_DRUG.is_lot_batch_num_enabled = LS_DB_DRUG_TMP.is_lot_batch_num_enabled,LS_DB_DRUG.is_coded_de_ml = LS_DB_DRUG_TMP.is_coded_de_ml,LS_DB_DRUG.is_coded = LS_DB_DRUG_TMP.is_coded,LS_DB_DRUG.is_added_from_krjp = LS_DB_DRUG_TMP.is_added_from_krjp,LS_DB_DRUG.investigational_prod_blinded_de_ml = LS_DB_DRUG_TMP.investigational_prod_blinded_de_ml,LS_DB_DRUG.investigational_prod_blinded = LS_DB_DRUG_TMP.investigational_prod_blinded,LS_DB_DRUG.invented_name = LS_DB_DRUG_TMP.invented_name,LS_DB_DRUG.internal_drug_code = LS_DB_DRUG_TMP.internal_drug_code,LS_DB_DRUG.intended_use_name = LS_DB_DRUG_TMP.intended_use_name,LS_DB_DRUG.inq_rec_id = LS_DB_DRUG_TMP.inq_rec_id,LS_DB_DRUG.generic_name_cn = LS_DB_DRUG_TMP.generic_name_cn,LS_DB_DRUG.generic_name_as_coded = LS_DB_DRUG_TMP.generic_name_as_coded,LS_DB_DRUG.generic_name = LS_DB_DRUG_TMP.generic_name,LS_DB_DRUG.generic_de_ml = LS_DB_DRUG_TMP.generic_de_ml,LS_DB_DRUG.generic = LS_DB_DRUG_TMP.generic,LS_DB_DRUG.frequency = LS_DB_DRUG_TMP.frequency,LS_DB_DRUG.form_strength_unit_de_ml = LS_DB_DRUG_TMP.form_strength_unit_de_ml,LS_DB_DRUG.form_strength_unit = LS_DB_DRUG_TMP.form_strength_unit,LS_DB_DRUG.form_strength = LS_DB_DRUG_TMP.form_strength,LS_DB_DRUG.form_name = LS_DB_DRUG_TMP.form_name,LS_DB_DRUG.food_cosm_related = LS_DB_DRUG_TMP.food_cosm_related,LS_DB_DRUG.fk_apat_rec_id = LS_DB_DRUG_TMP.fk_apat_rec_id,LS_DB_DRUG.fei_number_checked = LS_DB_DRUG_TMP.fei_number_checked,LS_DB_DRUG.fei_number_as_coded = LS_DB_DRUG_TMP.fei_number_as_coded,LS_DB_DRUG.fei_number = LS_DB_DRUG_TMP.fei_number,LS_DB_DRUG.ext_clob_fld = LS_DB_DRUG_TMP.ext_clob_fld,LS_DB_DRUG.expiration_date_text = LS_DB_DRUG_TMP.expiration_date_text,LS_DB_DRUG.expiration_date_fmt = LS_DB_DRUG_TMP.expiration_date_fmt,LS_DB_DRUG.expiration_date = LS_DB_DRUG_TMP.expiration_date,LS_DB_DRUG.exclude_for_qde = LS_DB_DRUG_TMP.exclude_for_qde,LS_DB_DRUG.equivalent_drug_de_ml = LS_DB_DRUG_TMP.equivalent_drug_de_ml,LS_DB_DRUG.equivalent_drug = LS_DB_DRUG_TMP.equivalent_drug,LS_DB_DRUG.entity_updated = LS_DB_DRUG_TMP.entity_updated,LS_DB_DRUG.e2b_r3_pharmaprodid_datenumber = LS_DB_DRUG_TMP.e2b_r3_pharmaprodid_datenumber,LS_DB_DRUG.e2b_r3_pharma_prodid = LS_DB_DRUG_TMP.e2b_r3_pharma_prodid,LS_DB_DRUG.e2b_r3_medprodid_datenumber = LS_DB_DRUG_TMP.e2b_r3_medprodid_datenumber,LS_DB_DRUG.e2b_r3_med_prodid = LS_DB_DRUG_TMP.e2b_r3_med_prodid,LS_DB_DRUG.duns_number_checked = LS_DB_DRUG_TMP.duns_number_checked,LS_DB_DRUG.duns_number_as_coded = LS_DB_DRUG_TMP.duns_number_as_coded,LS_DB_DRUG.duns_number = LS_DB_DRUG_TMP.duns_number,LS_DB_DRUG.drugtreatmentduration_lang = LS_DB_DRUG_TMP.drugtreatmentduration_lang,LS_DB_DRUG.drugtreatmentduration = LS_DB_DRUG_TMP.drugtreatmentduration,LS_DB_DRUG.drugtreatdurationunit = LS_DB_DRUG_TMP.drugtreatdurationunit,LS_DB_DRUG.drugstructuredosageunit_lang = LS_DB_DRUG_TMP.drugstructuredosageunit_lang,LS_DB_DRUG.drugstructuredosageunit = LS_DB_DRUG_TMP.drugstructuredosageunit,LS_DB_DRUG.drugstructuredosagenumb_lang = LS_DB_DRUG_TMP.drugstructuredosagenumb_lang,LS_DB_DRUG.drugstructuredosagenumb = LS_DB_DRUG_TMP.drugstructuredosagenumb,LS_DB_DRUG.drugstartperiodunit_de_ml = LS_DB_DRUG_TMP.drugstartperiodunit_de_ml,LS_DB_DRUG.drugstartperiodunit = LS_DB_DRUG_TMP.drugstartperiodunit,LS_DB_DRUG.drugstartperiod_lang = LS_DB_DRUG_TMP.drugstartperiod_lang,LS_DB_DRUG.drugstartperiod = LS_DB_DRUG_TMP.drugstartperiod,LS_DB_DRUG.drugstartdatefmt = LS_DB_DRUG_TMP.drugstartdatefmt,LS_DB_DRUG.drugstartdate = LS_DB_DRUG_TMP.drugstartdate,LS_DB_DRUG.drugseparatedosageoth = LS_DB_DRUG_TMP.drugseparatedosageoth,LS_DB_DRUG.drugseparatedosagenumb_lang = LS_DB_DRUG_TMP.drugseparatedosagenumb_lang,LS_DB_DRUG.drugseparatedosagenumb = LS_DB_DRUG_TMP.drugseparatedosagenumb,LS_DB_DRUG.drugrecurreadministration_de_ml = LS_DB_DRUG_TMP.drugrecurreadministration_de_ml,LS_DB_DRUG.drugrecurreadministration = LS_DB_DRUG_TMP.drugrecurreadministration,LS_DB_DRUG.drugparadministration_lang = LS_DB_DRUG_TMP.drugparadministration_lang,LS_DB_DRUG.drugparadministration = LS_DB_DRUG_TMP.drugparadministration,LS_DB_DRUG.druglastperiodunit_de_ml = LS_DB_DRUG_TMP.druglastperiodunit_de_ml,LS_DB_DRUG.druglastperiodunit = LS_DB_DRUG_TMP.druglastperiodunit,LS_DB_DRUG.druglastperiod_lang = LS_DB_DRUG_TMP.druglastperiod_lang,LS_DB_DRUG.druglastperiod = LS_DB_DRUG_TMP.druglastperiod,LS_DB_DRUG.drugintdosageunitnumb_lang = LS_DB_DRUG_TMP.drugintdosageunitnumb_lang,LS_DB_DRUG.drugintdosageunitnumb = LS_DB_DRUG_TMP.drugintdosageunitnumb,LS_DB_DRUG.drugintdosageunitdef_lang = LS_DB_DRUG_TMP.drugintdosageunitdef_lang,LS_DB_DRUG.drugintdosageunitdef_de_ml = LS_DB_DRUG_TMP.drugintdosageunitdef_de_ml,LS_DB_DRUG.drugintdosageunitdef = LS_DB_DRUG_TMP.drugintdosageunitdef,LS_DB_DRUG.drugindicationmeddraver_lang = LS_DB_DRUG_TMP.drugindicationmeddraver_lang,LS_DB_DRUG.drugindicationmeddraver = LS_DB_DRUG_TMP.drugindicationmeddraver,LS_DB_DRUG.drugindication_lang = LS_DB_DRUG_TMP.drugindication_lang,LS_DB_DRUG.drugindication = LS_DB_DRUG_TMP.drugindication,LS_DB_DRUG.drugenddatefmt = LS_DB_DRUG_TMP.drugenddatefmt,LS_DB_DRUG.drugenddate = LS_DB_DRUG_TMP.drugenddate,LS_DB_DRUG.drugdosagetext_lang = LS_DB_DRUG_TMP.drugdosagetext_lang,LS_DB_DRUG.drugdosagetext = LS_DB_DRUG_TMP.drugdosagetext,LS_DB_DRUG.drugdosageform_lang = LS_DB_DRUG_TMP.drugdosageform_lang,LS_DB_DRUG.drugdosageform = LS_DB_DRUG_TMP.drugdosageform,LS_DB_DRUG.drugcumulativedosageunit_lang = LS_DB_DRUG_TMP.drugcumulativedosageunit_lang,LS_DB_DRUG.drugcumulativedosageunit_de_ml = LS_DB_DRUG_TMP.drugcumulativedosageunit_de_ml,LS_DB_DRUG.drugcumulativedosageunit = LS_DB_DRUG_TMP.drugcumulativedosageunit,LS_DB_DRUG.drugcumulativedosagenumb_lang = LS_DB_DRUG_TMP.drugcumulativedosagenumb_lang,LS_DB_DRUG.drugcumulativedosagenumb = LS_DB_DRUG_TMP.drugcumulativedosagenumb,LS_DB_DRUG.drugcharacterization_de_ml = LS_DB_DRUG_TMP.drugcharacterization_de_ml,LS_DB_DRUG.drugcharacterization = LS_DB_DRUG_TMP.drugcharacterization,LS_DB_DRUG.drugbatchnumb_lang = LS_DB_DRUG_TMP.drugbatchnumb_lang,LS_DB_DRUG.drugbatchnumb = LS_DB_DRUG_TMP.drugbatchnumb,LS_DB_DRUG.drugauthorizationtype = LS_DB_DRUG_TMP.drugauthorizationtype,LS_DB_DRUG.drugauthorizationnumb_lang = LS_DB_DRUG_TMP.drugauthorizationnumb_lang,LS_DB_DRUG.drugauthorizationnumb = LS_DB_DRUG_TMP.drugauthorizationnumb,LS_DB_DRUG.drugauthorizationholder_lang = LS_DB_DRUG_TMP.drugauthorizationholder_lang,LS_DB_DRUG.drugauthorizationholder_as_coded = LS_DB_DRUG_TMP.drugauthorizationholder_as_coded,LS_DB_DRUG.drugauthorizationholder = LS_DB_DRUG_TMP.drugauthorizationholder,LS_DB_DRUG.drugauthorizationcountry_lang = LS_DB_DRUG_TMP.drugauthorizationcountry_lang,LS_DB_DRUG.drugauthorizationcountry_de_ml = LS_DB_DRUG_TMP.drugauthorizationcountry_de_ml,LS_DB_DRUG.drugauthorizationcountry = LS_DB_DRUG_TMP.drugauthorizationcountry,LS_DB_DRUG.drugauthnumbascoded = LS_DB_DRUG_TMP.drugauthnumbascoded,LS_DB_DRUG.drugadministrationroute_ocr = LS_DB_DRUG_TMP.drugadministrationroute_ocr,LS_DB_DRUG.drugadministrationroute_lang = LS_DB_DRUG_TMP.drugadministrationroute_lang,LS_DB_DRUG.drugadministrationroute_de_ml = LS_DB_DRUG_TMP.drugadministrationroute_de_ml,LS_DB_DRUG.drugadministrationroute = LS_DB_DRUG_TMP.drugadministrationroute,LS_DB_DRUG.drugadditionalinfo = LS_DB_DRUG_TMP.drugadditionalinfo,LS_DB_DRUG.drugadditional_lang = LS_DB_DRUG_TMP.drugadditional_lang,LS_DB_DRUG.drugadditional_code = LS_DB_DRUG_TMP.drugadditional_code,LS_DB_DRUG.drugadditional = LS_DB_DRUG_TMP.drugadditional,LS_DB_DRUG.drug_vigilancetype_de_ml = LS_DB_DRUG_TMP.drug_vigilancetype_de_ml,LS_DB_DRUG.drug_vigilancetype = LS_DB_DRUG_TMP.drug_vigilancetype,LS_DB_DRUG.drug_type = LS_DB_DRUG_TMP.drug_type,LS_DB_DRUG.drug_sequence_id = LS_DB_DRUG_TMP.drug_sequence_id,LS_DB_DRUG.drug_mfg_coded_flag = LS_DB_DRUG_TMP.drug_mfg_coded_flag,LS_DB_DRUG.drug_lang = LS_DB_DRUG_TMP.drug_lang,LS_DB_DRUG.drug_json_text = LS_DB_DRUG_TMP.drug_json_text,LS_DB_DRUG.drug_hash_code = LS_DB_DRUG_TMP.drug_hash_code,LS_DB_DRUG.drug_coded_flag = LS_DB_DRUG_TMP.drug_coded_flag,LS_DB_DRUG.drug_auth_as_coded = LS_DB_DRUG_TMP.drug_auth_as_coded,LS_DB_DRUG.drug_admin_at_ae = LS_DB_DRUG_TMP.drug_admin_at_ae,LS_DB_DRUG.drlprefterm = LS_DB_DRUG_TMP.drlprefterm,LS_DB_DRUG.did_ae_abate = LS_DB_DRUG_TMP.did_ae_abate,LS_DB_DRUG.device_returned_to_mfr = LS_DB_DRUG_TMP.device_returned_to_mfr,LS_DB_DRUG.device_reprocessor_unit = LS_DB_DRUG_TMP.device_reprocessor_unit,LS_DB_DRUG.device_reprocessed_and_reused = LS_DB_DRUG_TMP.device_reprocessed_and_reused,LS_DB_DRUG.device_name = LS_DB_DRUG_TMP.device_name,LS_DB_DRUG.device_avail_eval = LS_DB_DRUG_TMP.device_avail_eval,LS_DB_DRUG.default_product = LS_DB_DRUG_TMP.default_product,LS_DB_DRUG.dechallenge_de_ml = LS_DB_DRUG_TMP.dechallenge_de_ml,LS_DB_DRUG.dechallenge = LS_DB_DRUG_TMP.dechallenge,LS_DB_DRUG.date_modified = LS_DB_DRUG_TMP.date_modified,LS_DB_DRUG.date_discontinued = LS_DB_DRUG_TMP.date_discontinued,LS_DB_DRUG.date_created = LS_DB_DRUG_TMP.date_created,LS_DB_DRUG.country_as_coded = LS_DB_DRUG_TMP.country_as_coded,LS_DB_DRUG.country = LS_DB_DRUG_TMP.country,LS_DB_DRUG.copied_blinded = LS_DB_DRUG_TMP.copied_blinded,LS_DB_DRUG.container_name = LS_DB_DRUG_TMP.container_name,LS_DB_DRUG.concomitant_product = LS_DB_DRUG_TMP.concomitant_product,LS_DB_DRUG.component_type_de_ml = LS_DB_DRUG_TMP.component_type_de_ml,LS_DB_DRUG.component_type = LS_DB_DRUG_TMP.component_type,LS_DB_DRUG.company_unit_code = LS_DB_DRUG_TMP.company_unit_code,LS_DB_DRUG.company_product_de_ml = LS_DB_DRUG_TMP.company_product_de_ml,LS_DB_DRUG.company_product = LS_DB_DRUG_TMP.company_product,LS_DB_DRUG.comp_rec_id = LS_DB_DRUG_TMP.comp_rec_id,LS_DB_DRUG.common_device_code = LS_DB_DRUG_TMP.common_device_code,LS_DB_DRUG.coding_type = LS_DB_DRUG_TMP.coding_type,LS_DB_DRUG.coding_comments = LS_DB_DRUG_TMP.coding_comments,LS_DB_DRUG.coding_class_de_ml = LS_DB_DRUG_TMP.coding_class_de_ml,LS_DB_DRUG.coding_class = LS_DB_DRUG_TMP.coding_class,LS_DB_DRUG.coded_product = LS_DB_DRUG_TMP.coded_product,LS_DB_DRUG.clinical_drug_code_jpn = LS_DB_DRUG_TMP.clinical_drug_code_jpn,LS_DB_DRUG.clinical_drug_code = LS_DB_DRUG_TMP.clinical_drug_code,LS_DB_DRUG.classify_product_de_ml = LS_DB_DRUG_TMP.classify_product_de_ml,LS_DB_DRUG.classify_product = LS_DB_DRUG_TMP.classify_product,LS_DB_DRUG.city_as_coded = LS_DB_DRUG_TMP.city_as_coded,LS_DB_DRUG.city = LS_DB_DRUG_TMP.city,LS_DB_DRUG.caused_by_lo_effect_de_ml = LS_DB_DRUG_TMP.caused_by_lo_effect_de_ml,LS_DB_DRUG.caused_by_lo_effect = LS_DB_DRUG_TMP.caused_by_lo_effect,LS_DB_DRUG.causality_required = LS_DB_DRUG_TMP.causality_required,LS_DB_DRUG.blinded_product_rec_id = LS_DB_DRUG_TMP.blinded_product_rec_id,LS_DB_DRUG.biosimilar_de_ml = LS_DB_DRUG_TMP.biosimilar_de_ml,LS_DB_DRUG.biosimilar = LS_DB_DRUG_TMP.biosimilar,LS_DB_DRUG.bio_father_drug_de_ml = LS_DB_DRUG_TMP.bio_father_drug_de_ml,LS_DB_DRUG.bio_father_drug = LS_DB_DRUG_TMP.bio_father_drug,LS_DB_DRUG.arisg_trade_id = LS_DB_DRUG_TMP.arisg_trade_id,LS_DB_DRUG.arisg_product_id = LS_DB_DRUG_TMP.arisg_product_id,LS_DB_DRUG.arisg_approval_id = LS_DB_DRUG_TMP.arisg_approval_id,LS_DB_DRUG.ari_rec_id = LS_DB_DRUG_TMP.ari_rec_id,LS_DB_DRUG.adr_trade_rec_id = LS_DB_DRUG_TMP.adr_trade_rec_id,LS_DB_DRUG.address_as_coded = LS_DB_DRUG_TMP.address_as_coded,LS_DB_DRUG.address = LS_DB_DRUG_TMP.address,LS_DB_DRUG.active_substance_coded_flag = LS_DB_DRUG_TMP.active_substance_coded_flag,LS_DB_DRUG.active_ing_record_id = LS_DB_DRUG_TMP.active_ing_record_id,LS_DB_DRUG.actiondrug_de_ml = LS_DB_DRUG_TMP.actiondrug_de_ml,LS_DB_DRUG.actiondrug = LS_DB_DRUG_TMP.actiondrug,LS_DB_DRUG.account_record_id = LS_DB_DRUG_TMP.account_record_id,LS_DB_DRUG.access_to_otc = LS_DB_DRUG_TMP.access_to_otc,
LS_DB_DRUG.PROCESSING_DT = LS_DB_DRUG_TMP.PROCESSING_DT ,
LS_DB_DRUG.receipt_id     =LS_DB_DRUG_TMP.receipt_id        ,
LS_DB_DRUG.case_no        =LS_DB_DRUG_TMP.case_no           ,
LS_DB_DRUG.case_version   =LS_DB_DRUG_TMP.case_version      ,
LS_DB_DRUG.version_no     =LS_DB_DRUG_TMP.version_no        ,
LS_DB_DRUG.expiry_date    =LS_DB_DRUG_TMP.expiry_date       ,
LS_DB_DRUG.load_ts        =LS_DB_DRUG_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP 
WHERE 	LS_DB_DRUG.INTEGRATION_ID = LS_DB_DRUG_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_TMP.PROCESSING_DT = LS_DB_DRUG.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,ziporpostalcode_as_coded,
ziporpostalcode,
whoddversion,
whoddcodeflag,
whoddcode,
whodd_decode,
was_prd_stopped,
was_prd_restarted,
version,
vaccinetype_sf,
vaccinetype_nf,
vaccine_type_de_ml,
vaccine_type,
vaccine_given_de_ml,
vaccine_given,
vaccination_military_flag_nf,
vaccination_military_flag_de_ml,
vaccination_military_flag,
uuid,
user_modified,
user_created,
un_blinded,
trademark_name,
total_number_of_lots,
tolerated_de_ml,
tolerated,
synonym_drug,
study_relation_desc,
study_product_type_de_ml,
study_product_type,
strength_ocr,
strength_name,
street_addrs_as_coded2,
street_addrs_as_coded1,
street_addrs2,
street_addrs1,
state_as_coded,
state,
spr_id,
specialized_product_category_de_ml,
specialized_product_category,
serial_numb,
scientific_name,
returned_to_mfr_date_fmt,
returned_to_mfr_date,
reportedmedicinalproduct,
repo_drug_id,
related_device_cn,
record_id,
reactgestationperiodunit_de_ml,
reactgestationperiodunit,
reactgestationperiod_lang,
reactgestationperiod,
rank_order,
pv_product_id,
protocol_product_type_de_ml,
protocol_product_type,
productseqgen,
product_type_de_ml,
product_type,
product_returned_date,
product_record_id,
product_over_counter_de_ml,
product_over_counter,
product_name_as_reported_cn,
product_name,
product_level,
product_groupname_id,
product_group,
product_formulation_de_ml,
product_formulation,
product_family,
product_compounded_de_ml,
product_compounded,
product_availfor_eval,
product_available_flag_de_ml,
product_available_flag,
product_active_ingredient,
prodasrepted,
prod_ref_required_de_ml,
prod_ref_required,
prod_ref_comments,
prod_pqms,
prod_addition_info,
prior_use_de_ml,
prior_use,
primary_suspect_drug_de_ml,
primary_suspect_drug,
primary_suspect_ctdrug,
primary_product_from_reporter,
previous_rec_id,
prev_rec_id,
prefered_product_description,
prefered_code,
pre_1938,
pqc_number,
pmda_medicine_name,
paternal_exposure_de_ml,
paternal_exposure,
otc_risk_classfn,
obtaindrugcountry_lang,
obtaindrugcountry_de_ml,
obtaindrugcountry,
not_reportable_jpn_de_ml,
not_reportable_jpn,
new_drug_classifn,
ndc_text,
ndc_no,
ndc,
most_suspect_product_de_ml,
most_suspect_product,
medicinalproduct_lang,
medicinalproduct,
medicinal_prod_nf,
medicinal_prod_id,
manufacturer_name,
manufacturer_group,
manufacturer,
mah_as_coded,
kdd_code,
jpn_drug_code,
is_whood_ing,
is_pqc_event,
is_lot_batch_num_enabled,
is_coded_de_ml,
is_coded,
is_added_from_krjp,
investigational_prod_blinded_de_ml,
investigational_prod_blinded,
invented_name,
internal_drug_code,
intended_use_name,
inq_rec_id,
generic_name_cn,
generic_name_as_coded,
generic_name,
generic_de_ml,
generic,
frequency,
form_strength_unit_de_ml,
form_strength_unit,
form_strength,
form_name,
food_cosm_related,
fk_apat_rec_id,
fei_number_checked,
fei_number_as_coded,
fei_number,
ext_clob_fld,
expiration_date_text,
expiration_date_fmt,
expiration_date,
exclude_for_qde,
equivalent_drug_de_ml,
equivalent_drug,
entity_updated,
e2b_r3_pharmaprodid_datenumber,
e2b_r3_pharma_prodid,
e2b_r3_medprodid_datenumber,
e2b_r3_med_prodid,
duns_number_checked,
duns_number_as_coded,
duns_number,
drugtreatmentduration_lang,
drugtreatmentduration,
drugtreatdurationunit,
drugstructuredosageunit_lang,
drugstructuredosageunit,
drugstructuredosagenumb_lang,
drugstructuredosagenumb,
drugstartperiodunit_de_ml,
drugstartperiodunit,
drugstartperiod_lang,
drugstartperiod,
drugstartdatefmt,
drugstartdate,
drugseparatedosageoth,
drugseparatedosagenumb_lang,
drugseparatedosagenumb,
drugrecurreadministration_de_ml,
drugrecurreadministration,
drugparadministration_lang,
drugparadministration,
druglastperiodunit_de_ml,
druglastperiodunit,
druglastperiod_lang,
druglastperiod,
drugintdosageunitnumb_lang,
drugintdosageunitnumb,
drugintdosageunitdef_lang,
drugintdosageunitdef_de_ml,
drugintdosageunitdef,
drugindicationmeddraver_lang,
drugindicationmeddraver,
drugindication_lang,
drugindication,
drugenddatefmt,
drugenddate,
drugdosagetext_lang,
drugdosagetext,
drugdosageform_lang,
drugdosageform,
drugcumulativedosageunit_lang,
drugcumulativedosageunit_de_ml,
drugcumulativedosageunit,
drugcumulativedosagenumb_lang,
drugcumulativedosagenumb,
drugcharacterization_de_ml,
drugcharacterization,
drugbatchnumb_lang,
drugbatchnumb,
drugauthorizationtype,
drugauthorizationnumb_lang,
drugauthorizationnumb,
drugauthorizationholder_lang,
drugauthorizationholder_as_coded,
drugauthorizationholder,
drugauthorizationcountry_lang,
drugauthorizationcountry_de_ml,
drugauthorizationcountry,
drugauthnumbascoded,
drugadministrationroute_ocr,
drugadministrationroute_lang,
drugadministrationroute_de_ml,
drugadministrationroute,
drugadditionalinfo,
drugadditional_lang,
drugadditional_code,
drugadditional,
drug_vigilancetype_de_ml,
drug_vigilancetype,
drug_type,
drug_sequence_id,
drug_mfg_coded_flag,
drug_lang,
drug_json_text,
drug_hash_code,
drug_coded_flag,
drug_auth_as_coded,
drug_admin_at_ae,
drlprefterm,
did_ae_abate,
device_returned_to_mfr,
device_reprocessor_unit,
device_reprocessed_and_reused,
device_name,
device_avail_eval,
default_product,
dechallenge_de_ml,
dechallenge,
date_modified,
date_discontinued,
date_created,
country_as_coded,
country,
copied_blinded,
container_name,
concomitant_product,
component_type_de_ml,
component_type,
company_unit_code,
company_product_de_ml,
company_product,
comp_rec_id,
common_device_code,
coding_type,
coding_comments,
coding_class_de_ml,
coding_class,
coded_product,
clinical_drug_code_jpn,
clinical_drug_code,
classify_product_de_ml,
classify_product,
city_as_coded,
city,
caused_by_lo_effect_de_ml,
caused_by_lo_effect,
causality_required,
blinded_product_rec_id,
biosimilar_de_ml,
biosimilar,
bio_father_drug_de_ml,
bio_father_drug,
arisg_trade_id,
arisg_product_id,
arisg_approval_id,
ari_rec_id,
adr_trade_rec_id,
address_as_coded,
address,
active_substance_coded_flag,
active_ing_record_id,
actiondrug_de_ml,
actiondrug,
account_record_id,
access_to_otc)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,ziporpostalcode_as_coded,
ziporpostalcode,
whoddversion,
whoddcodeflag,
whoddcode,
whodd_decode,
was_prd_stopped,
was_prd_restarted,
version,
vaccinetype_sf,
vaccinetype_nf,
vaccine_type_de_ml,
vaccine_type,
vaccine_given_de_ml,
vaccine_given,
vaccination_military_flag_nf,
vaccination_military_flag_de_ml,
vaccination_military_flag,
uuid,
user_modified,
user_created,
un_blinded,
trademark_name,
total_number_of_lots,
tolerated_de_ml,
tolerated,
synonym_drug,
study_relation_desc,
study_product_type_de_ml,
study_product_type,
strength_ocr,
strength_name,
street_addrs_as_coded2,
street_addrs_as_coded1,
street_addrs2,
street_addrs1,
state_as_coded,
state,
spr_id,
specialized_product_category_de_ml,
specialized_product_category,
serial_numb,
scientific_name,
returned_to_mfr_date_fmt,
returned_to_mfr_date,
reportedmedicinalproduct,
repo_drug_id,
related_device_cn,
record_id,
reactgestationperiodunit_de_ml,
reactgestationperiodunit,
reactgestationperiod_lang,
reactgestationperiod,
rank_order,
pv_product_id,
protocol_product_type_de_ml,
protocol_product_type,
productseqgen,
product_type_de_ml,
product_type,
product_returned_date,
product_record_id,
product_over_counter_de_ml,
product_over_counter,
product_name_as_reported_cn,
product_name,
product_level,
product_groupname_id,
product_group,
product_formulation_de_ml,
product_formulation,
product_family,
product_compounded_de_ml,
product_compounded,
product_availfor_eval,
product_available_flag_de_ml,
product_available_flag,
product_active_ingredient,
prodasrepted,
prod_ref_required_de_ml,
prod_ref_required,
prod_ref_comments,
prod_pqms,
prod_addition_info,
prior_use_de_ml,
prior_use,
primary_suspect_drug_de_ml,
primary_suspect_drug,
primary_suspect_ctdrug,
primary_product_from_reporter,
previous_rec_id,
prev_rec_id,
prefered_product_description,
prefered_code,
pre_1938,
pqc_number,
pmda_medicine_name,
paternal_exposure_de_ml,
paternal_exposure,
otc_risk_classfn,
obtaindrugcountry_lang,
obtaindrugcountry_de_ml,
obtaindrugcountry,
not_reportable_jpn_de_ml,
not_reportable_jpn,
new_drug_classifn,
ndc_text,
ndc_no,
ndc,
most_suspect_product_de_ml,
most_suspect_product,
medicinalproduct_lang,
medicinalproduct,
medicinal_prod_nf,
medicinal_prod_id,
manufacturer_name,
manufacturer_group,
manufacturer,
mah_as_coded,
kdd_code,
jpn_drug_code,
is_whood_ing,
is_pqc_event,
is_lot_batch_num_enabled,
is_coded_de_ml,
is_coded,
is_added_from_krjp,
investigational_prod_blinded_de_ml,
investigational_prod_blinded,
invented_name,
internal_drug_code,
intended_use_name,
inq_rec_id,
generic_name_cn,
generic_name_as_coded,
generic_name,
generic_de_ml,
generic,
frequency,
form_strength_unit_de_ml,
form_strength_unit,
form_strength,
form_name,
food_cosm_related,
fk_apat_rec_id,
fei_number_checked,
fei_number_as_coded,
fei_number,
ext_clob_fld,
expiration_date_text,
expiration_date_fmt,
expiration_date,
exclude_for_qde,
equivalent_drug_de_ml,
equivalent_drug,
entity_updated,
e2b_r3_pharmaprodid_datenumber,
e2b_r3_pharma_prodid,
e2b_r3_medprodid_datenumber,
e2b_r3_med_prodid,
duns_number_checked,
duns_number_as_coded,
duns_number,
drugtreatmentduration_lang,
drugtreatmentduration,
drugtreatdurationunit,
drugstructuredosageunit_lang,
drugstructuredosageunit,
drugstructuredosagenumb_lang,
drugstructuredosagenumb,
drugstartperiodunit_de_ml,
drugstartperiodunit,
drugstartperiod_lang,
drugstartperiod,
drugstartdatefmt,
drugstartdate,
drugseparatedosageoth,
drugseparatedosagenumb_lang,
drugseparatedosagenumb,
drugrecurreadministration_de_ml,
drugrecurreadministration,
drugparadministration_lang,
drugparadministration,
druglastperiodunit_de_ml,
druglastperiodunit,
druglastperiod_lang,
druglastperiod,
drugintdosageunitnumb_lang,
drugintdosageunitnumb,
drugintdosageunitdef_lang,
drugintdosageunitdef_de_ml,
drugintdosageunitdef,
drugindicationmeddraver_lang,
drugindicationmeddraver,
drugindication_lang,
drugindication,
drugenddatefmt,
drugenddate,
drugdosagetext_lang,
drugdosagetext,
drugdosageform_lang,
drugdosageform,
drugcumulativedosageunit_lang,
drugcumulativedosageunit_de_ml,
drugcumulativedosageunit,
drugcumulativedosagenumb_lang,
drugcumulativedosagenumb,
drugcharacterization_de_ml,
drugcharacterization,
drugbatchnumb_lang,
drugbatchnumb,
drugauthorizationtype,
drugauthorizationnumb_lang,
drugauthorizationnumb,
drugauthorizationholder_lang,
drugauthorizationholder_as_coded,
drugauthorizationholder,
drugauthorizationcountry_lang,
drugauthorizationcountry_de_ml,
drugauthorizationcountry,
drugauthnumbascoded,
drugadministrationroute_ocr,
drugadministrationroute_lang,
drugadministrationroute_de_ml,
drugadministrationroute,
drugadditionalinfo,
drugadditional_lang,
drugadditional_code,
drugadditional,
drug_vigilancetype_de_ml,
drug_vigilancetype,
drug_type,
drug_sequence_id,
drug_mfg_coded_flag,
drug_lang,
drug_json_text,
drug_hash_code,
drug_coded_flag,
drug_auth_as_coded,
drug_admin_at_ae,
drlprefterm,
did_ae_abate,
device_returned_to_mfr,
device_reprocessor_unit,
device_reprocessed_and_reused,
device_name,
device_avail_eval,
default_product,
dechallenge_de_ml,
dechallenge,
date_modified,
date_discontinued,
date_created,
country_as_coded,
country,
copied_blinded,
container_name,
concomitant_product,
component_type_de_ml,
component_type,
company_unit_code,
company_product_de_ml,
company_product,
comp_rec_id,
common_device_code,
coding_type,
coding_comments,
coding_class_de_ml,
coding_class,
coded_product,
clinical_drug_code_jpn,
clinical_drug_code,
classify_product_de_ml,
classify_product,
city_as_coded,
city,
caused_by_lo_effect_de_ml,
caused_by_lo_effect,
causality_required,
blinded_product_rec_id,
biosimilar_de_ml,
biosimilar,
bio_father_drug_de_ml,
bio_father_drug,
arisg_trade_id,
arisg_product_id,
arisg_approval_id,
ari_rec_id,
adr_trade_rec_id,
address_as_coded,
address,
active_substance_coded_flag,
active_ing_record_id,
actiondrug_de_ml,
actiondrug,
account_record_id,
access_to_otc
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP 
WHERE 	TO_DATE(LS_DB_DRUG.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_TMP.PROCESSING_DT)
AND LS_DB_DRUG.INTEGRATION_ID = LS_DB_DRUG_TMP.INTEGRATION_ID
AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DRUG Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DRUG not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
