
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_APPROVAL()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_APPROVAL'),
	'LSDB','Case','LS_DB_DRUG_APPROVAL',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_APPROVAL' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_APPROVAL' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_APPROVAL_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_APPROVAL_DELETION_TMP  As select RECORD_ID,'lsmv_drug_approval' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_approval WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10127','10128','1015','5015','5040','7077','709')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_drug_approval_SUBSET AS 
(
select * from 
    (SELECT  
    access_to_otc  access_to_otc,access_to_otc_nf  access_to_otc_nf,active_device  active_device,approval_number_cn  approval_number_cn,ari_rec_id  ari_rec_id,companion_diagnostic  companion_diagnostic,custom_made  custom_made,date_created  date_created,date_modified  date_modified,demographic_was_designed  demographic_was_designed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10128' AND CD=CAST(demographic_was_designed AS VARCHAR(100)) )demographic_was_designed_de_ml , dev_marketmonths  dev_marketmonths,dev_marketyears  dev_marketyears,device_class  device_class,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5040' AND CD=CAST(device_class AS VARCHAR(100)) )device_class_de_ml , device_commercial  device_commercial,device_commercial_date_fmt  device_commercial_date_fmt,device_marketed_before  device_marketed_before,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(device_marketed_before AS VARCHAR(100)) )device_marketed_before_de_ml , drugauthorizationcountry  drugauthorizationcountry,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(drugauthorizationcountry AS VARCHAR(100)) )drugauthorizationcountry_de_ml , drugauthorizationholder  drugauthorizationholder,drugauthorizationnumb  drugauthorizationnumb,drugauthorizationtype  drugauthorizationtype,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='709' AND CD=CAST(drugauthorizationtype AS VARCHAR(100)) )drugauthorizationtype_de_ml , fk_drug_rec_id  fk_drug_rec_id,hc_id_number  hc_id_number,implantable  implantable,instrument  instrument,intended_medical_prod  intended_medical_prod,ivdr_type  ivdr_type,mah_as_coded  mah_as_coded,mah_record_id  mah_record_id,mdr_system  mdr_system,mdr_type  mdr_type,measureing_fucntion  measureing_fucntion,near_patient  near_patient,non_medical_purpose  non_medical_purpose,notified_body_ident_no  notified_body_ident_no,notified_certificate_no  notified_certificate_no,otc_risk_classification  otc_risk_classification,pmda_medicine_name  pmda_medicine_name,procedure_packs  procedure_packs,product_type  product_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5015' AND CD=CAST(product_type AS VARCHAR(100)) )product_type_de_ml , professtional_testing  professtional_testing,reagent  reagent,record_id  record_id,reusabe_instruments  reusabe_instruments,self_testing  self_testing,software_ivdr  software_ivdr,software_mdr  software_mdr,spr_id  spr_id,srerile_cond_ivdr  srerile_cond_ivdr,sterile_condition  sterile_condition,substance_name  substance_name,trade_name  trade_name,trade_package_desc  trade_package_desc,type_of_device  type_of_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10127' AND CD=CAST(type_of_device AS VARCHAR(100)) )type_of_device_de_ml , type_of_device_sf  type_of_device_sf,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_approval
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_approval')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_approval_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_approval_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_approval_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_approval_SUBSET.user_modified  ,lsmv_drug_approval_SUBSET.user_created  ,lsmv_drug_approval_SUBSET.type_of_device_sf  ,lsmv_drug_approval_SUBSET.type_of_device_de_ml  ,lsmv_drug_approval_SUBSET.type_of_device  ,lsmv_drug_approval_SUBSET.trade_package_desc  ,lsmv_drug_approval_SUBSET.trade_name  ,lsmv_drug_approval_SUBSET.substance_name  ,lsmv_drug_approval_SUBSET.sterile_condition  ,lsmv_drug_approval_SUBSET.srerile_cond_ivdr  ,lsmv_drug_approval_SUBSET.spr_id  ,lsmv_drug_approval_SUBSET.software_mdr  ,lsmv_drug_approval_SUBSET.software_ivdr  ,lsmv_drug_approval_SUBSET.self_testing  ,lsmv_drug_approval_SUBSET.reusabe_instruments  ,lsmv_drug_approval_SUBSET.record_id  ,lsmv_drug_approval_SUBSET.reagent  ,lsmv_drug_approval_SUBSET.professtional_testing  ,lsmv_drug_approval_SUBSET.product_type_de_ml  ,lsmv_drug_approval_SUBSET.product_type  ,lsmv_drug_approval_SUBSET.procedure_packs  ,lsmv_drug_approval_SUBSET.pmda_medicine_name  ,lsmv_drug_approval_SUBSET.otc_risk_classification  ,lsmv_drug_approval_SUBSET.notified_certificate_no  ,lsmv_drug_approval_SUBSET.notified_body_ident_no  ,lsmv_drug_approval_SUBSET.non_medical_purpose  ,lsmv_drug_approval_SUBSET.near_patient  ,lsmv_drug_approval_SUBSET.measureing_fucntion  ,lsmv_drug_approval_SUBSET.mdr_type  ,lsmv_drug_approval_SUBSET.mdr_system  ,lsmv_drug_approval_SUBSET.mah_record_id  ,lsmv_drug_approval_SUBSET.mah_as_coded  ,lsmv_drug_approval_SUBSET.ivdr_type  ,lsmv_drug_approval_SUBSET.intended_medical_prod  ,lsmv_drug_approval_SUBSET.instrument  ,lsmv_drug_approval_SUBSET.implantable  ,lsmv_drug_approval_SUBSET.hc_id_number  ,lsmv_drug_approval_SUBSET.fk_drug_rec_id  ,lsmv_drug_approval_SUBSET.drugauthorizationtype_de_ml  ,lsmv_drug_approval_SUBSET.drugauthorizationtype  ,lsmv_drug_approval_SUBSET.drugauthorizationnumb  ,lsmv_drug_approval_SUBSET.drugauthorizationholder  ,lsmv_drug_approval_SUBSET.drugauthorizationcountry_de_ml  ,lsmv_drug_approval_SUBSET.drugauthorizationcountry  ,lsmv_drug_approval_SUBSET.device_marketed_before_de_ml  ,lsmv_drug_approval_SUBSET.device_marketed_before  ,lsmv_drug_approval_SUBSET.device_commercial_date_fmt  ,lsmv_drug_approval_SUBSET.device_commercial  ,lsmv_drug_approval_SUBSET.device_class_de_ml  ,lsmv_drug_approval_SUBSET.device_class  ,lsmv_drug_approval_SUBSET.dev_marketyears  ,lsmv_drug_approval_SUBSET.dev_marketmonths  ,lsmv_drug_approval_SUBSET.demographic_was_designed_de_ml  ,lsmv_drug_approval_SUBSET.demographic_was_designed  ,lsmv_drug_approval_SUBSET.date_modified  ,lsmv_drug_approval_SUBSET.date_created  ,lsmv_drug_approval_SUBSET.custom_made  ,lsmv_drug_approval_SUBSET.companion_diagnostic  ,lsmv_drug_approval_SUBSET.ari_rec_id  ,lsmv_drug_approval_SUBSET.approval_number_cn  ,lsmv_drug_approval_SUBSET.active_device  ,lsmv_drug_approval_SUBSET.access_to_otc_nf  ,lsmv_drug_approval_SUBSET.access_to_otc ,CONCAT(NVL(lsmv_drug_approval_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_approval_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_approval_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP)
where target_table_name='LS_DB_DRUG_APPROVAL'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL   
SET LS_DB_DRUG_APPROVAL.user_modified = LS_DB_DRUG_APPROVAL_TMP.user_modified,LS_DB_DRUG_APPROVAL.user_created = LS_DB_DRUG_APPROVAL_TMP.user_created,LS_DB_DRUG_APPROVAL.type_of_device_sf = LS_DB_DRUG_APPROVAL_TMP.type_of_device_sf,LS_DB_DRUG_APPROVAL.type_of_device_de_ml = LS_DB_DRUG_APPROVAL_TMP.type_of_device_de_ml,LS_DB_DRUG_APPROVAL.type_of_device = LS_DB_DRUG_APPROVAL_TMP.type_of_device,LS_DB_DRUG_APPROVAL.trade_package_desc = LS_DB_DRUG_APPROVAL_TMP.trade_package_desc,LS_DB_DRUG_APPROVAL.trade_name = LS_DB_DRUG_APPROVAL_TMP.trade_name,LS_DB_DRUG_APPROVAL.substance_name = LS_DB_DRUG_APPROVAL_TMP.substance_name,LS_DB_DRUG_APPROVAL.sterile_condition = LS_DB_DRUG_APPROVAL_TMP.sterile_condition,LS_DB_DRUG_APPROVAL.srerile_cond_ivdr = LS_DB_DRUG_APPROVAL_TMP.srerile_cond_ivdr,LS_DB_DRUG_APPROVAL.spr_id = LS_DB_DRUG_APPROVAL_TMP.spr_id,LS_DB_DRUG_APPROVAL.software_mdr = LS_DB_DRUG_APPROVAL_TMP.software_mdr,LS_DB_DRUG_APPROVAL.software_ivdr = LS_DB_DRUG_APPROVAL_TMP.software_ivdr,LS_DB_DRUG_APPROVAL.self_testing = LS_DB_DRUG_APPROVAL_TMP.self_testing,LS_DB_DRUG_APPROVAL.reusabe_instruments = LS_DB_DRUG_APPROVAL_TMP.reusabe_instruments,LS_DB_DRUG_APPROVAL.record_id = LS_DB_DRUG_APPROVAL_TMP.record_id,LS_DB_DRUG_APPROVAL.reagent = LS_DB_DRUG_APPROVAL_TMP.reagent,LS_DB_DRUG_APPROVAL.professtional_testing = LS_DB_DRUG_APPROVAL_TMP.professtional_testing,LS_DB_DRUG_APPROVAL.product_type_de_ml = LS_DB_DRUG_APPROVAL_TMP.product_type_de_ml,LS_DB_DRUG_APPROVAL.product_type = LS_DB_DRUG_APPROVAL_TMP.product_type,LS_DB_DRUG_APPROVAL.procedure_packs = LS_DB_DRUG_APPROVAL_TMP.procedure_packs,LS_DB_DRUG_APPROVAL.pmda_medicine_name = LS_DB_DRUG_APPROVAL_TMP.pmda_medicine_name,LS_DB_DRUG_APPROVAL.otc_risk_classification = LS_DB_DRUG_APPROVAL_TMP.otc_risk_classification,LS_DB_DRUG_APPROVAL.notified_certificate_no = LS_DB_DRUG_APPROVAL_TMP.notified_certificate_no,LS_DB_DRUG_APPROVAL.notified_body_ident_no = LS_DB_DRUG_APPROVAL_TMP.notified_body_ident_no,LS_DB_DRUG_APPROVAL.non_medical_purpose = LS_DB_DRUG_APPROVAL_TMP.non_medical_purpose,LS_DB_DRUG_APPROVAL.near_patient = LS_DB_DRUG_APPROVAL_TMP.near_patient,LS_DB_DRUG_APPROVAL.measureing_fucntion = LS_DB_DRUG_APPROVAL_TMP.measureing_fucntion,LS_DB_DRUG_APPROVAL.mdr_type = LS_DB_DRUG_APPROVAL_TMP.mdr_type,LS_DB_DRUG_APPROVAL.mdr_system = LS_DB_DRUG_APPROVAL_TMP.mdr_system,LS_DB_DRUG_APPROVAL.mah_record_id = LS_DB_DRUG_APPROVAL_TMP.mah_record_id,LS_DB_DRUG_APPROVAL.mah_as_coded = LS_DB_DRUG_APPROVAL_TMP.mah_as_coded,LS_DB_DRUG_APPROVAL.ivdr_type = LS_DB_DRUG_APPROVAL_TMP.ivdr_type,LS_DB_DRUG_APPROVAL.intended_medical_prod = LS_DB_DRUG_APPROVAL_TMP.intended_medical_prod,LS_DB_DRUG_APPROVAL.instrument = LS_DB_DRUG_APPROVAL_TMP.instrument,LS_DB_DRUG_APPROVAL.implantable = LS_DB_DRUG_APPROVAL_TMP.implantable,LS_DB_DRUG_APPROVAL.hc_id_number = LS_DB_DRUG_APPROVAL_TMP.hc_id_number,LS_DB_DRUG_APPROVAL.fk_drug_rec_id = LS_DB_DRUG_APPROVAL_TMP.fk_drug_rec_id,LS_DB_DRUG_APPROVAL.drugauthorizationtype_de_ml = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationtype_de_ml,LS_DB_DRUG_APPROVAL.drugauthorizationtype = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationtype,LS_DB_DRUG_APPROVAL.drugauthorizationnumb = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationnumb,LS_DB_DRUG_APPROVAL.drugauthorizationholder = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationholder,LS_DB_DRUG_APPROVAL.drugauthorizationcountry_de_ml = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationcountry_de_ml,LS_DB_DRUG_APPROVAL.drugauthorizationcountry = LS_DB_DRUG_APPROVAL_TMP.drugauthorizationcountry,LS_DB_DRUG_APPROVAL.device_marketed_before_de_ml = LS_DB_DRUG_APPROVAL_TMP.device_marketed_before_de_ml,LS_DB_DRUG_APPROVAL.device_marketed_before = LS_DB_DRUG_APPROVAL_TMP.device_marketed_before,LS_DB_DRUG_APPROVAL.device_commercial_date_fmt = LS_DB_DRUG_APPROVAL_TMP.device_commercial_date_fmt,LS_DB_DRUG_APPROVAL.device_commercial = LS_DB_DRUG_APPROVAL_TMP.device_commercial,LS_DB_DRUG_APPROVAL.device_class_de_ml = LS_DB_DRUG_APPROVAL_TMP.device_class_de_ml,LS_DB_DRUG_APPROVAL.device_class = LS_DB_DRUG_APPROVAL_TMP.device_class,LS_DB_DRUG_APPROVAL.dev_marketyears = LS_DB_DRUG_APPROVAL_TMP.dev_marketyears,LS_DB_DRUG_APPROVAL.dev_marketmonths = LS_DB_DRUG_APPROVAL_TMP.dev_marketmonths,LS_DB_DRUG_APPROVAL.demographic_was_designed_de_ml = LS_DB_DRUG_APPROVAL_TMP.demographic_was_designed_de_ml,LS_DB_DRUG_APPROVAL.demographic_was_designed = LS_DB_DRUG_APPROVAL_TMP.demographic_was_designed,LS_DB_DRUG_APPROVAL.date_modified = LS_DB_DRUG_APPROVAL_TMP.date_modified,LS_DB_DRUG_APPROVAL.date_created = LS_DB_DRUG_APPROVAL_TMP.date_created,LS_DB_DRUG_APPROVAL.custom_made = LS_DB_DRUG_APPROVAL_TMP.custom_made,LS_DB_DRUG_APPROVAL.companion_diagnostic = LS_DB_DRUG_APPROVAL_TMP.companion_diagnostic,LS_DB_DRUG_APPROVAL.ari_rec_id = LS_DB_DRUG_APPROVAL_TMP.ari_rec_id,LS_DB_DRUG_APPROVAL.approval_number_cn = LS_DB_DRUG_APPROVAL_TMP.approval_number_cn,LS_DB_DRUG_APPROVAL.active_device = LS_DB_DRUG_APPROVAL_TMP.active_device,LS_DB_DRUG_APPROVAL.access_to_otc_nf = LS_DB_DRUG_APPROVAL_TMP.access_to_otc_nf,LS_DB_DRUG_APPROVAL.access_to_otc = LS_DB_DRUG_APPROVAL_TMP.access_to_otc,
LS_DB_DRUG_APPROVAL.PROCESSING_DT = LS_DB_DRUG_APPROVAL_TMP.PROCESSING_DT ,
LS_DB_DRUG_APPROVAL.receipt_id     =LS_DB_DRUG_APPROVAL_TMP.receipt_id        ,
LS_DB_DRUG_APPROVAL.case_no        =LS_DB_DRUG_APPROVAL_TMP.case_no           ,
LS_DB_DRUG_APPROVAL.case_version   =LS_DB_DRUG_APPROVAL_TMP.case_version      ,
LS_DB_DRUG_APPROVAL.version_no     =LS_DB_DRUG_APPROVAL_TMP.version_no        ,
LS_DB_DRUG_APPROVAL.expiry_date    =LS_DB_DRUG_APPROVAL_TMP.expiry_date       ,
LS_DB_DRUG_APPROVAL.load_ts        =LS_DB_DRUG_APPROVAL_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP 
WHERE 	LS_DB_DRUG_APPROVAL.INTEGRATION_ID = LS_DB_DRUG_APPROVAL_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_APPROVAL_TMP.PROCESSING_DT = LS_DB_DRUG_APPROVAL.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
type_of_device_sf,
type_of_device_de_ml,
type_of_device,
trade_package_desc,
trade_name,
substance_name,
sterile_condition,
srerile_cond_ivdr,
spr_id,
software_mdr,
software_ivdr,
self_testing,
reusabe_instruments,
record_id,
reagent,
professtional_testing,
product_type_de_ml,
product_type,
procedure_packs,
pmda_medicine_name,
otc_risk_classification,
notified_certificate_no,
notified_body_ident_no,
non_medical_purpose,
near_patient,
measureing_fucntion,
mdr_type,
mdr_system,
mah_record_id,
mah_as_coded,
ivdr_type,
intended_medical_prod,
instrument,
implantable,
hc_id_number,
fk_drug_rec_id,
drugauthorizationtype_de_ml,
drugauthorizationtype,
drugauthorizationnumb,
drugauthorizationholder,
drugauthorizationcountry_de_ml,
drugauthorizationcountry,
device_marketed_before_de_ml,
device_marketed_before,
device_commercial_date_fmt,
device_commercial,
device_class_de_ml,
device_class,
dev_marketyears,
dev_marketmonths,
demographic_was_designed_de_ml,
demographic_was_designed,
date_modified,
date_created,
custom_made,
companion_diagnostic,
ari_rec_id,
approval_number_cn,
active_device,
access_to_otc_nf,
access_to_otc)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
type_of_device_sf,
type_of_device_de_ml,
type_of_device,
trade_package_desc,
trade_name,
substance_name,
sterile_condition,
srerile_cond_ivdr,
spr_id,
software_mdr,
software_ivdr,
self_testing,
reusabe_instruments,
record_id,
reagent,
professtional_testing,
product_type_de_ml,
product_type,
procedure_packs,
pmda_medicine_name,
otc_risk_classification,
notified_certificate_no,
notified_body_ident_no,
non_medical_purpose,
near_patient,
measureing_fucntion,
mdr_type,
mdr_system,
mah_record_id,
mah_as_coded,
ivdr_type,
intended_medical_prod,
instrument,
implantable,
hc_id_number,
fk_drug_rec_id,
drugauthorizationtype_de_ml,
drugauthorizationtype,
drugauthorizationnumb,
drugauthorizationholder,
drugauthorizationcountry_de_ml,
drugauthorizationcountry,
device_marketed_before_de_ml,
device_marketed_before,
device_commercial_date_fmt,
device_commercial,
device_class_de_ml,
device_class,
dev_marketyears,
dev_marketmonths,
demographic_was_designed_de_ml,
demographic_was_designed,
date_modified,
date_created,
custom_made,
companion_diagnostic,
ari_rec_id,
approval_number_cn,
active_device,
access_to_otc_nf,
access_to_otc
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_APPROVAL.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_APPROVAL_TMP.PROCESSING_DT)
AND LS_DB_DRUG_APPROVAL.INTEGRATION_ID = LS_DB_DRUG_APPROVAL_TMP.INTEGRATION_ID
AND LS_DB_DRUG_APPROVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_approval')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_approval')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_APPROVAL_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_APPROVAL'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_APPROVAL Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_APPROVAL'),
	'LSDB','Case','LS_DB_DRUG_APPROVAL',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_APPROVAL not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
