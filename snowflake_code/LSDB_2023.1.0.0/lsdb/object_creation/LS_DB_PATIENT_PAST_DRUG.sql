
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT_PAST_DRUG()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PAST_DRUG'),
	'LSRA','Case','LS_DB_PATIENT_PAST_DRUG',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PATIENT_PAST_DRUG' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_PAST_DRUG'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_PAST_DRUG'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  As select RECORD_ID,'lsmv_patient_past_drug_therapy' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drug_therapy WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_patient_past_drugsub' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drugsub WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1016','9070')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drugsub WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_appdt_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drugsub WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_patient_past_drugsub_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  patpdgsub_ari_rec_id,date_created  patpdgsub_date_created,date_modified  patpdgsub_date_modified,fk_appdt_rec_id  patpdgsub_fk_appdt_rec_id,record_id  patpdgsub_record_id,spr_id  patpdgsub_spr_id,substance_name  patpdgsub_substance_name,substance_strength_number  patpdgsub_substance_strength_number,substance_strength_unit  patpdgsub_substance_strength_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9070' AND CD=CAST(substance_strength_unit AS VARCHAR(100)) )patpdgsub_substance_strength_unit_de_ml , substance_termid  patpdgsub_substance_termid,substance_termid_version  patpdgsub_substance_termid_version,user_created  patpdgsub_user_created,user_modified  patpdgsub_user_modified,uuid  patpdgsub_uuid,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drugsub
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_appdt_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drugsub')
  ) where REC_RANK=1 )
  , lsmv_patient_past_drug_therapy_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  patpdgth_ari_rec_id,coding_comments_ind  patpdgth_coding_comments_ind,coding_comments_recact  patpdgth_coding_comments_recact,coding_type_for_indication  patpdgth_coding_type_for_indication,coding_type_for_reaction  patpdgth_coding_type_for_reaction,comp_rec_id  patpdgth_comp_rec_id,container_name  patpdgth_container_name,date_created  patpdgth_date_created,date_modified  patpdgth_date_modified,device_name  patpdgth_device_name,drugenddate  patpdgth_drugenddate,drugenddate_nf  patpdgth_drugenddate_nf,drugenddate_tz  patpdgth_drugenddate_tz,drugenddatefmt  patpdgth_drugenddatefmt,drugindication  patpdgth_drugindication,drugindication_code  patpdgth_drugindication_code,drugindication_coded_flag  patpdgth_drugindication_coded_flag,drugindication_decode  patpdgth_drugindication_decode,drugindication_lang  patpdgth_drugindication_lang,drugindication_ptcode  patpdgth_drugindication_ptcode,drugindicationlevel  patpdgth_drugindicationlevel,drugname  patpdgth_drugname,drugname_lang  patpdgth_drugname_lang,drugname_nf  patpdgth_drugname_nf,drugreaction  patpdgth_drugreaction,drugreaction_code  patpdgth_drugreaction_code,drugreaction_coded_flag  patpdgth_drugreaction_coded_flag,drugreaction_decode  patpdgth_drugreaction_decode,drugreaction_lang  patpdgth_drugreaction_lang,drugreaction_ptcode  patpdgth_drugreaction_ptcode,drugreactionlevel  patpdgth_drugreactionlevel,drugreactionmeddraver  patpdgth_drugreactionmeddraver,drugreactionmeddraver_lang  patpdgth_drugreactionmeddraver_lang,drugstartdate  patpdgth_drugstartdate,drugstartdate_nf  patpdgth_drugstartdate_nf,drugstartdate_tz  patpdgth_drugstartdate_tz,drugstartdatefmt  patpdgth_drugstartdatefmt,e2b_r3_med_prodid  patpdgth_e2b_r3_med_prodid,e2b_r3_medprodid_date  patpdgth_e2b_r3_medprodid_date,e2b_r3_medprodid_datenumber  patpdgth_e2b_r3_medprodid_datenumber,e2b_r3_pharma_prodid  patpdgth_e2b_r3_pharma_prodid,e2b_r3_pharmaprodid_date  patpdgth_e2b_r3_pharmaprodid_date,e2b_r3_pharmaprodid_datenumber  patpdgth_e2b_r3_pharmaprodid_datenumber,entity_updated  patpdgth_entity_updated,ext_clob_fld  patpdgth_ext_clob_fld,fk_apat_rec_id  patpdgth_fk_apat_rec_id,form_name  patpdgth_form_name,indicationmeddraver  patpdgth_indicationmeddraver,indicationmeddraver_lang  patpdgth_indicationmeddraver_lang,inq_rec_id  patpdgth_inq_rec_id,intended_use_name  patpdgth_intended_use_name,invented_name  patpdgth_invented_name,kdd_code  patpdgth_kdd_code,med_product_id  patpdgth_med_product_id,pastdrugtherapy_lang  patpdgth_pastdrugtherapy_lang,patient_age_at_vaccine  patpdgth_patient_age_at_vaccine,patient_age_at_vaccine_unit  patpdgth_patient_age_at_vaccine_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(patient_age_at_vaccine_unit AS VARCHAR(100)) )patpdgth_patient_age_at_vaccine_unit_de_ml , prod_pat_codingflag  patpdgth_prod_pat_codingflag,prod_pat_codingtype  patpdgth_prod_pat_codingtype,product_description  patpdgth_product_description,product_nameas_reported  patpdgth_product_nameas_reported,record_id  patpdgth_record_id,scientific_name  patpdgth_scientific_name,spr_id  patpdgth_spr_id,strength_name  patpdgth_strength_name,trademark_name  patpdgth_trademark_name,user_created  patpdgth_user_created,user_modified  patpdgth_user_modified,uuid  patpdgth_uuid,version  patpdgth_version,whodd_code  patpdgth_whodd_code,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_patient_past_drug_therapy
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drug_therapy')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_patient_past_drugsub_SUBSET.patpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_patient_past_drug_therapy_SUBSET.patpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_USER_MODIFIED USER_MODIFIED,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_USER_CREATED CREATED_BY,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_patient_past_drugsub_SUBSET.patpdgsub_uuid  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_user_modified  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_user_created  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_termid_version  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_termid  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_strength_unit_de_ml  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_strength_unit  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_strength_number  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_substance_name  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_spr_id  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_record_id  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_fk_appdt_rec_id  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_date_modified  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_date_created  ,lsmv_patient_past_drugsub_SUBSET.patpdgsub_ari_rec_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_whodd_code  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_version  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_uuid  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_user_modified  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_user_created  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_trademark_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_strength_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_spr_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_scientific_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_record_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_product_nameas_reported  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_product_description  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_prod_pat_codingtype  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_prod_pat_codingflag  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_patient_age_at_vaccine_unit_de_ml  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_patient_age_at_vaccine_unit  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_patient_age_at_vaccine  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_pastdrugtherapy_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_med_product_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_kdd_code  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_invented_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_intended_use_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_inq_rec_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_indicationmeddraver_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_indicationmeddraver  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_form_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_fk_apat_rec_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_ext_clob_fld  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_entity_updated  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_pharmaprodid_datenumber  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_pharmaprodid_date  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_pharma_prodid  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_medprodid_datenumber  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_medprodid_date  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_e2b_r3_med_prodid  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugstartdatefmt  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugstartdate_tz  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugstartdate_nf  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugstartdate  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreactionmeddraver_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreactionmeddraver  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreactionlevel  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction_ptcode  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction_decode  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction_coded_flag  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction_code  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugreaction  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugname_nf  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugname_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugname  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindicationlevel  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication_ptcode  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication_lang  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication_decode  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication_coded_flag  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication_code  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugindication  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugenddatefmt  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugenddate_tz  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugenddate_nf  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_drugenddate  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_device_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_date_modified  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_date_created  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_container_name  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_comp_rec_id  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_coding_type_for_reaction  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_coding_type_for_indication  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_coding_comments_recact  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_coding_comments_ind  ,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_ari_rec_id ,CONCAT( NVL(lsmv_patient_past_drugsub_SUBSET.patpdgsub_RECORD_ID,-1),'||',NVL(lsmv_patient_past_drug_therapy_SUBSET.patpdgth_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_patient_past_drug_therapy_SUBSET  LEFT JOIN lsmv_patient_past_drugsub_SUBSET ON lsmv_patient_past_drug_therapy_SUBSET.patpdgth_record_id=lsmv_patient_past_drugsub_SUBSET.patpdgsub_fk_appdt_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_patient_past_drugsub_SUBSET.patpdgsub_RECORD_ID,lsmv_patient_past_drug_therapy_SUBSET.patpdgth_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP)
where target_table_name='LS_DB_PATIENT_PAST_DRUG'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PAST_DRUG'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG   
SET LS_DB_PATIENT_PAST_DRUG.patpdgsub_uuid = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_uuid,LS_DB_PATIENT_PAST_DRUG.patpdgsub_user_modified = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_user_modified,LS_DB_PATIENT_PAST_DRUG.patpdgsub_user_created = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_user_created,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_termid_version = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_termid_version,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_termid = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_termid,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_strength_unit_de_ml = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_strength_unit_de_ml,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_strength_unit = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_strength_unit,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_strength_number = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_strength_number,LS_DB_PATIENT_PAST_DRUG.patpdgsub_substance_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_substance_name,LS_DB_PATIENT_PAST_DRUG.patpdgsub_spr_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_spr_id,LS_DB_PATIENT_PAST_DRUG.patpdgsub_record_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_record_id,LS_DB_PATIENT_PAST_DRUG.patpdgsub_fk_appdt_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_fk_appdt_rec_id,LS_DB_PATIENT_PAST_DRUG.patpdgsub_date_modified = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_date_modified,LS_DB_PATIENT_PAST_DRUG.patpdgsub_date_created = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_date_created,LS_DB_PATIENT_PAST_DRUG.patpdgsub_ari_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_ari_rec_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_whodd_code = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_whodd_code,LS_DB_PATIENT_PAST_DRUG.patpdgth_version = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_version,LS_DB_PATIENT_PAST_DRUG.patpdgth_uuid = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_uuid,LS_DB_PATIENT_PAST_DRUG.patpdgth_user_modified = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_user_modified,LS_DB_PATIENT_PAST_DRUG.patpdgth_user_created = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_user_created,LS_DB_PATIENT_PAST_DRUG.patpdgth_trademark_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_trademark_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_strength_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_strength_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_spr_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_spr_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_scientific_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_scientific_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_record_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_record_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_product_nameas_reported = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_product_nameas_reported,LS_DB_PATIENT_PAST_DRUG.patpdgth_product_description = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_product_description,LS_DB_PATIENT_PAST_DRUG.patpdgth_prod_pat_codingtype = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_prod_pat_codingtype,LS_DB_PATIENT_PAST_DRUG.patpdgth_prod_pat_codingflag = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_prod_pat_codingflag,LS_DB_PATIENT_PAST_DRUG.patpdgth_patient_age_at_vaccine_unit_de_ml = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_patient_age_at_vaccine_unit_de_ml,LS_DB_PATIENT_PAST_DRUG.patpdgth_patient_age_at_vaccine_unit = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_patient_age_at_vaccine_unit,LS_DB_PATIENT_PAST_DRUG.patpdgth_patient_age_at_vaccine = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_patient_age_at_vaccine,LS_DB_PATIENT_PAST_DRUG.patpdgth_pastdrugtherapy_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_pastdrugtherapy_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_med_product_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_med_product_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_kdd_code = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_kdd_code,LS_DB_PATIENT_PAST_DRUG.patpdgth_invented_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_invented_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_intended_use_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_intended_use_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_inq_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_inq_rec_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_indicationmeddraver_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_indicationmeddraver_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_indicationmeddraver = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_indicationmeddraver,LS_DB_PATIENT_PAST_DRUG.patpdgth_form_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_form_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_fk_apat_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_fk_apat_rec_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_ext_clob_fld = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_ext_clob_fld,LS_DB_PATIENT_PAST_DRUG.patpdgth_entity_updated = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_entity_updated,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_pharmaprodid_datenumber = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_pharmaprodid_datenumber,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_pharmaprodid_date = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_pharmaprodid_date,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_pharma_prodid = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_pharma_prodid,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_medprodid_datenumber = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_medprodid_datenumber,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_medprodid_date = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_medprodid_date,LS_DB_PATIENT_PAST_DRUG.patpdgth_e2b_r3_med_prodid = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_e2b_r3_med_prodid,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugstartdatefmt = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugstartdatefmt,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugstartdate_tz = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugstartdate_tz,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugstartdate_nf = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugstartdate_nf,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugstartdate = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugstartdate,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreactionmeddraver_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreactionmeddraver_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreactionmeddraver = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreactionmeddraver,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreactionlevel = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreactionlevel,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction_ptcode = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction_ptcode,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction_decode = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction_decode,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction_coded_flag = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction_coded_flag,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction_code = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction_code,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugreaction = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugreaction,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugname_nf = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugname_nf,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugname_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugname_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugname = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugname,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindicationlevel = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindicationlevel,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication_ptcode = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication_ptcode,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication_lang = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication_lang,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication_decode = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication_decode,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication_coded_flag = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication_coded_flag,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication_code = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication_code,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugindication = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugindication,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugenddatefmt = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugenddatefmt,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugenddate_tz = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugenddate_tz,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugenddate_nf = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugenddate_nf,LS_DB_PATIENT_PAST_DRUG.patpdgth_drugenddate = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_drugenddate,LS_DB_PATIENT_PAST_DRUG.patpdgth_device_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_device_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_date_modified = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_date_modified,LS_DB_PATIENT_PAST_DRUG.patpdgth_date_created = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_date_created,LS_DB_PATIENT_PAST_DRUG.patpdgth_container_name = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_container_name,LS_DB_PATIENT_PAST_DRUG.patpdgth_comp_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_comp_rec_id,LS_DB_PATIENT_PAST_DRUG.patpdgth_coding_type_for_reaction = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_coding_type_for_reaction,LS_DB_PATIENT_PAST_DRUG.patpdgth_coding_type_for_indication = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_coding_type_for_indication,LS_DB_PATIENT_PAST_DRUG.patpdgth_coding_comments_recact = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_coding_comments_recact,LS_DB_PATIENT_PAST_DRUG.patpdgth_coding_comments_ind = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_coding_comments_ind,LS_DB_PATIENT_PAST_DRUG.patpdgth_ari_rec_id = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_ari_rec_id,
LS_DB_PATIENT_PAST_DRUG.PROCESSING_DT = LS_DB_PATIENT_PAST_DRUG_TMP.PROCESSING_DT,
LS_DB_PATIENT_PAST_DRUG.receipt_id     =LS_DB_PATIENT_PAST_DRUG_TMP.receipt_id    ,
LS_DB_PATIENT_PAST_DRUG.case_no        =LS_DB_PATIENT_PAST_DRUG_TMP.case_no           ,
LS_DB_PATIENT_PAST_DRUG.case_version   =LS_DB_PATIENT_PAST_DRUG_TMP.case_version      ,
LS_DB_PATIENT_PAST_DRUG.version_no     =LS_DB_PATIENT_PAST_DRUG_TMP.version_no        ,
LS_DB_PATIENT_PAST_DRUG.user_modified  =LS_DB_PATIENT_PAST_DRUG_TMP.user_modified     ,
LS_DB_PATIENT_PAST_DRUG.date_modified  =LS_DB_PATIENT_PAST_DRUG_TMP.date_modified     ,
LS_DB_PATIENT_PAST_DRUG.expiry_date    =LS_DB_PATIENT_PAST_DRUG_TMP.expiry_date       ,
LS_DB_PATIENT_PAST_DRUG.created_by     =LS_DB_PATIENT_PAST_DRUG_TMP.created_by        ,
LS_DB_PATIENT_PAST_DRUG.created_dt     =LS_DB_PATIENT_PAST_DRUG_TMP.created_dt        ,
LS_DB_PATIENT_PAST_DRUG.load_ts        =LS_DB_PATIENT_PAST_DRUG_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP 
WHERE 	LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PATIENT_PAST_DRUG_TMP.PROCESSING_DT = LS_DB_PATIENT_PAST_DRUG.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PATIENT_PAST_DRUG.patpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT_PAST_DRUG.patpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PATIENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID ,LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP 
ON LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID=LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_RECORD_ID
AND LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID 
where LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID  is null AND LS_DB_PATIENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID in (select patpdgth_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP )
) TMP where LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PATIENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID ,LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP 
    ON LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID=LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_RECORD_ID
    AND LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID 
    where LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID  is null AND LS_DB_PATIENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID in (select patpdgth_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG
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
integration_id ,patpdgsub_uuid,
patpdgsub_user_modified,
patpdgsub_user_created,
patpdgsub_substance_termid_version,
patpdgsub_substance_termid,
patpdgsub_substance_strength_unit_de_ml,
patpdgsub_substance_strength_unit,
patpdgsub_substance_strength_number,
patpdgsub_substance_name,
patpdgsub_spr_id,
patpdgsub_record_id,
patpdgsub_fk_appdt_rec_id,
patpdgsub_date_modified,
patpdgsub_date_created,
patpdgsub_ari_rec_id,
patpdgth_whodd_code,
patpdgth_version,
patpdgth_uuid,
patpdgth_user_modified,
patpdgth_user_created,
patpdgth_trademark_name,
patpdgth_strength_name,
patpdgth_spr_id,
patpdgth_scientific_name,
patpdgth_record_id,
patpdgth_product_nameas_reported,
patpdgth_product_description,
patpdgth_prod_pat_codingtype,
patpdgth_prod_pat_codingflag,
patpdgth_patient_age_at_vaccine_unit_de_ml,
patpdgth_patient_age_at_vaccine_unit,
patpdgth_patient_age_at_vaccine,
patpdgth_pastdrugtherapy_lang,
patpdgth_med_product_id,
patpdgth_kdd_code,
patpdgth_invented_name,
patpdgth_intended_use_name,
patpdgth_inq_rec_id,
patpdgth_indicationmeddraver_lang,
patpdgth_indicationmeddraver,
patpdgth_form_name,
patpdgth_fk_apat_rec_id,
patpdgth_ext_clob_fld,
patpdgth_entity_updated,
patpdgth_e2b_r3_pharmaprodid_datenumber,
patpdgth_e2b_r3_pharmaprodid_date,
patpdgth_e2b_r3_pharma_prodid,
patpdgth_e2b_r3_medprodid_datenumber,
patpdgth_e2b_r3_medprodid_date,
patpdgth_e2b_r3_med_prodid,
patpdgth_drugstartdatefmt,
patpdgth_drugstartdate_tz,
patpdgth_drugstartdate_nf,
patpdgth_drugstartdate,
patpdgth_drugreactionmeddraver_lang,
patpdgth_drugreactionmeddraver,
patpdgth_drugreactionlevel,
patpdgth_drugreaction_ptcode,
patpdgth_drugreaction_lang,
patpdgth_drugreaction_decode,
patpdgth_drugreaction_coded_flag,
patpdgth_drugreaction_code,
patpdgth_drugreaction,
patpdgth_drugname_nf,
patpdgth_drugname_lang,
patpdgth_drugname,
patpdgth_drugindicationlevel,
patpdgth_drugindication_ptcode,
patpdgth_drugindication_lang,
patpdgth_drugindication_decode,
patpdgth_drugindication_coded_flag,
patpdgth_drugindication_code,
patpdgth_drugindication,
patpdgth_drugenddatefmt,
patpdgth_drugenddate_tz,
patpdgth_drugenddate_nf,
patpdgth_drugenddate,
patpdgth_device_name,
patpdgth_date_modified,
patpdgth_date_created,
patpdgth_container_name,
patpdgth_comp_rec_id,
patpdgth_coding_type_for_reaction,
patpdgth_coding_type_for_indication,
patpdgth_coding_comments_recact,
patpdgth_coding_comments_ind,
patpdgth_ari_rec_id)
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
integration_id ,patpdgsub_uuid,
patpdgsub_user_modified,
patpdgsub_user_created,
patpdgsub_substance_termid_version,
patpdgsub_substance_termid,
patpdgsub_substance_strength_unit_de_ml,
patpdgsub_substance_strength_unit,
patpdgsub_substance_strength_number,
patpdgsub_substance_name,
patpdgsub_spr_id,
patpdgsub_record_id,
patpdgsub_fk_appdt_rec_id,
patpdgsub_date_modified,
patpdgsub_date_created,
patpdgsub_ari_rec_id,
patpdgth_whodd_code,
patpdgth_version,
patpdgth_uuid,
patpdgth_user_modified,
patpdgth_user_created,
patpdgth_trademark_name,
patpdgth_strength_name,
patpdgth_spr_id,
patpdgth_scientific_name,
patpdgth_record_id,
patpdgth_product_nameas_reported,
patpdgth_product_description,
patpdgth_prod_pat_codingtype,
patpdgth_prod_pat_codingflag,
patpdgth_patient_age_at_vaccine_unit_de_ml,
patpdgth_patient_age_at_vaccine_unit,
patpdgth_patient_age_at_vaccine,
patpdgth_pastdrugtherapy_lang,
patpdgth_med_product_id,
patpdgth_kdd_code,
patpdgth_invented_name,
patpdgth_intended_use_name,
patpdgth_inq_rec_id,
patpdgth_indicationmeddraver_lang,
patpdgth_indicationmeddraver,
patpdgth_form_name,
patpdgth_fk_apat_rec_id,
patpdgth_ext_clob_fld,
patpdgth_entity_updated,
patpdgth_e2b_r3_pharmaprodid_datenumber,
patpdgth_e2b_r3_pharmaprodid_date,
patpdgth_e2b_r3_pharma_prodid,
patpdgth_e2b_r3_medprodid_datenumber,
patpdgth_e2b_r3_medprodid_date,
patpdgth_e2b_r3_med_prodid,
patpdgth_drugstartdatefmt,
patpdgth_drugstartdate_tz,
patpdgth_drugstartdate_nf,
patpdgth_drugstartdate,
patpdgth_drugreactionmeddraver_lang,
patpdgth_drugreactionmeddraver,
patpdgth_drugreactionlevel,
patpdgth_drugreaction_ptcode,
patpdgth_drugreaction_lang,
patpdgth_drugreaction_decode,
patpdgth_drugreaction_coded_flag,
patpdgth_drugreaction_code,
patpdgth_drugreaction,
patpdgth_drugname_nf,
patpdgth_drugname_lang,
patpdgth_drugname,
patpdgth_drugindicationlevel,
patpdgth_drugindication_ptcode,
patpdgth_drugindication_lang,
patpdgth_drugindication_decode,
patpdgth_drugindication_coded_flag,
patpdgth_drugindication_code,
patpdgth_drugindication,
patpdgth_drugenddatefmt,
patpdgth_drugenddate_tz,
patpdgth_drugenddate_nf,
patpdgth_drugenddate,
patpdgth_device_name,
patpdgth_date_modified,
patpdgth_date_created,
patpdgth_container_name,
patpdgth_comp_rec_id,
patpdgth_coding_type_for_reaction,
patpdgth_coding_type_for_indication,
patpdgth_coding_comments_recact,
patpdgth_coding_comments_ind,
patpdgth_ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP 
WHERE 	TO_DATE(LS_DB_PATIENT_PAST_DRUG.PROCESSING_DT) < TO_DATE(LS_DB_PATIENT_PAST_DRUG_TMP.PROCESSING_DT)
AND LS_DB_PATIENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PATIENT_PAST_DRUG_TMP.INTEGRATION_ID
AND LS_DB_PATIENT_PAST_DRUG.patpdgth_RECORD_ID = LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_RECORD_ID
AND LS_DB_PATIENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PATIENT_PAST_DRUG.patpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT_PAST_DRUG.patpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PATIENT_PAST_DRUG_TMP.patpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_PAST_DRUG_TMP.patpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG TGT
WHERE  ( patpdgth_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drug_therapy') OR patpdgsub_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drugsub')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( patpdgth_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drug_therapy') OR patpdgsub_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_past_drugsub')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PAST_DRUG_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PATIENT_PAST_DRUG'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PAST_DRUG'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PATIENT_PAST_DRUG Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PATIENT_PAST_DRUG'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PATIENT_PAST_DRUG not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
