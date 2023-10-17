
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PARENT_PAST_DRUG()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PARENT_PAST_DRUG'),
	'LSDB','Case','LS_DB_PARENT_PAST_DRUG',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PARENT_PAST_DRUG' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PARENT_PAST_DRUG' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  As select RECORD_ID,'lsmv_parent_past_drug_therapy' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drug_therapy WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_parent_past_drugsubstance' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drugsubstance WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9070')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drugsubstance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_apardt_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drugsubstance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drug_therapy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARENT_PAST_DRUG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_parent_past_drugsubstance_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  parpdgsub_ari_rec_id,date_created  parpdgsub_date_created,date_modified  parpdgsub_date_modified,fk_apardt_rec_id  parpdgsub_fk_apardt_rec_id,record_id  parpdgsub_record_id,spr_id  parpdgsub_spr_id,substance_name  parpdgsub_substance_name,substance_strength_number  parpdgsub_substance_strength_number,substance_strength_unit  parpdgsub_substance_strength_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9070' AND CD=CAST(substance_strength_unit AS VARCHAR(100)) )parpdgsub_substance_strength_unit_de_ml , substance_termid  parpdgsub_substance_termid,substance_termid_version  parpdgsub_substance_termid_version,user_created  parpdgsub_user_created,user_modified  parpdgsub_user_modified,uuid  parpdgsub_uuid,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drugsubstance
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_apardt_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drugsubstance')
  ) where REC_RANK=1 )
  , lsmv_parent_past_drug_therapy_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  parpdgth_ari_rec_id,coding_comments_ind  parpdgth_coding_comments_ind,coding_comments_recact  parpdgth_coding_comments_recact,coding_type_for_indication  parpdgth_coding_type_for_indication,coding_type_for_reaction  parpdgth_coding_type_for_reaction,comp_rec_id  parpdgth_comp_rec_id,container_name  parpdgth_container_name,date_created  parpdgth_date_created,date_modified  parpdgth_date_modified,device_name  parpdgth_device_name,drugenddate  parpdgth_drugenddate,drugenddate_nf  parpdgth_drugenddate_nf,drugenddate_tz  parpdgth_drugenddate_tz,drugenddatefmt  parpdgth_drugenddatefmt,drugindication  parpdgth_drugindication,drugindication_code  parpdgth_drugindication_code,drugindication_coded_flag  parpdgth_drugindication_coded_flag,drugindication_decode  parpdgth_drugindication_decode,drugindication_lang  parpdgth_drugindication_lang,drugindication_ptcode  parpdgth_drugindication_ptcode,drugindicationlevel  parpdgth_drugindicationlevel,drugname  parpdgth_drugname,drugname_lang  parpdgth_drugname_lang,drugreaction  parpdgth_drugreaction,drugreaction_code  parpdgth_drugreaction_code,drugreaction_coded_flag  parpdgth_drugreaction_coded_flag,drugreaction_decode  parpdgth_drugreaction_decode,drugreaction_lang  parpdgth_drugreaction_lang,drugreaction_ptcode  parpdgth_drugreaction_ptcode,drugreactionlevel  parpdgth_drugreactionlevel,drugreactionmeddraver  parpdgth_drugreactionmeddraver,drugreactionmeddraver_lang  parpdgth_drugreactionmeddraver_lang,drugstartdate  parpdgth_drugstartdate,drugstartdate_nf  parpdgth_drugstartdate_nf,drugstartdate_tz  parpdgth_drugstartdate_tz,drugstartdatefmt  parpdgth_drugstartdatefmt,e2b_r3_med_prodid  parpdgth_e2b_r3_med_prodid,e2b_r3_medprodid_date  parpdgth_e2b_r3_medprodid_date,e2b_r3_medprodid_date_fmt  parpdgth_e2b_r3_medprodid_date_fmt,e2b_r3_medprodid_datenumber  parpdgth_e2b_r3_medprodid_datenumber,e2b_r3_pharma_prodid  parpdgth_e2b_r3_pharma_prodid,e2b_r3_pharmaprodid_date  parpdgth_e2b_r3_pharmaprodid_date,e2b_r3_pharmaprodid_date_fmt  parpdgth_e2b_r3_pharmaprodid_date_fmt,e2b_r3_pharmaprodid_datenumber  parpdgth_e2b_r3_pharmaprodid_datenumber,entity_updated  parpdgth_entity_updated,ext_clob_fld  parpdgth_ext_clob_fld,fk_apar_rec_id  parpdgth_fk_apar_rec_id,form_name  parpdgth_form_name,indicationmeddraver  parpdgth_indicationmeddraver,indicationmeddraver_lang  parpdgth_indicationmeddraver_lang,inq_rec_id  parpdgth_inq_rec_id,intended_use_name  parpdgth_intended_use_name,invented_name  parpdgth_invented_name,kdd_code  parpdgth_kdd_code,med_product_id  parpdgth_med_product_id,pastdrugtherapy_lang  parpdgth_pastdrugtherapy_lang,prod_par_codingflag  parpdgth_prod_par_codingflag,prod_par_codingtype  parpdgth_prod_par_codingtype,product_description  parpdgth_product_description,product_nameas_reported  parpdgth_product_nameas_reported,record_id  parpdgth_record_id,scientific_name  parpdgth_scientific_name,spr_id  parpdgth_spr_id,strength_name  parpdgth_strength_name,trademark_name  parpdgth_trademark_name,user_created  parpdgth_user_created,user_modified  parpdgth_user_modified,uuid  parpdgth_uuid,version  parpdgth_version,whodd_code  parpdgth_whodd_code,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_parent_past_drug_therapy
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drug_therapy')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_parent_past_drug_therapy_SUBSET.parpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_USER_MODIFIED USER_MODIFIED,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_USER_CREATED CREATED_BY,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_uuid  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_user_modified  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_user_created  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_termid_version  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_termid  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_strength_unit_de_ml  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_strength_unit  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_strength_number  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_substance_name  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_spr_id  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_record_id  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_fk_apardt_rec_id  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_date_modified  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_date_created  ,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_ari_rec_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_whodd_code  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_version  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_uuid  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_user_modified  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_user_created  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_trademark_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_strength_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_spr_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_scientific_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_record_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_product_nameas_reported  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_product_description  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_prod_par_codingtype  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_prod_par_codingflag  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_pastdrugtherapy_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_med_product_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_kdd_code  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_invented_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_intended_use_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_inq_rec_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_indicationmeddraver_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_indicationmeddraver  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_form_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_fk_apar_rec_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_ext_clob_fld  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_entity_updated  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_pharmaprodid_datenumber  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_pharmaprodid_date_fmt  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_pharmaprodid_date  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_pharma_prodid  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_medprodid_datenumber  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_medprodid_date_fmt  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_medprodid_date  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_e2b_r3_med_prodid  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugstartdatefmt  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugstartdate_tz  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugstartdate_nf  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugstartdate  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreactionmeddraver_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreactionmeddraver  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreactionlevel  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction_ptcode  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction_decode  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction_coded_flag  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction_code  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugreaction  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugname_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugname  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindicationlevel  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication_ptcode  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication_lang  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication_decode  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication_coded_flag  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication_code  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugindication  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugenddatefmt  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugenddate_tz  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugenddate_nf  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_drugenddate  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_device_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_date_modified  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_date_created  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_container_name  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_comp_rec_id  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_coding_type_for_reaction  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_coding_type_for_indication  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_coding_comments_recact  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_coding_comments_ind  ,lsmv_parent_past_drug_therapy_SUBSET.parpdgth_ari_rec_id ,CONCAT( NVL(lsmv_parent_past_drug_therapy_SUBSET.parpdgth_RECORD_ID,-1),'||',NVL(lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_parent_past_drug_therapy_SUBSET  LEFT JOIN lsmv_parent_past_drugsubstance_SUBSET ON lsmv_parent_past_drug_therapy_SUBSET.parpdgth_record_id=lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_fk_apardt_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_parent_past_drug_therapy_SUBSET.parpdgth_RECORD_ID,lsmv_parent_past_drugsubstance_SUBSET.parpdgsub_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP)
where target_table_name='LS_DB_PARENT_PAST_DRUG'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG   
SET LS_DB_PARENT_PAST_DRUG.parpdgsub_uuid = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_uuid,LS_DB_PARENT_PAST_DRUG.parpdgsub_user_modified = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_user_modified,LS_DB_PARENT_PAST_DRUG.parpdgsub_user_created = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_user_created,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_termid_version = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_termid_version,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_termid = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_termid,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_strength_unit_de_ml = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_strength_unit_de_ml,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_strength_unit = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_strength_unit,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_strength_number = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_strength_number,LS_DB_PARENT_PAST_DRUG.parpdgsub_substance_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_substance_name,LS_DB_PARENT_PAST_DRUG.parpdgsub_spr_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_spr_id,LS_DB_PARENT_PAST_DRUG.parpdgsub_record_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_record_id,LS_DB_PARENT_PAST_DRUG.parpdgsub_fk_apardt_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_fk_apardt_rec_id,LS_DB_PARENT_PAST_DRUG.parpdgsub_date_modified = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_date_modified,LS_DB_PARENT_PAST_DRUG.parpdgsub_date_created = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_date_created,LS_DB_PARENT_PAST_DRUG.parpdgsub_ari_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_ari_rec_id,LS_DB_PARENT_PAST_DRUG.parpdgth_whodd_code = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_whodd_code,LS_DB_PARENT_PAST_DRUG.parpdgth_version = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_version,LS_DB_PARENT_PAST_DRUG.parpdgth_uuid = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_uuid,LS_DB_PARENT_PAST_DRUG.parpdgth_user_modified = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_user_modified,LS_DB_PARENT_PAST_DRUG.parpdgth_user_created = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_user_created,LS_DB_PARENT_PAST_DRUG.parpdgth_trademark_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_trademark_name,LS_DB_PARENT_PAST_DRUG.parpdgth_strength_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_strength_name,LS_DB_PARENT_PAST_DRUG.parpdgth_spr_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_spr_id,LS_DB_PARENT_PAST_DRUG.parpdgth_scientific_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_scientific_name,LS_DB_PARENT_PAST_DRUG.parpdgth_record_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_record_id,LS_DB_PARENT_PAST_DRUG.parpdgth_product_nameas_reported = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_product_nameas_reported,LS_DB_PARENT_PAST_DRUG.parpdgth_product_description = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_product_description,LS_DB_PARENT_PAST_DRUG.parpdgth_prod_par_codingtype = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_prod_par_codingtype,LS_DB_PARENT_PAST_DRUG.parpdgth_prod_par_codingflag = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_prod_par_codingflag,LS_DB_PARENT_PAST_DRUG.parpdgth_pastdrugtherapy_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_pastdrugtherapy_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_med_product_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_med_product_id,LS_DB_PARENT_PAST_DRUG.parpdgth_kdd_code = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_kdd_code,LS_DB_PARENT_PAST_DRUG.parpdgth_invented_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_invented_name,LS_DB_PARENT_PAST_DRUG.parpdgth_intended_use_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_intended_use_name,LS_DB_PARENT_PAST_DRUG.parpdgth_inq_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_inq_rec_id,LS_DB_PARENT_PAST_DRUG.parpdgth_indicationmeddraver_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_indicationmeddraver_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_indicationmeddraver = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_indicationmeddraver,LS_DB_PARENT_PAST_DRUG.parpdgth_form_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_form_name,LS_DB_PARENT_PAST_DRUG.parpdgth_fk_apar_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_fk_apar_rec_id,LS_DB_PARENT_PAST_DRUG.parpdgth_ext_clob_fld = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_ext_clob_fld,LS_DB_PARENT_PAST_DRUG.parpdgth_entity_updated = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_entity_updated,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_pharmaprodid_datenumber = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_pharmaprodid_datenumber,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_pharmaprodid_date_fmt = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_pharmaprodid_date_fmt,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_pharmaprodid_date = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_pharmaprodid_date,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_pharma_prodid = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_pharma_prodid,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_medprodid_datenumber = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_medprodid_datenumber,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_medprodid_date_fmt = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_medprodid_date_fmt,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_medprodid_date = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_medprodid_date,LS_DB_PARENT_PAST_DRUG.parpdgth_e2b_r3_med_prodid = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_e2b_r3_med_prodid,LS_DB_PARENT_PAST_DRUG.parpdgth_drugstartdatefmt = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugstartdatefmt,LS_DB_PARENT_PAST_DRUG.parpdgth_drugstartdate_tz = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugstartdate_tz,LS_DB_PARENT_PAST_DRUG.parpdgth_drugstartdate_nf = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugstartdate_nf,LS_DB_PARENT_PAST_DRUG.parpdgth_drugstartdate = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugstartdate,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreactionmeddraver_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreactionmeddraver_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreactionmeddraver = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreactionmeddraver,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreactionlevel = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreactionlevel,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction_ptcode = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction_ptcode,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction_decode = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction_decode,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction_coded_flag = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction_coded_flag,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction_code = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction_code,LS_DB_PARENT_PAST_DRUG.parpdgth_drugreaction = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugreaction,LS_DB_PARENT_PAST_DRUG.parpdgth_drugname_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugname_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_drugname = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugname,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindicationlevel = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindicationlevel,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication_ptcode = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication_ptcode,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication_lang = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication_lang,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication_decode = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication_decode,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication_coded_flag = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication_coded_flag,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication_code = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication_code,LS_DB_PARENT_PAST_DRUG.parpdgth_drugindication = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugindication,LS_DB_PARENT_PAST_DRUG.parpdgth_drugenddatefmt = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugenddatefmt,LS_DB_PARENT_PAST_DRUG.parpdgth_drugenddate_tz = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugenddate_tz,LS_DB_PARENT_PAST_DRUG.parpdgth_drugenddate_nf = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugenddate_nf,LS_DB_PARENT_PAST_DRUG.parpdgth_drugenddate = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_drugenddate,LS_DB_PARENT_PAST_DRUG.parpdgth_device_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_device_name,LS_DB_PARENT_PAST_DRUG.parpdgth_date_modified = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_date_modified,LS_DB_PARENT_PAST_DRUG.parpdgth_date_created = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_date_created,LS_DB_PARENT_PAST_DRUG.parpdgth_container_name = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_container_name,LS_DB_PARENT_PAST_DRUG.parpdgth_comp_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_comp_rec_id,LS_DB_PARENT_PAST_DRUG.parpdgth_coding_type_for_reaction = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_coding_type_for_reaction,LS_DB_PARENT_PAST_DRUG.parpdgth_coding_type_for_indication = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_coding_type_for_indication,LS_DB_PARENT_PAST_DRUG.parpdgth_coding_comments_recact = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_coding_comments_recact,LS_DB_PARENT_PAST_DRUG.parpdgth_coding_comments_ind = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_coding_comments_ind,LS_DB_PARENT_PAST_DRUG.parpdgth_ari_rec_id = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_ari_rec_id,
LS_DB_PARENT_PAST_DRUG.PROCESSING_DT = LS_DB_PARENT_PAST_DRUG_TMP.PROCESSING_DT,
LS_DB_PARENT_PAST_DRUG.receipt_id     =LS_DB_PARENT_PAST_DRUG_TMP.receipt_id    ,
LS_DB_PARENT_PAST_DRUG.case_no        =LS_DB_PARENT_PAST_DRUG_TMP.case_no           ,
LS_DB_PARENT_PAST_DRUG.case_version   =LS_DB_PARENT_PAST_DRUG_TMP.case_version      ,
LS_DB_PARENT_PAST_DRUG.version_no     =LS_DB_PARENT_PAST_DRUG_TMP.version_no        ,
LS_DB_PARENT_PAST_DRUG.user_modified  =LS_DB_PARENT_PAST_DRUG_TMP.user_modified     ,
LS_DB_PARENT_PAST_DRUG.date_modified  =LS_DB_PARENT_PAST_DRUG_TMP.date_modified     ,
LS_DB_PARENT_PAST_DRUG.expiry_date    =LS_DB_PARENT_PAST_DRUG_TMP.expiry_date       ,
LS_DB_PARENT_PAST_DRUG.created_by     =LS_DB_PARENT_PAST_DRUG_TMP.created_by        ,
LS_DB_PARENT_PAST_DRUG.created_dt     =LS_DB_PARENT_PAST_DRUG_TMP.created_dt        ,
LS_DB_PARENT_PAST_DRUG.load_ts        =LS_DB_PARENT_PAST_DRUG_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP 
WHERE 	LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PARENT_PAST_DRUG_TMP.PROCESSING_DT = LS_DB_PARENT_PAST_DRUG.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PARENT_PAST_DRUG.parpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PARENT_PAST_DRUG.parpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PARENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID ,LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP 
ON LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID=LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_RECORD_ID
AND LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID 
where LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID  is null AND LS_DB_PARENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID in (select parpdgth_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP )
) TMP where LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PARENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID ,LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP 
    ON LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID=LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_RECORD_ID
    AND LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID 
    where LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID  is null AND LS_DB_PARENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID in (select parpdgth_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG
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
integration_id ,parpdgsub_uuid,
parpdgsub_user_modified,
parpdgsub_user_created,
parpdgsub_substance_termid_version,
parpdgsub_substance_termid,
parpdgsub_substance_strength_unit_de_ml,
parpdgsub_substance_strength_unit,
parpdgsub_substance_strength_number,
parpdgsub_substance_name,
parpdgsub_spr_id,
parpdgsub_record_id,
parpdgsub_fk_apardt_rec_id,
parpdgsub_date_modified,
parpdgsub_date_created,
parpdgsub_ari_rec_id,
parpdgth_whodd_code,
parpdgth_version,
parpdgth_uuid,
parpdgth_user_modified,
parpdgth_user_created,
parpdgth_trademark_name,
parpdgth_strength_name,
parpdgth_spr_id,
parpdgth_scientific_name,
parpdgth_record_id,
parpdgth_product_nameas_reported,
parpdgth_product_description,
parpdgth_prod_par_codingtype,
parpdgth_prod_par_codingflag,
parpdgth_pastdrugtherapy_lang,
parpdgth_med_product_id,
parpdgth_kdd_code,
parpdgth_invented_name,
parpdgth_intended_use_name,
parpdgth_inq_rec_id,
parpdgth_indicationmeddraver_lang,
parpdgth_indicationmeddraver,
parpdgth_form_name,
parpdgth_fk_apar_rec_id,
parpdgth_ext_clob_fld,
parpdgth_entity_updated,
parpdgth_e2b_r3_pharmaprodid_datenumber,
parpdgth_e2b_r3_pharmaprodid_date_fmt,
parpdgth_e2b_r3_pharmaprodid_date,
parpdgth_e2b_r3_pharma_prodid,
parpdgth_e2b_r3_medprodid_datenumber,
parpdgth_e2b_r3_medprodid_date_fmt,
parpdgth_e2b_r3_medprodid_date,
parpdgth_e2b_r3_med_prodid,
parpdgth_drugstartdatefmt,
parpdgth_drugstartdate_tz,
parpdgth_drugstartdate_nf,
parpdgth_drugstartdate,
parpdgth_drugreactionmeddraver_lang,
parpdgth_drugreactionmeddraver,
parpdgth_drugreactionlevel,
parpdgth_drugreaction_ptcode,
parpdgth_drugreaction_lang,
parpdgth_drugreaction_decode,
parpdgth_drugreaction_coded_flag,
parpdgth_drugreaction_code,
parpdgth_drugreaction,
parpdgth_drugname_lang,
parpdgth_drugname,
parpdgth_drugindicationlevel,
parpdgth_drugindication_ptcode,
parpdgth_drugindication_lang,
parpdgth_drugindication_decode,
parpdgth_drugindication_coded_flag,
parpdgth_drugindication_code,
parpdgth_drugindication,
parpdgth_drugenddatefmt,
parpdgth_drugenddate_tz,
parpdgth_drugenddate_nf,
parpdgth_drugenddate,
parpdgth_device_name,
parpdgth_date_modified,
parpdgth_date_created,
parpdgth_container_name,
parpdgth_comp_rec_id,
parpdgth_coding_type_for_reaction,
parpdgth_coding_type_for_indication,
parpdgth_coding_comments_recact,
parpdgth_coding_comments_ind,
parpdgth_ari_rec_id)
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
integration_id ,parpdgsub_uuid,
parpdgsub_user_modified,
parpdgsub_user_created,
parpdgsub_substance_termid_version,
parpdgsub_substance_termid,
parpdgsub_substance_strength_unit_de_ml,
parpdgsub_substance_strength_unit,
parpdgsub_substance_strength_number,
parpdgsub_substance_name,
parpdgsub_spr_id,
parpdgsub_record_id,
parpdgsub_fk_apardt_rec_id,
parpdgsub_date_modified,
parpdgsub_date_created,
parpdgsub_ari_rec_id,
parpdgth_whodd_code,
parpdgth_version,
parpdgth_uuid,
parpdgth_user_modified,
parpdgth_user_created,
parpdgth_trademark_name,
parpdgth_strength_name,
parpdgth_spr_id,
parpdgth_scientific_name,
parpdgth_record_id,
parpdgth_product_nameas_reported,
parpdgth_product_description,
parpdgth_prod_par_codingtype,
parpdgth_prod_par_codingflag,
parpdgth_pastdrugtherapy_lang,
parpdgth_med_product_id,
parpdgth_kdd_code,
parpdgth_invented_name,
parpdgth_intended_use_name,
parpdgth_inq_rec_id,
parpdgth_indicationmeddraver_lang,
parpdgth_indicationmeddraver,
parpdgth_form_name,
parpdgth_fk_apar_rec_id,
parpdgth_ext_clob_fld,
parpdgth_entity_updated,
parpdgth_e2b_r3_pharmaprodid_datenumber,
parpdgth_e2b_r3_pharmaprodid_date_fmt,
parpdgth_e2b_r3_pharmaprodid_date,
parpdgth_e2b_r3_pharma_prodid,
parpdgth_e2b_r3_medprodid_datenumber,
parpdgth_e2b_r3_medprodid_date_fmt,
parpdgth_e2b_r3_medprodid_date,
parpdgth_e2b_r3_med_prodid,
parpdgth_drugstartdatefmt,
parpdgth_drugstartdate_tz,
parpdgth_drugstartdate_nf,
parpdgth_drugstartdate,
parpdgth_drugreactionmeddraver_lang,
parpdgth_drugreactionmeddraver,
parpdgth_drugreactionlevel,
parpdgth_drugreaction_ptcode,
parpdgth_drugreaction_lang,
parpdgth_drugreaction_decode,
parpdgth_drugreaction_coded_flag,
parpdgth_drugreaction_code,
parpdgth_drugreaction,
parpdgth_drugname_lang,
parpdgth_drugname,
parpdgth_drugindicationlevel,
parpdgth_drugindication_ptcode,
parpdgth_drugindication_lang,
parpdgth_drugindication_decode,
parpdgth_drugindication_coded_flag,
parpdgth_drugindication_code,
parpdgth_drugindication,
parpdgth_drugenddatefmt,
parpdgth_drugenddate_tz,
parpdgth_drugenddate_nf,
parpdgth_drugenddate,
parpdgth_device_name,
parpdgth_date_modified,
parpdgth_date_created,
parpdgth_container_name,
parpdgth_comp_rec_id,
parpdgth_coding_type_for_reaction,
parpdgth_coding_type_for_indication,
parpdgth_coding_comments_recact,
parpdgth_coding_comments_ind,
parpdgth_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP 
WHERE 	TO_DATE(LS_DB_PARENT_PAST_DRUG.PROCESSING_DT) < TO_DATE(LS_DB_PARENT_PAST_DRUG_TMP.PROCESSING_DT)
AND LS_DB_PARENT_PAST_DRUG.INTEGRATION_ID = LS_DB_PARENT_PAST_DRUG_TMP.INTEGRATION_ID
AND LS_DB_PARENT_PAST_DRUG.parpdgth_RECORD_ID = LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_RECORD_ID
AND LS_DB_PARENT_PAST_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PARENT_PAST_DRUG.parpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PARENT_PAST_DRUG.parpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PARENT_PAST_DRUG_TMP.parpdgth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PARENT_PAST_DRUG_TMP.parpdgsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG TGT
WHERE  ( parpdgth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drug_therapy') OR parpdgsub_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drugsubstance')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( parpdgth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drug_therapy') OR parpdgsub_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARENT_PAST_DRUG_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_past_drugsubstance')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARENT_PAST_DRUG_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PARENT_PAST_DRUG'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PARENT_PAST_DRUG Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PARENT_PAST_DRUG'),
	'LSDB','Case','LS_DB_PARENT_PAST_DRUG',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_PARENT_PAST_DRUG not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
