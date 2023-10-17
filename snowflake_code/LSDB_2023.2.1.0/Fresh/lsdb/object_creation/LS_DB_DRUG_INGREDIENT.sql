
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INGREDIENT()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INGREDIENT'),
	'LSDB','Case','LS_DB_DRUG_INGREDIENT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_INGREDIENT' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_INGREDIENT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  As select RECORD_ID,'lsmv_active_substance' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_active_substance WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_drug_ingredients' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_ingredients WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, fk_ad_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_active_substance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INGREDIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_ad_rec_id record_id, fk_ad_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_active_substance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INGREDIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, fk_ad_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_ingredients WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INGREDIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_ad_rec_id record_id, fk_ad_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_ingredients WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INGREDIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_active_substance_SUBSET AS 
(
select * from 
    (SELECT  
    activesubstance_lang  actsub_activesubstance_lang,activesubstancename  actsub_activesubstancename,activesubstancename_lang  actsub_activesubstancename_lang,ari_rec_id  actsub_ari_rec_id,cas_number  actsub_cas_number,clinica_drug_code_jpn  actsub_clinica_drug_code_jpn,comp_rec_id  actsub_comp_rec_id,date_created  actsub_date_created,date_modified  actsub_date_modified,entity_updated  actsub_entity_updated,ext_clob_fld  actsub_ext_clob_fld,fk_ad_rec_id  actsub_fk_ad_rec_id,inq_rec_id  actsub_inq_rec_id,kdd_code  actsub_kdd_code,primart_active  actsub_primart_active,record_id  actsub_record_id,spr_id  actsub_spr_id,substance_strength  actsub_substance_strength,substance_strength_unit  actsub_substance_strength_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9070' AND CD=CAST(substance_strength_unit AS VARCHAR(100)) )actsub_substance_strength_unit_de_ml , substance_termid  actsub_substance_termid,substance_termid_version  actsub_substance_termid_version,user_created  actsub_user_created,user_modified  actsub_user_modified,version  actsub_version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_active_substance
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_ad_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_substance')
  ) where REC_RANK=1 )
  , lsmv_drug_ingredients_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  druging_ari_rec_id,date_created  druging_date_created,date_modified  druging_date_modified,fk_ad_rec_id  druging_fk_ad_rec_id,inq_rec_id  druging_inq_rec_id,record_id  druging_record_id,spr_id  druging_spr_id,strength  druging_strength,strength_unit  druging_strength_unit,substance_name  druging_substance_name,user_created  druging_user_created,user_modified  druging_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_ingredients
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_ad_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_ingredients')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_ingredients_SUBSET.druging_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_active_substance_SUBSET.actsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_active_substance_SUBSET.actsub_USER_MODIFIED USER_MODIFIED,lsmv_active_substance_SUBSET.actsub_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_active_substance_SUBSET.actsub_USER_CREATED CREATED_BY,lsmv_active_substance_SUBSET.actsub_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_ingredients_SUBSET.druging_user_modified  ,lsmv_drug_ingredients_SUBSET.druging_user_created  ,lsmv_drug_ingredients_SUBSET.druging_substance_name  ,lsmv_drug_ingredients_SUBSET.druging_strength_unit  ,lsmv_drug_ingredients_SUBSET.druging_strength  ,lsmv_drug_ingredients_SUBSET.druging_spr_id  ,lsmv_drug_ingredients_SUBSET.druging_record_id  ,lsmv_drug_ingredients_SUBSET.druging_inq_rec_id  ,lsmv_drug_ingredients_SUBSET.druging_fk_ad_rec_id  ,lsmv_drug_ingredients_SUBSET.druging_date_modified  ,lsmv_drug_ingredients_SUBSET.druging_date_created  ,lsmv_drug_ingredients_SUBSET.druging_ari_rec_id  ,lsmv_active_substance_SUBSET.actsub_version  ,lsmv_active_substance_SUBSET.actsub_user_modified  ,lsmv_active_substance_SUBSET.actsub_user_created  ,lsmv_active_substance_SUBSET.actsub_substance_termid_version  ,lsmv_active_substance_SUBSET.actsub_substance_termid  ,lsmv_active_substance_SUBSET.actsub_substance_strength_unit_de_ml  ,lsmv_active_substance_SUBSET.actsub_substance_strength_unit  ,lsmv_active_substance_SUBSET.actsub_substance_strength  ,lsmv_active_substance_SUBSET.actsub_spr_id  ,lsmv_active_substance_SUBSET.actsub_record_id  ,lsmv_active_substance_SUBSET.actsub_primart_active  ,lsmv_active_substance_SUBSET.actsub_kdd_code  ,lsmv_active_substance_SUBSET.actsub_inq_rec_id  ,lsmv_active_substance_SUBSET.actsub_fk_ad_rec_id  ,lsmv_active_substance_SUBSET.actsub_ext_clob_fld  ,lsmv_active_substance_SUBSET.actsub_entity_updated  ,lsmv_active_substance_SUBSET.actsub_date_modified  ,lsmv_active_substance_SUBSET.actsub_date_created  ,lsmv_active_substance_SUBSET.actsub_comp_rec_id  ,lsmv_active_substance_SUBSET.actsub_clinica_drug_code_jpn  ,lsmv_active_substance_SUBSET.actsub_cas_number  ,lsmv_active_substance_SUBSET.actsub_ari_rec_id  ,lsmv_active_substance_SUBSET.actsub_activesubstancename_lang  ,lsmv_active_substance_SUBSET.actsub_activesubstancename  ,lsmv_active_substance_SUBSET.actsub_activesubstance_lang ,CONCAT( NVL(lsmv_drug_ingredients_SUBSET.druging_RECORD_ID,-1),'||',NVL(lsmv_active_substance_SUBSET.actsub_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_active_substance_SUBSET  FULL OUTER JOIN lsmv_drug_ingredients_SUBSET ON lsmv_active_substance_SUBSET.actsub_fk_ad_rec_id=lsmv_drug_ingredients_SUBSET.druging_fk_ad_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON   COALESCE(lsmv_drug_ingredients_SUBSET.druging_fk_ad_rec_id,lsmv_active_substance_SUBSET.actsub_fk_ad_rec_id) = LSMV_COMMON_COLUMN_SUBSET.COMMON_PARENT_KEY WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP)
where target_table_name='LS_DB_DRUG_INGREDIENT'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT   
SET LS_DB_DRUG_INGREDIENT.druging_user_modified = LS_DB_DRUG_INGREDIENT_TMP.druging_user_modified,LS_DB_DRUG_INGREDIENT.druging_user_created = LS_DB_DRUG_INGREDIENT_TMP.druging_user_created,LS_DB_DRUG_INGREDIENT.druging_substance_name = LS_DB_DRUG_INGREDIENT_TMP.druging_substance_name,LS_DB_DRUG_INGREDIENT.druging_strength_unit = LS_DB_DRUG_INGREDIENT_TMP.druging_strength_unit,LS_DB_DRUG_INGREDIENT.druging_strength = LS_DB_DRUG_INGREDIENT_TMP.druging_strength,LS_DB_DRUG_INGREDIENT.druging_spr_id = LS_DB_DRUG_INGREDIENT_TMP.druging_spr_id,LS_DB_DRUG_INGREDIENT.druging_record_id = LS_DB_DRUG_INGREDIENT_TMP.druging_record_id,LS_DB_DRUG_INGREDIENT.druging_inq_rec_id = LS_DB_DRUG_INGREDIENT_TMP.druging_inq_rec_id,LS_DB_DRUG_INGREDIENT.druging_fk_ad_rec_id = LS_DB_DRUG_INGREDIENT_TMP.druging_fk_ad_rec_id,LS_DB_DRUG_INGREDIENT.druging_date_modified = LS_DB_DRUG_INGREDIENT_TMP.druging_date_modified,LS_DB_DRUG_INGREDIENT.druging_date_created = LS_DB_DRUG_INGREDIENT_TMP.druging_date_created,LS_DB_DRUG_INGREDIENT.druging_ari_rec_id = LS_DB_DRUG_INGREDIENT_TMP.druging_ari_rec_id,LS_DB_DRUG_INGREDIENT.actsub_version = LS_DB_DRUG_INGREDIENT_TMP.actsub_version,LS_DB_DRUG_INGREDIENT.actsub_user_modified = LS_DB_DRUG_INGREDIENT_TMP.actsub_user_modified,LS_DB_DRUG_INGREDIENT.actsub_user_created = LS_DB_DRUG_INGREDIENT_TMP.actsub_user_created,LS_DB_DRUG_INGREDIENT.actsub_substance_termid_version = LS_DB_DRUG_INGREDIENT_TMP.actsub_substance_termid_version,LS_DB_DRUG_INGREDIENT.actsub_substance_termid = LS_DB_DRUG_INGREDIENT_TMP.actsub_substance_termid,LS_DB_DRUG_INGREDIENT.actsub_substance_strength_unit_de_ml = LS_DB_DRUG_INGREDIENT_TMP.actsub_substance_strength_unit_de_ml,LS_DB_DRUG_INGREDIENT.actsub_substance_strength_unit = LS_DB_DRUG_INGREDIENT_TMP.actsub_substance_strength_unit,LS_DB_DRUG_INGREDIENT.actsub_substance_strength = LS_DB_DRUG_INGREDIENT_TMP.actsub_substance_strength,LS_DB_DRUG_INGREDIENT.actsub_spr_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_spr_id,LS_DB_DRUG_INGREDIENT.actsub_record_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_record_id,LS_DB_DRUG_INGREDIENT.actsub_primart_active = LS_DB_DRUG_INGREDIENT_TMP.actsub_primart_active,LS_DB_DRUG_INGREDIENT.actsub_kdd_code = LS_DB_DRUG_INGREDIENT_TMP.actsub_kdd_code,LS_DB_DRUG_INGREDIENT.actsub_inq_rec_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_inq_rec_id,LS_DB_DRUG_INGREDIENT.actsub_fk_ad_rec_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_fk_ad_rec_id,LS_DB_DRUG_INGREDIENT.actsub_ext_clob_fld = LS_DB_DRUG_INGREDIENT_TMP.actsub_ext_clob_fld,LS_DB_DRUG_INGREDIENT.actsub_entity_updated = LS_DB_DRUG_INGREDIENT_TMP.actsub_entity_updated,LS_DB_DRUG_INGREDIENT.actsub_date_modified = LS_DB_DRUG_INGREDIENT_TMP.actsub_date_modified,LS_DB_DRUG_INGREDIENT.actsub_date_created = LS_DB_DRUG_INGREDIENT_TMP.actsub_date_created,LS_DB_DRUG_INGREDIENT.actsub_comp_rec_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_comp_rec_id,LS_DB_DRUG_INGREDIENT.actsub_clinica_drug_code_jpn = LS_DB_DRUG_INGREDIENT_TMP.actsub_clinica_drug_code_jpn,LS_DB_DRUG_INGREDIENT.actsub_cas_number = LS_DB_DRUG_INGREDIENT_TMP.actsub_cas_number,LS_DB_DRUG_INGREDIENT.actsub_ari_rec_id = LS_DB_DRUG_INGREDIENT_TMP.actsub_ari_rec_id,LS_DB_DRUG_INGREDIENT.actsub_activesubstancename_lang = LS_DB_DRUG_INGREDIENT_TMP.actsub_activesubstancename_lang,LS_DB_DRUG_INGREDIENT.actsub_activesubstancename = LS_DB_DRUG_INGREDIENT_TMP.actsub_activesubstancename,LS_DB_DRUG_INGREDIENT.actsub_activesubstance_lang = LS_DB_DRUG_INGREDIENT_TMP.actsub_activesubstance_lang,
LS_DB_DRUG_INGREDIENT.PROCESSING_DT = LS_DB_DRUG_INGREDIENT_TMP.PROCESSING_DT,
LS_DB_DRUG_INGREDIENT.receipt_id     =LS_DB_DRUG_INGREDIENT_TMP.receipt_id    ,
LS_DB_DRUG_INGREDIENT.case_no        =LS_DB_DRUG_INGREDIENT_TMP.case_no           ,
LS_DB_DRUG_INGREDIENT.case_version   =LS_DB_DRUG_INGREDIENT_TMP.case_version      ,
LS_DB_DRUG_INGREDIENT.version_no     =LS_DB_DRUG_INGREDIENT_TMP.version_no        ,
LS_DB_DRUG_INGREDIENT.user_modified  =LS_DB_DRUG_INGREDIENT_TMP.user_modified     ,
LS_DB_DRUG_INGREDIENT.date_modified  =LS_DB_DRUG_INGREDIENT_TMP.date_modified     ,
LS_DB_DRUG_INGREDIENT.expiry_date    =LS_DB_DRUG_INGREDIENT_TMP.expiry_date       ,
LS_DB_DRUG_INGREDIENT.created_by     =LS_DB_DRUG_INGREDIENT_TMP.created_by        ,
LS_DB_DRUG_INGREDIENT.created_dt     =LS_DB_DRUG_INGREDIENT_TMP.created_dt        ,
LS_DB_DRUG_INGREDIENT.load_ts        =LS_DB_DRUG_INGREDIENT_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP 
WHERE 	LS_DB_DRUG_INGREDIENT.INTEGRATION_ID = LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_INGREDIENT_TMP.PROCESSING_DT = LS_DB_DRUG_INGREDIENT.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DRUG_INGREDIENT.druging_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_INGREDIENT.actsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DRUG_INGREDIENT_TMP.druging_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_INGREDIENT_TMP.actsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID ,LS_DB_DRUG_INGREDIENT.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP 
ON LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID=LS_DB_DRUG_INGREDIENT_TMP.actsub_RECORD_ID
AND LS_DB_DRUG_INGREDIENT.INTEGRATION_ID = LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID 
where LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID  is null AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID in (select actsub_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP )
) TMP where LS_DB_DRUG_INGREDIENT.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID ,LS_DB_DRUG_INGREDIENT.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP 
    ON LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID=LS_DB_DRUG_INGREDIENT_TMP.actsub_RECORD_ID
    AND LS_DB_DRUG_INGREDIENT.INTEGRATION_ID = LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID 
    where LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID  is null AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID in (select actsub_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT
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
integration_id ,druging_user_modified,
druging_user_created,
druging_substance_name,
druging_strength_unit,
druging_strength,
druging_spr_id,
druging_record_id,
druging_inq_rec_id,
druging_fk_ad_rec_id,
druging_date_modified,
druging_date_created,
druging_ari_rec_id,
actsub_version,
actsub_user_modified,
actsub_user_created,
actsub_substance_termid_version,
actsub_substance_termid,
actsub_substance_strength_unit_de_ml,
actsub_substance_strength_unit,
actsub_substance_strength,
actsub_spr_id,
actsub_record_id,
actsub_primart_active,
actsub_kdd_code,
actsub_inq_rec_id,
actsub_fk_ad_rec_id,
actsub_ext_clob_fld,
actsub_entity_updated,
actsub_date_modified,
actsub_date_created,
actsub_comp_rec_id,
actsub_clinica_drug_code_jpn,
actsub_cas_number,
actsub_ari_rec_id,
actsub_activesubstancename_lang,
actsub_activesubstancename,
actsub_activesubstance_lang)
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
integration_id ,druging_user_modified,
druging_user_created,
druging_substance_name,
druging_strength_unit,
druging_strength,
druging_spr_id,
druging_record_id,
druging_inq_rec_id,
druging_fk_ad_rec_id,
druging_date_modified,
druging_date_created,
druging_ari_rec_id,
actsub_version,
actsub_user_modified,
actsub_user_created,
actsub_substance_termid_version,
actsub_substance_termid,
actsub_substance_strength_unit_de_ml,
actsub_substance_strength_unit,
actsub_substance_strength,
actsub_spr_id,
actsub_record_id,
actsub_primart_active,
actsub_kdd_code,
actsub_inq_rec_id,
actsub_fk_ad_rec_id,
actsub_ext_clob_fld,
actsub_entity_updated,
actsub_date_modified,
actsub_date_created,
actsub_comp_rec_id,
actsub_clinica_drug_code_jpn,
actsub_cas_number,
actsub_ari_rec_id,
actsub_activesubstancename_lang,
actsub_activesubstancename,
actsub_activesubstance_lang
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_INGREDIENT.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_INGREDIENT_TMP.PROCESSING_DT)
AND LS_DB_DRUG_INGREDIENT.INTEGRATION_ID = LS_DB_DRUG_INGREDIENT_TMP.INTEGRATION_ID
AND LS_DB_DRUG_INGREDIENT.actsub_RECORD_ID = LS_DB_DRUG_INGREDIENT_TMP.actsub_RECORD_ID
AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DRUG_INGREDIENT.druging_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DRUG_INGREDIENT.actsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DRUG_INGREDIENT_TMP.druging_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DRUG_INGREDIENT_TMP.actsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT TGT
WHERE  ( actsub_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_substance') OR druging_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_ingredients')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( actsub_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_substance') OR druging_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INGREDIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_ingredients')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_INGREDIENT'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_INGREDIENT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INGREDIENT'),
	'LSDB','Case','LS_DB_DRUG_INGREDIENT',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_INGREDIENT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
