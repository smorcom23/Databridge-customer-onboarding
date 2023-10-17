
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LIB()
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
ROW_WID           NUMBER(38,0),
FUNCTIONAL_AREA        VARCHAR(25),
ENTITY_NAME   VARCHAR(25),
TARGET_TABLE_NAME   VARCHAR(100),
LOAD_TS              TIMESTAMP_NTZ(9),
LOAD_START_TS               TIMESTAMP_NTZ(9),
LOAD_END_TS   TIMESTAMP_NTZ(9),
REC_READ_CNT NUMBER(38,0),
REC_PROCESSED_CNT    NUMBER(38,0),
ERROR_REC_CNT              NUMBER(38,0),
ERROR_DETAILS VARCHAR(8000),
LOAD_STATUS   VARCHAR(15),
CHANGED_REC_SET        VARIANT);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LIB'),
                'LSDB','Case','LS_DB_PRODUCT_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PRODUCT_LIB' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PRODUCT_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_product' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_aesi' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_aesi WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_category' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_category WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_country' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_country WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_device' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_device WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_group' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_group_search' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group_search WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_license' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_license WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_tradename' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_tradename WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_trade_brand_name' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('5015','5061','709','805','816','9741')
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
LSMV_CASE_NO_SUBSET_1 as
(

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_product_grp_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_AGX_TRADE_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_license WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_PRODUCT_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_license WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_tradename WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_agx_product_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_tradename WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_aesi WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_pro_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_aesi WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_device WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_product_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_device WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_category WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_agx_trade_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_category WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_country WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT FK_APT_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_country WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
),LSMV_CASE_NO_SUBSET as
(

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group_search WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_product_grp_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group_search WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT FK_AGX_TRADE_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_group WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_license WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT FK_PRODUCT_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_license WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_tradename WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_agx_product_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_tradename WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_aesi WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_pro_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_aesi WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_device WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_product_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_device WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_category WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_agx_trade_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_category WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_country WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT FK_APT_REC_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_country WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
)
, lsmv_product_country_SUBSET AS 
(
select * from 
    (SELECT  
    approval_id  prdcntry_approval_id,approval_no  prdcntry_approval_no,approval_type  prdcntry_approval_type,country_code  prdcntry_country_code,date_created  prdcntry_date_created,date_modified  prdcntry_date_modified,fk_apt_rec_id  prdcntry_fk_apt_rec_id,record_id  prdcntry_record_id,spr_id  prdcntry_spr_id,user_created  prdcntry_user_created,user_modified  prdcntry_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_country
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_APT_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_country')
  ) where REC_RANK=1 )
  , lsmv_product_device_SUBSET AS 
(
select * from 
    (SELECT  
    allergenicity_indicator  prddvc_allergenicity_indicator,brand_name  prddvc_brand_name,catalogue_number  prddvc_catalogue_number,centers_for_medicare  prddvc_centers_for_medicare,class_of_device  prddvc_class_of_device,code_system  prddvc_code_system,code_value  prddvc_code_value,common_dev_name  prddvc_common_dev_name,competent_authoriy  prddvc_competent_authoriy,date_created  prddvc_date_created,date_modified  prddvc_date_modified,department_division  prddvc_department_division,dev_compo_name  prddvc_dev_compo_name,dev_compotermid  prddvc_dev_compotermid,dev_compoversion  prddvc_dev_compoversion,dev_manf_site  prddvc_dev_manf_site,dev_nomendecode  prddvc_dev_nomendecode,dev_udi_type  prddvc_dev_udi_type,dev_udi_value  prddvc_dev_udi_value,device_manufactured_by  prddvc_device_manufactured_by,device_part  prddvc_device_part,device_pro_code  prddvc_device_pro_code,device_quantity  prddvc_device_quantity,device_type  prddvc_device_type,device_usage  prddvc_device_usage,disable_lsrims_fields  prddvc_disable_lsrims_fields,fda_reg_num  prddvc_fda_reg_num,fk_product_rec_id  prddvc_fk_product_rec_id,is_prod_combination  prddvc_is_prod_combination,manufactured_by  prddvc_manufactured_by,model_number  prddvc_model_number,nomen_sysothtext  prddvc_nomen_sysothtext,nomenclature_code  prddvc_nomenclature_code,prod_artg_num  prddvc_prod_artg_num,prod_autho_rep  prddvc_prod_autho_rep,prod_device_cenum  prddvc_prod_device_cenum,record_id  prddvc_record_id,spr_id  prddvc_spr_id,udi  prddvc_udi,user_created  prddvc_user_created,user_modified  prddvc_user_modified,warranty_period_months  prddvc_warranty_period_months,warranty_period_years  prddvc_warranty_period_years,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_device
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_product_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_device')
  ) where REC_RANK=1 )
  , lsmv_product_license_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdlicense_date_created,date_modified  prdlicense_date_modified,fk_product_id  prdlicense_fk_product_id,license_record_id  prdlicense_license_record_id,license_status  prdlicense_license_status,original_license_no  prdlicense_original_license_no,pref_name_code  prdlicense_pref_name_code,record_id  prdlicense_record_id,spr_id  prdlicense_spr_id,trade_name  prdlicense_trade_name,user_created  prdlicense_user_created,user_modified  prdlicense_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_license
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_PRODUCT_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_license')
  ) where REC_RANK=1 )
  , lsmv_product_tradename_SUBSET AS 
(
select * from 
    (SELECT  
    access_to_otc  prdtradname_access_to_otc,account_record_id_agent  prdtradname_account_record_id_agent,account_record_id_mah  prdtradname_account_record_id_mah,agent_name  prdtradname_agent_name,anda_no  prdtradname_anda_no,application_no  prdtradname_application_no,approval_date  prdtradname_approval_date,approval_desc  prdtradname_approval_desc,approval_no  prdtradname_approval_no,approval_start_date  prdtradname_approval_start_date,approval_status  prdtradname_approval_status,approval_submission_type  prdtradname_approval_submission_type,approval_type  prdtradname_approval_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='709' AND CD=CAST(approval_type AS VARCHAR(100)) )prdtradname_approval_type_de_ml , arisg_approval_id  prdtradname_arisg_approval_id,arisg_product_id  prdtradname_arisg_product_id,arisg_record_id  prdtradname_arisg_record_id,atc_code  prdtradname_atc_code,atc_vet_code  prdtradname_atc_vet_code,bio_ref_name  prdtradname_bio_ref_name,class_of_devices  prdtradname_class_of_devices,clinical_drug_code  prdtradname_clinical_drug_code,code_name  prdtradname_code_name,company_unit_record_id_mah  prdtradname_company_unit_record_id_mah,container_type  prdtradname_container_type,copy_approval_checked  prdtradname_copy_approval_checked,country_code  prdtradname_country_code,data_sheet_rec_id  prdtradname_data_sheet_rec_id,date_created  prdtradname_date_created,date_modified  prdtradname_date_modified,demographic_was_designed  prdtradname_demographic_was_designed,dev_commerdate  prdtradname_dev_commerdate,dev_marketedbefore  prdtradname_dev_marketedbefore,dev_marketmonths  prdtradname_dev_marketmonths,dev_marketyears  prdtradname_dev_marketyears,disable_lsrims_fields  prdtradname_disable_lsrims_fields,external_id  prdtradname_external_id,fei_no  prdtradname_fei_no,fk_agx_product_rec_id  prdtradname_fk_agx_product_rec_id,fk_app_rec_id  prdtradname_fk_app_rec_id,form_admin  prdtradname_form_admin,health_canada_idnum  prdtradname_health_canada_idnum,identify_not_body  prdtradname_identify_not_body,inteded_use_name  prdtradname_inteded_use_name,internal_prd_id  prdtradname_internal_prd_id,kdd_code  prdtradname_kdd_code,license_id  prdtradname_license_id,license_status  prdtradname_license_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5061' AND CD=CAST(license_status AS VARCHAR(100)) )prdtradname_license_status_de_ml , local_tradename  prdtradname_local_tradename,ltn_component_type  prdtradname_ltn_component_type,ltn_jddcode  prdtradname_ltn_jddcode,ltn_package_desc  prdtradname_ltn_package_desc,mah_name  prdtradname_mah_name,mah_type  prdtradname_mah_type,marketing_category  prdtradname_marketing_category,marketing_status  prdtradname_marketing_status,marketting_date  prdtradname_marketting_date,med_product_ltn  prdtradname_med_product_ltn,medicinal_product_name  prdtradname_medicinal_product_name,mpid  prdtradname_mpid,mpid_ver_date  prdtradname_mpid_ver_date,mpid_ver_number  prdtradname_mpid_ver_number,ndc_no  prdtradname_ndc_no,notified_cenum  prdtradname_notified_cenum,otc_altnte_id  prdtradname_otc_altnte_id,otc_risk_classfn  prdtradname_otc_risk_classfn,pmda_medicine_name  prdtradname_pmda_medicine_name,prod_drlcode  prdtradname_prod_drlcode,product_type  prdtradname_product_type,proprietary_name_suffix  prdtradname_proprietary_name_suffix,record_id  prdtradname_record_id,route_of_admin  prdtradname_route_of_admin,source  prdtradname_source,spr_id  prdtradname_spr_id,strength  prdtradname_strength,strength_unit  prdtradname_strength_unit,tiken  prdtradname_tiken,trade_id  prdtradname_trade_id,trade_ivdrtype  prdtradname_trade_ivdrtype,trade_mdrtype  prdtradname_trade_mdrtype,trademark_name  prdtradname_trademark_name,type_of_device  prdtradname_type_of_device,type_of_device_other  prdtradname_type_of_device_other,user_created  prdtradname_user_created,user_modified  prdtradname_user_modified,vaccine_type  prdtradname_vaccine_type,verified  prdtradname_verified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_tradename
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_agx_product_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_tradename')
  ) where REC_RANK=1 )
  , lsmv_product_category_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdcatg_date_created,date_modified  prdcatg_date_modified,fk_agx_trade_rec_id  prdcatg_fk_agx_trade_rec_id,ndc  prdcatg_ndc,pro_end_date  prdcatg_pro_end_date,pro_start_date  prdcatg_pro_start_date,product_attrib_value  prdcatg_product_attrib_value,product_category_type  prdcatg_product_category_type,product_number  prdcatg_product_number,record_id  prdcatg_record_id,spr_id  prdcatg_spr_id,user_created  prdcatg_user_created,user_modified  prdcatg_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_category
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_agx_trade_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_category')
  ) where REC_RANK=1 )
  , lsmv_product_group_SUBSET AS 
(
select * from 
    (SELECT  
    active  prdgrp_active,activeing_search_result  prdgrp_activeing_search_result,assign_to  prdgrp_assign_to,assigned_to  prdgrp_assigned_to,date_created  prdgrp_date_created,date_modified  prdgrp_date_modified,description  prdgrp_description,end_date  prdgrp_end_date,product_coding_level  prdgrp_product_coding_level,product_group_name  prdgrp_product_group_name,product_search_result  prdgrp_product_search_result,record_id  prdgrp_record_id,satrt_date  prdgrp_satrt_date,search_type  prdgrp_search_type,spr_id  prdgrp_spr_id,tradename_search_result  prdgrp_tradename_search_result,user_created  prdgrp_user_created,user_modified  prdgrp_user_modified,value prdgrp_PRODUCT_RECORD_ID,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_group, LATERAL STRTOK_SPLIT_TO_TABLE(LSMV_PRODUCT_GROUP.PRODUCT_SEARCH_RESULT, ',')
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group')
  ) where REC_RANK=1 )
  , lsmv_product_SUBSET AS 
(
select * from 
    (SELECT  
    access_to_otc  prd_access_to_otc,account_record_id  prd_account_record_id,active_ing_record_id  prd_active_ing_record_id,ag_prod_sync_old_name  prd_ag_prod_sync_old_name,ag_prod_sync_operation  prd_ag_prod_sync_operation,allergenicity_indicator  prd_allergenicity_indicator,arisg_record_id  prd_arisg_record_id,atc_code  prd_atc_code,atc_vet_code  prd_atc_vet_code,auto_approval  prd_auto_approval,auto_labelling_active  prd_auto_labelling_active,black_traingle_product  prd_black_traingle_product,case_processing_system  prd_case_processing_system,catalogue_number  prd_catalogue_number,center  prd_center,centers_for_medicare  prd_centers_for_medicare,class_of_device  prd_class_of_device,combined_cdc  prd_combined_cdc,combined_pharma_dose  prd_combined_pharma_dose,company_name_part  prd_company_name_part,company_product  prd_company_product,conflict_comments  prd_conflict_comments,container_size  prd_container_size,cpd_jddcode  prd_cpd_jddcode,created_date  prd_created_date,date_created  prd_date_created,date_modified  prd_date_modified,department_division  prd_department_division,device_part  prd_device_part,device_quantity  prd_device_quantity,device_usage  prd_device_usage,disable_lsrims_fields  prd_disable_lsrims_fields,drl_code  prd_drl_code,external_app_updated_date  prd_external_app_updated_date,external_id  prd_external_id,fda_registration_number  prd_fda_registration_number,fei_no  prd_fei_no,fk_base_prd_rec_id  prd_fk_base_prd_rec_id,form_admin  prd_form_admin,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='805' AND CD=CAST(form_admin AS VARCHAR(100)) )prd_form_admin_de_ml , formulation_part  prd_formulation_part,future_product  prd_future_product,generic_name  prd_generic_name,generic_namecn  prd_generic_namecn,gmdn_code  prd_gmdn_code,impid_cross_ref  prd_impid_cross_ref,inteded_use_name  prd_inteded_use_name,internal_drug_code  prd_internal_drug_code,international_birth_date  prd_international_birth_date,kdd_code  prd_kdd_code,keywords  prd_keywords,language  prd_language,listednessgrp_to  prd_listednessgrp_to,lsmv_product_id_for_litpro  prd_lsmv_product_id_for_litpro,made_by  prd_made_by,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='816' AND CD=CAST(made_by AS VARCHAR(100)) )prd_made_by_de_ml , manufactured_by  prd_manufactured_by,manufacturer_group  prd_manufacturer_group,med_product_cpd  prd_med_product_cpd,mfr_name  prd_mfr_name,model_number  prd_model_number,mpid  prd_mpid,mpid_ver_date  prd_mpid_ver_date,mpid_ver_number  prd_mpid_ver_number,multiple_ingredients  prd_multiple_ingredients,nlp_corpus_sync_status  prd_nlp_corpus_sync_status,notified_body_ident_no  prd_notified_body_ident_no,otc_risk_classfn  prd_otc_risk_classfn,pack_part  prd_pack_part,package_description  prd_package_description,pai  prd_pai,part_no  prd_part_no,pharma_dose_form_part  prd_pharma_dose_form_part,pharma_dose_form_term_id  prd_pharma_dose_form_term_id,pmda_medicine_name  prd_pmda_medicine_name,pref_name_code  prd_pref_name_code,prefered_code  prd_prefered_code,pro_component_type  prd_pro_component_type,prod_device_name  prd_prod_device_name,prod_intdev_birthdate  prd_prod_intdev_birthdate,prod_invented_name  prd_prod_invented_name,prod_vigilancetype  prd_prod_vigilancetype,prod_whodd_decode  prd_prod_whodd_decode,product_active  prd_product_active,product_approval_status  prd_product_approval_status,product_class  prd_product_class,product_description  prd_product_description,product_group  prd_product_group,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9741' AND CD=CAST(product_group AS VARCHAR(100)) )prd_product_group_de_ml , product_id  prd_product_id,product_name  prd_product_name,product_name_reported_chinese  prd_product_name_reported_chinese,product_name_upper  prd_product_name_upper,product_status  prd_product_status,product_type  prd_product_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5015' AND CD=CAST(product_type AS VARCHAR(100)) )prd_product_type_de_ml , product_url  prd_product_url,product_used_in  prd_product_used_in,product_version  prd_product_version,qc_review  prd_qc_review,reason_for_deactive  prd_reason_for_deactive,record_id  prd_record_id,redact_flag  prd_redact_flag,rm_flag  prd_rm_flag,route_of_admin  prd_route_of_admin,route_of_admin_term_id  prd_route_of_admin_term_id,sim_pro_flag  prd_sim_pro_flag,spr_id  prd_spr_id,staging_status  prd_staging_status,strength  prd_strength,strength_flag  prd_strength_flag,strength_unit  prd_strength_unit,study_product_type  prd_study_product_type,subject_to_risk  prd_subject_to_risk,synchonization_date  prd_synchonization_date,target_population_part  prd_target_population_part,task_flag  prd_task_flag,udi  prd_udi,unit_of_presentation  prd_unit_of_presentation,unit_price  prd_unit_price,unstructured_strength  prd_unstructured_strength,user_created  prd_user_created,user_modified  prd_user_modified,vaccine_type  prd_vaccine_type,version  prd_version,viewpdfflag  prd_viewpdfflag,warranty_period_months  prd_warranty_period_months,warranty_period_years  prd_warranty_period_years,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product')
  ) where REC_RANK=1 )
  , lsmv_trade_brand_name_SUBSET AS 
(
select * from 
    (SELECT  
    account_id  trdbrand_account_id,account_record_id  trdbrand_account_record_id,date_created  trdbrand_date_created,date_modified  trdbrand_date_modified,fei_no  trdbrand_fei_no,firm_function  trdbrand_firm_function,firm_name  trdbrand_firm_name,fk_agx_trade_rec_id  trdbrand_fk_agx_trade_rec_id,record_id  trdbrand_record_id,spr_id  trdbrand_spr_id,user_created  trdbrand_user_created,user_modified  trdbrand_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_trade_brand_name
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_AGX_TRADE_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_trade_brand_name')
  ) where REC_RANK=1 )
  , lsmv_product_group_search_SUBSET AS 
(
select * from 
    (SELECT  
    condition  prdgrpsrch_condition,date_created  prdgrpsrch_date_created,date_modified  prdgrpsrch_date_modified,date_value  prdgrpsrch_date_value,decode_values  prdgrpsrch_decode_values,field_name  prdgrpsrch_field_name,fk_product_grp_rec_id  prdgrpsrch_fk_product_grp_rec_id,record_id  prdgrpsrch_record_id,spr_id  prdgrpsrch_spr_id,test  prdgrpsrch_test,to_date_value  prdgrpsrch_to_date_value,user_created  prdgrpsrch_user_created,user_modified  prdgrpsrch_user_modified,value  prdgrpsrch_value,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_group_search
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_product_grp_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group_search')
  ) where REC_RANK=1 )
  , lsmv_product_aesi_SUBSET AS 
(
select * from 
    (SELECT  
    aesi_broad_narrow  prdaesi_aesi_broad_narrow,aesi_smqcmq_code  prdaesi_aesi_smqcmq_code,aesi_smqcmq_term  prdaesi_aesi_smqcmq_term,date_created  prdaesi_date_created,date_modified  prdaesi_date_modified,fk_dsofaesi_rec_id  prdaesi_fk_dsofaesi_rec_id,fk_pro_rec_id  prdaesi_fk_pro_rec_id,is_fatal  prdaesi_is_fatal,is_lifethreatening  prdaesi_is_lifethreatening,is_oth_seriouness  prdaesi_is_oth_seriouness,meddra_code  prdaesi_meddra_code,meddra_llt_code  prdaesi_meddra_llt_code,meddra_pt_term  prdaesi_meddra_pt_term,meddra_version  prdaesi_meddra_version,record_id  prdaesi_record_id,soc_name  prdaesi_soc_name,spr_id  prdaesi_spr_id,user_created  prdaesi_user_created,user_modified  prdaesi_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_aesi
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_pro_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_aesi')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_product_country_SUBSET.prdcntry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_category_SUBSET.prdcatg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_device_SUBSET.prddvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_aesi_SUBSET.prdaesi_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_SUBSET.prd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_tradename_SUBSET.prdtradname_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_license_SUBSET.prdlicense_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_group_SUBSET.prdgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_trade_brand_name_SUBSET.trdbrand_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_group_search_SUBSET.prdgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_product_SUBSET.prd_USER_MODIFIED USER_MODIFIED,lsmv_product_SUBSET.prd_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE ,lsmv_product_SUBSET.prd_USER_CREATED CREATED_BY,lsmv_product_SUBSET.prd_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_trade_brand_name_SUBSET.trdbrand_user_modified  ,lsmv_trade_brand_name_SUBSET.trdbrand_user_created  ,lsmv_trade_brand_name_SUBSET.trdbrand_spr_id  ,lsmv_trade_brand_name_SUBSET.trdbrand_record_id  ,lsmv_trade_brand_name_SUBSET.trdbrand_fk_agx_trade_rec_id  ,lsmv_trade_brand_name_SUBSET.trdbrand_firm_name  ,lsmv_trade_brand_name_SUBSET.trdbrand_firm_function  ,lsmv_trade_brand_name_SUBSET.trdbrand_fei_no  ,lsmv_trade_brand_name_SUBSET.trdbrand_date_modified  ,lsmv_trade_brand_name_SUBSET.trdbrand_date_created  ,lsmv_trade_brand_name_SUBSET.trdbrand_account_record_id  ,lsmv_trade_brand_name_SUBSET.trdbrand_account_id  ,lsmv_product_SUBSET.prd_warranty_period_years  ,lsmv_product_SUBSET.prd_warranty_period_months  ,lsmv_product_SUBSET.prd_viewpdfflag  ,lsmv_product_SUBSET.prd_version  ,lsmv_product_SUBSET.prd_vaccine_type  ,lsmv_product_SUBSET.prd_user_modified  ,lsmv_product_SUBSET.prd_user_created  ,lsmv_product_SUBSET.prd_unstructured_strength  ,lsmv_product_SUBSET.prd_unit_price  ,lsmv_product_SUBSET.prd_unit_of_presentation  ,lsmv_product_SUBSET.prd_udi  ,lsmv_product_SUBSET.prd_task_flag  ,lsmv_product_SUBSET.prd_target_population_part  ,lsmv_product_SUBSET.prd_synchonization_date  ,lsmv_product_SUBSET.prd_subject_to_risk  ,lsmv_product_SUBSET.prd_study_product_type  ,lsmv_product_SUBSET.prd_strength_unit  ,lsmv_product_SUBSET.prd_strength_flag  ,lsmv_product_SUBSET.prd_strength  ,lsmv_product_SUBSET.prd_staging_status  ,lsmv_product_SUBSET.prd_spr_id  ,lsmv_product_SUBSET.prd_sim_pro_flag  ,lsmv_product_SUBSET.prd_route_of_admin_term_id  ,lsmv_product_SUBSET.prd_route_of_admin  ,lsmv_product_SUBSET.prd_rm_flag  ,lsmv_product_SUBSET.prd_redact_flag  ,lsmv_product_SUBSET.prd_record_id  ,lsmv_product_SUBSET.prd_reason_for_deactive  ,lsmv_product_SUBSET.prd_qc_review  ,lsmv_product_SUBSET.prd_product_version  ,lsmv_product_SUBSET.prd_product_used_in  ,lsmv_product_SUBSET.prd_product_url  ,lsmv_product_SUBSET.prd_product_type_de_ml  ,lsmv_product_SUBSET.prd_product_type  ,lsmv_product_SUBSET.prd_product_status  ,lsmv_product_SUBSET.prd_product_name_upper  ,lsmv_product_SUBSET.prd_product_name_reported_chinese  ,lsmv_product_SUBSET.prd_product_name  ,lsmv_product_SUBSET.prd_product_id  ,lsmv_product_SUBSET.prd_product_group_de_ml  ,lsmv_product_SUBSET.prd_product_group  ,lsmv_product_SUBSET.prd_product_description  ,lsmv_product_SUBSET.prd_product_class  ,lsmv_product_SUBSET.prd_product_approval_status  ,lsmv_product_SUBSET.prd_product_active  ,lsmv_product_SUBSET.prd_prod_whodd_decode  ,lsmv_product_SUBSET.prd_prod_vigilancetype  ,lsmv_product_SUBSET.prd_prod_invented_name  ,lsmv_product_SUBSET.prd_prod_intdev_birthdate  ,lsmv_product_SUBSET.prd_prod_device_name  ,lsmv_product_SUBSET.prd_pro_component_type  ,lsmv_product_SUBSET.prd_prefered_code  ,lsmv_product_SUBSET.prd_pref_name_code  ,lsmv_product_SUBSET.prd_pmda_medicine_name  ,lsmv_product_SUBSET.prd_pharma_dose_form_term_id  ,lsmv_product_SUBSET.prd_pharma_dose_form_part  ,lsmv_product_SUBSET.prd_part_no  ,lsmv_product_SUBSET.prd_pai  ,lsmv_product_SUBSET.prd_package_description  ,lsmv_product_SUBSET.prd_pack_part  ,lsmv_product_SUBSET.prd_otc_risk_classfn  ,lsmv_product_SUBSET.prd_notified_body_ident_no  ,lsmv_product_SUBSET.prd_nlp_corpus_sync_status  ,lsmv_product_SUBSET.prd_multiple_ingredients  ,lsmv_product_SUBSET.prd_mpid_ver_number  ,lsmv_product_SUBSET.prd_mpid_ver_date  ,lsmv_product_SUBSET.prd_mpid  ,lsmv_product_SUBSET.prd_model_number  ,lsmv_product_SUBSET.prd_mfr_name  ,lsmv_product_SUBSET.prd_med_product_cpd  ,lsmv_product_SUBSET.prd_manufacturer_group  ,lsmv_product_SUBSET.prd_manufactured_by  ,lsmv_product_SUBSET.prd_made_by_de_ml  ,lsmv_product_SUBSET.prd_made_by  ,lsmv_product_SUBSET.prd_lsmv_product_id_for_litpro  ,lsmv_product_SUBSET.prd_listednessgrp_to  ,lsmv_product_SUBSET.prd_language  ,lsmv_product_SUBSET.prd_keywords  ,lsmv_product_SUBSET.prd_kdd_code  ,lsmv_product_SUBSET.prd_international_birth_date  ,lsmv_product_SUBSET.prd_internal_drug_code  ,lsmv_product_SUBSET.prd_inteded_use_name  ,lsmv_product_SUBSET.prd_impid_cross_ref  ,lsmv_product_SUBSET.prd_gmdn_code  ,lsmv_product_SUBSET.prd_generic_namecn  ,lsmv_product_SUBSET.prd_generic_name  ,lsmv_product_SUBSET.prd_future_product  ,lsmv_product_SUBSET.prd_formulation_part  ,lsmv_product_SUBSET.prd_form_admin_de_ml  ,lsmv_product_SUBSET.prd_form_admin  ,lsmv_product_SUBSET.prd_fk_base_prd_rec_id  ,lsmv_product_SUBSET.prd_fei_no  ,lsmv_product_SUBSET.prd_fda_registration_number  ,lsmv_product_SUBSET.prd_external_id  ,lsmv_product_SUBSET.prd_external_app_updated_date  ,lsmv_product_SUBSET.prd_drl_code  ,lsmv_product_SUBSET.prd_disable_lsrims_fields  ,lsmv_product_SUBSET.prd_device_usage  ,lsmv_product_SUBSET.prd_device_quantity  ,lsmv_product_SUBSET.prd_device_part  ,lsmv_product_SUBSET.prd_department_division  ,lsmv_product_SUBSET.prd_date_modified  ,lsmv_product_SUBSET.prd_date_created  ,lsmv_product_SUBSET.prd_created_date  ,lsmv_product_SUBSET.prd_cpd_jddcode  ,lsmv_product_SUBSET.prd_container_size  ,lsmv_product_SUBSET.prd_conflict_comments  ,lsmv_product_SUBSET.prd_company_product  ,lsmv_product_SUBSET.prd_company_name_part  ,lsmv_product_SUBSET.prd_combined_pharma_dose  ,lsmv_product_SUBSET.prd_combined_cdc  ,lsmv_product_SUBSET.prd_class_of_device  ,lsmv_product_SUBSET.prd_centers_for_medicare  ,lsmv_product_SUBSET.prd_center  ,lsmv_product_SUBSET.prd_catalogue_number  ,lsmv_product_SUBSET.prd_case_processing_system  ,lsmv_product_SUBSET.prd_black_traingle_product  ,lsmv_product_SUBSET.prd_auto_labelling_active  ,lsmv_product_SUBSET.prd_auto_approval  ,lsmv_product_SUBSET.prd_atc_vet_code  ,lsmv_product_SUBSET.prd_atc_code  ,lsmv_product_SUBSET.prd_arisg_record_id  ,lsmv_product_SUBSET.prd_allergenicity_indicator  ,lsmv_product_SUBSET.prd_ag_prod_sync_operation  ,lsmv_product_SUBSET.prd_ag_prod_sync_old_name  ,lsmv_product_SUBSET.prd_active_ing_record_id  ,lsmv_product_SUBSET.prd_account_record_id  ,lsmv_product_SUBSET.prd_access_to_otc  ,lsmv_product_tradename_SUBSET.prdtradname_verified  ,lsmv_product_tradename_SUBSET.prdtradname_vaccine_type  ,lsmv_product_tradename_SUBSET.prdtradname_user_modified  ,lsmv_product_tradename_SUBSET.prdtradname_user_created  ,lsmv_product_tradename_SUBSET.prdtradname_type_of_device_other  ,lsmv_product_tradename_SUBSET.prdtradname_type_of_device  ,lsmv_product_tradename_SUBSET.prdtradname_trademark_name  ,lsmv_product_tradename_SUBSET.prdtradname_trade_mdrtype  ,lsmv_product_tradename_SUBSET.prdtradname_trade_ivdrtype  ,lsmv_product_tradename_SUBSET.prdtradname_trade_id  ,lsmv_product_tradename_SUBSET.prdtradname_tiken  ,lsmv_product_tradename_SUBSET.prdtradname_strength_unit  ,lsmv_product_tradename_SUBSET.prdtradname_strength  ,lsmv_product_tradename_SUBSET.prdtradname_spr_id  ,lsmv_product_tradename_SUBSET.prdtradname_source  ,lsmv_product_tradename_SUBSET.prdtradname_route_of_admin  ,lsmv_product_tradename_SUBSET.prdtradname_record_id  ,lsmv_product_tradename_SUBSET.prdtradname_proprietary_name_suffix  ,lsmv_product_tradename_SUBSET.prdtradname_product_type  ,lsmv_product_tradename_SUBSET.prdtradname_prod_drlcode  ,lsmv_product_tradename_SUBSET.prdtradname_pmda_medicine_name  ,lsmv_product_tradename_SUBSET.prdtradname_otc_risk_classfn  ,lsmv_product_tradename_SUBSET.prdtradname_otc_altnte_id  ,lsmv_product_tradename_SUBSET.prdtradname_notified_cenum  ,lsmv_product_tradename_SUBSET.prdtradname_ndc_no  ,lsmv_product_tradename_SUBSET.prdtradname_mpid_ver_number  ,lsmv_product_tradename_SUBSET.prdtradname_mpid_ver_date  ,lsmv_product_tradename_SUBSET.prdtradname_mpid  ,lsmv_product_tradename_SUBSET.prdtradname_medicinal_product_name  ,lsmv_product_tradename_SUBSET.prdtradname_med_product_ltn  ,lsmv_product_tradename_SUBSET.prdtradname_marketting_date  ,lsmv_product_tradename_SUBSET.prdtradname_marketing_status  ,lsmv_product_tradename_SUBSET.prdtradname_marketing_category  ,lsmv_product_tradename_SUBSET.prdtradname_mah_type  ,lsmv_product_tradename_SUBSET.prdtradname_mah_name  ,lsmv_product_tradename_SUBSET.prdtradname_ltn_package_desc  ,lsmv_product_tradename_SUBSET.prdtradname_ltn_jddcode  ,lsmv_product_tradename_SUBSET.prdtradname_ltn_component_type  ,lsmv_product_tradename_SUBSET.prdtradname_local_tradename  ,lsmv_product_tradename_SUBSET.prdtradname_license_status_de_ml  ,lsmv_product_tradename_SUBSET.prdtradname_license_status  ,lsmv_product_tradename_SUBSET.prdtradname_license_id  ,lsmv_product_tradename_SUBSET.prdtradname_kdd_code  ,lsmv_product_tradename_SUBSET.prdtradname_internal_prd_id  ,lsmv_product_tradename_SUBSET.prdtradname_inteded_use_name  ,lsmv_product_tradename_SUBSET.prdtradname_identify_not_body  ,lsmv_product_tradename_SUBSET.prdtradname_health_canada_idnum  ,lsmv_product_tradename_SUBSET.prdtradname_form_admin  ,lsmv_product_tradename_SUBSET.prdtradname_fk_app_rec_id  ,lsmv_product_tradename_SUBSET.prdtradname_fk_agx_product_rec_id  ,lsmv_product_tradename_SUBSET.prdtradname_fei_no  ,lsmv_product_tradename_SUBSET.prdtradname_external_id  ,lsmv_product_tradename_SUBSET.prdtradname_disable_lsrims_fields  ,lsmv_product_tradename_SUBSET.prdtradname_dev_marketyears  ,lsmv_product_tradename_SUBSET.prdtradname_dev_marketmonths  ,lsmv_product_tradename_SUBSET.prdtradname_dev_marketedbefore  ,lsmv_product_tradename_SUBSET.prdtradname_dev_commerdate  ,lsmv_product_tradename_SUBSET.prdtradname_demographic_was_designed  ,lsmv_product_tradename_SUBSET.prdtradname_date_modified  ,lsmv_product_tradename_SUBSET.prdtradname_date_created  ,lsmv_product_tradename_SUBSET.prdtradname_data_sheet_rec_id  ,lsmv_product_tradename_SUBSET.prdtradname_country_code  ,lsmv_product_tradename_SUBSET.prdtradname_copy_approval_checked  ,lsmv_product_tradename_SUBSET.prdtradname_container_type  ,lsmv_product_tradename_SUBSET.prdtradname_company_unit_record_id_mah  ,lsmv_product_tradename_SUBSET.prdtradname_code_name  ,lsmv_product_tradename_SUBSET.prdtradname_clinical_drug_code  ,lsmv_product_tradename_SUBSET.prdtradname_class_of_devices  ,lsmv_product_tradename_SUBSET.prdtradname_bio_ref_name  ,lsmv_product_tradename_SUBSET.prdtradname_atc_vet_code  ,lsmv_product_tradename_SUBSET.prdtradname_atc_code  ,lsmv_product_tradename_SUBSET.prdtradname_arisg_record_id  ,lsmv_product_tradename_SUBSET.prdtradname_arisg_product_id  ,lsmv_product_tradename_SUBSET.prdtradname_arisg_approval_id  ,lsmv_product_tradename_SUBSET.prdtradname_approval_type_de_ml  ,lsmv_product_tradename_SUBSET.prdtradname_approval_type  ,lsmv_product_tradename_SUBSET.prdtradname_approval_submission_type  ,lsmv_product_tradename_SUBSET.prdtradname_approval_status  ,lsmv_product_tradename_SUBSET.prdtradname_approval_start_date  ,lsmv_product_tradename_SUBSET.prdtradname_approval_no  ,lsmv_product_tradename_SUBSET.prdtradname_approval_desc  ,lsmv_product_tradename_SUBSET.prdtradname_approval_date  ,lsmv_product_tradename_SUBSET.prdtradname_application_no  ,lsmv_product_tradename_SUBSET.prdtradname_anda_no  ,lsmv_product_tradename_SUBSET.prdtradname_agent_name  ,lsmv_product_tradename_SUBSET.prdtradname_account_record_id_mah  ,lsmv_product_tradename_SUBSET.prdtradname_account_record_id_agent  ,lsmv_product_tradename_SUBSET.prdtradname_access_to_otc  ,lsmv_product_license_SUBSET.prdlicense_user_modified  ,lsmv_product_license_SUBSET.prdlicense_user_created  ,lsmv_product_license_SUBSET.prdlicense_trade_name  ,lsmv_product_license_SUBSET.prdlicense_spr_id  ,lsmv_product_license_SUBSET.prdlicense_record_id  ,lsmv_product_license_SUBSET.prdlicense_pref_name_code  ,lsmv_product_license_SUBSET.prdlicense_original_license_no  ,lsmv_product_license_SUBSET.prdlicense_license_status  ,lsmv_product_license_SUBSET.prdlicense_license_record_id  ,lsmv_product_license_SUBSET.prdlicense_fk_product_id  ,lsmv_product_license_SUBSET.prdlicense_date_modified  ,lsmv_product_license_SUBSET.prdlicense_date_created  ,lsmv_product_group_SUBSET.prdgrp_user_modified  ,lsmv_product_group_SUBSET.prdgrp_user_created  ,lsmv_product_group_SUBSET.prdgrp_tradename_search_result  ,lsmv_product_group_SUBSET.prdgrp_spr_id  ,lsmv_product_group_SUBSET.prdgrp_search_type  ,lsmv_product_group_SUBSET.prdgrp_satrt_date  ,lsmv_product_group_SUBSET.prdgrp_record_id  ,lsmv_product_group_SUBSET.prdgrp_product_search_result  ,lsmv_product_group_SUBSET.prdgrp_product_group_name  ,lsmv_product_group_SUBSET.prdgrp_product_coding_level  ,lsmv_product_group_SUBSET.prdgrp_end_date  ,lsmv_product_group_SUBSET.prdgrp_description  ,lsmv_product_group_SUBSET.prdgrp_date_modified  ,lsmv_product_group_SUBSET.prdgrp_date_created  ,lsmv_product_group_SUBSET.prdgrp_assigned_to  ,lsmv_product_group_SUBSET.prdgrp_assign_to  ,lsmv_product_group_SUBSET.prdgrp_activeing_search_result  ,lsmv_product_group_SUBSET.prdgrp_active  ,lsmv_product_group_search_SUBSET.prdgrpsrch_value  ,lsmv_product_group_search_SUBSET.prdgrpsrch_user_modified  ,lsmv_product_group_search_SUBSET.prdgrpsrch_user_created  ,lsmv_product_group_search_SUBSET.prdgrpsrch_to_date_value  ,lsmv_product_group_search_SUBSET.prdgrpsrch_test  ,lsmv_product_group_search_SUBSET.prdgrpsrch_spr_id  ,lsmv_product_group_search_SUBSET.prdgrpsrch_record_id  ,lsmv_product_group_search_SUBSET.prdgrpsrch_fk_product_grp_rec_id  ,lsmv_product_group_search_SUBSET.prdgrpsrch_field_name  ,lsmv_product_group_search_SUBSET.prdgrpsrch_decode_values  ,lsmv_product_group_search_SUBSET.prdgrpsrch_date_value  ,lsmv_product_group_search_SUBSET.prdgrpsrch_date_modified  ,lsmv_product_group_search_SUBSET.prdgrpsrch_date_created  ,lsmv_product_group_search_SUBSET.prdgrpsrch_condition  ,lsmv_product_device_SUBSET.prddvc_warranty_period_years  ,lsmv_product_device_SUBSET.prddvc_warranty_period_months  ,lsmv_product_device_SUBSET.prddvc_user_modified  ,lsmv_product_device_SUBSET.prddvc_user_created  ,lsmv_product_device_SUBSET.prddvc_udi  ,lsmv_product_device_SUBSET.prddvc_spr_id  ,lsmv_product_device_SUBSET.prddvc_record_id  ,lsmv_product_device_SUBSET.prddvc_prod_device_cenum  ,lsmv_product_device_SUBSET.prddvc_prod_autho_rep  ,lsmv_product_device_SUBSET.prddvc_prod_artg_num  ,lsmv_product_device_SUBSET.prddvc_nomenclature_code  ,lsmv_product_device_SUBSET.prddvc_nomen_sysothtext  ,lsmv_product_device_SUBSET.prddvc_model_number  ,lsmv_product_device_SUBSET.prddvc_manufactured_by  ,lsmv_product_device_SUBSET.prddvc_is_prod_combination  ,lsmv_product_device_SUBSET.prddvc_fk_product_rec_id  ,lsmv_product_device_SUBSET.prddvc_fda_reg_num  ,lsmv_product_device_SUBSET.prddvc_disable_lsrims_fields  ,lsmv_product_device_SUBSET.prddvc_device_usage  ,lsmv_product_device_SUBSET.prddvc_device_type  ,lsmv_product_device_SUBSET.prddvc_device_quantity  ,lsmv_product_device_SUBSET.prddvc_device_pro_code  ,lsmv_product_device_SUBSET.prddvc_device_part  ,lsmv_product_device_SUBSET.prddvc_device_manufactured_by  ,lsmv_product_device_SUBSET.prddvc_dev_udi_value  ,lsmv_product_device_SUBSET.prddvc_dev_udi_type  ,lsmv_product_device_SUBSET.prddvc_dev_nomendecode  ,lsmv_product_device_SUBSET.prddvc_dev_manf_site  ,lsmv_product_device_SUBSET.prddvc_dev_compoversion  ,lsmv_product_device_SUBSET.prddvc_dev_compotermid  ,lsmv_product_device_SUBSET.prddvc_dev_compo_name  ,lsmv_product_device_SUBSET.prddvc_department_division  ,lsmv_product_device_SUBSET.prddvc_date_modified  ,lsmv_product_device_SUBSET.prddvc_date_created  ,lsmv_product_device_SUBSET.prddvc_competent_authoriy  ,lsmv_product_device_SUBSET.prddvc_common_dev_name  ,lsmv_product_device_SUBSET.prddvc_code_value  ,lsmv_product_device_SUBSET.prddvc_code_system  ,lsmv_product_device_SUBSET.prddvc_class_of_device  ,lsmv_product_device_SUBSET.prddvc_centers_for_medicare  ,lsmv_product_device_SUBSET.prddvc_catalogue_number  ,lsmv_product_device_SUBSET.prddvc_brand_name  ,lsmv_product_device_SUBSET.prddvc_allergenicity_indicator  ,lsmv_product_country_SUBSET.prdcntry_user_modified  ,lsmv_product_country_SUBSET.prdcntry_user_created  ,lsmv_product_country_SUBSET.prdcntry_spr_id  ,lsmv_product_country_SUBSET.prdcntry_record_id  ,lsmv_product_country_SUBSET.prdcntry_fk_apt_rec_id  ,lsmv_product_country_SUBSET.prdcntry_date_modified  ,lsmv_product_country_SUBSET.prdcntry_date_created  ,lsmv_product_country_SUBSET.prdcntry_country_code  ,lsmv_product_country_SUBSET.prdcntry_approval_type  ,lsmv_product_country_SUBSET.prdcntry_approval_no  ,lsmv_product_country_SUBSET.prdcntry_approval_id  ,lsmv_product_category_SUBSET.prdcatg_user_modified  ,lsmv_product_category_SUBSET.prdcatg_user_created  ,lsmv_product_category_SUBSET.prdcatg_spr_id  ,lsmv_product_category_SUBSET.prdcatg_record_id  ,lsmv_product_category_SUBSET.prdcatg_product_number  ,lsmv_product_category_SUBSET.prdcatg_product_category_type  ,lsmv_product_category_SUBSET.prdcatg_product_attrib_value  ,lsmv_product_category_SUBSET.prdcatg_pro_start_date  ,lsmv_product_category_SUBSET.prdcatg_pro_end_date  ,lsmv_product_category_SUBSET.prdcatg_ndc  ,lsmv_product_category_SUBSET.prdcatg_fk_agx_trade_rec_id  ,lsmv_product_category_SUBSET.prdcatg_date_modified  ,lsmv_product_category_SUBSET.prdcatg_date_created  ,lsmv_product_aesi_SUBSET.prdaesi_user_modified  ,lsmv_product_aesi_SUBSET.prdaesi_user_created  ,lsmv_product_aesi_SUBSET.prdaesi_spr_id  ,lsmv_product_aesi_SUBSET.prdaesi_soc_name  ,lsmv_product_aesi_SUBSET.prdaesi_record_id  ,lsmv_product_aesi_SUBSET.prdaesi_meddra_version  ,lsmv_product_aesi_SUBSET.prdaesi_meddra_pt_term  ,lsmv_product_aesi_SUBSET.prdaesi_meddra_llt_code  ,lsmv_product_aesi_SUBSET.prdaesi_meddra_code  ,lsmv_product_aesi_SUBSET.prdaesi_is_oth_seriouness  ,lsmv_product_aesi_SUBSET.prdaesi_is_lifethreatening  ,lsmv_product_aesi_SUBSET.prdaesi_is_fatal  ,lsmv_product_aesi_SUBSET.prdaesi_fk_pro_rec_id  ,lsmv_product_aesi_SUBSET.prdaesi_fk_dsofaesi_rec_id  ,lsmv_product_aesi_SUBSET.prdaesi_date_modified  ,lsmv_product_aesi_SUBSET.prdaesi_date_created  ,lsmv_product_aesi_SUBSET.prdaesi_aesi_smqcmq_term  ,lsmv_product_aesi_SUBSET.prdaesi_aesi_smqcmq_code  ,lsmv_product_aesi_SUBSET.prdaesi_aesi_broad_narrow ,CONCAT(NVL(lsmv_product_country_SUBSET.prdcntry_RECORD_ID,-1),'||',NVL(lsmv_product_category_SUBSET.prdcatg_RECORD_ID,-1),'||',NVL(lsmv_product_device_SUBSET.prddvc_RECORD_ID,-1),'||',NVL(lsmv_product_aesi_SUBSET.prdaesi_RECORD_ID,-1),'||',NVL(lsmv_product_SUBSET.prd_RECORD_ID,-1),'||',NVL(lsmv_product_tradename_SUBSET.prdtradname_RECORD_ID,-1),'||',NVL(lsmv_product_license_SUBSET.prdlicense_RECORD_ID,-1),'||',NVL(lsmv_product_group_SUBSET.prdgrp_RECORD_ID,-1),'||',NVL(lsmv_trade_brand_name_SUBSET.trdbrand_RECORD_ID,-1),'||',NVL(lsmv_product_group_search_SUBSET.prdgrpsrch_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_product_SUBSET  LEFT JOIN lsmv_product_aesi_SUBSET ON lsmv_product_SUBSET.prd_RECORD_ID=lsmv_product_aesi_SUBSET.prdaesi_fk_pro_rec_id
                         LEFT JOIN lsmv_product_device_SUBSET ON lsmv_product_SUBSET.prd_RECORD_ID=lsmv_product_device_SUBSET.prddvc_fk_product_rec_id
                         LEFT JOIN lsmv_product_tradename_SUBSET ON lsmv_product_SUBSET.prd_RECORD_ID=lsmv_product_tradename_SUBSET.prdtradname_fk_agx_product_rec_id
                         LEFT JOIN lsmv_product_license_SUBSET ON lsmv_product_SUBSET.prd_RECORD_ID=lsmv_product_license_SUBSET.prdlicense_FK_PRODUCT_ID
                         LEFT JOIN LSMV_PRODUCT_GROUP_SUBSET ON lsmv_product_SUBSET.prd_RECORD_ID=LSMV_PRODUCT_GROUP_SUBSET.prdgrp_PRODUCT_RECORD_ID
                         LEFT JOIN lsmv_product_category_SUBSET ON lsmv_product_tradename_SUBSET.prdtradname_RECORD_ID=lsmv_product_category_SUBSET.prdcatg_fk_agx_trade_rec_id
                         LEFT JOIN lsmv_trade_brand_name_SUBSET ON lsmv_product_tradename_SUBSET.prdtradname_RECORD_ID=lsmv_trade_brand_name_SUBSET.trdbrand_FK_AGX_TRADE_REC_ID
                         LEFT JOIN lsmv_product_group_search_SUBSET ON lsmv_product_group_SUBSET.prdgrp_RECORD_ID=lsmv_product_group_search_SUBSET.prdgrpsrch_fk_product_grp_rec_id
                         LEFT JOIN lsmv_product_country_SUBSET ON lsmv_product_tradename_SUBSET.prdtradname_RECORD_ID=lsmv_product_country_SUBSET.prdcntry_FK_APT_REC_ID
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP)
where target_table_name='LS_DB_PRODUCT_LIB'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB   
SET LS_DB_PRODUCT_LIB.trdbrand_user_modified = LS_DB_PRODUCT_LIB_TMP.trdbrand_user_modified,LS_DB_PRODUCT_LIB.trdbrand_user_created = LS_DB_PRODUCT_LIB_TMP.trdbrand_user_created,LS_DB_PRODUCT_LIB.trdbrand_spr_id = LS_DB_PRODUCT_LIB_TMP.trdbrand_spr_id,LS_DB_PRODUCT_LIB.trdbrand_record_id = LS_DB_PRODUCT_LIB_TMP.trdbrand_record_id,LS_DB_PRODUCT_LIB.trdbrand_fk_agx_trade_rec_id = LS_DB_PRODUCT_LIB_TMP.trdbrand_fk_agx_trade_rec_id,LS_DB_PRODUCT_LIB.trdbrand_firm_name = LS_DB_PRODUCT_LIB_TMP.trdbrand_firm_name,LS_DB_PRODUCT_LIB.trdbrand_firm_function = LS_DB_PRODUCT_LIB_TMP.trdbrand_firm_function,LS_DB_PRODUCT_LIB.trdbrand_fei_no = LS_DB_PRODUCT_LIB_TMP.trdbrand_fei_no,LS_DB_PRODUCT_LIB.trdbrand_date_modified = LS_DB_PRODUCT_LIB_TMP.trdbrand_date_modified,LS_DB_PRODUCT_LIB.trdbrand_date_created = LS_DB_PRODUCT_LIB_TMP.trdbrand_date_created,LS_DB_PRODUCT_LIB.trdbrand_account_record_id = LS_DB_PRODUCT_LIB_TMP.trdbrand_account_record_id,LS_DB_PRODUCT_LIB.trdbrand_account_id = LS_DB_PRODUCT_LIB_TMP.trdbrand_account_id,LS_DB_PRODUCT_LIB.prd_warranty_period_years = LS_DB_PRODUCT_LIB_TMP.prd_warranty_period_years,LS_DB_PRODUCT_LIB.prd_warranty_period_months = LS_DB_PRODUCT_LIB_TMP.prd_warranty_period_months,LS_DB_PRODUCT_LIB.prd_viewpdfflag = LS_DB_PRODUCT_LIB_TMP.prd_viewpdfflag,LS_DB_PRODUCT_LIB.prd_version = LS_DB_PRODUCT_LIB_TMP.prd_version,LS_DB_PRODUCT_LIB.prd_vaccine_type = LS_DB_PRODUCT_LIB_TMP.prd_vaccine_type,LS_DB_PRODUCT_LIB.prd_user_modified = LS_DB_PRODUCT_LIB_TMP.prd_user_modified,LS_DB_PRODUCT_LIB.prd_user_created = LS_DB_PRODUCT_LIB_TMP.prd_user_created,LS_DB_PRODUCT_LIB.prd_unstructured_strength = LS_DB_PRODUCT_LIB_TMP.prd_unstructured_strength,LS_DB_PRODUCT_LIB.prd_unit_price = LS_DB_PRODUCT_LIB_TMP.prd_unit_price,LS_DB_PRODUCT_LIB.prd_unit_of_presentation = LS_DB_PRODUCT_LIB_TMP.prd_unit_of_presentation,LS_DB_PRODUCT_LIB.prd_udi = LS_DB_PRODUCT_LIB_TMP.prd_udi,LS_DB_PRODUCT_LIB.prd_task_flag = LS_DB_PRODUCT_LIB_TMP.prd_task_flag,LS_DB_PRODUCT_LIB.prd_target_population_part = LS_DB_PRODUCT_LIB_TMP.prd_target_population_part,LS_DB_PRODUCT_LIB.prd_synchonization_date = LS_DB_PRODUCT_LIB_TMP.prd_synchonization_date,LS_DB_PRODUCT_LIB.prd_subject_to_risk = LS_DB_PRODUCT_LIB_TMP.prd_subject_to_risk,LS_DB_PRODUCT_LIB.prd_study_product_type = LS_DB_PRODUCT_LIB_TMP.prd_study_product_type,LS_DB_PRODUCT_LIB.prd_strength_unit = LS_DB_PRODUCT_LIB_TMP.prd_strength_unit,LS_DB_PRODUCT_LIB.prd_strength_flag = LS_DB_PRODUCT_LIB_TMP.prd_strength_flag,LS_DB_PRODUCT_LIB.prd_strength = LS_DB_PRODUCT_LIB_TMP.prd_strength,LS_DB_PRODUCT_LIB.prd_staging_status = LS_DB_PRODUCT_LIB_TMP.prd_staging_status,LS_DB_PRODUCT_LIB.prd_spr_id = LS_DB_PRODUCT_LIB_TMP.prd_spr_id,LS_DB_PRODUCT_LIB.prd_sim_pro_flag = LS_DB_PRODUCT_LIB_TMP.prd_sim_pro_flag,LS_DB_PRODUCT_LIB.prd_route_of_admin_term_id = LS_DB_PRODUCT_LIB_TMP.prd_route_of_admin_term_id,LS_DB_PRODUCT_LIB.prd_route_of_admin = LS_DB_PRODUCT_LIB_TMP.prd_route_of_admin,LS_DB_PRODUCT_LIB.prd_rm_flag = LS_DB_PRODUCT_LIB_TMP.prd_rm_flag,LS_DB_PRODUCT_LIB.prd_redact_flag = LS_DB_PRODUCT_LIB_TMP.prd_redact_flag,LS_DB_PRODUCT_LIB.prd_record_id = LS_DB_PRODUCT_LIB_TMP.prd_record_id,LS_DB_PRODUCT_LIB.prd_reason_for_deactive = LS_DB_PRODUCT_LIB_TMP.prd_reason_for_deactive,LS_DB_PRODUCT_LIB.prd_qc_review = LS_DB_PRODUCT_LIB_TMP.prd_qc_review,LS_DB_PRODUCT_LIB.prd_product_version = LS_DB_PRODUCT_LIB_TMP.prd_product_version,LS_DB_PRODUCT_LIB.prd_product_used_in = LS_DB_PRODUCT_LIB_TMP.prd_product_used_in,LS_DB_PRODUCT_LIB.prd_product_url = LS_DB_PRODUCT_LIB_TMP.prd_product_url,LS_DB_PRODUCT_LIB.prd_product_type_de_ml = LS_DB_PRODUCT_LIB_TMP.prd_product_type_de_ml,LS_DB_PRODUCT_LIB.prd_product_type = LS_DB_PRODUCT_LIB_TMP.prd_product_type,LS_DB_PRODUCT_LIB.prd_product_status = LS_DB_PRODUCT_LIB_TMP.prd_product_status,LS_DB_PRODUCT_LIB.prd_product_name_upper = LS_DB_PRODUCT_LIB_TMP.prd_product_name_upper,LS_DB_PRODUCT_LIB.prd_product_name_reported_chinese = LS_DB_PRODUCT_LIB_TMP.prd_product_name_reported_chinese,LS_DB_PRODUCT_LIB.prd_product_name = LS_DB_PRODUCT_LIB_TMP.prd_product_name,LS_DB_PRODUCT_LIB.prd_product_id = LS_DB_PRODUCT_LIB_TMP.prd_product_id,LS_DB_PRODUCT_LIB.prd_product_group_de_ml = LS_DB_PRODUCT_LIB_TMP.prd_product_group_de_ml,LS_DB_PRODUCT_LIB.prd_product_group = LS_DB_PRODUCT_LIB_TMP.prd_product_group,LS_DB_PRODUCT_LIB.prd_product_description = LS_DB_PRODUCT_LIB_TMP.prd_product_description,LS_DB_PRODUCT_LIB.prd_product_class = LS_DB_PRODUCT_LIB_TMP.prd_product_class,LS_DB_PRODUCT_LIB.prd_product_approval_status = LS_DB_PRODUCT_LIB_TMP.prd_product_approval_status,LS_DB_PRODUCT_LIB.prd_product_active = LS_DB_PRODUCT_LIB_TMP.prd_product_active,LS_DB_PRODUCT_LIB.prd_prod_whodd_decode = LS_DB_PRODUCT_LIB_TMP.prd_prod_whodd_decode,LS_DB_PRODUCT_LIB.prd_prod_vigilancetype = LS_DB_PRODUCT_LIB_TMP.prd_prod_vigilancetype,LS_DB_PRODUCT_LIB.prd_prod_invented_name = LS_DB_PRODUCT_LIB_TMP.prd_prod_invented_name,LS_DB_PRODUCT_LIB.prd_prod_intdev_birthdate = LS_DB_PRODUCT_LIB_TMP.prd_prod_intdev_birthdate,LS_DB_PRODUCT_LIB.prd_prod_device_name = LS_DB_PRODUCT_LIB_TMP.prd_prod_device_name,LS_DB_PRODUCT_LIB.prd_pro_component_type = LS_DB_PRODUCT_LIB_TMP.prd_pro_component_type,LS_DB_PRODUCT_LIB.prd_prefered_code = LS_DB_PRODUCT_LIB_TMP.prd_prefered_code,LS_DB_PRODUCT_LIB.prd_pref_name_code = LS_DB_PRODUCT_LIB_TMP.prd_pref_name_code,LS_DB_PRODUCT_LIB.prd_pmda_medicine_name = LS_DB_PRODUCT_LIB_TMP.prd_pmda_medicine_name,LS_DB_PRODUCT_LIB.prd_pharma_dose_form_term_id = LS_DB_PRODUCT_LIB_TMP.prd_pharma_dose_form_term_id,LS_DB_PRODUCT_LIB.prd_pharma_dose_form_part = LS_DB_PRODUCT_LIB_TMP.prd_pharma_dose_form_part,LS_DB_PRODUCT_LIB.prd_part_no = LS_DB_PRODUCT_LIB_TMP.prd_part_no,LS_DB_PRODUCT_LIB.prd_pai = LS_DB_PRODUCT_LIB_TMP.prd_pai,LS_DB_PRODUCT_LIB.prd_package_description = LS_DB_PRODUCT_LIB_TMP.prd_package_description,LS_DB_PRODUCT_LIB.prd_pack_part = LS_DB_PRODUCT_LIB_TMP.prd_pack_part,LS_DB_PRODUCT_LIB.prd_otc_risk_classfn = LS_DB_PRODUCT_LIB_TMP.prd_otc_risk_classfn,LS_DB_PRODUCT_LIB.prd_notified_body_ident_no = LS_DB_PRODUCT_LIB_TMP.prd_notified_body_ident_no,LS_DB_PRODUCT_LIB.prd_nlp_corpus_sync_status = LS_DB_PRODUCT_LIB_TMP.prd_nlp_corpus_sync_status,LS_DB_PRODUCT_LIB.prd_multiple_ingredients = LS_DB_PRODUCT_LIB_TMP.prd_multiple_ingredients,LS_DB_PRODUCT_LIB.prd_mpid_ver_number = LS_DB_PRODUCT_LIB_TMP.prd_mpid_ver_number,LS_DB_PRODUCT_LIB.prd_mpid_ver_date = LS_DB_PRODUCT_LIB_TMP.prd_mpid_ver_date,LS_DB_PRODUCT_LIB.prd_mpid = LS_DB_PRODUCT_LIB_TMP.prd_mpid,LS_DB_PRODUCT_LIB.prd_model_number = LS_DB_PRODUCT_LIB_TMP.prd_model_number,LS_DB_PRODUCT_LIB.prd_mfr_name = LS_DB_PRODUCT_LIB_TMP.prd_mfr_name,LS_DB_PRODUCT_LIB.prd_med_product_cpd = LS_DB_PRODUCT_LIB_TMP.prd_med_product_cpd,LS_DB_PRODUCT_LIB.prd_manufacturer_group = LS_DB_PRODUCT_LIB_TMP.prd_manufacturer_group,LS_DB_PRODUCT_LIB.prd_manufactured_by = LS_DB_PRODUCT_LIB_TMP.prd_manufactured_by,LS_DB_PRODUCT_LIB.prd_made_by_de_ml = LS_DB_PRODUCT_LIB_TMP.prd_made_by_de_ml,LS_DB_PRODUCT_LIB.prd_made_by = LS_DB_PRODUCT_LIB_TMP.prd_made_by,LS_DB_PRODUCT_LIB.prd_lsmv_product_id_for_litpro = LS_DB_PRODUCT_LIB_TMP.prd_lsmv_product_id_for_litpro,LS_DB_PRODUCT_LIB.prd_listednessgrp_to = LS_DB_PRODUCT_LIB_TMP.prd_listednessgrp_to,LS_DB_PRODUCT_LIB.prd_language = LS_DB_PRODUCT_LIB_TMP.prd_language,LS_DB_PRODUCT_LIB.prd_keywords = LS_DB_PRODUCT_LIB_TMP.prd_keywords,LS_DB_PRODUCT_LIB.prd_kdd_code = LS_DB_PRODUCT_LIB_TMP.prd_kdd_code,LS_DB_PRODUCT_LIB.prd_international_birth_date = LS_DB_PRODUCT_LIB_TMP.prd_international_birth_date,LS_DB_PRODUCT_LIB.prd_internal_drug_code = LS_DB_PRODUCT_LIB_TMP.prd_internal_drug_code,LS_DB_PRODUCT_LIB.prd_inteded_use_name = LS_DB_PRODUCT_LIB_TMP.prd_inteded_use_name,LS_DB_PRODUCT_LIB.prd_impid_cross_ref = LS_DB_PRODUCT_LIB_TMP.prd_impid_cross_ref,LS_DB_PRODUCT_LIB.prd_gmdn_code = LS_DB_PRODUCT_LIB_TMP.prd_gmdn_code,LS_DB_PRODUCT_LIB.prd_generic_namecn = LS_DB_PRODUCT_LIB_TMP.prd_generic_namecn,LS_DB_PRODUCT_LIB.prd_generic_name = LS_DB_PRODUCT_LIB_TMP.prd_generic_name,LS_DB_PRODUCT_LIB.prd_future_product = LS_DB_PRODUCT_LIB_TMP.prd_future_product,LS_DB_PRODUCT_LIB.prd_formulation_part = LS_DB_PRODUCT_LIB_TMP.prd_formulation_part,LS_DB_PRODUCT_LIB.prd_form_admin_de_ml = LS_DB_PRODUCT_LIB_TMP.prd_form_admin_de_ml,LS_DB_PRODUCT_LIB.prd_form_admin = LS_DB_PRODUCT_LIB_TMP.prd_form_admin,LS_DB_PRODUCT_LIB.prd_fk_base_prd_rec_id = LS_DB_PRODUCT_LIB_TMP.prd_fk_base_prd_rec_id,LS_DB_PRODUCT_LIB.prd_fei_no = LS_DB_PRODUCT_LIB_TMP.prd_fei_no,LS_DB_PRODUCT_LIB.prd_fda_registration_number = LS_DB_PRODUCT_LIB_TMP.prd_fda_registration_number,LS_DB_PRODUCT_LIB.prd_external_id = LS_DB_PRODUCT_LIB_TMP.prd_external_id,LS_DB_PRODUCT_LIB.prd_external_app_updated_date = LS_DB_PRODUCT_LIB_TMP.prd_external_app_updated_date,LS_DB_PRODUCT_LIB.prd_drl_code = LS_DB_PRODUCT_LIB_TMP.prd_drl_code,LS_DB_PRODUCT_LIB.prd_disable_lsrims_fields = LS_DB_PRODUCT_LIB_TMP.prd_disable_lsrims_fields,LS_DB_PRODUCT_LIB.prd_device_usage = LS_DB_PRODUCT_LIB_TMP.prd_device_usage,LS_DB_PRODUCT_LIB.prd_device_quantity = LS_DB_PRODUCT_LIB_TMP.prd_device_quantity,LS_DB_PRODUCT_LIB.prd_device_part = LS_DB_PRODUCT_LIB_TMP.prd_device_part,LS_DB_PRODUCT_LIB.prd_department_division = LS_DB_PRODUCT_LIB_TMP.prd_department_division,LS_DB_PRODUCT_LIB.prd_date_modified = LS_DB_PRODUCT_LIB_TMP.prd_date_modified,LS_DB_PRODUCT_LIB.prd_date_created = LS_DB_PRODUCT_LIB_TMP.prd_date_created,LS_DB_PRODUCT_LIB.prd_created_date = LS_DB_PRODUCT_LIB_TMP.prd_created_date,LS_DB_PRODUCT_LIB.prd_cpd_jddcode = LS_DB_PRODUCT_LIB_TMP.prd_cpd_jddcode,LS_DB_PRODUCT_LIB.prd_container_size = LS_DB_PRODUCT_LIB_TMP.prd_container_size,LS_DB_PRODUCT_LIB.prd_conflict_comments = LS_DB_PRODUCT_LIB_TMP.prd_conflict_comments,LS_DB_PRODUCT_LIB.prd_company_product = LS_DB_PRODUCT_LIB_TMP.prd_company_product,LS_DB_PRODUCT_LIB.prd_company_name_part = LS_DB_PRODUCT_LIB_TMP.prd_company_name_part,LS_DB_PRODUCT_LIB.prd_combined_pharma_dose = LS_DB_PRODUCT_LIB_TMP.prd_combined_pharma_dose,LS_DB_PRODUCT_LIB.prd_combined_cdc = LS_DB_PRODUCT_LIB_TMP.prd_combined_cdc,LS_DB_PRODUCT_LIB.prd_class_of_device = LS_DB_PRODUCT_LIB_TMP.prd_class_of_device,LS_DB_PRODUCT_LIB.prd_centers_for_medicare = LS_DB_PRODUCT_LIB_TMP.prd_centers_for_medicare,LS_DB_PRODUCT_LIB.prd_center = LS_DB_PRODUCT_LIB_TMP.prd_center,LS_DB_PRODUCT_LIB.prd_catalogue_number = LS_DB_PRODUCT_LIB_TMP.prd_catalogue_number,LS_DB_PRODUCT_LIB.prd_case_processing_system = LS_DB_PRODUCT_LIB_TMP.prd_case_processing_system,LS_DB_PRODUCT_LIB.prd_black_traingle_product = LS_DB_PRODUCT_LIB_TMP.prd_black_traingle_product,LS_DB_PRODUCT_LIB.prd_auto_labelling_active = LS_DB_PRODUCT_LIB_TMP.prd_auto_labelling_active,LS_DB_PRODUCT_LIB.prd_auto_approval = LS_DB_PRODUCT_LIB_TMP.prd_auto_approval,LS_DB_PRODUCT_LIB.prd_atc_vet_code = LS_DB_PRODUCT_LIB_TMP.prd_atc_vet_code,LS_DB_PRODUCT_LIB.prd_atc_code = LS_DB_PRODUCT_LIB_TMP.prd_atc_code,LS_DB_PRODUCT_LIB.prd_arisg_record_id = LS_DB_PRODUCT_LIB_TMP.prd_arisg_record_id,LS_DB_PRODUCT_LIB.prd_allergenicity_indicator = LS_DB_PRODUCT_LIB_TMP.prd_allergenicity_indicator,LS_DB_PRODUCT_LIB.prd_ag_prod_sync_operation = LS_DB_PRODUCT_LIB_TMP.prd_ag_prod_sync_operation,LS_DB_PRODUCT_LIB.prd_ag_prod_sync_old_name = LS_DB_PRODUCT_LIB_TMP.prd_ag_prod_sync_old_name,LS_DB_PRODUCT_LIB.prd_active_ing_record_id = LS_DB_PRODUCT_LIB_TMP.prd_active_ing_record_id,LS_DB_PRODUCT_LIB.prd_account_record_id = LS_DB_PRODUCT_LIB_TMP.prd_account_record_id,LS_DB_PRODUCT_LIB.prd_access_to_otc = LS_DB_PRODUCT_LIB_TMP.prd_access_to_otc,LS_DB_PRODUCT_LIB.prdtradname_verified = LS_DB_PRODUCT_LIB_TMP.prdtradname_verified,LS_DB_PRODUCT_LIB.prdtradname_vaccine_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_vaccine_type,LS_DB_PRODUCT_LIB.prdtradname_user_modified = LS_DB_PRODUCT_LIB_TMP.prdtradname_user_modified,LS_DB_PRODUCT_LIB.prdtradname_user_created = LS_DB_PRODUCT_LIB_TMP.prdtradname_user_created,LS_DB_PRODUCT_LIB.prdtradname_type_of_device_other = LS_DB_PRODUCT_LIB_TMP.prdtradname_type_of_device_other,LS_DB_PRODUCT_LIB.prdtradname_type_of_device = LS_DB_PRODUCT_LIB_TMP.prdtradname_type_of_device,LS_DB_PRODUCT_LIB.prdtradname_trademark_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_trademark_name,LS_DB_PRODUCT_LIB.prdtradname_trade_mdrtype = LS_DB_PRODUCT_LIB_TMP.prdtradname_trade_mdrtype,LS_DB_PRODUCT_LIB.prdtradname_trade_ivdrtype = LS_DB_PRODUCT_LIB_TMP.prdtradname_trade_ivdrtype,LS_DB_PRODUCT_LIB.prdtradname_trade_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_trade_id,LS_DB_PRODUCT_LIB.prdtradname_tiken = LS_DB_PRODUCT_LIB_TMP.prdtradname_tiken,LS_DB_PRODUCT_LIB.prdtradname_strength_unit = LS_DB_PRODUCT_LIB_TMP.prdtradname_strength_unit,LS_DB_PRODUCT_LIB.prdtradname_strength = LS_DB_PRODUCT_LIB_TMP.prdtradname_strength,LS_DB_PRODUCT_LIB.prdtradname_spr_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_spr_id,LS_DB_PRODUCT_LIB.prdtradname_source = LS_DB_PRODUCT_LIB_TMP.prdtradname_source,LS_DB_PRODUCT_LIB.prdtradname_route_of_admin = LS_DB_PRODUCT_LIB_TMP.prdtradname_route_of_admin,LS_DB_PRODUCT_LIB.prdtradname_record_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_record_id,LS_DB_PRODUCT_LIB.prdtradname_proprietary_name_suffix = LS_DB_PRODUCT_LIB_TMP.prdtradname_proprietary_name_suffix,LS_DB_PRODUCT_LIB.prdtradname_product_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_product_type,LS_DB_PRODUCT_LIB.prdtradname_prod_drlcode = LS_DB_PRODUCT_LIB_TMP.prdtradname_prod_drlcode,LS_DB_PRODUCT_LIB.prdtradname_pmda_medicine_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_pmda_medicine_name,LS_DB_PRODUCT_LIB.prdtradname_otc_risk_classfn = LS_DB_PRODUCT_LIB_TMP.prdtradname_otc_risk_classfn,LS_DB_PRODUCT_LIB.prdtradname_otc_altnte_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_otc_altnte_id,LS_DB_PRODUCT_LIB.prdtradname_notified_cenum = LS_DB_PRODUCT_LIB_TMP.prdtradname_notified_cenum,LS_DB_PRODUCT_LIB.prdtradname_ndc_no = LS_DB_PRODUCT_LIB_TMP.prdtradname_ndc_no,LS_DB_PRODUCT_LIB.prdtradname_mpid_ver_number = LS_DB_PRODUCT_LIB_TMP.prdtradname_mpid_ver_number,LS_DB_PRODUCT_LIB.prdtradname_mpid_ver_date = LS_DB_PRODUCT_LIB_TMP.prdtradname_mpid_ver_date,LS_DB_PRODUCT_LIB.prdtradname_mpid = LS_DB_PRODUCT_LIB_TMP.prdtradname_mpid,LS_DB_PRODUCT_LIB.prdtradname_medicinal_product_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_medicinal_product_name,LS_DB_PRODUCT_LIB.prdtradname_med_product_ltn = LS_DB_PRODUCT_LIB_TMP.prdtradname_med_product_ltn,LS_DB_PRODUCT_LIB.prdtradname_marketting_date = LS_DB_PRODUCT_LIB_TMP.prdtradname_marketting_date,LS_DB_PRODUCT_LIB.prdtradname_marketing_status = LS_DB_PRODUCT_LIB_TMP.prdtradname_marketing_status,LS_DB_PRODUCT_LIB.prdtradname_marketing_category = LS_DB_PRODUCT_LIB_TMP.prdtradname_marketing_category,LS_DB_PRODUCT_LIB.prdtradname_mah_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_mah_type,LS_DB_PRODUCT_LIB.prdtradname_mah_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_mah_name,LS_DB_PRODUCT_LIB.prdtradname_ltn_package_desc = LS_DB_PRODUCT_LIB_TMP.prdtradname_ltn_package_desc,LS_DB_PRODUCT_LIB.prdtradname_ltn_jddcode = LS_DB_PRODUCT_LIB_TMP.prdtradname_ltn_jddcode,LS_DB_PRODUCT_LIB.prdtradname_ltn_component_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_ltn_component_type,LS_DB_PRODUCT_LIB.prdtradname_local_tradename = LS_DB_PRODUCT_LIB_TMP.prdtradname_local_tradename,LS_DB_PRODUCT_LIB.prdtradname_license_status_de_ml = LS_DB_PRODUCT_LIB_TMP.prdtradname_license_status_de_ml,LS_DB_PRODUCT_LIB.prdtradname_license_status = LS_DB_PRODUCT_LIB_TMP.prdtradname_license_status,LS_DB_PRODUCT_LIB.prdtradname_license_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_license_id,LS_DB_PRODUCT_LIB.prdtradname_kdd_code = LS_DB_PRODUCT_LIB_TMP.prdtradname_kdd_code,LS_DB_PRODUCT_LIB.prdtradname_internal_prd_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_internal_prd_id,LS_DB_PRODUCT_LIB.prdtradname_inteded_use_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_inteded_use_name,LS_DB_PRODUCT_LIB.prdtradname_identify_not_body = LS_DB_PRODUCT_LIB_TMP.prdtradname_identify_not_body,LS_DB_PRODUCT_LIB.prdtradname_health_canada_idnum = LS_DB_PRODUCT_LIB_TMP.prdtradname_health_canada_idnum,LS_DB_PRODUCT_LIB.prdtradname_form_admin = LS_DB_PRODUCT_LIB_TMP.prdtradname_form_admin,LS_DB_PRODUCT_LIB.prdtradname_fk_app_rec_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_fk_app_rec_id,LS_DB_PRODUCT_LIB.prdtradname_fk_agx_product_rec_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_fk_agx_product_rec_id,LS_DB_PRODUCT_LIB.prdtradname_fei_no = LS_DB_PRODUCT_LIB_TMP.prdtradname_fei_no,LS_DB_PRODUCT_LIB.prdtradname_external_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_external_id,LS_DB_PRODUCT_LIB.prdtradname_disable_lsrims_fields = LS_DB_PRODUCT_LIB_TMP.prdtradname_disable_lsrims_fields,LS_DB_PRODUCT_LIB.prdtradname_dev_marketyears = LS_DB_PRODUCT_LIB_TMP.prdtradname_dev_marketyears,LS_DB_PRODUCT_LIB.prdtradname_dev_marketmonths = LS_DB_PRODUCT_LIB_TMP.prdtradname_dev_marketmonths,LS_DB_PRODUCT_LIB.prdtradname_dev_marketedbefore = LS_DB_PRODUCT_LIB_TMP.prdtradname_dev_marketedbefore,LS_DB_PRODUCT_LIB.prdtradname_dev_commerdate = LS_DB_PRODUCT_LIB_TMP.prdtradname_dev_commerdate,LS_DB_PRODUCT_LIB.prdtradname_demographic_was_designed = LS_DB_PRODUCT_LIB_TMP.prdtradname_demographic_was_designed,LS_DB_PRODUCT_LIB.prdtradname_date_modified = LS_DB_PRODUCT_LIB_TMP.prdtradname_date_modified,LS_DB_PRODUCT_LIB.prdtradname_date_created = LS_DB_PRODUCT_LIB_TMP.prdtradname_date_created,LS_DB_PRODUCT_LIB.prdtradname_data_sheet_rec_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_data_sheet_rec_id,LS_DB_PRODUCT_LIB.prdtradname_country_code = LS_DB_PRODUCT_LIB_TMP.prdtradname_country_code,LS_DB_PRODUCT_LIB.prdtradname_copy_approval_checked = LS_DB_PRODUCT_LIB_TMP.prdtradname_copy_approval_checked,LS_DB_PRODUCT_LIB.prdtradname_container_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_container_type,LS_DB_PRODUCT_LIB.prdtradname_company_unit_record_id_mah = LS_DB_PRODUCT_LIB_TMP.prdtradname_company_unit_record_id_mah,LS_DB_PRODUCT_LIB.prdtradname_code_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_code_name,LS_DB_PRODUCT_LIB.prdtradname_clinical_drug_code = LS_DB_PRODUCT_LIB_TMP.prdtradname_clinical_drug_code,LS_DB_PRODUCT_LIB.prdtradname_class_of_devices = LS_DB_PRODUCT_LIB_TMP.prdtradname_class_of_devices,LS_DB_PRODUCT_LIB.prdtradname_bio_ref_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_bio_ref_name,LS_DB_PRODUCT_LIB.prdtradname_atc_vet_code = LS_DB_PRODUCT_LIB_TMP.prdtradname_atc_vet_code,LS_DB_PRODUCT_LIB.prdtradname_atc_code = LS_DB_PRODUCT_LIB_TMP.prdtradname_atc_code,LS_DB_PRODUCT_LIB.prdtradname_arisg_record_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_arisg_record_id,LS_DB_PRODUCT_LIB.prdtradname_arisg_product_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_arisg_product_id,LS_DB_PRODUCT_LIB.prdtradname_arisg_approval_id = LS_DB_PRODUCT_LIB_TMP.prdtradname_arisg_approval_id,LS_DB_PRODUCT_LIB.prdtradname_approval_type_de_ml = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_type_de_ml,LS_DB_PRODUCT_LIB.prdtradname_approval_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_type,LS_DB_PRODUCT_LIB.prdtradname_approval_submission_type = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_submission_type,LS_DB_PRODUCT_LIB.prdtradname_approval_status = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_status,LS_DB_PRODUCT_LIB.prdtradname_approval_start_date = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_start_date,LS_DB_PRODUCT_LIB.prdtradname_approval_no = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_no,LS_DB_PRODUCT_LIB.prdtradname_approval_desc = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_desc,LS_DB_PRODUCT_LIB.prdtradname_approval_date = LS_DB_PRODUCT_LIB_TMP.prdtradname_approval_date,LS_DB_PRODUCT_LIB.prdtradname_application_no = LS_DB_PRODUCT_LIB_TMP.prdtradname_application_no,LS_DB_PRODUCT_LIB.prdtradname_anda_no = LS_DB_PRODUCT_LIB_TMP.prdtradname_anda_no,LS_DB_PRODUCT_LIB.prdtradname_agent_name = LS_DB_PRODUCT_LIB_TMP.prdtradname_agent_name,LS_DB_PRODUCT_LIB.prdtradname_account_record_id_mah = LS_DB_PRODUCT_LIB_TMP.prdtradname_account_record_id_mah,LS_DB_PRODUCT_LIB.prdtradname_account_record_id_agent = LS_DB_PRODUCT_LIB_TMP.prdtradname_account_record_id_agent,LS_DB_PRODUCT_LIB.prdtradname_access_to_otc = LS_DB_PRODUCT_LIB_TMP.prdtradname_access_to_otc,LS_DB_PRODUCT_LIB.prdlicense_user_modified = LS_DB_PRODUCT_LIB_TMP.prdlicense_user_modified,LS_DB_PRODUCT_LIB.prdlicense_user_created = LS_DB_PRODUCT_LIB_TMP.prdlicense_user_created,LS_DB_PRODUCT_LIB.prdlicense_trade_name = LS_DB_PRODUCT_LIB_TMP.prdlicense_trade_name,LS_DB_PRODUCT_LIB.prdlicense_spr_id = LS_DB_PRODUCT_LIB_TMP.prdlicense_spr_id,LS_DB_PRODUCT_LIB.prdlicense_record_id = LS_DB_PRODUCT_LIB_TMP.prdlicense_record_id,LS_DB_PRODUCT_LIB.prdlicense_pref_name_code = LS_DB_PRODUCT_LIB_TMP.prdlicense_pref_name_code,LS_DB_PRODUCT_LIB.prdlicense_original_license_no = LS_DB_PRODUCT_LIB_TMP.prdlicense_original_license_no,LS_DB_PRODUCT_LIB.prdlicense_license_status = LS_DB_PRODUCT_LIB_TMP.prdlicense_license_status,LS_DB_PRODUCT_LIB.prdlicense_license_record_id = LS_DB_PRODUCT_LIB_TMP.prdlicense_license_record_id,LS_DB_PRODUCT_LIB.prdlicense_fk_product_id = LS_DB_PRODUCT_LIB_TMP.prdlicense_fk_product_id,LS_DB_PRODUCT_LIB.prdlicense_date_modified = LS_DB_PRODUCT_LIB_TMP.prdlicense_date_modified,LS_DB_PRODUCT_LIB.prdlicense_date_created = LS_DB_PRODUCT_LIB_TMP.prdlicense_date_created,LS_DB_PRODUCT_LIB.prdgrp_user_modified = LS_DB_PRODUCT_LIB_TMP.prdgrp_user_modified,LS_DB_PRODUCT_LIB.prdgrp_user_created = LS_DB_PRODUCT_LIB_TMP.prdgrp_user_created,LS_DB_PRODUCT_LIB.prdgrp_tradename_search_result = LS_DB_PRODUCT_LIB_TMP.prdgrp_tradename_search_result,LS_DB_PRODUCT_LIB.prdgrp_spr_id = LS_DB_PRODUCT_LIB_TMP.prdgrp_spr_id,LS_DB_PRODUCT_LIB.prdgrp_search_type = LS_DB_PRODUCT_LIB_TMP.prdgrp_search_type,LS_DB_PRODUCT_LIB.prdgrp_satrt_date = LS_DB_PRODUCT_LIB_TMP.prdgrp_satrt_date,LS_DB_PRODUCT_LIB.prdgrp_record_id = LS_DB_PRODUCT_LIB_TMP.prdgrp_record_id,LS_DB_PRODUCT_LIB.prdgrp_product_search_result = LS_DB_PRODUCT_LIB_TMP.prdgrp_product_search_result,LS_DB_PRODUCT_LIB.prdgrp_product_group_name = LS_DB_PRODUCT_LIB_TMP.prdgrp_product_group_name,LS_DB_PRODUCT_LIB.prdgrp_product_coding_level = LS_DB_PRODUCT_LIB_TMP.prdgrp_product_coding_level,LS_DB_PRODUCT_LIB.prdgrp_end_date = LS_DB_PRODUCT_LIB_TMP.prdgrp_end_date,LS_DB_PRODUCT_LIB.prdgrp_description = LS_DB_PRODUCT_LIB_TMP.prdgrp_description,LS_DB_PRODUCT_LIB.prdgrp_date_modified = LS_DB_PRODUCT_LIB_TMP.prdgrp_date_modified,LS_DB_PRODUCT_LIB.prdgrp_date_created = LS_DB_PRODUCT_LIB_TMP.prdgrp_date_created,LS_DB_PRODUCT_LIB.prdgrp_assigned_to = LS_DB_PRODUCT_LIB_TMP.prdgrp_assigned_to,LS_DB_PRODUCT_LIB.prdgrp_assign_to = LS_DB_PRODUCT_LIB_TMP.prdgrp_assign_to,LS_DB_PRODUCT_LIB.prdgrp_activeing_search_result = LS_DB_PRODUCT_LIB_TMP.prdgrp_activeing_search_result,LS_DB_PRODUCT_LIB.prdgrp_active = LS_DB_PRODUCT_LIB_TMP.prdgrp_active,LS_DB_PRODUCT_LIB.prdgrpsrch_value = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_value,LS_DB_PRODUCT_LIB.prdgrpsrch_user_modified = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_user_modified,LS_DB_PRODUCT_LIB.prdgrpsrch_user_created = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_user_created,LS_DB_PRODUCT_LIB.prdgrpsrch_to_date_value = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_to_date_value,LS_DB_PRODUCT_LIB.prdgrpsrch_test = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_test,LS_DB_PRODUCT_LIB.prdgrpsrch_spr_id = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_spr_id,LS_DB_PRODUCT_LIB.prdgrpsrch_record_id = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_record_id,LS_DB_PRODUCT_LIB.prdgrpsrch_fk_product_grp_rec_id = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_fk_product_grp_rec_id,LS_DB_PRODUCT_LIB.prdgrpsrch_field_name = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_field_name,LS_DB_PRODUCT_LIB.prdgrpsrch_decode_values = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_decode_values,LS_DB_PRODUCT_LIB.prdgrpsrch_date_value = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_date_value,LS_DB_PRODUCT_LIB.prdgrpsrch_date_modified = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_date_modified,LS_DB_PRODUCT_LIB.prdgrpsrch_date_created = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_date_created,LS_DB_PRODUCT_LIB.prdgrpsrch_condition = LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_condition,LS_DB_PRODUCT_LIB.prddvc_warranty_period_years = LS_DB_PRODUCT_LIB_TMP.prddvc_warranty_period_years,LS_DB_PRODUCT_LIB.prddvc_warranty_period_months = LS_DB_PRODUCT_LIB_TMP.prddvc_warranty_period_months,LS_DB_PRODUCT_LIB.prddvc_user_modified = LS_DB_PRODUCT_LIB_TMP.prddvc_user_modified,LS_DB_PRODUCT_LIB.prddvc_user_created = LS_DB_PRODUCT_LIB_TMP.prddvc_user_created,LS_DB_PRODUCT_LIB.prddvc_udi = LS_DB_PRODUCT_LIB_TMP.prddvc_udi,LS_DB_PRODUCT_LIB.prddvc_spr_id = LS_DB_PRODUCT_LIB_TMP.prddvc_spr_id,LS_DB_PRODUCT_LIB.prddvc_record_id = LS_DB_PRODUCT_LIB_TMP.prddvc_record_id,LS_DB_PRODUCT_LIB.prddvc_prod_device_cenum = LS_DB_PRODUCT_LIB_TMP.prddvc_prod_device_cenum,LS_DB_PRODUCT_LIB.prddvc_prod_autho_rep = LS_DB_PRODUCT_LIB_TMP.prddvc_prod_autho_rep,LS_DB_PRODUCT_LIB.prddvc_prod_artg_num = LS_DB_PRODUCT_LIB_TMP.prddvc_prod_artg_num,LS_DB_PRODUCT_LIB.prddvc_nomenclature_code = LS_DB_PRODUCT_LIB_TMP.prddvc_nomenclature_code,LS_DB_PRODUCT_LIB.prddvc_nomen_sysothtext = LS_DB_PRODUCT_LIB_TMP.prddvc_nomen_sysothtext,LS_DB_PRODUCT_LIB.prddvc_model_number = LS_DB_PRODUCT_LIB_TMP.prddvc_model_number,LS_DB_PRODUCT_LIB.prddvc_manufactured_by = LS_DB_PRODUCT_LIB_TMP.prddvc_manufactured_by,LS_DB_PRODUCT_LIB.prddvc_is_prod_combination = LS_DB_PRODUCT_LIB_TMP.prddvc_is_prod_combination,LS_DB_PRODUCT_LIB.prddvc_fk_product_rec_id = LS_DB_PRODUCT_LIB_TMP.prddvc_fk_product_rec_id,LS_DB_PRODUCT_LIB.prddvc_fda_reg_num = LS_DB_PRODUCT_LIB_TMP.prddvc_fda_reg_num,LS_DB_PRODUCT_LIB.prddvc_disable_lsrims_fields = LS_DB_PRODUCT_LIB_TMP.prddvc_disable_lsrims_fields,LS_DB_PRODUCT_LIB.prddvc_device_usage = LS_DB_PRODUCT_LIB_TMP.prddvc_device_usage,LS_DB_PRODUCT_LIB.prddvc_device_type = LS_DB_PRODUCT_LIB_TMP.prddvc_device_type,LS_DB_PRODUCT_LIB.prddvc_device_quantity = LS_DB_PRODUCT_LIB_TMP.prddvc_device_quantity,LS_DB_PRODUCT_LIB.prddvc_device_pro_code = LS_DB_PRODUCT_LIB_TMP.prddvc_device_pro_code,LS_DB_PRODUCT_LIB.prddvc_device_part = LS_DB_PRODUCT_LIB_TMP.prddvc_device_part,LS_DB_PRODUCT_LIB.prddvc_device_manufactured_by = LS_DB_PRODUCT_LIB_TMP.prddvc_device_manufactured_by,LS_DB_PRODUCT_LIB.prddvc_dev_udi_value = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_udi_value,LS_DB_PRODUCT_LIB.prddvc_dev_udi_type = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_udi_type,LS_DB_PRODUCT_LIB.prddvc_dev_nomendecode = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_nomendecode,LS_DB_PRODUCT_LIB.prddvc_dev_manf_site = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_manf_site,LS_DB_PRODUCT_LIB.prddvc_dev_compoversion = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_compoversion,LS_DB_PRODUCT_LIB.prddvc_dev_compotermid = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_compotermid,LS_DB_PRODUCT_LIB.prddvc_dev_compo_name = LS_DB_PRODUCT_LIB_TMP.prddvc_dev_compo_name,LS_DB_PRODUCT_LIB.prddvc_department_division = LS_DB_PRODUCT_LIB_TMP.prddvc_department_division,LS_DB_PRODUCT_LIB.prddvc_date_modified = LS_DB_PRODUCT_LIB_TMP.prddvc_date_modified,LS_DB_PRODUCT_LIB.prddvc_date_created = LS_DB_PRODUCT_LIB_TMP.prddvc_date_created,LS_DB_PRODUCT_LIB.prddvc_competent_authoriy = LS_DB_PRODUCT_LIB_TMP.prddvc_competent_authoriy,LS_DB_PRODUCT_LIB.prddvc_common_dev_name = LS_DB_PRODUCT_LIB_TMP.prddvc_common_dev_name,LS_DB_PRODUCT_LIB.prddvc_code_value = LS_DB_PRODUCT_LIB_TMP.prddvc_code_value,LS_DB_PRODUCT_LIB.prddvc_code_system = LS_DB_PRODUCT_LIB_TMP.prddvc_code_system,LS_DB_PRODUCT_LIB.prddvc_class_of_device = LS_DB_PRODUCT_LIB_TMP.prddvc_class_of_device,LS_DB_PRODUCT_LIB.prddvc_centers_for_medicare = LS_DB_PRODUCT_LIB_TMP.prddvc_centers_for_medicare,LS_DB_PRODUCT_LIB.prddvc_catalogue_number = LS_DB_PRODUCT_LIB_TMP.prddvc_catalogue_number,LS_DB_PRODUCT_LIB.prddvc_brand_name = LS_DB_PRODUCT_LIB_TMP.prddvc_brand_name,LS_DB_PRODUCT_LIB.prddvc_allergenicity_indicator = LS_DB_PRODUCT_LIB_TMP.prddvc_allergenicity_indicator,LS_DB_PRODUCT_LIB.prdcntry_user_modified = LS_DB_PRODUCT_LIB_TMP.prdcntry_user_modified,LS_DB_PRODUCT_LIB.prdcntry_user_created = LS_DB_PRODUCT_LIB_TMP.prdcntry_user_created,LS_DB_PRODUCT_LIB.prdcntry_spr_id = LS_DB_PRODUCT_LIB_TMP.prdcntry_spr_id,LS_DB_PRODUCT_LIB.prdcntry_record_id = LS_DB_PRODUCT_LIB_TMP.prdcntry_record_id,LS_DB_PRODUCT_LIB.prdcntry_fk_apt_rec_id = LS_DB_PRODUCT_LIB_TMP.prdcntry_fk_apt_rec_id,LS_DB_PRODUCT_LIB.prdcntry_date_modified = LS_DB_PRODUCT_LIB_TMP.prdcntry_date_modified,LS_DB_PRODUCT_LIB.prdcntry_date_created = LS_DB_PRODUCT_LIB_TMP.prdcntry_date_created,LS_DB_PRODUCT_LIB.prdcntry_country_code = LS_DB_PRODUCT_LIB_TMP.prdcntry_country_code,LS_DB_PRODUCT_LIB.prdcntry_approval_type = LS_DB_PRODUCT_LIB_TMP.prdcntry_approval_type,LS_DB_PRODUCT_LIB.prdcntry_approval_no = LS_DB_PRODUCT_LIB_TMP.prdcntry_approval_no,LS_DB_PRODUCT_LIB.prdcntry_approval_id = LS_DB_PRODUCT_LIB_TMP.prdcntry_approval_id,LS_DB_PRODUCT_LIB.prdcatg_user_modified = LS_DB_PRODUCT_LIB_TMP.prdcatg_user_modified,LS_DB_PRODUCT_LIB.prdcatg_user_created = LS_DB_PRODUCT_LIB_TMP.prdcatg_user_created,LS_DB_PRODUCT_LIB.prdcatg_spr_id = LS_DB_PRODUCT_LIB_TMP.prdcatg_spr_id,LS_DB_PRODUCT_LIB.prdcatg_record_id = LS_DB_PRODUCT_LIB_TMP.prdcatg_record_id,LS_DB_PRODUCT_LIB.prdcatg_product_number = LS_DB_PRODUCT_LIB_TMP.prdcatg_product_number,LS_DB_PRODUCT_LIB.prdcatg_product_category_type = LS_DB_PRODUCT_LIB_TMP.prdcatg_product_category_type,LS_DB_PRODUCT_LIB.prdcatg_product_attrib_value = LS_DB_PRODUCT_LIB_TMP.prdcatg_product_attrib_value,LS_DB_PRODUCT_LIB.prdcatg_pro_start_date = LS_DB_PRODUCT_LIB_TMP.prdcatg_pro_start_date,LS_DB_PRODUCT_LIB.prdcatg_pro_end_date = LS_DB_PRODUCT_LIB_TMP.prdcatg_pro_end_date,LS_DB_PRODUCT_LIB.prdcatg_ndc = LS_DB_PRODUCT_LIB_TMP.prdcatg_ndc,LS_DB_PRODUCT_LIB.prdcatg_fk_agx_trade_rec_id = LS_DB_PRODUCT_LIB_TMP.prdcatg_fk_agx_trade_rec_id,LS_DB_PRODUCT_LIB.prdcatg_date_modified = LS_DB_PRODUCT_LIB_TMP.prdcatg_date_modified,LS_DB_PRODUCT_LIB.prdcatg_date_created = LS_DB_PRODUCT_LIB_TMP.prdcatg_date_created,LS_DB_PRODUCT_LIB.prdaesi_user_modified = LS_DB_PRODUCT_LIB_TMP.prdaesi_user_modified,LS_DB_PRODUCT_LIB.prdaesi_user_created = LS_DB_PRODUCT_LIB_TMP.prdaesi_user_created,LS_DB_PRODUCT_LIB.prdaesi_spr_id = LS_DB_PRODUCT_LIB_TMP.prdaesi_spr_id,LS_DB_PRODUCT_LIB.prdaesi_soc_name = LS_DB_PRODUCT_LIB_TMP.prdaesi_soc_name,LS_DB_PRODUCT_LIB.prdaesi_record_id = LS_DB_PRODUCT_LIB_TMP.prdaesi_record_id,LS_DB_PRODUCT_LIB.prdaesi_meddra_version = LS_DB_PRODUCT_LIB_TMP.prdaesi_meddra_version,LS_DB_PRODUCT_LIB.prdaesi_meddra_pt_term = LS_DB_PRODUCT_LIB_TMP.prdaesi_meddra_pt_term,LS_DB_PRODUCT_LIB.prdaesi_meddra_llt_code = LS_DB_PRODUCT_LIB_TMP.prdaesi_meddra_llt_code,LS_DB_PRODUCT_LIB.prdaesi_meddra_code = LS_DB_PRODUCT_LIB_TMP.prdaesi_meddra_code,LS_DB_PRODUCT_LIB.prdaesi_is_oth_seriouness = LS_DB_PRODUCT_LIB_TMP.prdaesi_is_oth_seriouness,LS_DB_PRODUCT_LIB.prdaesi_is_lifethreatening = LS_DB_PRODUCT_LIB_TMP.prdaesi_is_lifethreatening,LS_DB_PRODUCT_LIB.prdaesi_is_fatal = LS_DB_PRODUCT_LIB_TMP.prdaesi_is_fatal,LS_DB_PRODUCT_LIB.prdaesi_fk_pro_rec_id = LS_DB_PRODUCT_LIB_TMP.prdaesi_fk_pro_rec_id,LS_DB_PRODUCT_LIB.prdaesi_fk_dsofaesi_rec_id = LS_DB_PRODUCT_LIB_TMP.prdaesi_fk_dsofaesi_rec_id,LS_DB_PRODUCT_LIB.prdaesi_date_modified = LS_DB_PRODUCT_LIB_TMP.prdaesi_date_modified,LS_DB_PRODUCT_LIB.prdaesi_date_created = LS_DB_PRODUCT_LIB_TMP.prdaesi_date_created,LS_DB_PRODUCT_LIB.prdaesi_aesi_smqcmq_term = LS_DB_PRODUCT_LIB_TMP.prdaesi_aesi_smqcmq_term,LS_DB_PRODUCT_LIB.prdaesi_aesi_smqcmq_code = LS_DB_PRODUCT_LIB_TMP.prdaesi_aesi_smqcmq_code,LS_DB_PRODUCT_LIB.prdaesi_aesi_broad_narrow = LS_DB_PRODUCT_LIB_TMP.prdaesi_aesi_broad_narrow,
LS_DB_PRODUCT_LIB.PROCESSING_DT = LS_DB_PRODUCT_LIB_TMP.PROCESSING_DT,
LS_DB_PRODUCT_LIB.user_modified  =LS_DB_PRODUCT_LIB_TMP.user_modified     ,
LS_DB_PRODUCT_LIB.date_modified  =LS_DB_PRODUCT_LIB_TMP.date_modified     ,
LS_DB_PRODUCT_LIB.expiry_date    =LS_DB_PRODUCT_LIB_TMP.expiry_date       ,
LS_DB_PRODUCT_LIB.created_by     =LS_DB_PRODUCT_LIB_TMP.created_by        ,
LS_DB_PRODUCT_LIB.created_dt     =LS_DB_PRODUCT_LIB_TMP.created_dt        ,
LS_DB_PRODUCT_LIB.load_ts        =LS_DB_PRODUCT_LIB_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP 
WHERE LS_DB_PRODUCT_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PRODUCT_LIB_TMP.PROCESSING_DT = LS_DB_PRODUCT_LIB.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PRODUCT_LIB.prdcntry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdcatg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prddvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdaesi_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdtradname_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdlicense_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.trdbrand_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_LIB_TMP.prdcntry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdcatg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prddvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdaesi_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdtradname_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdlicense_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.trdbrand_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PRODUCT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PRODUCT_LIB.prd_RECORD_ID ,LS_DB_PRODUCT_LIB.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP 
ON LS_DB_PRODUCT_LIB.prd_RECORD_ID=LS_DB_PRODUCT_LIB_TMP.prd_RECORD_ID
AND LS_DB_PRODUCT_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID 
where LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PRODUCT_LIB.prd_RECORD_ID in (select prd_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP )
) TMP where LS_DB_PRODUCT_LIB.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PRODUCT_LIB.prd_RECORD_ID ,LS_DB_PRODUCT_LIB.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP 
    ON LS_DB_PRODUCT_LIB.prd_RECORD_ID=LS_DB_PRODUCT_LIB_TMP.prd_RECORD_ID
    AND LS_DB_PRODUCT_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID 
    where LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PRODUCT_LIB.prd_RECORD_ID in (select prd_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,trdbrand_user_modified,
trdbrand_user_created,
trdbrand_spr_id,
trdbrand_record_id,
trdbrand_fk_agx_trade_rec_id,
trdbrand_firm_name,
trdbrand_firm_function,
trdbrand_fei_no,
trdbrand_date_modified,
trdbrand_date_created,
trdbrand_account_record_id,
trdbrand_account_id,
prd_warranty_period_years,
prd_warranty_period_months,
prd_viewpdfflag,
prd_version,
prd_vaccine_type,
prd_user_modified,
prd_user_created,
prd_unstructured_strength,
prd_unit_price,
prd_unit_of_presentation,
prd_udi,
prd_task_flag,
prd_target_population_part,
prd_synchonization_date,
prd_subject_to_risk,
prd_study_product_type,
prd_strength_unit,
prd_strength_flag,
prd_strength,
prd_staging_status,
prd_spr_id,
prd_sim_pro_flag,
prd_route_of_admin_term_id,
prd_route_of_admin,
prd_rm_flag,
prd_redact_flag,
prd_record_id,
prd_reason_for_deactive,
prd_qc_review,
prd_product_version,
prd_product_used_in,
prd_product_url,
prd_product_type_de_ml,
prd_product_type,
prd_product_status,
prd_product_name_upper,
prd_product_name_reported_chinese,
prd_product_name,
prd_product_id,
prd_product_group_de_ml,
prd_product_group,
prd_product_description,
prd_product_class,
prd_product_approval_status,
prd_product_active,
prd_prod_whodd_decode,
prd_prod_vigilancetype,
prd_prod_invented_name,
prd_prod_intdev_birthdate,
prd_prod_device_name,
prd_pro_component_type,
prd_prefered_code,
prd_pref_name_code,
prd_pmda_medicine_name,
prd_pharma_dose_form_term_id,
prd_pharma_dose_form_part,
prd_part_no,
prd_pai,
prd_package_description,
prd_pack_part,
prd_otc_risk_classfn,
prd_notified_body_ident_no,
prd_nlp_corpus_sync_status,
prd_multiple_ingredients,
prd_mpid_ver_number,
prd_mpid_ver_date,
prd_mpid,
prd_model_number,
prd_mfr_name,
prd_med_product_cpd,
prd_manufacturer_group,
prd_manufactured_by,
prd_made_by_de_ml,
prd_made_by,
prd_lsmv_product_id_for_litpro,
prd_listednessgrp_to,
prd_language,
prd_keywords,
prd_kdd_code,
prd_international_birth_date,
prd_internal_drug_code,
prd_inteded_use_name,
prd_impid_cross_ref,
prd_gmdn_code,
prd_generic_namecn,
prd_generic_name,
prd_future_product,
prd_formulation_part,
prd_form_admin_de_ml,
prd_form_admin,
prd_fk_base_prd_rec_id,
prd_fei_no,
prd_fda_registration_number,
prd_external_id,
prd_external_app_updated_date,
prd_drl_code,
prd_disable_lsrims_fields,
prd_device_usage,
prd_device_quantity,
prd_device_part,
prd_department_division,
prd_date_modified,
prd_date_created,
prd_created_date,
prd_cpd_jddcode,
prd_container_size,
prd_conflict_comments,
prd_company_product,
prd_company_name_part,
prd_combined_pharma_dose,
prd_combined_cdc,
prd_class_of_device,
prd_centers_for_medicare,
prd_center,
prd_catalogue_number,
prd_case_processing_system,
prd_black_traingle_product,
prd_auto_labelling_active,
prd_auto_approval,
prd_atc_vet_code,
prd_atc_code,
prd_arisg_record_id,
prd_allergenicity_indicator,
prd_ag_prod_sync_operation,
prd_ag_prod_sync_old_name,
prd_active_ing_record_id,
prd_account_record_id,
prd_access_to_otc,
prdtradname_verified,
prdtradname_vaccine_type,
prdtradname_user_modified,
prdtradname_user_created,
prdtradname_type_of_device_other,
prdtradname_type_of_device,
prdtradname_trademark_name,
prdtradname_trade_mdrtype,
prdtradname_trade_ivdrtype,
prdtradname_trade_id,
prdtradname_tiken,
prdtradname_strength_unit,
prdtradname_strength,
prdtradname_spr_id,
prdtradname_source,
prdtradname_route_of_admin,
prdtradname_record_id,
prdtradname_proprietary_name_suffix,
prdtradname_product_type,
prdtradname_prod_drlcode,
prdtradname_pmda_medicine_name,
prdtradname_otc_risk_classfn,
prdtradname_otc_altnte_id,
prdtradname_notified_cenum,
prdtradname_ndc_no,
prdtradname_mpid_ver_number,
prdtradname_mpid_ver_date,
prdtradname_mpid,
prdtradname_medicinal_product_name,
prdtradname_med_product_ltn,
prdtradname_marketting_date,
prdtradname_marketing_status,
prdtradname_marketing_category,
prdtradname_mah_type,
prdtradname_mah_name,
prdtradname_ltn_package_desc,
prdtradname_ltn_jddcode,
prdtradname_ltn_component_type,
prdtradname_local_tradename,
prdtradname_license_status_de_ml,
prdtradname_license_status,
prdtradname_license_id,
prdtradname_kdd_code,
prdtradname_internal_prd_id,
prdtradname_inteded_use_name,
prdtradname_identify_not_body,
prdtradname_health_canada_idnum,
prdtradname_form_admin,
prdtradname_fk_app_rec_id,
prdtradname_fk_agx_product_rec_id,
prdtradname_fei_no,
prdtradname_external_id,
prdtradname_disable_lsrims_fields,
prdtradname_dev_marketyears,
prdtradname_dev_marketmonths,
prdtradname_dev_marketedbefore,
prdtradname_dev_commerdate,
prdtradname_demographic_was_designed,
prdtradname_date_modified,
prdtradname_date_created,
prdtradname_data_sheet_rec_id,
prdtradname_country_code,
prdtradname_copy_approval_checked,
prdtradname_container_type,
prdtradname_company_unit_record_id_mah,
prdtradname_code_name,
prdtradname_clinical_drug_code,
prdtradname_class_of_devices,
prdtradname_bio_ref_name,
prdtradname_atc_vet_code,
prdtradname_atc_code,
prdtradname_arisg_record_id,
prdtradname_arisg_product_id,
prdtradname_arisg_approval_id,
prdtradname_approval_type_de_ml,
prdtradname_approval_type,
prdtradname_approval_submission_type,
prdtradname_approval_status,
prdtradname_approval_start_date,
prdtradname_approval_no,
prdtradname_approval_desc,
prdtradname_approval_date,
prdtradname_application_no,
prdtradname_anda_no,
prdtradname_agent_name,
prdtradname_account_record_id_mah,
prdtradname_account_record_id_agent,
prdtradname_access_to_otc,
prdlicense_user_modified,
prdlicense_user_created,
prdlicense_trade_name,
prdlicense_spr_id,
prdlicense_record_id,
prdlicense_pref_name_code,
prdlicense_original_license_no,
prdlicense_license_status,
prdlicense_license_record_id,
prdlicense_fk_product_id,
prdlicense_date_modified,
prdlicense_date_created,
prdgrp_user_modified,
prdgrp_user_created,
prdgrp_tradename_search_result,
prdgrp_spr_id,
prdgrp_search_type,
prdgrp_satrt_date,
prdgrp_record_id,
prdgrp_product_search_result,
prdgrp_product_group_name,
prdgrp_product_coding_level,
prdgrp_end_date,
prdgrp_description,
prdgrp_date_modified,
prdgrp_date_created,
prdgrp_assigned_to,
prdgrp_assign_to,
prdgrp_activeing_search_result,
prdgrp_active,
prdgrpsrch_value,
prdgrpsrch_user_modified,
prdgrpsrch_user_created,
prdgrpsrch_to_date_value,
prdgrpsrch_test,
prdgrpsrch_spr_id,
prdgrpsrch_record_id,
prdgrpsrch_fk_product_grp_rec_id,
prdgrpsrch_field_name,
prdgrpsrch_decode_values,
prdgrpsrch_date_value,
prdgrpsrch_date_modified,
prdgrpsrch_date_created,
prdgrpsrch_condition,
prddvc_warranty_period_years,
prddvc_warranty_period_months,
prddvc_user_modified,
prddvc_user_created,
prddvc_udi,
prddvc_spr_id,
prddvc_record_id,
prddvc_prod_device_cenum,
prddvc_prod_autho_rep,
prddvc_prod_artg_num,
prddvc_nomenclature_code,
prddvc_nomen_sysothtext,
prddvc_model_number,
prddvc_manufactured_by,
prddvc_is_prod_combination,
prddvc_fk_product_rec_id,
prddvc_fda_reg_num,
prddvc_disable_lsrims_fields,
prddvc_device_usage,
prddvc_device_type,
prddvc_device_quantity,
prddvc_device_pro_code,
prddvc_device_part,
prddvc_device_manufactured_by,
prddvc_dev_udi_value,
prddvc_dev_udi_type,
prddvc_dev_nomendecode,
prddvc_dev_manf_site,
prddvc_dev_compoversion,
prddvc_dev_compotermid,
prddvc_dev_compo_name,
prddvc_department_division,
prddvc_date_modified,
prddvc_date_created,
prddvc_competent_authoriy,
prddvc_common_dev_name,
prddvc_code_value,
prddvc_code_system,
prddvc_class_of_device,
prddvc_centers_for_medicare,
prddvc_catalogue_number,
prddvc_brand_name,
prddvc_allergenicity_indicator,
prdcntry_user_modified,
prdcntry_user_created,
prdcntry_spr_id,
prdcntry_record_id,
prdcntry_fk_apt_rec_id,
prdcntry_date_modified,
prdcntry_date_created,
prdcntry_country_code,
prdcntry_approval_type,
prdcntry_approval_no,
prdcntry_approval_id,
prdcatg_user_modified,
prdcatg_user_created,
prdcatg_spr_id,
prdcatg_record_id,
prdcatg_product_number,
prdcatg_product_category_type,
prdcatg_product_attrib_value,
prdcatg_pro_start_date,
prdcatg_pro_end_date,
prdcatg_ndc,
prdcatg_fk_agx_trade_rec_id,
prdcatg_date_modified,
prdcatg_date_created,
prdaesi_user_modified,
prdaesi_user_created,
prdaesi_spr_id,
prdaesi_soc_name,
prdaesi_record_id,
prdaesi_meddra_version,
prdaesi_meddra_pt_term,
prdaesi_meddra_llt_code,
prdaesi_meddra_code,
prdaesi_is_oth_seriouness,
prdaesi_is_lifethreatening,
prdaesi_is_fatal,
prdaesi_fk_pro_rec_id,
prdaesi_fk_dsofaesi_rec_id,
prdaesi_date_modified,
prdaesi_date_created,
prdaesi_aesi_smqcmq_term,
prdaesi_aesi_smqcmq_code,
prdaesi_aesi_broad_narrow)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,trdbrand_user_modified,
trdbrand_user_created,
trdbrand_spr_id,
trdbrand_record_id,
trdbrand_fk_agx_trade_rec_id,
trdbrand_firm_name,
trdbrand_firm_function,
trdbrand_fei_no,
trdbrand_date_modified,
trdbrand_date_created,
trdbrand_account_record_id,
trdbrand_account_id,
prd_warranty_period_years,
prd_warranty_period_months,
prd_viewpdfflag,
prd_version,
prd_vaccine_type,
prd_user_modified,
prd_user_created,
prd_unstructured_strength,
prd_unit_price,
prd_unit_of_presentation,
prd_udi,
prd_task_flag,
prd_target_population_part,
prd_synchonization_date,
prd_subject_to_risk,
prd_study_product_type,
prd_strength_unit,
prd_strength_flag,
prd_strength,
prd_staging_status,
prd_spr_id,
prd_sim_pro_flag,
prd_route_of_admin_term_id,
prd_route_of_admin,
prd_rm_flag,
prd_redact_flag,
prd_record_id,
prd_reason_for_deactive,
prd_qc_review,
prd_product_version,
prd_product_used_in,
prd_product_url,
prd_product_type_de_ml,
prd_product_type,
prd_product_status,
prd_product_name_upper,
prd_product_name_reported_chinese,
prd_product_name,
prd_product_id,
prd_product_group_de_ml,
prd_product_group,
prd_product_description,
prd_product_class,
prd_product_approval_status,
prd_product_active,
prd_prod_whodd_decode,
prd_prod_vigilancetype,
prd_prod_invented_name,
prd_prod_intdev_birthdate,
prd_prod_device_name,
prd_pro_component_type,
prd_prefered_code,
prd_pref_name_code,
prd_pmda_medicine_name,
prd_pharma_dose_form_term_id,
prd_pharma_dose_form_part,
prd_part_no,
prd_pai,
prd_package_description,
prd_pack_part,
prd_otc_risk_classfn,
prd_notified_body_ident_no,
prd_nlp_corpus_sync_status,
prd_multiple_ingredients,
prd_mpid_ver_number,
prd_mpid_ver_date,
prd_mpid,
prd_model_number,
prd_mfr_name,
prd_med_product_cpd,
prd_manufacturer_group,
prd_manufactured_by,
prd_made_by_de_ml,
prd_made_by,
prd_lsmv_product_id_for_litpro,
prd_listednessgrp_to,
prd_language,
prd_keywords,
prd_kdd_code,
prd_international_birth_date,
prd_internal_drug_code,
prd_inteded_use_name,
prd_impid_cross_ref,
prd_gmdn_code,
prd_generic_namecn,
prd_generic_name,
prd_future_product,
prd_formulation_part,
prd_form_admin_de_ml,
prd_form_admin,
prd_fk_base_prd_rec_id,
prd_fei_no,
prd_fda_registration_number,
prd_external_id,
prd_external_app_updated_date,
prd_drl_code,
prd_disable_lsrims_fields,
prd_device_usage,
prd_device_quantity,
prd_device_part,
prd_department_division,
prd_date_modified,
prd_date_created,
prd_created_date,
prd_cpd_jddcode,
prd_container_size,
prd_conflict_comments,
prd_company_product,
prd_company_name_part,
prd_combined_pharma_dose,
prd_combined_cdc,
prd_class_of_device,
prd_centers_for_medicare,
prd_center,
prd_catalogue_number,
prd_case_processing_system,
prd_black_traingle_product,
prd_auto_labelling_active,
prd_auto_approval,
prd_atc_vet_code,
prd_atc_code,
prd_arisg_record_id,
prd_allergenicity_indicator,
prd_ag_prod_sync_operation,
prd_ag_prod_sync_old_name,
prd_active_ing_record_id,
prd_account_record_id,
prd_access_to_otc,
prdtradname_verified,
prdtradname_vaccine_type,
prdtradname_user_modified,
prdtradname_user_created,
prdtradname_type_of_device_other,
prdtradname_type_of_device,
prdtradname_trademark_name,
prdtradname_trade_mdrtype,
prdtradname_trade_ivdrtype,
prdtradname_trade_id,
prdtradname_tiken,
prdtradname_strength_unit,
prdtradname_strength,
prdtradname_spr_id,
prdtradname_source,
prdtradname_route_of_admin,
prdtradname_record_id,
prdtradname_proprietary_name_suffix,
prdtradname_product_type,
prdtradname_prod_drlcode,
prdtradname_pmda_medicine_name,
prdtradname_otc_risk_classfn,
prdtradname_otc_altnte_id,
prdtradname_notified_cenum,
prdtradname_ndc_no,
prdtradname_mpid_ver_number,
prdtradname_mpid_ver_date,
prdtradname_mpid,
prdtradname_medicinal_product_name,
prdtradname_med_product_ltn,
prdtradname_marketting_date,
prdtradname_marketing_status,
prdtradname_marketing_category,
prdtradname_mah_type,
prdtradname_mah_name,
prdtradname_ltn_package_desc,
prdtradname_ltn_jddcode,
prdtradname_ltn_component_type,
prdtradname_local_tradename,
prdtradname_license_status_de_ml,
prdtradname_license_status,
prdtradname_license_id,
prdtradname_kdd_code,
prdtradname_internal_prd_id,
prdtradname_inteded_use_name,
prdtradname_identify_not_body,
prdtradname_health_canada_idnum,
prdtradname_form_admin,
prdtradname_fk_app_rec_id,
prdtradname_fk_agx_product_rec_id,
prdtradname_fei_no,
prdtradname_external_id,
prdtradname_disable_lsrims_fields,
prdtradname_dev_marketyears,
prdtradname_dev_marketmonths,
prdtradname_dev_marketedbefore,
prdtradname_dev_commerdate,
prdtradname_demographic_was_designed,
prdtradname_date_modified,
prdtradname_date_created,
prdtradname_data_sheet_rec_id,
prdtradname_country_code,
prdtradname_copy_approval_checked,
prdtradname_container_type,
prdtradname_company_unit_record_id_mah,
prdtradname_code_name,
prdtradname_clinical_drug_code,
prdtradname_class_of_devices,
prdtradname_bio_ref_name,
prdtradname_atc_vet_code,
prdtradname_atc_code,
prdtradname_arisg_record_id,
prdtradname_arisg_product_id,
prdtradname_arisg_approval_id,
prdtradname_approval_type_de_ml,
prdtradname_approval_type,
prdtradname_approval_submission_type,
prdtradname_approval_status,
prdtradname_approval_start_date,
prdtradname_approval_no,
prdtradname_approval_desc,
prdtradname_approval_date,
prdtradname_application_no,
prdtradname_anda_no,
prdtradname_agent_name,
prdtradname_account_record_id_mah,
prdtradname_account_record_id_agent,
prdtradname_access_to_otc,
prdlicense_user_modified,
prdlicense_user_created,
prdlicense_trade_name,
prdlicense_spr_id,
prdlicense_record_id,
prdlicense_pref_name_code,
prdlicense_original_license_no,
prdlicense_license_status,
prdlicense_license_record_id,
prdlicense_fk_product_id,
prdlicense_date_modified,
prdlicense_date_created,
prdgrp_user_modified,
prdgrp_user_created,
prdgrp_tradename_search_result,
prdgrp_spr_id,
prdgrp_search_type,
prdgrp_satrt_date,
prdgrp_record_id,
prdgrp_product_search_result,
prdgrp_product_group_name,
prdgrp_product_coding_level,
prdgrp_end_date,
prdgrp_description,
prdgrp_date_modified,
prdgrp_date_created,
prdgrp_assigned_to,
prdgrp_assign_to,
prdgrp_activeing_search_result,
prdgrp_active,
prdgrpsrch_value,
prdgrpsrch_user_modified,
prdgrpsrch_user_created,
prdgrpsrch_to_date_value,
prdgrpsrch_test,
prdgrpsrch_spr_id,
prdgrpsrch_record_id,
prdgrpsrch_fk_product_grp_rec_id,
prdgrpsrch_field_name,
prdgrpsrch_decode_values,
prdgrpsrch_date_value,
prdgrpsrch_date_modified,
prdgrpsrch_date_created,
prdgrpsrch_condition,
prddvc_warranty_period_years,
prddvc_warranty_period_months,
prddvc_user_modified,
prddvc_user_created,
prddvc_udi,
prddvc_spr_id,
prddvc_record_id,
prddvc_prod_device_cenum,
prddvc_prod_autho_rep,
prddvc_prod_artg_num,
prddvc_nomenclature_code,
prddvc_nomen_sysothtext,
prddvc_model_number,
prddvc_manufactured_by,
prddvc_is_prod_combination,
prddvc_fk_product_rec_id,
prddvc_fda_reg_num,
prddvc_disable_lsrims_fields,
prddvc_device_usage,
prddvc_device_type,
prddvc_device_quantity,
prddvc_device_pro_code,
prddvc_device_part,
prddvc_device_manufactured_by,
prddvc_dev_udi_value,
prddvc_dev_udi_type,
prddvc_dev_nomendecode,
prddvc_dev_manf_site,
prddvc_dev_compoversion,
prddvc_dev_compotermid,
prddvc_dev_compo_name,
prddvc_department_division,
prddvc_date_modified,
prddvc_date_created,
prddvc_competent_authoriy,
prddvc_common_dev_name,
prddvc_code_value,
prddvc_code_system,
prddvc_class_of_device,
prddvc_centers_for_medicare,
prddvc_catalogue_number,
prddvc_brand_name,
prddvc_allergenicity_indicator,
prdcntry_user_modified,
prdcntry_user_created,
prdcntry_spr_id,
prdcntry_record_id,
prdcntry_fk_apt_rec_id,
prdcntry_date_modified,
prdcntry_date_created,
prdcntry_country_code,
prdcntry_approval_type,
prdcntry_approval_no,
prdcntry_approval_id,
prdcatg_user_modified,
prdcatg_user_created,
prdcatg_spr_id,
prdcatg_record_id,
prdcatg_product_number,
prdcatg_product_category_type,
prdcatg_product_attrib_value,
prdcatg_pro_start_date,
prdcatg_pro_end_date,
prdcatg_ndc,
prdcatg_fk_agx_trade_rec_id,
prdcatg_date_modified,
prdcatg_date_created,
prdaesi_user_modified,
prdaesi_user_created,
prdaesi_spr_id,
prdaesi_soc_name,
prdaesi_record_id,
prdaesi_meddra_version,
prdaesi_meddra_pt_term,
prdaesi_meddra_llt_code,
prdaesi_meddra_code,
prdaesi_is_oth_seriouness,
prdaesi_is_lifethreatening,
prdaesi_is_fatal,
prdaesi_fk_pro_rec_id,
prdaesi_fk_dsofaesi_rec_id,
prdaesi_date_modified,
prdaesi_date_created,
prdaesi_aesi_smqcmq_term,
prdaesi_aesi_smqcmq_code,
prdaesi_aesi_broad_narrow
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP 
WHERE TO_DATE(LS_DB_PRODUCT_LIB.PROCESSING_DT) < TO_DATE(LS_DB_PRODUCT_LIB_TMP.PROCESSING_DT)
AND LS_DB_PRODUCT_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LIB_TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_LIB.prd_RECORD_ID = LS_DB_PRODUCT_LIB_TMP.prd_RECORD_ID
AND LS_DB_PRODUCT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PRODUCT_LIB.prdcntry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdcatg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prddvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdaesi_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdtradname_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdlicense_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.trdbrand_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LIB.prdgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_LIB_TMP.prdcntry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdcatg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prddvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdaesi_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdtradname_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdlicense_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.trdbrand_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LIB_TMP.prdgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB TGT
WHERE  ( prd_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product') OR prdaesi_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_aesi') OR prdcatg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_category') OR prdcntry_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_country') OR prddvc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_device') OR prdgrp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group') OR prdgrpsrch_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group_search') OR prdlicense_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_license') OR prdtradname_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_tradename') OR trdbrand_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_trade_brand_name')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( prd_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product') OR prdaesi_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_aesi') OR prdcatg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_category') OR prdcntry_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_country') OR prddvc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_device') OR prdgrp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group') OR prdgrpsrch_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_group_search') OR prdlicense_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_license') OR prdtradname_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_tradename') OR trdbrand_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_trade_brand_name')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PRODUCT_LIB'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PRODUCT_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LIB'),
                'LSDB','Case','LS_DB_PRODUCT_LIB',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_PRODUCT_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
