
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LABEL_LIB()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LABEL_LIB'),
                'LSDB','Case','LS_DB_PRODUCT_LABEL_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PRODUCT_LABEL_LIB' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PRODUCT_LABEL_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_labelled_prod_attributes' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_attributes WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_labelled_prod_indication' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_indication WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_prod_listed_grp' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_prod_listed_grp WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_indication WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_prodlist_grp_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_indication WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_attributes WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_prodlist_grp_rec_id record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_attributes WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_prod_listed_grp WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_prod_listed_grp WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_LABEL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_labelled_prod_attributes_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  labprdattr_date_created,date_modified  labprdattr_date_modified,fk_llpe_rec_id  labprdattr_fk_llpe_rec_id,fk_prodlist_grp_rec_id  labprdattr_fk_prodlist_grp_rec_id,form_admin  labprdattr_form_admin,labeling_reference  labprdattr_labeling_reference,protocol_no  labprdattr_protocol_no,record_id  labprdattr_record_id,route_admin  labprdattr_route_admin,spr_id  labprdattr_spr_id,strength  labprdattr_strength,strength_unit  labprdattr_strength_unit,study_library_record_id  labprdattr_study_library_record_id,user_created  labprdattr_user_created,user_modified  labprdattr_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_attributes
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_prodlist_grp_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_attributes')
  ) where REC_RANK=1 )
  , lsmv_prod_listed_grp_SUBSET AS 
(
select * from 
    (SELECT  
    data_sheet_name  prdlistgrp_data_sheet_name,date_created  prdlistgrp_date_created,date_modified  prdlistgrp_date_modified,default_labeling  prdlistgrp_default_labeling,eff_labeling_date  prdlistgrp_eff_labeling_date,fk_prod_rec_id  prdlistgrp_fk_prod_rec_id,is_active  prdlistgrp_is_active,is_core  prdlistgrp_is_core,is_fatal  prdlistgrp_is_fatal,is_labeled  prdlistgrp_is_labeled,labeling_logpath  prdlistgrp_labeling_logpath,labelled_country  prdlistgrp_labelled_country,labelling_version  prdlistgrp_labelling_version,record_id  prdlistgrp_record_id,rule_id  prdlistgrp_rule_id,rule_name  prdlistgrp_rule_name,spr_id  prdlistgrp_spr_id,user_created  prdlistgrp_user_created,user_group_rec_id  prdlistgrp_user_group_rec_id,user_modified  prdlistgrp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_prod_listed_grp
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_prod_listed_grp')
  ) where REC_RANK=1 )
  , lsmv_labelled_prod_indication_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  labprdind_date_created,date_modified  labprdind_date_modified,fk_api_rec_id  labprdind_fk_api_rec_id,fk_lsmv_llpe_rec_id  labprdind_fk_lsmv_llpe_rec_id,fk_prodlist_grp_rec_id  labprdind_fk_prodlist_grp_rec_id,llt_code  labprdind_llt_code,medra_version  labprdind_medra_version,pt_code  labprdind_pt_code,record_id  labprdind_record_id,spr_id  labprdind_spr_id,user_created  labprdind_user_created,user_modified  labprdind_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_labelled_prod_indication
WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_prodlist_grp_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_indication')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_prod_listed_grp_SUBSET.prdlistgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_labelled_prod_attributes_SUBSET.labprdattr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_labelled_prod_indication_SUBSET.labprdind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_prod_listed_grp_SUBSET.prdlistgrp_USER_MODIFIED USER_MODIFIED,lsmv_prod_listed_grp_SUBSET.prdlistgrp_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_USER_CREATED CREATED_BY,lsmv_prod_listed_grp_SUBSET.prdlistgrp_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_prod_listed_grp_SUBSET.prdlistgrp_user_modified  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_user_group_rec_id  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_user_created  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_spr_id  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_rule_name  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_rule_id  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_record_id  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_labelling_version  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_labelled_country  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_labeling_logpath  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_is_labeled  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_is_fatal  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_is_core  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_is_active  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_fk_prod_rec_id  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_eff_labeling_date  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_default_labeling  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_date_modified  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_date_created  ,lsmv_prod_listed_grp_SUBSET.prdlistgrp_data_sheet_name  ,lsmv_labelled_prod_indication_SUBSET.labprdind_user_modified  ,lsmv_labelled_prod_indication_SUBSET.labprdind_user_created  ,lsmv_labelled_prod_indication_SUBSET.labprdind_spr_id  ,lsmv_labelled_prod_indication_SUBSET.labprdind_record_id  ,lsmv_labelled_prod_indication_SUBSET.labprdind_pt_code  ,lsmv_labelled_prod_indication_SUBSET.labprdind_medra_version  ,lsmv_labelled_prod_indication_SUBSET.labprdind_llt_code  ,lsmv_labelled_prod_indication_SUBSET.labprdind_fk_prodlist_grp_rec_id  ,lsmv_labelled_prod_indication_SUBSET.labprdind_fk_lsmv_llpe_rec_id  ,lsmv_labelled_prod_indication_SUBSET.labprdind_fk_api_rec_id  ,lsmv_labelled_prod_indication_SUBSET.labprdind_date_modified  ,lsmv_labelled_prod_indication_SUBSET.labprdind_date_created  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_user_modified  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_user_created  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_study_library_record_id  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_strength_unit  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_strength  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_spr_id  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_route_admin  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_record_id  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_protocol_no  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_labeling_reference  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_form_admin  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_fk_prodlist_grp_rec_id  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_fk_llpe_rec_id  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_date_modified  ,lsmv_labelled_prod_attributes_SUBSET.labprdattr_date_created ,CONCAT(NVL(lsmv_prod_listed_grp_SUBSET.prdlistgrp_RECORD_ID,-1),'||',NVL(lsmv_labelled_prod_attributes_SUBSET.labprdattr_RECORD_ID,-1),'||',NVL(lsmv_labelled_prod_indication_SUBSET.labprdind_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_prod_listed_grp_SUBSET  LEFT JOIN lsmv_labelled_prod_attributes_SUBSET ON lsmv_prod_listed_grp_SUBSET.prdlistgrp_record_id=lsmv_labelled_prod_attributes_SUBSET.labprdattr_fk_prodlist_grp_rec_id
                         LEFT JOIN lsmv_labelled_prod_indication_SUBSET ON lsmv_prod_listed_grp_SUBSET.prdlistgrp_record_id=lsmv_labelled_prod_indication_SUBSET.labprdind_fk_prodlist_grp_rec_id
                         WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP)
where target_table_name='LS_DB_PRODUCT_LABEL_LIB'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB   
SET LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_user_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_user_modified,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_user_group_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_user_group_rec_id,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_user_created = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_user_created,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_spr_id = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_spr_id,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_rule_name = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_rule_name,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_rule_id = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_rule_id,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_record_id = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_record_id,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_labelling_version = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_labelling_version,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_labelled_country = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_labelled_country,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_labeling_logpath = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_labeling_logpath,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_is_labeled = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_is_labeled,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_is_fatal = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_is_fatal,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_is_core = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_is_core,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_is_active = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_is_active,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_fk_prod_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_fk_prod_rec_id,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_eff_labeling_date = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_eff_labeling_date,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_default_labeling = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_default_labeling,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_date_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_date_modified,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_date_created = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_date_created,LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_data_sheet_name = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_data_sheet_name,LS_DB_PRODUCT_LABEL_LIB.labprdind_user_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_user_modified,LS_DB_PRODUCT_LABEL_LIB.labprdind_user_created = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_user_created,LS_DB_PRODUCT_LABEL_LIB.labprdind_spr_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_spr_id,LS_DB_PRODUCT_LABEL_LIB.labprdind_record_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_record_id,LS_DB_PRODUCT_LABEL_LIB.labprdind_pt_code = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_pt_code,LS_DB_PRODUCT_LABEL_LIB.labprdind_medra_version = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_medra_version,LS_DB_PRODUCT_LABEL_LIB.labprdind_llt_code = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_llt_code,LS_DB_PRODUCT_LABEL_LIB.labprdind_fk_prodlist_grp_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_fk_prodlist_grp_rec_id,LS_DB_PRODUCT_LABEL_LIB.labprdind_fk_lsmv_llpe_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_fk_lsmv_llpe_rec_id,LS_DB_PRODUCT_LABEL_LIB.labprdind_fk_api_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_fk_api_rec_id,LS_DB_PRODUCT_LABEL_LIB.labprdind_date_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_date_modified,LS_DB_PRODUCT_LABEL_LIB.labprdind_date_created = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_date_created,LS_DB_PRODUCT_LABEL_LIB.labprdattr_user_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_user_modified,LS_DB_PRODUCT_LABEL_LIB.labprdattr_user_created = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_user_created,LS_DB_PRODUCT_LABEL_LIB.labprdattr_study_library_record_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_study_library_record_id,LS_DB_PRODUCT_LABEL_LIB.labprdattr_strength_unit = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_strength_unit,LS_DB_PRODUCT_LABEL_LIB.labprdattr_strength = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_strength,LS_DB_PRODUCT_LABEL_LIB.labprdattr_spr_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_spr_id,LS_DB_PRODUCT_LABEL_LIB.labprdattr_route_admin = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_route_admin,LS_DB_PRODUCT_LABEL_LIB.labprdattr_record_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_record_id,LS_DB_PRODUCT_LABEL_LIB.labprdattr_protocol_no = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_protocol_no,LS_DB_PRODUCT_LABEL_LIB.labprdattr_labeling_reference = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_labeling_reference,LS_DB_PRODUCT_LABEL_LIB.labprdattr_form_admin = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_form_admin,LS_DB_PRODUCT_LABEL_LIB.labprdattr_fk_prodlist_grp_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_fk_prodlist_grp_rec_id,LS_DB_PRODUCT_LABEL_LIB.labprdattr_fk_llpe_rec_id = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_fk_llpe_rec_id,LS_DB_PRODUCT_LABEL_LIB.labprdattr_date_modified = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_date_modified,LS_DB_PRODUCT_LABEL_LIB.labprdattr_date_created = LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_date_created,
LS_DB_PRODUCT_LABEL_LIB.PROCESSING_DT = LS_DB_PRODUCT_LABEL_LIB_TMP.PROCESSING_DT,
LS_DB_PRODUCT_LABEL_LIB.user_modified  =LS_DB_PRODUCT_LABEL_LIB_TMP.user_modified     ,
LS_DB_PRODUCT_LABEL_LIB.date_modified  =LS_DB_PRODUCT_LABEL_LIB_TMP.date_modified     ,
LS_DB_PRODUCT_LABEL_LIB.expiry_date    =LS_DB_PRODUCT_LABEL_LIB_TMP.expiry_date       ,
LS_DB_PRODUCT_LABEL_LIB.created_by     =LS_DB_PRODUCT_LABEL_LIB_TMP.created_by        ,
LS_DB_PRODUCT_LABEL_LIB.created_dt     =LS_DB_PRODUCT_LABEL_LIB_TMP.created_dt        ,
LS_DB_PRODUCT_LABEL_LIB.load_ts        =LS_DB_PRODUCT_LABEL_LIB_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP 
WHERE LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PRODUCT_LABEL_LIB_TMP.PROCESSING_DT = LS_DB_PRODUCT_LABEL_LIB.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB.labprdattr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB.labprdind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID ,LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP 
ON LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID=LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_RECORD_ID
AND LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID 
where LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID in (select prdlistgrp_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP )
) TMP where LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID ,LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP 
    ON LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID=LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_RECORD_ID
    AND LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID 
    where LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID in (select prdlistgrp_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdlistgrp_user_modified,
prdlistgrp_user_group_rec_id,
prdlistgrp_user_created,
prdlistgrp_spr_id,
prdlistgrp_rule_name,
prdlistgrp_rule_id,
prdlistgrp_record_id,
prdlistgrp_labelling_version,
prdlistgrp_labelled_country,
prdlistgrp_labeling_logpath,
prdlistgrp_is_labeled,
prdlistgrp_is_fatal,
prdlistgrp_is_core,
prdlistgrp_is_active,
prdlistgrp_fk_prod_rec_id,
prdlistgrp_eff_labeling_date,
prdlistgrp_default_labeling,
prdlistgrp_date_modified,
prdlistgrp_date_created,
prdlistgrp_data_sheet_name,
labprdind_user_modified,
labprdind_user_created,
labprdind_spr_id,
labprdind_record_id,
labprdind_pt_code,
labprdind_medra_version,
labprdind_llt_code,
labprdind_fk_prodlist_grp_rec_id,
labprdind_fk_lsmv_llpe_rec_id,
labprdind_fk_api_rec_id,
labprdind_date_modified,
labprdind_date_created,
labprdattr_user_modified,
labprdattr_user_created,
labprdattr_study_library_record_id,
labprdattr_strength_unit,
labprdattr_strength,
labprdattr_spr_id,
labprdattr_route_admin,
labprdattr_record_id,
labprdattr_protocol_no,
labprdattr_labeling_reference,
labprdattr_form_admin,
labprdattr_fk_prodlist_grp_rec_id,
labprdattr_fk_llpe_rec_id,
labprdattr_date_modified,
labprdattr_date_created)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdlistgrp_user_modified,
prdlistgrp_user_group_rec_id,
prdlistgrp_user_created,
prdlistgrp_spr_id,
prdlistgrp_rule_name,
prdlistgrp_rule_id,
prdlistgrp_record_id,
prdlistgrp_labelling_version,
prdlistgrp_labelled_country,
prdlistgrp_labeling_logpath,
prdlistgrp_is_labeled,
prdlistgrp_is_fatal,
prdlistgrp_is_core,
prdlistgrp_is_active,
prdlistgrp_fk_prod_rec_id,
prdlistgrp_eff_labeling_date,
prdlistgrp_default_labeling,
prdlistgrp_date_modified,
prdlistgrp_date_created,
prdlistgrp_data_sheet_name,
labprdind_user_modified,
labprdind_user_created,
labprdind_spr_id,
labprdind_record_id,
labprdind_pt_code,
labprdind_medra_version,
labprdind_llt_code,
labprdind_fk_prodlist_grp_rec_id,
labprdind_fk_lsmv_llpe_rec_id,
labprdind_fk_api_rec_id,
labprdind_date_modified,
labprdind_date_created,
labprdattr_user_modified,
labprdattr_user_created,
labprdattr_study_library_record_id,
labprdattr_strength_unit,
labprdattr_strength,
labprdattr_spr_id,
labprdattr_route_admin,
labprdattr_record_id,
labprdattr_protocol_no,
labprdattr_labeling_reference,
labprdattr_form_admin,
labprdattr_fk_prodlist_grp_rec_id,
labprdattr_fk_llpe_rec_id,
labprdattr_date_modified,
labprdattr_date_created
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP 
WHERE TO_DATE(LS_DB_PRODUCT_LABEL_LIB.PROCESSING_DT) < TO_DATE(LS_DB_PRODUCT_LABEL_LIB_TMP.PROCESSING_DT)
AND LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LABEL_LIB_TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_RECORD_ID = LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_RECORD_ID
AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB.labprdattr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB.labprdind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.prdlistgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.labprdattr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_LABEL_LIB_TMP.labprdind_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB TGT
WHERE  ( labprdattr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_attributes') OR labprdind_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_indication') OR prdlistgrp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_prod_listed_grp')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( labprdattr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_attributes') OR labprdind_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_labelled_prod_indication') OR prdlistgrp_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRODUCT_LABEL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_prod_listed_grp')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PRODUCT_LABEL_LIB'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PRODUCT_LABEL_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LABEL_LIB'),
                'LSDB','Case','LS_DB_PRODUCT_LABEL_LIB',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_PRODUCT_LABEL_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
