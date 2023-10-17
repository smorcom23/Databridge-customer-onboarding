
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PROD_INGREDIENT_LIB()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_INGREDIENT_LIB'),
	'LSRA','Case','LS_DB_PROD_INGREDIENT_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PROD_INGREDIENT_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PROD_INGREDIENT_LIB'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PROD_INGREDIENT_LIB'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_active_ingredient' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_active_ingredient WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_ingredient' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_ingredient WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_ingredient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ingredient_recid record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_ingredient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_active_ingredient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_active_ingredient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_INGREDIENT_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_active_ingredient_SUBSET AS 
(
select * from 
    (SELECT  
    active_ingredient_id  actingre_active_ingredient_id,active_ingredients  actingre_active_ingredients,active_ingredients_text  actingre_active_ingredients_text,atc_code  actingre_atc_code,bdnum  actingre_bdnum,cas_num_substance  actingre_cas_num_substance,class_ingredient  actingre_class_ingredient,class_of_ingredient  actingre_class_of_ingredient,date_created  actingre_date_created,date_modified  actingre_date_modified,disable_lsrims_fields  actingre_disable_lsrims_fields,eu_tct_code  actingre_eu_tct_code,external_id  actingre_external_id,fda_subst_code  actingre_fda_subst_code,fk_product_rec_id  actingre_fk_product_rec_id,ingredient_code  actingre_ingredient_code,ingredient_role  actingre_ingredient_role,kdd_code  actingre_kdd_code,lsmv_substance_id_for_litpro  actingre_lsmv_substance_id_for_litpro,mf_name  actingre_mf_name,preferred_who_dd  actingre_preferred_who_dd,record_id  actingre_record_id,spr_id  actingre_spr_id,strength  actingre_strength,strength_flag  actingre_strength_flag,strength_unit  actingre_strength_unit,substance_id  actingre_substance_id,substance_term_id  actingre_substance_term_id,substance_version_date_no  actingre_substance_version_date_no,unii  actingre_unii,unstructured_strength  actingre_unstructured_strength,user_created  actingre_user_created,user_modified  actingre_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_active_ingredient
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_ingredient')
  ) where REC_RANK=1 )
  , lsmv_product_ingredient_SUBSET AS 
(
select * from 
    (SELECT  
    active_ingredients_text  prdingre_active_ingredients_text,cas_number  prdingre_cas_number,class_ingredient  prdingre_class_ingredient,date_created  prdingre_date_created,date_modified  prdingre_date_modified,disable_lsrims_fields  prdingre_disable_lsrims_fields,external_id  prdingre_external_id,fk_account_rec_id  prdingre_fk_account_rec_id,fk_product_rec_id  prdingre_fk_product_rec_id,ingredient_code  prdingre_ingredient_code,ingredient_recid  prdingre_ingredient_recid,ingredient_role  prdingre_ingredient_role,ingredient_strength_unit  prdingre_ingredient_strength_unit,ingredient_type  prdingre_ingredient_type,kdd_code  prdingre_kdd_code,measurement_point  prdingre_measurement_point,record_id  prdingre_record_id,spr_id  prdingre_spr_id,ssi_confidentiality  prdingre_ssi_confidentiality,ssi_group  prdingre_ssi_group,strength  prdingre_strength,strength_flag  prdingre_strength_flag,strength_unit  prdingre_strength_unit,unstructured_strength  prdingre_unstructured_strength,user_created  prdingre_user_created,user_modified  prdingre_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_ingredient
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ingredient_recid IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_ingredient')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_active_ingredient_SUBSET.actingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_ingredient_SUBSET.prdingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_active_ingredient_SUBSET.actingre_USER_MODIFIED USER_MODIFIED,lsmv_active_ingredient_SUBSET.actingre_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_active_ingredient_SUBSET.actingre_USER_CREATED CREATED_BY,lsmv_active_ingredient_SUBSET.actingre_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_product_ingredient_SUBSET.prdingre_user_modified  ,lsmv_product_ingredient_SUBSET.prdingre_user_created  ,lsmv_product_ingredient_SUBSET.prdingre_unstructured_strength  ,lsmv_product_ingredient_SUBSET.prdingre_strength_unit  ,lsmv_product_ingredient_SUBSET.prdingre_strength_flag  ,lsmv_product_ingredient_SUBSET.prdingre_strength  ,lsmv_product_ingredient_SUBSET.prdingre_ssi_group  ,lsmv_product_ingredient_SUBSET.prdingre_ssi_confidentiality  ,lsmv_product_ingredient_SUBSET.prdingre_spr_id  ,lsmv_product_ingredient_SUBSET.prdingre_record_id  ,lsmv_product_ingredient_SUBSET.prdingre_measurement_point  ,lsmv_product_ingredient_SUBSET.prdingre_kdd_code  ,lsmv_product_ingredient_SUBSET.prdingre_ingredient_type  ,lsmv_product_ingredient_SUBSET.prdingre_ingredient_strength_unit  ,lsmv_product_ingredient_SUBSET.prdingre_ingredient_role  ,lsmv_product_ingredient_SUBSET.prdingre_ingredient_recid  ,lsmv_product_ingredient_SUBSET.prdingre_ingredient_code  ,lsmv_product_ingredient_SUBSET.prdingre_fk_product_rec_id  ,lsmv_product_ingredient_SUBSET.prdingre_fk_account_rec_id  ,lsmv_product_ingredient_SUBSET.prdingre_external_id  ,lsmv_product_ingredient_SUBSET.prdingre_disable_lsrims_fields  ,lsmv_product_ingredient_SUBSET.prdingre_date_modified  ,lsmv_product_ingredient_SUBSET.prdingre_date_created  ,lsmv_product_ingredient_SUBSET.prdingre_class_ingredient  ,lsmv_product_ingredient_SUBSET.prdingre_cas_number  ,lsmv_product_ingredient_SUBSET.prdingre_active_ingredients_text  ,lsmv_active_ingredient_SUBSET.actingre_user_modified  ,lsmv_active_ingredient_SUBSET.actingre_user_created  ,lsmv_active_ingredient_SUBSET.actingre_unstructured_strength  ,lsmv_active_ingredient_SUBSET.actingre_unii  ,lsmv_active_ingredient_SUBSET.actingre_substance_version_date_no  ,lsmv_active_ingredient_SUBSET.actingre_substance_term_id  ,lsmv_active_ingredient_SUBSET.actingre_substance_id  ,lsmv_active_ingredient_SUBSET.actingre_strength_unit  ,lsmv_active_ingredient_SUBSET.actingre_strength_flag  ,lsmv_active_ingredient_SUBSET.actingre_strength  ,lsmv_active_ingredient_SUBSET.actingre_spr_id  ,lsmv_active_ingredient_SUBSET.actingre_record_id  ,lsmv_active_ingredient_SUBSET.actingre_preferred_who_dd  ,lsmv_active_ingredient_SUBSET.actingre_mf_name  ,lsmv_active_ingredient_SUBSET.actingre_lsmv_substance_id_for_litpro  ,lsmv_active_ingredient_SUBSET.actingre_kdd_code  ,lsmv_active_ingredient_SUBSET.actingre_ingredient_role  ,lsmv_active_ingredient_SUBSET.actingre_ingredient_code  ,lsmv_active_ingredient_SUBSET.actingre_fk_product_rec_id  ,lsmv_active_ingredient_SUBSET.actingre_fda_subst_code  ,lsmv_active_ingredient_SUBSET.actingre_external_id  ,lsmv_active_ingredient_SUBSET.actingre_eu_tct_code  ,lsmv_active_ingredient_SUBSET.actingre_disable_lsrims_fields  ,lsmv_active_ingredient_SUBSET.actingre_date_modified  ,lsmv_active_ingredient_SUBSET.actingre_date_created  ,lsmv_active_ingredient_SUBSET.actingre_class_of_ingredient  ,lsmv_active_ingredient_SUBSET.actingre_class_ingredient  ,lsmv_active_ingredient_SUBSET.actingre_cas_num_substance  ,lsmv_active_ingredient_SUBSET.actingre_bdnum  ,lsmv_active_ingredient_SUBSET.actingre_atc_code  ,lsmv_active_ingredient_SUBSET.actingre_active_ingredients_text  ,lsmv_active_ingredient_SUBSET.actingre_active_ingredients  ,lsmv_active_ingredient_SUBSET.actingre_active_ingredient_id ,CONCAT(NVL(lsmv_active_ingredient_SUBSET.actingre_RECORD_ID,-1),'||',NVL(lsmv_product_ingredient_SUBSET.prdingre_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_active_ingredient_SUBSET  LEFT JOIN lsmv_product_ingredient_SUBSET ON lsmv_active_ingredient_SUBSET.actingre_record_id=lsmv_product_ingredient_SUBSET.prdingre_ingredient_recid
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP)
where target_table_name='LS_DB_PROD_INGREDIENT_LIB'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_INGREDIENT_LIB'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB   
SET LS_DB_PROD_INGREDIENT_LIB.prdingre_user_modified = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_user_modified,LS_DB_PROD_INGREDIENT_LIB.prdingre_user_created = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_user_created,LS_DB_PROD_INGREDIENT_LIB.prdingre_unstructured_strength = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_unstructured_strength,LS_DB_PROD_INGREDIENT_LIB.prdingre_strength_unit = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_strength_unit,LS_DB_PROD_INGREDIENT_LIB.prdingre_strength_flag = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_strength_flag,LS_DB_PROD_INGREDIENT_LIB.prdingre_strength = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_strength,LS_DB_PROD_INGREDIENT_LIB.prdingre_ssi_group = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ssi_group,LS_DB_PROD_INGREDIENT_LIB.prdingre_ssi_confidentiality = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ssi_confidentiality,LS_DB_PROD_INGREDIENT_LIB.prdingre_spr_id = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_spr_id,LS_DB_PROD_INGREDIENT_LIB.prdingre_record_id = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_record_id,LS_DB_PROD_INGREDIENT_LIB.prdingre_measurement_point = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_measurement_point,LS_DB_PROD_INGREDIENT_LIB.prdingre_kdd_code = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_kdd_code,LS_DB_PROD_INGREDIENT_LIB.prdingre_ingredient_type = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ingredient_type,LS_DB_PROD_INGREDIENT_LIB.prdingre_ingredient_strength_unit = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ingredient_strength_unit,LS_DB_PROD_INGREDIENT_LIB.prdingre_ingredient_role = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ingredient_role,LS_DB_PROD_INGREDIENT_LIB.prdingre_ingredient_recid = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ingredient_recid,LS_DB_PROD_INGREDIENT_LIB.prdingre_ingredient_code = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_ingredient_code,LS_DB_PROD_INGREDIENT_LIB.prdingre_fk_product_rec_id = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_fk_product_rec_id,LS_DB_PROD_INGREDIENT_LIB.prdingre_fk_account_rec_id = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_fk_account_rec_id,LS_DB_PROD_INGREDIENT_LIB.prdingre_external_id = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_external_id,LS_DB_PROD_INGREDIENT_LIB.prdingre_disable_lsrims_fields = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_disable_lsrims_fields,LS_DB_PROD_INGREDIENT_LIB.prdingre_date_modified = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_date_modified,LS_DB_PROD_INGREDIENT_LIB.prdingre_date_created = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_date_created,LS_DB_PROD_INGREDIENT_LIB.prdingre_class_ingredient = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_class_ingredient,LS_DB_PROD_INGREDIENT_LIB.prdingre_cas_number = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_cas_number,LS_DB_PROD_INGREDIENT_LIB.prdingre_active_ingredients_text = LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_active_ingredients_text,LS_DB_PROD_INGREDIENT_LIB.actingre_user_modified = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_user_modified,LS_DB_PROD_INGREDIENT_LIB.actingre_user_created = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_user_created,LS_DB_PROD_INGREDIENT_LIB.actingre_unstructured_strength = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_unstructured_strength,LS_DB_PROD_INGREDIENT_LIB.actingre_unii = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_unii,LS_DB_PROD_INGREDIENT_LIB.actingre_substance_version_date_no = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_substance_version_date_no,LS_DB_PROD_INGREDIENT_LIB.actingre_substance_term_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_substance_term_id,LS_DB_PROD_INGREDIENT_LIB.actingre_substance_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_substance_id,LS_DB_PROD_INGREDIENT_LIB.actingre_strength_unit = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_strength_unit,LS_DB_PROD_INGREDIENT_LIB.actingre_strength_flag = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_strength_flag,LS_DB_PROD_INGREDIENT_LIB.actingre_strength = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_strength,LS_DB_PROD_INGREDIENT_LIB.actingre_spr_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_spr_id,LS_DB_PROD_INGREDIENT_LIB.actingre_record_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_record_id,LS_DB_PROD_INGREDIENT_LIB.actingre_preferred_who_dd = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_preferred_who_dd,LS_DB_PROD_INGREDIENT_LIB.actingre_mf_name = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_mf_name,LS_DB_PROD_INGREDIENT_LIB.actingre_lsmv_substance_id_for_litpro = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_lsmv_substance_id_for_litpro,LS_DB_PROD_INGREDIENT_LIB.actingre_kdd_code = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_kdd_code,LS_DB_PROD_INGREDIENT_LIB.actingre_ingredient_role = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_ingredient_role,LS_DB_PROD_INGREDIENT_LIB.actingre_ingredient_code = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_ingredient_code,LS_DB_PROD_INGREDIENT_LIB.actingre_fk_product_rec_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_fk_product_rec_id,LS_DB_PROD_INGREDIENT_LIB.actingre_fda_subst_code = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_fda_subst_code,LS_DB_PROD_INGREDIENT_LIB.actingre_external_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_external_id,LS_DB_PROD_INGREDIENT_LIB.actingre_eu_tct_code = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_eu_tct_code,LS_DB_PROD_INGREDIENT_LIB.actingre_disable_lsrims_fields = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_disable_lsrims_fields,LS_DB_PROD_INGREDIENT_LIB.actingre_date_modified = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_date_modified,LS_DB_PROD_INGREDIENT_LIB.actingre_date_created = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_date_created,LS_DB_PROD_INGREDIENT_LIB.actingre_class_of_ingredient = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_class_of_ingredient,LS_DB_PROD_INGREDIENT_LIB.actingre_class_ingredient = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_class_ingredient,LS_DB_PROD_INGREDIENT_LIB.actingre_cas_num_substance = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_cas_num_substance,LS_DB_PROD_INGREDIENT_LIB.actingre_bdnum = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_bdnum,LS_DB_PROD_INGREDIENT_LIB.actingre_atc_code = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_atc_code,LS_DB_PROD_INGREDIENT_LIB.actingre_active_ingredients_text = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_active_ingredients_text,LS_DB_PROD_INGREDIENT_LIB.actingre_active_ingredients = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_active_ingredients,LS_DB_PROD_INGREDIENT_LIB.actingre_active_ingredient_id = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_active_ingredient_id,
LS_DB_PROD_INGREDIENT_LIB.PROCESSING_DT = LS_DB_PROD_INGREDIENT_LIB_TMP.PROCESSING_DT,
LS_DB_PROD_INGREDIENT_LIB.user_modified  =LS_DB_PROD_INGREDIENT_LIB_TMP.user_modified     ,
LS_DB_PROD_INGREDIENT_LIB.date_modified  =LS_DB_PROD_INGREDIENT_LIB_TMP.date_modified     ,
LS_DB_PROD_INGREDIENT_LIB.expiry_date    =LS_DB_PROD_INGREDIENT_LIB_TMP.expiry_date       ,
LS_DB_PROD_INGREDIENT_LIB.created_by     =LS_DB_PROD_INGREDIENT_LIB_TMP.created_by        ,
LS_DB_PROD_INGREDIENT_LIB.created_dt     =LS_DB_PROD_INGREDIENT_LIB_TMP.created_dt        ,
LS_DB_PROD_INGREDIENT_LIB.load_ts        =LS_DB_PROD_INGREDIENT_LIB_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP 
WHERE 	LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID = LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PROD_INGREDIENT_LIB_TMP.PROCESSING_DT = LS_DB_PROD_INGREDIENT_LIB.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PROD_INGREDIENT_LIB.actingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_INGREDIENT_LIB.prdingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PROD_INGREDIENT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID ,LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP 
ON LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID=LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_RECORD_ID
AND LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID = LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID 
where LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PROD_INGREDIENT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID in (select actingre_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP )
) TMP where LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PROD_INGREDIENT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID ,LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP 
    ON LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID=LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_RECORD_ID
    AND LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID = LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID 
    where LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PROD_INGREDIENT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID in (select actingre_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdingre_user_modified,
prdingre_user_created,
prdingre_unstructured_strength,
prdingre_strength_unit,
prdingre_strength_flag,
prdingre_strength,
prdingre_ssi_group,
prdingre_ssi_confidentiality,
prdingre_spr_id,
prdingre_record_id,
prdingre_measurement_point,
prdingre_kdd_code,
prdingre_ingredient_type,
prdingre_ingredient_strength_unit,
prdingre_ingredient_role,
prdingre_ingredient_recid,
prdingre_ingredient_code,
prdingre_fk_product_rec_id,
prdingre_fk_account_rec_id,
prdingre_external_id,
prdingre_disable_lsrims_fields,
prdingre_date_modified,
prdingre_date_created,
prdingre_class_ingredient,
prdingre_cas_number,
prdingre_active_ingredients_text,
actingre_user_modified,
actingre_user_created,
actingre_unstructured_strength,
actingre_unii,
actingre_substance_version_date_no,
actingre_substance_term_id,
actingre_substance_id,
actingre_strength_unit,
actingre_strength_flag,
actingre_strength,
actingre_spr_id,
actingre_record_id,
actingre_preferred_who_dd,
actingre_mf_name,
actingre_lsmv_substance_id_for_litpro,
actingre_kdd_code,
actingre_ingredient_role,
actingre_ingredient_code,
actingre_fk_product_rec_id,
actingre_fda_subst_code,
actingre_external_id,
actingre_eu_tct_code,
actingre_disable_lsrims_fields,
actingre_date_modified,
actingre_date_created,
actingre_class_of_ingredient,
actingre_class_ingredient,
actingre_cas_num_substance,
actingre_bdnum,
actingre_atc_code,
actingre_active_ingredients_text,
actingre_active_ingredients,
actingre_active_ingredient_id)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdingre_user_modified,
prdingre_user_created,
prdingre_unstructured_strength,
prdingre_strength_unit,
prdingre_strength_flag,
prdingre_strength,
prdingre_ssi_group,
prdingre_ssi_confidentiality,
prdingre_spr_id,
prdingre_record_id,
prdingre_measurement_point,
prdingre_kdd_code,
prdingre_ingredient_type,
prdingre_ingredient_strength_unit,
prdingre_ingredient_role,
prdingre_ingredient_recid,
prdingre_ingredient_code,
prdingre_fk_product_rec_id,
prdingre_fk_account_rec_id,
prdingre_external_id,
prdingre_disable_lsrims_fields,
prdingre_date_modified,
prdingre_date_created,
prdingre_class_ingredient,
prdingre_cas_number,
prdingre_active_ingredients_text,
actingre_user_modified,
actingre_user_created,
actingre_unstructured_strength,
actingre_unii,
actingre_substance_version_date_no,
actingre_substance_term_id,
actingre_substance_id,
actingre_strength_unit,
actingre_strength_flag,
actingre_strength,
actingre_spr_id,
actingre_record_id,
actingre_preferred_who_dd,
actingre_mf_name,
actingre_lsmv_substance_id_for_litpro,
actingre_kdd_code,
actingre_ingredient_role,
actingre_ingredient_code,
actingre_fk_product_rec_id,
actingre_fda_subst_code,
actingre_external_id,
actingre_eu_tct_code,
actingre_disable_lsrims_fields,
actingre_date_modified,
actingre_date_created,
actingre_class_of_ingredient,
actingre_class_ingredient,
actingre_cas_num_substance,
actingre_bdnum,
actingre_atc_code,
actingre_active_ingredients_text,
actingre_active_ingredients,
actingre_active_ingredient_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP 
WHERE 	TO_DATE(LS_DB_PROD_INGREDIENT_LIB.PROCESSING_DT) < TO_DATE(LS_DB_PROD_INGREDIENT_LIB_TMP.PROCESSING_DT)
AND LS_DB_PROD_INGREDIENT_LIB.INTEGRATION_ID = LS_DB_PROD_INGREDIENT_LIB_TMP.INTEGRATION_ID
AND LS_DB_PROD_INGREDIENT_LIB.actingre_RECORD_ID = LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_RECORD_ID
AND LS_DB_PROD_INGREDIENT_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PROD_INGREDIENT_LIB.actingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_INGREDIENT_LIB.prdingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PROD_INGREDIENT_LIB_TMP.actingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_INGREDIENT_LIB_TMP.prdingre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB TGT
WHERE  ( actingre_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_ingredient') OR prdingre_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_ingredient')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( actingre_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_active_ingredient') OR prdingre_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_INGREDIENT_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_ingredient')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_INGREDIENT_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PROD_INGREDIENT_LIB'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_INGREDIENT_LIB'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PROD_INGREDIENT_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PROD_INGREDIENT_LIB'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PROD_INGREDIENT_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
