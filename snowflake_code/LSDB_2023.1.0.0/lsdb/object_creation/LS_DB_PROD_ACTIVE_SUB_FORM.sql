
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PROD_ACTIVE_SUB_FORM()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'),
	'LSRA','Case','LS_DB_PROD_ACTIVE_SUB_FORM',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PROD_ACTIVE_SUB_FORM' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PROD_ACTIVE_SUB_FORM'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PROD_ACTIVE_SUB_FORM'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  As select RECORD_ID,'lsmv_product_active_substance' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_active_substance WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_actives' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_actives WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_product_formulation' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_formulation WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_actives WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_apas_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_actives WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_active_substance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_active_substance WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_formulation WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_apas_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_formulation WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PROD_ACTIVE_SUB_FORM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_product_formulation_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdform_date_created,date_modified  prdform_date_modified,fk_apas_rec_id  prdform_fk_apas_rec_id,formulation_code  prdform_formulation_code,formulation_name  prdform_formulation_name,record_id  prdform_record_id,spr_id  prdform_spr_id,user_created  prdform_user_created,user_modified  prdform_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_formulation
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_apas_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_formulation')
  ) where REC_RANK=1 )
  , lsmv_product_actives_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdact_date_created,date_modified  prdact_date_modified,fk_apas_rec_id  prdact_fk_apas_rec_id,made_by  prdact_made_by,product_status  prdact_product_status,record_id  prdact_record_id,spr_id  prdact_spr_id,user_created  prdact_user_created,user_modified  prdact_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_actives
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_apas_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_actives')
  ) where REC_RANK=1 )
  , lsmv_product_active_substance_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdactsub_date_created,date_modified  prdactsub_date_modified,generic_name  prdactsub_generic_name,generic_name_upper  prdactsub_generic_name_upper,iso_site  prdactsub_iso_site,record_id  prdactsub_record_id,spr_id  prdactsub_spr_id,user_created  prdactsub_user_created,user_modified  prdactsub_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_active_substance
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_active_substance')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_product_formulation_SUBSET.prdform_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_active_substance_SUBSET.prdactsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_product_actives_SUBSET.prdact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_product_active_substance_SUBSET.prdactsub_USER_MODIFIED USER_MODIFIED,lsmv_product_active_substance_SUBSET.prdactsub_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_product_active_substance_SUBSET.prdactsub_USER_CREATED CREATED_BY,lsmv_product_active_substance_SUBSET.prdactsub_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_product_formulation_SUBSET.prdform_user_modified  ,lsmv_product_formulation_SUBSET.prdform_user_created  ,lsmv_product_formulation_SUBSET.prdform_spr_id  ,lsmv_product_formulation_SUBSET.prdform_record_id  ,lsmv_product_formulation_SUBSET.prdform_formulation_name  ,lsmv_product_formulation_SUBSET.prdform_formulation_code  ,lsmv_product_formulation_SUBSET.prdform_fk_apas_rec_id  ,lsmv_product_formulation_SUBSET.prdform_date_modified  ,lsmv_product_formulation_SUBSET.prdform_date_created  ,lsmv_product_actives_SUBSET.prdact_user_modified  ,lsmv_product_actives_SUBSET.prdact_user_created  ,lsmv_product_actives_SUBSET.prdact_spr_id  ,lsmv_product_actives_SUBSET.prdact_record_id  ,lsmv_product_actives_SUBSET.prdact_product_status  ,lsmv_product_actives_SUBSET.prdact_made_by  ,lsmv_product_actives_SUBSET.prdact_fk_apas_rec_id  ,lsmv_product_actives_SUBSET.prdact_date_modified  ,lsmv_product_actives_SUBSET.prdact_date_created  ,lsmv_product_active_substance_SUBSET.prdactsub_user_modified  ,lsmv_product_active_substance_SUBSET.prdactsub_user_created  ,lsmv_product_active_substance_SUBSET.prdactsub_spr_id  ,lsmv_product_active_substance_SUBSET.prdactsub_record_id  ,lsmv_product_active_substance_SUBSET.prdactsub_iso_site  ,lsmv_product_active_substance_SUBSET.prdactsub_generic_name_upper  ,lsmv_product_active_substance_SUBSET.prdactsub_generic_name  ,lsmv_product_active_substance_SUBSET.prdactsub_date_modified  ,lsmv_product_active_substance_SUBSET.prdactsub_date_created ,CONCAT(NVL(lsmv_product_formulation_SUBSET.prdform_RECORD_ID,-1),'||',NVL(lsmv_product_active_substance_SUBSET.prdactsub_RECORD_ID,-1),'||',NVL(lsmv_product_actives_SUBSET.prdact_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_product_active_substance_SUBSET  LEFT JOIN lsmv_product_actives_SUBSET ON lsmv_product_active_substance_SUBSET.prdactsub_record_id=lsmv_product_actives_SUBSET.prdact_fk_apas_rec_id
                         LEFT JOIN lsmv_product_formulation_SUBSET ON lsmv_product_active_substance_SUBSET.prdactsub_record_id=lsmv_product_formulation_SUBSET.prdform_fk_apas_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP)
where target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM   
SET LS_DB_PROD_ACTIVE_SUB_FORM.prdform_user_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_user_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_user_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_user_created,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_spr_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_spr_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_record_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_record_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_formulation_name = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_formulation_name,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_formulation_code = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_formulation_code,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_fk_apas_rec_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_fk_apas_rec_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_date_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_date_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdform_date_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_date_created,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_user_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_user_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_user_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_user_created,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_spr_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_spr_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_record_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_record_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_product_status = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_product_status,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_made_by = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_made_by,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_fk_apas_rec_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_fk_apas_rec_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_date_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_date_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdact_date_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_date_created,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_user_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_user_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_user_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_user_created,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_spr_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_spr_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_record_id = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_record_id,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_iso_site = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_iso_site,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_generic_name_upper = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_generic_name_upper,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_generic_name = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_generic_name,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_date_modified = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_date_modified,LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_date_created = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_date_created,
LS_DB_PROD_ACTIVE_SUB_FORM.PROCESSING_DT = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.PROCESSING_DT,
LS_DB_PROD_ACTIVE_SUB_FORM.user_modified  =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.user_modified     ,
LS_DB_PROD_ACTIVE_SUB_FORM.date_modified  =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.date_modified     ,
LS_DB_PROD_ACTIVE_SUB_FORM.expiry_date    =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.expiry_date       ,
LS_DB_PROD_ACTIVE_SUB_FORM.created_by     =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.created_by        ,
LS_DB_PROD_ACTIVE_SUB_FORM.created_dt     =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.created_dt        ,
LS_DB_PROD_ACTIVE_SUB_FORM.load_ts        =LS_DB_PROD_ACTIVE_SUB_FORM_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP 
WHERE 	LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PROD_ACTIVE_SUB_FORM_TMP.PROCESSING_DT = LS_DB_PROD_ACTIVE_SUB_FORM.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdform_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PROD_ACTIVE_SUB_FORM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID ,LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP 
ON LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID=LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_RECORD_ID
AND LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID 
where LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID  is null AND LS_DB_PROD_ACTIVE_SUB_FORM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID in (select prdactsub_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP )
) TMP where LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PROD_ACTIVE_SUB_FORM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID ,LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP 
    ON LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID=LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_RECORD_ID
    AND LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID 
    where LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID  is null AND LS_DB_PROD_ACTIVE_SUB_FORM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID in (select prdactsub_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdform_user_modified,
prdform_user_created,
prdform_spr_id,
prdform_record_id,
prdform_formulation_name,
prdform_formulation_code,
prdform_fk_apas_rec_id,
prdform_date_modified,
prdform_date_created,
prdact_user_modified,
prdact_user_created,
prdact_spr_id,
prdact_record_id,
prdact_product_status,
prdact_made_by,
prdact_fk_apas_rec_id,
prdact_date_modified,
prdact_date_created,
prdactsub_user_modified,
prdactsub_user_created,
prdactsub_spr_id,
prdactsub_record_id,
prdactsub_iso_site,
prdactsub_generic_name_upper,
prdactsub_generic_name,
prdactsub_date_modified,
prdactsub_date_created)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,prdform_user_modified,
prdform_user_created,
prdform_spr_id,
prdform_record_id,
prdform_formulation_name,
prdform_formulation_code,
prdform_fk_apas_rec_id,
prdform_date_modified,
prdform_date_created,
prdact_user_modified,
prdact_user_created,
prdact_spr_id,
prdact_record_id,
prdact_product_status,
prdact_made_by,
prdact_fk_apas_rec_id,
prdact_date_modified,
prdact_date_created,
prdactsub_user_modified,
prdactsub_user_created,
prdactsub_spr_id,
prdactsub_record_id,
prdactsub_iso_site,
prdactsub_generic_name_upper,
prdactsub_generic_name,
prdactsub_date_modified,
prdactsub_date_created
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP 
WHERE 	TO_DATE(LS_DB_PROD_ACTIVE_SUB_FORM.PROCESSING_DT) < TO_DATE(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.PROCESSING_DT)
AND LS_DB_PROD_ACTIVE_SUB_FORM.INTEGRATION_ID = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.INTEGRATION_ID
AND LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_RECORD_ID = LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_RECORD_ID
AND LS_DB_PROD_ACTIVE_SUB_FORM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdform_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdactsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM.prdact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdform_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdactsub_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PROD_ACTIVE_SUB_FORM_TMP.prdact_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM TGT
WHERE  ( prdactsub_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_active_substance') OR prdact_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_actives') OR prdform_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_formulation')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( prdactsub_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_active_substance') OR prdact_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_actives') OR prdform_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PROD_ACTIVE_SUB_FORM_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_formulation')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PROD_ACTIVE_SUB_FORM_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PROD_ACTIVE_SUB_FORM Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PROD_ACTIVE_SUB_FORM'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PROD_ACTIVE_SUB_FORM not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
