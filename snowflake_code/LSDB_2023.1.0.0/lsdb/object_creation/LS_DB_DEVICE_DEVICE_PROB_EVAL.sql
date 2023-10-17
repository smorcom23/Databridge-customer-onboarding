
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_DEVICE_DEVICE_PROB_EVAL()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'),
	'LSRA','Case','LS_DB_DEVICE_DEVICE_PROB_EVAL',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DEVICE_DEVICE_PROB_EVAL'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DEVICE_DEVICE_PROB_EVAL'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  As select RECORD_ID,'lsmv_device_prob_eva' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eva WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_device_prob_eval_imdrf' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eval_imdrf WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9851','9945')
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
 
select DISTINCT record_id record_id, fk_device_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eval_imdrf WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_device_rec_id record_id, fk_device_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eval_imdrf WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id record_id, fk_device_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eva WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_device_rec_id record_id, fk_device_rec_id common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eva WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_DEVICE_PROB_EVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_device_prob_eval_imdrf_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  dvcprbevalimdrf_ari_rec_id,corrective_prevt_narrative  dvcprbevalimdrf_corrective_prevt_narrative,date_created  dvcprbevalimdrf_date_created,date_modified  dvcprbevalimdrf_date_modified,device_component_narrative  dvcprbevalimdrf_device_component_narrative,device_problem_code_narrative  dvcprbevalimdrf_device_problem_code_narrative,evaluation_type_imdrf  dvcprbevalimdrf_evaluation_type_imdrf,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9945' AND CD=CAST(evaluation_type_imdrf AS VARCHAR(100)) )dvcprbevalimdrf_evaluation_type_imdrf_de_ml , evaluation_type_imdrf_sf  dvcprbevalimdrf_evaluation_type_imdrf_sf,evaluation_value_imdrf  dvcprbevalimdrf_evaluation_value_imdrf,evaluation_value_imdrf_sf  dvcprbevalimdrf_evaluation_value_imdrf_sf,fk_device_rec_id  dvcprbevalimdrf_fk_device_rec_id,identified_acton_narrative  dvcprbevalimdrf_identified_acton_narrative,imdrf_code  dvcprbevalimdrf_imdrf_code,imdrf_investigation_narrative  dvcprbevalimdrf_imdrf_investigation_narrative,manufacturer_narrative  dvcprbevalimdrf_manufacturer_narrative,record_id  dvcprbevalimdrf_record_id,spr_id  dvcprbevalimdrf_spr_id,user_created  dvcprbevalimdrf_user_created,user_modified  dvcprbevalimdrf_user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eval_imdrf
 WHERE ( record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_device_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eval_imdrf')
  ) where REC_RANK=1 )
  , lsmv_device_prob_eva_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  dvcprbeval_ari_rec_id,date_created  dvcprbeval_date_created,date_modified  dvcprbeval_date_modified,evaluation_type  dvcprbeval_evaluation_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9851' AND CD=CAST(evaluation_type AS VARCHAR(100)) )dvcprbeval_evaluation_type_de_ml , evaluation_value  dvcprbeval_evaluation_value,fk_device_rec_id  dvcprbeval_fk_device_rec_id,record_id  dvcprbeval_record_id,spr_id  dvcprbeval_spr_id,user_created  dvcprbeval_user_created,user_modified  dvcprbeval_user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_device_prob_eva
 WHERE ( record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_device_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eva')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_device_prob_eva_SUBSET.dvcprbeval_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_device_prob_eva_SUBSET.dvcprbeval_USER_MODIFIED USER_MODIFIED,lsmv_device_prob_eva_SUBSET.dvcprbeval_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_device_prob_eva_SUBSET.dvcprbeval_USER_CREATED CREATED_BY,lsmv_device_prob_eva_SUBSET.dvcprbeval_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_user_modified  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_user_created  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_spr_id  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_record_id  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_manufacturer_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_imdrf_investigation_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_imdrf_code  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_identified_acton_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_fk_device_rec_id  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_evaluation_value_imdrf_sf  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_evaluation_value_imdrf  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_evaluation_type_imdrf_sf  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_evaluation_type_imdrf_de_ml  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_evaluation_type_imdrf  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_device_problem_code_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_device_component_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_date_modified  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_date_created  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_corrective_prevt_narrative  ,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_ari_rec_id  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_user_modified  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_user_created  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_spr_id  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_record_id  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_fk_device_rec_id  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_evaluation_value  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_evaluation_type_de_ml  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_evaluation_type  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_date_modified  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_date_created  ,lsmv_device_prob_eva_SUBSET.dvcprbeval_ari_rec_id ,CONCAT( NVL(lsmv_device_prob_eva_SUBSET.dvcprbeval_RECORD_ID,-1),'||',NVL(lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_device_prob_eva_SUBSET  FULL OUTER JOIN lsmv_device_prob_eval_imdrf_SUBSET ON lsmv_device_prob_eva_SUBSET.dvcprbeval_fk_device_rec_id=lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_fk_device_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON   COALESCE(lsmv_device_prob_eva_SUBSET.dvcprbeval_fk_device_rec_id,lsmv_device_prob_eval_imdrf_SUBSET.dvcprbevalimdrf_fk_device_rec_id) = LSMV_COMMON_COLUMN_SUBSET.COMMON_PARENT_KEY WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP)
where target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL   
SET LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_user_modified = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_user_modified,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_user_created = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_user_created,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_spr_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_spr_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_record_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_record_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_manufacturer_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_manufacturer_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_imdrf_investigation_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_imdrf_investigation_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_imdrf_code = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_imdrf_code,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_identified_acton_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_identified_acton_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_fk_device_rec_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_fk_device_rec_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_evaluation_value_imdrf_sf = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_evaluation_value_imdrf_sf,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_evaluation_value_imdrf = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_evaluation_value_imdrf,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_evaluation_type_imdrf_sf = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_evaluation_type_imdrf_sf,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_evaluation_type_imdrf_de_ml = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_evaluation_type_imdrf_de_ml,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_evaluation_type_imdrf = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_evaluation_type_imdrf,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_device_problem_code_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_device_problem_code_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_device_component_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_device_component_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_date_modified = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_date_modified,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_date_created = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_date_created,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_corrective_prevt_narrative = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_corrective_prevt_narrative,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_ari_rec_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_ari_rec_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_user_modified = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_user_modified,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_user_created = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_user_created,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_spr_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_spr_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_record_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_fk_device_rec_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_fk_device_rec_id,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_evaluation_value = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_evaluation_value,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_evaluation_type_de_ml = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_evaluation_type_de_ml,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_evaluation_type = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_evaluation_type,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_date_modified = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_date_modified,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_date_created = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_date_created,LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_ari_rec_id = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_ari_rec_id,
LS_DB_DEVICE_DEVICE_PROB_EVAL.PROCESSING_DT = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.PROCESSING_DT,
LS_DB_DEVICE_DEVICE_PROB_EVAL.receipt_id     =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.receipt_id    ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.case_no        =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.case_no           ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.case_version   =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.case_version      ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.version_no     =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.version_no        ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.user_modified  =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.user_modified     ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.date_modified  =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.date_modified     ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.expiry_date    =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.expiry_date       ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.created_by     =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.created_by        ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.created_dt     =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.created_dt        ,
LS_DB_DEVICE_DEVICE_PROB_EVAL.load_ts        =LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP 
WHERE 	LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.PROCESSING_DT = LS_DB_DEVICE_DEVICE_PROB_EVAL.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DEVICE_DEVICE_PROB_EVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id ,LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP 
ON LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id=LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_record_id
AND LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID 
where LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID  is null AND LS_DB_DEVICE_DEVICE_PROB_EVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id in (select dvcprbeval_record_id from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP )
) TMP where LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DEVICE_DEVICE_PROB_EVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id ,LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP 
    ON LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id=LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_record_id
    AND LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID 
    where LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID  is null AND LS_DB_DEVICE_DEVICE_PROB_EVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id in (select dvcprbeval_record_id from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL
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
integration_id ,dvcprbevalimdrf_user_modified,
dvcprbevalimdrf_user_created,
dvcprbevalimdrf_spr_id,
dvcprbevalimdrf_record_id,
dvcprbevalimdrf_manufacturer_narrative,
dvcprbevalimdrf_imdrf_investigation_narrative,
dvcprbevalimdrf_imdrf_code,
dvcprbevalimdrf_identified_acton_narrative,
dvcprbevalimdrf_fk_device_rec_id,
dvcprbevalimdrf_evaluation_value_imdrf_sf,
dvcprbevalimdrf_evaluation_value_imdrf,
dvcprbevalimdrf_evaluation_type_imdrf_sf,
dvcprbevalimdrf_evaluation_type_imdrf_de_ml,
dvcprbevalimdrf_evaluation_type_imdrf,
dvcprbevalimdrf_device_problem_code_narrative,
dvcprbevalimdrf_device_component_narrative,
dvcprbevalimdrf_date_modified,
dvcprbevalimdrf_date_created,
dvcprbevalimdrf_corrective_prevt_narrative,
dvcprbevalimdrf_ari_rec_id,
dvcprbeval_user_modified,
dvcprbeval_user_created,
dvcprbeval_spr_id,
dvcprbeval_record_id,
dvcprbeval_fk_device_rec_id,
dvcprbeval_evaluation_value,
dvcprbeval_evaluation_type_de_ml,
dvcprbeval_evaluation_type,
dvcprbeval_date_modified,
dvcprbeval_date_created,
dvcprbeval_ari_rec_id)
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
integration_id ,dvcprbevalimdrf_user_modified,
dvcprbevalimdrf_user_created,
dvcprbevalimdrf_spr_id,
dvcprbevalimdrf_record_id,
dvcprbevalimdrf_manufacturer_narrative,
dvcprbevalimdrf_imdrf_investigation_narrative,
dvcprbevalimdrf_imdrf_code,
dvcprbevalimdrf_identified_acton_narrative,
dvcprbevalimdrf_fk_device_rec_id,
dvcprbevalimdrf_evaluation_value_imdrf_sf,
dvcprbevalimdrf_evaluation_value_imdrf,
dvcprbevalimdrf_evaluation_type_imdrf_sf,
dvcprbevalimdrf_evaluation_type_imdrf_de_ml,
dvcprbevalimdrf_evaluation_type_imdrf,
dvcprbevalimdrf_device_problem_code_narrative,
dvcprbevalimdrf_device_component_narrative,
dvcprbevalimdrf_date_modified,
dvcprbevalimdrf_date_created,
dvcprbevalimdrf_corrective_prevt_narrative,
dvcprbevalimdrf_ari_rec_id,
dvcprbeval_user_modified,
dvcprbeval_user_created,
dvcprbeval_spr_id,
dvcprbeval_record_id,
dvcprbeval_fk_device_rec_id,
dvcprbeval_evaluation_value,
dvcprbeval_evaluation_type_de_ml,
dvcprbeval_evaluation_type,
dvcprbeval_date_modified,
dvcprbeval_date_created,
dvcprbeval_ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP 
WHERE 	TO_DATE(LS_DB_DEVICE_DEVICE_PROB_EVAL.PROCESSING_DT) < TO_DATE(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.PROCESSING_DT)
AND LS_DB_DEVICE_DEVICE_PROB_EVAL.INTEGRATION_ID = LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.INTEGRATION_ID
AND nvl(LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_record_id,-1) = nvl(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_record_id,-1)
AND LS_DB_DEVICE_DEVICE_PROB_EVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbeval_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL.dvcprbevalimdrf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbeval_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP.dvcprbevalimdrf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL TGT
WHERE  ( dvcprbeval_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eva') OR dvcprbevalimdrf_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eval_imdrf')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( dvcprbeval_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eva') OR dvcprbevalimdrf_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DEVICE_DEVICE_PROB_EVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_prob_eval_imdrf')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DEVICE_DEVICE_PROB_EVAL_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DEVICE_DEVICE_PROB_EVAL Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DEVICE_DEVICE_PROB_EVAL'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DEVICE_DEVICE_PROB_EVAL not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
