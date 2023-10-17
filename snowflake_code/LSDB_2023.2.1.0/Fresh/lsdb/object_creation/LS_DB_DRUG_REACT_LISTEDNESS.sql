
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_LISTEDNESS()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_LISTEDNESS'),
	'LSDB','Case','LS_DB_DRUG_REACT_LISTEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_REACT_LISTEDNESS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_REACT_LISTEDNESS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_LISTEDNESS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_LISTEDNESS_DELETION_TMP  As select RECORD_ID,'lsmv_drug_react_listedness' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('7077','7077','7077','7077')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_LISTEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_LISTEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_drug_react_listedness_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,case_level_expectedness  case_level_expectedness,conditionally_labled  conditionally_labled,country  country,date_created  date_created,date_modified  date_modified,fk_adrr_rec_id  fk_adrr_rec_id,fk_dev_heal_rec_id  fk_dev_heal_rec_id,fl_susar  fl_susar,is_added_from_krjp  is_added_from_krjp,is_listed  is_listed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(is_listed AS VARCHAR(100)) )is_listed_de_ml , is_mannual  is_mannual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(is_mannual AS VARCHAR(100)) )is_mannual_de_ml , is_override_listedness  is_override_listedness,labeled_pro_rec_id  labeled_pro_rec_id,labeling_version_manual  labeling_version_manual,labelling_version  labelling_version,reason_for_listedness  reason_for_listedness,record_id  record_id,smq_cmq_pt_code  smq_cmq_pt_code,spr_id  spr_id,susar  susar,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(susar AS VARCHAR(100)) )susar_de_ml , susar_labelling_type  susar_labelling_type,susar_manual  susar_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(susar_manual AS VARCHAR(100)) )susar_manual_de_ml , trade_record_id  trade_record_id,tradename  tradename,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_LISTEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_react_listedness_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_react_listedness_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_react_listedness_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_react_listedness_SUBSET.user_modified  ,lsmv_drug_react_listedness_SUBSET.user_created  ,lsmv_drug_react_listedness_SUBSET.tradename  ,lsmv_drug_react_listedness_SUBSET.trade_record_id  ,lsmv_drug_react_listedness_SUBSET.susar_manual_de_ml  ,lsmv_drug_react_listedness_SUBSET.susar_manual  ,lsmv_drug_react_listedness_SUBSET.susar_labelling_type  ,lsmv_drug_react_listedness_SUBSET.susar_de_ml  ,lsmv_drug_react_listedness_SUBSET.susar  ,lsmv_drug_react_listedness_SUBSET.spr_id  ,lsmv_drug_react_listedness_SUBSET.smq_cmq_pt_code  ,lsmv_drug_react_listedness_SUBSET.record_id  ,lsmv_drug_react_listedness_SUBSET.reason_for_listedness  ,lsmv_drug_react_listedness_SUBSET.labelling_version  ,lsmv_drug_react_listedness_SUBSET.labeling_version_manual  ,lsmv_drug_react_listedness_SUBSET.labeled_pro_rec_id  ,lsmv_drug_react_listedness_SUBSET.is_override_listedness  ,lsmv_drug_react_listedness_SUBSET.is_mannual_de_ml  ,lsmv_drug_react_listedness_SUBSET.is_mannual  ,lsmv_drug_react_listedness_SUBSET.is_listed_de_ml  ,lsmv_drug_react_listedness_SUBSET.is_listed  ,lsmv_drug_react_listedness_SUBSET.is_added_from_krjp  ,lsmv_drug_react_listedness_SUBSET.fl_susar  ,lsmv_drug_react_listedness_SUBSET.fk_dev_heal_rec_id  ,lsmv_drug_react_listedness_SUBSET.fk_adrr_rec_id  ,lsmv_drug_react_listedness_SUBSET.date_modified  ,lsmv_drug_react_listedness_SUBSET.date_created  ,lsmv_drug_react_listedness_SUBSET.country  ,lsmv_drug_react_listedness_SUBSET.conditionally_labled  ,lsmv_drug_react_listedness_SUBSET.case_level_expectedness  ,lsmv_drug_react_listedness_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_drug_react_listedness_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_react_listedness_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_react_listedness_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP)
where target_table_name='LS_DB_DRUG_REACT_LISTEDNESS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS   
SET LS_DB_DRUG_REACT_LISTEDNESS.user_modified = LS_DB_DRUG_REACT_LISTEDNESS_TMP.user_modified,LS_DB_DRUG_REACT_LISTEDNESS.user_created = LS_DB_DRUG_REACT_LISTEDNESS_TMP.user_created,LS_DB_DRUG_REACT_LISTEDNESS.tradename = LS_DB_DRUG_REACT_LISTEDNESS_TMP.tradename,LS_DB_DRUG_REACT_LISTEDNESS.trade_record_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.trade_record_id,LS_DB_DRUG_REACT_LISTEDNESS.susar_manual_de_ml = LS_DB_DRUG_REACT_LISTEDNESS_TMP.susar_manual_de_ml,LS_DB_DRUG_REACT_LISTEDNESS.susar_manual = LS_DB_DRUG_REACT_LISTEDNESS_TMP.susar_manual,LS_DB_DRUG_REACT_LISTEDNESS.susar_labelling_type = LS_DB_DRUG_REACT_LISTEDNESS_TMP.susar_labelling_type,LS_DB_DRUG_REACT_LISTEDNESS.susar_de_ml = LS_DB_DRUG_REACT_LISTEDNESS_TMP.susar_de_ml,LS_DB_DRUG_REACT_LISTEDNESS.susar = LS_DB_DRUG_REACT_LISTEDNESS_TMP.susar,LS_DB_DRUG_REACT_LISTEDNESS.spr_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.spr_id,LS_DB_DRUG_REACT_LISTEDNESS.smq_cmq_pt_code = LS_DB_DRUG_REACT_LISTEDNESS_TMP.smq_cmq_pt_code,LS_DB_DRUG_REACT_LISTEDNESS.record_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.record_id,LS_DB_DRUG_REACT_LISTEDNESS.reason_for_listedness = LS_DB_DRUG_REACT_LISTEDNESS_TMP.reason_for_listedness,LS_DB_DRUG_REACT_LISTEDNESS.labelling_version = LS_DB_DRUG_REACT_LISTEDNESS_TMP.labelling_version,LS_DB_DRUG_REACT_LISTEDNESS.labeling_version_manual = LS_DB_DRUG_REACT_LISTEDNESS_TMP.labeling_version_manual,LS_DB_DRUG_REACT_LISTEDNESS.labeled_pro_rec_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.labeled_pro_rec_id,LS_DB_DRUG_REACT_LISTEDNESS.is_override_listedness = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_override_listedness,LS_DB_DRUG_REACT_LISTEDNESS.is_mannual_de_ml = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_mannual_de_ml,LS_DB_DRUG_REACT_LISTEDNESS.is_mannual = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_mannual,LS_DB_DRUG_REACT_LISTEDNESS.is_listed_de_ml = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_listed_de_ml,LS_DB_DRUG_REACT_LISTEDNESS.is_listed = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_listed,LS_DB_DRUG_REACT_LISTEDNESS.is_added_from_krjp = LS_DB_DRUG_REACT_LISTEDNESS_TMP.is_added_from_krjp,LS_DB_DRUG_REACT_LISTEDNESS.fl_susar = LS_DB_DRUG_REACT_LISTEDNESS_TMP.fl_susar,LS_DB_DRUG_REACT_LISTEDNESS.fk_dev_heal_rec_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.fk_dev_heal_rec_id,LS_DB_DRUG_REACT_LISTEDNESS.fk_adrr_rec_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.fk_adrr_rec_id,LS_DB_DRUG_REACT_LISTEDNESS.date_modified = LS_DB_DRUG_REACT_LISTEDNESS_TMP.date_modified,LS_DB_DRUG_REACT_LISTEDNESS.date_created = LS_DB_DRUG_REACT_LISTEDNESS_TMP.date_created,LS_DB_DRUG_REACT_LISTEDNESS.country = LS_DB_DRUG_REACT_LISTEDNESS_TMP.country,LS_DB_DRUG_REACT_LISTEDNESS.conditionally_labled = LS_DB_DRUG_REACT_LISTEDNESS_TMP.conditionally_labled,LS_DB_DRUG_REACT_LISTEDNESS.case_level_expectedness = LS_DB_DRUG_REACT_LISTEDNESS_TMP.case_level_expectedness,LS_DB_DRUG_REACT_LISTEDNESS.ari_rec_id = LS_DB_DRUG_REACT_LISTEDNESS_TMP.ari_rec_id,
LS_DB_DRUG_REACT_LISTEDNESS.PROCESSING_DT = LS_DB_DRUG_REACT_LISTEDNESS_TMP.PROCESSING_DT ,
LS_DB_DRUG_REACT_LISTEDNESS.receipt_id     =LS_DB_DRUG_REACT_LISTEDNESS_TMP.receipt_id        ,
LS_DB_DRUG_REACT_LISTEDNESS.case_no        =LS_DB_DRUG_REACT_LISTEDNESS_TMP.case_no           ,
LS_DB_DRUG_REACT_LISTEDNESS.case_version   =LS_DB_DRUG_REACT_LISTEDNESS_TMP.case_version      ,
LS_DB_DRUG_REACT_LISTEDNESS.version_no     =LS_DB_DRUG_REACT_LISTEDNESS_TMP.version_no        ,
LS_DB_DRUG_REACT_LISTEDNESS.expiry_date    =LS_DB_DRUG_REACT_LISTEDNESS_TMP.expiry_date       ,
LS_DB_DRUG_REACT_LISTEDNESS.load_ts        =LS_DB_DRUG_REACT_LISTEDNESS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP 
WHERE 	LS_DB_DRUG_REACT_LISTEDNESS.INTEGRATION_ID = LS_DB_DRUG_REACT_LISTEDNESS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_REACT_LISTEDNESS_TMP.PROCESSING_DT = LS_DB_DRUG_REACT_LISTEDNESS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS
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
tradename,
trade_record_id,
susar_manual_de_ml,
susar_manual,
susar_labelling_type,
susar_de_ml,
susar,
spr_id,
smq_cmq_pt_code,
record_id,
reason_for_listedness,
labelling_version,
labeling_version_manual,
labeled_pro_rec_id,
is_override_listedness,
is_mannual_de_ml,
is_mannual,
is_listed_de_ml,
is_listed,
is_added_from_krjp,
fl_susar,
fk_dev_heal_rec_id,
fk_adrr_rec_id,
date_modified,
date_created,
country,
conditionally_labled,
case_level_expectedness,
ari_rec_id)
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
tradename,
trade_record_id,
susar_manual_de_ml,
susar_manual,
susar_labelling_type,
susar_de_ml,
susar,
spr_id,
smq_cmq_pt_code,
record_id,
reason_for_listedness,
labelling_version,
labeling_version_manual,
labeled_pro_rec_id,
is_override_listedness,
is_mannual_de_ml,
is_mannual,
is_listed_de_ml,
is_listed,
is_added_from_krjp,
fl_susar,
fk_dev_heal_rec_id,
fk_adrr_rec_id,
date_modified,
date_created,
country,
conditionally_labled,
case_level_expectedness,
ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_REACT_LISTEDNESS.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_REACT_LISTEDNESS_TMP.PROCESSING_DT)
AND LS_DB_DRUG_REACT_LISTEDNESS.INTEGRATION_ID = LS_DB_DRUG_REACT_LISTEDNESS_TMP.INTEGRATION_ID
AND LS_DB_DRUG_REACT_LISTEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_LISTEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_LISTEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_LISTEDNESS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_REACT_LISTEDNESS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_REACT_LISTEDNESS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_LISTEDNESS'),
	'LSDB','Case','LS_DB_DRUG_REACT_LISTEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_REACT_LISTEDNESS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
