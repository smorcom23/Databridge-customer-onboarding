
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_HEALTH_RELATEDNESS()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE_HEALTH_RELATEDNESS'),
	'LSDB','Case','LS_DB_DEVICE_HEALTH_RELATEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DEVICE_HEALTH_RELATEDNESS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DEVICE_HEALTH_RELATEDNESS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_HEALTH_RELATEDNESS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_HEALTH_RELATEDNESS_DELETION_TMP  As select RECORD_ID,'lsmv_device_health_relatedness' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_health_relatedness WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_health_relatedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_HEALTH_RELATEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_health_relatedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE_HEALTH_RELATEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_device_health_relatedness_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,company_causality  company_causality,date_created  date_created,date_modified  date_modified,device_company_causality  device_company_causality,device_health_type  device_health_type,device_problem_ref_id  device_problem_ref_id,device_reporter_causality  device_reporter_causality,fk_drug  fk_drug,health_damage_ref_id  health_damage_ref_id,record_id  record_id,reporter_causality  reporter_causality,spr_id  spr_id,user_created  user_created,user_modified  user_modified,uuid  uuid,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_device_health_relatedness
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_HEALTH_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_health_relatedness')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_device_health_relatedness_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_device_health_relatedness_SUBSET.USER_CREATED CREATED_BY,lsmv_device_health_relatedness_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_device_health_relatedness_SUBSET.uuid  ,lsmv_device_health_relatedness_SUBSET.user_modified  ,lsmv_device_health_relatedness_SUBSET.user_created  ,lsmv_device_health_relatedness_SUBSET.spr_id  ,lsmv_device_health_relatedness_SUBSET.reporter_causality  ,lsmv_device_health_relatedness_SUBSET.record_id  ,lsmv_device_health_relatedness_SUBSET.health_damage_ref_id  ,lsmv_device_health_relatedness_SUBSET.fk_drug  ,lsmv_device_health_relatedness_SUBSET.device_reporter_causality  ,lsmv_device_health_relatedness_SUBSET.device_problem_ref_id  ,lsmv_device_health_relatedness_SUBSET.device_health_type  ,lsmv_device_health_relatedness_SUBSET.device_company_causality  ,lsmv_device_health_relatedness_SUBSET.date_modified  ,lsmv_device_health_relatedness_SUBSET.date_created  ,lsmv_device_health_relatedness_SUBSET.company_causality  ,lsmv_device_health_relatedness_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_device_health_relatedness_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_device_health_relatedness_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_device_health_relatedness_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP)
where target_table_name='LS_DB_DEVICE_HEALTH_RELATEDNESS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS   
SET LS_DB_DEVICE_HEALTH_RELATEDNESS.uuid = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.uuid,LS_DB_DEVICE_HEALTH_RELATEDNESS.user_modified = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.user_modified,LS_DB_DEVICE_HEALTH_RELATEDNESS.user_created = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.user_created,LS_DB_DEVICE_HEALTH_RELATEDNESS.spr_id = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.spr_id,LS_DB_DEVICE_HEALTH_RELATEDNESS.reporter_causality = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.reporter_causality,LS_DB_DEVICE_HEALTH_RELATEDNESS.record_id = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.record_id,LS_DB_DEVICE_HEALTH_RELATEDNESS.health_damage_ref_id = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.health_damage_ref_id,LS_DB_DEVICE_HEALTH_RELATEDNESS.fk_drug = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.fk_drug,LS_DB_DEVICE_HEALTH_RELATEDNESS.device_reporter_causality = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.device_reporter_causality,LS_DB_DEVICE_HEALTH_RELATEDNESS.device_problem_ref_id = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.device_problem_ref_id,LS_DB_DEVICE_HEALTH_RELATEDNESS.device_health_type = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.device_health_type,LS_DB_DEVICE_HEALTH_RELATEDNESS.device_company_causality = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.device_company_causality,LS_DB_DEVICE_HEALTH_RELATEDNESS.date_modified = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.date_modified,LS_DB_DEVICE_HEALTH_RELATEDNESS.date_created = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.date_created,LS_DB_DEVICE_HEALTH_RELATEDNESS.company_causality = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.company_causality,LS_DB_DEVICE_HEALTH_RELATEDNESS.ari_rec_id = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.ari_rec_id,
LS_DB_DEVICE_HEALTH_RELATEDNESS.PROCESSING_DT = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.PROCESSING_DT ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.receipt_id     =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.receipt_id        ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.case_no        =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.case_no           ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.case_version   =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.case_version      ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.version_no     =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.version_no        ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.expiry_date    =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.expiry_date       ,
LS_DB_DEVICE_HEALTH_RELATEDNESS.load_ts        =LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP 
WHERE 	LS_DB_DEVICE_HEALTH_RELATEDNESS.INTEGRATION_ID = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.PROCESSING_DT = LS_DB_DEVICE_HEALTH_RELATEDNESS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uuid,
user_modified,
user_created,
spr_id,
reporter_causality,
record_id,
health_damage_ref_id,
fk_drug,
device_reporter_causality,
device_problem_ref_id,
device_health_type,
device_company_causality,
date_modified,
date_created,
company_causality,
ari_rec_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uuid,
user_modified,
user_created,
spr_id,
reporter_causality,
record_id,
health_damage_ref_id,
fk_drug,
device_reporter_causality,
device_problem_ref_id,
device_health_type,
device_company_causality,
date_modified,
date_created,
company_causality,
ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP 
WHERE 	TO_DATE(LS_DB_DEVICE_HEALTH_RELATEDNESS.PROCESSING_DT) < TO_DATE(LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.PROCESSING_DT)
AND LS_DB_DEVICE_HEALTH_RELATEDNESS.INTEGRATION_ID = LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP.INTEGRATION_ID
AND LS_DB_DEVICE_HEALTH_RELATEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_HEALTH_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_health_relatedness')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_HEALTH_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_health_relatedness')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_HEALTH_RELATEDNESS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DEVICE_HEALTH_RELATEDNESS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DEVICE_HEALTH_RELATEDNESS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE_HEALTH_RELATEDNESS'),
	'LSDB','Case','LS_DB_DEVICE_HEALTH_RELATEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DEVICE_HEALTH_RELATEDNESS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
