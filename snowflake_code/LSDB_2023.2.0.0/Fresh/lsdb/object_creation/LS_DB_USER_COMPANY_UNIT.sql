
-- USE SCHEMA${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_USER_COMPANY_UNIT()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_USER_COMPANY_UNIT'),
                'LSDB','Case','LS_DB_USER_COMPANY_UNIT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_USER_COMPANY_UNIT' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_USER_COMPANY_UNIT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_COMPANY_UNIT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_COMPANY_UNIT_DELETION_TMP  As select RECORD_ID,'lsmv_user_company_unit' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_user_company_unit WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_user_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_USER_COMPANY_UNIT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_user_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_USER_COMPANY_UNIT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_user_company_unit_SUBSET AS 
(
select * from 
    (SELECT  
    cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,company_unit_id  company_unit_id,company_unit_name  company_unit_name,company_unit_record_id  company_unit_record_id,company_unit_type  company_unit_type,date_created  date_created,date_modified  date_modified,default_cu  default_cu,fk_user_record_id  fk_user_record_id,read_only_access  read_only_access,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_user_company_unit
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_user_company_unit')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_user_company_unit_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_user_company_unit_SUBSET.USER_CREATED CREATED_BY,lsmv_user_company_unit_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_user_company_unit_SUBSET.user_modified  ,lsmv_user_company_unit_SUBSET.user_created  ,lsmv_user_company_unit_SUBSET.spr_id  ,lsmv_user_company_unit_SUBSET.record_id  ,lsmv_user_company_unit_SUBSET.read_only_access  ,lsmv_user_company_unit_SUBSET.fk_user_record_id  ,lsmv_user_company_unit_SUBSET.default_cu  ,lsmv_user_company_unit_SUBSET.date_modified  ,lsmv_user_company_unit_SUBSET.date_created  ,lsmv_user_company_unit_SUBSET.company_unit_type  ,lsmv_user_company_unit_SUBSET.company_unit_record_id  ,lsmv_user_company_unit_SUBSET.company_unit_name  ,lsmv_user_company_unit_SUBSET.company_unit_id  ,lsmv_user_company_unit_SUBSET.cdc_operation_type  ,lsmv_user_company_unit_SUBSET.cdc_operation_time ,CONCAT( NVL(lsmv_user_company_unit_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_user_company_unit_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP)
where target_table_name='LS_DB_USER_COMPANY_UNIT'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT   
SET LS_DB_USER_COMPANY_UNIT.user_modified = LS_DB_USER_COMPANY_UNIT_TMP.user_modified,LS_DB_USER_COMPANY_UNIT.user_created = LS_DB_USER_COMPANY_UNIT_TMP.user_created,LS_DB_USER_COMPANY_UNIT.spr_id = LS_DB_USER_COMPANY_UNIT_TMP.spr_id,LS_DB_USER_COMPANY_UNIT.record_id = LS_DB_USER_COMPANY_UNIT_TMP.record_id,LS_DB_USER_COMPANY_UNIT.read_only_access = LS_DB_USER_COMPANY_UNIT_TMP.read_only_access,LS_DB_USER_COMPANY_UNIT.fk_user_record_id = LS_DB_USER_COMPANY_UNIT_TMP.fk_user_record_id,LS_DB_USER_COMPANY_UNIT.default_cu = LS_DB_USER_COMPANY_UNIT_TMP.default_cu,LS_DB_USER_COMPANY_UNIT.date_modified = LS_DB_USER_COMPANY_UNIT_TMP.date_modified,LS_DB_USER_COMPANY_UNIT.date_created = LS_DB_USER_COMPANY_UNIT_TMP.date_created,LS_DB_USER_COMPANY_UNIT.company_unit_type = LS_DB_USER_COMPANY_UNIT_TMP.company_unit_type,LS_DB_USER_COMPANY_UNIT.company_unit_record_id = LS_DB_USER_COMPANY_UNIT_TMP.company_unit_record_id,LS_DB_USER_COMPANY_UNIT.company_unit_name = LS_DB_USER_COMPANY_UNIT_TMP.company_unit_name,LS_DB_USER_COMPANY_UNIT.company_unit_id = LS_DB_USER_COMPANY_UNIT_TMP.company_unit_id,LS_DB_USER_COMPANY_UNIT.cdc_operation_type = LS_DB_USER_COMPANY_UNIT_TMP.cdc_operation_type,LS_DB_USER_COMPANY_UNIT.cdc_operation_time = LS_DB_USER_COMPANY_UNIT_TMP.cdc_operation_time,
LS_DB_USER_COMPANY_UNIT.PROCESSING_DT = LS_DB_USER_COMPANY_UNIT_TMP.PROCESSING_DT ,
LS_DB_USER_COMPANY_UNIT.expiry_date    =LS_DB_USER_COMPANY_UNIT_TMP.expiry_date       ,
LS_DB_USER_COMPANY_UNIT.created_by     =LS_DB_USER_COMPANY_UNIT_TMP.created_by        ,
LS_DB_USER_COMPANY_UNIT.created_dt     =LS_DB_USER_COMPANY_UNIT_TMP.created_dt        ,
LS_DB_USER_COMPANY_UNIT.load_ts        =LS_DB_USER_COMPANY_UNIT_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP 
WHERE LS_DB_USER_COMPANY_UNIT.INTEGRATION_ID = LS_DB_USER_COMPANY_UNIT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_USER_COMPANY_UNIT_TMP.PROCESSING_DT = LS_DB_USER_COMPANY_UNIT.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
read_only_access,
fk_user_record_id,
default_cu,
date_modified,
date_created,
company_unit_type,
company_unit_record_id,
company_unit_name,
company_unit_id,
cdc_operation_type,
cdc_operation_time)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
read_only_access,
fk_user_record_id,
default_cu,
date_modified,
date_created,
company_unit_type,
company_unit_record_id,
company_unit_name,
company_unit_id,
cdc_operation_type,
cdc_operation_time
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP 
WHERE TO_DATE(LS_DB_USER_COMPANY_UNIT.PROCESSING_DT) < TO_DATE(LS_DB_USER_COMPANY_UNIT_TMP.PROCESSING_DT)
AND LS_DB_USER_COMPANY_UNIT.INTEGRATION_ID = LS_DB_USER_COMPANY_UNIT_TMP.INTEGRATION_ID
AND LS_DB_USER_COMPANY_UNIT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_user_company_unit')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_user_company_unit')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_COMPANY_UNIT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_USER_COMPANY_UNIT'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_USER_COMPANY_UNIT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_USER_COMPANY_UNIT'),
                'LSDB','Case','LS_DB_USER_COMPANY_UNIT',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_USER_COMPANY_UNIT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
