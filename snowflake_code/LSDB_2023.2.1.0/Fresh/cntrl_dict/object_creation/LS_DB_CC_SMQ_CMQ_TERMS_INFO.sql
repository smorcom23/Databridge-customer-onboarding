
-- USE SCHEMA${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_SMQ_CMQ_TERMS_INFO()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;

CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP (
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

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_SMQ_CMQ_TERMS_INFO'),
                'LSDB','Case','LS_DB_CC_SMQ_CMQ_TERMS_INFO',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_CC_SMQ_CMQ_TERMS_INFO' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CC_SMQ_CMQ_TERMS_INFO' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_DELETION_TMP  As select RECORD_ID,'lsmv_cc_smq_cmq_terms_info' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_terms_info WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
(

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_terms_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_TERMS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_terms_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_TERMS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_cc_smq_cmq_terms_info_SUBSET AS 
(
select * from 
    (SELECT  
    cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,date_created  date_created,date_modified  date_modified,dbid  dbid,language_code  language_code,record_id  record_id,smq_code  smq_code,smq_meddra_version  smq_meddra_version,soc_code  soc_code,spr_id  spr_id,term_addition_version  term_addition_version,term_category  term_category,term_code  term_code,term_last_modified_version  term_last_modified_version,term_level  term_level,term_med_version  term_med_version,term_scope  term_scope,term_status  term_status,term_weight  term_weight,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_terms_info
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_terms_info')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_cc_smq_cmq_terms_info_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_cc_smq_cmq_terms_info_SUBSET.USER_CREATED CREATED_BY,lsmv_cc_smq_cmq_terms_info_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_cc_smq_cmq_terms_info_SUBSET.user_modified  ,lsmv_cc_smq_cmq_terms_info_SUBSET.user_created  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_weight  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_status  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_scope  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_med_version  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_level  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_last_modified_version  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_code  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_category  ,lsmv_cc_smq_cmq_terms_info_SUBSET.term_addition_version  ,lsmv_cc_smq_cmq_terms_info_SUBSET.spr_id  ,lsmv_cc_smq_cmq_terms_info_SUBSET.soc_code  ,lsmv_cc_smq_cmq_terms_info_SUBSET.smq_meddra_version  ,lsmv_cc_smq_cmq_terms_info_SUBSET.smq_code  ,lsmv_cc_smq_cmq_terms_info_SUBSET.record_id  ,lsmv_cc_smq_cmq_terms_info_SUBSET.language_code  ,lsmv_cc_smq_cmq_terms_info_SUBSET.dbid  ,lsmv_cc_smq_cmq_terms_info_SUBSET.date_modified  ,lsmv_cc_smq_cmq_terms_info_SUBSET.date_created  ,lsmv_cc_smq_cmq_terms_info_SUBSET.cdc_operation_type  ,lsmv_cc_smq_cmq_terms_info_SUBSET.cdc_operation_time ,CONCAT( NVL(lsmv_cc_smq_cmq_terms_info_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_cc_smq_cmq_terms_info_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP)
where target_table_name='LS_DB_CC_SMQ_CMQ_TERMS_INFO'

; 






UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO   
SET LS_DB_CC_SMQ_CMQ_TERMS_INFO.user_modified = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.user_modified,LS_DB_CC_SMQ_CMQ_TERMS_INFO.user_created = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.user_created,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_weight = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_weight,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_status = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_status,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_scope = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_scope,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_med_version = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_med_version,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_level = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_level,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_last_modified_version = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_last_modified_version,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_code = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_code,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_category = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_category,LS_DB_CC_SMQ_CMQ_TERMS_INFO.term_addition_version = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.term_addition_version,LS_DB_CC_SMQ_CMQ_TERMS_INFO.spr_id = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.spr_id,LS_DB_CC_SMQ_CMQ_TERMS_INFO.soc_code = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.soc_code,LS_DB_CC_SMQ_CMQ_TERMS_INFO.smq_meddra_version = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.smq_meddra_version,LS_DB_CC_SMQ_CMQ_TERMS_INFO.smq_code = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.smq_code,LS_DB_CC_SMQ_CMQ_TERMS_INFO.record_id = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.record_id,LS_DB_CC_SMQ_CMQ_TERMS_INFO.language_code = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.language_code,LS_DB_CC_SMQ_CMQ_TERMS_INFO.dbid = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.dbid,LS_DB_CC_SMQ_CMQ_TERMS_INFO.date_modified = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.date_modified,LS_DB_CC_SMQ_CMQ_TERMS_INFO.date_created = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.date_created,LS_DB_CC_SMQ_CMQ_TERMS_INFO.cdc_operation_type = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.cdc_operation_type,LS_DB_CC_SMQ_CMQ_TERMS_INFO.cdc_operation_time = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.cdc_operation_time,
LS_DB_CC_SMQ_CMQ_TERMS_INFO.PROCESSING_DT = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.PROCESSING_DT ,
LS_DB_CC_SMQ_CMQ_TERMS_INFO.expiry_date    =LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.expiry_date       ,
LS_DB_CC_SMQ_CMQ_TERMS_INFO.created_by     =LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.created_by        ,
LS_DB_CC_SMQ_CMQ_TERMS_INFO.created_dt     =LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.created_dt        ,
LS_DB_CC_SMQ_CMQ_TERMS_INFO.load_ts        =LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.load_ts         
FROM    ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP 
WHERE LS_DB_CC_SMQ_CMQ_TERMS_INFO.INTEGRATION_ID = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.PROCESSING_DT = LS_DB_CC_SMQ_CMQ_TERMS_INFO.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
term_weight,
term_status,
term_scope,
term_med_version,
term_level,
term_last_modified_version,
term_code,
term_category,
term_addition_version,
spr_id,
soc_code,
smq_meddra_version,
smq_code,
record_id,
language_code,
dbid,
date_modified,
date_created,
cdc_operation_type,
cdc_operation_time)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
term_weight,
term_status,
term_scope,
term_med_version,
term_level,
term_last_modified_version,
term_code,
term_category,
term_addition_version,
spr_id,
soc_code,
smq_meddra_version,
smq_code,
record_id,
language_code,
dbid,
date_modified,
date_created,
cdc_operation_type,
cdc_operation_time
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP 
WHERE TO_DATE(LS_DB_CC_SMQ_CMQ_TERMS_INFO.PROCESSING_DT) < TO_DATE(LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.PROCESSING_DT)
AND LS_DB_CC_SMQ_CMQ_TERMS_INFO.INTEGRATION_ID = LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP.INTEGRATION_ID
AND LS_DB_CC_SMQ_CMQ_TERMS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_terms_info')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_terms_info')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_TERMS_INFO_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CC_SMQ_CMQ_TERMS_INFO'
;

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_CC_SMQ_CMQ_TERMS_INFO Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_SMQ_CMQ_TERMS_INFO'),
                'LSDB','Case','LS_DB_CC_SMQ_CMQ_TERMS_INFO',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_CC_SMQ_CMQ_TERMS_INFO not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
