
--USE SCHEMA ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_SMQ_CMQ_LIST()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_SMQ_CMQ_LIST'),
	'LSRA','Case','LS_DB_CC_SMQ_CMQ_LIST',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CC_SMQ_CMQ_LIST' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CC_SMQ_CMQ_LIST'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_CC_SMQ_CMQ_LIST'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_LIST_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_LIST_DELETION_TMP  As select RECORD_ID,'lsmv_cc_smq_cmq_list' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_list WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_list WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_LIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_LIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_list WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_LIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CC_SMQ_CMQ_LIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_cc_smq_cmq_list_SUBSET AS 
(
select * from 
    (SELECT  
    approval_state  approval_state,date_created  date_created,date_modified  date_modified,dbid  dbid,language_code  language_code,list_source  list_source,meddra_version  meddra_version,portfolio_flag  portfolio_flag,private_query_flag  private_query_flag,record_id  record_id,smq_algorithm  smq_algorithm,smq_code  smq_code,smq_description  smq_description,smq_description_j  smq_description_j,smq_level  smq_level,smq_name  smq_name,smq_name_j  smq_name_j,smq_note  smq_note,smq_source  smq_source,spr_id  spr_id,status  status,term_from  term_from,user_created  user_created,user_id  user_id,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_smq_cmq_list
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_LIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_list')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_cc_smq_cmq_list_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_cc_smq_cmq_list_SUBSET.USER_CREATED CREATED_BY,lsmv_cc_smq_cmq_list_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_cc_smq_cmq_list_SUBSET.user_modified  ,lsmv_cc_smq_cmq_list_SUBSET.user_id  ,lsmv_cc_smq_cmq_list_SUBSET.user_created  ,lsmv_cc_smq_cmq_list_SUBSET.term_from  ,lsmv_cc_smq_cmq_list_SUBSET.status  ,lsmv_cc_smq_cmq_list_SUBSET.spr_id  ,lsmv_cc_smq_cmq_list_SUBSET.smq_source  ,lsmv_cc_smq_cmq_list_SUBSET.smq_note  ,lsmv_cc_smq_cmq_list_SUBSET.smq_name_j  ,lsmv_cc_smq_cmq_list_SUBSET.smq_name  ,lsmv_cc_smq_cmq_list_SUBSET.smq_level  ,lsmv_cc_smq_cmq_list_SUBSET.smq_description_j  ,lsmv_cc_smq_cmq_list_SUBSET.smq_description  ,lsmv_cc_smq_cmq_list_SUBSET.smq_code  ,lsmv_cc_smq_cmq_list_SUBSET.smq_algorithm  ,lsmv_cc_smq_cmq_list_SUBSET.record_id  ,lsmv_cc_smq_cmq_list_SUBSET.private_query_flag  ,lsmv_cc_smq_cmq_list_SUBSET.portfolio_flag  ,lsmv_cc_smq_cmq_list_SUBSET.meddra_version  ,lsmv_cc_smq_cmq_list_SUBSET.list_source  ,lsmv_cc_smq_cmq_list_SUBSET.language_code  ,lsmv_cc_smq_cmq_list_SUBSET.dbid  ,lsmv_cc_smq_cmq_list_SUBSET.date_modified  ,lsmv_cc_smq_cmq_list_SUBSET.date_created  ,lsmv_cc_smq_cmq_list_SUBSET.approval_state ,CONCAT( NVL(lsmv_cc_smq_cmq_list_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_cc_smq_cmq_list_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP)
where target_table_name='LS_DB_CC_SMQ_CMQ_LIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_SMQ_CMQ_LIST'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST   
SET LS_DB_CC_SMQ_CMQ_LIST.user_modified = LS_DB_CC_SMQ_CMQ_LIST_TMP.user_modified,LS_DB_CC_SMQ_CMQ_LIST.user_id = LS_DB_CC_SMQ_CMQ_LIST_TMP.user_id,LS_DB_CC_SMQ_CMQ_LIST.user_created = LS_DB_CC_SMQ_CMQ_LIST_TMP.user_created,LS_DB_CC_SMQ_CMQ_LIST.term_from = LS_DB_CC_SMQ_CMQ_LIST_TMP.term_from,LS_DB_CC_SMQ_CMQ_LIST.status = LS_DB_CC_SMQ_CMQ_LIST_TMP.status,LS_DB_CC_SMQ_CMQ_LIST.spr_id = LS_DB_CC_SMQ_CMQ_LIST_TMP.spr_id,LS_DB_CC_SMQ_CMQ_LIST.smq_source = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_source,LS_DB_CC_SMQ_CMQ_LIST.smq_note = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_note,LS_DB_CC_SMQ_CMQ_LIST.smq_name_j = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_name_j,LS_DB_CC_SMQ_CMQ_LIST.smq_name = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_name,LS_DB_CC_SMQ_CMQ_LIST.smq_level = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_level,LS_DB_CC_SMQ_CMQ_LIST.smq_description_j = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_description_j,LS_DB_CC_SMQ_CMQ_LIST.smq_description = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_description,LS_DB_CC_SMQ_CMQ_LIST.smq_code = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_code,LS_DB_CC_SMQ_CMQ_LIST.smq_algorithm = LS_DB_CC_SMQ_CMQ_LIST_TMP.smq_algorithm,LS_DB_CC_SMQ_CMQ_LIST.record_id = LS_DB_CC_SMQ_CMQ_LIST_TMP.record_id,LS_DB_CC_SMQ_CMQ_LIST.private_query_flag = LS_DB_CC_SMQ_CMQ_LIST_TMP.private_query_flag,LS_DB_CC_SMQ_CMQ_LIST.portfolio_flag = LS_DB_CC_SMQ_CMQ_LIST_TMP.portfolio_flag,LS_DB_CC_SMQ_CMQ_LIST.meddra_version = LS_DB_CC_SMQ_CMQ_LIST_TMP.meddra_version,LS_DB_CC_SMQ_CMQ_LIST.list_source = LS_DB_CC_SMQ_CMQ_LIST_TMP.list_source,LS_DB_CC_SMQ_CMQ_LIST.language_code = LS_DB_CC_SMQ_CMQ_LIST_TMP.language_code,LS_DB_CC_SMQ_CMQ_LIST.dbid = LS_DB_CC_SMQ_CMQ_LIST_TMP.dbid,LS_DB_CC_SMQ_CMQ_LIST.date_modified = LS_DB_CC_SMQ_CMQ_LIST_TMP.date_modified,LS_DB_CC_SMQ_CMQ_LIST.date_created = LS_DB_CC_SMQ_CMQ_LIST_TMP.date_created,LS_DB_CC_SMQ_CMQ_LIST.approval_state = LS_DB_CC_SMQ_CMQ_LIST_TMP.approval_state,
LS_DB_CC_SMQ_CMQ_LIST.PROCESSING_DT = LS_DB_CC_SMQ_CMQ_LIST_TMP.PROCESSING_DT ,
LS_DB_CC_SMQ_CMQ_LIST.expiry_date    =LS_DB_CC_SMQ_CMQ_LIST_TMP.expiry_date       ,
LS_DB_CC_SMQ_CMQ_LIST.created_by     =LS_DB_CC_SMQ_CMQ_LIST_TMP.created_by        ,
LS_DB_CC_SMQ_CMQ_LIST.created_dt     =LS_DB_CC_SMQ_CMQ_LIST_TMP.created_dt        ,
LS_DB_CC_SMQ_CMQ_LIST.load_ts        =LS_DB_CC_SMQ_CMQ_LIST_TMP.load_ts         
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP 
WHERE 	LS_DB_CC_SMQ_CMQ_LIST.INTEGRATION_ID = LS_DB_CC_SMQ_CMQ_LIST_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CC_SMQ_CMQ_LIST_TMP.PROCESSING_DT = LS_DB_CC_SMQ_CMQ_LIST.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_id,
user_created,
term_from,
status,
spr_id,
smq_source,
smq_note,
smq_name_j,
smq_name,
smq_level,
smq_description_j,
smq_description,
smq_code,
smq_algorithm,
record_id,
private_query_flag,
portfolio_flag,
meddra_version,
list_source,
language_code,
dbid,
date_modified,
date_created,
approval_state)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_id,
user_created,
term_from,
status,
spr_id,
smq_source,
smq_note,
smq_name_j,
smq_name,
smq_level,
smq_description_j,
smq_description,
smq_code,
smq_algorithm,
record_id,
private_query_flag,
portfolio_flag,
meddra_version,
list_source,
language_code,
dbid,
date_modified,
date_created,
approval_state
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP 
WHERE 	TO_DATE(LS_DB_CC_SMQ_CMQ_LIST.PROCESSING_DT) < TO_DATE(LS_DB_CC_SMQ_CMQ_LIST_TMP.PROCESSING_DT)
AND LS_DB_CC_SMQ_CMQ_LIST.INTEGRATION_ID = LS_DB_CC_SMQ_CMQ_LIST_TMP.INTEGRATION_ID
AND LS_DB_CC_SMQ_CMQ_LIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_LIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_list')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_SMQ_CMQ_LIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_smq_cmq_list')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CC_SMQ_CMQ_LIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_SMQ_CMQ_LIST'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CC_SMQ_CMQ_LIST Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CC_SMQ_CMQ_LIST'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CC_SMQ_CMQ_LIST not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
