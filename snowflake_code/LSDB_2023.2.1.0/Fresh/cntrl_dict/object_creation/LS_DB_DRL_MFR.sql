
-- USE SCHEMA ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_MFR()
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
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_MFR'),
	'LSRA','Case','LS_DB_DRL_MFR',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRL_MFR' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRL_MFR'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DRL_MFR'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_DRL_MFR_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_DRL_MFR_DELETION_TMP  As select RECORD_ID,'lsmv_drl_mfr' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_drl_mfr WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_drl_mfr WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_MFR' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_MFR' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_drl_mfr WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_MFR' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_MFR' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_drl_mfr_SUBSET AS 
(
select * from 
    (SELECT  
    country  country,date_created  date_created,date_modified  date_modified,dictionary_version  dictionary_version,mfr_code  mfr_code,name  name,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_drl_mfr
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_DRL_MFR_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_mfr')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drl_mfr_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_drl_mfr_SUBSET.USER_CREATED CREATED_BY,lsmv_drl_mfr_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drl_mfr_SUBSET.user_modified  ,lsmv_drl_mfr_SUBSET.user_created  ,lsmv_drl_mfr_SUBSET.spr_id  ,lsmv_drl_mfr_SUBSET.record_id  ,lsmv_drl_mfr_SUBSET.name  ,lsmv_drl_mfr_SUBSET.mfr_code  ,lsmv_drl_mfr_SUBSET.dictionary_version  ,lsmv_drl_mfr_SUBSET.date_modified  ,lsmv_drl_mfr_SUBSET.date_created  ,lsmv_drl_mfr_SUBSET.country ,CONCAT( NVL(lsmv_drl_mfr_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drl_mfr_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP)
where target_table_name='LS_DB_DRL_MFR'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_MFR'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR   
SET LS_DB_DRL_MFR.user_modified = LS_DB_DRL_MFR_TMP.user_modified,LS_DB_DRL_MFR.user_created = LS_DB_DRL_MFR_TMP.user_created,LS_DB_DRL_MFR.spr_id = LS_DB_DRL_MFR_TMP.spr_id,LS_DB_DRL_MFR.record_id = LS_DB_DRL_MFR_TMP.record_id,LS_DB_DRL_MFR.name = LS_DB_DRL_MFR_TMP.name,LS_DB_DRL_MFR.mfr_code = LS_DB_DRL_MFR_TMP.mfr_code,LS_DB_DRL_MFR.dictionary_version = LS_DB_DRL_MFR_TMP.dictionary_version,LS_DB_DRL_MFR.date_modified = LS_DB_DRL_MFR_TMP.date_modified,LS_DB_DRL_MFR.date_created = LS_DB_DRL_MFR_TMP.date_created,LS_DB_DRL_MFR.country = LS_DB_DRL_MFR_TMP.country,
LS_DB_DRL_MFR.PROCESSING_DT = LS_DB_DRL_MFR_TMP.PROCESSING_DT ,
LS_DB_DRL_MFR.expiry_date    =LS_DB_DRL_MFR_TMP.expiry_date       ,
LS_DB_DRL_MFR.created_by     =LS_DB_DRL_MFR_TMP.created_by        ,
LS_DB_DRL_MFR.created_dt     =LS_DB_DRL_MFR_TMP.created_dt        ,
LS_DB_DRL_MFR.load_ts        =LS_DB_DRL_MFR_TMP.load_ts         
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP 
WHERE 	LS_DB_DRL_MFR.INTEGRATION_ID = LS_DB_DRL_MFR_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRL_MFR_TMP.PROCESSING_DT = LS_DB_DRL_MFR.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
name,
mfr_code,
dictionary_version,
date_modified,
date_created,
country)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
name,
mfr_code,
dictionary_version,
date_modified,
date_created,
country
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP 
WHERE 	TO_DATE(LS_DB_DRL_MFR.PROCESSING_DT) < TO_DATE(LS_DB_DRL_MFR_TMP.PROCESSING_DT)
AND LS_DB_DRL_MFR.INTEGRATION_ID = LS_DB_DRL_MFR_TMP.INTEGRATION_ID
AND LS_DB_DRL_MFR.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_DRL_MFR_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_mfr')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_DRL_MFR_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_mfr')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRL_MFR'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_MFR'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DRL_MFR Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRL_MFR'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DRL_MFR not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
