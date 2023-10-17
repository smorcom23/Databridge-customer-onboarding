
-- USE SCHEMA ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_HLGT_PREF_TERM()
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
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HLGT_PREF_TERM'),
	'LSRA','Case','LS_DB_HLGT_PREF_TERM',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_HLGT_PREF_TERM' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_HLGT_PREF_TERM'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_HLGT_PREF_TERM'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_HLGT_PREF_TERM_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_HLGT_PREF_TERM_DELETION_TMP  As select RECORD_ID,'lsmv_hlgt_pref_term' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_hlgt_pref_term WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_hlgt_pref_term WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HLGT_PREF_TERM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HLGT_PREF_TERM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_hlgt_pref_term WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HLGT_PREF_TERM' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HLGT_PREF_TERM' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_hlgt_pref_term_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  date_created,date_modified  date_modified,hlgt_code  hlgt_code,hlgt_costart_sym  hlgt_costart_sym,hlgt_harts_code  hlgt_harts_code,hlgt_icd10_code  hlgt_icd10_code,hlgt_icd9_code  hlgt_icd9_code,hlgt_icd9cm_code  hlgt_icd9cm_code,hlgt_jart_code  hlgt_jart_code,hlgt_kanji  hlgt_kanji,hlgt_name  hlgt_name,hlgt_name_cn  hlgt_name_cn,hlgt_whoart_code  hlgt_whoart_code,meddra_version  meddra_version,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_hlgt_pref_term
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_HLGT_PREF_TERM_DELETION_TMP  WHERE TABLE_NAME='lsmv_hlgt_pref_term')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_hlgt_pref_term_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_hlgt_pref_term_SUBSET.USER_CREATED CREATED_BY,lsmv_hlgt_pref_term_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_hlgt_pref_term_SUBSET.user_modified  ,lsmv_hlgt_pref_term_SUBSET.user_created  ,lsmv_hlgt_pref_term_SUBSET.spr_id  ,lsmv_hlgt_pref_term_SUBSET.record_id  ,lsmv_hlgt_pref_term_SUBSET.meddra_version  ,lsmv_hlgt_pref_term_SUBSET.hlgt_whoart_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_name_cn  ,lsmv_hlgt_pref_term_SUBSET.hlgt_name  ,lsmv_hlgt_pref_term_SUBSET.hlgt_kanji  ,lsmv_hlgt_pref_term_SUBSET.hlgt_jart_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_icd9cm_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_icd9_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_icd10_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_harts_code  ,lsmv_hlgt_pref_term_SUBSET.hlgt_costart_sym  ,lsmv_hlgt_pref_term_SUBSET.hlgt_code  ,lsmv_hlgt_pref_term_SUBSET.date_modified  ,lsmv_hlgt_pref_term_SUBSET.date_created ,CONCAT( NVL(lsmv_hlgt_pref_term_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_hlgt_pref_term_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP)
where target_table_name='LS_DB_HLGT_PREF_TERM'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HLGT_PREF_TERM'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM   
SET LS_DB_HLGT_PREF_TERM.user_modified = LS_DB_HLGT_PREF_TERM_TMP.user_modified,LS_DB_HLGT_PREF_TERM.user_created = LS_DB_HLGT_PREF_TERM_TMP.user_created,LS_DB_HLGT_PREF_TERM.spr_id = LS_DB_HLGT_PREF_TERM_TMP.spr_id,LS_DB_HLGT_PREF_TERM.record_id = LS_DB_HLGT_PREF_TERM_TMP.record_id,LS_DB_HLGT_PREF_TERM.meddra_version = LS_DB_HLGT_PREF_TERM_TMP.meddra_version,LS_DB_HLGT_PREF_TERM.hlgt_whoart_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_whoart_code,LS_DB_HLGT_PREF_TERM.hlgt_name_cn = LS_DB_HLGT_PREF_TERM_TMP.hlgt_name_cn,LS_DB_HLGT_PREF_TERM.hlgt_name = LS_DB_HLGT_PREF_TERM_TMP.hlgt_name,LS_DB_HLGT_PREF_TERM.hlgt_kanji = LS_DB_HLGT_PREF_TERM_TMP.hlgt_kanji,LS_DB_HLGT_PREF_TERM.hlgt_jart_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_jart_code,LS_DB_HLGT_PREF_TERM.hlgt_icd9cm_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_icd9cm_code,LS_DB_HLGT_PREF_TERM.hlgt_icd9_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_icd9_code,LS_DB_HLGT_PREF_TERM.hlgt_icd10_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_icd10_code,LS_DB_HLGT_PREF_TERM.hlgt_harts_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_harts_code,LS_DB_HLGT_PREF_TERM.hlgt_costart_sym = LS_DB_HLGT_PREF_TERM_TMP.hlgt_costart_sym,LS_DB_HLGT_PREF_TERM.hlgt_code = LS_DB_HLGT_PREF_TERM_TMP.hlgt_code,LS_DB_HLGT_PREF_TERM.date_modified = LS_DB_HLGT_PREF_TERM_TMP.date_modified,LS_DB_HLGT_PREF_TERM.date_created = LS_DB_HLGT_PREF_TERM_TMP.date_created,
LS_DB_HLGT_PREF_TERM.PROCESSING_DT = LS_DB_HLGT_PREF_TERM_TMP.PROCESSING_DT ,
LS_DB_HLGT_PREF_TERM.expiry_date    =LS_DB_HLGT_PREF_TERM_TMP.expiry_date       ,
LS_DB_HLGT_PREF_TERM.created_by     =LS_DB_HLGT_PREF_TERM_TMP.created_by        ,
LS_DB_HLGT_PREF_TERM.created_dt     =LS_DB_HLGT_PREF_TERM_TMP.created_dt        ,
LS_DB_HLGT_PREF_TERM.load_ts        =LS_DB_HLGT_PREF_TERM_TMP.load_ts         
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP 
WHERE 	LS_DB_HLGT_PREF_TERM.INTEGRATION_ID = LS_DB_HLGT_PREF_TERM_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_HLGT_PREF_TERM_TMP.PROCESSING_DT = LS_DB_HLGT_PREF_TERM.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
meddra_version,
hlgt_whoart_code,
hlgt_name_cn,
hlgt_name,
hlgt_kanji,
hlgt_jart_code,
hlgt_icd9cm_code,
hlgt_icd9_code,
hlgt_icd10_code,
hlgt_harts_code,
hlgt_costart_sym,
hlgt_code,
date_modified,
date_created)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
meddra_version,
hlgt_whoart_code,
hlgt_name_cn,
hlgt_name,
hlgt_kanji,
hlgt_jart_code,
hlgt_icd9cm_code,
hlgt_icd9_code,
hlgt_icd10_code,
hlgt_harts_code,
hlgt_costart_sym,
hlgt_code,
date_modified,
date_created
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP 
WHERE 	TO_DATE(LS_DB_HLGT_PREF_TERM.PROCESSING_DT) < TO_DATE(LS_DB_HLGT_PREF_TERM_TMP.PROCESSING_DT)
AND LS_DB_HLGT_PREF_TERM.INTEGRATION_ID = LS_DB_HLGT_PREF_TERM_TMP.INTEGRATION_ID
AND LS_DB_HLGT_PREF_TERM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_HLGT_PREF_TERM_DELETION_TMP  WHERE TABLE_NAME='lsmv_hlgt_pref_term')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_HLGT_PREF_TERM_DELETION_TMP  WHERE TABLE_NAME='lsmv_hlgt_pref_term')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_HLGT_PREF_TERM'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HLGT_PREF_TERM'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_HLGT_PREF_TERM Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_HLGT_PREF_TERM'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_HLGT_PREF_TERM not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
