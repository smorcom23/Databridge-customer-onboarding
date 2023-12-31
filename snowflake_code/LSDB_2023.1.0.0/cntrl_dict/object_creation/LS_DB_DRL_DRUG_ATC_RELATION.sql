
-- USE SCHEMA $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.PRC_LS_DB_DRL_DRUG_ATC_RELATION()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'),
	'LSRA','Case','LS_DB_DRL_DRUG_ATC_RELATION',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRL_DRUG_ATC_RELATION' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRL_DRUG_ATC_RELATION'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DRL_DRUG_ATC_RELATION'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LSMV_DRL_DRUG_ATC_RELATION_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LSMV_DRL_DRUG_ATC_RELATION_DELETION_TMP  As select RECORD_ID,'lsmv_drl_drug_atc_relation' AS TABLE_NAME FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.lsmv_drl_drug_atc_relation WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP;
CREATE TEMPORARY TABLE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.lsmv_drl_drug_atc_relation WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_DRUG_ATC_RELATION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_DRUG_ATC_RELATION' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.lsmv_drl_drug_atc_relation WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_DRUG_ATC_RELATION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRL_DRUG_ATC_RELATION' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_drl_drug_atc_relation_SUBSET AS 
(
select * from 
    (SELECT  
    atc_code  atc_code,date_created  date_created,date_modified  date_modified,dictionary_version  dictionary_version,preferred_code  preferred_code,quarter_amended  quarter_amended,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_CNTRL_DB_NAME.$$LSDB_RPL.lsmv_drl_drug_atc_relation
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LSMV_DRL_DRUG_ATC_RELATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_drug_atc_relation')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drl_drug_atc_relation_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_drl_drug_atc_relation_SUBSET.USER_CREATED CREATED_BY,lsmv_drl_drug_atc_relation_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drl_drug_atc_relation_SUBSET.user_modified  ,lsmv_drl_drug_atc_relation_SUBSET.user_created  ,lsmv_drl_drug_atc_relation_SUBSET.spr_id  ,lsmv_drl_drug_atc_relation_SUBSET.record_id  ,lsmv_drl_drug_atc_relation_SUBSET.quarter_amended  ,lsmv_drl_drug_atc_relation_SUBSET.preferred_code  ,lsmv_drl_drug_atc_relation_SUBSET.dictionary_version  ,lsmv_drl_drug_atc_relation_SUBSET.date_modified  ,lsmv_drl_drug_atc_relation_SUBSET.date_created  ,lsmv_drl_drug_atc_relation_SUBSET.atc_code ,CONCAT( NVL(lsmv_drl_drug_atc_relation_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drl_drug_atc_relation_SUBSET  WHERE 1=1  
;



UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP)
where target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION   
SET LS_DB_DRL_DRUG_ATC_RELATION.user_modified = LS_DB_DRL_DRUG_ATC_RELATION_TMP.user_modified,LS_DB_DRL_DRUG_ATC_RELATION.user_created = LS_DB_DRL_DRUG_ATC_RELATION_TMP.user_created,LS_DB_DRL_DRUG_ATC_RELATION.spr_id = LS_DB_DRL_DRUG_ATC_RELATION_TMP.spr_id,LS_DB_DRL_DRUG_ATC_RELATION.record_id = LS_DB_DRL_DRUG_ATC_RELATION_TMP.record_id,LS_DB_DRL_DRUG_ATC_RELATION.quarter_amended = LS_DB_DRL_DRUG_ATC_RELATION_TMP.quarter_amended,LS_DB_DRL_DRUG_ATC_RELATION.preferred_code = LS_DB_DRL_DRUG_ATC_RELATION_TMP.preferred_code,LS_DB_DRL_DRUG_ATC_RELATION.dictionary_version = LS_DB_DRL_DRUG_ATC_RELATION_TMP.dictionary_version,LS_DB_DRL_DRUG_ATC_RELATION.date_modified = LS_DB_DRL_DRUG_ATC_RELATION_TMP.date_modified,LS_DB_DRL_DRUG_ATC_RELATION.date_created = LS_DB_DRL_DRUG_ATC_RELATION_TMP.date_created,LS_DB_DRL_DRUG_ATC_RELATION.atc_code = LS_DB_DRL_DRUG_ATC_RELATION_TMP.atc_code,
LS_DB_DRL_DRUG_ATC_RELATION.PROCESSING_DT = LS_DB_DRL_DRUG_ATC_RELATION_TMP.PROCESSING_DT ,
LS_DB_DRL_DRUG_ATC_RELATION.expiry_date    =LS_DB_DRL_DRUG_ATC_RELATION_TMP.expiry_date       ,
LS_DB_DRL_DRUG_ATC_RELATION.created_by     =LS_DB_DRL_DRUG_ATC_RELATION_TMP.created_by        ,
LS_DB_DRL_DRUG_ATC_RELATION.created_dt     =LS_DB_DRL_DRUG_ATC_RELATION_TMP.created_dt        ,
LS_DB_DRL_DRUG_ATC_RELATION.load_ts        =LS_DB_DRL_DRUG_ATC_RELATION_TMP.load_ts         
FROM 	$$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP 
WHERE 	LS_DB_DRL_DRUG_ATC_RELATION.INTEGRATION_ID = LS_DB_DRL_DRUG_ATC_RELATION_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRL_DRUG_ATC_RELATION_TMP.PROCESSING_DT = LS_DB_DRL_DRUG_ATC_RELATION.PROCESSING_DT,1=1);


INSERT INTO $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
quarter_amended,
preferred_code,
dictionary_version,
date_modified,
date_created,
atc_code)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
quarter_amended,
preferred_code,
dictionary_version,
date_modified,
date_created,
atc_code
FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP 
WHERE 	TO_DATE(LS_DB_DRL_DRUG_ATC_RELATION.PROCESSING_DT) < TO_DATE(LS_DB_DRL_DRUG_ATC_RELATION_TMP.PROCESSING_DT)
AND LS_DB_DRL_DRUG_ATC_RELATION.INTEGRATION_ID = LS_DB_DRL_DRUG_ATC_RELATION_TMP.INTEGRATION_ID
AND LS_DB_DRL_DRUG_ATC_RELATION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LSMV_DRL_DRUG_ATC_RELATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_drug_atc_relation')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LSMV_DRL_DRUG_ATC_RELATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drl_drug_atc_relation')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION where LOAD_TS= (select LOAD_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DRL_DRUG_ATC_RELATION Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRL_DRUG_ATC_RELATION'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DRL_DRUG_ATC_RELATION not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
