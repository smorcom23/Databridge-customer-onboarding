
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_JPN_PRODUCT_INFO()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_PRODUCT_INFO'),
	'LSRA','Case','LS_DB_JPN_PRODUCT_INFO',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_JPN_PRODUCT_INFO' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_JPN_PRODUCT_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_JPN_PRODUCT_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_PRODUCT_INFO_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_PRODUCT_INFO_DELETION_TMP  As select RECORD_ID,'lsmv_jpn_product_info' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_product_info WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_product_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_PRODUCT_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_PRODUCT_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_product_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_PRODUCT_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_PRODUCT_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_jpn_product_info_SUBSET AS 
(
select * from 
    (SELECT  
    ct_submission_times  ct_submission_times,date_created  date_created,date_modified  date_modified,fk_product_rec_id  fk_product_rec_id,patient_under_treatment  patient_under_treatment,record_id  record_id,spr_id  spr_id,study_phase  study_phase,targeted_disease  targeted_disease,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_product_info
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_PRODUCT_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_product_info')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_jpn_product_info_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_jpn_product_info_SUBSET.USER_CREATED CREATED_BY,lsmv_jpn_product_info_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_jpn_product_info_SUBSET.user_modified  ,lsmv_jpn_product_info_SUBSET.user_created  ,lsmv_jpn_product_info_SUBSET.targeted_disease  ,lsmv_jpn_product_info_SUBSET.study_phase  ,lsmv_jpn_product_info_SUBSET.spr_id  ,lsmv_jpn_product_info_SUBSET.record_id  ,lsmv_jpn_product_info_SUBSET.patient_under_treatment  ,lsmv_jpn_product_info_SUBSET.fk_product_rec_id  ,lsmv_jpn_product_info_SUBSET.date_modified  ,lsmv_jpn_product_info_SUBSET.date_created  ,lsmv_jpn_product_info_SUBSET.ct_submission_times ,CONCAT( NVL(lsmv_jpn_product_info_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_jpn_product_info_SUBSET  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP)
where target_table_name='LS_DB_JPN_PRODUCT_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_PRODUCT_INFO'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO   
SET LS_DB_JPN_PRODUCT_INFO.user_modified = LS_DB_JPN_PRODUCT_INFO_TMP.user_modified,LS_DB_JPN_PRODUCT_INFO.user_created = LS_DB_JPN_PRODUCT_INFO_TMP.user_created,LS_DB_JPN_PRODUCT_INFO.targeted_disease = LS_DB_JPN_PRODUCT_INFO_TMP.targeted_disease,LS_DB_JPN_PRODUCT_INFO.study_phase = LS_DB_JPN_PRODUCT_INFO_TMP.study_phase,LS_DB_JPN_PRODUCT_INFO.spr_id = LS_DB_JPN_PRODUCT_INFO_TMP.spr_id,LS_DB_JPN_PRODUCT_INFO.record_id = LS_DB_JPN_PRODUCT_INFO_TMP.record_id,LS_DB_JPN_PRODUCT_INFO.patient_under_treatment = LS_DB_JPN_PRODUCT_INFO_TMP.patient_under_treatment,LS_DB_JPN_PRODUCT_INFO.fk_product_rec_id = LS_DB_JPN_PRODUCT_INFO_TMP.fk_product_rec_id,LS_DB_JPN_PRODUCT_INFO.date_modified = LS_DB_JPN_PRODUCT_INFO_TMP.date_modified,LS_DB_JPN_PRODUCT_INFO.date_created = LS_DB_JPN_PRODUCT_INFO_TMP.date_created,LS_DB_JPN_PRODUCT_INFO.ct_submission_times = LS_DB_JPN_PRODUCT_INFO_TMP.ct_submission_times,
LS_DB_JPN_PRODUCT_INFO.PROCESSING_DT = LS_DB_JPN_PRODUCT_INFO_TMP.PROCESSING_DT ,
LS_DB_JPN_PRODUCT_INFO.expiry_date    =LS_DB_JPN_PRODUCT_INFO_TMP.expiry_date       ,
LS_DB_JPN_PRODUCT_INFO.created_by     =LS_DB_JPN_PRODUCT_INFO_TMP.created_by        ,
LS_DB_JPN_PRODUCT_INFO.created_dt     =LS_DB_JPN_PRODUCT_INFO_TMP.created_dt        ,
LS_DB_JPN_PRODUCT_INFO.load_ts        =LS_DB_JPN_PRODUCT_INFO_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP 
WHERE 	LS_DB_JPN_PRODUCT_INFO.INTEGRATION_ID = LS_DB_JPN_PRODUCT_INFO_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_JPN_PRODUCT_INFO_TMP.PROCESSING_DT = LS_DB_JPN_PRODUCT_INFO.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
targeted_disease,
study_phase,
spr_id,
record_id,
patient_under_treatment,
fk_product_rec_id,
date_modified,
date_created,
ct_submission_times)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
targeted_disease,
study_phase,
spr_id,
record_id,
patient_under_treatment,
fk_product_rec_id,
date_modified,
date_created,
ct_submission_times
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP 
WHERE 	TO_DATE(LS_DB_JPN_PRODUCT_INFO.PROCESSING_DT) < TO_DATE(LS_DB_JPN_PRODUCT_INFO_TMP.PROCESSING_DT)
AND LS_DB_JPN_PRODUCT_INFO.INTEGRATION_ID = LS_DB_JPN_PRODUCT_INFO_TMP.INTEGRATION_ID
AND LS_DB_JPN_PRODUCT_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_PRODUCT_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_product_info')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_PRODUCT_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_product_info')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_PRODUCT_INFO_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_JPN_PRODUCT_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_PRODUCT_INFO'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_JPN_PRODUCT_INFO Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_JPN_PRODUCT_INFO'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_JPN_PRODUCT_INFO not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
