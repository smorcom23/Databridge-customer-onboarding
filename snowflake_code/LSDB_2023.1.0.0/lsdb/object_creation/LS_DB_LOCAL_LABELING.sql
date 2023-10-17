
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_LOCAL_LABELING()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LOCAL_LABELING'),
	'LSRA','Case','LS_DB_LOCAL_LABELING',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_LOCAL_LABELING' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_LOCAL_LABELING'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_LOCAL_LABELING'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_LOCAL_LABELING_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_LOCAL_LABELING_DELETION_TMP  As select RECORD_ID,'lsmv_local_labeling' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_local_labeling WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_local_labeling WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_LOCAL_LABELING' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_LOCAL_LABELING' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_local_labeling WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_LOCAL_LABELING' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_LOCAL_LABELING' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_local_labeling_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,cl_checkbox  cl_checkbox,condition  condition,date_created  date_created,date_modified  date_modified,fk_ad_rec_id  fk_ad_rec_id,fk_ar_rec_id  fk_ar_rec_id,fk_asr_rec_id  fk_asr_rec_id,inq_rec_id  inq_rec_id,labeled  labeled,labeling_country  labeling_country,manual_checkbox  manual_checkbox,record_id  record_id,reported_term  reported_term,spr_id  spr_id,time_flag  time_flag,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_local_labeling
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_LOCAL_LABELING_DELETION_TMP  WHERE TABLE_NAME='lsmv_local_labeling')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_local_labeling_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_local_labeling_SUBSET.USER_CREATED CREATED_BY,lsmv_local_labeling_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_local_labeling_SUBSET.user_modified  ,lsmv_local_labeling_SUBSET.user_created  ,lsmv_local_labeling_SUBSET.time_flag  ,lsmv_local_labeling_SUBSET.spr_id  ,lsmv_local_labeling_SUBSET.reported_term  ,lsmv_local_labeling_SUBSET.record_id  ,lsmv_local_labeling_SUBSET.manual_checkbox  ,lsmv_local_labeling_SUBSET.labeling_country  ,lsmv_local_labeling_SUBSET.labeled  ,lsmv_local_labeling_SUBSET.inq_rec_id  ,lsmv_local_labeling_SUBSET.fk_asr_rec_id  ,lsmv_local_labeling_SUBSET.fk_ar_rec_id  ,lsmv_local_labeling_SUBSET.fk_ad_rec_id  ,lsmv_local_labeling_SUBSET.date_modified  ,lsmv_local_labeling_SUBSET.date_created  ,lsmv_local_labeling_SUBSET.condition  ,lsmv_local_labeling_SUBSET.cl_checkbox  ,lsmv_local_labeling_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_local_labeling_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_local_labeling_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_local_labeling_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP)
where target_table_name='LS_DB_LOCAL_LABELING'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LOCAL_LABELING'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING   
SET LS_DB_LOCAL_LABELING.user_modified = LS_DB_LOCAL_LABELING_TMP.user_modified,LS_DB_LOCAL_LABELING.user_created = LS_DB_LOCAL_LABELING_TMP.user_created,LS_DB_LOCAL_LABELING.time_flag = LS_DB_LOCAL_LABELING_TMP.time_flag,LS_DB_LOCAL_LABELING.spr_id = LS_DB_LOCAL_LABELING_TMP.spr_id,LS_DB_LOCAL_LABELING.reported_term = LS_DB_LOCAL_LABELING_TMP.reported_term,LS_DB_LOCAL_LABELING.record_id = LS_DB_LOCAL_LABELING_TMP.record_id,LS_DB_LOCAL_LABELING.manual_checkbox = LS_DB_LOCAL_LABELING_TMP.manual_checkbox,LS_DB_LOCAL_LABELING.labeling_country = LS_DB_LOCAL_LABELING_TMP.labeling_country,LS_DB_LOCAL_LABELING.labeled = LS_DB_LOCAL_LABELING_TMP.labeled,LS_DB_LOCAL_LABELING.inq_rec_id = LS_DB_LOCAL_LABELING_TMP.inq_rec_id,LS_DB_LOCAL_LABELING.fk_asr_rec_id = LS_DB_LOCAL_LABELING_TMP.fk_asr_rec_id,LS_DB_LOCAL_LABELING.fk_ar_rec_id = LS_DB_LOCAL_LABELING_TMP.fk_ar_rec_id,LS_DB_LOCAL_LABELING.fk_ad_rec_id = LS_DB_LOCAL_LABELING_TMP.fk_ad_rec_id,LS_DB_LOCAL_LABELING.date_modified = LS_DB_LOCAL_LABELING_TMP.date_modified,LS_DB_LOCAL_LABELING.date_created = LS_DB_LOCAL_LABELING_TMP.date_created,LS_DB_LOCAL_LABELING.condition = LS_DB_LOCAL_LABELING_TMP.condition,LS_DB_LOCAL_LABELING.cl_checkbox = LS_DB_LOCAL_LABELING_TMP.cl_checkbox,LS_DB_LOCAL_LABELING.ari_rec_id = LS_DB_LOCAL_LABELING_TMP.ari_rec_id,
LS_DB_LOCAL_LABELING.PROCESSING_DT = LS_DB_LOCAL_LABELING_TMP.PROCESSING_DT ,
LS_DB_LOCAL_LABELING.receipt_id     =LS_DB_LOCAL_LABELING_TMP.receipt_id        ,
LS_DB_LOCAL_LABELING.case_no        =LS_DB_LOCAL_LABELING_TMP.case_no           ,
LS_DB_LOCAL_LABELING.case_version   =LS_DB_LOCAL_LABELING_TMP.case_version      ,
LS_DB_LOCAL_LABELING.version_no     =LS_DB_LOCAL_LABELING_TMP.version_no        ,
LS_DB_LOCAL_LABELING.expiry_date    =LS_DB_LOCAL_LABELING_TMP.expiry_date       ,
LS_DB_LOCAL_LABELING.load_ts        =LS_DB_LOCAL_LABELING_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP 
WHERE 	LS_DB_LOCAL_LABELING.INTEGRATION_ID = LS_DB_LOCAL_LABELING_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_LOCAL_LABELING_TMP.PROCESSING_DT = LS_DB_LOCAL_LABELING.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING
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
time_flag,
spr_id,
reported_term,
record_id,
manual_checkbox,
labeling_country,
labeled,
inq_rec_id,
fk_asr_rec_id,
fk_ar_rec_id,
fk_ad_rec_id,
date_modified,
date_created,
condition,
cl_checkbox,
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
time_flag,
spr_id,
reported_term,
record_id,
manual_checkbox,
labeling_country,
labeled,
inq_rec_id,
fk_asr_rec_id,
fk_ar_rec_id,
fk_ad_rec_id,
date_modified,
date_created,
condition,
cl_checkbox,
ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP 
WHERE 	TO_DATE(LS_DB_LOCAL_LABELING.PROCESSING_DT) < TO_DATE(LS_DB_LOCAL_LABELING_TMP.PROCESSING_DT)
AND LS_DB_LOCAL_LABELING.INTEGRATION_ID = LS_DB_LOCAL_LABELING_TMP.INTEGRATION_ID
AND LS_DB_LOCAL_LABELING.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_LOCAL_LABELING_DELETION_TMP  WHERE TABLE_NAME='lsmv_local_labeling')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_LOCAL_LABELING_DELETION_TMP  WHERE TABLE_NAME='lsmv_local_labeling')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LOCAL_LABELING_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_LOCAL_LABELING'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LOCAL_LABELING'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_LOCAL_LABELING Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_LOCAL_LABELING'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_LOCAL_LABELING not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
