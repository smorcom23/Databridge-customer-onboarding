
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_OVERALL_LATENESS_REASON()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_OVERALL_LATENESS_REASON'),
	'LSDB','Case','LS_DB_OVERALL_LATENESS_REASON',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_OVERALL_LATENESS_REASON' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_OVERALL_LATENESS_REASON' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_OVERALL_LATENESS_REASON_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_OVERALL_LATENESS_REASON_DELETION_TMP  As 
select RECORD_ID,'LSMV_MESSAGE' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE WHERE CDC_OPERATION_TYPE IN ('D') ;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9922')
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
					), 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID  
 FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_OVERALL_LATENESS_REASON' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= CURRENT_TIMESTAMP()

)
 ,LSMV_COMMON_COLUMN_SUBSET as
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

), LSMV_MESSAGE_SUBSET AS 
(
select  * from 
    (SELECT  record_id,
    fk_ari_rec_id ,
	OVERALL_LATENESS_REASON,
	TRIM(VALUE::STRING) AS SPLIT_VALUE, 
  (SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9922' AND CAST(CD AS VARCHAR(100))=CAST(TRIM(VALUE::STRING) AS VARCHAR(100)) )OVERALL_LATENESS_REASON_de_ml,
  t.DATE_MODIFIED,
  row_number() OVER ( PARTITION BY t.RECORD_ID,VALUE ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
  
  FROM 
${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE t,
  LATERAL SPLIT_TO_TABLE(t.OVERALL_LATENESS_REASON, ',') AS s
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_OVERALL_LATENESS_REASON_DELETION_TMP  WHERE TABLE_NAME='LSMV_SAFETY_REPORT')
  ) where REC_RANK=1 
)
   SELECT DISTINCT  to_date(NVL(LSMV_MESSAGE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))PROCESSING_DT 
   ,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO  
   ,:CURRENT_TS_VAR LOAD_TS
   ,LSMV_MESSAGE_SUBSET.record_id                                                                                     
   ,LSMV_MESSAGE_SUBSET.fk_ari_rec_id
   ,LSMV_MESSAGE_SUBSET.OVERALL_LATENESS_REASON
   ,LSMV_MESSAGE_SUBSET.SPLIT_VALUE
   ,LSMV_MESSAGE_SUBSET.OVERALL_LATENESS_REASON_de_ml
   ,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
   ,CONCAT(NVL(LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID,-1),'||',NVL(LSMV_MESSAGE_SUBSET.RECORD_ID,-1),'||',NVL(LSMV_MESSAGE_SUBSET.SPLIT_VALUE,'-1')) INTEGRATION_ID 
 FROM LSMV_MESSAGE_SUBSET   LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON LSMV_MESSAGE_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  

;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP)
where target_table_name='LS_DB_OVERALL_LATENESS_REASON'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON   
SET LS_DB_OVERALL_LATENESS_REASON.FK_ARI_REC_ID = LS_DB_OVERALL_LATENESS_REASON_TMP.FK_ARI_REC_ID,
LS_DB_OVERALL_LATENESS_REASON.record_id = LS_DB_OVERALL_LATENESS_REASON_TMP.record_id,
LS_DB_OVERALL_LATENESS_REASON.OVERALL_LATENESS_REASON = LS_DB_OVERALL_LATENESS_REASON_TMP.OVERALL_LATENESS_REASON,
LS_DB_OVERALL_LATENESS_REASON.SPLIT_VALUE = LS_DB_OVERALL_LATENESS_REASON_TMP.SPLIT_VALUE,
LS_DB_OVERALL_LATENESS_REASON.OVERALL_LATENESS_REASON_de_ml = LS_DB_OVERALL_LATENESS_REASON_TMP.OVERALL_LATENESS_REASON_de_ml,
LS_DB_OVERALL_LATENESS_REASON.PROCESSING_DT = LS_DB_OVERALL_LATENESS_REASON_TMP.PROCESSING_DT ,
LS_DB_OVERALL_LATENESS_REASON.receipt_id     =LS_DB_OVERALL_LATENESS_REASON_TMP.receipt_id        ,
LS_DB_OVERALL_LATENESS_REASON.case_no        =LS_DB_OVERALL_LATENESS_REASON_TMP.case_no           ,
LS_DB_OVERALL_LATENESS_REASON.case_version   =LS_DB_OVERALL_LATENESS_REASON_TMP.case_version      ,
LS_DB_OVERALL_LATENESS_REASON.version_no     =LS_DB_OVERALL_LATENESS_REASON_TMP.version_no        ,
LS_DB_OVERALL_LATENESS_REASON.expiry_date    =LS_DB_OVERALL_LATENESS_REASON_TMP.expiry_date       ,
LS_DB_OVERALL_LATENESS_REASON.load_ts        =LS_DB_OVERALL_LATENESS_REASON_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP 
WHERE 	LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID = LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_OVERALL_LATENESS_REASON_TMP.PROCESSING_DT = LS_DB_OVERALL_LATENESS_REASON.PROCESSING_DT,1=1);


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_OVERALL_LATENESS_REASON.RECORD_ID ,LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP 
ON LS_DB_OVERALL_LATENESS_REASON.RECORD_ID=LS_DB_OVERALL_LATENESS_REASON_TMP.RECORD_ID
AND LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID = LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID 
where LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID  is null AND LS_DB_OVERALL_LATENESS_REASON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_OVERALL_LATENESS_REASON.RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP )
) TMP where LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_OVERALL_LATENESS_REASON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_OVERALL_LATENESS_REASON.RECORD_ID ,LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP 
    ON LS_DB_OVERALL_LATENESS_REASON.RECORD_ID=LS_DB_OVERALL_LATENESS_REASON_TMP.RECORD_ID
    AND LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID = LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID 
    where LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID  is null AND LS_DB_OVERALL_LATENESS_REASON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_OVERALL_LATENESS_REASON.RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON
(
receipt_id    ,  
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,
FK_ARI_REC_ID,
record_id,  
OVERALL_LATENESS_REASON,
SPLIT_VALUE,
OVERALL_LATENESS_REASON_de_ml
)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,
FK_ARI_REC_ID,
record_id,                                                                                        
OVERALL_LATENESS_REASON,
SPLIT_VALUE,
OVERALL_LATENESS_REASON_de_ml

FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON TGT)
                                                                                ; 
COMMIT;


                                                                                        
                                                                                        
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP 
WHERE 	TO_DATE(LS_DB_OVERALL_LATENESS_REASON.PROCESSING_DT) < TO_DATE(LS_DB_OVERALL_LATENESS_REASON_TMP.PROCESSING_DT)
AND LS_DB_OVERALL_LATENESS_REASON.INTEGRATION_ID = LS_DB_OVERALL_LATENESS_REASON_TMP.INTEGRATION_ID
AND LS_DB_OVERALL_LATENESS_REASON.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_OVERALL_LATENESS_REASON_DELETION_TMP  WHERE TABLE_NAME='LSMV_MESSAGE')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_OVERALL_LATENESS_REASON_DELETION_TMP  WHERE TABLE_NAME='LSMV_MESSAGE')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = :CURRENT_TS_VAR,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_OVERALL_LATENESS_REASON_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_OVERALL_LATENESS_REASON'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_OVERALL_LATENESS_REASON Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_OVERALL_LATENESS_REASON'),
	'LSDB','Case','LS_DB_OVERALL_LATENESS_REASON',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_OVERALL_LATENESS_REASON not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;