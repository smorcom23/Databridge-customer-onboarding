
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SENDER_DIAGNOSIS()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SENDER_DIAGNOSIS'),
	'LSRA','Case','LS_DB_SENDER_DIAGNOSIS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SENDER_DIAGNOSIS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SENDER_DIAGNOSIS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_SENDER_DIAGNOSIS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SENDER_DIAGNOSIS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SENDER_DIAGNOSIS_DELETION_TMP  As select RECORD_ID,'lsmv_sender_diagnosis' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_sender_diagnosis WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
					),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_sender_diagnosis WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SENDER_DIAGNOSIS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SENDER_DIAGNOSIS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_sender_diagnosis WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SENDER_DIAGNOSIS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SENDER_DIAGNOSIS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_sender_diagnosis_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,coding_comments  coding_comments,coding_type  coding_type,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,diagnosis_code  diagnosis_code,diagnosis_term  diagnosis_term,fk_summary_rec_id  fk_summary_rec_id,inq_rec_id  inq_rec_id,llt_code  llt_code,llt_decode  llt_decode,meddra_version  meddra_version,pt_code  pt_code,record_id  record_id,senderdiagnosis_coded_flag  senderdiagnosis_coded_flag,senderdiagnosislevel  senderdiagnosislevel,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_sender_diagnosis
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SENDER_DIAGNOSIS_DELETION_TMP  WHERE TABLE_NAME='lsmv_sender_diagnosis')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_sender_diagnosis_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_sender_diagnosis_SUBSET.USER_CREATED CREATED_BY,lsmv_sender_diagnosis_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_sender_diagnosis_SUBSET.user_modified  ,lsmv_sender_diagnosis_SUBSET.user_created  ,lsmv_sender_diagnosis_SUBSET.spr_id  ,lsmv_sender_diagnosis_SUBSET.senderdiagnosislevel  ,lsmv_sender_diagnosis_SUBSET.senderdiagnosis_coded_flag  ,lsmv_sender_diagnosis_SUBSET.record_id  ,
try_to_number(lsmv_sender_diagnosis_SUBSET.pt_code,38) pt_code  ,lsmv_sender_diagnosis_SUBSET.meddra_version  ,lsmv_sender_diagnosis_SUBSET.llt_decode  ,try_to_number(lsmv_sender_diagnosis_SUBSET.llt_code,38) llt_code ,lsmv_sender_diagnosis_SUBSET.inq_rec_id  ,lsmv_sender_diagnosis_SUBSET.fk_summary_rec_id  ,lsmv_sender_diagnosis_SUBSET.diagnosis_term  ,lsmv_sender_diagnosis_SUBSET.diagnosis_code  ,lsmv_sender_diagnosis_SUBSET.date_modified  ,lsmv_sender_diagnosis_SUBSET.date_created  ,lsmv_sender_diagnosis_SUBSET.comp_rec_id  ,lsmv_sender_diagnosis_SUBSET.coding_type  ,lsmv_sender_diagnosis_SUBSET.coding_comments  ,lsmv_sender_diagnosis_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_sender_diagnosis_SUBSET.RECORD_ID,-1)) INTEGRATION_ID,lsmv_sender_diagnosis_SUBSET.llt_code llt_code_1,lsmv_sender_diagnosis_SUBSET.pt_code pt_code_1 FROM lsmv_sender_diagnosis_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_sender_diagnosis_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP)
where target_table_name='LS_DB_SENDER_DIAGNOSIS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SENDER_DIAGNOSIS'
					and LOAD_STATUS = 'In Progress') 
; 



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_sender_diagnosis' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'PT_CODE' ,
PT_CODE_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on PT_CODE'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP
WHERE (PT_CODE is null and PT_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_sender_diagnosis' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'LLT_CODE' ,
LLT_CODE_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on LLT_CODE'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP
WHERE (LLT_CODE is null and LLT_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP
WHERE (PT_CODE is null and PT_CODE_1 is not null)
or (LLT_CODE is null and LLT_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS   
SET LS_DB_SENDER_DIAGNOSIS.user_modified = LS_DB_SENDER_DIAGNOSIS_TMP.user_modified,LS_DB_SENDER_DIAGNOSIS.user_created = LS_DB_SENDER_DIAGNOSIS_TMP.user_created,LS_DB_SENDER_DIAGNOSIS.spr_id = LS_DB_SENDER_DIAGNOSIS_TMP.spr_id,LS_DB_SENDER_DIAGNOSIS.senderdiagnosislevel = LS_DB_SENDER_DIAGNOSIS_TMP.senderdiagnosislevel,LS_DB_SENDER_DIAGNOSIS.senderdiagnosis_coded_flag = LS_DB_SENDER_DIAGNOSIS_TMP.senderdiagnosis_coded_flag,LS_DB_SENDER_DIAGNOSIS.record_id = LS_DB_SENDER_DIAGNOSIS_TMP.record_id,LS_DB_SENDER_DIAGNOSIS.pt_code = LS_DB_SENDER_DIAGNOSIS_TMP.pt_code,LS_DB_SENDER_DIAGNOSIS.meddra_version = LS_DB_SENDER_DIAGNOSIS_TMP.meddra_version,LS_DB_SENDER_DIAGNOSIS.llt_decode = LS_DB_SENDER_DIAGNOSIS_TMP.llt_decode,LS_DB_SENDER_DIAGNOSIS.llt_code = LS_DB_SENDER_DIAGNOSIS_TMP.llt_code,LS_DB_SENDER_DIAGNOSIS.inq_rec_id = LS_DB_SENDER_DIAGNOSIS_TMP.inq_rec_id,LS_DB_SENDER_DIAGNOSIS.fk_summary_rec_id = LS_DB_SENDER_DIAGNOSIS_TMP.fk_summary_rec_id,LS_DB_SENDER_DIAGNOSIS.diagnosis_term = LS_DB_SENDER_DIAGNOSIS_TMP.diagnosis_term,LS_DB_SENDER_DIAGNOSIS.diagnosis_code = LS_DB_SENDER_DIAGNOSIS_TMP.diagnosis_code,LS_DB_SENDER_DIAGNOSIS.date_modified = LS_DB_SENDER_DIAGNOSIS_TMP.date_modified,LS_DB_SENDER_DIAGNOSIS.date_created = LS_DB_SENDER_DIAGNOSIS_TMP.date_created,LS_DB_SENDER_DIAGNOSIS.comp_rec_id = LS_DB_SENDER_DIAGNOSIS_TMP.comp_rec_id,LS_DB_SENDER_DIAGNOSIS.coding_type = LS_DB_SENDER_DIAGNOSIS_TMP.coding_type,LS_DB_SENDER_DIAGNOSIS.coding_comments = LS_DB_SENDER_DIAGNOSIS_TMP.coding_comments,LS_DB_SENDER_DIAGNOSIS.ari_rec_id = LS_DB_SENDER_DIAGNOSIS_TMP.ari_rec_id,
LS_DB_SENDER_DIAGNOSIS.PROCESSING_DT = LS_DB_SENDER_DIAGNOSIS_TMP.PROCESSING_DT ,
LS_DB_SENDER_DIAGNOSIS.receipt_id     =LS_DB_SENDER_DIAGNOSIS_TMP.receipt_id        ,
LS_DB_SENDER_DIAGNOSIS.case_no        =LS_DB_SENDER_DIAGNOSIS_TMP.case_no           ,
LS_DB_SENDER_DIAGNOSIS.case_version   =LS_DB_SENDER_DIAGNOSIS_TMP.case_version      ,
LS_DB_SENDER_DIAGNOSIS.version_no     =LS_DB_SENDER_DIAGNOSIS_TMP.version_no        ,
LS_DB_SENDER_DIAGNOSIS.expiry_date    =LS_DB_SENDER_DIAGNOSIS_TMP.expiry_date       ,
LS_DB_SENDER_DIAGNOSIS.load_ts        =LS_DB_SENDER_DIAGNOSIS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP 
WHERE 	LS_DB_SENDER_DIAGNOSIS.INTEGRATION_ID = LS_DB_SENDER_DIAGNOSIS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SENDER_DIAGNOSIS_TMP.PROCESSING_DT = LS_DB_SENDER_DIAGNOSIS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS
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
spr_id,
senderdiagnosislevel,
senderdiagnosis_coded_flag,
record_id,
pt_code,
meddra_version,
llt_decode,
llt_code,
inq_rec_id,
fk_summary_rec_id,
diagnosis_term,
diagnosis_code,
date_modified,
date_created,
comp_rec_id,
coding_type,
coding_comments,
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
spr_id,
senderdiagnosislevel,
senderdiagnosis_coded_flag,
record_id,
pt_code,
meddra_version,
llt_decode,
llt_code,
inq_rec_id,
fk_summary_rec_id,
diagnosis_term,
diagnosis_code,
date_modified,
date_created,
comp_rec_id,
coding_type,
coding_comments,
ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP 
WHERE 	TO_DATE(LS_DB_SENDER_DIAGNOSIS.PROCESSING_DT) < TO_DATE(LS_DB_SENDER_DIAGNOSIS_TMP.PROCESSING_DT)
AND LS_DB_SENDER_DIAGNOSIS.INTEGRATION_ID = LS_DB_SENDER_DIAGNOSIS_TMP.INTEGRATION_ID
AND LS_DB_SENDER_DIAGNOSIS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SENDER_DIAGNOSIS_DELETION_TMP  WHERE TABLE_NAME='lsmv_sender_diagnosis')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SENDER_DIAGNOSIS_DELETION_TMP  WHERE TABLE_NAME='lsmv_sender_diagnosis')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SENDER_DIAGNOSIS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SENDER_DIAGNOSIS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SENDER_DIAGNOSIS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_SENDER_DIAGNOSIS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SENDER_DIAGNOSIS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_SENDER_DIAGNOSIS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
