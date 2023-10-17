
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LINKED_AE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LINKED_AE'),
	'LSDB','Case','LS_DB_LINKED_AE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_LINKED_AE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_LINKED_AE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LINKED_AE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LINKED_AE_DELETION_TMP  As select RECORD_ID,'lsmv_linked_ae' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_linked_ae WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_linked_ae WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LINKED_AE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_linked_ae WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LINKED_AE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
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

), lsmv_linked_ae_SUBSET AS 
(
select * from 
    (SELECT  
    added_flag  added_flag,ari_rec_id  ari_rec_id,auto_manual_link_flag  auto_manual_link_flag,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,description  description,inq_rec_id  inq_rec_id,linked_reason  linked_reason,lrnno  lrnno,parent_lrn  parent_lrn,parent_rcpt_no  parent_rcpt_no,rcptno  rcptno,record_id  record_id,safety_report_id  safety_report_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,worldwide_unique_no  worldwide_unique_no,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_linked_ae
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LINKED_AE_DELETION_TMP  WHERE TABLE_NAME='lsmv_linked_ae')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_linked_ae_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_linked_ae_SUBSET.USER_CREATED CREATED_BY,lsmv_linked_ae_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_linked_ae_SUBSET.worldwide_unique_no  ,lsmv_linked_ae_SUBSET.user_modified  ,lsmv_linked_ae_SUBSET.user_created  ,lsmv_linked_ae_SUBSET.spr_id  ,lsmv_linked_ae_SUBSET.safety_report_id  ,lsmv_linked_ae_SUBSET.record_id  ,lsmv_linked_ae_SUBSET.rcptno  ,lsmv_linked_ae_SUBSET.parent_rcpt_no  ,lsmv_linked_ae_SUBSET.parent_lrn  ,lsmv_linked_ae_SUBSET.lrnno  ,lsmv_linked_ae_SUBSET.linked_reason  ,lsmv_linked_ae_SUBSET.inq_rec_id  ,lsmv_linked_ae_SUBSET.description  ,lsmv_linked_ae_SUBSET.date_modified  ,lsmv_linked_ae_SUBSET.date_created  ,lsmv_linked_ae_SUBSET.comp_rec_id  ,lsmv_linked_ae_SUBSET.auto_manual_link_flag  ,lsmv_linked_ae_SUBSET.ari_rec_id  ,lsmv_linked_ae_SUBSET.added_flag ,CONCAT(NVL(lsmv_linked_ae_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_linked_ae_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_linked_ae_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP)
where target_table_name='LS_DB_LINKED_AE'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE   
SET LS_DB_LINKED_AE.worldwide_unique_no = LS_DB_LINKED_AE_TMP.worldwide_unique_no,LS_DB_LINKED_AE.user_modified = LS_DB_LINKED_AE_TMP.user_modified,LS_DB_LINKED_AE.user_created = LS_DB_LINKED_AE_TMP.user_created,LS_DB_LINKED_AE.spr_id = LS_DB_LINKED_AE_TMP.spr_id,LS_DB_LINKED_AE.safety_report_id = LS_DB_LINKED_AE_TMP.safety_report_id,LS_DB_LINKED_AE.record_id = LS_DB_LINKED_AE_TMP.record_id,LS_DB_LINKED_AE.rcptno = LS_DB_LINKED_AE_TMP.rcptno,LS_DB_LINKED_AE.parent_rcpt_no = LS_DB_LINKED_AE_TMP.parent_rcpt_no,LS_DB_LINKED_AE.parent_lrn = LS_DB_LINKED_AE_TMP.parent_lrn,LS_DB_LINKED_AE.lrnno = LS_DB_LINKED_AE_TMP.lrnno,LS_DB_LINKED_AE.linked_reason = LS_DB_LINKED_AE_TMP.linked_reason,LS_DB_LINKED_AE.inq_rec_id = LS_DB_LINKED_AE_TMP.inq_rec_id,LS_DB_LINKED_AE.description = LS_DB_LINKED_AE_TMP.description,LS_DB_LINKED_AE.date_modified = LS_DB_LINKED_AE_TMP.date_modified,LS_DB_LINKED_AE.date_created = LS_DB_LINKED_AE_TMP.date_created,LS_DB_LINKED_AE.comp_rec_id = LS_DB_LINKED_AE_TMP.comp_rec_id,LS_DB_LINKED_AE.auto_manual_link_flag = LS_DB_LINKED_AE_TMP.auto_manual_link_flag,LS_DB_LINKED_AE.ari_rec_id = LS_DB_LINKED_AE_TMP.ari_rec_id,LS_DB_LINKED_AE.added_flag = LS_DB_LINKED_AE_TMP.added_flag,
LS_DB_LINKED_AE.PROCESSING_DT = LS_DB_LINKED_AE_TMP.PROCESSING_DT ,
LS_DB_LINKED_AE.receipt_id     =LS_DB_LINKED_AE_TMP.receipt_id        ,
LS_DB_LINKED_AE.case_no        =LS_DB_LINKED_AE_TMP.case_no           ,
LS_DB_LINKED_AE.case_version   =LS_DB_LINKED_AE_TMP.case_version      ,
LS_DB_LINKED_AE.version_no     =LS_DB_LINKED_AE_TMP.version_no        ,
LS_DB_LINKED_AE.expiry_date    =LS_DB_LINKED_AE_TMP.expiry_date       ,
LS_DB_LINKED_AE.load_ts        =LS_DB_LINKED_AE_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP 
WHERE 	LS_DB_LINKED_AE.INTEGRATION_ID = LS_DB_LINKED_AE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_LINKED_AE_TMP.PROCESSING_DT = LS_DB_LINKED_AE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,worldwide_unique_no,
user_modified,
user_created,
spr_id,
safety_report_id,
record_id,
rcptno,
parent_rcpt_no,
parent_lrn,
lrnno,
linked_reason,
inq_rec_id,
description,
date_modified,
date_created,
comp_rec_id,
auto_manual_link_flag,
ari_rec_id,
added_flag)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,worldwide_unique_no,
user_modified,
user_created,
spr_id,
safety_report_id,
record_id,
rcptno,
parent_rcpt_no,
parent_lrn,
lrnno,
linked_reason,
inq_rec_id,
description,
date_modified,
date_created,
comp_rec_id,
auto_manual_link_flag,
ari_rec_id,
added_flag
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE TGT)
                                                                                ; 
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LINKED_AE_DELETION_TMP  WHERE TABLE_NAME='lsmv_linked_ae')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP 
WHERE 	TO_DATE(LS_DB_LINKED_AE.PROCESSING_DT) < TO_DATE(LS_DB_LINKED_AE_TMP.PROCESSING_DT)
AND LS_DB_LINKED_AE.INTEGRATION_ID = LS_DB_LINKED_AE_TMP.INTEGRATION_ID
AND LS_DB_LINKED_AE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LINKED_AE_DELETION_TMP  WHERE TABLE_NAME='lsmv_linked_ae')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LINKED_AE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_LINKED_AE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_LINKED_AE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LINKED_AE'),
	'LSDB','Case','LS_DB_LINKED_AE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_LINKED_AE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
