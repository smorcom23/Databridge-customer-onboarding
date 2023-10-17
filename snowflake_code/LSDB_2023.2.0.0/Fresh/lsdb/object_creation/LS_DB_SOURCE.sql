
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE'),
	'LSDB','Case','LS_DB_SOURCE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_SOURCE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SOURCE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DELETION_TMP  As select RECORD_ID,'lsmv_source' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_source WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('346','7077','9060')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_source WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SOURCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_source WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SOURCE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_source_SUBSET AS 
(
select * from 
    (SELECT  
    account_id  account_id,account_record_id  account_record_id,ari_rec_id  ari_rec_id,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,date_received  date_received,date_received_format  date_received_format,description  description,entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fk_asr_rec_id  fk_asr_rec_id,follow_up_number  follow_up_number,identification_number  identification_number,inq_rec_id  inq_rec_id,originating_account  originating_account,primary_source_flag  primary_source_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(primary_source_flag AS VARCHAR(100)) )primary_source_flag_de_ml , record_id  record_id,reference_type  reference_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9060' AND CD=CAST(reference_type AS VARCHAR(100)) )reference_type_de_ml , report_source  report_source,source  source,source_aer_number  source_aer_number,source_auto_flag  source_auto_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='346' AND CD=CAST(source AS VARCHAR(100)) )source_de_ml , spr_id  spr_id,user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_source
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_source')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_source_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_source_SUBSET.USER_CREATED CREATED_BY,lsmv_source_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_source_SUBSET.version  ,lsmv_source_SUBSET.uuid  ,lsmv_source_SUBSET.user_modified  ,lsmv_source_SUBSET.user_created  ,lsmv_source_SUBSET.spr_id  ,lsmv_source_SUBSET.source_de_ml  ,lsmv_source_SUBSET.source_auto_flag  ,lsmv_source_SUBSET.source_aer_number  ,lsmv_source_SUBSET.source  ,lsmv_source_SUBSET.report_source  ,lsmv_source_SUBSET.reference_type_de_ml  ,lsmv_source_SUBSET.reference_type  ,lsmv_source_SUBSET.record_id  ,lsmv_source_SUBSET.primary_source_flag_de_ml  ,lsmv_source_SUBSET.primary_source_flag  ,lsmv_source_SUBSET.originating_account  ,lsmv_source_SUBSET.inq_rec_id  ,lsmv_source_SUBSET.identification_number  ,lsmv_source_SUBSET.follow_up_number  ,lsmv_source_SUBSET.fk_asr_rec_id  ,lsmv_source_SUBSET.ext_clob_fld  ,lsmv_source_SUBSET.entity_updated  ,lsmv_source_SUBSET.description  ,lsmv_source_SUBSET.date_received_format  ,lsmv_source_SUBSET.date_received  ,lsmv_source_SUBSET.date_modified  ,lsmv_source_SUBSET.date_created  ,lsmv_source_SUBSET.comp_rec_id  ,lsmv_source_SUBSET.ari_rec_id  ,lsmv_source_SUBSET.account_record_id  ,lsmv_source_SUBSET.account_id ,CONCAT(NVL(lsmv_source_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_source_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_source_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP)
where target_table_name='LS_DB_SOURCE'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE   
SET LS_DB_SOURCE.version = LS_DB_SOURCE_TMP.version,LS_DB_SOURCE.uuid = LS_DB_SOURCE_TMP.uuid,LS_DB_SOURCE.user_modified = LS_DB_SOURCE_TMP.user_modified,LS_DB_SOURCE.user_created = LS_DB_SOURCE_TMP.user_created,LS_DB_SOURCE.spr_id = LS_DB_SOURCE_TMP.spr_id,LS_DB_SOURCE.source_de_ml = LS_DB_SOURCE_TMP.source_de_ml,LS_DB_SOURCE.source_auto_flag = LS_DB_SOURCE_TMP.source_auto_flag,LS_DB_SOURCE.source_aer_number = LS_DB_SOURCE_TMP.source_aer_number,LS_DB_SOURCE.source = LS_DB_SOURCE_TMP.source,LS_DB_SOURCE.report_source = LS_DB_SOURCE_TMP.report_source,LS_DB_SOURCE.reference_type_de_ml = LS_DB_SOURCE_TMP.reference_type_de_ml,LS_DB_SOURCE.reference_type = LS_DB_SOURCE_TMP.reference_type,LS_DB_SOURCE.record_id = LS_DB_SOURCE_TMP.record_id,LS_DB_SOURCE.primary_source_flag_de_ml = LS_DB_SOURCE_TMP.primary_source_flag_de_ml,LS_DB_SOURCE.primary_source_flag = LS_DB_SOURCE_TMP.primary_source_flag,LS_DB_SOURCE.originating_account = LS_DB_SOURCE_TMP.originating_account,LS_DB_SOURCE.inq_rec_id = LS_DB_SOURCE_TMP.inq_rec_id,LS_DB_SOURCE.identification_number = LS_DB_SOURCE_TMP.identification_number,LS_DB_SOURCE.follow_up_number = LS_DB_SOURCE_TMP.follow_up_number,LS_DB_SOURCE.fk_asr_rec_id = LS_DB_SOURCE_TMP.fk_asr_rec_id,LS_DB_SOURCE.ext_clob_fld = LS_DB_SOURCE_TMP.ext_clob_fld,LS_DB_SOURCE.entity_updated = LS_DB_SOURCE_TMP.entity_updated,LS_DB_SOURCE.description = LS_DB_SOURCE_TMP.description,LS_DB_SOURCE.date_received_format = LS_DB_SOURCE_TMP.date_received_format,LS_DB_SOURCE.date_received = LS_DB_SOURCE_TMP.date_received,LS_DB_SOURCE.date_modified = LS_DB_SOURCE_TMP.date_modified,LS_DB_SOURCE.date_created = LS_DB_SOURCE_TMP.date_created,LS_DB_SOURCE.comp_rec_id = LS_DB_SOURCE_TMP.comp_rec_id,LS_DB_SOURCE.ari_rec_id = LS_DB_SOURCE_TMP.ari_rec_id,LS_DB_SOURCE.account_record_id = LS_DB_SOURCE_TMP.account_record_id,LS_DB_SOURCE.account_id = LS_DB_SOURCE_TMP.account_id,
LS_DB_SOURCE.PROCESSING_DT = LS_DB_SOURCE_TMP.PROCESSING_DT ,
LS_DB_SOURCE.receipt_id     =LS_DB_SOURCE_TMP.receipt_id        ,
LS_DB_SOURCE.case_no        =LS_DB_SOURCE_TMP.case_no           ,
LS_DB_SOURCE.case_version   =LS_DB_SOURCE_TMP.case_version      ,
LS_DB_SOURCE.version_no     =LS_DB_SOURCE_TMP.version_no        ,
LS_DB_SOURCE.expiry_date    =LS_DB_SOURCE_TMP.expiry_date       ,
LS_DB_SOURCE.load_ts        =LS_DB_SOURCE_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP 
WHERE 	LS_DB_SOURCE.INTEGRATION_ID = LS_DB_SOURCE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SOURCE_TMP.PROCESSING_DT = LS_DB_SOURCE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
uuid,
user_modified,
user_created,
spr_id,
source_de_ml,
source_auto_flag,
source_aer_number,
source,
report_source,
reference_type_de_ml,
reference_type,
record_id,
primary_source_flag_de_ml,
primary_source_flag,
originating_account,
inq_rec_id,
identification_number,
follow_up_number,
fk_asr_rec_id,
ext_clob_fld,
entity_updated,
description,
date_received_format,
date_received,
date_modified,
date_created,
comp_rec_id,
ari_rec_id,
account_record_id,
account_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
uuid,
user_modified,
user_created,
spr_id,
source_de_ml,
source_auto_flag,
source_aer_number,
source,
report_source,
reference_type_de_ml,
reference_type,
record_id,
primary_source_flag_de_ml,
primary_source_flag,
originating_account,
inq_rec_id,
identification_number,
follow_up_number,
fk_asr_rec_id,
ext_clob_fld,
entity_updated,
description,
date_received_format,
date_received,
date_modified,
date_created,
comp_rec_id,
ari_rec_id,
account_record_id,
account_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP 
WHERE 	TO_DATE(LS_DB_SOURCE.PROCESSING_DT) < TO_DATE(LS_DB_SOURCE_TMP.PROCESSING_DT)
AND LS_DB_SOURCE.INTEGRATION_ID = LS_DB_SOURCE_TMP.INTEGRATION_ID
AND LS_DB_SOURCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_source')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DELETION_TMP  WHERE TABLE_NAME='lsmv_source')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SOURCE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_SOURCE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE'),
	'LSDB','Case','LS_DB_SOURCE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_SOURCE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
