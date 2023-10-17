
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE_DOCS()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE_DOCS'),
	'LSDB','Case','LS_DB_SOURCE_DOCS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_SOURCE_DOCS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SOURCE_DOCS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DOCS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DOCS_DELETION_TMP  As select RECORD_ID,'lsmv_aer_source_docs' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_aer_source_docs WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('316')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_aer_source_docs WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SOURCE_DOCS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_aer_source_docs WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_SOURCE_DOCS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_aer_source_docs_SUBSET AS 
(
select * from 
    (SELECT  
    ack_doc_id  ack_doc_id,ari_rec_id  ari_rec_id,attached_by  attached_by,bypass_aer_wm  bypass_aer_wm,category_code  category_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='316' AND CD=CAST(category_code AS VARCHAR(100)) )category_code_de_ml , checkflag  checkflag,checkflagfordisplay  checkflagfordisplay,checkout_status  checkout_status,comp_rec_id  comp_rec_id,data_entry_site_code  data_entry_site_code,date_attached  date_attached,date_created  date_created,date_modified  date_modified,doc_attached_type  doc_attached_type,doc_comment  doc_comment,doc_description  doc_description,doc_id  doc_id,doc_name  doc_name,doc_size  doc_size,doc_type  doc_type,doc_user  doc_user,fk_aim_rec_id  fk_aim_rec_id,from_weblink  from_weblink,included  included,included_document  included_document,inq_rec_id  inq_rec_id,is_local  is_local,literature_doc  literature_doc,local_submission_required  local_submission_required,ocr_applied  ocr_applied,ocr_json  ocr_json,ocr_src_doc_page_det  ocr_src_doc_page_det,read_flag  read_flag,record_id  record_id,source_doc_type  source_doc_type,spr_id  spr_id,user_created  user_created,user_modified  user_modified,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_aer_source_docs
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DOCS_DELETION_TMP  WHERE TABLE_NAME='lsmv_aer_source_docs')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_aer_source_docs_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_aer_source_docs_SUBSET.USER_CREATED CREATED_BY,lsmv_aer_source_docs_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_aer_source_docs_SUBSET.version  ,lsmv_aer_source_docs_SUBSET.user_modified  ,lsmv_aer_source_docs_SUBSET.user_created  ,lsmv_aer_source_docs_SUBSET.spr_id  ,lsmv_aer_source_docs_SUBSET.source_doc_type  ,lsmv_aer_source_docs_SUBSET.record_id  ,lsmv_aer_source_docs_SUBSET.read_flag  ,lsmv_aer_source_docs_SUBSET.ocr_src_doc_page_det  ,lsmv_aer_source_docs_SUBSET.ocr_json  ,lsmv_aer_source_docs_SUBSET.ocr_applied  ,lsmv_aer_source_docs_SUBSET.local_submission_required  ,lsmv_aer_source_docs_SUBSET.literature_doc  ,lsmv_aer_source_docs_SUBSET.is_local  ,lsmv_aer_source_docs_SUBSET.inq_rec_id  ,lsmv_aer_source_docs_SUBSET.included_document  ,lsmv_aer_source_docs_SUBSET.included  ,lsmv_aer_source_docs_SUBSET.from_weblink  ,lsmv_aer_source_docs_SUBSET.fk_aim_rec_id  ,lsmv_aer_source_docs_SUBSET.doc_user  ,lsmv_aer_source_docs_SUBSET.doc_type  ,lsmv_aer_source_docs_SUBSET.doc_size  ,lsmv_aer_source_docs_SUBSET.doc_name  ,lsmv_aer_source_docs_SUBSET.doc_id  ,lsmv_aer_source_docs_SUBSET.doc_description  ,lsmv_aer_source_docs_SUBSET.doc_comment  ,lsmv_aer_source_docs_SUBSET.doc_attached_type  ,lsmv_aer_source_docs_SUBSET.date_modified  ,lsmv_aer_source_docs_SUBSET.date_created  ,lsmv_aer_source_docs_SUBSET.date_attached  ,lsmv_aer_source_docs_SUBSET.data_entry_site_code  ,lsmv_aer_source_docs_SUBSET.comp_rec_id  ,lsmv_aer_source_docs_SUBSET.checkout_status  ,lsmv_aer_source_docs_SUBSET.checkflagfordisplay  ,lsmv_aer_source_docs_SUBSET.checkflag  ,lsmv_aer_source_docs_SUBSET.category_code_de_ml  ,lsmv_aer_source_docs_SUBSET.category_code  ,lsmv_aer_source_docs_SUBSET.bypass_aer_wm  ,lsmv_aer_source_docs_SUBSET.attached_by  ,lsmv_aer_source_docs_SUBSET.ari_rec_id  ,lsmv_aer_source_docs_SUBSET.ack_doc_id ,CONCAT(NVL(lsmv_aer_source_docs_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_aer_source_docs_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_aer_source_docs_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP)
where target_table_name='LS_DB_SOURCE_DOCS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS   
SET LS_DB_SOURCE_DOCS.version = LS_DB_SOURCE_DOCS_TMP.version,LS_DB_SOURCE_DOCS.user_modified = LS_DB_SOURCE_DOCS_TMP.user_modified,LS_DB_SOURCE_DOCS.user_created = LS_DB_SOURCE_DOCS_TMP.user_created,LS_DB_SOURCE_DOCS.spr_id = LS_DB_SOURCE_DOCS_TMP.spr_id,LS_DB_SOURCE_DOCS.source_doc_type = LS_DB_SOURCE_DOCS_TMP.source_doc_type,LS_DB_SOURCE_DOCS.record_id = LS_DB_SOURCE_DOCS_TMP.record_id,LS_DB_SOURCE_DOCS.read_flag = LS_DB_SOURCE_DOCS_TMP.read_flag,LS_DB_SOURCE_DOCS.ocr_src_doc_page_det = LS_DB_SOURCE_DOCS_TMP.ocr_src_doc_page_det,LS_DB_SOURCE_DOCS.ocr_json = LS_DB_SOURCE_DOCS_TMP.ocr_json,LS_DB_SOURCE_DOCS.ocr_applied = LS_DB_SOURCE_DOCS_TMP.ocr_applied,LS_DB_SOURCE_DOCS.local_submission_required = LS_DB_SOURCE_DOCS_TMP.local_submission_required,LS_DB_SOURCE_DOCS.literature_doc = LS_DB_SOURCE_DOCS_TMP.literature_doc,LS_DB_SOURCE_DOCS.is_local = LS_DB_SOURCE_DOCS_TMP.is_local,LS_DB_SOURCE_DOCS.inq_rec_id = LS_DB_SOURCE_DOCS_TMP.inq_rec_id,LS_DB_SOURCE_DOCS.included_document = LS_DB_SOURCE_DOCS_TMP.included_document,LS_DB_SOURCE_DOCS.included = LS_DB_SOURCE_DOCS_TMP.included,LS_DB_SOURCE_DOCS.from_weblink = LS_DB_SOURCE_DOCS_TMP.from_weblink,LS_DB_SOURCE_DOCS.fk_aim_rec_id = LS_DB_SOURCE_DOCS_TMP.fk_aim_rec_id,LS_DB_SOURCE_DOCS.doc_user = LS_DB_SOURCE_DOCS_TMP.doc_user,LS_DB_SOURCE_DOCS.doc_type = LS_DB_SOURCE_DOCS_TMP.doc_type,LS_DB_SOURCE_DOCS.doc_size = LS_DB_SOURCE_DOCS_TMP.doc_size,LS_DB_SOURCE_DOCS.doc_name = LS_DB_SOURCE_DOCS_TMP.doc_name,LS_DB_SOURCE_DOCS.doc_id = LS_DB_SOURCE_DOCS_TMP.doc_id,LS_DB_SOURCE_DOCS.doc_description = LS_DB_SOURCE_DOCS_TMP.doc_description,LS_DB_SOURCE_DOCS.doc_comment = LS_DB_SOURCE_DOCS_TMP.doc_comment,LS_DB_SOURCE_DOCS.doc_attached_type = LS_DB_SOURCE_DOCS_TMP.doc_attached_type,LS_DB_SOURCE_DOCS.date_modified = LS_DB_SOURCE_DOCS_TMP.date_modified,LS_DB_SOURCE_DOCS.date_created = LS_DB_SOURCE_DOCS_TMP.date_created,LS_DB_SOURCE_DOCS.date_attached = LS_DB_SOURCE_DOCS_TMP.date_attached,LS_DB_SOURCE_DOCS.data_entry_site_code = LS_DB_SOURCE_DOCS_TMP.data_entry_site_code,LS_DB_SOURCE_DOCS.comp_rec_id = LS_DB_SOURCE_DOCS_TMP.comp_rec_id,LS_DB_SOURCE_DOCS.checkout_status = LS_DB_SOURCE_DOCS_TMP.checkout_status,LS_DB_SOURCE_DOCS.checkflagfordisplay = LS_DB_SOURCE_DOCS_TMP.checkflagfordisplay,LS_DB_SOURCE_DOCS.checkflag = LS_DB_SOURCE_DOCS_TMP.checkflag,LS_DB_SOURCE_DOCS.category_code_de_ml = LS_DB_SOURCE_DOCS_TMP.category_code_de_ml,LS_DB_SOURCE_DOCS.category_code = LS_DB_SOURCE_DOCS_TMP.category_code,LS_DB_SOURCE_DOCS.bypass_aer_wm = LS_DB_SOURCE_DOCS_TMP.bypass_aer_wm,LS_DB_SOURCE_DOCS.attached_by = LS_DB_SOURCE_DOCS_TMP.attached_by,LS_DB_SOURCE_DOCS.ari_rec_id = LS_DB_SOURCE_DOCS_TMP.ari_rec_id,LS_DB_SOURCE_DOCS.ack_doc_id = LS_DB_SOURCE_DOCS_TMP.ack_doc_id,
LS_DB_SOURCE_DOCS.PROCESSING_DT = LS_DB_SOURCE_DOCS_TMP.PROCESSING_DT ,
LS_DB_SOURCE_DOCS.receipt_id     =LS_DB_SOURCE_DOCS_TMP.receipt_id        ,
LS_DB_SOURCE_DOCS.case_no        =LS_DB_SOURCE_DOCS_TMP.case_no           ,
LS_DB_SOURCE_DOCS.case_version   =LS_DB_SOURCE_DOCS_TMP.case_version      ,
LS_DB_SOURCE_DOCS.version_no     =LS_DB_SOURCE_DOCS_TMP.version_no        ,
LS_DB_SOURCE_DOCS.expiry_date    =LS_DB_SOURCE_DOCS_TMP.expiry_date       ,
LS_DB_SOURCE_DOCS.load_ts        =LS_DB_SOURCE_DOCS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP 
WHERE 	LS_DB_SOURCE_DOCS.INTEGRATION_ID = LS_DB_SOURCE_DOCS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SOURCE_DOCS_TMP.PROCESSING_DT = LS_DB_SOURCE_DOCS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
spr_id,
source_doc_type,
record_id,
read_flag,
ocr_src_doc_page_det,
ocr_json,
ocr_applied,
local_submission_required,
literature_doc,
is_local,
inq_rec_id,
included_document,
included,
from_weblink,
fk_aim_rec_id,
doc_user,
doc_type,
doc_size,
doc_name,
doc_id,
doc_description,
doc_comment,
doc_attached_type,
date_modified,
date_created,
date_attached,
data_entry_site_code,
comp_rec_id,
checkout_status,
checkflagfordisplay,
checkflag,
category_code_de_ml,
category_code,
bypass_aer_wm,
attached_by,
ari_rec_id,
ack_doc_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
spr_id,
source_doc_type,
record_id,
read_flag,
ocr_src_doc_page_det,
ocr_json,
ocr_applied,
local_submission_required,
literature_doc,
is_local,
inq_rec_id,
included_document,
included,
from_weblink,
fk_aim_rec_id,
doc_user,
doc_type,
doc_size,
doc_name,
doc_id,
doc_description,
doc_comment,
doc_attached_type,
date_modified,
date_created,
date_attached,
data_entry_site_code,
comp_rec_id,
checkout_status,
checkflagfordisplay,
checkflag,
category_code_de_ml,
category_code,
bypass_aer_wm,
attached_by,
ari_rec_id,
ack_doc_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP 
WHERE 	TO_DATE(LS_DB_SOURCE_DOCS.PROCESSING_DT) < TO_DATE(LS_DB_SOURCE_DOCS_TMP.PROCESSING_DT)
AND LS_DB_SOURCE_DOCS.INTEGRATION_ID = LS_DB_SOURCE_DOCS_TMP.INTEGRATION_ID
AND LS_DB_SOURCE_DOCS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DOCS_DELETION_TMP  WHERE TABLE_NAME='lsmv_aer_source_docs')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_SOURCE_DOCS_DELETION_TMP  WHERE TABLE_NAME='lsmv_aer_source_docs')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_DOCS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SOURCE_DOCS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_SOURCE_DOCS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE_DOCS'),
	'LSDB','Case','LS_DB_SOURCE_DOCS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_SOURCE_DOCS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
