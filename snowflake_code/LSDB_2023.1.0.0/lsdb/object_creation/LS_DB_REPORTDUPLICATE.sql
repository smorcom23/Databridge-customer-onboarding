
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_REPORTDUPLICATE()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTDUPLICATE'),
	'LSRA','Case','LS_DB_REPORTDUPLICATE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_REPORTDUPLICATE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTDUPLICATE'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTDUPLICATE'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTDUPLICATE_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTDUPLICATE_DELETION_TMP  As select RECORD_ID,'lsmv_reportduplicate' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reportduplicate WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reportduplicate WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTDUPLICATE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTDUPLICATE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reportduplicate WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTDUPLICATE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REPORTDUPLICATE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_reportduplicate_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,duplicatenumb  duplicatenumb,duplicatenumb_lang  duplicatenumb_lang,duplicatesource  duplicatesource,duplicatesource_lang  duplicatesource_lang,e2b_r3_link_report_no  e2b_r3_link_report_no,entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fk_asr_rec_id  fk_asr_rec_id,inq_rec_id  inq_rec_id,previously_reported  previously_reported,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(previously_reported AS VARCHAR(100)) )previously_reported_de_ml , previously_reported_nf  previously_reported_nf,record_id  record_id,reportduplicate_lang  reportduplicate_lang,spr_id  spr_id,user_created  user_created,user_modified  user_modified,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_reportduplicate
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTDUPLICATE_DELETION_TMP  WHERE TABLE_NAME='lsmv_reportduplicate')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_reportduplicate_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_reportduplicate_SUBSET.USER_CREATED CREATED_BY,lsmv_reportduplicate_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_reportduplicate_SUBSET.version  ,lsmv_reportduplicate_SUBSET.user_modified  ,lsmv_reportduplicate_SUBSET.user_created  ,lsmv_reportduplicate_SUBSET.spr_id  ,lsmv_reportduplicate_SUBSET.reportduplicate_lang  ,lsmv_reportduplicate_SUBSET.record_id  ,lsmv_reportduplicate_SUBSET.previously_reported_nf  ,lsmv_reportduplicate_SUBSET.previously_reported_de_ml  ,lsmv_reportduplicate_SUBSET.previously_reported  ,lsmv_reportduplicate_SUBSET.inq_rec_id  ,lsmv_reportduplicate_SUBSET.fk_asr_rec_id  ,lsmv_reportduplicate_SUBSET.ext_clob_fld  ,lsmv_reportduplicate_SUBSET.entity_updated  ,lsmv_reportduplicate_SUBSET.e2b_r3_link_report_no  ,lsmv_reportduplicate_SUBSET.duplicatesource_lang  ,lsmv_reportduplicate_SUBSET.duplicatesource  ,lsmv_reportduplicate_SUBSET.duplicatenumb_lang  ,lsmv_reportduplicate_SUBSET.duplicatenumb  ,lsmv_reportduplicate_SUBSET.date_modified  ,lsmv_reportduplicate_SUBSET.date_created  ,lsmv_reportduplicate_SUBSET.comp_rec_id  ,lsmv_reportduplicate_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_reportduplicate_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_reportduplicate_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_reportduplicate_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP)
where target_table_name='LS_DB_REPORTDUPLICATE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTDUPLICATE'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE   
SET LS_DB_REPORTDUPLICATE.version = LS_DB_REPORTDUPLICATE_TMP.version,LS_DB_REPORTDUPLICATE.user_modified = LS_DB_REPORTDUPLICATE_TMP.user_modified,LS_DB_REPORTDUPLICATE.user_created = LS_DB_REPORTDUPLICATE_TMP.user_created,LS_DB_REPORTDUPLICATE.spr_id = LS_DB_REPORTDUPLICATE_TMP.spr_id,LS_DB_REPORTDUPLICATE.reportduplicate_lang = LS_DB_REPORTDUPLICATE_TMP.reportduplicate_lang,LS_DB_REPORTDUPLICATE.record_id = LS_DB_REPORTDUPLICATE_TMP.record_id,LS_DB_REPORTDUPLICATE.previously_reported_nf = LS_DB_REPORTDUPLICATE_TMP.previously_reported_nf,LS_DB_REPORTDUPLICATE.previously_reported_de_ml = LS_DB_REPORTDUPLICATE_TMP.previously_reported_de_ml,LS_DB_REPORTDUPLICATE.previously_reported = LS_DB_REPORTDUPLICATE_TMP.previously_reported,LS_DB_REPORTDUPLICATE.inq_rec_id = LS_DB_REPORTDUPLICATE_TMP.inq_rec_id,LS_DB_REPORTDUPLICATE.fk_asr_rec_id = LS_DB_REPORTDUPLICATE_TMP.fk_asr_rec_id,LS_DB_REPORTDUPLICATE.ext_clob_fld = LS_DB_REPORTDUPLICATE_TMP.ext_clob_fld,LS_DB_REPORTDUPLICATE.entity_updated = LS_DB_REPORTDUPLICATE_TMP.entity_updated,LS_DB_REPORTDUPLICATE.e2b_r3_link_report_no = LS_DB_REPORTDUPLICATE_TMP.e2b_r3_link_report_no,LS_DB_REPORTDUPLICATE.duplicatesource_lang = LS_DB_REPORTDUPLICATE_TMP.duplicatesource_lang,LS_DB_REPORTDUPLICATE.duplicatesource = LS_DB_REPORTDUPLICATE_TMP.duplicatesource,LS_DB_REPORTDUPLICATE.duplicatenumb_lang = LS_DB_REPORTDUPLICATE_TMP.duplicatenumb_lang,LS_DB_REPORTDUPLICATE.duplicatenumb = LS_DB_REPORTDUPLICATE_TMP.duplicatenumb,LS_DB_REPORTDUPLICATE.date_modified = LS_DB_REPORTDUPLICATE_TMP.date_modified,LS_DB_REPORTDUPLICATE.date_created = LS_DB_REPORTDUPLICATE_TMP.date_created,LS_DB_REPORTDUPLICATE.comp_rec_id = LS_DB_REPORTDUPLICATE_TMP.comp_rec_id,LS_DB_REPORTDUPLICATE.ari_rec_id = LS_DB_REPORTDUPLICATE_TMP.ari_rec_id,
LS_DB_REPORTDUPLICATE.PROCESSING_DT = LS_DB_REPORTDUPLICATE_TMP.PROCESSING_DT ,
LS_DB_REPORTDUPLICATE.receipt_id     =LS_DB_REPORTDUPLICATE_TMP.receipt_id        ,
LS_DB_REPORTDUPLICATE.case_no        =LS_DB_REPORTDUPLICATE_TMP.case_no           ,
LS_DB_REPORTDUPLICATE.case_version   =LS_DB_REPORTDUPLICATE_TMP.case_version      ,
LS_DB_REPORTDUPLICATE.version_no     =LS_DB_REPORTDUPLICATE_TMP.version_no        ,
LS_DB_REPORTDUPLICATE.expiry_date    =LS_DB_REPORTDUPLICATE_TMP.expiry_date       ,
LS_DB_REPORTDUPLICATE.load_ts        =LS_DB_REPORTDUPLICATE_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP 
WHERE 	LS_DB_REPORTDUPLICATE.INTEGRATION_ID = LS_DB_REPORTDUPLICATE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_REPORTDUPLICATE_TMP.PROCESSING_DT = LS_DB_REPORTDUPLICATE.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE
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
reportduplicate_lang,
record_id,
previously_reported_nf,
previously_reported_de_ml,
previously_reported,
inq_rec_id,
fk_asr_rec_id,
ext_clob_fld,
entity_updated,
e2b_r3_link_report_no,
duplicatesource_lang,
duplicatesource,
duplicatenumb_lang,
duplicatenumb,
date_modified,
date_created,
comp_rec_id,
ari_rec_id)
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
reportduplicate_lang,
record_id,
previously_reported_nf,
previously_reported_de_ml,
previously_reported,
inq_rec_id,
fk_asr_rec_id,
ext_clob_fld,
entity_updated,
e2b_r3_link_report_no,
duplicatesource_lang,
duplicatesource,
duplicatenumb_lang,
duplicatenumb,
date_modified,
date_created,
comp_rec_id,
ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP 
WHERE 	TO_DATE(LS_DB_REPORTDUPLICATE.PROCESSING_DT) < TO_DATE(LS_DB_REPORTDUPLICATE_TMP.PROCESSING_DT)
AND LS_DB_REPORTDUPLICATE.INTEGRATION_ID = LS_DB_REPORTDUPLICATE_TMP.INTEGRATION_ID
AND LS_DB_REPORTDUPLICATE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTDUPLICATE_DELETION_TMP  WHERE TABLE_NAME='lsmv_reportduplicate')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REPORTDUPLICATE_DELETION_TMP  WHERE TABLE_NAME='lsmv_reportduplicate')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REPORTDUPLICATE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REPORTDUPLICATE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTDUPLICATE'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_REPORTDUPLICATE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REPORTDUPLICATE'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_REPORTDUPLICATE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
