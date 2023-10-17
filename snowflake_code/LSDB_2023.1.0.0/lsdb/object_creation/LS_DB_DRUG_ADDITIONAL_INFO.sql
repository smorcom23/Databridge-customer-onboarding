
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_DRUG_ADDITIONAL_INFO()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'),
	'LSRA','Case','LS_DB_DRUG_ADDITIONAL_INFO',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_ADDITIONAL_INFO' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_ADDITIONAL_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_ADDITIONAL_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_ADDITIONAL_INFO_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_ADDITIONAL_INFO_DELETION_TMP  As select RECORD_ID,'lsmv_product_additional_info' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_additional_info WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9063')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_additional_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_ADDITIONAL_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_ADDITIONAL_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_additional_info WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_ADDITIONAL_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_ADDITIONAL_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_product_additional_info_SUBSET AS 
(
select * from 
    (SELECT  
    additional_info_code  additional_info_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9063' AND CD=CAST(additional_info_code AS VARCHAR(100)) )additional_info_code_de_ml , date_created  date_created,date_modified  date_modified,fk_ari_rec_id  fk_ari_rec_id,fk_calllog_id  fk_calllog_id,fk_drug_rec_id  fk_drug_rec_id,inq_rec_id  inq_rec_id,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_additional_info
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_ADDITIONAL_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_additional_info')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_product_additional_info_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_product_additional_info_SUBSET.USER_CREATED CREATED_BY,lsmv_product_additional_info_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_product_additional_info_SUBSET.user_modified  ,lsmv_product_additional_info_SUBSET.user_created  ,lsmv_product_additional_info_SUBSET.spr_id  ,lsmv_product_additional_info_SUBSET.record_id  ,lsmv_product_additional_info_SUBSET.inq_rec_id  ,lsmv_product_additional_info_SUBSET.fk_drug_rec_id  ,lsmv_product_additional_info_SUBSET.fk_calllog_id  ,lsmv_product_additional_info_SUBSET.fk_ari_rec_id  ,lsmv_product_additional_info_SUBSET.date_modified  ,lsmv_product_additional_info_SUBSET.date_created  ,lsmv_product_additional_info_SUBSET.additional_info_code_de_ml  ,lsmv_product_additional_info_SUBSET.additional_info_code ,CONCAT(NVL(lsmv_product_additional_info_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_product_additional_info_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_product_additional_info_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP)
where target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO   
SET LS_DB_DRUG_ADDITIONAL_INFO.user_modified = LS_DB_DRUG_ADDITIONAL_INFO_TMP.user_modified,LS_DB_DRUG_ADDITIONAL_INFO.user_created = LS_DB_DRUG_ADDITIONAL_INFO_TMP.user_created,LS_DB_DRUG_ADDITIONAL_INFO.spr_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.spr_id,LS_DB_DRUG_ADDITIONAL_INFO.record_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.record_id,LS_DB_DRUG_ADDITIONAL_INFO.inq_rec_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.inq_rec_id,LS_DB_DRUG_ADDITIONAL_INFO.fk_drug_rec_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.fk_drug_rec_id,LS_DB_DRUG_ADDITIONAL_INFO.fk_calllog_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.fk_calllog_id,LS_DB_DRUG_ADDITIONAL_INFO.fk_ari_rec_id = LS_DB_DRUG_ADDITIONAL_INFO_TMP.fk_ari_rec_id,LS_DB_DRUG_ADDITIONAL_INFO.date_modified = LS_DB_DRUG_ADDITIONAL_INFO_TMP.date_modified,LS_DB_DRUG_ADDITIONAL_INFO.date_created = LS_DB_DRUG_ADDITIONAL_INFO_TMP.date_created,LS_DB_DRUG_ADDITIONAL_INFO.additional_info_code_de_ml = LS_DB_DRUG_ADDITIONAL_INFO_TMP.additional_info_code_de_ml,LS_DB_DRUG_ADDITIONAL_INFO.additional_info_code = LS_DB_DRUG_ADDITIONAL_INFO_TMP.additional_info_code,
LS_DB_DRUG_ADDITIONAL_INFO.PROCESSING_DT = LS_DB_DRUG_ADDITIONAL_INFO_TMP.PROCESSING_DT ,
LS_DB_DRUG_ADDITIONAL_INFO.receipt_id     =LS_DB_DRUG_ADDITIONAL_INFO_TMP.receipt_id        ,
LS_DB_DRUG_ADDITIONAL_INFO.case_no        =LS_DB_DRUG_ADDITIONAL_INFO_TMP.case_no           ,
LS_DB_DRUG_ADDITIONAL_INFO.case_version   =LS_DB_DRUG_ADDITIONAL_INFO_TMP.case_version      ,
LS_DB_DRUG_ADDITIONAL_INFO.version_no     =LS_DB_DRUG_ADDITIONAL_INFO_TMP.version_no        ,
LS_DB_DRUG_ADDITIONAL_INFO.expiry_date    =LS_DB_DRUG_ADDITIONAL_INFO_TMP.expiry_date       ,
LS_DB_DRUG_ADDITIONAL_INFO.load_ts        =LS_DB_DRUG_ADDITIONAL_INFO_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP 
WHERE 	LS_DB_DRUG_ADDITIONAL_INFO.INTEGRATION_ID = LS_DB_DRUG_ADDITIONAL_INFO_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_ADDITIONAL_INFO_TMP.PROCESSING_DT = LS_DB_DRUG_ADDITIONAL_INFO.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO
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
record_id,
inq_rec_id,
fk_drug_rec_id,
fk_calllog_id,
fk_ari_rec_id,
date_modified,
date_created,
additional_info_code_de_ml,
additional_info_code)
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
record_id,
inq_rec_id,
fk_drug_rec_id,
fk_calllog_id,
fk_ari_rec_id,
date_modified,
date_created,
additional_info_code_de_ml,
additional_info_code
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_ADDITIONAL_INFO.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_ADDITIONAL_INFO_TMP.PROCESSING_DT)
AND LS_DB_DRUG_ADDITIONAL_INFO.INTEGRATION_ID = LS_DB_DRUG_ADDITIONAL_INFO_TMP.INTEGRATION_ID
AND LS_DB_DRUG_ADDITIONAL_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_ADDITIONAL_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_additional_info')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DRUG_ADDITIONAL_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_additional_info')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DRUG_ADDITIONAL_INFO_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DRUG_ADDITIONAL_INFO Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG_ADDITIONAL_INFO'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DRUG_ADDITIONAL_INFO not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
