
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_DLIST()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DLIST'),
	'LSRA','Case','LS_DB_DLIST',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DLIST' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DLIST'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_DLIST'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  As select RECORD_ID,'lsmv_dlist_master' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_master WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_dlist_queue' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_queue WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, fk_aim_rec_id common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_aim_rec_id record_id, fk_aim_rec_id common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_queue WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, fk_aim_rec_id common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_master WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_aim_rec_id record_id, fk_aim_rec_id common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_master WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_DLIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_dlist_queue_SUBSET AS 
(
select * from 
    (SELECT  
    blinded  dlistqueue_blinded,date_created  dlistqueue_date_created,date_modified  dlistqueue_date_modified,doc_format  dlistqueue_doc_format,doc_id  dlistqueue_doc_id,fk_aim_rec_id  dlistqueue_fk_aim_rec_id,fk_lpd_rec_id  dlistqueue_fk_lpd_rec_id,lock_status  dlistqueue_lock_status,masked  dlistqueue_masked,record_id  dlistqueue_record_id,spr_id  dlistqueue_spr_id,user_created  dlistqueue_user_created,user_modified  dlistqueue_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_queue
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_aim_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_queue')
  ) where REC_RANK=1 )
  , lsmv_dlist_master_SUBSET AS 
(
select * from 
    (SELECT  
    aer_no  dlistmaster_aer_no,aer_version_no  dlistmaster_aer_version_no,app_req_id  dlistmaster_app_req_id,date_created  dlistmaster_date_created,date_modified  dlistmaster_date_modified,fk_aim_rec_id  dlistmaster_fk_aim_rec_id,is_archived  dlistmaster_is_archived,is_distributed  dlistmaster_is_distributed,is_literature  dlistmaster_is_literature,lock_status  dlistmaster_lock_status,mhlw_report_type  dlistmaster_mhlw_report_type,only_deviated  dlistmaster_only_deviated,receipt_no  dlistmaster_receipt_no,record_id  dlistmaster_record_id,spr_id  dlistmaster_spr_id,status  dlistmaster_status,user_created  dlistmaster_user_created,user_modified  dlistmaster_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_dlist_master
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_aim_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_master')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_dlist_master_SUBSET.dlistmaster_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_dlist_queue_SUBSET.dlistqueue_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_dlist_queue_SUBSET.dlistqueue_USER_MODIFIED USER_MODIFIED,lsmv_dlist_queue_SUBSET.dlistqueue_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_dlist_queue_SUBSET.dlistqueue_USER_CREATED CREATED_BY,lsmv_dlist_queue_SUBSET.dlistqueue_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_dlist_queue_SUBSET.dlistqueue_user_modified  ,lsmv_dlist_queue_SUBSET.dlistqueue_user_created  ,lsmv_dlist_queue_SUBSET.dlistqueue_spr_id  ,lsmv_dlist_queue_SUBSET.dlistqueue_record_id  ,lsmv_dlist_queue_SUBSET.dlistqueue_masked  ,lsmv_dlist_queue_SUBSET.dlistqueue_lock_status  ,lsmv_dlist_queue_SUBSET.dlistqueue_fk_lpd_rec_id  ,lsmv_dlist_queue_SUBSET.dlistqueue_fk_aim_rec_id  ,lsmv_dlist_queue_SUBSET.dlistqueue_doc_id  ,lsmv_dlist_queue_SUBSET.dlistqueue_doc_format  ,lsmv_dlist_queue_SUBSET.dlistqueue_date_modified  ,lsmv_dlist_queue_SUBSET.dlistqueue_date_created  ,lsmv_dlist_queue_SUBSET.dlistqueue_blinded  ,lsmv_dlist_master_SUBSET.dlistmaster_user_modified  ,lsmv_dlist_master_SUBSET.dlistmaster_user_created  ,lsmv_dlist_master_SUBSET.dlistmaster_status  ,lsmv_dlist_master_SUBSET.dlistmaster_spr_id  ,lsmv_dlist_master_SUBSET.dlistmaster_record_id  ,lsmv_dlist_master_SUBSET.dlistmaster_receipt_no  ,lsmv_dlist_master_SUBSET.dlistmaster_only_deviated  ,lsmv_dlist_master_SUBSET.dlistmaster_mhlw_report_type  ,lsmv_dlist_master_SUBSET.dlistmaster_lock_status  ,lsmv_dlist_master_SUBSET.dlistmaster_is_literature  ,lsmv_dlist_master_SUBSET.dlistmaster_is_distributed  ,lsmv_dlist_master_SUBSET.dlistmaster_is_archived  ,lsmv_dlist_master_SUBSET.dlistmaster_fk_aim_rec_id  ,lsmv_dlist_master_SUBSET.dlistmaster_date_modified  ,lsmv_dlist_master_SUBSET.dlistmaster_date_created  ,lsmv_dlist_master_SUBSET.dlistmaster_app_req_id  ,lsmv_dlist_master_SUBSET.dlistmaster_aer_version_no  ,lsmv_dlist_master_SUBSET.dlistmaster_aer_no ,CONCAT(NVL(lsmv_dlist_master_SUBSET.dlistmaster_RECORD_ID,-1),'||',NVL(lsmv_dlist_queue_SUBSET.dlistqueue_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_dlist_queue_SUBSET  FULL OUTER JOIN lsmv_dlist_master_SUBSET ON lsmv_dlist_queue_SUBSET.dlistqueue_fk_aim_rec_id=lsmv_dlist_master_SUBSET.dlistmaster_fk_aim_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP)
where target_table_name='LS_DB_DLIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DLIST'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST   
SET LS_DB_DLIST.dlistqueue_user_modified = LS_DB_DLIST_TMP.dlistqueue_user_modified,LS_DB_DLIST.dlistqueue_user_created = LS_DB_DLIST_TMP.dlistqueue_user_created,LS_DB_DLIST.dlistqueue_spr_id = LS_DB_DLIST_TMP.dlistqueue_spr_id,LS_DB_DLIST.dlistqueue_record_id = LS_DB_DLIST_TMP.dlistqueue_record_id,LS_DB_DLIST.dlistqueue_masked = LS_DB_DLIST_TMP.dlistqueue_masked,LS_DB_DLIST.dlistqueue_lock_status = LS_DB_DLIST_TMP.dlistqueue_lock_status,LS_DB_DLIST.dlistqueue_fk_lpd_rec_id = LS_DB_DLIST_TMP.dlistqueue_fk_lpd_rec_id,LS_DB_DLIST.dlistqueue_fk_aim_rec_id = LS_DB_DLIST_TMP.dlistqueue_fk_aim_rec_id,LS_DB_DLIST.dlistqueue_doc_id = LS_DB_DLIST_TMP.dlistqueue_doc_id,LS_DB_DLIST.dlistqueue_doc_format = LS_DB_DLIST_TMP.dlistqueue_doc_format,LS_DB_DLIST.dlistqueue_date_modified = LS_DB_DLIST_TMP.dlistqueue_date_modified,LS_DB_DLIST.dlistqueue_date_created = LS_DB_DLIST_TMP.dlistqueue_date_created,LS_DB_DLIST.dlistqueue_blinded = LS_DB_DLIST_TMP.dlistqueue_blinded,LS_DB_DLIST.dlistmaster_user_modified = LS_DB_DLIST_TMP.dlistmaster_user_modified,LS_DB_DLIST.dlistmaster_user_created = LS_DB_DLIST_TMP.dlistmaster_user_created,LS_DB_DLIST.dlistmaster_status = LS_DB_DLIST_TMP.dlistmaster_status,LS_DB_DLIST.dlistmaster_spr_id = LS_DB_DLIST_TMP.dlistmaster_spr_id,LS_DB_DLIST.dlistmaster_record_id = LS_DB_DLIST_TMP.dlistmaster_record_id,LS_DB_DLIST.dlistmaster_receipt_no = LS_DB_DLIST_TMP.dlistmaster_receipt_no,LS_DB_DLIST.dlistmaster_only_deviated = LS_DB_DLIST_TMP.dlistmaster_only_deviated,LS_DB_DLIST.dlistmaster_mhlw_report_type = LS_DB_DLIST_TMP.dlistmaster_mhlw_report_type,LS_DB_DLIST.dlistmaster_lock_status = LS_DB_DLIST_TMP.dlistmaster_lock_status,LS_DB_DLIST.dlistmaster_is_literature = LS_DB_DLIST_TMP.dlistmaster_is_literature,LS_DB_DLIST.dlistmaster_is_distributed = LS_DB_DLIST_TMP.dlistmaster_is_distributed,LS_DB_DLIST.dlistmaster_is_archived = LS_DB_DLIST_TMP.dlistmaster_is_archived,LS_DB_DLIST.dlistmaster_fk_aim_rec_id = LS_DB_DLIST_TMP.dlistmaster_fk_aim_rec_id,LS_DB_DLIST.dlistmaster_date_modified = LS_DB_DLIST_TMP.dlistmaster_date_modified,LS_DB_DLIST.dlistmaster_date_created = LS_DB_DLIST_TMP.dlistmaster_date_created,LS_DB_DLIST.dlistmaster_app_req_id = LS_DB_DLIST_TMP.dlistmaster_app_req_id,LS_DB_DLIST.dlistmaster_aer_version_no = LS_DB_DLIST_TMP.dlistmaster_aer_version_no,LS_DB_DLIST.dlistmaster_aer_no = LS_DB_DLIST_TMP.dlistmaster_aer_no,
LS_DB_DLIST.PROCESSING_DT = LS_DB_DLIST_TMP.PROCESSING_DT,
LS_DB_DLIST.user_modified  =LS_DB_DLIST_TMP.user_modified     ,
LS_DB_DLIST.date_modified  =LS_DB_DLIST_TMP.date_modified     ,
LS_DB_DLIST.expiry_date    =LS_DB_DLIST_TMP.expiry_date       ,
LS_DB_DLIST.created_by     =LS_DB_DLIST_TMP.created_by        ,
LS_DB_DLIST.created_dt     =LS_DB_DLIST_TMP.created_dt        ,
LS_DB_DLIST.load_ts        =LS_DB_DLIST_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP 
WHERE 	LS_DB_DLIST.INTEGRATION_ID = LS_DB_DLIST_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DLIST_TMP.PROCESSING_DT = LS_DB_DLIST.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DLIST.dlistmaster_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DLIST.dlistqueue_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DLIST_TMP.dlistmaster_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DLIST_TMP.dlistqueue_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DLIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DLIST.dlistqueue_RECORD_ID ,LS_DB_DLIST.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP 
ON LS_DB_DLIST.dlistqueue_RECORD_ID=LS_DB_DLIST_TMP.dlistqueue_RECORD_ID
AND LS_DB_DLIST.INTEGRATION_ID = LS_DB_DLIST_TMP.INTEGRATION_ID 
where LS_DB_DLIST_TMP.INTEGRATION_ID  is null AND LS_DB_DLIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DLIST.dlistqueue_RECORD_ID in (select dlistqueue_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP )
) TMP where LS_DB_DLIST.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DLIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DLIST.dlistqueue_RECORD_ID ,LS_DB_DLIST.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP 
    ON LS_DB_DLIST.dlistqueue_RECORD_ID=LS_DB_DLIST_TMP.dlistqueue_RECORD_ID
    AND LS_DB_DLIST.INTEGRATION_ID = LS_DB_DLIST_TMP.INTEGRATION_ID 
    where LS_DB_DLIST_TMP.INTEGRATION_ID  is null AND LS_DB_DLIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DLIST.dlistqueue_RECORD_ID in (select dlistqueue_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,dlistqueue_user_modified,
dlistqueue_user_created,
dlistqueue_spr_id,
dlistqueue_record_id,
dlistqueue_masked,
dlistqueue_lock_status,
dlistqueue_fk_lpd_rec_id,
dlistqueue_fk_aim_rec_id,
dlistqueue_doc_id,
dlistqueue_doc_format,
dlistqueue_date_modified,
dlistqueue_date_created,
dlistqueue_blinded,
dlistmaster_user_modified,
dlistmaster_user_created,
dlistmaster_status,
dlistmaster_spr_id,
dlistmaster_record_id,
dlistmaster_receipt_no,
dlistmaster_only_deviated,
dlistmaster_mhlw_report_type,
dlistmaster_lock_status,
dlistmaster_is_literature,
dlistmaster_is_distributed,
dlistmaster_is_archived,
dlistmaster_fk_aim_rec_id,
dlistmaster_date_modified,
dlistmaster_date_created,
dlistmaster_app_req_id,
dlistmaster_aer_version_no,
dlistmaster_aer_no)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,dlistqueue_user_modified,
dlistqueue_user_created,
dlistqueue_spr_id,
dlistqueue_record_id,
dlistqueue_masked,
dlistqueue_lock_status,
dlistqueue_fk_lpd_rec_id,
dlistqueue_fk_aim_rec_id,
dlistqueue_doc_id,
dlistqueue_doc_format,
dlistqueue_date_modified,
dlistqueue_date_created,
dlistqueue_blinded,
dlistmaster_user_modified,
dlistmaster_user_created,
dlistmaster_status,
dlistmaster_spr_id,
dlistmaster_record_id,
dlistmaster_receipt_no,
dlistmaster_only_deviated,
dlistmaster_mhlw_report_type,
dlistmaster_lock_status,
dlistmaster_is_literature,
dlistmaster_is_distributed,
dlistmaster_is_archived,
dlistmaster_fk_aim_rec_id,
dlistmaster_date_modified,
dlistmaster_date_created,
dlistmaster_app_req_id,
dlistmaster_aer_version_no,
dlistmaster_aer_no
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST TGT
WHERE  ( dlistmaster_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_master') OR dlistqueue_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_queue')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP 
WHERE 	TO_DATE(LS_DB_DLIST.PROCESSING_DT) < TO_DATE(LS_DB_DLIST_TMP.PROCESSING_DT)
AND LS_DB_DLIST.INTEGRATION_ID = LS_DB_DLIST_TMP.INTEGRATION_ID
AND nvl(LS_DB_DLIST.dlistqueue_RECORD_ID,-1) = nvl(LS_DB_DLIST_TMP.dlistqueue_RECORD_ID,-1)
AND LS_DB_DLIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DLIST.dlistmaster_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DLIST.dlistqueue_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DLIST_TMP.dlistmaster_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DLIST_TMP.dlistqueue_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( dlistmaster_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_master') OR dlistqueue_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_DLIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_dlist_queue')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DLIST_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DLIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DLIST'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_DLIST Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DLIST'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_DLIST not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
