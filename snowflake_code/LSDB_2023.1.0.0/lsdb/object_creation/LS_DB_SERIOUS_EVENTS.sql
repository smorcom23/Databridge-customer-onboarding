
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SERIOUS_EVENTS()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SERIOUS_EVENTS'),
	'LSRA','Case','LS_DB_SERIOUS_EVENTS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SERIOUS_EVENTS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SERIOUS_EVENTS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_SERIOUS_EVENTS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  As select RECORD_ID,'lsmv_serious_events' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_serious_events_details' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events_details WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_lsmv_always_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SERIOUS_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_serious_events_details_SUBSET AS 
(
select * from 
    (SELECT  
    active  sereventdet_active,always_broad_narrow  sereventdet_always_broad_narrow,always_smqcmq_code  sereventdet_always_smqcmq_code,always_smqcmq_term  sereventdet_always_smqcmq_term,comments  sereventdet_comments,date_created  sereventdet_date_created,date_modified  sereventdet_date_modified,fk_lsmv_always_rec_id  sereventdet_fk_lsmv_always_rec_id,is_coded  sereventdet_is_coded,meddra_code  sereventdet_meddra_code,meddra_pt_term  sereventdet_meddra_pt_term,record_id  sereventdet_record_id,report_type  sereventdet_report_type,soc_name  sereventdet_soc_name,spr_id  sereventdet_spr_id,study_type  sereventdet_study_type,user_created  sereventdet_user_created,user_modified  sereventdet_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events_details
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_lsmv_always_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events_details')
  ) where REC_RANK=1 )
  , lsmv_serious_events_SUBSET AS 
(
select * from 
    (SELECT  
    active  serevent_active,date_created  serevent_date_created,date_modified  serevent_date_modified,meddra_name  serevent_meddra_name,meddra_version  serevent_meddra_version,record_id  serevent_record_id,spr_id  serevent_spr_id,user_created  serevent_user_created,user_modified  serevent_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_serious_events
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_serious_events_SUBSET.serevent_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_serious_events_details_SUBSET.sereventdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_serious_events_SUBSET.serevent_USER_MODIFIED USER_MODIFIED,lsmv_serious_events_SUBSET.serevent_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_serious_events_SUBSET.serevent_USER_CREATED CREATED_BY,lsmv_serious_events_SUBSET.serevent_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_serious_events_SUBSET.serevent_user_modified  ,lsmv_serious_events_SUBSET.serevent_user_created  ,lsmv_serious_events_SUBSET.serevent_spr_id  ,lsmv_serious_events_SUBSET.serevent_record_id  ,lsmv_serious_events_SUBSET.serevent_meddra_version  ,lsmv_serious_events_SUBSET.serevent_meddra_name  ,lsmv_serious_events_SUBSET.serevent_date_modified  ,lsmv_serious_events_SUBSET.serevent_date_created  ,lsmv_serious_events_SUBSET.serevent_active  ,lsmv_serious_events_details_SUBSET.sereventdet_user_modified  ,lsmv_serious_events_details_SUBSET.sereventdet_user_created  ,lsmv_serious_events_details_SUBSET.sereventdet_study_type  ,lsmv_serious_events_details_SUBSET.sereventdet_spr_id  ,lsmv_serious_events_details_SUBSET.sereventdet_soc_name  ,lsmv_serious_events_details_SUBSET.sereventdet_report_type  ,lsmv_serious_events_details_SUBSET.sereventdet_record_id  ,lsmv_serious_events_details_SUBSET.sereventdet_meddra_pt_term  ,lsmv_serious_events_details_SUBSET.sereventdet_meddra_code  ,lsmv_serious_events_details_SUBSET.sereventdet_is_coded  ,lsmv_serious_events_details_SUBSET.sereventdet_fk_lsmv_always_rec_id  ,lsmv_serious_events_details_SUBSET.sereventdet_date_modified  ,lsmv_serious_events_details_SUBSET.sereventdet_date_created  ,lsmv_serious_events_details_SUBSET.sereventdet_comments  ,lsmv_serious_events_details_SUBSET.sereventdet_always_smqcmq_term  ,lsmv_serious_events_details_SUBSET.sereventdet_always_smqcmq_code  ,lsmv_serious_events_details_SUBSET.sereventdet_always_broad_narrow  ,lsmv_serious_events_details_SUBSET.sereventdet_active ,CONCAT(NVL(lsmv_serious_events_SUBSET.serevent_RECORD_ID,-1),'||',NVL(lsmv_serious_events_details_SUBSET.sereventdet_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_serious_events_SUBSET  LEFT JOIN lsmv_serious_events_details_SUBSET ON lsmv_serious_events_SUBSET.serevent_record_id=lsmv_serious_events_details_SUBSET.sereventdet_fk_lsmv_always_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP)
where target_table_name='LS_DB_SERIOUS_EVENTS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SERIOUS_EVENTS'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS   
SET LS_DB_SERIOUS_EVENTS.serevent_user_modified = LS_DB_SERIOUS_EVENTS_TMP.serevent_user_modified,LS_DB_SERIOUS_EVENTS.serevent_user_created = LS_DB_SERIOUS_EVENTS_TMP.serevent_user_created,LS_DB_SERIOUS_EVENTS.serevent_spr_id = LS_DB_SERIOUS_EVENTS_TMP.serevent_spr_id,LS_DB_SERIOUS_EVENTS.serevent_record_id = LS_DB_SERIOUS_EVENTS_TMP.serevent_record_id,LS_DB_SERIOUS_EVENTS.serevent_meddra_version = LS_DB_SERIOUS_EVENTS_TMP.serevent_meddra_version,LS_DB_SERIOUS_EVENTS.serevent_meddra_name = LS_DB_SERIOUS_EVENTS_TMP.serevent_meddra_name,LS_DB_SERIOUS_EVENTS.serevent_date_modified = LS_DB_SERIOUS_EVENTS_TMP.serevent_date_modified,LS_DB_SERIOUS_EVENTS.serevent_date_created = LS_DB_SERIOUS_EVENTS_TMP.serevent_date_created,LS_DB_SERIOUS_EVENTS.serevent_active = LS_DB_SERIOUS_EVENTS_TMP.serevent_active,LS_DB_SERIOUS_EVENTS.sereventdet_user_modified = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_user_modified,LS_DB_SERIOUS_EVENTS.sereventdet_user_created = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_user_created,LS_DB_SERIOUS_EVENTS.sereventdet_study_type = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_study_type,LS_DB_SERIOUS_EVENTS.sereventdet_spr_id = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_spr_id,LS_DB_SERIOUS_EVENTS.sereventdet_soc_name = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_soc_name,LS_DB_SERIOUS_EVENTS.sereventdet_report_type = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_report_type,LS_DB_SERIOUS_EVENTS.sereventdet_record_id = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_record_id,LS_DB_SERIOUS_EVENTS.sereventdet_meddra_pt_term = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_meddra_pt_term,LS_DB_SERIOUS_EVENTS.sereventdet_meddra_code = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_meddra_code,LS_DB_SERIOUS_EVENTS.sereventdet_is_coded = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_is_coded,LS_DB_SERIOUS_EVENTS.sereventdet_fk_lsmv_always_rec_id = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_fk_lsmv_always_rec_id,LS_DB_SERIOUS_EVENTS.sereventdet_date_modified = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_date_modified,LS_DB_SERIOUS_EVENTS.sereventdet_date_created = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_date_created,LS_DB_SERIOUS_EVENTS.sereventdet_comments = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_comments,LS_DB_SERIOUS_EVENTS.sereventdet_always_smqcmq_term = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_always_smqcmq_term,LS_DB_SERIOUS_EVENTS.sereventdet_always_smqcmq_code = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_always_smqcmq_code,LS_DB_SERIOUS_EVENTS.sereventdet_always_broad_narrow = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_always_broad_narrow,LS_DB_SERIOUS_EVENTS.sereventdet_active = LS_DB_SERIOUS_EVENTS_TMP.sereventdet_active,
LS_DB_SERIOUS_EVENTS.PROCESSING_DT = LS_DB_SERIOUS_EVENTS_TMP.PROCESSING_DT,
LS_DB_SERIOUS_EVENTS.user_modified  =LS_DB_SERIOUS_EVENTS_TMP.user_modified     ,
LS_DB_SERIOUS_EVENTS.date_modified  =LS_DB_SERIOUS_EVENTS_TMP.date_modified     ,
LS_DB_SERIOUS_EVENTS.expiry_date    =LS_DB_SERIOUS_EVENTS_TMP.expiry_date       ,
LS_DB_SERIOUS_EVENTS.created_by     =LS_DB_SERIOUS_EVENTS_TMP.created_by        ,
LS_DB_SERIOUS_EVENTS.created_dt     =LS_DB_SERIOUS_EVENTS_TMP.created_dt        ,
LS_DB_SERIOUS_EVENTS.load_ts        =LS_DB_SERIOUS_EVENTS_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP 
WHERE 	LS_DB_SERIOUS_EVENTS.INTEGRATION_ID = LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SERIOUS_EVENTS_TMP.PROCESSING_DT = LS_DB_SERIOUS_EVENTS.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_SERIOUS_EVENTS.serevent_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SERIOUS_EVENTS.sereventdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SERIOUS_EVENTS_TMP.serevent_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SERIOUS_EVENTS_TMP.sereventdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_SERIOUS_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID ,LS_DB_SERIOUS_EVENTS.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP 
ON LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID=LS_DB_SERIOUS_EVENTS_TMP.serevent_RECORD_ID
AND LS_DB_SERIOUS_EVENTS.INTEGRATION_ID = LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID 
where LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID  is null AND LS_DB_SERIOUS_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID in (select serevent_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP )
) TMP where LS_DB_SERIOUS_EVENTS.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_SERIOUS_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID ,LS_DB_SERIOUS_EVENTS.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP 
    ON LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID=LS_DB_SERIOUS_EVENTS_TMP.serevent_RECORD_ID
    AND LS_DB_SERIOUS_EVENTS.INTEGRATION_ID = LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID 
    where LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID  is null AND LS_DB_SERIOUS_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID in (select serevent_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,serevent_user_modified,
serevent_user_created,
serevent_spr_id,
serevent_record_id,
serevent_meddra_version,
serevent_meddra_name,
serevent_date_modified,
serevent_date_created,
serevent_active,
sereventdet_user_modified,
sereventdet_user_created,
sereventdet_study_type,
sereventdet_spr_id,
sereventdet_soc_name,
sereventdet_report_type,
sereventdet_record_id,
sereventdet_meddra_pt_term,
sereventdet_meddra_code,
sereventdet_is_coded,
sereventdet_fk_lsmv_always_rec_id,
sereventdet_date_modified,
sereventdet_date_created,
sereventdet_comments,
sereventdet_always_smqcmq_term,
sereventdet_always_smqcmq_code,
sereventdet_always_broad_narrow,
sereventdet_active)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,serevent_user_modified,
serevent_user_created,
serevent_spr_id,
serevent_record_id,
serevent_meddra_version,
serevent_meddra_name,
serevent_date_modified,
serevent_date_created,
serevent_active,
sereventdet_user_modified,
sereventdet_user_created,
sereventdet_study_type,
sereventdet_spr_id,
sereventdet_soc_name,
sereventdet_report_type,
sereventdet_record_id,
sereventdet_meddra_pt_term,
sereventdet_meddra_code,
sereventdet_is_coded,
sereventdet_fk_lsmv_always_rec_id,
sereventdet_date_modified,
sereventdet_date_created,
sereventdet_comments,
sereventdet_always_smqcmq_term,
sereventdet_always_smqcmq_code,
sereventdet_always_broad_narrow,
sereventdet_active
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS TGT
WHERE  ( serevent_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events') OR sereventdet_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events_details')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP 
WHERE 	TO_DATE(LS_DB_SERIOUS_EVENTS.PROCESSING_DT) < TO_DATE(LS_DB_SERIOUS_EVENTS_TMP.PROCESSING_DT)
AND LS_DB_SERIOUS_EVENTS.INTEGRATION_ID = LS_DB_SERIOUS_EVENTS_TMP.INTEGRATION_ID
AND LS_DB_SERIOUS_EVENTS.serevent_RECORD_ID = LS_DB_SERIOUS_EVENTS_TMP.serevent_RECORD_ID
AND LS_DB_SERIOUS_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_SERIOUS_EVENTS.serevent_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_SERIOUS_EVENTS.sereventdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_SERIOUS_EVENTS_TMP.serevent_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_SERIOUS_EVENTS_TMP.sereventdet_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( serevent_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events') OR sereventdet_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SERIOUS_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_serious_events_details')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SERIOUS_EVENTS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SERIOUS_EVENTS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SERIOUS_EVENTS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_SERIOUS_EVENTS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SERIOUS_EVENTS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_SERIOUS_EVENTS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
