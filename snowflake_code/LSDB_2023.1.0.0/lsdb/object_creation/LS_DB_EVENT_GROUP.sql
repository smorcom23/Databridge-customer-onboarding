
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_EVENT_GROUP()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_EVENT_GROUP'),
	'LSRA','Case','LS_DB_EVENT_GROUP',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_EVENT_GROUP' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_EVENT_GROUP'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_EVENT_GROUP'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  As select RECORD_ID,'lsmv_event_group' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_event_group_search' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group_search WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_event_grp_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_EVENT_GROUP' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_event_group_search_SUBSET AS 
(
select * from 
    (SELECT  
    condition  evntgrpsrch_condition,date_created  evntgrpsrch_date_created,date_modified  evntgrpsrch_date_modified,date_value  evntgrpsrch_date_value,decode_values  evntgrpsrch_decode_values,field_name  evntgrpsrch_field_name,fk_event_grp_rec_id  evntgrpsrch_fk_event_grp_rec_id,record_id  evntgrpsrch_record_id,spr_id  evntgrpsrch_spr_id,test  evntgrpsrch_test,to_date_value  evntgrpsrch_to_date_value,user_created  evntgrpsrch_user_created,user_modified  evntgrpsrch_user_modified,value  evntgrpsrch_value,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group_search
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_event_grp_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group_search')
  ) where REC_RANK=1 )
  , lsmv_event_group_SUBSET AS 
(
select * from 
    (SELECT  
    active  evntgrp_active,assign_to  evntgrp_assign_to,assigned_to  evntgrp_assigned_to,date_created  evntgrp_date_created,date_modified  evntgrp_date_modified,description  evntgrp_description,event_group_name  evntgrp_event_group_name,event_search_result  evntgrp_event_search_result,meddra_version  evntgrp_meddra_version,medra_search_level  evntgrp_medra_search_level,record_id  evntgrp_record_id,search_type  evntgrp_search_type,spr_id  evntgrp_spr_id,user_created  evntgrp_user_created,user_modified  evntgrp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_event_group
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_event_group_SUBSET.evntgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_event_group_search_SUBSET.evntgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_event_group_SUBSET.evntgrp_USER_MODIFIED USER_MODIFIED,lsmv_event_group_SUBSET.evntgrp_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_event_group_SUBSET.evntgrp_USER_CREATED CREATED_BY,lsmv_event_group_SUBSET.evntgrp_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_event_group_SUBSET.evntgrp_user_modified  ,lsmv_event_group_SUBSET.evntgrp_user_created  ,lsmv_event_group_SUBSET.evntgrp_spr_id  ,lsmv_event_group_SUBSET.evntgrp_search_type  ,lsmv_event_group_SUBSET.evntgrp_record_id  ,lsmv_event_group_SUBSET.evntgrp_medra_search_level  ,lsmv_event_group_SUBSET.evntgrp_meddra_version  ,lsmv_event_group_SUBSET.evntgrp_event_search_result  ,lsmv_event_group_SUBSET.evntgrp_event_group_name  ,lsmv_event_group_SUBSET.evntgrp_description  ,lsmv_event_group_SUBSET.evntgrp_date_modified  ,lsmv_event_group_SUBSET.evntgrp_date_created  ,lsmv_event_group_SUBSET.evntgrp_assigned_to  ,lsmv_event_group_SUBSET.evntgrp_assign_to  ,lsmv_event_group_SUBSET.evntgrp_active  ,lsmv_event_group_search_SUBSET.evntgrpsrch_value  ,lsmv_event_group_search_SUBSET.evntgrpsrch_user_modified  ,lsmv_event_group_search_SUBSET.evntgrpsrch_user_created  ,lsmv_event_group_search_SUBSET.evntgrpsrch_to_date_value  ,lsmv_event_group_search_SUBSET.evntgrpsrch_test  ,lsmv_event_group_search_SUBSET.evntgrpsrch_spr_id  ,lsmv_event_group_search_SUBSET.evntgrpsrch_record_id  ,lsmv_event_group_search_SUBSET.evntgrpsrch_fk_event_grp_rec_id  ,lsmv_event_group_search_SUBSET.evntgrpsrch_field_name  ,lsmv_event_group_search_SUBSET.evntgrpsrch_decode_values  ,lsmv_event_group_search_SUBSET.evntgrpsrch_date_value  ,lsmv_event_group_search_SUBSET.evntgrpsrch_date_modified  ,lsmv_event_group_search_SUBSET.evntgrpsrch_date_created  ,lsmv_event_group_search_SUBSET.evntgrpsrch_condition ,CONCAT(NVL(lsmv_event_group_SUBSET.evntgrp_RECORD_ID,-1),'||',NVL(lsmv_event_group_search_SUBSET.evntgrpsrch_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_event_group_SUBSET  LEFT JOIN lsmv_event_group_search_SUBSET ON lsmv_event_group_SUBSET.evntgrp_record_id=lsmv_event_group_search_SUBSET.evntgrpsrch_fk_event_grp_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP)
where target_table_name='LS_DB_EVENT_GROUP'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_EVENT_GROUP'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP   
SET LS_DB_EVENT_GROUP.evntgrp_user_modified = LS_DB_EVENT_GROUP_TMP.evntgrp_user_modified,LS_DB_EVENT_GROUP.evntgrp_user_created = LS_DB_EVENT_GROUP_TMP.evntgrp_user_created,LS_DB_EVENT_GROUP.evntgrp_spr_id = LS_DB_EVENT_GROUP_TMP.evntgrp_spr_id,LS_DB_EVENT_GROUP.evntgrp_search_type = LS_DB_EVENT_GROUP_TMP.evntgrp_search_type,LS_DB_EVENT_GROUP.evntgrp_record_id = LS_DB_EVENT_GROUP_TMP.evntgrp_record_id,LS_DB_EVENT_GROUP.evntgrp_medra_search_level = LS_DB_EVENT_GROUP_TMP.evntgrp_medra_search_level,LS_DB_EVENT_GROUP.evntgrp_meddra_version = LS_DB_EVENT_GROUP_TMP.evntgrp_meddra_version,LS_DB_EVENT_GROUP.evntgrp_event_search_result = LS_DB_EVENT_GROUP_TMP.evntgrp_event_search_result,LS_DB_EVENT_GROUP.evntgrp_event_group_name = LS_DB_EVENT_GROUP_TMP.evntgrp_event_group_name,LS_DB_EVENT_GROUP.evntgrp_description = LS_DB_EVENT_GROUP_TMP.evntgrp_description,LS_DB_EVENT_GROUP.evntgrp_date_modified = LS_DB_EVENT_GROUP_TMP.evntgrp_date_modified,LS_DB_EVENT_GROUP.evntgrp_date_created = LS_DB_EVENT_GROUP_TMP.evntgrp_date_created,LS_DB_EVENT_GROUP.evntgrp_assigned_to = LS_DB_EVENT_GROUP_TMP.evntgrp_assigned_to,LS_DB_EVENT_GROUP.evntgrp_assign_to = LS_DB_EVENT_GROUP_TMP.evntgrp_assign_to,LS_DB_EVENT_GROUP.evntgrp_active = LS_DB_EVENT_GROUP_TMP.evntgrp_active,LS_DB_EVENT_GROUP.evntgrpsrch_value = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_value,LS_DB_EVENT_GROUP.evntgrpsrch_user_modified = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_user_modified,LS_DB_EVENT_GROUP.evntgrpsrch_user_created = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_user_created,LS_DB_EVENT_GROUP.evntgrpsrch_to_date_value = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_to_date_value,LS_DB_EVENT_GROUP.evntgrpsrch_test = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_test,LS_DB_EVENT_GROUP.evntgrpsrch_spr_id = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_spr_id,LS_DB_EVENT_GROUP.evntgrpsrch_record_id = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_record_id,LS_DB_EVENT_GROUP.evntgrpsrch_fk_event_grp_rec_id = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_fk_event_grp_rec_id,LS_DB_EVENT_GROUP.evntgrpsrch_field_name = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_field_name,LS_DB_EVENT_GROUP.evntgrpsrch_decode_values = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_decode_values,LS_DB_EVENT_GROUP.evntgrpsrch_date_value = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_date_value,LS_DB_EVENT_GROUP.evntgrpsrch_date_modified = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_date_modified,LS_DB_EVENT_GROUP.evntgrpsrch_date_created = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_date_created,LS_DB_EVENT_GROUP.evntgrpsrch_condition = LS_DB_EVENT_GROUP_TMP.evntgrpsrch_condition,
LS_DB_EVENT_GROUP.PROCESSING_DT = LS_DB_EVENT_GROUP_TMP.PROCESSING_DT,
LS_DB_EVENT_GROUP.user_modified  =LS_DB_EVENT_GROUP_TMP.user_modified     ,
LS_DB_EVENT_GROUP.date_modified  =LS_DB_EVENT_GROUP_TMP.date_modified     ,
LS_DB_EVENT_GROUP.expiry_date    =LS_DB_EVENT_GROUP_TMP.expiry_date       ,
LS_DB_EVENT_GROUP.created_by     =LS_DB_EVENT_GROUP_TMP.created_by        ,
LS_DB_EVENT_GROUP.created_dt     =LS_DB_EVENT_GROUP_TMP.created_dt        ,
LS_DB_EVENT_GROUP.load_ts        =LS_DB_EVENT_GROUP_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP 
WHERE 	LS_DB_EVENT_GROUP.INTEGRATION_ID = LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_EVENT_GROUP_TMP.PROCESSING_DT = LS_DB_EVENT_GROUP.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_EVENT_GROUP.evntgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_EVENT_GROUP.evntgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_EVENT_GROUP_TMP.evntgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_EVENT_GROUP_TMP.evntgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_EVENT_GROUP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_EVENT_GROUP.evntgrp_RECORD_ID ,LS_DB_EVENT_GROUP.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP 
ON LS_DB_EVENT_GROUP.evntgrp_RECORD_ID=LS_DB_EVENT_GROUP_TMP.evntgrp_RECORD_ID
AND LS_DB_EVENT_GROUP.INTEGRATION_ID = LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID 
where LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID  is null AND LS_DB_EVENT_GROUP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_EVENT_GROUP.evntgrp_RECORD_ID in (select evntgrp_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP )
) TMP where LS_DB_EVENT_GROUP.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_EVENT_GROUP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_EVENT_GROUP.evntgrp_RECORD_ID ,LS_DB_EVENT_GROUP.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP 
    ON LS_DB_EVENT_GROUP.evntgrp_RECORD_ID=LS_DB_EVENT_GROUP_TMP.evntgrp_RECORD_ID
    AND LS_DB_EVENT_GROUP.INTEGRATION_ID = LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID 
    where LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID  is null AND LS_DB_EVENT_GROUP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_EVENT_GROUP.evntgrp_RECORD_ID in (select evntgrp_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,evntgrp_user_modified,
evntgrp_user_created,
evntgrp_spr_id,
evntgrp_search_type,
evntgrp_record_id,
evntgrp_medra_search_level,
evntgrp_meddra_version,
evntgrp_event_search_result,
evntgrp_event_group_name,
evntgrp_description,
evntgrp_date_modified,
evntgrp_date_created,
evntgrp_assigned_to,
evntgrp_assign_to,
evntgrp_active,
evntgrpsrch_value,
evntgrpsrch_user_modified,
evntgrpsrch_user_created,
evntgrpsrch_to_date_value,
evntgrpsrch_test,
evntgrpsrch_spr_id,
evntgrpsrch_record_id,
evntgrpsrch_fk_event_grp_rec_id,
evntgrpsrch_field_name,
evntgrpsrch_decode_values,
evntgrpsrch_date_value,
evntgrpsrch_date_modified,
evntgrpsrch_date_created,
evntgrpsrch_condition)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,evntgrp_user_modified,
evntgrp_user_created,
evntgrp_spr_id,
evntgrp_search_type,
evntgrp_record_id,
evntgrp_medra_search_level,
evntgrp_meddra_version,
evntgrp_event_search_result,
evntgrp_event_group_name,
evntgrp_description,
evntgrp_date_modified,
evntgrp_date_created,
evntgrp_assigned_to,
evntgrp_assign_to,
evntgrp_active,
evntgrpsrch_value,
evntgrpsrch_user_modified,
evntgrpsrch_user_created,
evntgrpsrch_to_date_value,
evntgrpsrch_test,
evntgrpsrch_spr_id,
evntgrpsrch_record_id,
evntgrpsrch_fk_event_grp_rec_id,
evntgrpsrch_field_name,
evntgrpsrch_decode_values,
evntgrpsrch_date_value,
evntgrpsrch_date_modified,
evntgrpsrch_date_created,
evntgrpsrch_condition
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP 
WHERE 	TO_DATE(LS_DB_EVENT_GROUP.PROCESSING_DT) < TO_DATE(LS_DB_EVENT_GROUP_TMP.PROCESSING_DT)
AND LS_DB_EVENT_GROUP.INTEGRATION_ID = LS_DB_EVENT_GROUP_TMP.INTEGRATION_ID
AND LS_DB_EVENT_GROUP.evntgrp_RECORD_ID = LS_DB_EVENT_GROUP_TMP.evntgrp_RECORD_ID
AND LS_DB_EVENT_GROUP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_EVENT_GROUP.evntgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_EVENT_GROUP.evntgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_EVENT_GROUP_TMP.evntgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_EVENT_GROUP_TMP.evntgrpsrch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP TGT
WHERE  ( evntgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group') OR evntgrpsrch_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group_search')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( evntgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group') OR evntgrpsrch_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_EVENT_GROUP_DELETION_TMP  WHERE TABLE_NAME='lsmv_event_group_search')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_EVENT_GROUP_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_EVENT_GROUP'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_EVENT_GROUP'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_EVENT_GROUP Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_EVENT_GROUP'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_EVENT_GROUP not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
