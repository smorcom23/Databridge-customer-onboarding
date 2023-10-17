
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_ACCOUNTS_SERIES()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_SERIES'),
	'LSRA','Case','LS_DB_ACCOUNTS_SERIES',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_ACCOUNTS_SERIES' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_ACCOUNTS_SERIES'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_ACCOUNTS_SERIES'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  As select RECORD_ID,'lsmv_account_series' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_account_series_search' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series_search WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_account_series_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series_search WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_SERIES' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_account_series_SUBSET AS 
(
select * from 
    (SELECT  
    account_search_result  accseries_account_search_result,account_series_name  accseries_account_series_name,active  accseries_active,assign_to  accseries_assign_to,assigned_group_user  accseries_assigned_group_user,assigned_to  accseries_assigned_to,condition  accseries_condition,date_created  accseries_date_created,date_modified  accseries_date_modified,description  accseries_description,include_serach_results  accseries_include_serach_results,record_id  accseries_record_id,search_type  accseries_search_type,spr_id  accseries_spr_id,user_created  accseries_user_created,user_modified  accseries_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series')
  ) where REC_RANK=1 )
  , lsmv_account_series_search_SUBSET AS 
(
select * from 
    (SELECT  
    condition  accsersearch_condition,date_created  accsersearch_date_created,date_modified  accsersearch_date_modified,date_value  accsersearch_date_value,decode_values  accsersearch_decode_values,field_name  accsersearch_field_name,fk_account_series_rec_id  accsersearch_fk_account_series_rec_id,record_id  accsersearch_record_id,spr_id  accsersearch_spr_id,test  accsersearch_test,to_date_value  accsersearch_to_date_value,user_created  accsersearch_user_created,user_modified  accsersearch_user_modified,value  accsersearch_value,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_series_search
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_account_series_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series_search')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_account_series_SUBSET.accseries_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_account_series_search_SUBSET.accsersearch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_account_series_SUBSET.accseries_USER_MODIFIED USER_MODIFIED,lsmv_account_series_SUBSET.accseries_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_account_series_SUBSET.accseries_USER_CREATED CREATED_BY,lsmv_account_series_SUBSET.accseries_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_account_series_SUBSET.accseries_user_modified  ,lsmv_account_series_SUBSET.accseries_user_created  ,lsmv_account_series_SUBSET.accseries_spr_id  ,lsmv_account_series_SUBSET.accseries_search_type  ,lsmv_account_series_SUBSET.accseries_record_id  ,lsmv_account_series_SUBSET.accseries_include_serach_results  ,lsmv_account_series_SUBSET.accseries_description  ,lsmv_account_series_SUBSET.accseries_date_modified  ,lsmv_account_series_SUBSET.accseries_date_created  ,lsmv_account_series_SUBSET.accseries_condition  ,lsmv_account_series_SUBSET.accseries_assigned_to  ,lsmv_account_series_SUBSET.accseries_assigned_group_user  ,lsmv_account_series_SUBSET.accseries_assign_to  ,lsmv_account_series_SUBSET.accseries_active  ,lsmv_account_series_SUBSET.accseries_account_series_name  ,lsmv_account_series_SUBSET.accseries_account_search_result  ,lsmv_account_series_search_SUBSET.accsersearch_value  ,lsmv_account_series_search_SUBSET.accsersearch_user_modified  ,lsmv_account_series_search_SUBSET.accsersearch_user_created  ,lsmv_account_series_search_SUBSET.accsersearch_to_date_value  ,lsmv_account_series_search_SUBSET.accsersearch_test  ,lsmv_account_series_search_SUBSET.accsersearch_spr_id  ,lsmv_account_series_search_SUBSET.accsersearch_record_id  ,lsmv_account_series_search_SUBSET.accsersearch_fk_account_series_rec_id  ,lsmv_account_series_search_SUBSET.accsersearch_field_name  ,lsmv_account_series_search_SUBSET.accsersearch_decode_values  ,lsmv_account_series_search_SUBSET.accsersearch_date_value  ,lsmv_account_series_search_SUBSET.accsersearch_date_modified  ,lsmv_account_series_search_SUBSET.accsersearch_date_created  ,lsmv_account_series_search_SUBSET.accsersearch_condition ,CONCAT(NVL(lsmv_account_series_SUBSET.accseries_RECORD_ID,-1),'||',NVL(lsmv_account_series_search_SUBSET.accsersearch_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_account_series_SUBSET  LEFT JOIN lsmv_account_series_search_SUBSET ON lsmv_account_series_SUBSET.accseries_record_id=lsmv_account_series_search_SUBSET.accsersearch_fk_account_series_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP)
where target_table_name='LS_DB_ACCOUNTS_SERIES'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_SERIES'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES   
SET LS_DB_ACCOUNTS_SERIES.accseries_user_modified = LS_DB_ACCOUNTS_SERIES_TMP.accseries_user_modified,LS_DB_ACCOUNTS_SERIES.accseries_user_created = LS_DB_ACCOUNTS_SERIES_TMP.accseries_user_created,LS_DB_ACCOUNTS_SERIES.accseries_spr_id = LS_DB_ACCOUNTS_SERIES_TMP.accseries_spr_id,LS_DB_ACCOUNTS_SERIES.accseries_search_type = LS_DB_ACCOUNTS_SERIES_TMP.accseries_search_type,LS_DB_ACCOUNTS_SERIES.accseries_record_id = LS_DB_ACCOUNTS_SERIES_TMP.accseries_record_id,LS_DB_ACCOUNTS_SERIES.accseries_include_serach_results = LS_DB_ACCOUNTS_SERIES_TMP.accseries_include_serach_results,LS_DB_ACCOUNTS_SERIES.accseries_description = LS_DB_ACCOUNTS_SERIES_TMP.accseries_description,LS_DB_ACCOUNTS_SERIES.accseries_date_modified = LS_DB_ACCOUNTS_SERIES_TMP.accseries_date_modified,LS_DB_ACCOUNTS_SERIES.accseries_date_created = LS_DB_ACCOUNTS_SERIES_TMP.accseries_date_created,LS_DB_ACCOUNTS_SERIES.accseries_condition = LS_DB_ACCOUNTS_SERIES_TMP.accseries_condition,LS_DB_ACCOUNTS_SERIES.accseries_assigned_to = LS_DB_ACCOUNTS_SERIES_TMP.accseries_assigned_to,LS_DB_ACCOUNTS_SERIES.accseries_assigned_group_user = LS_DB_ACCOUNTS_SERIES_TMP.accseries_assigned_group_user,LS_DB_ACCOUNTS_SERIES.accseries_assign_to = LS_DB_ACCOUNTS_SERIES_TMP.accseries_assign_to,LS_DB_ACCOUNTS_SERIES.accseries_active = LS_DB_ACCOUNTS_SERIES_TMP.accseries_active,LS_DB_ACCOUNTS_SERIES.accseries_account_series_name = LS_DB_ACCOUNTS_SERIES_TMP.accseries_account_series_name,LS_DB_ACCOUNTS_SERIES.accseries_account_search_result = LS_DB_ACCOUNTS_SERIES_TMP.accseries_account_search_result,LS_DB_ACCOUNTS_SERIES.accsersearch_value = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_value,LS_DB_ACCOUNTS_SERIES.accsersearch_user_modified = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_user_modified,LS_DB_ACCOUNTS_SERIES.accsersearch_user_created = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_user_created,LS_DB_ACCOUNTS_SERIES.accsersearch_to_date_value = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_to_date_value,LS_DB_ACCOUNTS_SERIES.accsersearch_test = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_test,LS_DB_ACCOUNTS_SERIES.accsersearch_spr_id = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_spr_id,LS_DB_ACCOUNTS_SERIES.accsersearch_record_id = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_record_id,LS_DB_ACCOUNTS_SERIES.accsersearch_fk_account_series_rec_id = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_fk_account_series_rec_id,LS_DB_ACCOUNTS_SERIES.accsersearch_field_name = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_field_name,LS_DB_ACCOUNTS_SERIES.accsersearch_decode_values = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_decode_values,LS_DB_ACCOUNTS_SERIES.accsersearch_date_value = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_date_value,LS_DB_ACCOUNTS_SERIES.accsersearch_date_modified = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_date_modified,LS_DB_ACCOUNTS_SERIES.accsersearch_date_created = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_date_created,LS_DB_ACCOUNTS_SERIES.accsersearch_condition = LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_condition,
LS_DB_ACCOUNTS_SERIES.PROCESSING_DT = LS_DB_ACCOUNTS_SERIES_TMP.PROCESSING_DT,
LS_DB_ACCOUNTS_SERIES.user_modified  =LS_DB_ACCOUNTS_SERIES_TMP.user_modified     ,
LS_DB_ACCOUNTS_SERIES.date_modified  =LS_DB_ACCOUNTS_SERIES_TMP.date_modified     ,
LS_DB_ACCOUNTS_SERIES.expiry_date    =LS_DB_ACCOUNTS_SERIES_TMP.expiry_date       ,
LS_DB_ACCOUNTS_SERIES.created_by     =LS_DB_ACCOUNTS_SERIES_TMP.created_by        ,
LS_DB_ACCOUNTS_SERIES.created_dt     =LS_DB_ACCOUNTS_SERIES_TMP.created_dt        ,
LS_DB_ACCOUNTS_SERIES.load_ts        =LS_DB_ACCOUNTS_SERIES_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP 
WHERE 	LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID = LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_ACCOUNTS_SERIES_TMP.PROCESSING_DT = LS_DB_ACCOUNTS_SERIES.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_ACCOUNTS_SERIES.accseries_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_SERIES.accsersearch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_ACCOUNTS_SERIES_TMP.accseries_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_ACCOUNTS_SERIES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID ,LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP 
ON LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID=LS_DB_ACCOUNTS_SERIES_TMP.accseries_RECORD_ID
AND LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID = LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID 
where LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID  is null AND LS_DB_ACCOUNTS_SERIES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID in (select accseries_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP )
) TMP where LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_ACCOUNTS_SERIES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID ,LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP 
    ON LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID=LS_DB_ACCOUNTS_SERIES_TMP.accseries_RECORD_ID
    AND LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID = LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID 
    where LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID  is null AND LS_DB_ACCOUNTS_SERIES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID in (select accseries_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,accseries_user_modified,
accseries_user_created,
accseries_spr_id,
accseries_search_type,
accseries_record_id,
accseries_include_serach_results,
accseries_description,
accseries_date_modified,
accseries_date_created,
accseries_condition,
accseries_assigned_to,
accseries_assigned_group_user,
accseries_assign_to,
accseries_active,
accseries_account_series_name,
accseries_account_search_result,
accsersearch_value,
accsersearch_user_modified,
accsersearch_user_created,
accsersearch_to_date_value,
accsersearch_test,
accsersearch_spr_id,
accsersearch_record_id,
accsersearch_fk_account_series_rec_id,
accsersearch_field_name,
accsersearch_decode_values,
accsersearch_date_value,
accsersearch_date_modified,
accsersearch_date_created,
accsersearch_condition)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,accseries_user_modified,
accseries_user_created,
accseries_spr_id,
accseries_search_type,
accseries_record_id,
accseries_include_serach_results,
accseries_description,
accseries_date_modified,
accseries_date_created,
accseries_condition,
accseries_assigned_to,
accseries_assigned_group_user,
accseries_assign_to,
accseries_active,
accseries_account_series_name,
accseries_account_search_result,
accsersearch_value,
accsersearch_user_modified,
accsersearch_user_created,
accsersearch_to_date_value,
accsersearch_test,
accsersearch_spr_id,
accsersearch_record_id,
accsersearch_fk_account_series_rec_id,
accsersearch_field_name,
accsersearch_decode_values,
accsersearch_date_value,
accsersearch_date_modified,
accsersearch_date_created,
accsersearch_condition
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP 
WHERE 	TO_DATE(LS_DB_ACCOUNTS_SERIES.PROCESSING_DT) < TO_DATE(LS_DB_ACCOUNTS_SERIES_TMP.PROCESSING_DT)
AND LS_DB_ACCOUNTS_SERIES.INTEGRATION_ID = LS_DB_ACCOUNTS_SERIES_TMP.INTEGRATION_ID
AND LS_DB_ACCOUNTS_SERIES.accseries_RECORD_ID = LS_DB_ACCOUNTS_SERIES_TMP.accseries_RECORD_ID
AND LS_DB_ACCOUNTS_SERIES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_ACCOUNTS_SERIES.accseries_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_SERIES.accsersearch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_ACCOUNTS_SERIES_TMP.accseries_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_SERIES_TMP.accsersearch_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES TGT
WHERE  ( accseries_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series') OR accsersearch_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series_search')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( accseries_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series') OR accsersearch_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_SERIES_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_series_search')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_SERIES_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_ACCOUNTS_SERIES'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_SERIES'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_ACCOUNTS_SERIES Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_ACCOUNTS_SERIES'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_ACCOUNTS_SERIES not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
