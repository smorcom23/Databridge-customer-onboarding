
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PRODUCT_THERAPEUTIC_LIB()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'),
	'LSRA','Case','LS_DB_PRODUCT_THERAPEUTIC_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_THERAPEUTIC_LIB'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_THERAPEUTIC_LIB'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_product_therapeutic_area' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_therapeutic_area WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_therapeutic_area' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_therapeutic_area WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_therapeutic_area WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_therapeutic_area WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_therapeutic_area WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_agx_therapeutic_area_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_product_therapeutic_area WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PRODUCT_THERAPEUTIC_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_product_therapeutic_area_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  prdtherapeutic_date_created,date_modified  prdtherapeutic_date_modified,fk_agx_product_rec_id  prdtherapeutic_fk_agx_product_rec_id,fk_agx_therapeutic_area_rec_id  prdtherapeutic_fk_agx_therapeutic_area_rec_id,priority  prdtherapeutic_priority,record_id  prdtherapeutic_record_id,spr_id  prdtherapeutic_spr_id,user_created  prdtherapeutic_user_created,user_modified  prdtherapeutic_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_product_therapeutic_area
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_agx_therapeutic_area_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_therapeutic_area')
  ) where REC_RANK=1 )
  , lsmv_therapeutic_area_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  therap_date_created,date_modified  therap_date_modified,record_id  therap_record_id,spr_id  therap_spr_id,therapeutic_area_desc  therap_therapeutic_area_desc,therapeutic_keywords  therap_therapeutic_keywords,therapeutic_name  therap_therapeutic_name,user_created  therap_user_created,user_modified  therap_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_therapeutic_area
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_therapeutic_area')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_therapeutic_area_SUBSET.therap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_therapeutic_area_SUBSET.therap_USER_MODIFIED USER_MODIFIED,lsmv_therapeutic_area_SUBSET.therap_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_therapeutic_area_SUBSET.therap_USER_CREATED CREATED_BY,lsmv_therapeutic_area_SUBSET.therap_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_therapeutic_area_SUBSET.therap_user_modified  ,lsmv_therapeutic_area_SUBSET.therap_user_created  ,lsmv_therapeutic_area_SUBSET.therap_therapeutic_name  ,lsmv_therapeutic_area_SUBSET.therap_therapeutic_keywords  ,lsmv_therapeutic_area_SUBSET.therap_therapeutic_area_desc  ,lsmv_therapeutic_area_SUBSET.therap_spr_id  ,lsmv_therapeutic_area_SUBSET.therap_record_id  ,lsmv_therapeutic_area_SUBSET.therap_date_modified  ,lsmv_therapeutic_area_SUBSET.therap_date_created  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_user_modified  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_user_created  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_spr_id  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_record_id  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_priority  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_fk_agx_therapeutic_area_rec_id  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_fk_agx_product_rec_id  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_date_modified  ,lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_date_created ,CONCAT(NVL(lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_RECORD_ID,-1),'||',NVL(lsmv_therapeutic_area_SUBSET.therap_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_therapeutic_area_SUBSET  LEFT JOIN lsmv_product_therapeutic_area_SUBSET ON lsmv_therapeutic_area_SUBSET.therap_record_id=lsmv_product_therapeutic_area_SUBSET.prdtherapeutic_fk_agx_therapeutic_area_rec_id
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP)
where target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB   
SET LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_user_modified = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_user_modified,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_user_created = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_user_created,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_therapeutic_name = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_therapeutic_name,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_therapeutic_keywords = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_therapeutic_keywords,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_therapeutic_area_desc = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_therapeutic_area_desc,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_spr_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_spr_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_record_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_record_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_date_modified = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_date_modified,LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_date_created = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_date_created,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_user_modified = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_user_modified,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_user_created = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_user_created,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_spr_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_spr_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_record_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_record_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_priority = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_priority,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_fk_agx_therapeutic_area_rec_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_fk_agx_therapeutic_area_rec_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_fk_agx_product_rec_id = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_fk_agx_product_rec_id,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_date_modified = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_date_modified,LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_date_created = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_date_created,
LS_DB_PRODUCT_THERAPEUTIC_LIB.PROCESSING_DT = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.PROCESSING_DT,
LS_DB_PRODUCT_THERAPEUTIC_LIB.user_modified  =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.user_modified     ,
LS_DB_PRODUCT_THERAPEUTIC_LIB.date_modified  =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.date_modified     ,
LS_DB_PRODUCT_THERAPEUTIC_LIB.expiry_date    =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.expiry_date       ,
LS_DB_PRODUCT_THERAPEUTIC_LIB.created_by     =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.created_by        ,
LS_DB_PRODUCT_THERAPEUTIC_LIB.created_dt     =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.created_dt        ,
LS_DB_PRODUCT_THERAPEUTIC_LIB.load_ts        =LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP 
WHERE 	LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.PROCESSING_DT = LS_DB_PRODUCT_THERAPEUTIC_LIB.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PRODUCT_THERAPEUTIC_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID ,LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP 
ON LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID=LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_RECORD_ID
AND LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID 
where LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_THERAPEUTIC_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID in (select therap_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP )
) TMP where LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_THERAPEUTIC_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID ,LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP 
    ON LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID=LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_RECORD_ID
    AND LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID 
    where LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID  is null AND LS_DB_PRODUCT_THERAPEUTIC_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID in (select therap_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,therap_user_modified,
therap_user_created,
therap_therapeutic_name,
therap_therapeutic_keywords,
therap_therapeutic_area_desc,
therap_spr_id,
therap_record_id,
therap_date_modified,
therap_date_created,
prdtherapeutic_user_modified,
prdtherapeutic_user_created,
prdtherapeutic_spr_id,
prdtherapeutic_record_id,
prdtherapeutic_priority,
prdtherapeutic_fk_agx_therapeutic_area_rec_id,
prdtherapeutic_fk_agx_product_rec_id,
prdtherapeutic_date_modified,
prdtherapeutic_date_created)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,therap_user_modified,
therap_user_created,
therap_therapeutic_name,
therap_therapeutic_keywords,
therap_therapeutic_area_desc,
therap_spr_id,
therap_record_id,
therap_date_modified,
therap_date_created,
prdtherapeutic_user_modified,
prdtherapeutic_user_created,
prdtherapeutic_spr_id,
prdtherapeutic_record_id,
prdtherapeutic_priority,
prdtherapeutic_fk_agx_therapeutic_area_rec_id,
prdtherapeutic_fk_agx_product_rec_id,
prdtherapeutic_date_modified,
prdtherapeutic_date_created
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP 
WHERE 	TO_DATE(LS_DB_PRODUCT_THERAPEUTIC_LIB.PROCESSING_DT) < TO_DATE(LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.PROCESSING_DT)
AND LS_DB_PRODUCT_THERAPEUTIC_LIB.INTEGRATION_ID = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.INTEGRATION_ID
AND LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_RECORD_ID = LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_RECORD_ID
AND LS_DB_PRODUCT_THERAPEUTIC_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB.prdtherapeutic_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB.therap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.prdtherapeutic_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP.therap_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB TGT
WHERE  ( prdtherapeutic_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_therapeutic_area') OR therap_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_therapeutic_area')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( prdtherapeutic_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_therapeutic_area') OR therap_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PRODUCT_THERAPEUTIC_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_therapeutic_area')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PRODUCT_THERAPEUTIC_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PRODUCT_THERAPEUTIC_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PRODUCT_THERAPEUTIC_LIB'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PRODUCT_THERAPEUTIC_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
