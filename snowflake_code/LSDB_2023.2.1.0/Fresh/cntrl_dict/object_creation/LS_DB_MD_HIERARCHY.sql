
-- USE SCHEMA ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_MD_HIERARCHY()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MD_HIERARCHY'),
	'LSRA','Case','LS_DB_MD_HIERARCHY',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_MD_HIERARCHY' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_MD_HIERARCHY'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_MD_HIERARCHY'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_MD_HIERARCHY_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_MD_HIERARCHY_DELETION_TMP  As select RECORD_ID,'lsmv_md_hierarchy' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_md_hierarchy WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_md_hierarchy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MD_HIERARCHY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MD_HIERARCHY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_md_hierarchy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MD_HIERARCHY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MD_HIERARCHY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_md_hierarchy_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  date_created,date_modified  date_modified,hlgt_code  hlgt_code,hlgt_name  hlgt_name,hlgt_name_cn  hlgt_name_cn,hlt_code  hlt_code,hlt_name  hlt_name,hlt_name_cn  hlt_name_cn,meddra_version  meddra_version,null_field  null_field,primary_soc_fg  primary_soc_fg,pt_code  pt_code,pt_name  pt_name,pt_name_cn  pt_name_cn,pt_soc_code  pt_soc_code,record_id  record_id,soc_abbrev  soc_abbrev,soc_code  soc_code,soc_name  soc_name,soc_name_cn  soc_name_cn,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_md_hierarchy
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_MD_HIERARCHY_DELETION_TMP  WHERE TABLE_NAME='lsmv_md_hierarchy')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_md_hierarchy_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_md_hierarchy_SUBSET.USER_CREATED CREATED_BY,lsmv_md_hierarchy_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_md_hierarchy_SUBSET.user_modified  ,lsmv_md_hierarchy_SUBSET.user_created  ,lsmv_md_hierarchy_SUBSET.spr_id  ,lsmv_md_hierarchy_SUBSET.soc_name_cn  ,lsmv_md_hierarchy_SUBSET.soc_name  ,lsmv_md_hierarchy_SUBSET.soc_code  ,lsmv_md_hierarchy_SUBSET.soc_abbrev  ,lsmv_md_hierarchy_SUBSET.record_id  ,lsmv_md_hierarchy_SUBSET.pt_soc_code  ,lsmv_md_hierarchy_SUBSET.pt_name_cn  ,lsmv_md_hierarchy_SUBSET.pt_name  ,lsmv_md_hierarchy_SUBSET.pt_code  ,lsmv_md_hierarchy_SUBSET.primary_soc_fg  ,lsmv_md_hierarchy_SUBSET.null_field  ,lsmv_md_hierarchy_SUBSET.meddra_version  ,lsmv_md_hierarchy_SUBSET.hlt_name_cn  ,lsmv_md_hierarchy_SUBSET.hlt_name  ,lsmv_md_hierarchy_SUBSET.hlt_code  ,lsmv_md_hierarchy_SUBSET.hlgt_name_cn  ,lsmv_md_hierarchy_SUBSET.hlgt_name  ,lsmv_md_hierarchy_SUBSET.hlgt_code  ,lsmv_md_hierarchy_SUBSET.date_modified  ,lsmv_md_hierarchy_SUBSET.date_created ,CONCAT( NVL(lsmv_md_hierarchy_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_md_hierarchy_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP)
where target_table_name='LS_DB_MD_HIERARCHY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MD_HIERARCHY'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY   
SET LS_DB_MD_HIERARCHY.user_modified = LS_DB_MD_HIERARCHY_TMP.user_modified,LS_DB_MD_HIERARCHY.user_created = LS_DB_MD_HIERARCHY_TMP.user_created,LS_DB_MD_HIERARCHY.spr_id = LS_DB_MD_HIERARCHY_TMP.spr_id,LS_DB_MD_HIERARCHY.soc_name_cn = LS_DB_MD_HIERARCHY_TMP.soc_name_cn,LS_DB_MD_HIERARCHY.soc_name = LS_DB_MD_HIERARCHY_TMP.soc_name,LS_DB_MD_HIERARCHY.soc_code = LS_DB_MD_HIERARCHY_TMP.soc_code,LS_DB_MD_HIERARCHY.soc_abbrev = LS_DB_MD_HIERARCHY_TMP.soc_abbrev,LS_DB_MD_HIERARCHY.record_id = LS_DB_MD_HIERARCHY_TMP.record_id,LS_DB_MD_HIERARCHY.pt_soc_code = LS_DB_MD_HIERARCHY_TMP.pt_soc_code,LS_DB_MD_HIERARCHY.pt_name_cn = LS_DB_MD_HIERARCHY_TMP.pt_name_cn,LS_DB_MD_HIERARCHY.pt_name = LS_DB_MD_HIERARCHY_TMP.pt_name,LS_DB_MD_HIERARCHY.pt_code = LS_DB_MD_HIERARCHY_TMP.pt_code,LS_DB_MD_HIERARCHY.primary_soc_fg = LS_DB_MD_HIERARCHY_TMP.primary_soc_fg,LS_DB_MD_HIERARCHY.null_field = LS_DB_MD_HIERARCHY_TMP.null_field,LS_DB_MD_HIERARCHY.meddra_version = LS_DB_MD_HIERARCHY_TMP.meddra_version,LS_DB_MD_HIERARCHY.hlt_name_cn = LS_DB_MD_HIERARCHY_TMP.hlt_name_cn,LS_DB_MD_HIERARCHY.hlt_name = LS_DB_MD_HIERARCHY_TMP.hlt_name,LS_DB_MD_HIERARCHY.hlt_code = LS_DB_MD_HIERARCHY_TMP.hlt_code,LS_DB_MD_HIERARCHY.hlgt_name_cn = LS_DB_MD_HIERARCHY_TMP.hlgt_name_cn,LS_DB_MD_HIERARCHY.hlgt_name = LS_DB_MD_HIERARCHY_TMP.hlgt_name,LS_DB_MD_HIERARCHY.hlgt_code = LS_DB_MD_HIERARCHY_TMP.hlgt_code,LS_DB_MD_HIERARCHY.date_modified = LS_DB_MD_HIERARCHY_TMP.date_modified,LS_DB_MD_HIERARCHY.date_created = LS_DB_MD_HIERARCHY_TMP.date_created,
LS_DB_MD_HIERARCHY.PROCESSING_DT = LS_DB_MD_HIERARCHY_TMP.PROCESSING_DT ,
LS_DB_MD_HIERARCHY.expiry_date    =LS_DB_MD_HIERARCHY_TMP.expiry_date       ,
LS_DB_MD_HIERARCHY.created_by     =LS_DB_MD_HIERARCHY_TMP.created_by        ,
LS_DB_MD_HIERARCHY.created_dt     =LS_DB_MD_HIERARCHY_TMP.created_dt        ,
LS_DB_MD_HIERARCHY.load_ts        =LS_DB_MD_HIERARCHY_TMP.load_ts         
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP 
WHERE 	LS_DB_MD_HIERARCHY.INTEGRATION_ID = LS_DB_MD_HIERARCHY_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_MD_HIERARCHY_TMP.PROCESSING_DT = LS_DB_MD_HIERARCHY.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
soc_name_cn,
soc_name,
soc_code,
soc_abbrev,
record_id,
pt_soc_code,
pt_name_cn,
pt_name,
pt_code,
primary_soc_fg,
null_field,
meddra_version,
hlt_name_cn,
hlt_name,
hlt_code,
hlgt_name_cn,
hlgt_name,
hlgt_code,
date_modified,
date_created)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
soc_name_cn,
soc_name,
soc_code,
soc_abbrev,
record_id,
pt_soc_code,
pt_name_cn,
pt_name,
pt_code,
primary_soc_fg,
null_field,
meddra_version,
hlt_name_cn,
hlt_name,
hlt_code,
hlgt_name_cn,
hlgt_name,
hlgt_code,
date_modified,
date_created
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP 
WHERE 	TO_DATE(LS_DB_MD_HIERARCHY.PROCESSING_DT) < TO_DATE(LS_DB_MD_HIERARCHY_TMP.PROCESSING_DT)
AND LS_DB_MD_HIERARCHY.INTEGRATION_ID = LS_DB_MD_HIERARCHY_TMP.INTEGRATION_ID
AND LS_DB_MD_HIERARCHY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_MD_HIERARCHY_DELETION_TMP  WHERE TABLE_NAME='lsmv_md_hierarchy')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_MD_HIERARCHY_DELETION_TMP  WHERE TABLE_NAME='lsmv_md_hierarchy')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_MD_HIERARCHY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MD_HIERARCHY'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_MD_HIERARCHY Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_MD_HIERARCHY'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_MD_HIERARCHY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
