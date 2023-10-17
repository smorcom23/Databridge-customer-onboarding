


-- select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_DER
--truncate table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER;

--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_DER();

---- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};

CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_DER()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

-- select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_CASE_DER';
-- delete  from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_DER';



insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_DER'),
	'LSRA','Case','LS_DB_CASE_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                      WHERE TARGET_TABLE_NAME='LS_DB_CASE_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  As 
select RECORD_ID,'lsmv_receipt_item' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'LSMV_PATIENT' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT  RECORD_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_AUTOPSY
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEATH_CAUSE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   FK_ASR_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_REPORTDUPLICATE
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_study
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select distinct  C.ARI_REC_ID  
FROM ${stage_db_name}.${stage_schema_name}.LSMV_LANG_REDUCTED A join ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE B on 
					A.ENTITY_RECORD_ID = B.RECORD_ID
					JOIN ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT  C ON A.FK_ASR_REC_ID=C.RECORD_ID
    where 
	(A.CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND A.CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
    )
	or (
	B.CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND B.CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
    )
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_RESEARCH_RPT
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_LISTEDNESS
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
select DISTINCT   ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION ALL
select distinct  C.Record_id  
FROM ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM  C join  (select DISTINCT   RECORD_ID,date_modified   FROM ${stage_db_name}.${stage_schema_name}.LSMV_PARTNER
															WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
																AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
																UNION ALL
															select DISTINCT   RECORD_ID,date_modified    FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACCOUNTS
																WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
																	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
 

											) A on  C.Account_id =A.RECORD_ID
),

lsmv_receipt_item_SUBSET as
 (
 
 
select  ARI_REC_ID,ACCOUNT_ID,max(date_modified) date_modified  
FROM
(
select record_id as ARI_REC_ID,ACCOUNT_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE 
    record_id in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
   -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='lsmv_receipt_item')
) where REC_RANK=1
group by 1,2
) ,LSMV_PATIENT_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_PATIENT')
) where
REC_RANK=1
group by 1
) ,LSMV_LITERATURE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_LITERATURE')
) where
REC_RANK=1
group by 1
),LSMV_PRIMARYSOURCE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_PRIMARYSOURCE')
) where
REC_RANK=1
group by 1
)
,LSMV_DRUG_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_drug WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG')
) where
REC_RANK=1
group by 1
)
,LSMV_REACTION_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_REACTION')
) where
REC_RANK=1
group by 1
),LSMV_SOURCE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_SOURCE')
) where
REC_RANK=1
group by 1
),LSMV_PATIENT_DEATH_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_PATIENT_DEATH')
) where
REC_RANK=1
group by 1
),LSMV_ACTIVE_SUBSTANCE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_ACTIVE_SUBSTANCE')
) where
REC_RANK=1
group by 1
),LSMV_DRUG_REACT_RELATEDNESS_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG_REACT_RELATEDNESS')
) where
REC_RANK=1
group by 1
)
,LSMV_PATIENT_MED_HIST_EPISODE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_PATIENT_MED_HIST_EPISODE')
) where
REC_RANK=1
group by 1
)
,LSMV_STUDY_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_STUDY WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_STUDY')
) where
REC_RANK=1
group by 1
),LSMV_PATIENT_AUTOPSY_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_AUTOPSY WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_PATIENT_AUTOPSY')
) where
REC_RANK=1
group by 1
),LSMV_MESSAGE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_MESSAGE')
) where
REC_RANK=1
group by 1
),LSMV_DEATH_CAUSE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEATH_CAUSE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DEATH_CAUSE')
) where
REC_RANK=1
group by 1
),LSMV_REPORTDUPLICATE_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select FK_ASR_REC_ID as ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_REPORTDUPLICATE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_REPORTDUPLICATE')
) where
REC_RANK=1
group by 1
),LSMV_DRUG_APPROVAL_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG_APPROVAL')
) where
REC_RANK=1
group by 1
),LSMV_DRUG_INDICATION_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG_INDICATION')
) where
REC_RANK=1
group by 1
),LSMV_SAFETY_REPORT_SUBSET as
(
select  ARI_REC_ID,record_id,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_SAFETY_REPORT')
) where
REC_RANK=1
group by 1,2
)

,LSMV_LANG_REDUCTED_SUBSET as
(
SELECT C.ARI_REC_ID,GREATEST(NVL(A.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(B.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(C.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) As DATE_MODIFIED
FROM
(
select  ENTITY_RECORD_ID,FK_ASR_REC_ID,max(date_modified) date_modified  
FROM
(
select ENTITY_RECORD_ID,FK_ASR_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_LANG_REDUCTED 
) where
REC_RANK=1
group by 1,2
) A JOIN 
(
select  record_id,max(date_modified) date_modified  
FROM
(
select record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE 
) where
REC_RANK=1
group by 1
) B on A.ENTITY_RECORD_ID = B.RECORD_ID 
Join LSMV_SAFETY_REPORT_SUBSET C on A.FK_ASR_REC_ID=C.RECORD_ID
)
,LSMV_RESEARCH_RPT_SUBSET as
(
select  ARI_REC_ID,record_id,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_RESEARCH_RPT WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_RESEARCH_RPT')
) where
REC_RANK=1
group by 1,2
),LSMV_IMRDF_EVALUATION_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_RESEARCH_RPT')
) where
REC_RANK=1
group by 1
),LSMV_DRUG_REACT_LISTEDNESS_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_LISTEDNESS WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG_REACT_LISTEDNESS')
) where
REC_RANK=1
group by 1
),LSMV_AER_INFO_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    --AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_AER_INFO')
) where
REC_RANK=1
group by 1
),
LSMV_ACCOUNTS_SUBSET as
(
SELECT C.ARI_REC_ID,max(GREATEST(NVL(A.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(C.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))) As DATE_MODIFIED
FROM
(
select  record_id,max(date_modified) date_modified  
FROM
(
select record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACCOUNTS 
) where
REC_RANK=1
group by 1
UNION all
select  record_id,max(date_modified) date_modified  
FROM
(
select record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PARTNER 
) where
REC_RANK=1
group by 1
) A 
Join lsmv_receipt_item_SUBSET C on A.RECORD_ID=C.Account_id
group by 1
)



 SELECT lsmv_receipt_item_SUBSET.ARI_REC_ID,
 max(to_date(GREATEST(NVL(lsmv_receipt_item_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_LITERATURE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PRIMARYSOURCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_REACTION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_SOURCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_DEATH_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACTIVE_SUBSTANCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_RELATEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_AUTOPSY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_MESSAGE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEATH_CAUSE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_REPORTDUPLICATE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_STUDY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_APPROVAL_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_INDICATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_SAFETY_REPORT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_LANG_REDUCTED_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_RESEARCH_RPT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_IMRDF_EVALUATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_LISTEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_AER_INFO_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACCOUNTS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))

)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(lsmv_receipt_item_SUBSET.ARI_REC_ID,-1)) INTEGRATION_ID
,max(GREATEST(NVL(lsmv_receipt_item_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_LITERATURE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PRIMARYSOURCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_REACTION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_SOURCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_DEATH_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACTIVE_SUBSTANCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_RELATEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_PATIENT_AUTOPSY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_MESSAGE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEATH_CAUSE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_REPORTDUPLICATE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_STUDY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_APPROVAL_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_INDICATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_SAFETY_REPORT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_LANG_REDUCTED_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_RESEARCH_RPT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_IMRDF_EVALUATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_LISTEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_AER_INFO_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACCOUNTS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))

)) as DATE_MODIFIED
FROM lsmv_receipt_item_SUBSET left join LSMV_PATIENT_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_PATIENT_SUBSET.ari_rec_id
left join LSMV_DRUG_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DRUG_SUBSET.ari_rec_id
left join LSMV_REACTION_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_REACTION_SUBSET.ari_rec_id
left join LSMV_LITERATURE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_LITERATURE_SUBSET.ari_rec_id
left join LSMV_PRIMARYSOURCE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_PRIMARYSOURCE_SUBSET.ari_rec_id
left join LSMV_SOURCE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_SOURCE_SUBSET.ari_rec_id
left join LSMV_PATIENT_DEATH_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_PATIENT_DEATH_SUBSET.ari_rec_id
left join LSMV_ACTIVE_SUBSTANCE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_ACTIVE_SUBSTANCE_SUBSET.ari_rec_id
left join LSMV_DRUG_REACT_RELATEDNESS_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DRUG_REACT_RELATEDNESS_SUBSET.ari_rec_id
left join LSMV_PATIENT_MED_HIST_EPISODE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.ari_rec_id
left join LSMV_PATIENT_AUTOPSY_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_PATIENT_AUTOPSY_SUBSET.ari_rec_id
left join LSMV_MESSAGE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_MESSAGE_SUBSET.ari_rec_id
left join LSMV_DEATH_CAUSE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DEATH_CAUSE_SUBSET.ari_rec_id
left join LSMV_REPORTDUPLICATE_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_REPORTDUPLICATE_SUBSET.ari_rec_id
left join LSMV_STUDY_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_STUDY_SUBSET.ari_rec_id
left join LSMV_DRUG_APPROVAL_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DRUG_APPROVAL_SUBSET.ari_rec_id
left join LSMV_DRUG_INDICATION_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DRUG_INDICATION_SUBSET.ari_rec_id
left join LSMV_SAFETY_REPORT_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_SAFETY_REPORT_SUBSET.ari_rec_id
left join LSMV_LANG_REDUCTED_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_LANG_REDUCTED_SUBSET.ari_rec_id
left join LSMV_RESEARCH_RPT_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_RESEARCH_RPT_SUBSET.ari_rec_id
left join LSMV_IMRDF_EVALUATION_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_IMRDF_EVALUATION_SUBSET.ari_rec_id
left join LSMV_DRUG_REACT_LISTEDNESS_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_DRUG_REACT_LISTEDNESS_SUBSET.ari_rec_id
left join LSMV_AER_INFO_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_AER_INFO_SUBSET.ari_rec_id
left join LSMV_ACCOUNTS_SUBSET 
on lsmv_receipt_item_SUBSET.ari_rec_id=LSMV_ACCOUNTS_SUBSET.ari_rec_id
group by 
lsmv_receipt_item_SUBSET.ARI_REC_ID
;


ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CHILD_CASE_FLAG TEXT(40);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_LITRTURE_IN_VANCOUVR_STYLE TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_MIN_THERAPY_START_DATE    TEXT(4000); 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_MAX_THERAPY_END_DATE    TEXT(4000);  	
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SUSPECT_DRUGS_PPD_COMB    TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SUSPECT_PPD     TEXT(4000);          
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_DRUG_NAME_CONCOMITANT    TEXT; 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_RANK_1_EVENT_PRIMARY_SOC  TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_RNK1_EVNT_PRIM_SOC_INTORDR INT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_EVENTS_COMBINED            TEXT(4000); 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_NON_PRIMARY_SOURCES   TEXT(4000);    
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_REFERENCE_TYPE     TEXT(10000);        
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SOURCE              TEXT(250) DEFAULT '-';       
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_STANDARD_PRIMARY_SOURCE   TEXT(1000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_AUTOPSY_DATE     TEXT(80);          
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_DEATH_DATE     TEXT(80);           
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_DATE_OF_BIRTH        TEXT(1200);       
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PATIENT_AGE_IN_YEARS   NUMBER(38,0);    
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SUS_DRG_PPD_SUB        TEXT(4000);   
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PRIMARY_SOURCE    TEXT(140);        
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CONCOMITANT_MDCTN_INGRDNTS TEXT DEFAULT '(-)';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_E2B_REPORT_DUPLICATE_NO   TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_MED_HISTORY_CONTINUING   TEXT(2000) DEFAULT '-';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CAUSE_OF_DEATH     TEXT(4000);        
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_AUTOPSY_DETERMINED  TEXT(4000);       
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CASE_TYPE TEXT(100);                 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CASE_CONFIRMED_BY_HP   TEXT(12);   
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SPECIAL_INTEREST_CASE_FLAG TEXT(3);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_ROLE_PPD_CONCAT TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PARENT_MEDICAL_HISTORY TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PREEX_HEPTC_IMPRMNT TEXT DEFAULT 'No';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_LACTATION_CASE_BROAD TEXT DEFAULT 'No';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN der_off_label_use_broad TEXT DEFAULT 'No';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SUSPECT_STUDY_PRODUCT TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_COD TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PREEX_RNL_IMPRMNT TEXT(9);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PAST_DISEASES_COMBINED TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_COUNTRY_PUBLISHED TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_MT_LITERATURE_REFERENCE TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_INCIDENT_IMDRF TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SL_CLASSIFICATION TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SOLICITED_CASE_SYMBOL VARCHAR(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_APPROVAL_TYPE VARCHAR(16000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CASE_SERIOUSNESS_CRITERIA VARCHAR(200);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_PPD_STUDY_PRDCT_TYPE_COMB VARCHAR(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CASE_UPGRADE_TO_FATAL_FLAG TEXT(10) DEFAULT 'No';
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_SENDER_ORGANIZATION TEXT; 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_CTRY_RPT_TYPE_CLASSIFICATION VARCHAR(100);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_AD_EXP_REACT_DRUG_CHAR VARCHAR(16000);


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED AS
(with drug_subset as (

select ARI_REC_ID,RECORD_ID,BLINDED_PRODUCT_REC_ID
from (
SELECT LSMV_DRUG.ARI_REC_ID,RECORD_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG --where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
		) where rank=1 and	CDC_OPERATION_TYPE IN ('I','U')
)

SELECT DISTINCT LSMV_DRUG.ARI_REC_ID ,
  LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,
  AD.record_id AS SEQ_UNBLINDED
FROM drug_subset LSMV_DRUG				 
LEFT JOIN
  drug_subset AD
ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.RECORD_ID
);

                  

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP;
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP as 
select record_id,ARI_REC_ID,FK_AD_REC_ID from 
			(select record_id,ARI_REC_ID,FK_AD_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP;
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP as 
select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,STUDY_PRODUCT_TYPE,SPR_ID from 
			(select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE,STUDY_PRODUCT_TYPE,CASE WHEN COALESCE(SPR_ID,'')= '' THEN '-9999' ELSE SPR_ID END AS SPR_ID
				FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;
                  

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRIMARYSOURCE_SUBSET_TMP;
CREATE TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRIMARYSOURCE_SUBSET_TMP AS 
SELECT RECORD_ID,ARI_REC_ID,SPR_ID,REPORTER_TYPE,REPORTER_TYPE_NF,REPORTER_OR_CONTACT,PRIMARY_REPORTER FROM 
			(SELECT RECORD_ID,ARI_REC_ID,ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK 
				,CDC_OPERATION_TYPE,CASE WHEN COALESCE(SPR_ID,'')= '' THEN '-9999' ELSE SPR_ID END AS SPR_ID,REPORTER_TYPE,REPORTER_TYPE_NF,REPORTER_OR_CONTACT,PRIMARY_REPORTER
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE 
				WHERE ARI_REC_ID IN (SELECT  ARI_REC_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP) 
			) WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U') ;



drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP as 
select * from 
(select RECORD_ID,ARI_REC_ID,reactmeddrallt_code,reactmeddrapt_code,
					  CDC_OPERATION_TYPE,event_type,REACTIONTERM,RANK_ORDER
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP) 
											 
										)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
;



DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
create TEMPORARY table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE from 
    (
              SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE
              ,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
                                                                        order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
                                                                                       FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1003','9037','8008','1015','1001','1004','709','8008')
									
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,EMDR_CODE,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,
                                   Coalesce(SPR_ID,'-9999') SPR_ID,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1; 


                  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_CHILD_CASE_FLAG=LS_DB_CASE_DER_FINAL.DER_CHILD_CASE_FLAG
FROM (
select distinct A.ari_rec_id,
	case when b.ARI_REC_ID is null then 'No' ELSE 'Yes' end DER_CHILD_CASE_FLAG 
	from (		
	select  ARI_REC_ID FROM
	   (select record_id ARI_REC_ID,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM
			where record_id in (select DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
	
	) A left join (	
select ARI_REC_ID,PATAGEGROUP,CDC_OPERATION_TYPE,row_number() over (partition by ARI_REC_ID order by CDC_OPERATION_TIME desc) rank
 from ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
) B ON A.ARI_REC_ID=B.ARI_REC_ID and rank=1 AND   CDC_OPERATION_TYPE IN ('I','U') and PATAGEGROUP IN ('1','2','3')

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_LITRTURE_IN_VANCOUVR_STYLE=case when LS_DB_CASE_DER_FINAL.DER_LITRTURE_IN_VANCOUVR_STYLE is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_LITRTURE_IN_VANCOUVR_STYLE)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_LITRTURE_IN_VANCOUVR_STYLE,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_LITRTURE_IN_VANCOUVR_STYLE end
	 else '-' end
FROM (select ARI_REC_ID,
	listagg(DER_LITRTURE_IN_VANCOUVR_STYLE,'\r\n') within group (order by ARI_REC_ID) DER_LITRTURE_IN_VANCOUVR_STYLE
FROM(
      
WITH LSMV_PRIMARYSOURCE_SUBSET as (		select ARI_REC_ID,
									 COALESCE(LAST_NAME,'') AS LAST_NAME,
                                     SEQ_PERSON,
                                     REPLACE((REPLACE((TRANSLATE(INITCAP(COALESCE(FIRST_NAME,'')),'Aabcdefghijklmnopqrstuvwxyz','A')),'.','')),'-','') FIRST_NAME_INIT,
                                     COALESCE(FIRST_NAME,'') AS FIRST_NAME
					FROM
					(
					select ARI_REC_ID,
							RECORD_ID As SEQ_PERSON,
							REPORTER_OR_CONTACT,
							CASE 
								WHEN REPORTERFAMILYNAME IS NULL THEN 
								LSMV_PRIMARYSOURCE.REPORTERFAMILYNAME_NF ELSE LSMV_PRIMARYSOURCE.REPORTERFAMILYNAME  END LAST_NAME,
							CASE 
								WHEN LSMV_PRIMARYSOURCE.REPORTERGIVENNAME IS NULL THEN 
								LSMV_PRIMARYSOURCE.REPORTERGIVENNAME_NF ELSE LSMV_PRIMARYSOURCE.REPORTERGIVENNAME
							END FIRST_NAME
							,CDC_OPERATION_TYPE
							,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM  ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE 
					WHERE ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP) 
					
					)  
						where rank=1 and REPORTER_OR_CONTACT='03' and CDC_OPERATION_TYPE IN ('I','U') 
					
					) 

SELECT ARI_REC_ID,
       CASE
         WHEN LENGTH(DER_LITRTURE_IN_VANCOUVR_STYLE) > 4000 THEN SUBSTR(DER_LITRTURE_IN_VANCOUVR_STYLE,0,3996) || ' ...'
         ELSE SUBSTR(DER_LITRTURE_IN_VANCOUVR_STYLE,0,4000)
       END AS DER_LITRTURE_IN_VANCOUVR_STYLE
FROM (
SELECT ARI_REC_ID,
       --SEQ_LITERATURE,
       CASE
         WHEN (DER_LITRTURE_IN_VANCOUVR_STYLE = '. ' or DER_LITRTURE_IN_VANCOUVR_STYLE =NULL) THEN '-'
         ELSE DER_LITRTURE_IN_VANCOUVR_STYLE
       END AS DER_LITRTURE_IN_VANCOUVR_STYLE
FROM (SELECT ARI_REC_ID,
             CASE
               WHEN LENGTH(DER_LITRTURE_IN_VANCOUVR_STYLE) > 4000 THEN SUBSTRING(DER_LITRTURE_IN_VANCOUVR_STYLE,0,3997) || '...'
               ELSE SUBSTRING(DER_LITRTURE_IN_VANCOUVR_STYLE,0,4000)
             END AS DER_LITRTURE_IN_VANCOUVR_STYLE
      FROM (SELECT ARI_REC_ID,
                   --SEQ_LITERATURE,
                   CASE
                     WHEN DER_LITRTURE_IN_VANCOUVR_STYLE = '. ' THEN '-'
                     ELSE DER_LITRTURE_IN_VANCOUVR_STYLE
                   END AS DER_LITRTURE_IN_VANCOUVR_STYLE
            FROM (SELECT AUTHOR_Q.ARI_REC_ID ARI_REC_ID,
                         --AUTHOR_Q.SEQ_LITERATURE SEQ_LITERATURE,
                         CASE
                           WHEN LIT_Q.ARTICLE IS NULL AND LIT_Q.JOURNAL IS NULL AND LIT_Q.PUB_DATE IS NULL AND LIT_Q.VOLUME IS NULL AND LIT_Q.EDITION IS NULL AND LIT_Q.PAGE_FROM IS NULL AND LIT_Q.PAGE_TO IS NULL AND LENGTH(AUTHOR_Q.AUTHOR_CONC) > 2 THEN SUBSTRING(AUTHOR_Q.AUTHOR_CONC,1,LENGTH(AUTHOR_Q.AUTHOR_CONC) - 2) || '. '
                           ELSE DECODE (AUTHOR_Q.AUTHOR_CONC,NULL,'',AUTHOR_Q.AUTHOR_CONC || '. ') || DECODE (LIT_Q.ARTICLE,NULL,'',LIT_Q.ARTICLE || '. ') || DECODE (LIT_Q.JOURNAL,NULL,'',LIT_Q.JOURNAL || '. ') || DECODE (LIT_Q.PUB_DATE,NULL,'',LIT_Q.PUB_DATE || ';  ') || DECODE (LIT_Q.EDITION,NULL,'',LIT_Q.EDITION || '') || DECODE (LIT_Q.VOLUME,NULL,'','(' ||LIT_Q.VOLUME || ')') || DECODE (LIT_Q.PAGE_FROM,NULL,'',':' ||LIT_Q.PAGE_FROM || '-') || DECODE (LIT_Q.PAGE_TO,NULL,'',LIT_Q.PAGE_TO || '.')
                         END AS DER_LITRTURE_IN_VANCOUVR_STYLE
                  FROM (SELECT ARI_REC_ID,
                               --SEQ_LITERATURE,
                               CASE
                                 WHEN AUTHOR_COUNT > 6 THEN SUBSTRING(AUTHOR_CONC,0,REGEXP_INSTR (AUTHOR_CONC,',',1,6)) || ' et al'
                                 ELSE (
                                   CASE
                                     WHEN AUTHOR_CONC = ' , ' THEN ''
                                     ELSE AUTHOR_CONC
                                   END )
                               END AUTHOR_CONC
                        FROM (SELECT AP.ARI_REC_ID,
                                     MAX(B.AUTHOR_COUNT) AS AUTHOR_COUNT,
                                     --AP.SEQ_LITERATURE,
                                     LISTAGG(AP.LAST_NAME || ' ' ||AP.FIRST_NAME_INIT || ', ') WITHIN GROUP(ORDER BY AP.SEQ_PERSON) AUTHOR_CONC
                              FROM LSMV_PRIMARYSOURCE_SUBSET AP,
                                   (SELECT ARI_REC_ID,SEQ_PERSON,
                                           COUNT(SEQ_PERSON) OVER (PARTITION BY (ARI_REC_ID)) AS AUTHOR_COUNT
                                    FROM LSMV_PRIMARYSOURCE_SUBSET) AS B
                              WHERE AP.ARI_REC_ID = B.ARI_REC_ID (+) and AP.SEQ_PERSON = B.SEQ_PERSON (+)
                              GROUP BY AP.ARI_REC_ID,
                                       AUTHOR_COUNT)) AUTHOR_Q,
                       (select 	ARI_REC_ID,		ARTICLE	,JOURNAL,PUB_DATE,EDITION,PAGE_FROM,PAGE_TO,VOLUME
		FROM
		(				select 		
								AL.ARI_REC_ID AS ARI_REC_ID,
								AL.ARTICLE_TITLE ARTICLE,
								AL.JOURNAL_TITLE JOURNAL,
								CASE
                                WHEN to_char(to_date(AL.PUB_DATE),'YYYY-MM-DD') LIKE '%-%-%' THEN LEFT(to_char(to_date(AL.PUB_DATE),'YYYY-MM-DD'), 4)
                                ELSE to_char(to_date(AL.PUB_DATE),'YYYY-MM-DD')
                                END As PUB_DATE,
								AL.EDITION As EDITION,
								AL.PAGE_FROM As PAGE_FROM,
								AL.PAGE_TO as PAGE_TO,
								AL.ISSUE AS VOLUME	,
								AL.CDC_OPERATION_TYPE								
								,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
		FROM  ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE AL
		WHERE   ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
		 
		) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		) LIT_Q
                  WHERE AUTHOR_Q.ARI_REC_ID = LIT_Q.ARI_REC_ID (+)))))
) group by ari_rec_id
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

-- DER_MIN_THERAPY_START_DATE
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_MIN_THERAPY_START_DATE=case when length(LS_DB_CASE_DER_FINAL.DER_MIN_THERAPY_START_DATE)>=4000 
                            then substring(LS_DB_CASE_DER_FINAL.DER_MIN_THERAPY_START_DATE,1,3996)||' ...' 
		else LS_DB_CASE_DER_FINAL.DER_MIN_THERAPY_START_DATE end  

FROM (
SELECT ARI_REC_ID,LISTAGG(DER_MIN_THERAPY_START_DATE,'\r\n')within group (ORDER BY RANK_ORDER) DER_MIN_THERAPY_START_DATE
FROM
  (SELECT distinct AP.ARI_REC_ID,
    AP.RECORD_ID,
    AP.RANK_ORDER ,
    'D'
    ||AP.RANK_ORDER
    ||': '
    ||
	CASE WHEN APT.DER_MIN_THERAPY IS NULL 
	THEN '-'
	ELSE UPPER(APT.DER_MIN_THERAPY)
	END AS DER_MIN_THERAPY_START_DATE
  FROM (select ARI_REC_ID,
    RECORD_ID,
    RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE,
	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where  
		ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
    
	) AP LEFT JOIN
    (SELECT ARI_REC_ID,FK_AD_REC_ID AS SEQ_PRODUCT,
	CASE
       WHEN DRUGSTARTDATEFMT IS NULL     THEN TO_CHAR((DRUGSTARTDATE),'DD-MON-YYYY')
       WHEN DRUGSTARTDATEFMT in (0,102)  THEN TO_CHAR((DRUGSTARTDATE),'DD-MON-YYYY')
       WHEN DRUGSTARTDATEFMT in (1,602)  THEN TO_CHAR((DRUGSTARTDATE),'YYYY')
       WHEN DRUGSTARTDATEFMT in (2,610)  THEN TO_CHAR((DRUGSTARTDATE),'MON-YYYY')
       WHEN DRUGSTARTDATEFMT in (3,4,5,6,7,8,9,204,611,203)       THEN TO_CHAR((DRUGSTARTDATE),'DD-MON-YYYY')
     END DER_MIN_THERAPY
    FROM (select ARI_REC_ID,FK_AD_REC_ID,DRUGSTARTDATEFMT,DRUGSTARTDATE,CDC_OPERATION_TYPE
			, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where 
		ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
	WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
	and 
	(ARI_REC_ID,FK_AD_REC_ID,DRUGSTARTDATE) IN
		(select ARI_REC_ID,
			FK_AD_REC_ID,	
			MIN(DRUGSTARTDATE) As MIN_START_THERAPY_DATE
			FROM      
					(select ARI_REC_ID,FK_AD_REC_ID,DRUGSTARTDATE,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where 
					ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
					
					)
			WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
		   group by 1,2
        )
    ) APT ON    AP.ARI_REC_ID        =APT.ARI_REC_ID
  AND AP.RECORD_ID   =APT.SEQ_PRODUCT
  where AP.rank=1 and AP.CDC_OPERATION_TYPE IN ('I','U') and AP.DRUGCHARACTERIZATION IN ('1','3')                  
                  and AP.ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
  AND  AP.ARI_REC_ID||'-'||AP.RECORD_ID NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
  ) group by 1
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

--- DERIVED_MAX_THERAPY_END_DATE
                                 
                                   
                                   
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_MAX_THERAPY_END_DATE=
case when length(LS_DB_CASE_DER_FINAL.DER_MAX_THERAPY_END_DATE)>=4000 then substring(LS_DB_CASE_DER_FINAL.DER_MAX_THERAPY_END_DATE,1,3996)||' ...' 
		else LS_DB_CASE_DER_FINAL.DER_MAX_THERAPY_END_DATE end
  
FROM (
SELECT ARI_REC_ID,LISTAGG(DER_MAX_THERAPY_END_DATE,'\r\n')within group (ORDER BY RANK_ORDER) DER_MAX_THERAPY_END_DATE
FROM
  (SELECT distinct AP.ARI_REC_ID,
    AP.RECORD_ID,
    AP.RANK_ORDER ,
    'D'
    ||AP.RANK_ORDER
    ||': '
    ||
	CASE WHEN APT.DER_MAX_THERAPY IS NULL 
	THEN '-'
	ELSE UPPER(APT.DER_MAX_THERAPY)
	END AS DER_MAX_THERAPY_END_DATE
  FROM (select ARI_REC_ID,
    RECORD_ID,
    RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
	,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where 
		ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
    
	)AP LEFT JOIN
    (SELECT ARI_REC_ID,FK_AD_REC_ID AS SEQ_PRODUCT,
	CASE
       WHEN DRUGENDDATEFMT IS NULL  THEN    TO_CHAR((DRUGENDDATE),'DD-MON-YYYY')
       WHEN DRUGENDDATEFMT in (0,102)  THEN TO_CHAR((DRUGENDDATE),'DD-MON-YYYY')
       WHEN DRUGENDDATEFMT in (1,602)  THEN TO_CHAR((DRUGENDDATE),'YYYY')
       WHEN DRUGENDDATEFMT in (2,610)  THEN TO_CHAR((DRUGENDDATE),'MON-YYYY')
       WHEN DRUGENDDATEFMT in (3,4,5,6,7,8,9,204,611,203)       THEN TO_CHAR((DRUGENDDATE),'DD-MON-YYYY')
     END DER_MAX_THERAPY
    FROM 
	    (select ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATEFMT,DRUGENDDATE,CDC_OPERATION_TYPE
			, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where 
		ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
	WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
	AND 
	(ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATE) IN
		(select ARI_REC_ID,
			FK_AD_REC_ID,	
			MAX(DRUGENDDATE) As MAX_END_THERAPY_DATE
			FROM (select ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATEFMT,DRUGENDDATE,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where 
					ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
					
				)
			WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')	
		group by 1,2
      )
	  
    ) APT ON  AP.ARI_REC_ID        =APT.ARI_REC_ID
  AND AP.RECORD_ID   =APT.SEQ_PRODUCT
  where AP.rank=1 and AP.CDC_OPERATION_TYPE IN ('I','U') 
  and AP.DRUGCHARACTERIZATION IN ('1','3') and AP.ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
   AND
  AP.ARI_REC_ID||'-'||AP.RECORD_ID NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)
  ) group by 1
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_SUSPECT_DRUGS_PPD_COMB=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_PPD_COMB is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_PPD_COMB)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_PPD_COMB,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_PPD_COMB end
	 else null end
  FROM(
 select  ari_rec_id,listagg(DER_SUSPECT_DRUGS_PPD_COMB,' | ') within group (order by ari_rec_id,class desc) as DER_SUSPECT_DRUGS_PPD_COMB
  FROM
(  
select ari_rec_id,class,case when class='BN' then '@\n'||listagg(PPD_VARIATION_COMB,'|') within group (order by ari_rec_id,AUTO_RANK,class)
else listagg(PPD_VARIATION_COMB,'|') within group (order by ari_rec_id,AUTO_RANK,class) end DER_SUSPECT_DRUGS_PPD_COMB from
  
(
(SELECT ari_rec_id,
       RANK_ORDER as AUTO_RANK,
       PREFERED_PRODUCT_DESCRIPTION as PPD_VARIATION_COMB ,
	   'UN' class
         FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,PREFERED_PRODUCT_DESCRIPTION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
		WHERE rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
		and DRUGCHARACTERIZATION in ('1','3')
        and PREFERED_PRODUCT_DESCRIPTION  is not null
        and ARI_REC_ID||'-'||RECORD_ID not IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
order by
	ari_rec_id,
	record_id	
)
union all 
(SELECT ari_rec_id,
       RANK_ORDER as AUTO_RANK,
       PREFERED_PRODUCT_DESCRIPTION as PPD_VARIATION_COMB ,
	   'BN' class
         FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,PREFERED_PRODUCT_DESCRIPTION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
		WHERE rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
		and DRUGCHARACTERIZATION in ('1','3')
        and PREFERED_PRODUCT_DESCRIPTION  is not null
        and ARI_REC_ID||'-'||RECORD_ID  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
order by
	ari_rec_id,
	record_id)
)
  group by ari_rec_id,class 
  order by  ari_rec_id,class desc  
)group by ari_rec_id
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
	



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_SUSPECT_PPD=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_PPD is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_PPD)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_PPD,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_SUSPECT_PPD end
	 else null end
  FROM(
select ari_rec_id,listagg(DER_SUSPECT_PPD,'\r\n') within group (order by class desc) DER_SUSPECT_PPD
FROM  
(select ari_rec_id,class,case when class='BN' then '@\n'||listagg(DER_SUSPECT_PPD,'|') within group (order by ari_rec_id,AUTO_RANK,class)
else listagg(DER_SUSPECT_PPD,'|') within group (order by ari_rec_id,AUTO_RANK,class) end DER_SUSPECT_PPD from
  
(
(SELECT ari_rec_id,
       RANK_ORDER as AUTO_RANK,
	    CASE 
			WHEN PREFERED_PRODUCT_DESCRIPTION IS NULL THEN 'D'||RANK_ORDER||': '||'(-)'
			ELSE  'D'||RANK_ORDER||': '||PREFERED_PRODUCT_DESCRIPTION 
		END AS DER_SUSPECT_PPD,
	   'UN' class
         FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,PREFERED_PRODUCT_DESCRIPTION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
		WHERE rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
	and DRUGCHARACTERIZATION in ('1','3')
        -- and PREFERED_PRODUCT_DESCRIPTION  is not null
        and ARI_REC_ID||'-'||RECORD_ID not IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
order by
	ari_rec_id,
	record_id	
)
union all 
(SELECT ari_rec_id,
       RANK_ORDER as AUTO_RANK,
       CASE 
			WHEN PREFERED_PRODUCT_DESCRIPTION IS NULL THEN 'D'||RANK_ORDER||': '||'(-)'
			ELSE  'D'||RANK_ORDER||': '||PREFERED_PRODUCT_DESCRIPTION 
		END AS DER_SUSPECT_PPD ,
	   'BN' class
         FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,PREFERED_PRODUCT_DESCRIPTION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		)
		WHERE rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
	and DRUGCHARACTERIZATION in ('1','3')
        -- and PREFERED_PRODUCT_DESCRIPTION  is not null
        and ARI_REC_ID||'-'||RECORD_ID  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is  null) 
order by
	ari_rec_id,
	record_id)
)
  group by ari_rec_id,class 
  order by  ari_rec_id,class desc
 ) group by ari_rec_id
             ) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

   

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_DRUG_NAME_CONCOMITANT=LS_DB_CASE_DER_FINAL.DER_DRUG_NAME_CONCOMITANT   

FROM (
SELECT ari_rec_id
         ,listagg( Coalesce(MEDICINALPRODUCT,'-'),'\n') within group (order by ari_rec_id,RANK_ORDER) as DER_DRUG_NAME_CONCOMITANT 
  
  FROM (
		select ARI_REC_ID,RECORD_ID,MEDICINALPRODUCT,RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
    and DRUGCHARACTERIZATION='2'
	and ARI_REC_ID||'-'||RECORD_ID NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
  group by ari_rec_id
 ) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

                    
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_RANK_1_EVENT_PRIMARY_SOC=LS_DB_CASE_DER_FINAL.DER_RANK_1_EVENT_PRIMARY_SOC,
LS_DB_CASE_DER_tmp.DER_RNK1_EVNT_PRIM_SOC_INTORDR=LS_DB_CASE_DER_FINAL.DER_RNK1_EVNT_PRIM_SOC_INTORDR
FROM (
SELECT distinct AR.ARI_REC_ID,
 DM.PRIMARY_SOC_NAME As DER_RANK_1_EVENT_PRIMARY_SOC,
 DM.INTERNATIONAL_SOC_ORDER  AS DER_RNK1_EVNT_PRIM_SOC_INTORDR 
FROM
(
select ARI_REC_ID,LLT_CODE,PT_CODE
FROM
(select ARI_REC_ID,
REACTMEDDRALLT_CODE As  LLT_CODE,
REACTMEDDRAPT_CODE AS PT_CODE,
RANK_ORDER as AUTO_RANK,
CDC_OPERATION_TYPE,
row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
from 
${stage_db_name}.${stage_schema_name}.LSMV_REACTION
where ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)

) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 
AND AUTO_RANK=1
) AR JOIN 
(
select LLT_CODE,PT_CODE,PRIMARY_SOC_NAME,INTERNATIONAL_SOC_ORDER from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD
where EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MEDDRA_VERSION =(select MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
AND LANGUAGE_CODE='001' AND PRIMARY_SOC_FG='Y'
) DM
ON  TRY_TO_NUMBER(AR.PT_CODE)=DM.PT_CODE
AND nvl(TRY_TO_NUMBER(AR.LLT_CODE),TRY_TO_NUMBER(AR.PT_CODE)) = DM.LLT_CODE
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_EVENTS_COMBINED=case when LS_DB_CASE_DER_FINAL.DER_EVENTS_COMBINED is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_EVENTS_COMBINED)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_EVENTS_COMBINED,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_EVENTS_COMBINED end
	 else null end
FROM (
SELECT 
	A.ARI_REC_ID,
	listagg(B.PT_NAME,' | ') within group (order by AUTO_RANK) DER_EVENTS_COMBINED
FROM  
  (select ARI_REC_ID,LLT_CODE,PT_CODE,AUTO_RANK
FROM
(select ARI_REC_ID,
REACTMEDDRALLT_CODE As  LLT_CODE,
REACTMEDDRAPT_CODE AS PT_CODE,
RANK_ORDER as AUTO_RANK,
CDC_OPERATION_TYPE,
row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 

 from 
${stage_db_name}.${stage_schema_name}.LSMV_REACTION
where ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)

) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U') 
)  A 
JOIN (	SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	from 
	(
	SELECT  RECORD_ID ,
  PT_CODE, 
  PT_NAME, 
  MEDDRA_VERSION,
  CDC_OPERATION_TYPE,
  row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank  
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM

) WHERE rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 
       AND MEDDRA_VERSION IN (select 'v.'||MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
) B on TRY_TO_NUMBER(A.PT_CODE)=B.PT_CODE

   
  GROUP BY A.ARI_REC_ID
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_NON_PRIMARY_SOURCES=CASE WHEN LENGTH(LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES) >= 4000 
                                                            THEN SUBSTRING(LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES,1,3996) || ' ...'
	ELSE LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES END

FROM (
select AES.ARI_REC_ID,
       LISTAGG( distinct D.DECODE,(' | ')) WITHIN GROUP (ORDER BY D.DECODE)  DER_NON_PRIMARY_SOURCES
FROM
    (
 select ARI_REC_ID,SOURCE,SPR_ID,RECORD_ID from 
  (select ARI_REC_ID,SOURCE,COALESCE(SPR_ID,'-9999') AS SPR_ID,RECORD_ID,PRIMARY_SOURCE_FLAG,CDC_OPERATION_TYPE
   ,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
    WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
	
  ) where rnk=1  and  CDC_OPERATION_TYPE IN ('I','U')  and (PRIMARY_SOURCE_FLAG='0' or PRIMARY_SOURCE_FLAG is null)
) AES
JOIN
(select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID
									,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					) D ON 
AES.SOURCE=D.CODE and AES.SPR_ID=D.SPR_ID
group by  ari_rec_id
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

          
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_REFERENCE_TYPE=CASE WHEN LENGTH(LS_DB_CASE_DER_FINAL.DER_REFERENCE_TYPE ) >= 4000 THEN SUBSTRING(LS_DB_CASE_DER_FINAL.DER_REFERENCE_TYPE,1,3996) || ' ...'
	ELSE LS_DB_CASE_DER_FINAL.DER_REFERENCE_TYPE END
FROM (
WITH CD_WITH AS (
select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9060')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 


)


SELECT ARI_REC_ID,
		LISTAGG(DER_REFERENCE_TYPE,' | ') WITHIN GROUP (ORDER BY RECORD_ID) AS  DER_REFERENCE_TYPE 
		FROM 
		(SELECT  
		ARI_REC_ID,
		RECORD_ID,
		COALESCE(CD_WITH.DE,'-') || ': ' || COALESCE(LS_DB_SOURCE.IDENTIFICATION_NUMBER,'-') AS DER_REFERENCE_TYPE
		FROM(
		select ARI_REC_ID,RECORD_ID,IDENTIFICATION_NUMBER,REFERENCE_TYPE,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		) LS_DB_SOURCE 
		LEFT Join CD_WITH on 
		LS_DB_SOURCE.REFERENCE_TYPE=CD_WITH.CD
		where rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
		) group  by ARI_REC_ID
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_NON_PRIMARY_SOURCES=CASE WHEN LENGTH(LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES) >= 4000 THEN SUBSTRING(LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES,1,3996) || ' ...'
	ELSE LS_DB_CASE_DER_FINAL.DER_NON_PRIMARY_SOURCES END

FROM (
select AES.ARI_REC_ID,
       LISTAGG( D.DECODE,(' | ')) WITHIN GROUP (ORDER BY AES.RECORD_ID)  DER_NON_PRIMARY_SOURCES
FROM
(
select ARI_REC_ID,SOURCE,SPR_ID,RECORD_ID
	FROM 
	(
		select ARI_REC_ID,RECORD_ID,SOURCE,PRIMARY_SOURCE_FLAG,coalesce(SPR_ID,'-9999') SPR_ID,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)	where rnk=1 and  CDC_OPERATION_TYPE IN ('I','U')
    and PRIMARY_SOURCE_FLAG='0' or PRIMARY_SOURCE_FLAG is null
) AES
JOIN
(select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					) D ON 
AES.SOURCE=D.CODE and AES.SPR_ID=D.SPR_ID
group by  ari_rec_id
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

   
 -- DER_SOURCE                               

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_SOURCE=COALESCE(LS_DB_CASE_DER_FINAL.DER_SOURCE,'-')

FROM (
SELECT ARI_REC_ID,DER_SOURCE FROM
(

WITH TEMP AS
(
select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,coalesce(SPR_ID,'-9999') As SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1
	) where rank=1 AND CODE IN ('1','02','25')
)
SELECT distinct ari_rec_id,
 DECODE  DER_SOURCE,
DENSE_RANK() OVER (PARTITION BY ari_rec_id ORDER BY ari_rec_id,record_id) AS PP
FROM 
	(
		select ARI_REC_ID,RECORD_ID,SOURCE,SPR_ID,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)	
 A left outer join temp on  A.SOURCE=TEMP.CODE
AND COALESCE(A.SPR_ID,'-9999')=temp.SPR_ID
WHERE A.rnk=1 and  A.CDC_OPERATION_TYPE IN ('I','U') AND SOURCE IN ('1','02','25')
) D WHERE PP=1) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

                     
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_STANDARD_PRIMARY_SOURCE=CASE WHEN LENGTH(LS_DB_CASE_DER_FINAL.DER_STANDARD_PRIMARY_SOURCE) >= 4000 
    THEN SUBSTRING(LS_DB_CASE_DER_FINAL.DER_STANDARD_PRIMARY_SOURCE,1,3996) || ' ...'
	ELSE LS_DB_CASE_DER_FINAL.DER_STANDARD_PRIMARY_SOURCE END

FROM (
select AES.ARI_REC_ID,
       LISTAGG( D.DECODE,(' | ')) WITHIN GROUP (ORDER BY AES.RECORD_ID)  DER_STANDARD_PRIMARY_SOURCE
FROM (
select ARI_REC_ID,SOURCE,COALESCE(SPR_ID,'-9999') SPR_ID,RECORD_ID
	FROM (
		select ARI_REC_ID,RECORD_ID,SOURCE,SPR_ID,PRIMARY_SOURCE_FLAG,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
    and PRIMARY_SOURCE_FLAG='1'
) AES
JOIN
(select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,
                                   Coalesce(SPR_ID,'-9999') SPR_ID,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					) D ON
AES.SOURCE=D.CODE AND AES.SPR_Id=D.SPR_Id
group by  ari_rec_id
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
   


 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_AUTOPSY_DATE=LS_DB_CASE_DER_FINAL.DER_AUTOPSY_DATE  , 
LS_DB_CASE_DER_tmp.DER_DEATH_DATE=LS_DB_CASE_DER_FINAL.DER_DEATH_DATE
FROM (SELECT 	distinct ARI_REC_ID,
		case  when  PATAUTOPSYDATEFMT is null  then to_char(PATAUTOPSYDATE,'DD-MON-YYYY')
	  when  PATAUTOPSYDATEFMT in (0,102)  then to_char(PATAUTOPSYDATE,'DD-MON-YYYY')
	  when  PATAUTOPSYDATEFMT in (1,602)  then to_char(PATAUTOPSYDATE,'YYYY')
	  when  PATAUTOPSYDATEFMT in (2,610)  then to_char(PATAUTOPSYDATE,'MON-YYYY')
	  when  PATAUTOPSYDATEFMT in (3,4,5,6,7,8,9,204,203)  then to_char(PATAUTOPSYDATE,'DD-MON-YYYY')
	end DER_AUTOPSY_DATE,
	case when  PATDEATHDATE_NF is not null then UPPER(PATDEATHDATE_NF)
	  when  PATDEATHDATEFMT is null  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY'))
	  when  PATDEATHDATEFMT in (0,7,8,9,102)  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY'))
	  when  PATDEATHDATEFMT in (1,602)  then to_char(PATDEATHDATE,'YYYY')
	  when  PATDEATHDATEFMT in (2,610)  then UPPER(to_char(PATDEATHDATE,'MON-YYYY'))
	  when  PATDEATHDATEFMT in (3,204)  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY'))
	  when  PATDEATHDATEFMT in (4,204)  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY hh24'))
	  when  PATDEATHDATEFMT in (5,204)  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY hh24:mi'))
	  when  PATDEATHDATEFMT in (6,204,203)  then UPPER(to_char(PATDEATHDATE,'DD-MON-YYYY hh24:mi:ss'))
	end DER_DEATH_DATE
	
FROM               
      (select ari_rec_id,PATAUTOPSYDATEFMT,PATAUTOPSYDATE,PATDEATHDATE_NF,PATDEATHDATEFMT,PATDEATHDATE
				,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH where 
					ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		) where rank=1 and CDC_OPERATION_TYPE IN ('I','U')

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_DATE_OF_BIRTH=LS_DB_CASE_DER_FINAL.DER_DATE_OF_BIRTH
FROM (SELECT 	distinct ARI_REC_ID,
	case  when  PATDOB_NF is not null then UPPER(PATDOB_NF)
	  when  PATDOB_FMT is null  then UPPER(to_char(PATDOB,'DD-MON-YYYY'))
	  when  PATDOB_FMT in (0,102)  then UPPER(to_char(PATDOB,'DD-MON-YYYY'))
	  when  PATDOB_FMT in (1,602)  then UPPER(to_char(PATDOB,'YYYY'))
	  when  PATDOB_FMT in (2,610)  then UPPER(to_char(PATDOB,'MON-YYYY'))
	  when  PATDOB_FMT in (3,4,5,6,7,8,9,204,203)  then UPPER(to_char(PATDOB,'DD-MON-YYYY'))
	end DER_DATE_OF_BIRTH
	
FROM               
      (select distinct ari_rec_id,PATDOB_FMT,PATDOB_NF,PATDOB
				,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT where 
					ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		) where rank=1 and CDC_OPERATION_TYPE IN ('I','U')		
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

                          
                                   
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_PATIENT_AGE_IN_YEARS=LS_DB_CASE_DER_FINAL.DER_PATIENT_AGE_IN_YEARS 
FROM (
select ARI_REC_ID,
CASE 
WHEN  AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '801' THEN FLOOR(ROUND(AGE,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '802' THEN FLOOR(ROUND(AGE/12,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '803' THEN FLOOR(ROUND(AGE/52,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '804' THEN FLOOR(ROUND(AGE/365,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '805' THEN FLOOR(ROUND(AGE/8760,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '800' THEN FLOOR(ROUND(AGE*10,3))-5
 WHEN REACTSTARTDATE IS NOT NULL AND DOB IS NOT NULL
THEN (CASE WHEN FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) < 0 
		   THEN FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) 
		   ELSE FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) END)
 ELSE NULL
  END AS DER_PATIENT_AGE_IN_YEARS
FROM

(
select distinct  A.ARI_REC_ID,REACTSTARTDATE,
case when LENGTH(A.PATONSETAGE) > 0 
     then CAST(A.PATONSETAGE AS DOUBLE PRECISION)
     else CAST(null AS DOUBLE PRECISION) end AS AGE,
                A.PATONSETAGEUNIT AS AGE_UNIT,
				A.PATDOB AS DOB

FROM (
		select ARI_REC_ID,PATONSETAGE,PATONSETAGEUNIT,PATDOB
		FROM  (
				select ARI_REC_ID,PATONSETAGE,PATONSETAGEUNIT,PATDOB,CDC_OPERATION_TYPE
				,row_number() over (partition by ARI_REC_ID order by CDC_OPERATION_TIME desc) rank
				from ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
			)
		where rank=1 and CDC_OPERATION_TYPE IN ('I','U')
	) A JOIN

(
select ARI_REC_ID,MIN(REACTSTARTDATE) as REACTSTARTDATE 
FROM  (
		select ARI_REC_ID,REACTSTARTDATE,REACTMEDDRAPT_CODE,CDC_OPERATION_TYPE
		,row_number() over (partition by ARI_REC_ID order by CDC_OPERATION_TIME desc) rank
		from ${stage_db_name}.${stage_schema_name}.LSMV_REACTION where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
		and coalesce(REACTMEDDRAPT_CODE,'-1')  not in ('My nose is bleeding','My back hurts too','EVENT02','EVENT01')
	  )
  where rank=1 and CDC_OPERATION_TYPE IN ('I','U')
group by ARI_REC_ID
) B on A.ari_rec_id=B.ARI_REC_ID
)) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

                                  

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_SUS_DRG_PPD_SUB=case when LS_DB_CASE_DER_FINAL.DER_SUS_DRG_PPD_SUB is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_SUS_DRG_PPD_SUB)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_SUS_DRG_PPD_SUB,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_SUS_DRG_PPD_SUB end
	 else null end

FROM   (
 SELECT ari_rec_id,listagg(DER_SUS_DRG_PPD_SUB,'\r\n') within group (order by class desc) as DER_SUS_DRG_PPD_SUB
                  
from  (                
select ari_rec_id,class,case when class='BN' then '@\n'||listagg(DER_SUS_DRG_PPD_SUB,'|') within group (order by ari_rec_id,AUTO_RANK,class)
else listagg(DER_SUS_DRG_PPD_SUB,'|') within group (order by ari_rec_id,AUTO_RANK,class) end DER_SUS_DRG_PPD_SUB 
from 
(
select * from 
(
select apd.ari_rec_id,
	case
  		when api.gen_concat is null then 'D' || apd.auto_rank ||': '||'(-)'
		else  'D' || apd.auto_rank ||': '|| api.gen_concat end as der_sus_drg_ppd_sub
		,'UN' class
        ,auto_rank
FROM
 (SELECT ari_rec_id,
       record_id SEQ_PRODUCT,
	   RANK_ORDER as AUTO_RANK
  FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
	and DRUGCHARACTERIZATION in ('1','3')
	and ARI_REC_ID||'-'||RECORD_ID NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
  ) APD,
( select ARI_REC_ID,SEQ_PRODUCT 
	,listagg(nvl(ACTIVESUBSTANCENAME,'(-)'),' | ') within group (order by ARI_REC_ID,seq_product) gen_concat
	from (select FK_AD_REC_ID as SEQ_PRODUCT,ARI_REC_ID,
				ACTIVESUBSTANCENAME,CDC_OPERATION_TYPE,
				row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
			FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE 
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		 ) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 	
group by 
	ARI_REC_ID,seq_product
) api
where apd.ARI_REC_ID        =api.ARI_REC_ID(+)
and apd.seq_product   =api.seq_product(+)
  order by apd.auto_rank
  ) NN where der_sus_drg_ppd_sub is not null


union all 
select * from 
(
select apd.ari_rec_id,
	case
  		when api.gen_concat is null then 'D' || apd.auto_rank ||': '||'(-)'
		else  'D' || apd.auto_rank ||': '|| api.gen_concat end as der_sus_drg_ppd_sub
		,'BN' class
        ,auto_rank
FROM
 (SELECT ari_rec_id,
       record_id SEQ_PRODUCT,
	   RANK_ORDER as AUTO_RANK
  FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
	and DRUGCHARACTERIZATION in ('1','3')
	and ARI_REC_ID||'-'||RECORD_ID  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
  ) apd,
( select ARI_REC_ID,SEQ_PRODUCT 
	,listagg(nvl(ACTIVESUBSTANCENAME,'(-)'),' | ') within group (order by ARI_REC_ID,seq_product) gen_concat
	from (select FK_AD_REC_ID as SEQ_PRODUCT,ARI_REC_ID,
				ACTIVESUBSTANCENAME,CDC_OPERATION_TYPE,
				row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
			FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
		 ) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 	
group by 
	ARI_REC_ID,seq_product
) api
where apd.ARI_REC_ID        =api.ARI_REC_ID(+)
and apd.seq_product   =api.seq_product(+)
  order by apd.auto_rank
  ) NN where der_sus_drg_ppd_sub is not null
 )
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc
) group by ARI_REC_ID
  
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_PRIMARY_SOURCE=LS_DB_CASE_DER_FINAL.DER_PRIMARY_SOURCE

FROM (
With TEMP_SOURCE as

(select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					) ,
 TEMP_STUDY_TYPE as 
(select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1004')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 

)		
		
select A.ARI_REC_ID,		
case
                           when    upper(SOURCE)='SPONTANEOUS' and PRIMARY_SOURCE_FLAG='1'     then      'Spontaneous'
                           when    upper(SOURCE)='REPORT FROM STUDY' and (UPPER(STUDY_TYPE)='CLINICAL TRIALS' and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG = '1' then 'Report from study-Clinical Trials'
                           when    upper(SOURCE)='REPORT FROM STUDY' and (UPPER(STUDY_TYPE)='INDIVIDUAL PATIENT USE' and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Report from study-Individual Patient use'
                           when    upper(SOURCE)='REPORT FROM STUDY' and (UPPER(STUDY_TYPE) not in ('CLINICAL TRIALS','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Report from study-Other Studies'
                           when    upper(SOURCE)='OTHER' and PRIMARY_SOURCE_FLAG='1' then 'Others'                     
                           when    upper(SOURCE)='NOT AVAILABLE TO SENDER (UNKNOWN)' and PRIMARY_SOURCE_FLAG='1' then 'Not specified by the sender'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='YES' AND upper(SOURCE)='STUDY' and (UPPER(STUDY_TYPE) in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Non-Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='YES' AND upper(SOURCE)='STUDY' and (UPPER(STUDY_TYPE) not in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='NO' AND upper(SOURCE)='CLINICAL STUDY' and (UPPER(STUDY_TYPE) in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Non-Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='NO' AND upper(SOURCE)='CLINICAL STUDY' and (UPPER(STUDY_TYPE) not in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Interventional'
                           when    SOURCE is not null and PRIMARY_SOURCE_FLAG='1'  then  SOURCE else SOURCE
              end AS DER_PRIMARY_SOURCE		
from 		
(select
    ARI_REC_ID,
    DE   as SOURCE,
	PRIMARY_SOURCE_FLAG
	from
(
    select  
        ARI_REC_ID,
        SOURCE,
		PRIMARY_SOURCE_FLAG,
		CDC_OPERATION_TYPE,
        row_number() over(partition by ARI_REC_ID order by record_id asc,CDC_OPERATION_TIME DESC) rnk
    from
        ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE  WHERE ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
        and PRIMARY_SOURCE_FLAG='1'    
) LSMV_SOURCE_SUBSET  left join  TEMP_SOURCE on  CD=SOURCE
where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
) A Join 
(select  
	ARI_REC_ID,
	DE as STUDY_TYPE
from (select ARI_REC_ID,STUDY_TYPE,CDC_OPERATION_TYPE,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
	FROM ${stage_db_name}.${stage_schema_name}.lsmv_study  WHERE ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		) LSMV_STUDY_SUBSET
	left join  TEMP_STUDY_TYPE on  CD=STUDY_TYPE where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
) B ON A.ARI_REC_ID=B.ARI_REC_ID
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_CONCOMITANT_MDCTN_INGRDNTS=  case when LS_DB_CASE_DER_FINAL.DER_CONCOMITANT_MDCTN_INGRDNTS is not null 
       then 
             case when length(LS_DB_CASE_DER_FINAL.DER_CONCOMITANT_MDCTN_INGRDNTS)>=4000 
                  then substring(LS_DB_CASE_DER_FINAL.DER_CONCOMITANT_MDCTN_INGRDNTS,0,3996)||' ...' 
                  else LS_DB_CASE_DER_FINAL.DER_CONCOMITANT_MDCTN_INGRDNTS end
        else '(-)' end
FROM (

WITH API as(
SELECT ARI_REC_ID,
	FK_AD_REC_ID as SEQ_PRODUCT,
	LISTAGG(ACTIVESUBSTANCENAME,'; ')
		  WITHIN GROUP (ORDER BY ARI_REC_ID,FK_AD_REC_ID,RECORD_ID) AS GENERIC_NAME 
	 FROM 
	 (
	 select ARI_REC_ID,FK_AD_REC_ID,RECORD_ID,ACTIVESUBSTANCENAME,CDC_OPERATION_TYPE,
	 row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	 FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE
	 where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			
	 ) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 
GROUP BY ARI_REC_ID,FK_AD_REC_ID
)
select ARI_REC_ID,
listagg(DER_CONCOMITANT_MDCTN_INGRDNTS,' | ') within group (order by ARI_REC_ID,class desc) DER_CONCOMITANT_MDCTN_INGRDNTS from
(
select ARI_REC_ID,class,case when class='BN' then '@\n'||listagg(DER_CONCOMITANT_MDCTN_INGRDNTS,'|') within group (order by ARI_REC_ID,AUTO_RANK,class)
else listagg(DER_CONCOMITANT_MDCTN_INGRDNTS,'|') within group (order by ARI_REC_ID,AUTO_RANK,class) end DER_CONCOMITANT_MDCTN_INGRDNTS from 
(
	(select 
		APD.ARI_REC_ID,'UN' class,
		COALESCE (API.GENERIC_NAME,' (-) ') as DER_CONCOMITANT_MDCTN_INGRDNTS,
		AUTO_RANK
	from 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER As AUTO_RANK,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			
        )  APD,API
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U')  and DRUGCHARACTERIZATION='2'
		AND APD.ARI_REC_ID=API.ARI_REC_ID
		and APD.SEQ_PRODUCT=API.SEQ_PRODUCT
		and APD.ARI_REC_ID||'-'||APD.SEQ_PRODUCT not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null
		)
	 order by APD.ARI_REC_ID,APD.AUTO_RANK
	 )
	 
	 Union all
	 
	 (select 
		APD.ARI_REC_ID,'BN' class,
		COALESCE (API.GENERIC_NAME,' (-) ') as DER_CONCOMITANT_MDCTN_INGRDNTS,
		AUTO_RANK
	from 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER As AUTO_RANK,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			
        ) APD,API
	where APD.rank=1 AND CDC_OPERATION_TYPE IN ('I','U')  and DRUGCHARACTERIZATION='2'
		AND APD.ARI_REC_ID=API.ARI_REC_ID
		and APD.SEQ_PRODUCT=API.SEQ_PRODUCT
		and APD.ARI_REC_ID||'-'||APD.SEQ_PRODUCT in 
		( select ARI_REC_ID||'-'||SEQ_PRODUCT  from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null
		)
	 order by APD.ARI_REC_ID,APD.AUTO_RANK
	 )
	 
	  )
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc
) group by ARI_REC_ID  ) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
-- join needs to be verified
  
  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_E2B_REPORT_DUPLICATE_NO=LS_DB_CASE_DER_FINAL.DER_E2B_REPORT_DUPLICATE_NO
FROM (
select 
ARI_REC_ID,
LISTAGG(DER_E2B_REPORT_DUPLICATE_NO,'|')
		  WITHIN GROUP (ORDER BY ARI_REC_ID) DER_E2B_REPORT_DUPLICATE_NO
from (

select  
 ARI_REC_ID,
COALESCE(DUPLICATESOURCE,'-') ||'(' || COALESCE(DUPLICATENUMB,'-') || ')' AS DER_E2B_REPORT_DUPLICATE_NO
FROM
(
select ERD.DUPLICATESOURCE,ERD.DUPLICATENUMB,ERD.FK_ASR_REC_ID,LS_DB_SAFETY_MASTER.ARI_REC_ID
        ,CDC_OPERATION_TYPE
		,row_number() over(partition by ERD.record_id order by ERD.CDC_OPERATION_TIME DESC) rnk
FROM ${stage_db_name}.${stage_schema_name}.LSMV_REPORTDUPLICATE   ERD	
	INNER JOIN (select RECORD_ID,ARI_REC_ID from 
					(select ARI_REC_ID,RECORD_ID,CDC_OPERATION_TYPE,
						row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT  
						where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
					) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
	        ) LS_DB_SAFETY_MASTER on ERD.FK_ASR_REC_ID = LS_DB_SAFETY_MASTER.RECORD_ID	

) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
group by ARI_REC_ID,
COALESCE(DUPLICATESOURCE,'-') ||'(' || COALESCE(DUPLICATENUMB,'-') || ')'
)final_data
group by  ARI_REC_ID

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	         







                      
                      
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_MED_HISTORY_CONTINUING=case when LS_DB_CASE_DER_FINAL.DER_MED_HISTORY_CONTINUING is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_MED_HISTORY_CONTINUING)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_MED_HISTORY_CONTINUING,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_MED_HISTORY_CONTINUING end
	 else '-' end
FROM (
WIth LSMV_PATIENT_MED_HIST_EPISODE_SUBSET as 
( 
SELECT ARI_REC_ID,RECORD_ID,MEDIHIST_LLTCODE,
			MEDICALCONTINUE,
			MEDICALCONTINUE_NF,SPR_ID from 
(
SELECT ARI_REC_ID,RECORD_ID,MEDIHIST_LLTCODE,
			MEDICALCONTINUE,
			MEDICALCONTINUE_NF
		,COALESCE(LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID,'-9999') AS SPR_ID
		,CDC_OPERATION_TYPE
       ,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
    from
        ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE
    where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			
) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')

),LSMV_PATIENT_DEATH_SUBSET as 
( 
SELECT ARI_REC_ID,RECORD_ID from 
(
SELECT ARI_REC_ID,RECORD_ID,CDC_OPERATION_TYPE
       ,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
    from
        ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH
    where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			
) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')

)
  
select ARI_REC_ID,LISTAGG(DER_MED_HISTORY_CONTINUING,'\r\n')
		  WITHIN GROUP (ORDER BY SEQ_DISEASE) DER_MED_HISTORY_CONTINUING
FROM (
select  
	AP.ARI_REC_ID,SEQ_DISEASE,
	LM.PT_NAME || ' (' || COALESCE(DC.DECODE,'-') || ')' AS DER_MED_HISTORY_CONTINUING
from 
	(SELECT
            LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID  ,          
			LSMV_PATIENT_MED_HIST_EPISODE.RECORD_ID AS SEQ_DISEASE,
            LSMV_PATIENT_MED_HIST_EPISODE.MEDIHIST_LLTCODE      AS PT_CODE,
            CASE
                WHEN LSMV_PATIENT_MED_HIST_EPISODE.MEDICALCONTINUE IS NULL THEN
                    LSMV_PATIENT_MED_HIST_EPISODE.MEDICALCONTINUE_NF
                ELSE
                    LSMV_PATIENT_MED_HIST_EPISODE.MEDICALCONTINUE
            END AS CONTINUING,
            CASE
                WHEN LSMV_PATIENT_MED_HIST_EPISODE.MEDICALCONTINUE = '1' THEN
                    '2'
                WHEN LSMV_PATIENT_MED_HIST_EPISODE.MEDICALCONTINUE = '2' THEN
                    '3'
                WHEN LSMV_DEATH_CAUSE.ARI_REC_ID IS NOT NULL THEN
                    '5'
                ELSE
                    '6'
            END AS DETAIL_TYPE,
    		CASE WHEN COALESCE(LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID,'')='' then '-9999' else LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID END AS SPR_ID
        FROM
            LSMV_PATIENT_MED_HIST_EPISODE_SUBSET LSMV_PATIENT_MED_HIST_EPISODE
			RIGHT JOIN LSMV_PATIENT_DEATH_SUBSET LSMV_PATIENT_DEATH 
            ON LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID = LSMV_PATIENT_DEATH.ARI_REC_ID
            RIGHT JOIN
			(
			SELECT DISTINCT ARI_REC_ID,FK_APD_REC_ID FROM 
				(
					SELECT ARI_REC_ID,FK_APD_REC_ID,CDC_OPERATION_TYPE
						,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
					from
						${stage_db_name}.${stage_schema_name}.LSMV_DEATH_CAUSE
						where FK_APD_REC_ID in (select  record_id from LSMV_PATIENT_DEATH_SUBSET) 
						
				) WHERE RNK=1 AND CDC_OPERATION_TYPE IN ('I','U')
			) LSMV_DEATH_CAUSE ON LSMV_PATIENT_DEATH.RECORD_ID = LSMV_DEATH_CAUSE.FK_APD_REC_ID
			WHERE
            LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID IS NOT NULL
 
 
	) AP,
	( 
	SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	from 
	(
		SELECT  RECORD_ID ,
		PT_CODE, 
		PT_NAME, 
		MEDDRA_VERSION,
		CDC_OPERATION_TYPE,
		row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank  
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
		  
	) WHERE rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
       AND MEDDRA_VERSION IN (select 'v.'||MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
	
	)  LM,
	( select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9037','1008')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,
                                   Coalesce(SPR_ID,'-9999') SPR_ID,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					
 ) DC
where
	TRY_TO_NUMBER(AP.PT_CODE)=LM.PT_CODE
	and AP.CONTINUING = DC.CODE (+)
	and COALESCE(AP.SPR_ID,'-9999') = DC.SPR_ID (+)
	 and ap.ARI_REC_ID in ( SELECT ARI_REC_ID from 
						(
							SELECT ARI_REC_ID,RECORD_ID,CASE
							WHEN LSMV_MESSAGE.INBOUND_ARCHIVE IN ('1','16','016') THEN 1 else 0 end AS AER_DELETED
							,CDC_OPERATION_TYPE
							,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
							from    ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE
							where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
							
						) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') and AER_DELETED=0
						)
)
group by ARI_REC_ID) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	         
                                   
                         
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_CAUSE_OF_DEATH=LS_DB_CASE_DER_FINAL.DER_CAUSE_OF_DEATH
FROM (
WITH MEDDRA_SUBSET as
(
SELECT distinct LLT_NAME,
LLT_CODE,
pt_name,
PT_CODE
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD 
  where meddra_version in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')
  AND LS_DB_MEDDRA_ICD.language_wid= (SELECT SK_LANGUAGE_WID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE
  WHERE CODE =(SELECT LANGUAGE_CODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_LANGUAGE_PARAMETERS WHERE DEFAULT_LANGUAGE='Y'))
),BASE_SUBSET as
(
 SELECT distinct 
     LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID ,
	LSMV_DEATH_CAUSE.PATDEATHREPORT_CODE,
	LSMV_DEATH_CAUSE.PATDEATHREPORT_PTCODE,
	LSMV_DEATH_CAUSE.RECORD_ID AS SEQ_DEATH_CAUSE
	
	FROM (select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE 
				 where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)  
			) AS LSMV_PATIENT_MED_HIST_EPISODE 
		RIGHT JOIN 
			(select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) AS LSMV_PATIENT_DEATH ON LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID = LSMV_PATIENT_DEATH.ARI_REC_ID 
				AND LSMV_PATIENT_MED_HIST_EPISODE.rank=1 AND LSMV_PATIENT_DEATH.rank=1 
				AND LSMV_PATIENT_MED_HIST_EPISODE.CDC_OPERATION_TYPE IN ('I','U')
				AND LSMV_PATIENT_DEATH.CDC_OPERATION_TYPE IN ('I','U')
        RIGHT JOIN 
			(select record_id,ARI_REC_ID,FK_APD_REC_ID,PATDEATHREPORT_CODE,PATDEATHREPORT_PTCODE,
			CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEATH_CAUSE 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) AS LSMV_DEATH_CAUSE ON LSMV_PATIENT_DEATH.RECORD_ID = LSMV_DEATH_CAUSE.FK_APD_REC_ID
			AND LSMV_DEATH_CAUSE.rank=1  AND LSMV_DEATH_CAUSE.CDC_OPERATION_TYPE IN ('I','U')
       ) 

select ARI_REC_ID,LISTAGG(DER_CAUSE_OF_DEATH,' | ')within group (order by SEQ_DEATH_CAUSE) DER_CAUSE_OF_DEATH from   (
SELECT
distinct
BASE_SUBSET.ARI_REC_ID AS ARI_REC_ID,
MEDDRA_LLT.llt_name ||' '||'('||MEDDRA_PT.pt_name||')' AS der_cause_of_death,
BASE_SUBSET.SEQ_DEATH_CAUSE
FROM BASE_SUBSET
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_PT
ON TRY_TO_NUMBER(BASE_SUBSET.PATDEATHREPORT_PTCODE) = MEDDRA_PT.pt_code
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_LLT
ON TRY_TO_NUMBER(BASE_SUBSET.PATDEATHREPORT_CODE) = MEDDRA_LLT.llt_code
WHERE BASE_SUBSET.ARI_REC_ID NOT IN (select distinct ARI_REC_ID from 
										(select ARI_REC_ID,INBOUND_ARCHIVE,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE 
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
											
										) where rank=1 AND  INBOUND_ARCHIVE IN ('1','16','016') AND CDC_OPERATION_TYPE IN ('I','U') )
) where der_cause_of_death is not null 
  group by 1
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_AUTOPSY_DETERMINED=LS_DB_CASE_DER_FINAL.DER_AUTOPSY_DETERMINED
FROM (
WITH MEDDRA_SUBSET as
(
SELECT distinct LLT_NAME,
LLT_CODE,
pt_name,
PT_CODE
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD 
  where meddra_version in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')
  AND LS_DB_MEDDRA_ICD.language_wid= (SELECT SK_LANGUAGE_WID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE
  WHERE CODE =(SELECT LANGUAGE_CODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_LANGUAGE_PARAMETERS WHERE DEFAULT_LANGUAGE='Y'))
),BASE_SUBSET as
(
 SELECT distinct 
     LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID ,
	LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_CODE,
	LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_PTCODE,
	LSMV_PATIENT_AUTOPSY.RECORD_ID AS SEQ_AUTOPSY
	
	FROM (select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
                 
			) AS LSMV_PATIENT_MED_HIST_EPISODE 
		RIGHT JOIN (select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
   
			) AS LSMV_PATIENT_DEATH ON LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID = LSMV_PATIENT_DEATH.ARI_REC_ID 
				AND LSMV_PATIENT_MED_HIST_EPISODE.rank=1 AND LSMV_PATIENT_DEATH.rank=1
				AND LSMV_PATIENT_MED_HIST_EPISODE.CDC_OPERATION_TYPE IN ('I','U') AND LSMV_PATIENT_DEATH.CDC_OPERATION_TYPE IN ('I','U') 
        RIGHT JOIN 
			(select record_id,ARI_REC_ID,FK_APD_REC_ID,PATDETAUTOPSY_CODE,PATDETAUTOPSY_PTCODE,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_AUTOPSY 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
  
  			) AS LSMV_PATIENT_AUTOPSY ON LSMV_PATIENT_DEATH.RECORD_ID = LSMV_PATIENT_AUTOPSY.FK_APD_REC_ID
			AND LSMV_PATIENT_AUTOPSY.rank=1 AND LSMV_PATIENT_AUTOPSY.CDC_OPERATION_TYPE IN ('I','U') 
			
  ) 

select ARI_REC_ID,LISTAGG(DER_AUTOPSY_DETERMINED,' |\n')within group (order by SEQ_AUTOPSY) DER_AUTOPSY_DETERMINED
 from   (
SELECT
distinct
BASE_SUBSET.ARI_REC_ID AS ARI_REC_ID,
MEDDRA_LLT1.llt_name ||' '||'('||MEDDRA_PT1.pt_name||')' AS der_autopsy_determined ,
BASE_SUBSET.SEQ_AUTOPSY
FROM BASE_SUBSET
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_PT1
ON TRY_TO_NUMBER(BASE_SUBSET.PATDETAUTOPSY_PTCODE )= MEDDRA_PT1.pt_code
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_LLT1
ON TRY_TO_NUMBER(BASE_SUBSET.PATDETAUTOPSY_CODE) = MEDDRA_LLT1.llt_code
WHERE BASE_SUBSET.ARI_REC_ID NOT IN (select distinct ARI_REC_ID from 
										(select ARI_REC_ID,INBOUND_ARCHIVE,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE 
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
										) where rank=1 AND  INBOUND_ARCHIVE IN ('1','16','016')
									AND CDC_OPERATION_TYPE IN ('I','U') 
									)
) where der_autopsy_determined is not null
group by 1

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_CASE_TYPE=LS_DB_CASE_DER_FINAL.DER_CASE_TYPE
FROM (
select distinct A.ARI_REC_ID,
	case
    	when  REPORT_CLASSIFICATION_CATEGORY LIKE '%2%'  and   PATIENT_PREGNANT='1'     then      'DEDP case'
    	when  (REPORT_CLASSIFICATION_CATEGORY LIKE '%2%' AND REPORT_CLASSIFICATION_CATEGORY LIKE '%1%') then  'Invalid non AE case'
    	when  REPORT_CLASSIFICATION_CATEGORY LIKE '%2%'  then  'Non AE case'
   	 	when  REPORT_CLASSIFICATION_CATEGORY LIKE '%1%'  then  'Invalid AE case'
    	when  REPORT_CLASSIFICATION_CATEGORY LIKE '%3%'  then  'Non case'
    	else  'AE case'
	end   DER_CASE_TYPE from 
	(select ARI_REC_ID,REPORT_CLASSIFICATION_CATEGORY from 
			(select ARI_REC_ID,CDC_OPERATION_TYPE,REPORT_CLASSIFICATION_CATEGORY,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_safety_report 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
	) A LEFT Join 
(	
select ARI_REC_ID,PATIENT_PREGNANT,CDC_OPERATION_TYPE,row_number() over (partition by record_id order by CDC_OPERATION_TIME desc) rank
 from ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT where  ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
) B ON A.ARI_REC_ID=B.ARI_REC_ID and rank=1 and CDC_OPERATION_TYPE IN ('I','U') and PATIENT_PREGNANT='1' 

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_CASE_CONFIRMED_BY_HP=LS_DB_CASE_DER_FINAL.DER_CASE_CONFIRMED_BY_HP
FROM (
select distinct A.ARI_REC_ID,
	case when b.ARI_REC_ID is null then 'No' ELSE 'Yes' end DER_CASE_CONFIRMED_BY_HP 
from (select ARI_REC_ID from 
			(select record_id as ARI_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_receipt_item 
					where record_id in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
	) A left join 
(	
select ARI_REC_ID,IS_HEALTH_PROF,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
 from ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE  
                  where ARI_REC_ID in (select distinct  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
 
) B ON A.ARI_REC_ID=B.ARI_REC_ID  and rank=1 AND CDC_OPERATION_TYPE IN ('I','U') and  IS_HEALTH_PROF='1'

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


       
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_SPECIAL_INTEREST_CASE_FLAG=LS_DB_CASE_DER_FINAL.DER_SPECIAL_INTEREST_CASE_FLAG
FROM (
select distinct LS_DB_SAFETY_MASTER.ARI_REC_ID,
	case when b.ARI_REC_ID is null then 'No' ELSE 'Yes' end DER_SPECIAL_INTEREST_CASE_FLAG
from (select ARI_REC_ID from 
			(select record_id as ARI_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_receipt_item 
					where record_id in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
	) LS_DB_SAFETY_MASTER left join 
	(
	select APAA.ARI_REC_ID from 
		(select ARI_REC_ID,	
			FK_DRUG_REC_ID AS SEQ_PRODUCT,
			FK_AR_REC_ID AS SEQ_REACT,
			ISAESI
			FROM 
			(select record_id,ARI_REC_ID,FK_DRUG_REC_ID,FK_AR_REC_ID,ISAESI,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 and ISAESI=1 AND CDC_OPERATION_TYPE IN ('I','U')
		) APAA Join 
		
		(select record_id AS SEQ_PRODUCT,ARI_REC_ID from 
			(select record_id,ARI_REC_ID,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND DRUGCHARACTERIZATION in ('1','3') AND CDC_OPERATION_TYPE IN ('I','U')
		) APD
		ON APAA.ARI_REC_ID     =APD.ARI_REC_ID AND APAA.SEQ_PRODUCT  =APD.SEQ_PRODUCT JOIN 
		(select record_id AS SEQ_REACT,ARI_REC_ID from 
			(select record_id,ARI_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 
		) AR 
		ON APAA.ARI_REC_ID       =AR.ARI_REC_ID and APAA.SEQ_REACT    =AR.SEQ_REACT
		where  APD.ARI_REC_ID||'-'||APD.SEQ_PRODUCT not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)
	) b on LS_DB_SAFETY_MASTER.ari_rec_id=b.ARI_REC_ID
	
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');





UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_ROLE_PPD_CONCAT=LS_DB_CASE_DER_FINAL.DER_ROLE_PPD_CONCAT
FROM (
SELECT 
	A.ARI_REC_ID, 
	listagg('(' || A.DRUGCHARACTERIZATION || ') ' || COALESCE(A.PREFERED_PRODUCT_DESCRIPTION,'-'),'\n' )
within group (order by A.ARI_REC_ID,A.AUTO_RANK)  as DER_ROLE_PPD_CONCAT 
FROM 
	(
	SELECT 
		ARI_REC_ID ARI_REC_ID,
		RANK_ORDER AUTO_RANK,
		CASE WHEN DRUGCHARACTERIZATION = '1' THEN 'S' 
	    WHEN DRUGCHARACTERIZATION = '2' THEN 'C'
	    WHEN DRUGCHARACTERIZATION = '3' THEN 'I'
		END DRUGCHARACTERIZATION,
		PREFERED_PRODUCT_DESCRIPTION
	FROM (select record_id,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER,CDC_OPERATION_TYPE,PREFERED_PRODUCT_DESCRIPTION,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND DRUGCHARACTERIZATION in ('1','2','3') AND CDC_OPERATION_TYPE IN ('I','U')
		AND ARI_REC_ID||'-'||record_id not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)
	
	) A
group by 
A.ARI_REC_ID 
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

      
      
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_PARENT_MEDICAL_HISTORY=LS_DB_CASE_DER_FINAL.DER_PARENT_MEDICAL_HISTORY
FROM (
SELECT ARI_REC_ID ,
listagg(DER_PARENT_MEDICAL_HISTORY,'\r\n') within group (order by ARI_REC_ID) as DER_PARENT_MEDICAL_HISTORY
from
(
select distinct 
A.ARI_REC_ID,
COALESCE(L_PREF_TERM.PT_NAME ,'-') as DER_PARENT_MEDICAL_HISTORY 
from
(select ARI_REC_ID ,
MEDICALEPISODENAME_PTCODE as PT_CODE 
from 
(SELECT ARI_REC_ID,MEDICALEPISODENAME_PTCODE
							,CDC_OPERATION_TYPE
							,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
							from    ${stage_db_name}.${stage_schema_name}.LSMV_PARENT_MED_HIST_EPISODE
							where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
							
						) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
                      AND ARI_REC_ID in (SELECT ARI_REC_ID from 
						(
							SELECT ARI_REC_ID,RECORD_ID,CASE
							WHEN LSMV_MESSAGE.INBOUND_ARCHIVE IN ('1','16','016') THEN 1 else 0 end AS AER_DELETED
							,CDC_OPERATION_TYPE
							,row_number() over(partition by ARI_REC_ID order by record_id) rnk
							from    ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE
							where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
							
						) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') and AER_DELETED=0
						)

 
 
 
) A JOIN ( select PT_CODE ,PT_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM C 
INNER JOIN ( SELECT 'v.'||meddra_version as meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION
WHERE EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') ) md
ON c.meddra_version = md.meddra_version
 )L_PREF_TERM
ON TRY_TO_NUMBER(A.PT_CODE)=L_PREF_TERM.PT_CODE

) group by 1

) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

      
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  ADD COLUMN DER_MUL_THERAPY_REC TEXT;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_MUL_THERAPY_REC=LS_DB_CASE_DER_FINAL.DER_MUL_THERAPY_REC
FROM (
select ARI_REC_ID, listagg(DER_MUL_THERAPY_REC,'\n') within group (order by ARI_REC_ID,RANK_ORDER)  DER_MUL_THERAPY_REC
from 
(
SELECT ap.ARI_REC_ID , ap.record_id as seq_product ,ap.RANK_ORDER,'D' || ap.RANK_ORDER || ': ' || 'No' as DER_MUL_THERAPY_REC
 FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP ap WHERE (ap.ARI_REC_ID || ap.record_id) NOT IN
(
SELECT ARI_REC_ID || seq_product FROM
(
SELECT a.ARI_REC_ID , a.FK_AD_REC_ID seq_product,b.RANK_ORDER
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP A , ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP b
WHERE A.ARI_REC_ID=b.ARI_REC_ID
AND A.FK_AD_REC_ID=b.record_id
and b.DRUGCHARACTERIZATION='1'
AND b.ARI_REC_ID||b.record_id NOT IN
(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
) GROUP BY ARI_REC_ID , seq_product
)
and ap.DRUGCHARACTERIZATION='1'
AND ap.ARI_REC_ID||'-'||ap.record_id NOT IN
(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
UNION
SELECT ARI_REC_ID , seq_product ,RANK_ORDER, 'D' || RANK_ORDER || ': ' || case when count(1) > 1 then 'Yes' else 'No' end as DER_MUL_THERAPY_REC 
FROM
(
SELECT a.ARI_REC_ID , a.FK_AD_REC_ID seq_product,b.RANK_ORDER
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP A , ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP b
WHERE A.ARI_REC_ID=b.ARI_REC_ID
AND A.FK_AD_REC_ID=b.record_id
and b.DRUGCHARACTERIZATION='1'
AND b.ARI_REC_ID||'-'||b.record_id NOT IN
(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
) GROUP BY ARI_REC_ID , seq_product , RANK_ORDER
) group by ARI_REC_ID
) LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  
  
   alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  add column DER_DRUG_NAME TEXT(4000);


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_DRUG_NAME=case when LS_DB_CASE_DER_FINAL.DER_DRUG_NAME is not null then 
substring(LS_DB_CASE_DER_FINAL.DER_DRUG_NAME,1,3996) ||
case when length(LS_DB_CASE_DER_FINAL.DER_DRUG_NAME)>4000 then ' ...' else '' end
else '(-)'
end  
FROM (  
select D.ARI_REC_ID,listagg(D.DER_DRUG_NAME,'\n') within group (order by D.ARI_REC_ID,D.AUTO_RANK) as DER_DRUG_NAME  from
(
select A.ARI_REC_ID,
CASE WHEN A.TRADE_NAME IS NULL then '-' ELSE A.TRADE_NAME END  as DER_DRUG_NAME,
A.AUTO_RANK
from 
(
SELECT 
	                AP.ARI_REC_ID,
					AP.SEQ_PRODUCT,
					AP.AUTO_RANK,
	                AP.TRADE_NAME
	FROM    (SELECT ari_rec_id ARI_REC_ID,record_id SEQ_PRODUCT,RANK_ORDER AUTO_RANK
				,MEDICINALPRODUCT TRADE_NAME, DRUGCHARACTERIZATION
				PRODUCT_FLAG FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP   WHERE DRUGCHARACTERIZATION IN ('1','3') 
                    AND ari_rec_id||'-'||SEQ_PRODUCT NOT IN 
					(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
	  ) AP 
) A
order by A.ARI_REC_ID,A.AUTO_RANK
) D
group by D.ARI_REC_ID
order by D.ARI_REC_ID)  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');      

   alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  add column DER_SUSPECT_PRODUCT_DESC TEXT(4000);  
 
 
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_SUSPECT_PRODUCT_DESC=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_PRODUCT_DESC is not null then 
substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_PRODUCT_DESC,1,3996) ||
case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_PRODUCT_DESC)>4000 then ' ...' else '' end
else '(-)'
end
FROM ( 
select ARI_REC_ID, listagg(DER_SUSPECT_PRODUCT_DESC,'\n') within group (order by ARI_REC_ID,class desc)  DER_SUSPECT_PRODUCT_DESC
from 
(
select ARI_REC_ID,class,case when class='BN' then '@\n'||listagg(DER_SUSPECT_PRODUCT_DESC,'|') within group (order by ARI_REC_ID,RANK_ORDER,class)
else listagg(DER_SUSPECT_PRODUCT_DESC,'|') within group (order by ARI_REC_ID,RANK_ORDER,class) end DER_SUSPECT_PRODUCT_DESC from (
(
select 
    APD.ARI_REC_ID,
    CASE WHEN APD.MEDICINALPRODUCT IS NULL THEN 'D'||APD.RANK_ORDER||': '||'-'
         ELSE  'D'||APD.RANK_ORDER||': '||APD.MEDICINALPRODUCT END as DER_SUSPECT_PRODUCT_DESC,
		 'UN' class,
		 RANK_ORDER
from 
  	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
where
    APD.DRUGCHARACTERIZATION IN ('1','3')
    and APD.ARI_REC_ID||'-'||APD.record_id  not  in
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
order by
	APD.ARI_REC_ID,
	APD.RANK_ORDER
)

UNION ALL

(
select 
    APD.ARI_REC_ID,
    CASE WHEN APD.MEDICINALPRODUCT IS NULL THEN 'D'||APD.RANK_ORDER||': '||'-'
         ELSE  'D'||APD.RANK_ORDER||': '||APD.MEDICINALPRODUCT END as DER_SUSPECT_PRODUCT_DESC,
		 'BN' class,
		 RANK_ORDER
from 
  	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
where
    APD.DRUGCHARACTERIZATION IN ('1','3')
    and APD.ARI_REC_ID||'-'||APD.record_id in
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is  null )
order by
	APD.ARI_REC_ID,
	APD.RANK_ORDER
)
)
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc
)
group by ARI_REC_ID
)  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

 alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP add column DER_PRODUCT_DESC_RANK_1 text;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_PRODUCT_DESC_RANK_1=LS_DB_CASE_DER_FINAL.DER_PRODUCT_DESC_RANK_1
FROM
  (SELECT 
    APD.ARI_REC_ID ARI_REC_ID,
    APD.MEDICINALPRODUCT AS DER_PRODUCT_DESC_RANK_1  
FROM 
  	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
WHERE
    APD.DRUGCHARACTERIZATION IN ('1','3')
    AND APD.ARI_REC_ID||'-'||APD.record_id  NOT  IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null)
     AND APD.RANK_ORDER='1' 
)  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  


alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP add column DER_APPROVAL_NO TEXT(4000);

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_APPROVAL_NO=case when LS_DB_CASE_DER_FINAL.DER_APPROVAL_NO is not null then 
substring(LS_DB_CASE_DER_FINAL.DER_APPROVAL_NO,1,3996) ||
case when length(LS_DB_CASE_DER_FINAL.DER_APPROVAL_NO)>4000 then ' ...' else '' end
else ''
end
from
(
select D.ARI_REC_ID,listagg(D.DER_APPROVAL_NO,'\n') within group (order by D.ARI_REC_ID,D.AUTO_RANK) as DER_APPROVAL_NO  from(
SELECT 
	PP.ARI_REC_ID,AUTO_RANK,
	CASE WHEN PP.APPROVAL_NO IS NULL then '-' else PP.APPROVAL_NO END as  DER_APPROVAL_NO
  FROM
	(
	SELECT 
	                AP.ARI_REC_ID,
					AP.SEQ_PRODUCT,
					AP.AUTO_RANK,
	                APA.APPROVAL_NO
	FROM    (SELECT ari_rec_id ARI_REC_ID,record_id SEQ_PRODUCT,RANK_ORDER AUTO_RANK,
		MEDICINALPRODUCT TRADE_NAME, DRUGCHARACTERIZATION
		PRODUCT_FLAG FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP   WHERE DRUGCHARACTERIZATION IN ('1','3') AND ari_rec_id||'-'||SEQ_PRODUCT NOT IN 
	(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
	) AP ,
	     (SELECT ARI_REC_ID ARI_REC_ID,FK_DRUG_REC_ID SEQ_PRODUCT,DRUGAUTHORIZATIONNUMB APPROVAL_NO FROM 
                                (SELECT ARI_REC_ID,FK_DRUG_REC_ID,DRUGAUTHORIZATIONCOUNTRY,DRUGAUTHORIZATIONNUMB
							,CDC_OPERATION_TYPE
							,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk
							from    ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL 
							where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) )
                                WHERE DRUGAUTHORIZATIONCOUNTRY='US' and rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')
                                ) APA
	WHERE AP.ARI_REC_ID=APA.ARI_REC_ID(+) AND 
	AP.SEQ_PRODUCT=APA.SEQ_PRODUCT(+)
	order by AP.ARI_REC_ID,AP.AUTO_RANK
	) PP
) d group by d.ARI_REC_ID
)  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  

      
     
      
alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP add column DER_SUSPECT_DRUGS_LTN_COMBINED TEXT(4000);

  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_SUSPECT_DRUGS_LTN_COMBINED=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_LTN_COMBINED is not null then 
substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_LTN_COMBINED,1,3996) ||
case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_LTN_COMBINED)>4000 then ' ...' else '' end
else '(-)'
end
FROM
(

select ARI_REC_ID,
listagg(DER_SUSPECT_DRUGS_LTN_COMBINED,' | ') within group (order by ARI_REC_ID,class desc) DER_SUSPECT_DRUGS_LTN_COMBINED from
(
select ARI_REC_ID,class,case when class='BN' then '@\n'||listagg(DER_SUSPECT_DRUGS_LTN_COMBINED,'|') within group (order by ARI_REC_ID,RANK_ORDER,class)
else listagg(DER_SUSPECT_DRUGS_LTN_COMBINED,'|') within group (order by ARI_REC_ID,RANK_ORDER,class) end DER_SUSPECT_DRUGS_LTN_COMBINED from (
(
select 
	AP.ARI_REC_ID,
	AP.MEDICINALPRODUCT  DER_SUSPECT_DRUGS_LTN_COMBINED,
	'UN' class,
	RANK_ORDER
from
	(
	select 
		APD.ARI_REC_ID,
		APD.RANK_ORDER,
		APD.MEDICINALPRODUCT
	from 
		${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
	where 
		APD.DRUGCHARACTERIZATION IN ('1','3')
		AND APD.MEDICINALPRODUCT IS NOT NULL
		and APD.ARI_REC_ID||'-'||APD.RECORD_ID  not  in
		(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
	) AP
order by 
	AP.ARI_REC_ID,
	AP.RANK_ORDER
)

UNION ALL

(
select 
	AP.ARI_REC_ID,
	AP.MEDICINALPRODUCT  DER_SUSPECT_DRUGS_LTN_COMBINED,
	'BN' class,
	RANK_ORDER
from
	(
	select 
		APD.ARI_REC_ID,
		APD.RANK_ORDER,
		APD.MEDICINALPRODUCT
	from 
		${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
	where 
		APD.DRUGCHARACTERIZATION IN ('1','3')
		AND APD.MEDICINALPRODUCT IS NOT NULL
	    and APD.ARI_REC_ID||'-'||APD.record_id in
		(select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is null )
	) AP
order by 
	AP.ARI_REC_ID,
	AP.RANK_ORDER
)
  )
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc
 ) group by ARI_REC_ID )  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  




alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP add column DER_SUSPECT_DRUGS_TN_PPD_COMB TEXT(4000);
  
  
  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_SUSPECT_DRUGS_TN_PPD_COMB=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_TN_PPD_COMB is not null then 
substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_TN_PPD_COMB,1,3996) ||
case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_DRUGS_TN_PPD_COMB)>4000 then ' ...' else '' end
else '(-)'
end
FROM
(
select ARI_REC_ID,
listagg(DER_SUSPECT_DRUGS_TN_PPD_COMB,' | ') within group (order by ARI_REC_ID,class desc) DER_SUSPECT_DRUGS_TN_PPD_COMB from
(
select ARI_REC_ID,class,case when class='BN' then '@\n'||listagg(DER_SUSPECT_DRUGS_TN_PPD_COMB,'|') within group (order by ARI_REC_ID,RANK_ORDER,class)
else listagg(DER_SUSPECT_DRUGS_TN_PPD_COMB,'|') within group (order by ARI_REC_ID,RANK_ORDER,class) end DER_SUSPECT_DRUGS_TN_PPD_COMB from (
(
select * from 
(
select 
	AP.ARI_REC_ID,
	CASE
		WHEN AP.PREFERED_PRODUCT_DESCRIPTION IS NOT NULL THEN nvl(AP.MEDICINALPRODUCT,'')||' ('||AP.PREFERED_PRODUCT_DESCRIPTION||')'
		WHEN AP.PREFERED_PRODUCT_DESCRIPTION IS NULL THEN nvl(AP.MEDICINALPRODUCT,null)
	END  as DER_SUSPECT_DRUGS_TN_PPD_COMB,
	'UN' class,
	RANK_ORDER
from
	(
	select 
		APD.ARI_REC_ID,
		APD.RANK_ORDER,
		APD.PREFERED_PRODUCT_DESCRIPTION,
		APD.MEDICINALPRODUCT
	from 
		${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
	where 
		APD.DRUGCHARACTERIZATION IN ('1','3')
		and APD.ARI_REC_ID||'-'||APD.RECORD_ID  not  in
		(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null)
	) AP
order by
	AP.ARI_REC_ID,
	AP.RANK_ORDER
) where DER_SUSPECT_DRUGS_TN_PPD_COMB is not null
)

UNION ALL

(
select * from 
(
select 
	AP.ARI_REC_ID,
	CASE
		WHEN AP.PREFERED_PRODUCT_DESCRIPTION IS NOT NULL THEN nvl(AP.MEDICINALPRODUCT,'')||' ('||AP.PREFERED_PRODUCT_DESCRIPTION||')'
		WHEN AP.PREFERED_PRODUCT_DESCRIPTION IS NULL THEN nvl(AP.MEDICINALPRODUCT,null)
	END  as DER_SUSPECT_DRUGS_TN_PPD_COMB,
	'BN' class,
	RANK_ORDER
from
	(
	select 
		APD.ARI_REC_ID,
		APD.RANK_ORDER,
		APD.PREFERED_PRODUCT_DESCRIPTION,
		APD.MEDICINALPRODUCT
	from 
		${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
	where 
		APD.DRUGCHARACTERIZATION IN ('1','3')
		and APD.ARI_REC_ID||'-'||APD.RECORD_ID in
		(select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is  null)
	) AP
order by
	AP.ARI_REC_ID,
	AP.RANK_ORDER
) where DER_SUSPECT_DRUGS_TN_PPD_COMB is not null
)
  )
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc) where DER_SUSPECT_DRUGS_TN_PPD_COMB is not null 
group by ARI_REC_ID
  )  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  



ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD COLUMN DER_STUDY_PRODUCT_TYPE_RANK_1 TEXT(250);

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_STUDY_PRODUCT_TYPE_RANK_1=T.DER_STUDY_PRODUCT_TYPE_RANK_1
FROM
(
select ARI_REC_ID,
DER_STUDY_PRODUCT_TYPE_RANK_1
from 
(
WITH TEMP_CODELIST as
(select CODE,DECODE,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE 
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) RANK,SPR_ID
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('8008')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID
									,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					)
SELECT 
	                AP.ARI_REC_ID,
					AP.SEQ_PRODUCT,
					AP.AUTO_RANK,
					TEMP_CODELIST.DECODE AS DER_STUDY_PRODUCT_TYPE_RANK_1,
					AP.SPR_ID
	FROM    (SELECT ari_rec_id ARI_REC_ID,record_id SEQ_PRODUCT,RANK_ORDER AUTO_RANK
				,MEDICINALPRODUCT TRADE_NAME, DRUGCHARACTERIZATION,STUDY_PRODUCT_TYPE,SPR_ID
				FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP   WHERE DRUGCHARACTERIZATION IN ('1','3') 
                    AND ari_rec_id||'-'||SEQ_PRODUCT NOT IN 
					(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
		
	  ) AP,TEMP_CODELIST 

	  WHERE AP.STUDY_PRODUCT_TYPE = TEMP_CODELIST.CODE(+) 
        AND AP.SPR_ID=TEMP_CODELIST.SPR_ID(+)
        AND  AUTO_RANK=1
	  
)
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = T.ARI_REC_ID	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP ADD DER_REPORTER_TYPE TEXT;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_REPORTER_TYPE=case when T.DER_REPORTER_TYPE is not null 
     then case when length(T.DER_REPORTER_TYPE)>=4000 
               then substring(T.DER_REPORTER_TYPE,0,3996)||' ...' else T.DER_REPORTER_TYPE end
	 else '-' end
FROM
(
SELECT AER_ID ,
listagg(REPORTER_TYPE,'|') within group (order by SEQ_PERSON) as DER_REPORTER_TYPE
from
(
select 
    APS.AER_ID,
	APS.SEQ_PERSON,
    B.DECODE AS REPORTER_TYPE
	FROM
( SELECT  ARI_REC_ID AS AER_ID,CASE WHEN COALESCE(SPR_ID,'')='' then '-9999' else SPR_ID END AS SPR_ID, RECORD_ID AS SEQ_PERSON,COALESCE (REPORTER_TYPE,REPORTER_TYPE_NF) REPORTER_TYPE
 , REPORTER_OR_CONTACT 
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PRIMARYSOURCE_SUBSET_TMP WHERE PRIMARY_REPORTER=1 ) APS,
	(select DECODE,code,CASE WHEN COALESCE(SPR_ID,'')='' then '-9999' else SPR_ID END AS SPR_ID  from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where CODELIST_ID IN (1003,9037) ) B
where
     APS.REPORTER_TYPE=B.CODE  and
	  APS.SPR_ID=B.SPR_ID 
order by APS.AER_ID, APS.SEQ_PERSON
) group by aer_id
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = T.AER_ID	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_PREEX_HEPTC_IMPRMNT=case when T.ARI_REC_ID is not null then 'Yes' else 'No' END
FROM
(
select 
  distinct AR.ARI_REC_ID
from 
 ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_MEDDRA_QUERY  DW,
 (select * from 
		(select ARI_REC_ID,MEDIHIST_LLTCODE as PT_CODE,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U') 
  UNION 		
  select * from 
		(select ARI_REC_ID,DRGINDCD_LLTCODE as PT_CODE,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U') 	
) AR
, ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_MEDDRA_VERSION  DM 
WHERE 
DW.MEDDRA_VERSION=DM.MEDDRA_VERSION AND 
COALESCE(DW.SPR_ID,'-9999')=COALESCE(DM.SPR_ID,'-9999') AND 
DM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') AND  
  AR.PT_CODE=DW.TERM_CODE
   and DW.TERM_LEVEL=4
  -- and AR.ARI_REC_ID in ( SELECT ARI_REC_ID FROM "+context.SF_STGDB_schemaName+".AER_S WHERE AER_DELETED='0')
  and DW.SMQ_CODE in (SELECT DEFAULT_VALUE_INT  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter  WHERE COLUMN_NAME='DER_PREEX_HEPTC_IMPRMNT'  AND PARAMETER_NAME = 'HD' )
 and DW.TERM_SCOPE in (select  
	CASE WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='D_CASE_INFO_DER_W' 
	and COLUMN_NAME='DER_PREEX_HEPTC_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='BROAD' 
	THEN '1'
	WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='D_CASE_INFO_DER_W' 
	and COLUMN_NAME='DER_PREEX_HEPTC_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='NARROW' 
	THEN '2'
	ELSE '1'
	END AS BROAD_NARROW
	from  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
	where  PARAMETER_NAME='GENERIC_BROAD_NARROW'
	union
	select  
	CASE WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='D_CASE_INFO_DER_W' 
	and COLUMN_NAME='DER_PREEX_HEPTC_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='BROAD' 
	THEN '2'
	WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='D_CASE_INFO_DER_W' 
	and COLUMN_NAME='DER_PREEX_HEPTC_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='NARROW' 
	THEN '2'
	ELSE '2'
	END AS BROAD_NARROW
	from  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
	where  PARAMETER_NAME='GENERIC_BROAD_NARROW'
  )
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = T.ARI_REC_ID	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_LACTATION_CASE_BROAD=case when T.ARI_REC_ID is not null then 'Yes' else 'No' END
FROM
(
	  SELECT  
		ARI_REC_ID
		
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query   dw,
            (
                SELECT DISTINCT
                    ari_rec_id                      AS ARI_REC_ID,
                    record_id                       AS seq_react ,                 
                    CASE
                        WHEN length(reactmeddrallt_code) > 0 THEN
                            CAST(reactmeddrallt_code AS INTEGER)
                        ELSE
                            CAST(NULL AS INTEGER)
                    END                                           AS llt_code,
                    CASE
                        WHEN length(reactmeddrapt_code) > 0 THEN
                            CAST(reactmeddrapt_code AS INTEGER)
                        ELSE
                            CAST(NULL AS INTEGER)
                    END                                           AS pt_code
                    
    
                FROM
                         ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP
                WHERE
                    ari_rec_id IS NOT NULL
                    AND coalesce(reactmeddrapt_code, '-1') NOT IN ( 'My nose is bleeding', 'My back hurts too', 'EVENT02', 'EVENT01' )
            )                           ar,
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION dm
        WHERE
                dw.meddra_version = dm.meddra_version
            AND dm.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
            AND ar.pt_code = dw.term_code
            AND dw.term_level = 4
            AND dw.smq_code = (
                SELECT
                    default_value_int
                FROM
                    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                WHERE
                        table_name = 'LS_DB_CASE_DER'
                    AND column_name = 'DER_LACTATION_CASE_BROAD'
                    AND parameter_name = 'SMQ_DLCB'
            )
            AND dw.term_scope IN (
                SELECT
                    CASE
                        WHEN coalesce(default_value_char,(
                            SELECT
                                default_value_char
                            FROM
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                            WHERE
                                    table_name = 'LS_DB_CASE_DER'
                                AND column_name = 'DER_LACTATION_CASE_BROAD'
                                AND parameter_name = 'BROAD_NARROW'
                        )) = 'BROAD'  THEN
                            '1'
                        WHEN coalesce(default_value_char,(
                            SELECT
                                default_value_char
                            FROM
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                            WHERE
                                    table_name = 'LS_DB_CASE_DER'
                                AND column_name = 'DER_LACTATION_CASE_BROAD'
                                AND parameter_name = 'BROAD_NARROW'
                        )) = 'NARROW' THEN
                            '2'
                        ELSE
                            '1'
                    END AS broad_narrow
                FROM
                    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                WHERE
                    parameter_name = 'GENERIC_BROAD_NARROW'
                UNION
                SELECT
                    CASE
                        WHEN coalesce(default_value_char,(
                            SELECT
                                default_value_char
                            FROM
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                            WHERE
                                    table_name = 'LS_DB_CASE_DER'
                                AND column_name = 'DER_LACTATION_CASE_BROAD'
                                AND parameter_name = 'BROAD_NARROW'
                        )) = 'BROAD'  THEN
                            '2'
                        WHEN coalesce(default_value_char,(
                            SELECT
                                default_value_char
                            FROM
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                            WHERE
                                    table_name = 'LS_DB_CASE_DER'
                                AND column_name = 'DER_LACTATION_CASE_BROAD'
                                AND parameter_name = 'BROAD_NARROW'
                        )) = 'NARROW' THEN
                            '2'
                        ELSE
                            '2'
                    END AS broad_narrow
                FROM
                    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                WHERE
                    parameter_name = 'GENERIC_BROAD_NARROW'
            )
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = T.ARI_REC_ID	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_OFF_LABEL_USE_BROAD=case when T.ARI_REC_ID is not null then 'Yes' else 'No' END
FROM
(SELECT
    ARI_REC_ID
FROM
    (
        SELECT
            CASE
                WHEN length(reactmeddrapt_code) > 0 THEN
                    CAST(reactmeddrapt_code AS INTEGER)
                ELSE
                    CAST(NULL AS INTEGER)
            END        AS pt_code,
            CASE
                WHEN length(reactmeddrallt_code) > 0 THEN
                    CAST(reactmeddrallt_code AS INTEGER)
                ELSE
                    CAST(NULL AS INTEGER)
            END        AS llt_code,
            ari_rec_id ARI_REC_ID
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP
    )                                   ar1,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_icd     dw1,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_version dm1
WHERE
        dw1.meddra_version = dm1.meddra_version
    AND dm1.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	AND nvl(TRY_TO_NUMBER(ar1.pt_code), - 1) = nvl(dw1.pt_code, - 1)
    AND nvl(TRY_TO_NUMBER(ar1.llt_code), TRY_TO_NUMBER(ar1.pt_code)) = nvl(dw1.llt_code, dw1.pt_code)
    AND ( dw1.hlt_code = (
        SELECT
            default_value_int
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
        WHERE
                table_name = 'LS_DB_CASE_DER'
            AND column_name = 'DER_OFF_LABEL_USE_BROAD'
            AND parameter_name = 'OLU_HLT'
    )
          OR dw1.soc_code = (
        SELECT
            default_value_int
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
        WHERE
                table_name = 'LS_DB_CASE_DER'
            AND column_name = 'DER_OFF_LABEL_USE_BROAD'
            AND parameter_name = 'OLU_SOC'
    ) )
UNION
SELECT  ARI_REC_ID FROM (select CASE WHEN LENGTH(REACTMEDDRAPT_CODE) > 0 
	THEN CAST(REACTMEDDRAPT_CODE AS INTEGER)
	ELSE CAST(NULL AS INTEGER)
	END	 AS PT_CODE, CASE WHEN LENGTH(REACTMEDDRALLT_CODE) > 0 
	THEN CAST(REACTMEDDRALLT_CODE AS INTEGER)
	ELSE CAST(NULL AS INTEGER)
	END AS LLT_CODE, ari_rec_id ARI_REC_ID,0 record_deleted from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP) WHERE TRY_TO_NUMBER(PT_CODE)= ( SELECT
    default_value_int
FROM
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
WHERE
        table_name = 'LS_DB_CASE_DER'
    AND column_name = 'DER_OFF_LABEL_USE_BROAD'
        AND parameter_name = 'OLU_PT'
)
UNION
SELECT
    ar2.ARI_REC_ID
FROM
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query   dw,
    (
        SELECT
            CASE
                WHEN length(reactmeddrapt_code) > 0 THEN
                    CAST(reactmeddrapt_code AS INTEGER)
                ELSE
                    CAST(NULL AS INTEGER)
            END        AS pt_code,
            CASE
                WHEN length(reactmeddrallt_code) > 0 THEN
                    CAST(reactmeddrallt_code AS INTEGER)
                ELSE
                    CAST(NULL AS INTEGER)
            END        AS llt_code,
            ari_rec_id ARI_REC_ID,
            0          record_deleted
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP
    )                                   ar2,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_version dm
WHERE
        dw.meddra_version = dm.meddra_version
    AND dm.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	    AND TRY_TO_NUMBER(ar2.pt_code) = dw.term_code
            AND term_level = 4
                AND ar2.record_deleted = 0
                    AND smq_code IN (
        SELECT
            default_value_int
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
        WHERE
                table_name = 'LS_DB_CASE_DER'
            AND column_name = 'DER_OFF_LABEL_USE_BROAD'
                AND parameter_name IN ( 'OLU_SMQ', 'OLU_CMQ' )
    )
                        AND dw.term_scope IN (
        SELECT
            CASE
                WHEN coalesce(default_value_char,(
                    SELECT
                        default_value_char
                    FROM
                        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                    WHERE
                            table_name = 'LS_DB_CASE_DER'
                        AND column_name = 'DER_OFF_LABEL_USE_BROAD'
                            AND parameter_name = 'BROAD_NARROW'
                )) = 'BROAD'  THEN
                    '1'
                WHEN coalesce(default_value_char,(
                    SELECT
                        default_value_char
                    FROM
                        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                    WHERE
                            table_name = 'LS_DB_CASE_DER'
                        AND column_name = 'DER_OFF_LABEL_USE_BROAD'
                            AND parameter_name = 'BROAD_NARROW'
                )) = 'NARROW' THEN
                    '2'
                ELSE
                    '1'
            END AS broad_narrow
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
        WHERE
            parameter_name = 'GENERIC_BROAD_NARROW'
        UNION
        SELECT
            CASE
                WHEN coalesce(default_value_char,(
                    SELECT
                        default_value_char
                    FROM
                        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                    WHERE
                            table_name = 'LS_DB_CASE_DER'
                        AND column_name = 'DER_OFF_LABEL_USE_BROAD'
                            AND parameter_name = 'BROAD_NARROW'
                )) = 'BROAD'  THEN
                    '2'
                WHEN coalesce(default_value_char,(
                    SELECT
                        default_value_char
                    FROM
                        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
                    WHERE
                            table_name = 'LS_DB_CASE_DER'
                        AND column_name = 'DER_OFF_LABEL_USE_BROAD'
                            AND parameter_name = 'BROAD_NARROW'
                )) = 'NARROW' THEN
                    '2'
                ELSE
                    '2'
            END AS broad_narrow
        FROM
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter
        WHERE
            parameter_name = 'GENERIC_BROAD_NARROW'
    )  
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = T.ARI_REC_ID(+)	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND LS_DB_CASE_DER_TMP.ARI_REC_ID IN (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp);



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp   
SET LS_DB_CASE_DER_tmp.DER_SUSPECT_STUDY_PRODUCT=case when LS_DB_CASE_DER_FINAL.DER_SUSPECT_STUDY_PRODUCT is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_SUSPECT_STUDY_PRODUCT)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_SUSPECT_STUDY_PRODUCT,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_SUSPECT_STUDY_PRODUCT end
	 else '-' end
  FROM
  (
select  aer_id,listagg(DER_SUSPECT_STUDY_PRODUCT,'\n') within group (order by aer_id,class desc) as DER_SUSPECT_STUDY_PRODUCT
  FROM
(
select aer_id,class,case when class='BN' then '@\n'||listagg(DER_SUSPECT_STUDY_PRODUCT,'|') within group (order by aer_id,AUTO_RANK,class)
else listagg(DER_SUSPECT_STUDY_PRODUCT,'|') within group (order by aer_id,AUTO_RANK,class) end DER_SUSPECT_STUDY_PRODUCT from
(
(

select 
    APD.ARI_REC_ID as AER_ID,
    CASE WHEN APD.STUDY_PRODUCT_TYPE IS NULL THEN nvl('D'||APD.RANK_ORDER||': '||'(-)','-')
         ELSE  nvl('D'||APD.RANK_ORDER||': '||B.DECODE,'-')
         END as DER_SUSPECT_STUDY_PRODUCT,
	'UN' class,
	RANK_ORDER AS AUTO_RANK
		 
from 
  	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP  APD,(select CODE,DECODE,SPR_ID,CODELIST_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where CODELIST_ID=8008) B
where
    APD.DRUGCHARACTERIZATION IN ('1','3')
    --and APD.RECORD_DELETED=0
     AND ari_rec_id||'-'||RECORD_ID NOT IN 
					(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
    AND APD.STUDY_PRODUCT_TYPE=B.CODE(+)
    AND APD.SPR_ID=B.SPR_ID(+)
order by APD.ARI_REC_ID, APD.RECORD_ID
)

UNION ALL

(
select 
    APD.ARI_REC_ID as AER_ID,
    CASE WHEN APD.STUDY_PRODUCT_TYPE IS NULL THEN nvl('D'||APD.RANK_ORDER||': '||'(-)','-')
         ELSE  nvl('D'||APD.RANK_ORDER||': '||B.DECODE,'-')
         END as DER_SUSPECT_STUDY_PRODUCT,
	'BN' class,
	RANK_ORDER AS AUTO_RANK
from 
  	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP  APD,(select CODE,DECODE,SPR_ID,CODELIST_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where CODELIST_ID=8008) B
where
    APD.DRUGCHARACTERIZATION IN ('1','3')
    --and APD.RECORD_DELETED=0
    and APD.ARI_REC_ID||APD.RECORD_ID   in
    (select B.ARI_REC_ID||B.RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP  B where B.BLINDED_PRODUCT_REC_ID is null)
    AND APD.STUDY_PRODUCT_TYPE=B.CODE(+)
    AND APD.SPR_ID=B.SPR_ID(+)
order by APD.ARI_REC_ID, APD.RECORD_ID
)
) 
  group by AER_ID,class 
  order by  AER_ID,class desc
)  GROUP BY aer_id
)  LS_DB_CASE_DER_FINAL
    WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.AER_ID(+)
	AND LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	AND LS_DB_CASE_DER_TMP.ARI_REC_ID IN (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp);


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.APD_DER_COD;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.APD_DER_COD
as
SELECT
            LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.ARI_REC_ID            AS AER_ID,
            LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.RECORD_ID AS SEQ_DISEASE,
            NULL AS SEQ_PRODUCT,
            LSMV_PATIENT_AUTOPSY_SUBSET.DISEASE_TERM_CODE,
            LSMV_PATIENT_AUTOPSY_SUBSET.DISEASE_TERM_PT_CODE,
            LSMV_DEATH_CAUSE_SUBSET.CAUSE_OF_DEATH_LLT_CODE,
            LSMV_DEATH_CAUSE_SUBSET.CAUSE_OF_DEATH_PT_CODE,
            LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.SPR_ID,
			LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.MEDIHIST_LLTCODE      AS PT_CODE
        FROM
            (
            select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) RANK,
                        CASE WHEN COALESCE(LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID,'')='' then '-9999' else LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID END AS SPR_ID,
						LSMV_PATIENT_MED_HIST_EPISODE.MEDIHIST_LLTCODE
                FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE 
                where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
                
                 
            ) AS LSMV_PATIENT_MED_HIST_EPISODE_SUBSET 
            RIGHT JOIN 
            (
            select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
                FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_DEATH 
                where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
                 
            ) AS LSMV_PATIENT_DEATH_SUBSET ON LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.ARI_REC_ID = LSMV_PATIENT_DEATH_SUBSET.ARI_REC_ID
            RIGHT JOIN 
            (
            select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank,
                LSMV_DEATH_CAUSE.PATDEATHREPORT_CODE AS CAUSE_OF_DEATH_LLT_CODE,LSMV_DEATH_CAUSE.PATDEATHREPORT_PTCODE AS CAUSE_OF_DEATH_PT_CODE,
                FK_APD_REC_ID
                FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEATH_CAUSE 
                where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
                 
            ) AS LSMV_DEATH_CAUSE_SUBSET ON LSMV_PATIENT_DEATH_SUBSET.RECORD_ID = LSMV_DEATH_CAUSE_SUBSET.FK_APD_REC_ID
            RIGHT JOIN 
            (
            select record_id,ARI_REC_ID,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank,
                LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_CODE     AS DISEASE_TERM_CODE,
                LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_PTCODE   AS DISEASE_TERM_PT_CODE,
                FK_APD_REC_ID
                FROM ${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_AUTOPSY 
                where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
                 
            ) AS LSMV_PATIENT_AUTOPSY_SUBSET ON LSMV_PATIENT_DEATH_SUBSET.RECORD_ID = LSMV_PATIENT_AUTOPSY_SUBSET.FK_APD_REC_ID
        WHERE
            LSMV_PATIENT_MED_HIST_EPISODE_SUBSET.ARI_REC_ID IS NOT NULL;




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_COD=
case when LS_DB_CASE_DER_FINAL.DER_COD is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_COD)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_COD,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_COD end
	 else '-' end 
FROM
(
SELECT LS_DB_CASE_DER_TMP.ARI_REC_ID, (RPT.RPT || ' | ' || ATP.ATP) AS DER_COD
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
LEFT OUTER JOIN
(
SELECT
AER_ID, RPT 
FROM
(
SELECT
AER_ID,
LISTAGG(RPT,';') WITHIN GROUP (ORDER BY AER_ID) RPT
FROM
(
SELECT DISTINCT
APD.AER_ID,
COALESCE(RP.PT_NAME,'-') || ' ('|| COALESCE(RL.LLT_NAME,'-') ||')' RPT,
APD.CAUSE_OF_DEATH_PT_CODE,
APD.CAUSE_OF_DEATH_LLT_CODE
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.APD_DER_COD AS APD,
			( 
	SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	FROM 
	(
		SELECT  RECORD_ID ,
		PT_CODE, 
		PT_NAME, 
		MEDDRA_VERSION,
		CDC_OPERATION_TYPE,
		ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK  
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
		  
	) WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')
       AND MEDDRA_VERSION IN (SELECT 'v.'||MEDDRA_VERSION FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
	
	) RP,
	(SELECT RECORD_ID,LLT_NAME,LLT_CODE,MEDDRA_VERSION,CDC_OPERATION_TYPE
						,ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_LOW_LEVEL_TERM) RL
 WHERE
 	TRY_TO_NUMBER(APD.CAUSE_OF_DEATH_PT_CODE)=RP.PT_CODE (+)
AND TRY_TO_NUMBER(APD.CAUSE_OF_DEATH_LLT_CODE)=RL.LLT_CODE(+)
)
GROUP BY
AER_ID
)
) RPT
ON LS_DB_CASE_DER_TMP.ARI_REC_ID =  RPT.AER_ID
LEFT OUTER JOIN 
(
SELECT
AER_ID, ATP 
FROM
(
SELECT
AER_ID,
LISTAGG(ATP,';') WITHIN GROUP (ORDER BY AER_ID) ATP
FROM
(
SELECT DISTINCT
APD.AER_ID,
COALESCE(AP.PT_NAME,'-') || ' ('|| COALESCE(AL.LLT_NAME,'-') ||')' ATP,
DISEASE_TERM_PT_CODE,
DISEASE_TERM_CODE
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.APD_DER_COD AS APD,
			( 
	SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	FROM 
	(
		SELECT  RECORD_ID ,
		PT_CODE, 
		PT_NAME, 
		MEDDRA_VERSION,
		CDC_OPERATION_TYPE,
		ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK  
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
		  
	) WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')
       AND MEDDRA_VERSION IN (SELECT 'v.'||MEDDRA_VERSION FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
	
	) AP,
	(SELECT RECORD_ID,LLT_NAME,LLT_CODE,MEDDRA_VERSION,CDC_OPERATION_TYPE
						,ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_LOW_LEVEL_TERM) AL
 WHERE
 	TRY_TO_NUMBER(APD.DISEASE_TERM_PT_CODE)=AP.PT_CODE (+)
AND TRY_TO_NUMBER(APD.DISEASE_TERM_CODE)=AL.LLT_CODE(+)
)
GROUP BY
AER_ID

)
) ATP
ON LS_DB_CASE_DER_TMP.ARI_REC_ID =  ATP.AER_ID
WHERE 
	regexp_count(ATP, '[[:alnum:]]')>0
	OR
	regexp_count(RPT, '[[:alnum:]]')>0

) LS_DB_CASE_DER_FINAL
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID(+)	
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND LS_DB_CASE_DER_TMP.ARI_REC_ID IN (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp);




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET DER_PREEX_RNL_IMPRMNT = CASE WHEN t.AER_ID IS NOT NULL THEN 'Yes' ELSE 'No' END
FROM
	(
	select 
  distinct AR.AER_ID
  
from 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.Ls_db_meddra_query  DW,
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.APD_DER_COD AR, 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION  DM 
WHERE 
DW.MEDDRA_VERSION=DM.MEDDRA_VERSION 
AND COALESCE(DW.SPR_ID,'-9999')=COALESCE(DM.SPR_ID,'-9999') 
AND DW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') 
AND DM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') 
 AND 
  AR.PT_CODE=DW.TERM_CODE
  and DW.TERM_LEVEL=4 --AND aer_id  IN (1719336,1805066,2114803,2116479)
  and AR.AER_ID in ( SELECT ARI_REC_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
  and CAST(char(39)||DW.SMQ_CODE||char(39) AS varchar) IN 
  
 (
 
 SELECT value
    FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_column_parameter, LATERAL SPLIT_TO_TABLE(etl_column_parameter.DEFAULT_VALUE_CHAR, ',')
    WHERE COLUMN_NAME='DER_PREEX_RNL_IMPRMNT' AND PARAMETER_NAME = 'CKD_OR_ARF'

)
 and DW.TERM_SCOPE in (select  
	CASE WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='LS_DB_CASE_DER' 
	and COLUMN_NAME='DER_PREEX_RNL_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='BROAD' 
	THEN '1'
	WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='LS_DB_CASE_DER' 
	and COLUMN_NAME='DER_PREEX_RNL_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='NARROW' 
	THEN '2'
	ELSE '1'
	END AS BROAD_NARROW
	from  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
	where  PARAMETER_NAME='GENERIC_BROAD_NARROW'
	union
	select  
	CASE WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='LS_DB_CASE_DER' 
	and COLUMN_NAME='DER_PREEX_RNL_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='BROAD' 
	THEN '2'
	WHEN 
	coalesce(DEFAULT_VALUE_CHAR,(select DEFAULT_VALUE_CHAR from  
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER where  TABLE_NAME='LS_DB_CASE_DER' 
	and COLUMN_NAME='DER_PREEX_RNL_IMPRMNT' and  PARAMETER_NAME='BROAD_NARROW')) ='NARROW' 
	THEN '2'
	ELSE '2'
	END AS BROAD_NARROW
	from  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
	where  PARAMETER_NAME='GENERIC_BROAD_NARROW'
  )
  )
  T
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = t.AER_ID(+)
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	AND LS_DB_CASE_DER_TMP.ARI_REC_ID IN (SELECT ARI_REC_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP);


 alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  add column DER_FATAL_CASE_FLAG text (10) default 'No';

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_FATAL_CASE_FLAG=case when LS_DB_CASE_DER_FINAL.ARI_REC_ID is not null then 'Yes' else 'No' end
FROM (
select distinct ARI_REC_ID from 
			(select ARI_REC_ID,CDC_OPERATION_TYPE,death,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_safety_report 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U') and DEATH in ('01','1')
	)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

 alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP  add column DER_MT_LITERATURE_REFERENCE_JAP TEXT(4000) ;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP   
SET LS_DB_CASE_DER_TMP.DER_MT_LITERATURE_REFERENCE_JAP=case when LS_DB_CASE_DER_FINAL.DER_MT_LITERATURE_REFERENCE_JAP is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_MT_LITERATURE_REFERENCE_JAP)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_MT_LITERATURE_REFERENCE_JAP,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_MT_LITERATURE_REFERENCE_JAP end
	 else null end
FROM (

select ARI_REC_ID,listagg(LITERATURE_REFERENCE,'\r\n') within group (order by SEQ_LITERATURE) as DER_MT_LITERATURE_REFERENCE_JAP from 

(
select ARI_REC_ID,ENTITY_RECORD_ID as SEQ_LITERATURE,
MAX(LITERATURE_REFERENCE) LITERATURE_REFERENCE
 from 
			(select C.ari_rec_id,A.ENTITY_RECORD_ID,A.FIELD_ID,A.record_id,
			A.CONTEXT_NAME,A.SPR_ID,A.CDC_OPERATION_TYPE as CDC_OPERATION_TYPE_1
			,B.CDC_OPERATION_TYPE as CDC_OPERATION_TYPE_2
			,CASE WHEN A.FIELD_ID='956212' 
					AND A.CONTEXT_NAME= 'LITERATURE_REFERENCE' 
					THEN COALESCE(A.VALUE,A.VALUE_NF) ELSE NULL 
			END LITERATURE_REFERENCE,
			CASE
				WHEN A.LANGUAGE_CODE='127' THEN '001'
				WHEN A.LANGUAGE_CODE='110' THEN '002'
				WHEN A.LANGUAGE_CODE='404' THEN '003'
				WHEN A.LANGUAGE_CODE='142' THEN '005'
				WHEN A.LANGUAGE_CODE='205' THEN '007'
				WHEN A.LANGUAGE_CODE='208' THEN '008'
				WHEN A.LANGUAGE_CODE='81' THEN '009'
				WHEN A.LANGUAGE_CODE='351' THEN '010'
				ELSE A.LANGUAGE_CODE
			END LANGUAGE_CODE,
					row_number() over (partition by A.RECORD_ID,B.RECORD_ID order by A.CDC_OPERATION_TIME desc,B.CDC_OPERATION_TIME desc) rank 
					
                FROM ${stage_db_name}.${stage_schema_name}.LSMV_LANG_REDUCTED A join ${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE B on 
					A.ENTITY_RECORD_ID = B.RECORD_ID
					JOIN (
						select distinct ARI_REC_ID,record_id from 
							(select ARI_REC_ID,record_id,CDC_OPERATION_TYPE,
							row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
							FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT 
						where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
						) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
						) C ON A.FK_ASR_REC_ID=C.RECORD_ID
					
			) where rank=1 AND  CDC_OPERATION_TYPE_1 IN ('I','U') and CDC_OPERATION_TYPE_2 IN ('I','U') 
AND LANGUAGE_CODE='008' 
group by 1,2 
) where LITERATURE_REFERENCE IS NOT NULL 
group by 1)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_PAST_DISEASES_COMBINED=LS_DB_CASE_DER_FINAL.DER_PAST_DISEASES_COMBINED
FROM (
select src.ARI_REC_ID,
              LISTAGG( src.DISEASES,(' | ')) WITHIN GROUP (ORDER BY src.SEQ_DISEASE) OVER ( PARTITION BY src.ARI_REC_ID )  DER_PAST_DISEASES_COMBINED
                from
(select  
              AP.ARI_REC_ID,
			 	AP.SEQ_DISEASE,	  
              LM.PT_NAME  DISEASES
from 
              ( SELECT distinct
            ARI_REC_ID            AS ARI_REC_ID,
			RECORD_ID AS SEQ_DISEASE,
            MEDIHIST_LLTCODE      AS PT_CODE,
            CASE
                WHEN MEDICALCONTINUE = '1' THEN
                    '2'
                WHEN MEDICALCONTINUE = '2' THEN
                    '3'
                ELSE
                    '6'
            END AS DETAIL_TYPE
        FROM
           ( SELECT ARI_REC_ID,RECORD_ID,MEDIHIST_LLTCODE,
								MEDICALCONTINUE,
								MEDICALCONTINUE_NF,SPR_ID from 
					(
					SELECT ARI_REC_ID,RECORD_ID,MEDIHIST_LLTCODE,
								MEDICALCONTINUE,
								MEDICALCONTINUE_NF
							,COALESCE(LSMV_PATIENT_MED_HIST_EPISODE.SPR_ID,'-9999') AS SPR_ID
							,CDC_OPERATION_TYPE
						,row_number() over(PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) rnk
						from
							${stage_db_name}.${stage_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE
						where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
								
					) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')
		)


        WHERE
            ARI_REC_ID IS NOT NULL) AP,
              (SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
							from 
							(
							SELECT  RECORD_ID ,
						PT_CODE, 
						Case  when (Select LANGUAGE_CODE from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_LANGUAGE_PARAMETERS where DEFAULT_LANGUAGE='Y')='001'
								then   PT_NAME
								else     PT_KANJI END PT_NAME , 
						MEDDRA_VERSION,
						CDC_OPERATION_TYPE,
						row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank  
						FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
						
						) WHERE rank=1 AND CDC_OPERATION_TYPE IN ('I','U')  and MEDDRA_VERSION = (select 'v.'||meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')  
			)  LM
			  where TRY_TO_NUMBER(AP.PT_CODE)=LM.PT_CODE
              and AP.DETAIL_TYPE='3'
order by
              AP.ARI_REC_ID, 
              AP.SEQ_DISEASE) src
			  
)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

		  
-------------- DER_COUNTRY_PUBLISHED

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_JPN_tmp;
create TEMPORARY table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_JPN_tmp AS 
select CODE,DECODE from 
    (
              SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1015')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID
									,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE 
                                   WHERE LANGUAGE_CODE='ja'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_COUNTRY_PUBLISHED=LS_DB_CASE_DER_FINAL.DER_COUNTRY_PUBLISHED
FROM (
select  ARI_REC_ID,
        LISTAGG( DER_COUNTRY,('\r\n')) WITHIN GROUP  ( ORDER BY record_id)  DER_COUNTRY_PUBLISHED
     from(   
 
SELECT
ARI_REC_ID,RECORD_ID,
row_number() over(partition by ARI_REC_ID
ORDER BY RECORD_ID)  || '. ' || DER_COUNTRY as DER_COUNTRY
from 
 (SELECT
distinct 
LPS.ARI_REC_ID,
CDLIST.DECODE AS DER_COUNTRY,
LPS.COUNTRY_PUBLISHED_JPN,
LPS.RECORD_ID
  FROM
  (select ARI_REC_ID ARI_REC_ID,RECORD_ID,COUNTRY_PUBLISHED_JPN 
  FROM  
  
				(
					SELECT ARI_REC_ID,RECORD_ID,COUNTRY_PUBLISHED_JPN,CDC_OPERATION_TYPE
						,row_number() over(PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) rnk
						from
							${stage_db_name}.${stage_schema_name}.LSMV_RESEARCH_RPT
						where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
								
					) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') and COUNTRY_PUBLISHED_JPN is not null 
   ) LPS, 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_JPN_tmp CDLIST
WHERE 	LPS.COUNTRY_PUBLISHED_JPN=CDLIST.CODE	
 order by LPS.ARI_REC_ID,LPS.record_id) src )
  group by 1
)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


         
--------------------------------------------------------		 

------------- DER_MT_LITERATURE_REFERENCE



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_MT_LITERATURE_REFERENCE=LS_DB_CASE_DER_FINAL.DER_MT_LITERATURE_REFERENCE
FROM (
select distinct src.ARI_REC_ID,
        LISTAGG( src.LITERATUREREFERENCE,(' | ')) WITHIN GROUP (ORDER BY src.ARI_REC_ID) OVER ( PARTITION BY src.ARI_REC_ID )  DER_MT_LITERATURE_REFERENCE
     from(   
   SELECT
distinct 
LPS.ARI_REC_ID,
LPS.LITERATUREREFERENCE,
LPS.RECORD_ID
  FROM
  (select ARI_REC_ID ,RECORD_ID,LITERATUREREFERENCE 
  FROM  (
					SELECT ARI_REC_ID,RECORD_ID,LITERATUREREFERENCE,CDC_OPERATION_TYPE
						,row_number() over(PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) rnk
						from
							${stage_db_name}.${stage_schema_name}.LSMV_LITERATURE
						where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
								
					) where rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') and LITERATUREREFERENCE is not null 
 ) LPS 
 order by LPS.ARI_REC_ID,LPS.record_id) src )LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

         	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_INCIDENT_IMDRF=LS_DB_CASE_DER_FINAL.DER_INCIDENT_IMDRF
FROM (

SELECT 
	A.ARI_REC_ID,
	listagg(B.PT_NAME || '(' || COALESCE(C.IMRDF_CODES,'-')  || ')','\n')
within group (order by A.ARI_REC_ID,A.AUTO_RANK)  as DER_INCIDENT_IMDRF
FROM  
  (select ARI_REC_ID,LLT_CODE,PT_CODE,AUTO_RANK,RECORD_ID
FROM
(select ARI_REC_ID,RECORD_ID,
REACTMEDDRALLT_CODE As  LLT_CODE,
REACTMEDDRAPT_CODE AS PT_CODE,
RANK_ORDER as AUTO_RANK,
CDC_OPERATION_TYPE,event_type,
row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 

 from 
${stage_db_name}.${stage_schema_name}.LSMV_REACTION
where ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)

) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')  and event_type='2'
)  A 
JOIN (	SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	from 
	(
	SELECT  RECORD_ID ,
  PT_CODE, 
  PT_NAME, 
  MEDDRA_VERSION,
  CDC_OPERATION_TYPE,
  row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank  
FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM

) WHERE rank=1 AND CDC_OPERATION_TYPE IN ('I','U') 
       AND MEDDRA_VERSION IN (select 'v.'||MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
) B on TRY_TO_NUMBER(A.PT_CODE)=B.PT_CODE
JOIN (
select distinct FK_AD_REC_ID,IMRDF_CODES
FROM
(select FK_AD_REC_ID,IMRDF_CODES,
CDC_OPERATION_TYPE,
row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 

 from 
${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION
where ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)

) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
)  C on A.RECORD_ID=C.FK_AD_REC_ID
group by 1 
 
 )LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

      	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_SL_CLASSIFICATION=LS_DB_CASE_DER_FINAL.DER_SL_CLASSIFICATION
FROM (

WITH DER_CASE_EVENT_SERIOUSNESS_SUBSET as (

SELECT ARI_REC_ID,
             RECORD_ID as SEQ_REACT,
             DER_CASE_EVENT_SERIOUSNESS
      FROM (SELECT A.ARI_REC_ID AS ARI_REC_ID,
                   A.RECORD_ID AS RECORD_ID,
                   CASE
                     WHEN (SELECT TRIM(UPPER(default_value_char))
                           FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                           WHERE column_name = 'DER_CASE_EVENT_SERIOUSNESS'
                           AND   table_name = 'B_CASE_EVENT_W') = 'COMPANY' THEN (
                       CASE
                         WHEN SERIOUSNESS_COMPANY IS NULL THEN NULL
                         WHEN (SERIOUSNESS_COMPANY = '01' OR SERIOUSNESS_COMPANY = '1') THEN 'Serious'
                         WHEN (SERIOUSNESS_COMPANY = '02' OR SERIOUSNESS_COMPANY = '2') THEN 'Non-serious'
                       END )
                     ELSE
                       CASE
                         WHEN (SELECT TRIM(UPPER(default_value_char))
                               FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                               WHERE column_name = 'DER_CASE_EVENT_SERIOUSNESS'
                               AND   table_name = 'B_CASE_EVENT_W') = 'REPORTER' THEN (
                           CASE
                             WHEN SERIOUSNESS IS NULL THEN NULL
                             WHEN (SERIOUSNESS = '01' OR SERIOUSNESS = '1') THEN 'Serious'
                             WHEN (SERIOUSNESS = '02' OR SERIOUSNESS = '2') THEN 'Non-serious'
                           END )
                         ELSE
                           CASE
                             WHEN (SELECT TRIM(UPPER(default_value_char))
                                   FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                                   WHERE column_name = 'DER_CASE_EVENT_SERIOUSNESS'
                                   AND   table_name = 'B_CASE_EVENT_W') IN ('COMPANY OR REPORTER','REPORTER OR COMPANY') THEN (
                               CASE
                                 WHEN (SERIOUSNESS IS NULL AND SERIOUSNESS_COMPANY IS NULL) THEN NULL
                                 WHEN (SERIOUSNESS IS NULL AND (SERIOUSNESS_COMPANY = '02' OR SERIOUSNESS_COMPANY = '2')) THEN 'Non-serious'
                                 WHEN ((SERIOUSNESS = '02' OR SERIOUSNESS = '2') AND SERIOUSNESS_COMPANY IS NULL) THEN 'Non-serious'
                                 WHEN ((SERIOUSNESS = '01' OR SERIOUSNESS = '1') OR (SERIOUSNESS_COMPANY = '01' OR SERIOUSNESS_COMPANY = '1')) THEN 'Serious'
                                 WHEN ((SERIOUSNESS = '02' OR SERIOUSNESS = '2') AND (SERIOUSNESS_COMPANY = '02' OR SERIOUSNESS_COMPANY = '2')) THEN 'Non-serious'
                               END )
                           END 
                       END 
                   END DER_CASE_EVENT_SERIOUSNESS,
                   ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,
                   CDC_OPERATION_TYPE
            FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION A
            WHERE A.ari_rec_id IN (SELECT ari_rec_id FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
			) A
      WHERE RANK = 1
      AND   CDC_OPERATION_TYPE IN ('I','U')
) 


select ARI_REC_ID,CASE 
WHEN MAX(PP)='1' THEN 'Non-Serious Labeled'
WHEN MAX(PP)='2' THEN 'Non-Serious Unlabeled'
WHEN MAX(PP)='3' THEN 'Serious Labeled'
WHEN MAX(PP)='4' THEN 'Serious Unlabeled'
ELSE NULL END as DER_SL_CLASSIFICATION
from 
(
select 
A.ARI_REC_ID,
A.SEQ_REACT,
C.record_id SEQ_PRODUCT,
CASE 
     WHEN (UPPER(A.der_case_event_seriousness)=UPPER('Serious') AND B.COUNTRY IN ('US') and B.IS_LISTED='1') THEN '3'
       WHEN (UPPER(A.der_case_event_seriousness)=UPPER('Non-serious') AND B.COUNTRY IN ('US') and B.IS_LISTED ='1') THEN '1'
     WHEN (UPPER(A.der_case_event_seriousness)=UPPER('Serious') AND B.COUNTRY IN ('US') and B.IS_LISTED ='0') THEN '4'
       WHEN (UPPER(A.der_case_event_seriousness)=UPPER('Non-serious') AND B.COUNTRY IN ('US') and B.IS_LISTED ='0') THEN '2'
       else null end PP 
from 
DER_CASE_EVENT_SERIOUSNESS_SUBSET A,

(SELECT  DISTINCT
LSMV_DRUG_REACT_LISTEDNESS.ARI_REC_ID,
LSMV_DRUG_REACT_RELATEDNESS.FK_DRUG_REC_ID SEQ_PRODUCT,
LSMV_DRUG_REACT_RELATEDNESS.FK_AR_REC_ID AS SEQ_REACT,
LSMV_DRUG_REACT_LISTEDNESS.COUNTRY ,
LSMV_DRUG_REACT_LISTEDNESS.IS_LISTED 

FROM 
	(select *
			FROM 
			(select record_id,ARI_REC_ID,FK_ADRR_REC_ID,
			COUNTRY,IS_LISTED,
			CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_LISTEDNESS 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 and COUNTRY='US' AND CDC_OPERATION_TYPE IN ('I','U')
	)  LSMV_DRUG_REACT_LISTEDNESS
	INNER JOIN 
	(select *
			FROM 
			(select ARI_REC_ID,	
			FK_DRUG_REC_ID,
			FK_AR_REC_ID ,
			RECORD_ID,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
			) where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
	 ) LSMV_DRUG_REACT_RELATEDNESS ON LSMV_DRUG_REACT_LISTEDNESS.FK_ADRR_REC_ID = LSMV_DRUG_REACT_RELATEDNESS.RECORD_ID
) B,${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP C
where A.ARI_REC_ID = C.ARI_REC_ID
AND   A.ARI_REC_ID= B.ARI_REC_ID
AND   A.seq_react=B.SEQ_REACT
AND   B.ARI_REC_ID=C.ARI_REC_ID
AND   B.seq_product=C.record_id
and   C.DRUGCHARACTERIZATION IN ('1','3')
AND   C.RANK_ORDER=1
and   C.ARI_REC_ID||'-'||C.record_id  not  in
(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)
) GROUP BY ARI_REC_ID


)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_SOLICITED_CASE_SYMBOL=CASE_STUDY_FINAL.DER_SOLICITED_CASE_SYMBOL
FROM
(
SELECT 
CASE_SUSBET.RECORD_ID,
CASE WHEN REPORTTYPE_1001.DECODE='Report from study' and STUDY_TYPE_1004.DECODE!='Clinical Trials' THEN '%'||CASE_SUSBET.AER_NO ELSE CASE_SUSBET.AER_NO END as DER_SOLICITED_CASE_SYMBOL
FROM
(
SELECT 
RECORD_ID,REPORTTYPE,AER_NO,SPR_ID
FROM
(
SELECT LSMV_RECEIPT_ITEM.RECORD_ID,REPORTTYPE,COALESCE(LSMV_AER_INFO.AER_NO, LSMV_AER_INFO.MAPPED_AER_NO) AS AER_NO,
CASE WHEN COALESCE(LSMV_RECEIPT_ITEM.SPR_ID,'')='' then '-9999' else LSMV_RECEIPT_ITEM.SPR_ID END AS SPR_ID,
LSMV_RECEIPT_ITEM.CDC_OPERATION_TYPE,row_number() over (partition by LSMV_RECEIPT_ITEM.RECORD_ID order by LSMV_RECEIPT_ITEM.CDC_OPERATION_TIME desc) rank 
FROM ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM 
LEFT OUTER JOIN ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT ON LSMV_RECEIPT_ITEM.RECORD_ID = LSMV_SAFETY_REPORT.ARI_REC_ID
LEFT OUTER JOIN ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO ON LSMV_RECEIPT_ITEM.RECORD_ID = LSMV_AER_INFO.ARI_REC_ID
WHERE LSMV_RECEIPT_ITEM.RECORD_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP) 
) CASE_SUSBET_INNER WHERE CASE_SUSBET_INNER.CDC_OPERATION_TYPE IN ('I','U') AND CASE_SUSBET_INNER.RANK=1 ) CASE_SUSBET 
LEFT OUTER JOIN (SELECT ARI_REC_ID,STUDY_TYPE,SPR_ID FROM (SELECT ARI_REC_ID,STUDY_TYPE,CASE WHEN COALESCE(LSMV_STUDY.SPR_ID,'')='' then '-9999' else LSMV_STUDY.SPR_ID END AS SPR_ID,
LSMV_STUDY.CDC_OPERATION_TYPE,row_number() over (partition by LSMV_STUDY.RECORD_ID order by LSMV_STUDY.CDC_OPERATION_TIME desc) RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_STUDY
WHERE LSMV_STUDY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
) STUDY_SUBSET_INNER WHERE STUDY_SUBSET_INNER.CDC_OPERATION_TYPE IN ('I','U') AND STUDY_SUBSET_INNER.RANK=1 ) STUDY_SUBSET
ON CASE_SUSBET.RECORD_ID=STUDY_SUBSET.ARI_REC_ID
LEFT OUTER JOIN (SELECT CODE,DECODE,SPR_ID,CODELIST_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE codelist_id=1001) REPORTTYPE_1001
ON  CASE_SUSBET.REPORTTYPE=REPORTTYPE_1001.CODE
AND CASE_SUSBET.SPR_ID=REPORTTYPE_1001.SPR_ID
LEFT OUTER JOIN (SELECT CODE,DECODE,SPR_ID,CODELIST_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE codelist_id=1004) STUDY_TYPE_1004
ON  STUDY_SUBSET.STUDY_TYPE=STUDY_TYPE_1004.CODE
AND STUDY_SUBSET.SPR_ID=STUDY_TYPE_1004.SPR_ID
) CASE_STUDY_FINAL
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID =CASE_STUDY_FINAL.record_id
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_APPROVAL_TYPE=case when CASE_APPROVAL_TYPE.DER_APPROVAL_TYPE is not null 
     then case when length(CASE_APPROVAL_TYPE.DER_APPROVAL_TYPE)>=4000 
               then substring(CASE_APPROVAL_TYPE.DER_APPROVAL_TYPE,0,3996)||' ...' else CASE_APPROVAL_TYPE.DER_APPROVAL_TYPE end
	 else '' end
FROM
(
SELECT 
ARI_REC_ID,
listagg(DER_APPROVAL_TYPE,'\n') within group (order by ARI_REC_ID,RANK_ORDER) as DER_APPROVAL_TYPE
FROM 
(
SELECT AP.ARI_REC_ID,MIN(RANK_ORDER)  RANK_ORDER,
CASE WHEN APA.DRUGAUTHORIZATIONTYPE IS NULL then '-' else APPROVAL_TYPE_709.decode END as  DER_APPROVAL_TYPE
FROM
	(SELECT ari_rec_id,
       record_id,
	   RANK_ORDER
	   	   
  FROM (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
	and DRUGCHARACTERIZATION in ('1','3')
	and ARI_REC_ID||'-'||RECORD_ID  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
  ) AP LEFT OUTER JOIN
 (SELECT ARI_REC_ID ARI_REC_ID,FK_DRUG_REC_ID,DRUGAUTHORIZATIONTYPE,SPR_ID FROM 
      (SELECT ARI_REC_ID,FK_DRUG_REC_ID,DRUGAUTHORIZATIONTYPE
							,CDC_OPERATION_TYPE
							,row_number() over(partition by RECORD_ID order by CDC_OPERATION_TIME desc) rnk,
							CASE WHEN COALESCE(SPR_ID,'')='' then '-9999' else SPR_ID END AS SPR_ID
							from    ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL WHERE DRUGAUTHORIZATIONCOUNTRY='US'
                            AND ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
		)
  WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')
  ) APA 
  	ON AP.ARI_REC_ID=APA.ARI_REC_ID
	AND   AP.RECORD_ID=APA.FK_DRUG_REC_ID
  LEFT OUTER JOIN 
	(SELECT CODE,DECODE,SPR_ID,CODELIST_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE codelist_id=709) APPROVAL_TYPE_709
	ON   APA.DRUGAUTHORIZATIONTYPE=APPROVAL_TYPE_709.CODE
	AND   APA.SPR_ID=APPROVAL_TYPE_709.SPR_ID
GROUP BY AP.ARI_REC_ID,CASE WHEN APA.DRUGAUTHORIZATIONTYPE IS NULL then '-' else APPROVAL_TYPE_709.decode END
) CASE_APPROVAL_TYPE_INNER
GROUP  BY ARI_REC_ID
) CASE_APPROVAL_TYPE
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID =CASE_APPROVAL_TYPE.ARI_REC_ID
AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_CASE_SERIOUSNESS_CRITERIA=T.DER_CASE_SERIOUSNESS_CRITERIA   
FROM
(
SELECT ARI_REC_ID,DER_CASE_SERIOUSNESS_CRITERIA
FROM
(
SELECT
    ARI_REC_ID,
	RECORD_ID,
	row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk,
	CDC_OPERATION_TYPE,
	RTRIM( NVL(case when nvl(A.CONGENITALANOMALI,'')='1' then 'C|' end,'')||
                    NVL( CASE WHEN nvl(A.DEATH,'')='1' THEN 'F|' END,'')||
                    NVL( CASE WHEN nvl(A.DISABLING,'')='1' THEN 'D|' END,'')||
                   -- NVL( CASE WHEN nvl(A.HOSP_PROLONGED,'')='1' THEN 'HP|' END,'')||
                    NVL( case when nvl(A.HOSPITALIZATION,'')='1' then 'H|' end,'')||
                    NVL( case when nvl(A.LIFETHREATENING,'')='1' then 'L|' end,'')||
                    NVL(  case when nvl(A.OTHER,'')='1' then 'O|' end,'')||
                    NVL( case when nvl(A.REQUIRED_INTERVENATION,'')='1' then 'I' end,''),'|') DER_CASE_SERIOUSNESS_CRITERIA
FROM
${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT A WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
) where rnk=1 and CDC_OPERATION_TYPE IN ('I','U')
) T
WHERE LS_DB_CASE_DER_tmp.ARI_REC_ID = T.ARI_REC_ID
AND   LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_PPD_STUDY_PRDCT_TYPE_COMB=
case when LS_DB_CASE_DER_FINAL.DER_PPD_STUDY_PRDCT_TYPE_COMB is not null 
     then case when length(LS_DB_CASE_DER_FINAL.DER_PPD_STUDY_PRDCT_TYPE_COMB)>=4000 
               then substring(LS_DB_CASE_DER_FINAL.DER_PPD_STUDY_PRDCT_TYPE_COMB,0,3996)||' ...' else LS_DB_CASE_DER_FINAL.DER_PPD_STUDY_PRDCT_TYPE_COMB end
	 else null end
FROM
(
SELECT ari_rec_id,
listagg(DER_PPD_STUDY_PRDCT_TYPE_COMB,' | ') within group (order by ARI_REC_ID,RANK_ORDER) DER_PPD_STUDY_PRDCT_TYPE_COMB
FROM
(
SELECT ari_rec_id,
       APD.PREFERED_PRODUCT_DESCRIPTION||' ('||nvl(STUDY_PRODUCT_TYPE_8008.DECODE,' ')||') ' as DER_PPD_STUDY_PRDCT_TYPE_COMB,
       RANK_ORDER
  FROM 
  (
  SELECT ARI_REC_ID,PREFERED_PRODUCT_DESCRIPTION,RANK_ORDER,STUDY_PRODUCT_TYPE
  FROM
  (
		select ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk,
		STUDY_PRODUCT_TYPE,
		PREFERED_PRODUCT_DESCRIPTION
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
	WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') 
	and DRUGCHARACTERIZATION in ('1','3')
	and ARI_REC_ID||'-'||RECORD_ID  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
      ) APD
      LEFT OUTER JOIN (SELECT CODE,DECODE,SPR_ID,CODELIST_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE codelist_id=8008) STUDY_PRODUCT_TYPE_8008
      ON APD.STUDY_PRODUCT_TYPE=STUDY_PRODUCT_TYPE_8008.CODE
ORDER BY ari_rec_id,RANK_ORDER
) GROUP BY ari_rec_id
) LS_DB_CASE_DER_FINAL
WHERE   LS_DB_CASE_DER_tmp.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
AND     LS_DB_CASE_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');	
	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_CASE_UPGRADE_TO_FATAL_FLAG=LS_DB_CASE_DER_FINAL.DER_CASE_UPGRADE_TO_FATAL_FLAG
FROM (

WITH AER_NO_SUBSET as 
(select distinct AER_NO,ari_rec_id from 
			(select COALESCE(LSMV_AER_INFO.AER_NO, LSMV_AER_INFO.MAPPED_AER_NO) AER_NO,ari_rec_id,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO
					 where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_tmp) 
					
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
),LSMV_AER_INFO_SUBSET as 
(select distinct ARI_REC_ID,AER_NO,AER_VERSION_NO from 
			(select ARI_REC_ID,COALESCE(LSMV_AER_INFO.AER_NO, LSMV_AER_INFO.MAPPED_AER_NO) AS AER_NO,
           LSMV_AER_INFO.AER_VERSION_NO,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO 
					where aer_no in (select  AER_NO from AER_NO_SUBSET) 
					or ARI_REC_ID in (select  ARI_REC_ID from AER_NO_SUBSET) 
			) where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U')
)	

select ARI_REC_ID,
 case when  AER_VERSION_NO=0  then  'No'
        when  DER_FATAL_CASE_FLAG is null then null
        when  LAG(DER_FATAL_CASE_FLAG,1) over ( partition by AER_NO order by AER_VERSION_NO) is null AND DER_FATAL_CASE_FLAG = 'Yes' THEN 'Yes'
        when  LAG(DER_FATAL_CASE_FLAG,1) over ( partition by AER_NO order by AER_VERSION_NO) = 'No' AND DER_FATAL_CASE_FLAG = 'Yes' THEN 'Yes'
        else  'No'
   end  DER_CASE_UPGRADE_TO_FATAL_FLAG
from 	
(select distinct A.ARI_REC_ID,B.AER_NO,B.AER_VERSION_NO,case when A.DEATH in ('01','1') then 'Yes' ELSE 'No' end DER_FATAL_CASE_FLAG
      

 from 
			(select ARI_REC_ID,CDC_OPERATION_TYPE,death,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_safety_report 
					where ARI_REC_ID in (select  ARI_REC_ID from LSMV_AER_INFO_SUBSET) 
					
			) A JOIN LSMV_AER_INFO_SUBSET B on A.ari_REC_ID=B.ARI_REC_ID 
			where rank=1 AND  CDC_OPERATION_TYPE IN ('I','U') 
) output_subset
)LS_DB_CASE_DER_FINAL
WHERE
	LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP
SET LS_DB_CASE_DER_TMP.DER_SENDER_ORGANIZATION=LS_DB_CASE_DER_FINAL.DER_SENDER_ORGANIZATION
FROM ( 	SELECT  distinct LSMV_RECEIPT_ITEM.RECORD_ID ARI_REC_ID, COMPANY_UNIT.RECORD_ID,
				Initial_Business_Unit_Code||'-'||Initial_Business_Unit||''  as DER_SENDER_ORGANIZATION 
				FROM (select *
					from (
						select RECORD_ID,ACCOUNT_ID,CDC_OPERATION_TYPE,	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						 from ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM
						where RECORD_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
						) 
					where rank=1 and  CDC_OPERATION_TYPE IN ('I','U') 
					)LSMV_RECEIPT_ITEM,
					(
					select RECORD_ID, Initial_Business_Unit_Code,Initial_Business_Unit,CDC_OPERATION_TYPE 
					from (
						select RECORD_ID,PARTNER_ID as Initial_Business_Unit_Code,  NAME as Initial_Business_Unit,CDC_OPERATION_TYPE,	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						from ${stage_db_name}.${stage_schema_name}.LSMV_PARTNER
						) 
					where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
					union  all
					select RECORD_ID, Initial_Business_Unit_code,Initial_Business_Unit,CDC_OPERATION_TYPE from
					(  	select RECORD_ID ,ACCOUNT_ID as Initial_Business_Unit_code,ACCOUNT_NAME as Initial_Business_Unit,CDC_OPERATION_TYPE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						from ${stage_db_name}.${stage_schema_name}.LSMV_ACCOUNTS) 
					where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
					)COMPANY_UNIT 
			WHERE LSMV_RECEIPT_ITEM.ACCOUNT_ID = COMPANY_UNIT.RECORD_ID
			 
      )LS_DB_CASE_DER_FINAL
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID = LS_DB_CASE_DER_FINAL.ARI_REC_ID
	AND LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	





UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_CTRY_RPT_TYPE_CLASSIFICATION=CASE_STUDY_FINAL.DER_CTRY_RPT_TYPE_CLASSIFICATION
FROM
(
SELECT 
RECORD_ID ,
DER_CTRY_RPT_TYPE_CLASSIFICATION
FROM 
(
select 
LSMV_RECEIPT_ITEM .RECORD_ID,
CASE 
	WHEN (LSMV_SAFETY_REPORT.PRIMARY_SRC_COUNTRY IN ('US','GU','AS','VI','MP','PR','UM') AND LSMV_SAFETY_REPORT.REPORTTYPE in ('1','3','4')) 
			THEN 'Domestic Spontaneous'
	WHEN (LSMV_SAFETY_REPORT.PRIMARY_SRC_COUNTRY IN ('US','GU','AS','VI','MP','PR','UM') AND LSMV_SAFETY_REPORT.REPORTTYPE='2'
	) 
		THEN 'Domestic Study'
	WHEN (LSMV_SAFETY_REPORT.PRIMARY_SRC_COUNTRY NOT IN ('US','GU','AS','VI','MP','PR','UM') AND LSMV_SAFETY_REPORT.REPORTTYPE in ('1','3','4')) 
			THEN 'Foreign Spontaneous'
	WHEN (LSMV_SAFETY_REPORT.PRIMARY_SRC_COUNTRY NOT IN ('US','GU','AS','VI','MP','PR','UM') AND LSMV_SAFETY_REPORT.REPORTTYPE='2' 
	) 
		THEN 'Foreign Study'
END AS DER_CTRY_RPT_TYPE_CLASSIFICATION,
row_number() over (partition by LSMV_SAFETY_REPORT.RECORD_ID order by LSMV_SAFETY_REPORT.CDC_OPERATION_TIME desc) RANK,
LSMV_SAFETY_REPORT.CDC_OPERATION_TYPE
FROM 
${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM 
LEFT OUTER JOIN ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT ON LSMV_RECEIPT_ITEM.RECORD_ID = LSMV_SAFETY_REPORT.ARI_REC_ID
WHERE LSMV_RECEIPT_ITEM.RECORD_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
) WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U') AND DER_CTRY_RPT_TYPE_CLASSIFICATION IS NOT NULL
) CASE_STUDY_FINAL
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID =CASE_STUDY_FINAL.record_id
AND   LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP SET DER_AD_EXP_REACT_DRUG_CHAR=
CASE WHEN T.DER_AD_EXP_REACT_DRUG_CHAR IS NOT NULL 
     THEN CASE WHEN LENGTH(T.DER_AD_EXP_REACT_DRUG_CHAR)>=4000 
               THEN SUBSTRING(T.DER_AD_EXP_REACT_DRUG_CHAR,0,3996)||' ...' ELSE T.DER_AD_EXP_REACT_DRUG_CHAR END
	 ELSE NULL END FROM
(
SELECT AA.ARI_REC_ID ,
LISTAGG( COALESCE(AA.LLT_NAME,'-') || ' (' || AA.TEXT1  || ')','\n' )
WITHIN GROUP (ORDER BY AA.ARI_REC_ID,AA.RANK_ORDER)  AS DER_AD_EXP_REACT_DRUG_CHAR  
FROM
(
SELECT DISTINCT
 B.ARI_REC_ID ,
 B.RANK_ORDER,
 L_LOW_LEVEL_TERM_S.LLT_NAME,
COALESCE(CASE
        WHEN B.REACTMEDDRAPT_CODE IS NOT NULL THEN L_PREF_TERM_S.PT_NAME
        ELSE B.REACTIONTERM
         END,'-')  AS TEXT1
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACTION_SUBSET_TMP B 
LEFT OUTER JOIN ( SELECT PT_CODE,PT_NAME,c.MEDDRA_VERSION FROM (SELECT DISTINCT PT_CODE,PT_NAME,MEDDRA_VERSION
	FROM 
	(
		SELECT  RECORD_ID ,
		PT_CODE, 
		PT_NAME, 
		MEDDRA_VERSION,
		CDC_OPERATION_TYPE,
		ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK  
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
		  
	) WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')) C 
INNER JOIN ( SELECT 'v.'||MEDDRA_VERSION AS MEDDRA_VERSION FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') ) MD
ON C.MEDDRA_VERSION = MD.MEDDRA_VERSION
 )L_PREF_TERM_S
ON TRY_TO_NUMBER(B.REACTMEDDRAPT_CODE)=L_PREF_TERM_S.PT_CODE
LEFT OUTER JOIN  (		SELECT RECORD_ID,LLT_NAME,LLT_CODE,D.MEDDRA_VERSION
		FROM
		(
		SELECT RECORD_ID,LLT_NAME,LLT_CODE,MEDDRA_VERSION
		FROM
		(
		SELECT RECORD_ID,LLT_NAME,LLT_CODE,MEDDRA_VERSION,CDC_OPERATION_TYPE
						,ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_LOW_LEVEL_TERM)
											WHERE RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')) D
INNER JOIN (SELECT 'v.'||MEDDRA_VERSION AS MEDDRA_VERSION FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE  EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')) MD
ON D.MEDDRA_VERSION = MD.MEDDRA_VERSION
 )L_LOW_LEVEL_TERM_S
ON TRY_TO_NUMBER(B.REACTMEDDRALLT_CODE)=L_LOW_LEVEL_TERM_S.LLT_CODE
INNER JOIN (SELECT ARI_REC_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP) D
ON D.ARI_REC_ID= B.ARI_REC_ID
WHERE 
 B.EVENT_TYPE IN ('1')
 )AA
GROUP BY 
AA.ARI_REC_ID
) T
WHERE LS_DB_CASE_DER_TMP.ARI_REC_ID =T.ARI_REC_ID
AND   LS_DB_CASE_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP)
where target_table_name='LS_DB_CASE_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                  where target_table_name='LS_DB_CASE_DER'
					and LOAD_STATUS = 'In Progress') 
; 
                  
                
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER   
SET LS_DB_CASE_DER.ARI_REC_ID                   = LS_DB_CASE_DER_TMP.ARI_REC_ID,
LS_DB_CASE_DER.PROCESSING_DT                    = LS_DB_CASE_DER_TMP.PROCESSING_DT,
LS_DB_CASE_DER.expiry_date                       =LS_DB_CASE_DER_TMP.expiry_date,
LS_DB_CASE_DER.date_modified                      =LS_DB_CASE_DER_TMP.date_modified,
LS_DB_CASE_DER.load_ts                          =LS_DB_CASE_DER_TMP.load_ts,
LS_DB_CASE_DER.DER_CHILD_CASE_FLAG              =LS_DB_CASE_DER_TMP.DER_CHILD_CASE_FLAG ,    
LS_DB_CASE_DER.DER_LITRTURE_IN_VANCOUVR_STYLE   =LS_DB_CASE_DER_TMP.DER_LITRTURE_IN_VANCOUVR_STYLE 
,LS_DB_CASE_DER.DER_MIN_THERAPY_START_DATE      =LS_DB_CASE_DER_TMP.DER_MIN_THERAPY_START_DATE     
,LS_DB_CASE_DER.DER_MAX_THERAPY_END_DATE        =LS_DB_CASE_DER_TMP.DER_MAX_THERAPY_END_DATE       
,LS_DB_CASE_DER.DER_SUSPECT_DRUGS_PPD_COMB      =LS_DB_CASE_DER_TMP.DER_SUSPECT_DRUGS_PPD_COMB     
,LS_DB_CASE_DER.DER_SUSPECT_PPD                 =LS_DB_CASE_DER_TMP.DER_SUSPECT_PPD                
,LS_DB_CASE_DER.DER_DRUG_NAME_CONCOMITANT       =LS_DB_CASE_DER_TMP.DER_DRUG_NAME_CONCOMITANT      
,LS_DB_CASE_DER.DER_RANK_1_EVENT_PRIMARY_SOC    =LS_DB_CASE_DER_TMP.DER_RANK_1_EVENT_PRIMARY_SOC   
,LS_DB_CASE_DER.DER_RNK1_EVNT_PRIM_SOC_INTORDR  =LS_DB_CASE_DER_TMP.DER_RNK1_EVNT_PRIM_SOC_INTORDR 
,LS_DB_CASE_DER.DER_EVENTS_COMBINED             =LS_DB_CASE_DER_TMP.DER_EVENTS_COMBINED            
,LS_DB_CASE_DER.DER_NON_PRIMARY_SOURCES         =LS_DB_CASE_DER_TMP.DER_NON_PRIMARY_SOURCES        
,LS_DB_CASE_DER.DER_REFERENCE_TYPE              =LS_DB_CASE_DER_TMP.DER_REFERENCE_TYPE             
,LS_DB_CASE_DER.DER_SOURCE                      =LS_DB_CASE_DER_TMP.DER_SOURCE                     
,LS_DB_CASE_DER.DER_STANDARD_PRIMARY_SOURCE     =LS_DB_CASE_DER_TMP.DER_STANDARD_PRIMARY_SOURCE    
,LS_DB_CASE_DER.DER_AUTOPSY_DATE                =LS_DB_CASE_DER_TMP.DER_AUTOPSY_DATE               
,LS_DB_CASE_DER.DER_DEATH_DATE                  =LS_DB_CASE_DER_TMP.DER_DEATH_DATE                 
,LS_DB_CASE_DER.DER_DATE_OF_BIRTH               =LS_DB_CASE_DER_TMP.DER_DATE_OF_BIRTH              
,LS_DB_CASE_DER.DER_PATIENT_AGE_IN_YEARS        =LS_DB_CASE_DER_TMP.DER_PATIENT_AGE_IN_YEARS       
,LS_DB_CASE_DER.DER_SUS_DRG_PPD_SUB             =LS_DB_CASE_DER_TMP.DER_SUS_DRG_PPD_SUB            
,LS_DB_CASE_DER.DER_PRIMARY_SOURCE              =LS_DB_CASE_DER_TMP.DER_PRIMARY_SOURCE             
,LS_DB_CASE_DER.DER_CONCOMITANT_MDCTN_INGRDNTS  =LS_DB_CASE_DER_TMP.DER_CONCOMITANT_MDCTN_INGRDNTS 
,LS_DB_CASE_DER.DER_E2B_REPORT_DUPLICATE_NO     =LS_DB_CASE_DER_TMP.DER_E2B_REPORT_DUPLICATE_NO    
,LS_DB_CASE_DER.DER_MED_HISTORY_CONTINUING      =LS_DB_CASE_DER_TMP.DER_MED_HISTORY_CONTINUING     
,LS_DB_CASE_DER.DER_CAUSE_OF_DEATH              =LS_DB_CASE_DER_TMP.DER_CAUSE_OF_DEATH             
,LS_DB_CASE_DER.DER_AUTOPSY_DETERMINED          =LS_DB_CASE_DER_TMP.DER_AUTOPSY_DETERMINED         
,LS_DB_CASE_DER.DER_CASE_TYPE                   =LS_DB_CASE_DER_TMP.DER_CASE_TYPE                  
,LS_DB_CASE_DER.DER_CASE_CONFIRMED_BY_HP        =LS_DB_CASE_DER_TMP.DER_CASE_CONFIRMED_BY_HP       
,LS_DB_CASE_DER.DER_SPECIAL_INTEREST_CASE_FLAG  =LS_DB_CASE_DER_TMP.DER_SPECIAL_INTEREST_CASE_FLAG 
,LS_DB_CASE_DER.DER_ROLE_PPD_CONCAT             =LS_DB_CASE_DER_TMP.DER_ROLE_PPD_CONCAT       
,LS_DB_CASE_DER.DER_PARENT_MEDICAL_HISTORY      =LS_DB_CASE_DER_TMP.DER_PARENT_MEDICAL_HISTORY 
,LS_DB_CASE_DER.DER_PRODUCT_DESC_RANK_1         =LS_DB_CASE_DER_TMP.DER_PRODUCT_DESC_RANK_1       
,LS_DB_CASE_DER.DER_SUSPECT_PRODUCT_DESC        =LS_DB_CASE_DER_TMP.DER_SUSPECT_PRODUCT_DESC      
,LS_DB_CASE_DER.DER_DRUG_NAME                   =LS_DB_CASE_DER_TMP.DER_DRUG_NAME                 
,LS_DB_CASE_DER.DER_MUL_THERAPY_REC             =LS_DB_CASE_DER_TMP.DER_MUL_THERAPY_REC           
,LS_DB_CASE_DER.DER_APPROVAL_NO                 =LS_DB_CASE_DER_TMP.DER_APPROVAL_NO               
,LS_DB_CASE_DER.DER_SUSPECT_DRUGS_LTN_COMBINED  =LS_DB_CASE_DER_TMP.DER_SUSPECT_DRUGS_LTN_COMBINED
,LS_DB_CASE_DER.DER_SUSPECT_DRUGS_TN_PPD_COMB   =LS_DB_CASE_DER_TMP.DER_SUSPECT_DRUGS_TN_PPD_COMB 
,LS_DB_CASE_DER.DER_STUDY_PRODUCT_TYPE_RANK_1   =LS_DB_CASE_DER_TMP.DER_STUDY_PRODUCT_TYPE_RANK_1
,LS_DB_CASE_DER.DER_REPORTER_TYPE               =LS_DB_CASE_DER_TMP.DER_REPORTER_TYPE
,LS_DB_CASE_DER.DER_PREEX_HEPTC_IMPRMNT         =LS_DB_CASE_DER_TMP.DER_PREEX_HEPTC_IMPRMNT
,LS_DB_CASE_DER.DER_LACTATION_CASE_BROAD        =LS_DB_CASE_DER_TMP.DER_LACTATION_CASE_BROAD
,LS_DB_CASE_DER.DER_OFF_LABEL_USE_BROAD         =LS_DB_CASE_DER_TMP.DER_OFF_LABEL_USE_BROAD
,LS_DB_CASE_DER.DER_SUSPECT_STUDY_PRODUCT       =LS_DB_CASE_DER_TMP.DER_SUSPECT_STUDY_PRODUCT
,LS_DB_CASE_DER.DER_COD       					=LS_DB_CASE_DER_TMP.DER_COD
,LS_DB_CASE_DER.DER_PREEX_RNL_IMPRMNT       	=LS_DB_CASE_DER_TMP.DER_PREEX_RNL_IMPRMNT
,LS_DB_CASE_DER.DER_FATAL_CASE_FLAG       	=LS_DB_CASE_DER_TMP.DER_FATAL_CASE_FLAG
,LS_DB_CASE_DER.DER_MT_LITERATURE_REFERENCE_JAP       	=LS_DB_CASE_DER_TMP.DER_MT_LITERATURE_REFERENCE_JAP
,LS_DB_CASE_DER.DER_PAST_DISEASES_COMBINED       	=LS_DB_CASE_DER_TMP.DER_PAST_DISEASES_COMBINED
,LS_DB_CASE_DER.DER_COUNTRY_PUBLISHED       	=LS_DB_CASE_DER_TMP.DER_COUNTRY_PUBLISHED
,LS_DB_CASE_DER.DER_MT_LITERATURE_REFERENCE       	=LS_DB_CASE_DER_TMP.DER_MT_LITERATURE_REFERENCE                  
,LS_DB_CASE_DER.DER_INCIDENT_IMDRF   	=LS_DB_CASE_DER_TMP.DER_INCIDENT_IMDRF 
,LS_DB_CASE_DER.DER_SL_CLASSIFICATION   	=LS_DB_CASE_DER_TMP.DER_SL_CLASSIFICATION 
,LS_DB_CASE_DER.DER_SOLICITED_CASE_SYMBOL   	=LS_DB_CASE_DER_TMP.DER_SOLICITED_CASE_SYMBOL 
,LS_DB_CASE_DER.DER_APPROVAL_TYPE   	=LS_DB_CASE_DER_TMP.DER_APPROVAL_TYPE 
,LS_DB_CASE_DER.DER_CASE_SERIOUSNESS_CRITERIA   	=LS_DB_CASE_DER_TMP.DER_CASE_SERIOUSNESS_CRITERIA 
,LS_DB_CASE_DER.DER_PPD_STUDY_PRDCT_TYPE_COMB   	=LS_DB_CASE_DER_TMP.DER_PPD_STUDY_PRDCT_TYPE_COMB 
,LS_DB_CASE_DER.DER_CASE_UPGRADE_TO_FATAL_FLAG   	=LS_DB_CASE_DER_TMP.DER_CASE_UPGRADE_TO_FATAL_FLAG 
,LS_DB_CASE_DER.DER_SENDER_ORGANIZATION   	=LS_DB_CASE_DER_TMP.DER_SENDER_ORGANIZATION 
,LS_DB_CASE_DER.DER_CTRY_RPT_TYPE_CLASSIFICATION =LS_DB_CASE_DER_TMP.DER_CTRY_RPT_TYPE_CLASSIFICATION
,LS_DB_CASE_DER.DER_AD_EXP_REACT_DRUG_CHAR =LS_DB_CASE_DER_TMP.DER_AD_EXP_REACT_DRUG_CHAR
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP 
WHERE 	LS_DB_CASE_DER.INTEGRATION_ID = LS_DB_CASE_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_DER_TMP.PROCESSING_DT = LS_DB_CASE_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
/*           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_DER.SEQ_PRODUCT ,LS_DB_CASE_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP 
ON LS_DB_CASE_DER.SEQ_PRODUCT=LS_DB_CASE_DER_TMP.SEQ_PRODUCT
AND LS_DB_CASE_DER.INTEGRATION_ID = LS_DB_CASE_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_DER.SEQ_PRODUCT in (select SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP )
) TMP where LS_DB_CASE_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_DER.SEQ_PRODUCT ,LS_DB_CASE_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP 
    ON LS_DB_CASE_DER.SEQ_PRODUCT=LS_DB_CASE_DER_TMP.SEQ_PRODUCT
    AND LS_DB_CASE_DER.INTEGRATION_ID = LS_DB_CASE_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_DER.SEQ_PRODUCT in (select SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

  */ 
     


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER
( ARI_REC_ID    ,
processing_dt ,
expiry_date   ,
load_ts, date_modified,
INTEGRATION_ID,
DER_CHILD_CASE_FLAG,
DER_LITRTURE_IN_VANCOUVR_STYLE
,DER_MIN_THERAPY_START_DATE    
,DER_MAX_THERAPY_END_DATE      	
,DER_SUSPECT_DRUGS_PPD_COMB    
,DER_SUSPECT_PPD               
,DER_DRUG_NAME_CONCOMITANT     
,DER_RANK_1_EVENT_PRIMARY_SOC  
,DER_RNK1_EVNT_PRIM_SOC_INTORDR
,DER_EVENTS_COMBINED           
,DER_NON_PRIMARY_SOURCES       
,DER_REFERENCE_TYPE            
,DER_SOURCE                    
,DER_STANDARD_PRIMARY_SOURCE   
,DER_AUTOPSY_DATE              
,DER_DEATH_DATE                
,DER_DATE_OF_BIRTH             
,DER_PATIENT_AGE_IN_YEARS      
,DER_SUS_DRG_PPD_SUB           
,DER_PRIMARY_SOURCE            
,DER_CONCOMITANT_MDCTN_INGRDNTS
,DER_E2B_REPORT_DUPLICATE_NO   
,DER_MED_HISTORY_CONTINUING    
,DER_CAUSE_OF_DEATH            
,DER_AUTOPSY_DETERMINED        
,DER_CASE_TYPE                 
,DER_CASE_CONFIRMED_BY_HP      
,DER_SPECIAL_INTEREST_CASE_FLAG
,DER_ROLE_PPD_CONCAT
,DER_PARENT_MEDICAL_HISTORY
,DER_PRODUCT_DESC_RANK_1         
,DER_SUSPECT_PRODUCT_DESC        
,DER_DRUG_NAME                   
,DER_MUL_THERAPY_REC             
,DER_APPROVAL_NO                 
,DER_SUSPECT_DRUGS_LTN_COMBINED  
,DER_SUSPECT_DRUGS_TN_PPD_COMB
,DER_STUDY_PRODUCT_TYPE_RANK_1 
,DER_REPORTER_TYPE 
,DER_PREEX_HEPTC_IMPRMNT
,DER_LACTATION_CASE_BROAD
,DER_OFF_LABEL_USE_BROAD
,DER_SUSPECT_STUDY_PRODUCT
,DER_COD
,DER_PREEX_RNL_IMPRMNT
,DER_FATAL_CASE_FLAG
,DER_MT_LITERATURE_REFERENCE_JAP
,DER_PAST_DISEASES_COMBINED 
,DER_COUNTRY_PUBLISHED      
,DER_MT_LITERATURE_REFERENCE
,DER_INCIDENT_IMDRF
,DER_SL_CLASSIFICATION
,DER_SOLICITED_CASE_SYMBOL
,DER_APPROVAL_TYPE
,DER_CASE_SERIOUSNESS_CRITERIA
,DER_PPD_STUDY_PRDCT_TYPE_COMB                            
,DER_CASE_UPGRADE_TO_FATAL_FLAG 
,DER_SENDER_ORGANIZATION
,DER_CTRY_RPT_TYPE_CLASSIFICATION  
,DER_AD_EXP_REACT_DRUG_CHAR              )
SELECT 
  ARI_REC_ID    ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,                  
INTEGRATION_ID,
DER_CHILD_CASE_FLAG ,
DER_LITRTURE_IN_VANCOUVR_STYLE 
,DER_MIN_THERAPY_START_DATE    
,DER_MAX_THERAPY_END_DATE      	
,DER_SUSPECT_DRUGS_PPD_COMB    
,DER_SUSPECT_PPD               
,DER_DRUG_NAME_CONCOMITANT     
,DER_RANK_1_EVENT_PRIMARY_SOC  
,DER_RNK1_EVNT_PRIM_SOC_INTORDR
,DER_EVENTS_COMBINED           
,DER_NON_PRIMARY_SOURCES       
,DER_REFERENCE_TYPE            
,DER_SOURCE                    
,DER_STANDARD_PRIMARY_SOURCE   
,DER_AUTOPSY_DATE              
,DER_DEATH_DATE                
,DER_DATE_OF_BIRTH             
,DER_PATIENT_AGE_IN_YEARS      
,DER_SUS_DRG_PPD_SUB           
,DER_PRIMARY_SOURCE            
,DER_CONCOMITANT_MDCTN_INGRDNTS
,DER_E2B_REPORT_DUPLICATE_NO   
,DER_MED_HISTORY_CONTINUING    
,DER_CAUSE_OF_DEATH            
,DER_AUTOPSY_DETERMINED        
,DER_CASE_TYPE                 
,DER_CASE_CONFIRMED_BY_HP      
,DER_SPECIAL_INTEREST_CASE_FLAG
,DER_ROLE_PPD_CONCAT
,DER_PARENT_MEDICAL_HISTORY
,DER_PRODUCT_DESC_RANK_1         
,DER_SUSPECT_PRODUCT_DESC        
,DER_DRUG_NAME                   
,DER_MUL_THERAPY_REC             
,DER_APPROVAL_NO                 
,DER_SUSPECT_DRUGS_LTN_COMBINED  
,DER_SUSPECT_DRUGS_TN_PPD_COMB
,DER_STUDY_PRODUCT_TYPE_RANK_1
,DER_REPORTER_TYPE
,DER_PREEX_HEPTC_IMPRMNT
,DER_LACTATION_CASE_BROAD
,DER_OFF_LABEL_USE_BROAD
,DER_SUSPECT_STUDY_PRODUCT
,DER_COD
,DER_PREEX_RNL_IMPRMNT
,DER_FATAL_CASE_FLAG
,DER_MT_LITERATURE_REFERENCE_JAP
,DER_PAST_DISEASES_COMBINED 
,DER_COUNTRY_PUBLISHED      
,DER_MT_LITERATURE_REFERENCE
,DER_INCIDENT_IMDRF   
,DER_SL_CLASSIFICATION
,DER_SOLICITED_CASE_SYMBOL
,DER_APPROVAL_TYPE
,DER_CASE_SERIOUSNESS_CRITERIA
,DER_PPD_STUDY_PRDCT_TYPE_COMB  
,DER_CASE_UPGRADE_TO_FATAL_FLAG  
,DER_SENDER_ORGANIZATION
,DER_CTRY_RPT_TYPE_CLASSIFICATION     
,DER_AD_EXP_REACT_DRUG_CHAR                    
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


/*
DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

*/

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_DER.INTEGRATION_ID = LS_DB_CASE_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
 AND MD5(NVL(LS_DB_CASE_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

/*

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');
*/

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;

