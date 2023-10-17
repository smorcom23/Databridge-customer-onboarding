	
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORTER_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_REPORTER_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_REPORTER_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER ADD COLUMN DER_REPORTER_COUNTRY TEXT;
*/




insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)
select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_REPORTER_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_REPORTER_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTER_DER'),
	'LSRA','Case','LS_DB_REPORTER_DER',null,CURRENT_TIMESTAMP(),null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_REPORTER_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER)
WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER_SEQ_PROD_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER_SEQ_PROD_QFC AS
select distinct ari_rec_id,record_id 
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_REPORTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_REPORTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
create TEMPORARY table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
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
                                   WHERE LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1; 


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER
SET LS_DB_REPORTER.DER_REPORTER_COUNTRY=LS_DB_REPORTER_TMP.DER_REPORTER_COUNTRY
FROM (
 SELECT
distinct 
LPS.AER_ID,
CDLIST.DECODE AS DER_REPORTER_COUNTRY,
LPS.SEQ_PERSON
FROM
(SELECT 	DISTINCT ARI_REC_ID AS AER_ID,
RECORD_ID AS SEQ_PERSON,	
CASE WHEN COALESCE(PRIMARY_REPORTER,'')='' then '0' else PRIMARY_REPORTER END AS PRIMARY_REPORTER_FLAG,
	CASE 
		WHEN REPORTERCOUNTRY IS NULL THEN REPORTERCOUNTRY_NF 
		ELSE REPORTERCOUNTRY 
   END COUNTRY FROM ${stage_db_name}.${stage_schema_name}.LSMV_PRIMARYSOURCE) LPS, CODELIST_SUBSET_tmp CDLIST,LS_DB_REPORTER_SEQ_PROD_QFC SEQ_PER_QRY
WHERE 	LPS.COUNTRY=CDLIST.CODE	
and LPS.AER_ID is not null    
and  LPS.AER_ID = SEQ_PER_QRY.ari_rec_id
and LPS.SEQ_PERSON = SEQ_PER_QRY.record_id                                   
) LS_DB_REPORTER_TMP
WHERE LS_DB_REPORTER.ari_rec_id = LS_DB_REPORTER_TMP.AER_ID	
and LS_DB_REPORTER.record_id = LS_DB_REPORTER_TMP.SEQ_PERSON
	AND LS_DB_REPORTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REPORTER),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REPORTER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REPORTER_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_REPORTER_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REPORTER_DER'
and LOAD_STATUS = 'In Progress'
;


  RETURN 'LS_DB_REPORTER_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;


