--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY_DER();
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();


-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_DRUG_THERAPY_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_DRUG_THERAPY_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY ADD COLUMN DER_THERAPY_END_DATE TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY ADD COLUMN DER_THERAPY_START_DATE TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY ADD COLUMN DER_TREATMENT_DURATION_NORMAL TEXT;
*/






insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_DRUG_THERAPY_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_DRUG_THERAPY_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_THERAPY_DER'),
	'LSRA','Case','LS_DB_DRUG_THERAPY_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_DRUG_THERAPY_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_THERAPY_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY)
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_THERAPY_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_QFC AS
select distinct dgth_ari_rec_id as ari_rec_id
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_THERAPY_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_THERAPY_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


/*( select UNBLINDED_REC from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_UNBLINDED
		)*/


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID||'-'||AD.SEQ_PRODUCT AS UNBLINDED_REC
				from 
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_QFC)
				
				) LSMV_DRUG 
				JOIN
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_QFC)
				
				) AD
				ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
				AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT
				AND LSMV_DRUG.rank=1 AND AD.rank=1  AND   LSMV_DRUG.CDC_OPERATION_TYPE IN ('I','U') 
				AND AD.CDC_OPERATION_TYPE IN ('I','U') 

;








UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY   
SET LS_DB_DRUG_THERAPY.DER_THERAPY_START_DATE=LS_DB_DRUG_THERAPY_TMP.DER_THERAPY_START_DATE   
,LS_DB_DRUG_THERAPY.DER_THERAPY_END_DATE=LS_DB_DRUG_THERAPY_TMP.DER_THERAPY_END_DATE
FROM (
select dgth_ari_rec_id,dgth_record_id,
case  when  dgth_DRUGSTARTDATE_NF IS NOT NULL THEN UPPER(dgth_DRUGSTARTDATE_NF)
	  when  dgth_DRUGSTARTDATEFMT is null  then null
	  when  dgth_DRUGSTARTDATEFMT in ('0','7','8','9','102')  then UPPER(to_char(dgth_DRUGSTARTDATE,'DD-MON-YYYY'))
	  when  dgth_DRUGSTARTDATEFMT in ('1','602')  then UPPER(to_char(dgth_DRUGSTARTDATE,'YYYY'))
	  when  dgth_DRUGSTARTDATEFMT in ('2','610')  then UPPER(to_char(dgth_DRUGSTARTDATE,'MON-YYYY'))
	--  when  DRUGSTARTDATEFMT in ('3','204')  then UPPER(to_char(DRUGSTARTDATE,'DD-MON-YYYY'))
	 -- when  DRUGSTARTDATEFMT in ('4','204')  then UPPER(to_char(DRUGSTARTDATE,'DD-MON-YYYY hh24'))
	  when  dgth_DRUGSTARTDATEFMT in ('5','203')  then UPPER(to_char(dgth_DRUGSTARTDATE,'DD-MON-YYYY hh24:mi'))
  	  when  dgth_DRUGSTARTDATEFMT in ('6','204')  then UPPER(to_char(dgth_DRUGSTARTDATE,'DD-MON-YYYY'))
	  when  dgth_DRUGSTARTDATEFMT in ('6','611')  then UPPER(to_char(dgth_DRUGSTARTDATE,'DD-MON-YYYY'))
end DER_THERAPY_START_DATE,
case  when  dgth_DRUGENDDATE_NF IS NOT NULL THEN UPPER(dgth_DRUGENDDATE_NF)
	  when  dgth_DRUGENDDATEFMT is null  then null
	  when  dgth_DRUGENDDATEFMT in ('0','7','8','9','102')  then UPPER(to_char(dgth_DRUGENDDATE,'DD-MON-YYYY'))
	  when  dgth_DRUGENDDATEFMT in  ('1','602')  then UPPER(to_char(dgth_DRUGENDDATE,'YYYY'))
	  when  dgth_DRUGENDDATEFMT in ('2','610')  then UPPER(to_char(dgth_DRUGENDDATE,'MON-YYYY'))
--	  when  dgth_DRUGENDDATEFMT in ('3','204')  then UPPER(to_char(DRUGENDDATE,'DD-MON-YYYY'))
	--  when  DRUGENDDATEFMT in ('4','204')  then UPPER(to_char(DRUGENDDATE,'DD-MON-YYYY hh24'))
	  when  dgth_DRUGENDDATEFMT in ('5','203')  then UPPER(to_char(dgth_DRUGENDDATE,'DD-MON-YYYY hh24:mi'))
  	  when  dgth_DRUGENDDATEFMT in ('6','204')  then UPPER(to_char(dgth_DRUGENDDATE,'DD-MON-YYYY'))
	  when  dgth_DRUGENDDATEFMT in ('6','611')  then UPPER(to_char(dgth_DRUGENDDATE,'DD-MON-YYYY'))
end DER_THERAPY_END_DATE

FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY where dgth_ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_DRUG_THERAPY_TMP
    WHERE LS_DB_DRUG_THERAPY.dgth_ari_rec_id = LS_DB_DRUG_THERAPY_TMP.dgth_ari_rec_id	
        and LS_DB_DRUG_THERAPY.dgth_record_id=LS_DB_DRUG_THERAPY_TMP.dgth_record_id
	AND LS_DB_DRUG_THERAPY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_THERAPY),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_THERAPY_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_THERAPY_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_DRUG_THERAPY_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG_THERAPY_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;