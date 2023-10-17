
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION_DER'
-- call PRC_LS_DB_REACTION_DER()
-- -- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACTION_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();




insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_REACTION_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_REACTION_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION_DER'),
	'LSRA','Case','LS_DB_REACTION_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_REACTION_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION)
WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC AS
select distinct RECORD_ID,ARI_REC_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_REACTION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID||'-'||AD.SEQ_PRODUCT AS UNBLINDED_REC
				from 
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC)
				
				) LSMV_DRUG 
				JOIN
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC)
				) AD
				ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
				AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT
				AND LSMV_DRUG.rank=1 AND AD.rank=1 AND   LSMV_DRUG.CDC_OPERATION_TYPE IN ('I','U')  AND   AD.CDC_OPERATION_TYPE IN ('I','U') ;





UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION   
SET LS_DB_REACTION.DER_AE_CESSATION_DATE=LS_DB_REACTION_TMP.DER_AE_CESSATION_DATE ,
LS_DB_REACTION.DER_INCIDENT_ONSET_DATE=LS_DB_REACTION_TMP.DER_INCIDENT_ONSET_DATE ,
LS_DB_REACTION.DER_EVENT_DURATION_NRMLZD_DAYS=LS_DB_REACTION_TMP.DER_EVENT_DURATION_NRMLZD_DAYS
FROM (SELECT 	distinct RECORD_ID,
		CASE    
		WHEN REACTENDDATE_NF IS NOT NULL THEN REACTENDDATE_NF
        WHEN REACTENDDATE IS NULL THEN to_char(REACTENDDATE,'DD-Mon-YYYY')
        WHEN REACTENDDATEFMT is NULL then to_char(REACTENDDATE,'DD-Mon-YYYY')
        WHEN REACTENDDATEFMT in (0,102) THEN to_char(REACTENDDATE,'DD-Mon-YYYY')
        WHEN REACTENDDATEFMT in (1,602) THEN to_char(REACTENDDATE,'YYYY')
        WHEN REACTENDDATEFMT in (2,610) THEN to_char(REACTENDDATE,'Mon-YYYY')
        WHEN REACTENDDATEFMT IN (3,4,5,6,7,8,9,204,203,611) THEN to_char(REACTENDDATE,'DD-Mon-YYYY')
        END DER_AE_CESSATION_DATE,
		case 	when  REACTSTARTDATEFMT is null  then to_char(REACTSTARTDATE,'DD-Mon-YYYY')
			when  REACTSTARTDATEFMT in (0,102)  then to_char(REACTSTARTDATE,'DD-Mon-YYYY')
			when  REACTSTARTDATEFMT in (1,602)  then to_char(REACTSTARTDATE,'YYYY')
			when  REACTSTARTDATEFMT in (2,610)  then to_char(REACTSTARTDATE,'Mon-YYYY')
			when  REACTSTARTDATEFMT in ('3','4','5','6','7','8','9',204,203,611)  then to_char(REACTSTARTDATE,'DD-Mon-YYYY')
		end DER_INCIDENT_ONSET_DATE,
		case
		when	REACTENDDATE is not null and REACTSTARTDATE is not null 
					and REACTSTARTDATEFMT  not in ('1','2','610','602')  and  REACTENDDATEFMT not in ('1','2','610','602')
			then	(datediff(day,REACTSTARTDATE,  REACTENDDATE))
		else
			ceil(case  
					when  REACTDURATIONUNIT='804'  then  REACTDURATION
					when  REACTDURATIONUNIT='807'  then  REACTDURATION/86400
					when  REACTDURATIONUNIT='806'  then  REACTDURATION/1440
					when  REACTDURATIONUNIT='805'  then  REACTDURATION/24
					when  REACTDURATIONUNIT='803'  then  REACTDURATION*7
					when  REACTDURATIONUNIT='802'  then  REACTDURATION*30
					when  REACTDURATIONUNIT='801'  then  REACTDURATION*365
					else  null
			end)
	end	AS	DER_EVENT_DURATION_NRMLZD_DAYS
	
	
FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION
 where RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC)
AND EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_REACTION_TMP
    WHERE LS_DB_REACTION.RECORD_ID = LS_DB_REACTION_TMP.RECORD_ID	
	AND LS_DB_REACTION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');






                                   

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION
   SET LS_DB_REACTION.DER_CASE_EVENT_SERIOUSNESS = LS_DB_REACTION_TMP.DER_CASE_EVENT_SERIOUSNESS
FROM (SELECT ARI_REC_ID,
             RECORD_ID,
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
            WHERE A.RECORD_ID IN (SELECT RECORD_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION_CASE_QFC)) A
      WHERE RANK = 1
      AND   CDC_OPERATION_TYPE IN ('I','U')) LS_DB_REACTION_TMP
WHERE LS_DB_REACTION.ARI_REC_ID = LS_DB_REACTION_TMP.ARI_REC_ID
AND   LS_DB_REACTION.record_id = LS_DB_REACTION_TMP.record_id
AND   LS_DB_REACTION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACTION),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REACTION_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION_DER'
and LOAD_STATUS = 'In Progress') ;	


 RETURN 'LS_DB_REACTION_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REACTION_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

                                   
                                   
                             
                                   
                                   
                           