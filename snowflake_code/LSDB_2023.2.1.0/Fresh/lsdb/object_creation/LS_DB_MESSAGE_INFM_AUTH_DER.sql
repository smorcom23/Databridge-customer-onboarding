--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH_DER();
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_MESSAGE_INFM_AUTH_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_MESSAGE_INFM_AUTH_DER';


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_MESSAGE_INFM_AUTH_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_MESSAGE_INFM_AUTH_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;
--delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MESSAGE_INFM_AUTH_DER'

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MESSAGE_INFM_AUTH_DER'),
	'LSRA','Case','LS_DB_MESSAGE_INFM_AUTH_DER',null,CURRENT_TIMESTAMP(),null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_MESSAGE_INFM_AUTH_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_MESSAGE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH)
WHERE TARGET_TABLE_NAME = 'LS_DB_MESSAGE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_CASE_QFC AS
select distinct receipt_id As ARI_REC_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_MESSAGE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_MESSAGE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


drop  table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
create temp table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
 
 select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE from 
    (
              SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE
              ,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
                                                                        order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
                                                                                       FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9601')
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
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
;




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH   
SET LS_DB_MESSAGE_INFM_AUTH.DER_OA_LATE_BY_DAYS =LS_DB_MESSAGE_INFM_AUTH_TMP.DER_OA_LATE_BY_DAYS   ,
LS_DB_MESSAGE_INFM_AUTH.DER_OA_LATE_FLAG        =LS_DB_MESSAGE_INFM_AUTH_TMP.DER_OA_LATE_FLAG ,
LS_DB_MESSAGE_INFM_AUTH.DER_TIME_TO_MDN         =LS_DB_MESSAGE_INFM_AUTH_TMP.DER_TIME_TO_MDN   
FROM (SELECT DISTINCT integration_id,
	CASE 
		WHEN STMSG_AFFILIATE_SUBMISSION_DUE_DATE IS NOT NULL 
		THEN 
		CASE
		WHEN
		DATEDIFF(DAY, STMSG_AFFILIATE_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) > 0 THEN 
		CAST(DATEDIFF(DAY, STMSG_AFFILIATE_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) AS VARCHAR)
		WHEN DATEDIFF(DAY, STMSG_AFFILIATE_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) <= 0 THEN '-'
		ELSE NULL
		END
		WHEN STMSG_AFFILIATE_SUBMISSION_DUE_DATE IS NULL THEN
		CASE
		WHEN
		DATEDIFF(DAY, STMSG_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) > 0 THEN 
		CAST(DATEDIFF(DAY, STMSG_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) AS VARCHAR)
		WHEN DATEDIFF(DAY, STMSG_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) <= 0 THEN '-'
		ELSE NULL
		END
	END AS DER_OA_LATE_BY_DAYS
	,
	CASE 
	WHEN STMSG_AFFILIATE_SUBMISSION_DUE_DATE IS NOT NULL 
	THEN 
		CASE
		WHEN
		DATEDIFF(DAY, STMSG_AFFILIATE_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) > 0 THEN 
		'YES'
		WHEN DATEDIFF(DAY, STMSG_AFFILIATE_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) <= 0 
		THEN 'NO'
		ELSE NULL
		END
	WHEN STMSG_AFFILIATE_SUBMISSION_DUE_DATE IS NULL THEN
		CASE
		WHEN
		DATEDIFF(DAY, STMSG_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) > 0 THEN 
		'YES'
		WHEN DATEDIFF(DAY, STMSG_SUBMISSION_DUE_DATE,INFMAUTH_DATE_INFORMED ) <= 0 THEN 'NO'
		ELSE NULL
		END
	END AS DER_OA_LATE_FLAG,
	
	CASE 
	WHEN (STMSG_LATEST_RECEIPT_DATE IS NOT NULL AND STMSG_MDN_DATE IS NOT NULL AND DER_TIME_TO_MDN IS NULL)
	THEN 
		CASE
		WHEN 
		STMSG_MDN_DATE <> STMSG_LATEST_RECEIPT_DATE THEN DATEDIFF(DAY, STMSG_LATEST_RECEIPT_DATE,STMSG_MDN_DATE )
		WHEN STMSG_MDN_DATE = STMSG_LATEST_RECEIPT_DATE THEN 0
		ELSE ''
		END 		
	END	 AS DER_TIME_TO_MDN

	FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH 
	where receipt_id in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	) LS_DB_MESSAGE_INFM_AUTH_TMP
    WHERE LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID	
	AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH   
SET 
LS_DB_MESSAGE_INFM_AUTH.DER_SUBMISSION_FORMAT  = LS_DB_MESSAGE_INFM_AUTH_TMP.DER_SUBMISSION_FORMAT_1

FROM (SELECT DISTINCT integration_id,expiry_date,
	CASE 
	WHEN (STMSG_FORMAT_TYPE IS NOT NULL)
	THEN 
		CASE
		WHEN
		CODELIST_SUBSET_tmp.DECODE='E2B' THEN 'XML'
		WHEN (CODELIST_SUBSET_tmp.DECODE='FDA3500A (Drug)' OR  CODELIST_SUBSET_tmp.DECODE = 'CIOMS I' ) THEN 'PDF'
		ELSE 'Others' 
		END 		
	END	 AS DER_SUBMISSION_FORMAT_1
    
	FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH left outer join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp on STMSG_FORMAT_TYPE=CODE 
	where receipt_id in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	) LS_DB_MESSAGE_INFM_AUTH_TMP
    WHERE LS_DB_MESSAGE_INFM_AUTH.INTEGRATION_ID = LS_DB_MESSAGE_INFM_AUTH_TMP.INTEGRATION_ID
    AND LS_DB_MESSAGE_INFM_AUTH.EXPIRY_DATE = LS_DB_MESSAGE_INFM_AUTH_TMP.EXPIRY_DATE;		

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH   
SET 
LS_DB_MESSAGE_INFM_AUTH.DER_SUBMISSION_STATUS  = CASE WHEN STMSG_MESSAGE_STATE IN(3,4,12,15) THEN 'Submitted'
		 WHEN STMSG_MESSAGE_STATE IN(6,9,11,16,17,93,94) THEN 'Not-Submitted'
		 ELSE 'In-Transit'
		 END 
WHERE 	receipt_id in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MESSAGE_INFM_AUTH),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_MESSAGE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MESSAGE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress') ;	
	
	
	
  RETURN 'LS_DB_MESSAGE_INFM_AUTH_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_MESSAGE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

