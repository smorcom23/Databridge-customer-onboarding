CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_MASTER_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER';


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_SAFETY_MASTER_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_SAFETY_MASTER_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_MASTER_DER'),
	'LSRA','Case','LS_DB_SAFETY_MASTER_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER)
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC AS
select distinct AERINFO_ARI_REC_ID AS ARI_REC_ID,CASE_NO
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	



DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID||'-'||AD.SEQ_PRODUCT AS UNBLINDED_REC
				from 
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
				AND   CDC_OPERATION_TYPE IN ('I','U') 
				) LSMV_DRUG 
				JOIN
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
				AND   CDC_OPERATION_TYPE IN ('I','U') 
				) AD
				ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
				AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT
				AND LSMV_DRUG.rank=1 AND AD.rank=1

;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_MISSING_CASE_VERSIONS_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_MISSING_CASE_VERSIONS_FLAG
FROM (	select
  CASE_NO,
  case  when  COUNT(distinct aerinfo_aer_version_no)<> MAX(aerinfo_aer_version_no)+1 then 'Yes'  
        else  'No'
  end   DER_MISSING_CASE_VERSIONS_FLAG
from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER 
where CASE_NO in (select  CASE_NO from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
group by CASE_NO
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.CASE_NO = LS_DB_SAFETY_MASTER_TMP.CASE_NO	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_SUSAR_DOWNGRADED=LS_DB_SAFETY_MASTER_TMP.DER_SUSAR_DOWNGRADED
FROM (	
select
DC.aerinfo_ARI_REC_ID as ARI_REC_ID,
CASE  when COALESCE(aerinfo_aer_version_no,-1) = 0 THEN 'No'
	  when CASE WHEN saftyrpt_susar = '1' THEN 1 ELSE 2 END is null and (LAG(CASE WHEN saftyrpt_susar = '1' THEN 1 ELSE 2 END,1) 
		RESPECT NULLS over ( partition by COALESCE(aerinfo_AER_NO, aerinfo_MAPPED_AER_NO) order by COALESCE(aerinfo_aer_version_no,-1))) is null then  'No'
	  WHEN LAG(CASE WHEN saftyrpt_susar = '1' THEN 1 ELSE 2 END,1) RESPECT NULLS over ( partition by COALESCE(aerinfo_AER_NO, aerinfo_MAPPED_AER_NO) order by COALESCE(aerinfo_aer_version_no,-1)) = '1'
			AND (CASE WHEN saftyrpt_susar = '1' THEN 1 ELSE 2 END IS NULL or CASE WHEN saftyrpt_susar = '1' THEN 1 ELSE 2 END = '0') THEN 'Yes'
else 'No'
END DER_SUSAR_DOWNGRADED
from
 ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER DC where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_INITIAL_RECEIVED_DATE=LS_DB_SAFETY_MASTER_TMP.DER_INITIAL_RECEIVED_DATE
FROM (	
select distinct 
CASE_NO,
case  when   saftyrpt_RECEIVE_DATE_FMT is null  then UPPER(to_char(saftyrpt_RECEIVE_DATE,'DD-MON-YYYY'))
    when   saftyrpt_RECEIVE_DATE_FMT in (0,102)  then UPPER(to_char(saftyrpt_RECEIVE_DATE,'DD-MON-YYYY'))
    when   saftyrpt_RECEIVE_DATE_FMT in (1,602)  then UPPER(to_char(saftyrpt_RECEIVE_DATE,'YYYY'))
    when   saftyrpt_RECEIVE_DATE_FMT in (2,610)  then UPPER(to_char(saftyrpt_RECEIVE_DATE,'MON-YYYY'))
    when   saftyrpt_RECEIVE_DATE_FMT in (3,4,5,6,7,8,9,204,611,203)  then UPPER(to_char(saftyrpt_RECEIVE_DATE,'DD-MON-YYYY'))
  end DER_INITIAL_RECEIVED_DATE

from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER where CASE_NO in (select  CASE_NO from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and CASE_VERSION='0'
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.CASE_NO = LS_DB_SAFETY_MASTER_TMP.CASE_NO	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_VERSION_DELETED_DATE=LS_DB_SAFETY_MASTER_TMP.DER_CASE_VERSION_DELETED_DATE,
LS_DB_SAFETY_MASTER.DER_CASE_FLT_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_CASE_FLT_FLAG
FROM (	
select
saftyrpt_ARI_REC_ID as ARI_REC_ID,
case when MSG_INBOUND_ARCHIVE IN ('1','16','016') then MSG_ARCHIVED_DATE else null end  As DER_CASE_VERSION_DELETED_DATE,
case when (saftyrpt_DEATH in ('01','1') or saftyrpt_lifethreatening in ('01','1')) and saftyrpt_ari_rec_id is not null then 'Yes' else 'No' end AS DER_CASE_FLT_FLAG
from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


/*


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_LAST_APPROVED=LS_DB_SAFETY_MASTER_TMP.DER_CASE_LAST_APPROVED
FROM (	
SELECT
	DISTINCT A.saftyrpt_ARI_REC_ID As ARI_REC_ID,
	CASE WHEN  a.saftyrpt_LATEST_AER_APPROVED=1
THEN 1
 ELSE 0 
END DER_CASE_LAST_APPROVED
FROM
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER A  where CASE_NO in (select  CASE_NO from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

*/




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET 
LS_DB_SAFETY_MASTER.DER_APPROVAL_DATE_YYYY    		=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_aer_approval_date, 'YYYY'),
LS_DB_SAFETY_MASTER.DER_APPROVAL_DATE_YYYYMM  		=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_aer_approval_date, 'YYYYMM'),
LS_DB_SAFETY_MASTER.DER_APPROVAL_DATE_YYYYMMDD		=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_aer_approval_date, 'YYYYMMDD'),
LS_DB_SAFETY_MASTER.DER_APPROVAL_DATE_YYYYQ   		=CONCAT(TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_aer_approval_date, 'YYYY'),'Q',QUARTER(LS_DB_SAFETY_MASTER_TMP.msg_aer_approval_date)),
LS_DB_SAFETY_MASTER.DER_RECEIVED_DATE_YYYY    		=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receipt_date, 'YYYY'),
LS_DB_SAFETY_MASTER.DER_RECEIVED_DATE_YYYYMM  		=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receipt_date, 'YYYYMM'),
LS_DB_SAFETY_MASTER.DER_RECEIVED_DATE_YYYYQ   		=CONCAT(TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receipt_date, 'YYYY'),'Q',QUARTER(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receipt_date)),
LS_DB_SAFETY_MASTER.DER_INITIAL_RECEIVED_DATE_YYYY  =TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receive_date, 'YYYY'),
LS_DB_SAFETY_MASTER.DER_INITIAL_RECEIVED_DATE_YYYYMM=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receive_date, 'YYYYMM'),
 LS_DB_SAFETY_MASTER.DER_INITIAL_RECEIVED_DATE_YYYYQ=CONCAT(TO_CHAR(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receive_date, 'YYYY'),'Q',QUARTER(LS_DB_SAFETY_MASTER_TMP.saftyrpt_receive_date))
FROM (	
SELECT
	DISTINCT A.msg_ARI_REC_ID As ARI_REC_ID,
	 A.msg_aer_approval_date,
	 A.saftyrpt_receive_date,
	 A.saftyrpt_receipt_date,
	 A.integration_id,
  A.expiry_date
FROM
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER A  where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
--	and EXPIRY_DATE < TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.msg_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	--  AND LS_DB_SAFETY_MASTER.EXPIRY_DATE < TO_DATE('9999-31-12','YYYY-DD-MM');
    and LS_DB_SAFETY_MASTER.integration_id=LS_DB_SAFETY_MASTER_TMP.integration_id
    and LS_DB_SAFETY_MASTER.expiry_date=LS_DB_SAFETY_MASTER_TMP.expiry_date;
	
--------------------------------
	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_INITIAL_APPROVAL_DATE_YYYY=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_initial_case_approval_date, 'YYYY'),
LS_DB_SAFETY_MASTER.DER_INITIAL_APPROVAL_DATE_YYYYMM=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_initial_case_approval_date, 'YYYYMM'),
LS_DB_SAFETY_MASTER.DER_INITIAL_APPROVAL_DATE_YYYYMMDD=TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_initial_case_approval_date, 'YYYYMMDD'),
LS_DB_SAFETY_MASTER.DER_INITIAL_APPROVAL_DATE_YYYYQ=CONCAT(TO_CHAR(LS_DB_SAFETY_MASTER_TMP.msg_initial_case_approval_date, 'YYYY'),'Q',QUARTER(LS_DB_SAFETY_MASTER_TMP.msg_initial_case_approval_date))
FROM (	
SELECT
	DISTINCT A.msg_ARI_REC_ID As ARI_REC_ID,
	A.msg_initial_case_approval_date,
	A.integration_id,
	A.expiry_date
FROM
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER A  where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
	--and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.msg_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	-- AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
    and LS_DB_SAFETY_MASTER.integration_id=LS_DB_SAFETY_MASTER_TMP.integration_id
    and LS_DB_SAFETY_MASTER.expiry_date=LS_DB_SAFETY_MASTER_TMP.expiry_date
	and LS_DB_SAFETY_MASTER.DER_INITIAL_APPROVAL_DATE_YYYY is null;
    	


















UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_LATEST_RECEIVED_DATE=LS_DB_SAFETY_MASTER_TMP.DER_LATEST_RECEIVED_DATE,
LS_DB_SAFETY_MASTER.DER_CASE_NO_VERSION=LS_DB_SAFETY_MASTER_TMP.DER_CASE_NO_VERSION,
LS_DB_SAFETY_MASTER.DER_DELAY_IMP_DURATION=LS_DB_SAFETY_MASTER_TMP.DER_DELAY_IMP_DURATION,
LS_DB_SAFETY_MASTER.DER_DELAY_SND_DURATION=LS_DB_SAFETY_MASTER_TMP.DER_DELAY_SND_DURATION

FROM (
select distinct LS_DB_SAFETY_MASTER.saftyrpt_ari_rec_id,
	case  when  saftyrpt_RECEIPT_DATE_FMT is null  then to_char(saftyrpt_RECEIPT_DATE,'DD-MON-YYYY')
	  when  saftyrpt_RECEIPT_DATE_FMT in (0,102)  then to_char(saftyrpt_RECEIPT_DATE,'DD-MON-YYYY')
	  when  saftyrpt_RECEIPT_DATE_FMT in (1,602)  then to_char(saftyrpt_RECEIPT_DATE,'YYYY')
	  when  saftyrpt_RECEIPT_DATE_FMT in (2,610)  then to_char(saftyrpt_RECEIPT_DATE,'MON-YYYY')
	  when  saftyrpt_RECEIPT_DATE_FMT in (3,4,5,6,7,8,9,204,203)  then to_char(saftyrpt_RECEIPT_DATE,'DD-MON-YYYY')
	end DER_LATEST_RECEIVED_DATE,
	CASE WHEN AERINFO_AER_VERSION_NO IS NULL OR AERINFO_AER_VERSION_NO = -1
		THEN  COALESCE(AERINFO_AER_NO, AERINFO_MAPPED_AER_NO) ||' ()'
		ELSE  COALESCE(AERINFO_AER_NO, AERINFO_MAPPED_AER_NO)||' ('||AERINFO_AER_VERSION_NO||')' 
	END AS DER_CASE_NO_VERSION,
	case when DATEDIFF(day, saftyrpt_COMPANY_RECEIVE_DATETIME, saftyrpt_TRANSMISSION_DATE)  is null then
		cast ( null as Integer)
		else 
		DATEDIFF(day, saftyrpt_COMPANY_RECEIVE_DATETIME, saftyrpt_TRANSMISSION_DATE) 
		end as DER_DELAY_IMP_DURATION
	,case 
			when saftyrpt_RECEIPT_DATE_FMT in (3,4,5,6,204,102,203,611) then DATEDIFF(day, saftyrpt_COMPANY_RECEIVE_DATETIME, saftyrpt_RECEIPT_DATE)
			ELSE CAST ( NULL AS Integer)
		end DER_DELAY_SND_DURATION	
	
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER	
where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ari_rec_id	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_UPGRADED_TO_SERIOUS=LS_DB_SAFETY_MASTER_TMP.DER_CASE_UPGRADED_TO_SERIOUS,
LS_DB_SAFETY_MASTER.DER_CASE_DWNGRD_TO_NONSERIOUS=LS_DB_SAFETY_MASTER_TMP.DER_CASE_DWNGRD_TO_NONSERIOUS
FROM (
select saftyrpt_ARI_REC_ID,
	CASE  when VERSION_NO = 0 THEN 'No'
		when SERIOUSNESS_DER is null and (LAG(SERIOUSNESS_DER,1) RESPECT NULLS over ( partition by AER_NO order by VERSION_NO)) is null then  'No'
		When (LAG(SERIOUSNESS_DER,1) RESPECT NULLS over ( partition by AER_NO order by VERSION_NO) is null or LAG(SERIOUSNESS_DER,1) RESPECT NULLS over ( partition by AER_NO order by VERSION_NO) = '02') AND SERIOUSNESS_DER = '01' THEN 'Yes'
		else 'No'
	END DER_CASE_UPGRADED_TO_SERIOUS,
	CASE  when VERSION_NO = 0 THEN 'No'
		when SERIOUSNESS_DER is null and (LAG(SERIOUSNESS_DER,1) RESPECT NULLS over ( partition by AER_NO order by VERSION_NO)) is null then  'No'
		When LAG(SERIOUSNESS_DER,1) RESPECT NULLS over ( partition by AER_NO order by VERSION_NO) = '01' AND (SERIOUSNESS_DER is null or SERIOUSNESS_DER = '02') THEN 'Yes'
		else 'No'
	END DER_CASE_DWNGRD_TO_NONSERIOUS
from 	

(
	SELECT
		saftyrpt_ARI_REC_ID ,
		saftyrpt_SERIOUS AS SERIOUSNESS,
		saftyrpt_SERIOUSNESS_COMPANY AS SERIOUSNESS_COMPANY,
		COALESCE(AERINFO_AER_NO, AERINFO_MAPPED_AER_NO)  AS AER_NO,
		COALESCE(AERINFO_AER_VERSION_NO, - 1) AS VERSION_NO,
		case when  (SELECT DEFAULT_VALUE_CHAR  	FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   )='REPORTER'  then saftyrpt_SERIOUS
	   when  (SELECT DEFAULT_VALUE_CHAR  	FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   )= 'COMPANY'   then  saftyrpt_SERIOUSNESS_COMPANY
	   when (SELECT DEFAULT_VALUE_CHAR  	FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   ) in  ('COMPANY OR REPORTER' , 'REPORTER OR COMPANY')  then
				case    when    saftyrpt_SERIOUS='1' or	saftyrpt_SERIOUSNESS_COMPANY='1'	then	'01'
						when	saftyrpt_SERIOUS='2' or	saftyrpt_SERIOUSNESS_COMPANY='2'	then	'02'
						when	saftyrpt_SERIOUS is null and	saftyrpt_SERIOUSNESS_COMPANY is null	then	'02'
				end
	end SERIOUSNESS_DER
		
		
	FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER	
	where case_no in (select  case_no from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') 
)final_data
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

  








      


      
      


-- ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER ADD COLUMN DER_PATIENT_AGE_IN_YEARS NUMBER(38,0);
      

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_Approval_Date_YYYY=TO_CHAR(msg_aer_approval_date, 'YYYY')
FROM (	
SELECT
	DISTINCT A.msg_ARI_REC_ID As ARI_REC_ID,
	 TO_CHAR(msg_aer_approval_date, 'YYYY') AS DER_Approval_Date_YYYY
FROM
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER A  where msg_ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.msg_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
      
  
  

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress') ;	




 RETURN 'LS_DB_SAFETY_MASTER_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	
