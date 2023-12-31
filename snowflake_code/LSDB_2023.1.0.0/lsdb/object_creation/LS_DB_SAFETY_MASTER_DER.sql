












--delete from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER';




-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SAFETY_MASTER_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();




/*
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_MISSING_CASE_VERSIONS_FLAG TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_SUSAR_DOWNGRADED TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_VERSION_DELETED_DATE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_FLT_FLAG TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_INITIAL_RECEIVED_DATE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_LAST_APPROVED TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_TYPE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CHILD_CASE_FLAG TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_CONFIRMED_BY_HP TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_SPECIAL_INTEREST_CASE_FLAG TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CAUSE_OF_DEATH TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_AUTOPSY_DETERMINED TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_LOT_NO TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_LATEST_RECEIVED_DATE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_NO_VERSION TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_UPGRADED_TO_SERIOUS TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_CASE_DWNGRD_TO_NONSERIOUS TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER ADD COLUMN DER_PRIMARY_SOURCE TEXT;



*/


-- delete from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER';
-- delete from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER';


insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_SAFETY_MASTER_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+2,'LS_DB_SAFETY_MASTER_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)
;

INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_MASTER_DER'),
	'LSRA','Case','LS_DB_SAFETY_MASTER_DER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_SAFETY_MASTER_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER)
WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC AS
select distinct AERINFO_ARI_REC_ID AS ARI_REC_ID,CASE_NO
FROM 
	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SAFETY_MASTER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	



DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_UNBLINDED;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID||'-'||AD.SEQ_PRODUCT AS UNBLINDED_REC
				from 
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
				AND   CDC_OPERATION_TYPE IN ('I','U') 
				) LSMV_DRUG 
				JOIN
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
				AND   CDC_OPERATION_TYPE IN ('I','U') 
				) AD
				ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
				AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT
				AND LSMV_DRUG.rank=1 AND AD.rank=1

;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_MISSING_CASE_VERSIONS_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_MISSING_CASE_VERSIONS_FLAG
FROM (	select
  CASE_NO,
  case  when  COUNT(distinct aerinfo_aer_version_no)<> MAX(aerinfo_aer_version_no)+1 then 'YES'  
        else  'NO'
  end   DER_MISSING_CASE_VERSIONS_FLAG
from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER 
where CASE_NO in (select  CASE_NO from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
group by CASE_NO
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.CASE_NO = LS_DB_SAFETY_MASTER_TMP.CASE_NO	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_SUSAR_DOWNGRADED=LS_DB_SAFETY_MASTER_TMP.DER_SUSAR_DOWNGRADED
FROM (	
select
DC.aerinfo_ARI_REC_ID as ARI_REC_ID,
CASE  
when aerinfo_aer_version_no = 0 THEN 'NO'
when DC.saftyrpt_susar is null and (LAG(DC.saftyrpt_susar,1) RESPECT NULLS over 
( partition by DC.CASE_NO order by aerinfo_aer_version_no)) is null then  'NO'
WHEN LAG(DC.saftyrpt_susar,1) RESPECT NULLS over ( partition by DC.CASE_NO order by DC.aerinfo_aer_version_no) = '1'
 AND (DC.saftyrpt_susar IS NULL or DC.saftyrpt_susar = '0') THEN 'YES'
else 'NO'
END DER_SUSAR_DOWNGRADED
from
 $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER DC where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
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

from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER where CASE_NO in (select  CASE_NO from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
where EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and CASE_VERSION='0'
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.CASE_NO = LS_DB_SAFETY_MASTER_TMP.CASE_NO	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
	
	

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_VERSION_DELETED_DATE=LS_DB_SAFETY_MASTER_TMP.DER_CASE_VERSION_DELETED_DATE,
LS_DB_SAFETY_MASTER.DER_CASE_FLT_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_CASE_FLT_FLAG
FROM (	
select
saftyrpt_ARI_REC_ID as ARI_REC_ID,
case when MSG_INBOUND_ARCHIVE IN ('1','16','016') then MSG_ARCHIVED_DATE else null end  As DER_CASE_VERSION_DELETED_DATE,
case when (saftyrpt_DEATH in ('01','1') or saftyrpt_lifethreatening in ('01','1')) and saftyrpt_ari_rec_id is not null then 'YES' else 'NO' end AS DER_CASE_FLT_FLAG
from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');





UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_LAST_APPROVED=LS_DB_SAFETY_MASTER_TMP.DER_CASE_LAST_APPROVED
FROM (	
SELECT
	DISTINCT A.saftyrpt_ARI_REC_ID As ARI_REC_ID,
	CASE WHEN B.CASE_NO IS NOT NULL 
and a.saftyrpt_LATEST_AER_APPROVED=1
THEN 1
 ELSE 0 
END DER_CASE_LAST_APPROVED
FROM
	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER A LEFT JOIN
	(
	SELECT
		CASE_NO,
		MAX(aerinfo_aer_version_no) aerinfo_aer_version_no
	FROM
		$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER where CASE_NO in (select  CASE_NO from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	GROUP BY
		CASE_NO
	) B
ON
	A.CASE_NO=B.CASE_NO AND
	A.aerinfo_aer_version_no=B.aerinfo_aer_version_no
	where A.CASE_NO in (select  CASE_NO from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_TYPE=LS_DB_SAFETY_MASTER_TMP.DER_CASE_TYPE
FROM (
select distinct saftyrpt_ARI_REC_ID,
	case
    	when  saftyrpt_REPORT_CLASSIFICATION_CATEGORY='2'  and   PATIENT_PREGNANT='1'     then      'DEDP case'
    	when  (saftyrpt_REPORT_CLASSIFICATION_CATEGORY LIKE '%2%' AND saftyrpt_REPORT_CLASSIFICATION_CATEGORY LIKE '%1%') then  'Invalid non AE case'
    	when  saftyrpt_REPORT_CLASSIFICATION_CATEGORY LIKE '%2%'  then  'Non AE case'
   	 	when  saftyrpt_REPORT_CLASSIFICATION_CATEGORY LIKE '%1%'  then  'Invalid AE case'
    	when  saftyrpt_REPORT_CLASSIFICATION_CATEGORY ='3'  then  'Non case'
    	else  'AE case'
	end   DER_CASE_TYPE from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER A LEFT Join 
(	
select ARI_REC_ID,PATIENT_PREGNANT,row_number() over (partition by record_id order by CDC_OPERATION_TIME desc) rank
 from $$STG_DB_NAME.$$LSDB_RPL.LSMV_PATIENT where  CDC_OPERATION_TYPE IN ('I','U') and PATIENT_PREGNANT='1' 
) B ON A.saftyrpt_ARI_REC_ID=B.ARI_REC_ID and rank=1 
where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CHILD_CASE_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_CHILD_CASE_FLAG
FROM (
select distinct A.saftyrpt_ari_rec_id,
	case when b.ARI_REC_ID is null then 'NO' ELSE 'YES' end DER_CHILD_CASE_FLAG 
	from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER A left join (	
select ARI_REC_ID,PATAGEGROUP,row_number() over (partition by ARI_REC_ID order by CDC_OPERATION_TIME desc) rank
 from $$STG_DB_NAME.$$LSDB_RPL.LSMV_PATIENT where  CDC_OPERATION_TYPE IN ('I','U') and PATAGEGROUP IN ('1','2','3')
) B ON A.saftyrpt_ARI_REC_ID=B.ARI_REC_ID and rank=1   
  where   saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CASE_CONFIRMED_BY_HP=LS_DB_SAFETY_MASTER_TMP.DER_CASE_CONFIRMED_BY_HP
FROM (
select distinct A.saftyrpt_ari_rec_id,
	case when b.ARI_REC_ID is null then 'NO' ELSE 'YES' end DER_CASE_CONFIRMED_BY_HP 
from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER A left join 
(	
select ARI_REC_ID,IS_HEALTH_PROF,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
 from $$STG_DB_NAME.$$LSDB_RPL.LSMV_PRIMARYSOURCE  where  CDC_OPERATION_TYPE IN ('I','U') and  IS_HEALTH_PROF='1'
) B ON A.saftyrpt_ARI_REC_ID=B.ARI_REC_ID  and rank=1
where  saftyrpt_ARI_REC_ID in (select distinct  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_LOT_NO=LS_DB_SAFETY_MASTER_TMP.DER_LOT_NO
FROM (
SELECT ARI_REC_ID,SEQ_PRODUCT,LISTAGG(LOT_NO,' | ')within group (order by SEQ_PRODUCT) DER_LOT_NO
FROM 
(
SELECT P.ARI_REC_ID,P.SEQ_PRODUCT,P.LOT_NO
  FROM
	
	(select 
		LSMV_DRUG.ARI_REC_ID,DRUGCHARACTERIZATION,
		LSMV_DRUG.RECORD_ID ,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
 		FROM 
		$$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG where  
		ARI_REC_ID in (select distinct  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
		and CDC_OPERATION_TYPE IN ('I','U')
	)	D JOIN	
	(
	select Distinct PL.ARI_REC_ID,PL.FK_AD_REC_ID SEQ_PRODUCT
	,COALESCE (COALESCE(PL.LOT_NUMBER,PL.LOT_NUMBER_NF),A.DE) LOT_NO
	FROM
		(
		select ARI_REC_ID,FK_AD_REC_ID,LOT_NUMBER,LOT_NUMBER_NF
		,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
		from $$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG_THERAPY 
		where  ARI_REC_ID in (select distinct  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
		and CDC_OPERATION_TYPE IN ('I','U')
		)	PL LEFT JOIN
		(SELECT code CD , language_code LN, decode DE, codelist_id CD_ID,
        row_number() OVER ( PARTITION BY codelist_id,code,language_code ORDER BY to_date(GREATEST(
                           NVL(lsmv_codelist_code.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_codelist_name.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
                           NVL(lsmv_codelist_decode.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))) DESC ) REC_RANK
        FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code,
        $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode,$$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name
        WHERE lsmv_codelist_code.record_id = lsmv_codelist_decode.fk_cl_code_rec_id
        AND lsmv_codelist_code.fk_cl_name_rec_id=lsmv_codelist_name.record_id
        AND codelist_id IN ('350')  and LN='en'
		)A ON   NVL(PL.LOT_NUMBER_NF,'-')=A.CD
	)	P ON P.ARI_REC_ID = D.ARI_REC_ID 
			AND P.SEQ_PRODUCT = D.RECORD_ID 
			and D.rank=1 and D.DRUGCHARACTERIZATION IN ('1','3') 
group by 		P.ARI_REC_ID,P.SEQ_PRODUCT,P.LOT_NO
)final_output
group by ARI_REC_ID,SEQ_PRODUCT
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');






UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_SPECIAL_INTEREST_CASE_FLAG=LS_DB_SAFETY_MASTER_TMP.DER_SPECIAL_INTEREST_CASE_FLAG
FROM (
select distinct LS_DB_SAFETY_MASTER.saftyrpt_ari_rec_id,
	case when b.ARI_REC_ID is null then 'NO' ELSE 'YES' end DER_SPECIAL_INTEREST_CASE_FLAG
from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER left join 
	(
	select APAA.ARI_REC_ID from 
		(select ARI_REC_ID,	
			FK_DRUG_REC_ID AS SEQ_PRODUCT,
			FK_AR_REC_ID AS SEQ_REACT,
			ISAESI
			FROM 
			(select record_id,ARI_REC_ID,FK_DRUG_REC_ID,FK_AR_REC_ID,ISAESI, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG_REACT_RELATEDNESS 
					where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U')
			) where rank=1 and ISAESI=1
		) APAA Join 
		
		(select record_id AS SEQ_PRODUCT,ARI_REC_ID from 
			(select record_id,ARI_REC_ID,DRUGCHARACTERIZATION,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
					FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_DRUG 
					where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U')
			) where rank=1 AND DRUGCHARACTERIZATION in ('1','3')
		) APD
		ON APAA.ARI_REC_ID     =APD.ARI_REC_ID AND APAA.SEQ_PRODUCT  =APD.SEQ_PRODUCT JOIN 
		(select record_id AS SEQ_REACT,ARI_REC_ID from 
			(select record_id,ARI_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_REACTION 
				where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U') 
			) where rank=1
		) AR 
		ON APAA.ARI_REC_ID       =AR.ARI_REC_ID and APAA.SEQ_REACT    =AR.SEQ_REACT
		where  APD.ARI_REC_ID||'-'||APD.SEQ_PRODUCT not in 
		( select UNBLINDED_REC from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_UNBLINDED
		)
	) b on LS_DB_SAFETY_MASTER.saftyrpt_ari_rec_id=b.ARI_REC_ID
	where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_CAUSE_OF_DEATH=LS_DB_SAFETY_MASTER_TMP.DER_CAUSE_OF_DEATH,
LS_DB_SAFETY_MASTER.DER_AUTOPSY_DETERMINED=LS_DB_SAFETY_MASTER_TMP.DER_AUTOPSY_DETERMINED
FROM (
WITH MEDDRA_SUBSET as
(
SELECT distinct LLT_NAME,
LLT_CODE,
pt_name,
PT_CODE
FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD 
  where meddra_version in (select meddra_version from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')
),BASE_SUBSET as
(
 SELECT distinct 
     LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID ,
	LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_CODE,
	LSMV_PATIENT_AUTOPSY.PATDETAUTOPSY_PTCODE,
	LSMV_DEATH_CAUSE.PATDEATHREPORT_CODE,
	LSMV_DEATH_CAUSE.PATDEATHREPORT_PTCODE,
	LSMV_DEATH_CAUSE.RECORD_ID AS SEQ_DEATH_CAUSE,
	LSMV_PATIENT_AUTOPSY.RECORD_ID AS SEQ_AUTOPSY
	
	FROM (select record_id,ARI_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_PATIENT_MED_HIST_EPISODE 
				where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U') 
			) AS LSMV_PATIENT_MED_HIST_EPISODE 
		RIGHT JOIN (select record_id,ARI_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_PATIENT_DEATH 
				where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U') 
			) AS LSMV_PATIENT_DEATH ON LSMV_PATIENT_MED_HIST_EPISODE.ARI_REC_ID = LSMV_PATIENT_DEATH.ARI_REC_ID 
				AND LSMV_PATIENT_MED_HIST_EPISODE.rank=1 AND LSMV_PATIENT_DEATH.rank=1
        RIGHT JOIN 
			(select record_id,ARI_REC_ID,FK_APD_REC_ID,PATDEATHREPORT_CODE,PATDEATHREPORT_PTCODE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_DEATH_CAUSE 
				--where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U') 
			) AS LSMV_DEATH_CAUSE ON LSMV_PATIENT_DEATH.RECORD_ID = LSMV_DEATH_CAUSE.FK_APD_REC_ID
			AND LSMV_DEATH_CAUSE.rank=1
        RIGHT JOIN 
			(select record_id,ARI_REC_ID,FK_APD_REC_ID,PATDETAUTOPSY_CODE,PATDETAUTOPSY_PTCODE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_PATIENT_AUTOPSY 
				where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U')
  			) AS LSMV_PATIENT_AUTOPSY ON LSMV_PATIENT_DEATH.RECORD_ID = LSMV_PATIENT_AUTOPSY.FK_APD_REC_ID
			AND LSMV_PATIENT_AUTOPSY.rank=1
  ) 

select * from   (
SELECT
distinct
BASE_SUBSET.ARI_REC_ID AS ARI_REC_ID,
MEDDRA_LLT.llt_name ||' '||'('||MEDDRA_PT.pt_name||')' AS der_cause_of_death,
MEDDRA_LLT1.llt_name ||' '||'('||MEDDRA_PT1.pt_name||')' AS der_autopsy_determined --,
--BASE_SUBSET.SEQ_DEATH_CAUSE,
--BASE_SUBSET.SEQ_AUTOPSY
FROM BASE_SUBSET
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_PT
ON BASE_SUBSET.PATDEATHREPORT_PTCODE = MEDDRA_PT.pt_code
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_LLT
ON BASE_SUBSET.PATDEATHREPORT_CODE = MEDDRA_LLT.llt_code
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_PT1
ON BASE_SUBSET.PATDETAUTOPSY_PTCODE = MEDDRA_PT1.pt_code
LEFT OUTER JOIN MEDDRA_SUBSET MEDDRA_LLT1
ON BASE_SUBSET.PATDETAUTOPSY_CODE = MEDDRA_LLT1.llt_code
WHERE BASE_SUBSET.ARI_REC_ID NOT IN (select distinct ARI_REC_ID from 
										(select ARI_REC_ID,INBOUND_ARCHIVE,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_MESSAGE 
											where ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC) AND CDC_OPERATION_TYPE IN ('I','U') 
										) where rank=1 AND  INBOUND_ARCHIVE IN ('1','16','016'))
) where der_cause_of_death is not null or 
der_autopsy_determined is not null
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


--Derived-Primary-Source --- codelist field multi language support is there -- yet to be fixed



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_PRIMARY_SOURCE=LS_DB_SAFETY_MASTER_TMP.DER_PRIMARY_SOURCE

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
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE codelist_id IN ('346')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
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
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE codelist_id IN ('1004')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
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
                           when    upper(SOURCE)='NOT AVAILABLE TO SENDER (UNKNOWN)' and PRIMARY_SOURCE_FLAG='1' then 'Not secified by the sender'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='YES' AND upper(SOURCE)='STUDY' and (UPPER(STUDY_TYPE) in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Non-Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='YES' AND upper(SOURCE)='STUDY' and (UPPER(STUDY_TYPE) not in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='NO' AND upper(SOURCE)='CLINICAL STUDY' and (UPPER(STUDY_TYPE) in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Non-Interventional'
                           when    (SELECT UPPER(DEFAULT_VALUE_CHAR) FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER WHERE PARAMETER_NAME='E2B_PRIMARY_SOURCE')='NO' AND upper(SOURCE)='CLINICAL STUDY' and (UPPER(STUDY_TYPE) not in ('OTHER STUDIES','INDIVIDUAL PATIENT USE') and STUDY_TYPE is not null) and PRIMARY_SOURCE_FLAG='1' then 'Clinical Study Interventional'
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
        SOURCE,PRIMARY_SOURCE_FLAG,
        row_number() over(partition by ARI_REC_ID order by record_id) rnk
    from
        $$STG_DB_NAME.$$LSDB_RPL.LSMV_SOURCE
            
) LSMV_SOURCE_SUBSET  left join  TEMP_SOURCE on  CD=SOURCE
where
    rnk=1 and PRIMARY_SOURCE_FLAG='1'
) A Join 
(select  
	ARI_REC_ID,
	DE as STUDY_TYPE
from (select ARI_REC_ID,STUDY_TYPE,row_number() over(partition by ARI_REC_ID order by record_id) rnk
	FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_study) LSMV_STUDY_SUBSET
	left join  TEMP_STUDY_TYPE on  CD=STUDY_TYPE where rnk=1
) B ON A.ARI_REC_ID=B.ARI_REC_ID
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
SET LS_DB_SAFETY_MASTER.DER_LATEST_RECEIVED_DATE=LS_DB_SAFETY_MASTER_TMP.DER_LATEST_RECEIVED_DATE,
LS_DB_SAFETY_MASTER.DER_CASE_NO_VERSION=LS_DB_SAFETY_MASTER_TMP.DER_CASE_NO_VERSION

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
	END AS DER_CASE_NO_VERSION
	
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER	
where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ari_rec_id	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER   
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
		case when  (SELECT DEFAULT_VALUE_CHAR  	FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   )='REPORTER'  then saftyrpt_SERIOUS
	   when  (SELECT DEFAULT_VALUE_CHAR  	FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   )= 'COMPANY'   then  saftyrpt_SERIOUSNESS_COMPANY
	   when (SELECT DEFAULT_VALUE_CHAR  	FROM $$STG_DB_NAME.$$LSDB_RPL.ETL_COLUMN_PARAMETER 
		              WHERE PARAMETER_NAME='SERIOUSNESS'
				   ) in  ('COMPANY OR REPORTER' , 'REPORTER OR COMPANY')  then
				case    when    saftyrpt_SERIOUS='1' or	saftyrpt_SERIOUSNESS_COMPANY='1'	then	'01'
						when	saftyrpt_SERIOUS='2' or	saftyrpt_SERIOUSNESS_COMPANY='2'	then	'02'
						when	saftyrpt_SERIOUS is null and	saftyrpt_SERIOUSNESS_COMPANY is null	then	'02'
				end
	end SERIOUSNESS_DER
		
		
	FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER	
	where saftyrpt_ARI_REC_ID in (select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER_CASE_QFC)
	and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') 
)final_data
) LS_DB_SAFETY_MASTER_TMP
    WHERE LS_DB_SAFETY_MASTER.saftyrpt_ARI_REC_ID = LS_DB_SAFETY_MASTER_TMP.saftyrpt_ARI_REC_ID	
	AND LS_DB_SAFETY_MASTER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');






UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SAFETY_MASTER),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress') ;	




 RETURN 'LS_DB_SAFETY_MASTER_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SAFETY_MASTER_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

/*

 CREATE or replace TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_SAFETY_MASTER
  WAREHOUSE = EXTRASMALL
  Schedule = '15 minute'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SAFETY_MASTER();
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SAFETY_MASTER_DER();
END;
$$
;

ALTER TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_SAFETY_MASTER RESUME; 
 				
  */
