


CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_INFM_AUTH_DER()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 


/*



truncate table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER;
call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER();
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_CASE_INFM_AUTH_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_INFM_AUTH_DER';


-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};

*/


CURRENT_TS_VAR := CURRENT_TIMESTAMP();

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_INFM_AUTH_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_INFM_AUTH_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_INFM_AUTH_DER'),
	'LSRA','Case','LS_DB_CASE_INFM_AUTH_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_INFM_AUTH_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_INFM_AUTH_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP  As 
select RECORD_ID,'LSMV_INFORMED_AUTHORITY' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_INFORMED_AUTHORITY WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_st_message' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
  select DISTINCT a.RECORD_ID record_id, 0 common_parent_key, b. ARI_REC_ID  
 FROM ${stage_db_name}.${stage_schema_name}.LSMV_INFORMED_AUTHORITY a JOIN (select record_id,ARI_REC_ID from ${stage_db_name}.${stage_schema_name}.lsmv_st_message) b on 
  A.FK_LSM_REC_ID=B.record_id
 WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')


UNION 
 
 select DISTINCT RECORD_ID record_id, 0 common_parent_key, b. ARI_REC_ID  
 FROM ${stage_db_name}.${stage_schema_name}.LSMV_DISTRIBUTION_FORMAT a JOIN (select DIST_FORMAT_REC_ID,ARI_REC_ID from ${stage_db_name}.${stage_schema_name}.lsmv_st_message) b on 
  A.RECORD_ID=B.DIST_FORMAT_REC_ID
 WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_INFM_AUTH_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
),lsmv_st_message_SUBSET as
(
select  ARI_REC_ID,DIST_FORMAT_REC_ID,record_id,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,DIST_FORMAT_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP where table_name='lsmv_st_message')
) where
REC_RANK=1
group by 1,2,3
),LSMV_INFORMED_AUTHORITY_SUBSET as
(
select  record_id,FK_LSM_REC_ID,max(date_modified) date_modified  
FROM
(
select a.record_id,a.FK_LSM_REC_ID,date_modified,row_number() OVER ( PARTITION BY a.RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM   ${stage_db_name}.${stage_schema_name}.LSMV_INFORMED_AUTHORITY a JOIN (select record_id,ARI_REC_ID from ${stage_db_name}.${stage_schema_name}.lsmv_st_message) b on 
  A.FK_LSM_REC_ID=B.record_id WHERE 
    b.ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP where table_name='LSMV_SAFETY_REPORT')
) where
REC_RANK=1
group by 1,2
)
,LSMV_DISTRIBUTION_FORMAT_SUBSET as
(
select  RECORD_ID,max(date_modified) date_modified  
FROM
(
select A.RECORD_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DISTRIBUTION_FORMAT A JOIN (select DIST_FORMAT_REC_ID,ARI_REC_ID from ${stage_db_name}.${stage_schema_name}.lsmv_st_message) B on 
  A.RECORD_ID=B.DIST_FORMAT_REC_ID WHERE B.ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP where table_name='LSMV_SAFETY_REPORT')
) where
REC_RANK=1
group by 1
)


 SELECT lsmv_st_message_SUBSET.ARI_REC_ID,
 LSMV_INFORMED_AUTHORITY_SUBSET.record_id as SEQ_INFM_AUTH,
 max(to_date(GREATEST(NVL(lsmv_st_message_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_INFORMED_AUTHORITY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DISTRIBUTION_FORMAT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(lsmv_st_message_SUBSET.ARI_REC_ID,-1),'||',NVL(LSMV_INFORMED_AUTHORITY_SUBSET.record_id,-1)) INTEGRATION_ID
,max(GREATEST(NVL(LSMV_ST_MESSAGE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_INFORMED_AUTHORITY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DISTRIBUTION_FORMAT_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)) as DATE_MODIFIED
from 	LSMV_ST_MESSAGE_SUBSET LEFT OUTER JOIN LSMV_INFORMED_AUTHORITY_SUBSET
    ON LSMV_ST_MESSAGE_SUBSET.RECORD_ID = LSMV_INFORMED_AUTHORITY_SUBSET.FK_LSM_REC_ID
    LEFT JOIN LSMV_DISTRIBUTION_FORMAT_SUBSET
    ON LSMV_ST_MESSAGE_SUBSET.DIST_FORMAT_REC_ID = LSMV_DISTRIBUTION_FORMAT_SUBSET.RECORD_ID
group by lsmv_st_message_SUBSET.ARI_REC_ID,
 LSMV_INFORMED_AUTHORITY_SUBSET.record_id
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

	

 ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_tmp ADD COLUMN DER_SUBMISSION_TYPE TEXT(100);	
 -- ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER ADD COLUMN DER_SUBMISSION_TYPE TEXT(100);	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_tmp   
SET LS_DB_CASE_INFM_AUTH_DER_tmp.DER_SUBMISSION_TYPE=LS_DB_CASE_INFM_AUTH_DER_FINAL.DER_SUBMISSION_TYPE   
FROM (
With LSMV_DISTRIBUTION_FORMAT_SUBSET as
(select * 
FROM (
		select RECORD_ID,TRUSTED_PARTNER,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DISTRIBUTION_FORMAT
		-- WHERE ARI_REC_ID in (select seq_infm_auth from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') ) ,
	
	LSMV_ST_MESSAGE_SUBSET as 
	(select * 
FROM (
		select ARI_REC_ID,RECORD_ID,DIST_FORMAT_REC_ID,format_type,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_ST_MESSAGE
		WHERE ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U')),
	
	LSMV_INFORMED_AUTHORITY_SUBSET as
	(select * 
FROM (
		select RECORD_ID,FK_LSM_REC_ID,CDC_OPERATION_TYPE
		,row_number() over(partition by record_id order by CDC_OPERATION_TIME DESC) rnk
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_INFORMED_AUTHORITY
		 WHERE record_id in (select seq_infm_auth from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_tmp)
		
	)
    WHERE rnk=1 AND CDC_OPERATION_TYPE IN ('I','U') )
SELECT  LSMV_ST_MESSAGE_SUBSET.ARI_REC_ID,
        LSMV_INFORMED_AUTHORITY_SUBSET.RECORD_ID as SEQ_INFM_AUTH,
	   CASE WHEN TRUSTED_PARTNER=1 THEN 'Direct Submission'
	        WHEN temp.decode='French Report for Clinical Studies' then 'French Report for Clinical Studies' 
			WHEN temp.decode='Other Report' THEN 'Other Report'
			WHEN temp.decode='BfArM' THEN 'BfArM'
			WHEN temp.decode='E2B' THEN 'E2B'
			WHEN temp.decode='FDA3500A (Drug)' THEN 'FDA3500A (Drug)'
			WHEN temp.decode='Austrian Clinical Study' THEN 'Austrian Clinical Study'
			WHEN temp.decode='CIOMS I' THEN 'CIOMS I'
			END AS DER_SUBMISSION_TYPE
		from 	LSMV_ST_MESSAGE_SUBSET LEFT OUTER JOIN LSMV_INFORMED_AUTHORITY_SUBSET
    ON LSMV_ST_MESSAGE_SUBSET.RECORD_ID = LSMV_INFORMED_AUTHORITY_SUBSET.FK_LSM_REC_ID
    LEFT JOIN LSMV_DISTRIBUTION_FORMAT_SUBSET
    ON LSMV_ST_MESSAGE_SUBSET.DIST_FORMAT_REC_ID = LSMV_DISTRIBUTION_FORMAT_SUBSET.RECORD_ID
	left outer join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp temp on format_type=CODE 
) LS_DB_CASE_INFM_AUTH_DER_FINAL
    WHERE LS_DB_CASE_INFM_AUTH_DER_tmp.ARI_REC_ID = LS_DB_CASE_INFM_AUTH_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_INFM_AUTH_DER_tmp.SEQ_INFM_AUTH= LS_DB_CASE_INFM_AUTH_DER_FINAL.SEQ_INFM_AUTH	
	AND LS_DB_CASE_INFM_AUTH_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP)
where target_table_name='LS_DB_CASE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                    where target_table_name='LS_DB_CASE_INFM_AUTH_DER'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER   
SET LS_DB_CASE_INFM_AUTH_DER.ARI_REC_ID = LS_DB_CASE_INFM_AUTH_DER_TMP.ARI_REC_ID,
LS_DB_CASE_INFM_AUTH_DER.SEQ_INFM_AUTH = LS_DB_CASE_INFM_AUTH_DER_TMP.SEQ_INFM_AUTH,
LS_DB_CASE_INFM_AUTH_DER.PROCESSING_DT = LS_DB_CASE_INFM_AUTH_DER_TMP.PROCESSING_DT,
LS_DB_CASE_INFM_AUTH_DER.expiry_date    =LS_DB_CASE_INFM_AUTH_DER_TMP.expiry_date,
LS_DB_CASE_INFM_AUTH_DER.date_modified    =LS_DB_CASE_INFM_AUTH_DER_TMP.date_modified,
LS_DB_CASE_INFM_AUTH_DER.load_ts    =LS_DB_CASE_INFM_AUTH_DER_TMP.load_ts,
LS_DB_CASE_INFM_AUTH_DER.DER_SUBMISSION_TYPE     =LS_DB_CASE_INFM_AUTH_DER_TMP.DER_SUBMISSION_TYPE
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP 
WHERE 	LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID = LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_INFM_AUTH_DER_TMP.PROCESSING_DT = LS_DB_CASE_INFM_AUTH_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_INFM_AUTH_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_INFM_AUTH_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_INFM_AUTH_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_INFM_AUTH_DER.ari_rec_id ,LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP 
ON LS_DB_CASE_INFM_AUTH_DER.ari_rec_id=LS_DB_CASE_INFM_AUTH_DER_TMP.ari_rec_id
AND LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID = LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_INFM_AUTH_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_INFM_AUTH_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP )
) TMP where LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_INFM_AUTH_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_INFM_AUTH_DER.ari_rec_id ,LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP 
    ON LS_DB_CASE_INFM_AUTH_DER.ari_rec_id=LS_DB_CASE_INFM_AUTH_DER_TMP.ari_rec_id
    AND LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID = LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_INFM_AUTH_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_INFM_AUTH_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER
( ARI_REC_ID    ,
SEQ_INFM_AUTH       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_SUBMISSION_TYPE
)
SELECT 
  ARI_REC_ID    ,
SEQ_INFM_AUTH       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_SUBMISSION_TYPE                              
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER TGT
WHERE  ( SEQ_INFM_AUTH  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP  WHERE TABLE_NAME='LSMV_INFORMED_AUTHORITY') )
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_INFM_AUTH_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_INFM_AUTH_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_INFM_AUTH_DER.INTEGRATION_ID = LS_DB_CASE_INFM_AUTH_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_INFM_AUTH_DER.SEQ_INFM_AUTH = LS_DB_CASE_INFM_AUTH_DER_TMP.SEQ_INFM_AUTH
AND LS_DB_CASE_INFM_AUTH_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
 AND MD5(NVL(LS_DB_CASE_INFM_AUTH_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_INFM_AUTH_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( SEQ_INFM_AUTH  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_DELETION_TMP  
           WHERE TABLE_NAME='LSMV_INFORMED_AUTHORITY')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_INFM_AUTH_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_INFM_AUTH_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_INFM_AUTH_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
