CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CMQ_SMQ()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_CMQ_SMQ';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CMQ_SMQ';



insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CMQ_SMQ' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CMQ_SMQ','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CMQ_SMQ'),
	'LSRA','Case','LS_DB_CMQ_SMQ',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CMQ_SMQ' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CMQ_SMQ'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CMQ_SMQ'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  As 
/*
select RECORD_ID,'LSMV_CC_SMQ_CMQ_LIST' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL 
*/
select RECORD_ID,'LSMV_CC_SMQ_CMQ_LIST_C' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST_C WHERE CDC_OPERATION_TYPE IN ('D')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP  AS 
WITH LSMV_CASE_NO_SUBSET as
 (
 
/*
   select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CMQ_SMQ' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CMQ_SMQ' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION ALL
*/
select DISTINCT RECORD_ID record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST_C WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CMQ_SMQ' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CMQ_SMQ' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 ,LSDB_CMQ_SMQ_W_SUBSET AS
(
SELECT DISTINCT 
  	BK_CMQ_SMQ_WID,
	COALESCE(SMQ_CODE,-1) SMQ_CODE,
	SPR_ID
FROM
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ

),LSMV_CC_SMQ_CMQ_LIST_SUBSET AS 
(
/*
  select * from 
    (SELECT RECORD_ID,
	DATE_CREATED,
	DATE_MODIFIED, 
	SMQ_CODE,
	SMQ_NAME,
	SMQ_NAME_J,
	SMQ_LEVEL,
	MEDDRA_VERSION,
	STATUS,
	LIST_SOURCE,
	TERM_FROM,
	USER_ID,
	PRIVATE_QUERY_FLAG,
	SMQ_ALGORITHM,
	LANGUAGE_CODE,
	SMQ_DESCRIPTION,
	SMQ_DESCRIPTION_J,
	SMQ_NOTE,
	SMQ_SOURCE,
	CASE WHEN COALESCE(LSMV_CC_SMQ_CMQ_LIST.SPR_ID,'')='' then '-9999' else LSMV_CC_SMQ_CMQ_LIST.SPR_ID END AS SPR_ID
    ,RANK() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM
	${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET)
	and RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST')
) where REC_RANK=1 )
UNION
  
*/  
select * from 
    (
SELECT 	RECORD_ID,DATE_CREATED,
	DATE_MODIFIED,
	SMQ_CODE,
	SMQ_NAME,
	SMQ_NAME_J,
	SMQ_LEVEL,
	MEDDRA_VERSION,
	STATUS,
	LIST_SOURCE,
	TERM_FROM,
	USER_ID,
	PRIVATE_QUERY_FLAG,
	SMQ_ALGORITHM,
	LANGUAGE_CODE,
	SMQ_DESCRIPTION,
	SMQ_DESCRIPTION_J,
	SMQ_NOTE,
	SMQ_SOURCE,
	CASE WHEN COALESCE(LSMV_CC_SMQ_CMQ_LIST_C.SPR_ID,'')='' then '-9999' else LSMV_CC_SMQ_CMQ_LIST_C.SPR_ID END AS SPR_ID
      ,RANK() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM
	${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST_C wHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET)
	and RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST_C')
) where REC_RANK=1 )

SELECT COALESCE(BK_CMQ_SMQ_WID,'-1') AS BK_CMQ_SMQ_WID
,to_date(GREATEST(NVL(f.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,CURRENT_TIMESTAMP LOAD_TS,
f.RECORD_ID,f.DATE_CREATED,
	f.DATE_MODIFIED,
	f.SMQ_CODE,
	f.SMQ_NAME,
	f.SMQ_LEVEL,
	f.MEDDRA_VERSION,
	f.STATUS,
	f.LIST_SOURCE,
	f.LANGUAGE_CODE,
    f.SMQ_CODE || ' (Broad)'  SMQ_TERM_SCOPE ,
    f.SMQ_NAME  || ' (Broad)'  SMQ_NAME_TERM_SCOPE,
    f.smq_source ,
    f.term_from  ,
    f.user_id    ,
    f.private_query_flag ,
    f.smq_algorithm    ,
    f.smq_name_j       ,
    f.smq_description  ,
    f.smq_description_j,
    f.smq_note,
	f.SPR_ID,
	CONCAT( NVL(f.RECORD_ID,-1), '||',f.LANGUAGE_CODE) INTEGRATION_ID
FROM
	(
	SELECT
		e.*,
		ROW_NUMBER() OVER(PARTITION BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE
						ORDER BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE,SPR_ID) RNK
	FROM
		
		(SELECT 	LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'001' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME IS NOT NULL
		
			
		UNION ALL
		SELECT LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME_J AS SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'008' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME_J IS NOT NULL
						
		)e
	)f
WHERE
	f.RNK = 1
UNION ALL
SELECT COALESCE(BK_CMQ_SMQ_WID,'-1') AS BK_CMQ_SMQ_WID
,to_date(GREATEST(NVL(f.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,CURRENT_TIMESTAMP LOAD_TS,f.RECORD_ID,f.DATE_CREATED,
	f.DATE_MODIFIED,
	f.SMQ_CODE,
	f.SMQ_NAME,
	f.SMQ_LEVEL,
	f.MEDDRA_VERSION,
	f.STATUS,
	f.LIST_SOURCE,
	f.LANGUAGE_CODE,
    f.SMQ_CODE || ' (Narrow)'  SMQ_TERM_SCOPE,
    f.SMQ_NAME   || ' (Narrow)'  SMQ_NAME_TERM_SCOPE,
    f.smq_source ,
    f.term_from  ,
    f.user_id    ,
    f.private_query_flag ,
    f.smq_algorithm    ,
    f.smq_name_j       ,
    f.smq_description  ,
    f.smq_description_j,
    f.smq_note ,
	f.SPR_ID,
	CONCAT( NVL(f.RECORD_ID,-1), '||',f.LANGUAGE_CODE) INTEGRATION_ID
FROM
	(
	SELECT
		e.*,
		ROW_NUMBER() OVER(PARTITION BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE
						ORDER BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE,SPR_ID) RNK
	FROM
		(
		SELECT LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'001' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME IS NOT NULL
			
		UNION ALL
		SELECT LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME_J AS SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'008' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME_J IS NOT NULL
			
			
		)e
	)f
WHERE
	f.RNK = 1
	
	UNION ALL
SELECT COALESCE(BK_CMQ_SMQ_WID,'-1') AS BK_CMQ_SMQ_WID
,to_date(GREATEST(NVL(f.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,CURRENT_TIMESTAMP LOAD_TS,f.RECORD_ID,f.DATE_CREATED,
	f.DATE_MODIFIED,
	f.SMQ_CODE,
	f.SMQ_NAME,
	f.SMQ_LEVEL,
	f.MEDDRA_VERSION,
	f.STATUS,
	f.LIST_SOURCE,
	f.LANGUAGE_CODE,
    f.SMQ_CODE || ' (Algorithm)'  SMQ_TERM_SCOPE,
    f.SMQ_NAME   || ' (Algorithm)'  SMQ_NAME_TERM_SCOPE,
    f.smq_source ,
    f.term_from  ,
    f.user_id    ,
    f.private_query_flag ,
    f.smq_algorithm    ,
    f.smq_name_j       ,
    f.smq_description  ,
    f.smq_description_j,
    f.smq_note,
	f.SPR_ID,
	CONCAT( NVL(f.RECORD_ID,-1), '||',f.LANGUAGE_CODE) INTEGRATION_ID
 
FROM
	(
	SELECT
		e.*,
		ROW_NUMBER() OVER(PARTITION BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE
						ORDER BY SMQ_CODE, MEDDRA_VERSION, LANGUAGE_CODE,SPR_ID) RNK
	FROM
		(
		SELECT LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'001' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME IS NOT NULL
			AND a.smq_algorithm <> 'N'
			
			
		UNION ALL
		
		SELECT LSDB_CMQ_SMQ_W_SUBSET.BK_CMQ_SMQ_WID,a.RECORD_ID,a.DATE_CREATED,
	        a.DATE_MODIFIED,
			a.SMQ_CODE,
			a.SMQ_NAME_J AS SMQ_NAME,
			a.SMQ_LEVEL,
			a.MEDDRA_VERSION,
			a.STATUS,
			a.LIST_SOURCE,
			'008' AS LANGUAGE_CODE,
			a.smq_source ,
            a.term_from  ,
            a.user_id    ,
            a.private_query_flag ,
            a.smq_algorithm    ,
            a.smq_name_j       ,
            a.smq_description  ,
            a.smq_description_j,
            a.smq_note ,
			a.SPR_ID
			
		FROM
			LSMV_CC_SMQ_CMQ_LIST_SUBSET a LEFT JOIN LSDB_CMQ_SMQ_W_SUBSET ON
	COALESCE(a.SMQ_CODE,-1)=COALESCE(LSDB_CMQ_SMQ_W_SUBSET.SMQ_CODE,-1)
	and a.SPR_ID=LSDB_CMQ_SMQ_W_SUBSET.SPR_ID
		WHERE
			a.SMQ_NAME_J IS NOT NULL
			AND a.smq_algorithm <> 'N'
				
		)e
	)f
WHERE
	f.RNK = 1
;

-- truncate table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TAB_CMQ_SMQ_W

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TAB_CMQ_SMQ_W(SMQ_CODE, SPR_ID)
SELECT
  DISTINCT 
   COALESCE(SMQ_CODE,-1) SMQ_CODE,
   COALESCE(SPR_ID,'-9999') SPR_ID
 FROM 
 ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP
 WHERE
  BK_CMQ_SMQ_WID=-1;
 
 
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
SET BK_CMQ_SMQ_WID= A.BK_CMQ_SMQ_WID
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TAB_CMQ_SMQ_W A
WHERE
   COALESCE(LS_DB_CMQ_SMQ_TMP.SMQ_CODE,-1)= A.SMQ_CODE 
    AND COALESCE(LS_DB_CMQ_SMQ_TMP.SPR_ID,'-9999')= A.SPR_ID 
	AND LS_DB_CMQ_SMQ_TMP.BK_CMQ_SMQ_WID=-1;

					
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP ADD COLUMN LANGUAGE_WID BIGINT;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
SET LANGUAGE_WID= A.SK_LANGUAGE_WID
FROM (SELECT 
		SK_LANGUAGE_WID, 
		COALESCE(CODE, '-1') LANGUAGE_CODE, 
		COALESCE(SPR_ID,'-9999') SPR_ID
		FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE WHERE EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	) A
WHERE
   COALESCE(LS_DB_CMQ_SMQ_TMP.LANGUAGE_CODE,-1)= A.LANGUAGE_CODE 
    AND COALESCE(LS_DB_CMQ_SMQ_TMP.SPR_ID,'-9999')= A.SPR_ID ;





UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP)
where target_table_name='LS_DB_CMQ_SMQ'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CMQ_SMQ'
					and LOAD_STATUS = 'In Progress') 
; 




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ   
SET 
 LS_DB_CMQ_SMQ.PROCESSING_DT  		=LS_DB_CMQ_SMQ_TMP.PROCESSING_DT
,LS_DB_CMQ_SMQ.EXPIRY_DATE    		=LS_DB_CMQ_SMQ_TMP.EXPIRY_DATE     
,LS_DB_CMQ_SMQ.LOAD_TS        		=LS_DB_CMQ_SMQ_TMP.LOAD_TS
,LS_DB_CMQ_SMQ.INTEGRATION_ID 		=LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID
,LS_DB_CMQ_SMQ.BK_CMQ_SMQ_WID 		=LS_DB_CMQ_SMQ_TMP.BK_CMQ_SMQ_WID 
,LS_DB_CMQ_SMQ.SMQ_CODE       		=LS_DB_CMQ_SMQ_TMP.SMQ_CODE
,LS_DB_CMQ_SMQ.SMQ_NAME       		=LS_DB_CMQ_SMQ_TMP.SMQ_NAME
,LS_DB_CMQ_SMQ.SMQ_LEVEL      		=LS_DB_CMQ_SMQ_TMP.SMQ_LEVEL
,LS_DB_CMQ_SMQ.MEDDRA_VERSION 		=LS_DB_CMQ_SMQ_TMP.MEDDRA_VERSION
,LS_DB_CMQ_SMQ.STATUS         		=LS_DB_CMQ_SMQ_TMP.STATUS
,LS_DB_CMQ_SMQ.LIST_SOURCE    		=LS_DB_CMQ_SMQ_TMP.LIST_SOURCE
,LS_DB_CMQ_SMQ.LANGUAGE_CODE  		=LS_DB_CMQ_SMQ_TMP.LANGUAGE_CODE
,LS_DB_CMQ_SMQ.SMQ_TERM_SCOPE       =LS_DB_CMQ_SMQ_TMP.SMQ_TERM_SCOPE  
,LS_DB_CMQ_SMQ.SMQ_NAME_TERM_SCOPE  =LS_DB_CMQ_SMQ_TMP.SMQ_NAME_TERM_SCOPE
,LS_DB_CMQ_SMQ.SMQ_SOURCE           =LS_DB_CMQ_SMQ_TMP.SMQ_SOURCE
,LS_DB_CMQ_SMQ.TERM_FROM            =LS_DB_CMQ_SMQ_TMP.TERM_FROM
,LS_DB_CMQ_SMQ.USER_ID              =LS_DB_CMQ_SMQ_TMP.USER_ID
,LS_DB_CMQ_SMQ.PRIVATE_QUERY_FLAG   =LS_DB_CMQ_SMQ_TMP.PRIVATE_QUERY_FLAG
,LS_DB_CMQ_SMQ.SMQ_ALGORITHM        =LS_DB_CMQ_SMQ_TMP.SMQ_ALGORITHM
,LS_DB_CMQ_SMQ.SMQ_NAME_J           =LS_DB_CMQ_SMQ_TMP.SMQ_NAME_J
,LS_DB_CMQ_SMQ.SMQ_DESCRIPTION      =LS_DB_CMQ_SMQ_TMP.SMQ_DESCRIPTION
,LS_DB_CMQ_SMQ.SMQ_DESCRIPTION_J    =LS_DB_CMQ_SMQ_TMP.SMQ_DESCRIPTION_J
,LS_DB_CMQ_SMQ.SMQ_NOTE             =LS_DB_CMQ_SMQ_TMP.SMQ_NOTE
,LS_DB_CMQ_SMQ.DATE_MODIFIED        =LS_DB_CMQ_SMQ_TMP.DATE_MODIFIED
,LS_DB_CMQ_SMQ.SPR_ID               =LS_DB_CMQ_SMQ_TMP.SPR_ID 
,LS_DB_CMQ_SMQ.record_id            =LS_DB_CMQ_SMQ_TMP.record_id 
,LS_DB_CMQ_SMQ.LANGUAGE_WID            =LS_DB_CMQ_SMQ_TMP.LANGUAGE_WID 
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
WHERE 	LS_DB_CMQ_SMQ.INTEGRATION_ID = LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CMQ_SMQ_TMP.PROCESSING_DT = LS_DB_CMQ_SMQ.PROCESSING_DT,1=1);





           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CMQ_SMQ.RECORD_ID ,LS_DB_CMQ_SMQ.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
ON LS_DB_CMQ_SMQ.RECORD_ID=LS_DB_CMQ_SMQ_TMP.RECORD_ID
AND LS_DB_CMQ_SMQ.INTEGRATION_ID = LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID 
where LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID  is null AND LS_DB_CMQ_SMQ.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CMQ_SMQ.RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP )
) TMP where LS_DB_CMQ_SMQ.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CMQ_SMQ.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CMQ_SMQ.RECORD_ID ,LS_DB_CMQ_SMQ.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
    ON LS_DB_CMQ_SMQ.RECORD_ID=LS_DB_CMQ_SMQ_TMP.RECORD_ID
    AND LS_DB_CMQ_SMQ.INTEGRATION_ID = LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID 
    where LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID  is null AND LS_DB_CMQ_SMQ.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CMQ_SMQ.RECORD_ID in (select RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ
( 
PROCESSING_DT
    ,EXPIRY_DATE
    ,LOAD_TS
    ,INTEGRATION_ID
    ,BK_CMQ_SMQ_WID 
    ,SMQ_CODE
    ,SMQ_NAME
    ,SMQ_LEVEL
    ,MEDDRA_VERSION
    ,STATUS
    ,LIST_SOURCE
    ,LANGUAGE_CODE
    ,SMQ_TERM_SCOPE  
    ,SMQ_NAME_TERM_SCOPE
    ,SMQ_SOURCE
    ,TERM_FROM
    ,USER_ID
    ,PRIVATE_QUERY_FLAG
    ,SMQ_ALGORITHM
    ,SMQ_NAME_J
    ,SMQ_DESCRIPTION
    ,SMQ_DESCRIPTION_J
    ,SMQ_NOTE
    ,DATE_MODIFIED
    ,SPR_ID
    ,record_id 
	,LANGUAGE_WID
	)
SELECT 
 
PROCESSING_DT
    ,EXPIRY_DATE
    ,LOAD_TS
    ,INTEGRATION_ID
    ,BK_CMQ_SMQ_WID 
    ,SMQ_CODE
    ,SMQ_NAME
    ,SMQ_LEVEL
    ,MEDDRA_VERSION
    ,STATUS
    ,LIST_SOURCE
    ,LANGUAGE_CODE
    ,SMQ_TERM_SCOPE  
    ,SMQ_NAME_TERM_SCOPE
    ,SMQ_SOURCE
    ,TERM_FROM
    ,USER_ID
    ,PRIVATE_QUERY_FLAG
    ,SMQ_ALGORITHM
    ,SMQ_NAME_J
    ,SMQ_DESCRIPTION
    ,SMQ_DESCRIPTION_J
    ,SMQ_NOTE
    ,DATE_MODIFIED
    ,SPR_ID
	,record_id
	,LANGUAGE_WID
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP 
WHERE 	TO_DATE(LS_DB_CMQ_SMQ.PROCESSING_DT) < TO_DATE(LS_DB_CMQ_SMQ_TMP.PROCESSING_DT)
AND LS_DB_CMQ_SMQ.INTEGRATION_ID = LS_DB_CMQ_SMQ_TMP.INTEGRATION_ID
AND LS_DB_CMQ_SMQ.RECORD_ID = LS_DB_CMQ_SMQ_TMP.RECORD_ID
AND LS_DB_CMQ_SMQ.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CMQ_SMQ.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CMQ_SMQ_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST') 
OR record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST_C') 
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST') 
OR record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_CC_SMQ_CMQ_DELETION_TMP  WHERE TABLE_NAME='LSMV_CC_SMQ_CMQ_LIST_C') 
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CMQ_SMQ_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CMQ_SMQ'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CMQ_SMQ'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CMQ_SMQ Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CMQ_SMQ'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CMQ_SMQ not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;


