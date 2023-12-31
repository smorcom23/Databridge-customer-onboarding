-- USE SCHEMA $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.PRC_LS_DB_MEDDRA_ICD()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();


insert into $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_MEDDRA_ICD' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG)+2,'LS_DB_MEDDRA_ICD','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG)
;



INSERT INTO $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MEDDRA_ICD'),
	'LSRA','Case','LS_DB_MEDDRA_ICD',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;



UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_MEDDRA_ICD'
                       AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_MEDDRA_ICD'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_MEDDRA_ICD'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;




drop table if exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP;
CREATE TEMPORARY TABLE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP As 
select RECORD_ID,'LSMV_LOW_LEVEL_TERM' AS TABLE_NAME 
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_LOW_LEVEL_TERM WHERE CDC_TRANSACTION_ID IN ('D')
UNION ALL
select RECORD_ID,'LSMV_MD_HIERARCHY' AS TABLE_NAME 
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_MD_HIERARCHY WHERE CDC_TRANSACTION_ID IN ('D')
 ;



DROP TABLE if exists  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP;

CREATE  TABLE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP AS
WITH 
LSMV_CASE_NO_SUBSET as
 (
select DISTINCT RECORD_ID record_id, 'LSMV_LOW_LEVEL_TERM' AS TABLE_NAME,CAST(PT_CODE AS VARCHAR(250)) AS PT_CODE FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_LOW_LEVEL_TERM
	WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MEDDRA_ICD' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MEDDRA_ICD' AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
UNION   
select DISTINCT RECORD_ID record_id, 'LSMV_MD_HIERARCHY' AS TABLE_NAME,CAST(PT_CODE AS VARCHAR(250)) AS PT_CODE  FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_MD_HIERARCHY WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MEDDRA_ICD' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_MEDDRA_ICD' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
),LSMV_LOW_LEVEL_TERM_SUBSET AS (
select * from (
SELECT 
DATE_CREATED     
,DATE_MODIFIED   
,CAST(LLT_CODE AS VARCHAR(250))   AS LLT_CODE     
,LLT_COSTART_SYM 
,LLT_CURRENCY    
,LLT_HARTS_CODE  
,LLT_ICD10_CODE  
,LLT_ICD9_CODE   
,LLT_ICD9CM_CODE 
,LLT_JART_CODE   
,LLT_JCURR       
,LLT_KANJI       
,LLT_NAME        
,CASE WHEN 'YES' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='CHN') THEN LLT_NAME_CN ELSE CAST(NULL AS VARCHAR(500)) END AS LLT_NAME_CN
,LLT_WHOART_CODE 
,replace(MEDDRA_VERSION,'v.','') AS MEDDRA_VERSION
,CAST(PT_CODE AS VARCHAR(250)) AS PT_CODE       
,RECORD_ID       
,SPR_ID          
,USER_CREATED    
,USER_MODIFIED
,RANK() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_LOW_LEVEL_TERM 
WHERE( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET where TABLE_NAME='LSMV_LOW_LEVEL_TERM') 
   or CAST(PT_CODE AS VARCHAR(250))  in (SELECT PT_CODE FROM LSMV_CASE_NO_SUBSET)
   )AND 
  RECORD_ID not in (select RECORD_ID from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP where TABLE_NAME='LSMV_LOW_LEVEL_TERM')

)where REC_RANK=1 ) ,
LSMV_MD_HIERARCHY_SUBSET AS
(
select * from (
SELECT 
DATE_CREATED   
,DATE_MODIFIED 
,HLGT_CODE     
,HLGT_NAME     
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN hlgt_name_cn ELSE CAST(NULL AS VARCHAR(500)) END AS hlgt_name_cn
,HLT_CODE 
,HLT_NAME 
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN hlt_name_cn ELSE CAST(NULL AS VARCHAR(500)) END AS hlt_name_cn
,replace(MEDDRA_VERSION,'v.','') AS mdhie_MEDDRA_VERSION
,NULL_FIELD 	
,PRIMARY_SOC_FG 
,CAST(PT_CODE AS VARCHAR(250)) AS PT_CODE  
,PT_NAME 
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN pt_name_cn ELSE CAST(NULL AS VARCHAR(500)) END AS  pt_name_cn
,PT_SOC_CODE 
,RECORD_ID   
,SOC_ABBREV  
,SOC_CODE    
,SOC_NAME    
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN soc_name_cn ELSE CAST(NULL AS VARCHAR(500)) END AS soc_name_cn
,SPR_ID 		
,USER_CREATED 	
,USER_MODIFIED  
,null as PT_NAME_OTHER
,null as SOC_NAME_OTHER 
,null as HLT_NAME_OTHER 
,null as HLGT_NAME_OTHER
,replace(MEDDRA_VERSION,'v.','') AS MEDDRA_VERSION  
,RANK() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_MD_HIERARCHY
WHERE (RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET where TABLE_NAME='LSMV_MD_HIERARCHY' )
  or CAST(PT_CODE AS VARCHAR(250)) in (SELECT PT_CODE FROM LSMV_CASE_NO_SUBSET)
  )
  AND 
  RECORD_ID not in (select RECORD_ID from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP where TABLE_NAME='LSMV_MD_HIERARCHY')


) where REC_RANK=1 ),
LSMV_HLGT_PREF_TERM_SUBSET AS
(
select 
DISTINCT HLGT_CODE
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN HLGT_NAME_CN ELSE HLGT_KANJI END   AS HLGT_NAME
,replace(MEDDRA_VERSION,'v.','') MEDDRA_VERSION
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_HLGT_PREF_TERM
),
LSMV_PREF_TERM_SUBSET AS
(SELECT DISTINCT 
PT_CODE
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN PT_NAME_CN ELSE PT_KANJI END   AS PT_NAME
,replace(MEDDRA_VERSION,'v.','') AS MEDDRA_VERSION
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_PREF_TERM)
,
LSMV_HLT_PREF_TERM_SUBSET AS
(select DISTINCT 
HLT_CODE
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN HLT_NAME_CN ELSE HLT_KANJI END AS HLT_NAME
,replace(MEDDRA_VERSION,'v.','') MEDDRA_VERSION
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_HLT_PREF_TERM
),
LSMV_SOC_TERM_SUBSET AS
( 
SELECT DISTINCT 
SOC_CODE
,CASE WHEN 'YES' = (SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE PARAM_NAME='CHN') THEN SOC_NAME_CN ELSE SOC_KANJI END   AS SOC_NAME
,replace(MEDDRA_VERSION,'v.','') MEDDRA_VERSION
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.LSMV_SOC_TERM),
LSMV_SOC_ORDER_SUBSET AS
(SELECT DISTINCT
SOC_CODE
,SOC_ORDER
,INTERNATIONAL_SOC_ORDER
FROM $$STG_CNTRL_DB_NAME.$$LSDB_RPL.ETL_SOC_ORDER
)
,LS_DB_MEDDRA_ICD_SUBSET AS
(
SELECT DISTINCT 
  	BK_MEDDRA_ICD_WID,
	COALESCE(ICD_CODE,'-1') ICD_CODE,
	COALESCE(LLT_CODE,'-1') LLT_CODE,
	COALESCE(PT_CODE,'-1') PT_CODE,
	COALESCE(SOC_CODE,-1) SOC_CODE
FROM
  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD

)


SELECT DISTINCT COALESCE(LS_DB_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,'-1') AS BK_MEDDRA_ICD_WID
,to_date(GREATEST(NVL(LSMV_LOW_LEVEL_TERM_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(LSMV_MD_HIERARCHY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,LSMV_LOW_LEVEL_TERM_SUBSET.USER_CREATED CREATED_BY
,LSMV_LOW_LEVEL_TERM_SUBSET.DATE_CREATED CREATED_DT
,CURRENT_TIMESTAMP LOAD_TS
,null ICD_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_NAME
,LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLGT_CODE
,LSMV_MD_HIERARCHY_SUBSET.SOC_CODE
,LSMV_MD_HIERARCHY_SUBSET.PT_NAME
,LSMV_MD_HIERARCHY_SUBSET.HLT_NAME
,LSMV_MD_HIERARCHY_SUBSET.HLGT_NAME
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME
,LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION
,NULL AS LLT_NAME_OTHER
,1 IS_MEDDRA_FLAG
,0 IS_ICD_FLAG
,'001' LANGUAGE_CODE 
,LSMV_SOC_ORDER_SUBSET.INTERNATIONAL_SOC_ORDER
,LSMV_SOC_ORDER_SUBSET.SOC_ORDER
,LSMV_SOC_ORDER_PRIMARY_SUBSET.PRIMARY_SOC_NAME
,LSMV_SOC_ORDER_PRIMARY_SUBSET.INTERNATIONAL_PRIMARY_ORDER
,LSMV_MD_HIERARCHY_SUBSET.HLGT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.HLT_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_SOC_CODE
,LSMV_MD_HIERARCHY_SUBSET.PRIMARY_SOC_FG
,LSMV_MD_HIERARCHY_SUBSET.RECORD_ID MDHI_RECORD_ID
,LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID  LOWLE_RECORD_ID
,CONCAT( NVL(LSMV_MD_HIERARCHY_SUBSET.RECORD_ID,-1), '||',NVL(LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID,-1),'||','001','||',COALESCE(LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION,LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION,'-1')) INTEGRATION_ID
  
FROM
LSMV_MD_HIERARCHY_SUBSET  INNER JOIN 
LSMV_LOW_LEVEL_TERM_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.PT_CODE = LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE 
	AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_SOC_ORDER_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_SUBSET.SOC_CODE 	
LEFT JOIN 
(SELECT 
DISTINCT 
  LMH.SOC_CODE,
  LMH.SOC_NAME AS PRIMARY_SOC_NAME,
  eso.international_soc_order INTERNATIONAL_PRIMARY_ORDER,
 replace(LMH.meddra_version,'v.','') MEDDRA_VERSION
FROM 
  LSMV_MD_HIERARCHY_SUBSET LMH ,
  LSMV_SOC_ORDER_SUBSET ESO
WHERE
  lmh.soc_code=eso.soc_code 
and lmh.primary_soc_fg='Y') LSMV_SOC_ORDER_PRIMARY_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_PRIMARY_SUBSET.SOC_CODE 
LEFT JOIN LS_DB_MEDDRA_ICD_SUBSET ON
	COALESCE(LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.LLT_CODE,-1)  AND
	COALESCE(LSMV_MD_HIERARCHY_SUBSET.PT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.PT_CODE,-1)


UNION ALL

SELECT DISTINCT COALESCE(LS_DB_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,'-1') AS BK_MEDDRA_ICD_WID
,to_date(GREATEST(NVL(LSMV_LOW_LEVEL_TERM_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(LSMV_MD_HIERARCHY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,LSMV_LOW_LEVEL_TERM_SUBSET.USER_CREATED CREATED_BY
,LSMV_LOW_LEVEL_TERM_SUBSET.DATE_CREATED CREATED_DT
,CURRENT_TIMESTAMP LOAD_TS
,null ICD_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_NAME
,LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLGT_CODE
,LSMV_MD_HIERARCHY_SUBSET.SOC_CODE
,LSMV_PREF_TERM_SUBSET.PT_NAME
,LSMV_HLT_PREF_TERM_SUBSET.HLT_NAME
,LSMV_HLGT_PREF_TERM_SUBSET.HLGT_NAME
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME
,LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION
,NULL AS LLT_NAME_OTHER
,1 IS_MEDDRA_FLAG
,0 IS_ICD_FLAG
,'008' LANGUAGE_CODE 
,LSMV_SOC_ORDER_SUBSET.INTERNATIONAL_SOC_ORDER
,LSMV_SOC_ORDER_SUBSET.SOC_ORDER
,LSMV_SOC_ORDER_PRIMARY_SUBSET.PRIMARY_SOC_NAME
,LSMV_SOC_ORDER_PRIMARY_SUBSET.INTERNATIONAL_PRIMARY_ORDER
,LSMV_MD_HIERARCHY_SUBSET.HLGT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.HLT_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_SOC_CODE
,LSMV_MD_HIERARCHY_SUBSET.PRIMARY_SOC_FG
,LSMV_MD_HIERARCHY_SUBSET.RECORD_ID MDHI_RECORD_ID
,LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID  LOWLE_RECORD_ID  
,CONCAT( NVL(LSMV_MD_HIERARCHY_SUBSET.RECORD_ID,-1), '||',NVL(LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID,-1),'||','008','||',COALESCE(LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION,LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION,'-1')) INTEGRATION_ID
FROM
LSMV_MD_HIERARCHY_SUBSET  INNER JOIN 
LSMV_LOW_LEVEL_TERM_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.PT_CODE = LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE 
	AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_SOC_ORDER_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_SUBSET.SOC_CODE 	
LEFT JOIN 
(SELECT 
DISTINCT 
  LMH.SOC_CODE,
  LMH.SOC_NAME AS PRIMARY_SOC_NAME,
  eso.international_soc_order INTERNATIONAL_PRIMARY_ORDER,
 replace(LMH.meddra_version,'v.','') MEDDRA_VERSION
FROM 
  LSMV_MD_HIERARCHY_SUBSET LMH ,
  LSMV_SOC_ORDER_SUBSET ESO
WHERE
  lmh.soc_code=eso.soc_code 
and lmh.primary_soc_fg='Y') LSMV_SOC_ORDER_PRIMARY_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_PRIMARY_SUBSET.SOC_CODE 
LEFT JOIN 
LSMV_HLGT_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.HLGT_CODE = LSMV_HLGT_PREF_TERM_SUBSET.HLGT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_HLGT_PREF_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.PT_CODE = LSMV_PREF_TERM_SUBSET.PT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_PREF_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_HLT_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.HLT_CODE = LSMV_HLT_PREF_TERM_SUBSET.HLT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_HLT_PREF_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_SOC_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_TERM_SUBSET.SOC_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_SOC_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN LS_DB_MEDDRA_ICD_SUBSET ON
	COALESCE(LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.LLT_CODE,-1) AND
	COALESCE(LSMV_MD_HIERARCHY_SUBSET.PT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.PT_CODE,-1)

WHERE LSMV_LOW_LEVEL_TERM_SUBSET.LLT_KANJI is not null

UNION ALL

SELECT DISTINCT  COALESCE(LS_DB_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,'-1') AS BK_MEDDRA_ICD_WID
,to_date(GREATEST(NVL(LSMV_LOW_LEVEL_TERM_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(LSMV_MD_HIERARCHY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))))PROCESSING_DT
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE
,LSMV_LOW_LEVEL_TERM_SUBSET.USER_CREATED CREATED_BY
,LSMV_LOW_LEVEL_TERM_SUBSET.DATE_CREATED CREATED_DT
,CURRENT_TIMESTAMP LOAD_TS
,null ICD_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE
,LSMV_LOW_LEVEL_TERM_SUBSET.LLT_NAME_CN AS LLT_NAME
,LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLT_CODE
,LSMV_MD_HIERARCHY_SUBSET.HLGT_CODE
,LSMV_MD_HIERARCHY_SUBSET.SOC_CODE
,LSMV_PREF_TERM_SUBSET.PT_NAME
,LSMV_HLT_PREF_TERM_SUBSET.HLT_NAME
,LSMV_HLGT_PREF_TERM_SUBSET.HLGT_NAME
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME
,LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION
,NULL AS LLT_NAME_OTHER
,1 IS_MEDDRA_FLAG
,0 IS_ICD_FLAG
,'009' LANGUAGE_CODE 
,LSMV_SOC_ORDER_SUBSET.INTERNATIONAL_SOC_ORDER
,LSMV_SOC_ORDER_SUBSET.SOC_ORDER
,LSMV_SOC_ORDER_PRIMARY_SUBSET.PRIMARY_SOC_NAME
,LSMV_SOC_ORDER_PRIMARY_SUBSET.INTERNATIONAL_PRIMARY_ORDER
,LSMV_MD_HIERARCHY_SUBSET.HLGT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.HLT_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_NAME_OTHER
,LSMV_MD_HIERARCHY_SUBSET.SOC_NAME_OTHER 
,LSMV_MD_HIERARCHY_SUBSET.PT_SOC_CODE
,LSMV_MD_HIERARCHY_SUBSET.PRIMARY_SOC_FG
,LSMV_MD_HIERARCHY_SUBSET.RECORD_ID MDHI_RECORD_ID
,LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID  LOWLE_RECORD_ID  
,CONCAT( NVL(LSMV_MD_HIERARCHY_SUBSET.RECORD_ID,-1), '||',NVL(LSMV_LOW_LEVEL_TERM_SUBSET.RECORD_ID,-1),'||','009','||',COALESCE(LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION,LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION,'-1')) INTEGRATION_ID
FROM
LSMV_MD_HIERARCHY_SUBSET  INNER JOIN 
LSMV_LOW_LEVEL_TERM_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.PT_CODE = LSMV_LOW_LEVEL_TERM_SUBSET.PT_CODE 
	AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_LOW_LEVEL_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_SOC_ORDER_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_SUBSET.SOC_CODE 	
LEFT JOIN 
(SELECT 
DISTINCT 
  LMH.SOC_CODE,
  LMH.SOC_NAME AS PRIMARY_SOC_NAME,
  eso.international_soc_order INTERNATIONAL_PRIMARY_ORDER,
 replace(LMH.meddra_version,'v.','') MEDDRA_VERSION
FROM 
  LSMV_MD_HIERARCHY_SUBSET LMH ,
  LSMV_SOC_ORDER_SUBSET ESO
WHERE
  lmh.soc_code=eso.soc_code 
and lmh.primary_soc_fg='Y') LSMV_SOC_ORDER_PRIMARY_SUBSET 
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_ORDER_PRIMARY_SUBSET.SOC_CODE 
LEFT JOIN 
LSMV_HLGT_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.HLGT_CODE = LSMV_HLGT_PREF_TERM_SUBSET.HLGT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_SOC_ORDER_PRIMARY_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.PT_CODE = LSMV_PREF_TERM_SUBSET.PT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_PREF_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_HLT_PREF_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.HLT_CODE = LSMV_HLT_PREF_TERM_SUBSET.HLT_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_HLT_PREF_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN 
LSMV_SOC_TERM_SUBSET
ON    LSMV_MD_HIERARCHY_SUBSET.SOC_CODE = LSMV_SOC_TERM_SUBSET.SOC_CODE 
AND LSMV_MD_HIERARCHY_SUBSET.MEDDRA_VERSION = LSMV_SOC_TERM_SUBSET.MEDDRA_VERSION
LEFT JOIN LS_DB_MEDDRA_ICD_SUBSET ON
	COALESCE(LSMV_LOW_LEVEL_TERM_SUBSET.LLT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.LLT_CODE,-1) AND
	COALESCE(LSMV_MD_HIERARCHY_SUBSET.PT_CODE,-1)=COALESCE(LS_DB_MEDDRA_ICD_SUBSET.PT_CODE,-1)

WHERE LSMV_LOW_LEVEL_TERM_SUBSET.LLT_NAME_CN is not null

;






UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP)
where target_table_name='LS_DB_MEDDRA_ICD'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL 
						where target_table_name='LS_DB_MEDDRA_ICD'
					and LOAD_STATUS = 'In Progress') ;


INSERT INTO $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.TAB_DMIW(PT_CODE, ICD_CODE, LLT_CODE)
SELECT
  DISTINCT
   COALESCE(PT_CODE,'-1') PT_CODE, 
   --COALESCE(SOC_CODE,-1) SOC_CODE, 
   COALESCE(ICD_CODE,'-1') ICD_CODE,
   COALESCE(LLT_CODE,'-1') LLT_CODE
 FROM 
  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP
 WHERE
  BK_MEDDRA_ICD_WID=-1;
 
 
UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP 
SET BK_MEDDRA_ICD_WID= A.BK_MEDDRA_ICD_WID
FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.TAB_DMIW A
WHERE
   COALESCE(LS_DB_MEDDRA_ICD_TMP.PT_CODE,'-1')= A.PT_CODE AND
   --COALESCE(LS_DB_MEDDRA_ICD_TMP.SOC_CODE,-1) = A.SOC_CODE AND 
   COALESCE(LS_DB_MEDDRA_ICD_TMP.ICD_CODE,'-1') = A.ICD_CODE AND
   COALESCE(LS_DB_MEDDRA_ICD_TMP.LLT_CODE,'-1') = A.LLT_CODE AND
	LS_DB_MEDDRA_ICD_TMP.BK_MEDDRA_ICD_WID=-1;

					
					
					
UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD   
SET  LS_DB_MEDDRA_ICD.BK_MEDDRA_ICD_WID       =LS_DB_MEDDRA_ICD_TMP.BK_MEDDRA_ICD_WID
,LS_DB_MEDDRA_ICD.PROCESSING_DT               =LS_DB_MEDDRA_ICD_TMP.PROCESSING_DT                   
,LS_DB_MEDDRA_ICD.EXPIRY_DATE                 =LS_DB_MEDDRA_ICD_TMP.EXPIRY_DATE 
,LS_DB_MEDDRA_ICD.CREATED_BY                  =LS_DB_MEDDRA_ICD_TMP.CREATED_BY
,LS_DB_MEDDRA_ICD.CREATED_DT                  =LS_DB_MEDDRA_ICD_TMP.CREATED_DT 
,LS_DB_MEDDRA_ICD.LOAD_TS                     =LS_DB_MEDDRA_ICD_TMP.LOAD_TS
,LS_DB_MEDDRA_ICD.ICD_CODE                    =LS_DB_MEDDRA_ICD_TMP.ICD_CODE  
,LS_DB_MEDDRA_ICD.LLT_CODE                    =LS_DB_MEDDRA_ICD_TMP.LLT_CODE 
,LS_DB_MEDDRA_ICD.LLT_NAME                    =LS_DB_MEDDRA_ICD_TMP.LLT_NAME   
,LS_DB_MEDDRA_ICD.PT_CODE                     =LS_DB_MEDDRA_ICD_TMP.PT_CODE  
,LS_DB_MEDDRA_ICD.HLT_CODE                    =LS_DB_MEDDRA_ICD_TMP.HLT_CODE  
,LS_DB_MEDDRA_ICD.HLGT_CODE                   =LS_DB_MEDDRA_ICD_TMP.HLGT_CODE  
,LS_DB_MEDDRA_ICD.SOC_CODE                    =LS_DB_MEDDRA_ICD_TMP.SOC_CODE  
,LS_DB_MEDDRA_ICD.PT_NAME                     =LS_DB_MEDDRA_ICD_TMP.PT_NAME  
,LS_DB_MEDDRA_ICD.HLT_NAME                    =LS_DB_MEDDRA_ICD_TMP.HLT_NAME  
,LS_DB_MEDDRA_ICD.HLGT_NAME                   =LS_DB_MEDDRA_ICD_TMP.HLGT_NAME  
,LS_DB_MEDDRA_ICD.SOC_NAME                    =LS_DB_MEDDRA_ICD_TMP.SOC_NAME 
,LS_DB_MEDDRA_ICD.MEDDRA_VERSION              =LS_DB_MEDDRA_ICD_TMP.MEDDRA_VERSION 
,LS_DB_MEDDRA_ICD.LLT_NAME_OTHER              =LS_DB_MEDDRA_ICD_TMP.LLT_NAME_OTHER   
,LS_DB_MEDDRA_ICD.IS_MEDDRA_FLAG              =LS_DB_MEDDRA_ICD_TMP.IS_MEDDRA_FLAG 
,LS_DB_MEDDRA_ICD.IS_ICD_FLAG                 =LS_DB_MEDDRA_ICD_TMP.IS_ICD_FLAG   
,LS_DB_MEDDRA_ICD.LANGUAGE_CODE               =LS_DB_MEDDRA_ICD_TMP.LANGUAGE_CODE 
,LS_DB_MEDDRA_ICD.INTERNATIONAL_SOC_ORDER     =LS_DB_MEDDRA_ICD_TMP.INTERNATIONAL_SOC_ORDER  
,LS_DB_MEDDRA_ICD.SOC_ORDER                   =LS_DB_MEDDRA_ICD_TMP.SOC_ORDER 
,LS_DB_MEDDRA_ICD.PRIMARY_SOC_NAME            =LS_DB_MEDDRA_ICD_TMP.PRIMARY_SOC_NAME  
,LS_DB_MEDDRA_ICD.INTERNATIONAL_PRIMARY_ORDER =LS_DB_MEDDRA_ICD_TMP.INTERNATIONAL_PRIMARY_ORDER 
,LS_DB_MEDDRA_ICD.HLGT_NAME_OTHER             =LS_DB_MEDDRA_ICD_TMP.HLGT_NAME_OTHER  
,LS_DB_MEDDRA_ICD.HLT_NAME_OTHER              =LS_DB_MEDDRA_ICD_TMP.HLT_NAME_OTHER  
,LS_DB_MEDDRA_ICD.PT_NAME_OTHER               =LS_DB_MEDDRA_ICD_TMP.PT_NAME_OTHER  
,LS_DB_MEDDRA_ICD.SOC_NAME_OTHER  	          =LS_DB_MEDDRA_ICD_TMP.SOC_NAME_OTHER  	
,LS_DB_MEDDRA_ICD.PT_SOC_CODE                 =LS_DB_MEDDRA_ICD_TMP.PT_SOC_CODE 
,LS_DB_MEDDRA_ICD.PRIMARY_SOC_FG              =LS_DB_MEDDRA_ICD_TMP.PRIMARY_SOC_FG     
,LS_DB_MEDDRA_ICD.MDHI_RECORD_ID              =LS_DB_MEDDRA_ICD_TMP.MDHI_RECORD_ID 
,LS_DB_MEDDRA_ICD.LOWLE_RECORD_ID             =LS_DB_MEDDRA_ICD_TMP.LOWLE_RECORD_ID       
FROM 	$$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP 
WHERE 	LS_DB_MEDDRA_ICD.INTEGRATION_ID = LS_DB_MEDDRA_ICD_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_MEDDRA_ICD_TMP.PROCESSING_DT = LS_DB_MEDDRA_ICD.PROCESSING_DT,1=1);









INSERT INTO LS_DB_MEDDRA_ICD
(BK_MEDDRA_ICD_WID
,PROCESSING_DT
,EXPIRY_DATE 
,CREATED_BY
,CREATED_DT 
,LOAD_TS
,INTEGRATION_ID
,ICD_CODE  
,LLT_CODE 
,LLT_NAME   
,PT_CODE  
,HLT_CODE  
,HLGT_CODE  
,SOC_CODE  
,PT_NAME  
,HLT_NAME  
,HLGT_NAME  
,SOC_NAME 
,MEDDRA_VERSION 
,LLT_NAME_OTHER   
,IS_MEDDRA_FLAG 
,IS_ICD_FLAG   
,LANGUAGE_CODE 
,INTERNATIONAL_SOC_ORDER  
,SOC_ORDER 
,PRIMARY_SOC_NAME  
,INTERNATIONAL_PRIMARY_ORDER  
,HLGT_NAME_OTHER  
,HLT_NAME_OTHER  
,PT_NAME_OTHER  
,SOC_NAME_OTHER  	
,PT_SOC_CODE 
,PRIMARY_SOC_FG 
,MDHI_RECORD_ID
,LOWLE_RECORD_ID
)
select BK_MEDDRA_ICD_WID
,PROCESSING_DT
,EXPIRY_DATE 
,CREATED_BY
,CREATED_DT 
,LOAD_TS
,INTEGRATION_ID
,ICD_CODE  
,LLT_CODE 
,LLT_NAME   
,PT_CODE  
,HLT_CODE  
,HLGT_CODE  
,SOC_CODE  
,PT_NAME  
,HLT_NAME  
,HLGT_NAME  
,SOC_NAME 
,MEDDRA_VERSION 
,LLT_NAME_OTHER   
,IS_MEDDRA_FLAG 
,IS_ICD_FLAG   
,LANGUAGE_CODE 
,INTERNATIONAL_SOC_ORDER  
,SOC_ORDER 
,PRIMARY_SOC_NAME  
,INTERNATIONAL_PRIMARY_ORDER  
,HLGT_NAME_OTHER  
,HLT_NAME_OTHER  
,PT_NAME_OTHER  
,SOC_NAME_OTHER  	
,PT_SOC_CODE 
,PRIMARY_SOC_FG   
,MDHI_RECORD_ID
,LOWLE_RECORD_ID   
FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP TMP 
where not exists (select 1 from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


COMMIT;



UPDATE  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP 
WHERE 	TO_DATE(LS_DB_MEDDRA_ICD.PROCESSING_DT) < TO_DATE(LS_DB_MEDDRA_ICD_TMP.PROCESSING_DT)
AND LS_DB_MEDDRA_ICD.INTEGRATION_ID = LS_DB_MEDDRA_ICD_TMP.INTEGRATION_ID
AND LS_DB_MEDDRA_ICD.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

DELETE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD TGT
WHERE  ( LOWLE_RECORD_ID  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP  WHERE TABLE_NAME='LSMV_LOW_LEVEL_TERM') 
OR MDHI_RECORD_ID  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP  WHERE TABLE_NAME='LSMV_MD_HIERARCHY')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( LOWLE_RECORD_ID  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP  WHERE TABLE_NAME='LSMV_LOW_LEVEL_TERM') 
OR MDHI_RECORD_ID  in (SELECT RECORD_ID FROM  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_DELETION_TMP  WHERE TABLE_NAME='LSMV_MD_HIERARCHY')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');
           
UPDATE $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD where LOAD_TS= (select LOAD_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_MEDDRA_ICD'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_MEDDRA_ICD'
and LOAD_STATUS = 'In Progress') ;

  RETURN 'LS_DB_MEDDRA_ICD Load completed';
           
EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_MEDDRA_ICD'
and LOAD_STATUS = 'In Progress'
;
  RETURN 'LS_DB_MEDDRA_ICD not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL from more details';
END;
$$
;
