-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS_DER()
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS ADD COLUMN DER_COMBINED_RELATEDNESS TEXT;
*/


-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS_DER'


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_DRUG_REACT_RELATEDNESS_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_DRUG_REACT_RELATEDNESS_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS_DER'),
	'LSRA','Case','LS_DB_DRUG_REACT_RELATEDNESS_DER',null,current_timestamp,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_DRUG_REACT_RELATEDNESS_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_REACT_RELATEDNESS_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS)
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_REACT_RELATEDNESS_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_CASE_QFC AS
select distinct ARI_REC_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_REACT_RELATEDNESS_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_REACT_RELATEDNESS_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_DRUG_REACT_RELATEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	



 drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp ;
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
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1017')
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


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS   
SET LS_DB_DRUG_REACT_RELATEDNESS.TIME_TO_ONSET_NORMALIZED_DAYS=LS_DB_DRUG_REACT_RELATEDNESS_TMP.TIME_TO_ONSET_NORMALIZED_DAYS

FROM (
select ari_rec_id,record_id,ceil(Normalized_To_Days*DRUGSTARTLATENCY)  As TIME_TO_ONSET_NORMALIZED_DAYS
from
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS APAA JOIN 
(select CODE,DECODE,SPR_ID,CASE WHEN decode='Day' THEN 1
    WHEN decode='Hour' THEN 0.0416667 
    WHEN decode='Week' THEN 7
    WHEN decode='Month' THEN 30
    WHEN decode='Minute' THEN 0.000694
    WHEN decode='Year' THEN 365
    ELSE 0
    END Normalized_To_Days from 
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where CODELIST_ID='1017'
	) D ON 	APAA.DRUGSTARTLATENCYUNIT=D.CODE and  COALESCE(APAA.SPR_ID,'-9999')=D.SPR_ID
		
where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_DRUG_REACT_RELATEDNESS_TMP
    WHERE LS_DB_DRUG_REACT_RELATEDNESS.ARI_REC_ID = LS_DB_DRUG_REACT_RELATEDNESS_TMP.ARI_REC_ID	
	and LS_DB_DRUG_REACT_RELATEDNESS.RECORD_ID = LS_DB_DRUG_REACT_RELATEDNESS_TMP.RECORD_ID
	AND LS_DB_DRUG_REACT_RELATEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
                                   
-- ALTER table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS   ADD COLUMN DER_TIME_TO_ONSET_COMBINED	TEXT (100);
	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS   
SET LS_DB_DRUG_REACT_RELATEDNESS.DER_TIME_TO_ONSET_COMBINED=LS_DB_DRUG_REACT_RELATEDNESS_TMP.DER_TIME_TO_ONSET_COMBINED

FROM (
select ari_rec_id,record_id,
		case when DRUGSTARTLATENCY  is not null then case when  decode  is null then DRUGSTARTLATENCY 
                                             else DRUGSTARTLATENCY||' '||decode end
			else null 	END AS DER_TIME_TO_ONSET_COMBINED
from
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS APAA JOIN 
(select CODE,DECODE,SPR_ID from 
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where CODELIST_ID='1017'
	) D ON 	APAA.DRUGSTARTLATENCYUNIT=D.CODE and  COALESCE(APAA.SPR_ID,'-9999')=D.SPR_ID
		
where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_CASE_QFC)
and EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_DRUG_REACT_RELATEDNESS_TMP
    WHERE  LS_DB_DRUG_REACT_RELATEDNESS.RECORD_ID = LS_DB_DRUG_REACT_RELATEDNESS_TMP.RECORD_ID
	AND LS_DB_DRUG_REACT_RELATEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');	
	
	
	
	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS_DER'
and LOAD_STATUS = 'In Progress') ;	




 RETURN 'LS_DB_DRUG_REACT_RELATEDNESS_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG_REACT_RELATEDNESS_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

