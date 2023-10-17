-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LABEL_LIB_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();


--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LABEL_LIB_DER()
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_PRODUCT_LABEL_LIB_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_PRODUCT_LABEL_LIB_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB ADD COLUMN DER_ALL_SOURCE TEXT;
*/






insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_PRODUCT_LABEL_LIB_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_PRODUCT_LABEL_LIB_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LABEL_LIB_DER'),
	'LSRA','Case','LS_DB_PRODUCT_LABEL_LIB_DER',null,CURRENT_TIMESTAMP(),null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_PRODUCT_LABEL_LIB_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_LABEL_LIB_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB)
WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_LABEL_LIB_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_CASE_QFC AS
select distinct INTEGRATION_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_LABEL_LIB_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_PRODUCT_LABEL_LIB_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	

/*
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
create TEMPORARY table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE,LANGUAGE_CODE,E2B_CODE from 
    (
              SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE,E2B_CODE,
                LSMV_CODELIST_DECODE.LANGUAGE_CODE
              ,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
                                                                        order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
                                                                                       FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('346','1019')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,EMDR_CODE,CODE as E2B_CODE,
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
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1; 




*/


-- ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB ADD COLUMN DER_PRODUCT_ID BIGINT;


UPDATE
              ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB
SET
LS_DB_PRODUCT_LABEL_LIB.DER_PRODUCT_ID=LS_DB_PRODUCT_LABEL_LIB_FINAL.DER_PRODUCT_ID
FROM (SELECT DISTINCT
                                                          LS_DB_PRODUCT_LABEL_LIB.integration_id,LSMV_PRODUCT.product_id as DER_PRODUCT_ID
                                           FROM
                                                          ( select record_id,product_id from 
                                                                        (select record_id,product_id,CDC_OPERATION_TYPE
                                                                                      , row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
                                                                        FROM ${stage_db_name}.${stage_schema_name}.lsmv_product
                                                                      --  where   ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_CASE_QFC)
                                                                        
                                                                        )
                                                                        WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U') 
                                                          ) LSMV_PRODUCT
                                                          INNER JOIN 
                                                          ( select INtegration_id,prdlistgrp_FK_PROD_REC_Id from 
                                                                        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB
                                                                        WHERE INtegration_id in (select INtegration_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_CASE_QFC)
                                                                        AND  EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
                                                          ) LS_DB_PRODUCT_LABEL_LIB ON LSMV_PRODUCT.record_id = LS_DB_PRODUCT_LABEL_LIB.prdlistgrp_FK_PROD_REC_Id
              ) LS_DB_PRODUCT_LABEL_LIB_FINAL 
WHERE  LS_DB_PRODUCT_LABEL_LIB.integration_id = LS_DB_PRODUCT_LABEL_LIB_FINAL.integration_id
  AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB ADD COLUMN DER_PRIMARY_PATH_WID BIGINT;

UPDATE
              ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB
SET
LS_DB_PRODUCT_LABEL_LIB.DER_PRIMARY_PATH_WID=LS_DB_PRODUCT_LABEL_LIB_FINAL.DER_PRIMARY_PATH_WID
FROM (
                                           SELECT distinct 
                                                          AR.INTEGRATION_ID,BK_MEDDRA_ICD_WID as DER_PRIMARY_PATH_WID

                                           FROM (select INTEGRATION_ID,labprdind_LLT_CODE,labprdind_PT_CODE from  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB
                                                                        WHERE INtegration_id in (select INtegration_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB_CASE_QFC)
																		and EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
																		)AR,
                                                          ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
                                                         ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
                                           WHERE
                                                          DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
                                                          AND DM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') and DW.PRIMARY_SOC_FG='Y'
                                                          AND try_to_number(COALESCE(TRY_TO_NUMBER(labprdind_LLT_CODE),TRY_TO_NUMBER(labprdind_PT_CODE)),38)=try_to_number(DW.LLT_CODE,38) 
                                                          and try_to_number(TRY_TO_NUMBER(labprdind_PT_CODE),38)=try_to_number(DW.PT_CODE,38)
                                                          
                                     
              ) LS_DB_PRODUCT_LABEL_LIB_FINAL 
WHERE  LS_DB_PRODUCT_LABEL_LIB.INTEGRATION_ID = LS_DB_PRODUCT_LABEL_LIB_FINAL.INTEGRATION_ID
  AND LS_DB_PRODUCT_LABEL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PRODUCT_LABEL_LIB),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PRODUCT_LABEL_LIB_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PRODUCT_LABEL_LIB_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_PRODUCT_LABEL_LIB_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PRODUCT_LABEL_LIB_DER'
and LOAD_STATUS = 'In Progress'
;


  RETURN 'LS_DB_PRODUCT_LABEL_LIB_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;