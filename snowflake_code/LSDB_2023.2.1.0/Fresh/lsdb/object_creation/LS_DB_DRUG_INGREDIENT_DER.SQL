
--select distinct DER_ACT_SUB_RANK from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT limit 10
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where target_table_name='LS_DB_DRUG_INGREDIENT_DER'
-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INGREDIENT_DER()
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INGREDIENT_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT ADD COLUMN DER_ACT_SUB_RANK TEXT;
*/


-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where target_table_name='LS_DB_DRUG_INGREDIENT_DER'


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_DRUG_INGREDIENT_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,'LS_DB_DRUG_INGREDIENT_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INGREDIENT_DER'),
	'LSRA','Case','LS_DB_DRUG_INGREDIENT_DER',null,current_timestamp,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_DRUG_INGREDIENT_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_INGREDIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT)
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_INGREDIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_CASE_QFC AS
select distinct RECEIPT_ID as ARI_REC_ID
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_INGREDIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_INGREDIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') 
AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


/*( select UNBLINDED_REC from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_CASE_UNBLINDED
		)


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT_CASE_UNBLINDED AS
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



*/

--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT   ADD COLUMN DER_ACT_SUB_RANK TEXT;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT   
SET LS_DB_DRUG_INGREDIENT.DER_ACT_SUB_RANK=LS_DB_DRUG_INGREDIENT_TMP.GENERIC_NAME

FROM (SELECT IQ.ARI_REC_ID,
       IQ.SEQ_PRODUCT,
       IQ.SEQ_PRODUCT_INGREDIENT,
       IQ.GENERIC_NAME,
       IQ.DER_ACT_SUB_RANK
FROM (SELECT DISTINCT API.ARI_REC_ID,
             API.SEQ_PRODUCT,
             API.SEQ_PRODUCT_INGREDIENT,
             API.GENERIC_NAME,
             DENSE_RANK() OVER (PARTITION BY API.ARI_REC_ID,API.SEQ_PRODUCT 
                                ORDER BY API.ARI_REC_ID,API.SEQ_PRODUCT,API.SEQ_PRODUCT_INGREDIENT) DER_ACT_SUB_RANK
      FROM (SELECT ACTSUB_ARI_REC_ID  as ARI_REC_ID,
                   ACTSUB_FK_AD_REC_ID,
                   ACTSUB_RECORD_ID AS SEQ_PRODUCT_INGREDIENT,
                  ACTSUB_FK_AD_REC_ID AS SEQ_PRODUCT,
                   ACTSUB_ACTIVESUBSTANCENAME AS GENERIC_NAME,
                   CASE
                     WHEN ACTSUB_SUBSTANCE_STRENGTH IS NULL THEN CAST(NULL AS DOUBLE PRECISION)
                     WHEN ACTSUB_SUBSTANCE_STRENGTH = ' ' THEN CAST(NULL AS DOUBLE PRECISION)
                     WHEN LENGTH(ACTSUB_SUBSTANCE_STRENGTH) <= 0 THEN CAST(NULL AS DOUBLE PRECISION)
                     ELSE CAST(ACTSUB_SUBSTANCE_STRENGTH AS DOUBLE PRECISION)
                   END AS STRENGTH,
                   ACTSUB_SUBSTANCE_STRENGTH_UNIT AS STRENGTH_UNIT,
                   ACTSUB_SUBSTANCE_TERMID AS TERM_ID,
                   ACTSUB_SUBSTANCE_TERMID_VERSION AS TERMID_OPERATION_DATE,
                   NULL AS TERMID_OPERATION_DATE_PREC,
                   NULL AS ENTRY_TYPE
            FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT
       WHERE ACTSUB_ARI_REC_ID in (select ari_rec_id from LS_DB_DRUG_INGREDIENT_CASE_QFC)
	   AND 
	   LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
	   ) API
      WHERE API.GENERIC_NAME IS NOT NULL 
      ORDER BY API.ARI_REC_ID,
               API.SEQ_PRODUCT,
               API.SEQ_PRODUCT_INGREDIENT) IQ
WHERE IQ.DER_ACT_SUB_RANK = 1 
) LS_DB_DRUG_INGREDIENT_TMP
    WHERE LS_DB_DRUG_INGREDIENT.ACTSUB_ARI_REC_ID = LS_DB_DRUG_INGREDIENT_TMP.ARI_REC_ID	
	and LS_DB_DRUG_INGREDIENT.ACTSUB_RECORD_ID = LS_DB_DRUG_INGREDIENT_TMP.SEQ_PRODUCT_INGREDIENT
	and LS_DB_DRUG_INGREDIENT.ACTSUB_FK_AD_REC_ID = LS_DB_DRUG_INGREDIENT_TMP.SEQ_PRODUCT
	AND LS_DB_DRUG_INGREDIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INGREDIENT),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_INGREDIENT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INGREDIENT_DER'
and LOAD_STATUS = 'In Progress') ;	




 RETURN 'LS_DB_DRUG_INGREDIENT_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG_INGREDIENT_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

