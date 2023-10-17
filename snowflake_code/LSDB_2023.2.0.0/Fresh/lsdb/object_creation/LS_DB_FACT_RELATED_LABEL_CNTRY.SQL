

--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY()

-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_FACT_RELATED_LABEL_CNTRY';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_FACT_RELATED_LABEL_CNTRY';
-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY() ;

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_FACT_RELATED_LABEL_CNTRY' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_FACT_RELATED_LABEL_CNTRY','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'),
	'LSRA','Case','LS_DB_FACT_RELATED_LABEL_CNTRY',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_FACT_RELATED_LABEL_CNTRY' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  As 
select RECORD_ID,'lsmv_drug' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_reaction' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_reaction WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_drug_react_listedness' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_drug_react_relatedness' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_receipt_item' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE CDC_OPERATION_TYPE IN ('D') ;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP  AS
with LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
 WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_reaction  WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT  record_id, 0 common_parent_key,   ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,   ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  RECORD_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FACT_RELATED_LABEL_CNTRY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 ,
lsmv_receipt_item_SUBSET AS 
(
select * from 
    (SELECT  record_id as ri_record_id,
    date_modified  as ri_date_modified,
	ari_rec_id  ,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_receipt_item
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
record_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_receipt_item')
  ) where REC_RANK=1 )
  , lsmv_drug_SUBSET AS 
(
select * from 
    (SELECT  record_id dr_record_id,
    date_modified  dr_date_modified,
	ari_rec_id  ,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ari_rec_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug')
  ) where REC_RANK=1 )
  , lsmv_reaction_SUBSET AS 
(
select * from 
    (SELECT  record_id rc_record_id,
    date_modified  rc_date_modified,
	ari_rec_id  ,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_reaction
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ari_rec_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_reaction')
  ) where REC_RANK=1  )
  , lsmv_drug_react_listedness_SUBSET AS 
(
select * from 
    (SELECT  record_id drli_record_id,
    date_modified  drli_date_modified,
	ari_rec_id  ,
	fk_adrr_rec_id ,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ari_rec_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
   ) where REC_RANK=1 )
  , lsmv_drug_react_relatedness_SUBSET AS 
(
select * from 
    (SELECT  record_id drre_record_id,
    date_modified  drre_date_modified,
	ari_rec_id  ,
	fk_ar_rec_id ,
	fk_drug_rec_id,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ari_rec_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
   ) where REC_RANK=1 )
 
 SELECT DISTINCT  
	to_date(GREATEST(NVL(lsmv_receipt_item_SUBSET.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(dr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(drli_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),	
	NVL(drre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,:CURRENT_TS_VAR LOAD_TS	 
,lsmv_receipt_item_SUBSET.ri_record_id AS ARI_REC_ID
,apr.ari_rec_id_prod
,apr.ari_rec_id_event
,apr.seq_product
,apr.seq_react
,al.seq_label
,apaa.seq_ae_association
,ri_date_modified
,rc_DATE_MODIFIED
,dr_DATE_MODIFIED
,drli_DATE_MODIFIED
,drre_DATE_MODIFIED
,CONCAT( NVL(lsmv_receipt_item_SUBSET.ri_RECORD_ID,-1),'||',
         NVL(apr.seq_product,-1),'||',
		 NVL(apr.seq_react,-1),'||',
		 NVL(al.seq_label,-1),'||',
		 NVL(apaa.seq_ae_association,-1)) INTEGRATION_ID
FROM
    lsmv_receipt_item_SUBSET
    LEFT JOIN
    (
        SELECT
            ap.ari_rec_id AS ari_rec_id_prod,
            ar.ari_rec_id AS ari_rec_id_event,
            ap.dr_record_id AS seq_product,
            ar.rc_record_id AS seq_react,
           	ar.rc_DATE_MODIFIED,
			ap.dr_DATE_MODIFIED
        FROM
            lsmv_drug_SUBSET ap
            FULL OUTER JOIN lsmv_reaction_SUBSET ar ON ap.ari_rec_id = ar.ari_rec_id
    ) apr ON apr.ari_rec_id_prod = lsmv_receipt_item_SUBSET.ri_record_id
    FULL OUTER JOIN
    (
        SELECT DISTINCT
            lsmv_drug_react_listedness_SUBSET.ari_rec_id,
            lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id,
            lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id,
            lsmv_drug_react_listedness_SUBSET.drli_record_id AS seq_label,
			lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id as seq_product,
			lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id as seq_react,
            /*CASE
                WHEN lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id IS NULL THEN NULL
                ELSE lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id
            END AS seq_product,
            CASE
                WHEN lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id IS NULL THEN NULL
                ELSE lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id
            END AS seq_react,*/
			lsmv_drug_react_listedness_SUBSET.drli_date_modified,
			lsmv_drug_react_relatedness_SUBSET.drre_date_modified		
        FROM
            lsmv_drug_react_listedness_SUBSET
            INNER JOIN lsmv_drug_react_relatedness_SUBSET ON lsmv_drug_react_listedness_SUBSET.fk_adrr_rec_id = lsmv_drug_react_relatedness_SUBSET.drre_record_id
        WHERE
            lsmv_drug_react_listedness_SUBSET.ari_rec_id IS NOT NULL
    ) al ON COALESCE(apr.ari_rec_id_prod, COALESCE(apr.ari_rec_id_event, -1)) = al.ari_rec_id
    AND COALESCE(apr.seq_product, -1) = COALESCE(al.seq_product, -1)
    AND COALESCE(apr.seq_react, -1) = COALESCE(al.seq_react, -1)
    LEFT OUTER JOIN
    (
        SELECT
            lsmv_drug_react_relatedness_SUBSET.ari_rec_id ,
            lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id,
            lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id,
            lsmv_drug_react_relatedness_SUBSET.drre_record_id AS seq_ae_association,
            lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id AS seq_product,
            lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id AS seq_react
        FROM
            lsmv_drug_react_relatedness_SUBSET
            LEFT OUTER JOIN lsmv_drug_SUBSET ON lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id = lsmv_drug_SUBSET.dr_record_id
    ) apaa ON COALESCE(apr.ari_rec_id_prod, COALESCE(apr.ari_rec_id_event, -1)) = apaa.ari_rec_id
    AND COALESCE(apr.seq_product, -1) = COALESCE(apaa.seq_product, -1)
    AND COALESCE(apr.seq_react, -1) = COALESCE(apaa.seq_react, -1)
;
 
 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP)
where target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'
					and LOAD_STATUS = 'In Progress') 
; 


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY   
SET LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ARI_REC_ID,
LS_DB_FACT_RELATED_LABEL_CNTRY.ari_rec_id_prod = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ari_rec_id_prod,
LS_DB_FACT_RELATED_LABEL_CNTRY.ari_rec_id_event = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ari_rec_id_event,
LS_DB_FACT_RELATED_LABEL_CNTRY.seq_product = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.seq_product,
LS_DB_FACT_RELATED_LABEL_CNTRY.seq_react = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.seq_react,
LS_DB_FACT_RELATED_LABEL_CNTRY.seq_label = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.seq_label,
LS_DB_FACT_RELATED_LABEL_CNTRY.seq_ae_association = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.seq_ae_association,
LS_DB_FACT_RELATED_LABEL_CNTRY.ri_date_modified = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ri_date_modified,
LS_DB_FACT_RELATED_LABEL_CNTRY.rc_DATE_MODIFIED = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.rc_DATE_MODIFIED,
LS_DB_FACT_RELATED_LABEL_CNTRY.dr_DATE_MODIFIED= LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.dr_DATE_MODIFIED,
LS_DB_FACT_RELATED_LABEL_CNTRY.drli_DATE_MODIFIED = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drli_DATE_MODIFIED,
LS_DB_FACT_RELATED_LABEL_CNTRY.drre_DATE_MODIFIED = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drre_DATE_MODIFIED,
LS_DB_FACT_RELATED_LABEL_CNTRY.PROCESSING_DT = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.PROCESSING_DT,
LS_DB_FACT_RELATED_LABEL_CNTRY.expiry_date    =LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.expiry_date       ,
LS_DB_FACT_RELATED_LABEL_CNTRY.load_ts        =LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
WHERE 	LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.PROCESSING_DT = LS_DB_FACT_RELATED_LABEL_CNTRY.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.dr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.drli_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.drre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.dr_date_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drli_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID ,LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
ON LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID=LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ARI_REC_ID
AND LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID 
where LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID  is null AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP )
) TMP where LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID ,LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
    ON LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID=LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ARI_REC_ID
    AND LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID 
    where LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID  is null AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
( 
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,
ari_rec_id,
ari_rec_id_prod,
ari_rec_id_event,
seq_product,
seq_react,
seq_label,
seq_ae_association,
ri_date_modified,
rc_DATE_MODIFIED,
dr_DATE_MODIFIED,
drli_DATE_MODIFIED,
drre_DATE_MODIFIED

)
SELECT 
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,
ari_rec_id,
ari_rec_id_prod,
ari_rec_id_event,
seq_product,
seq_react,
seq_label,
seq_ae_association,
ri_date_modified,
rc_DATE_MODIFIED,
dr_DATE_MODIFIED,
drli_DATE_MODIFIED,
drre_DATE_MODIFIED
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY TGT
WHERE  ( ari_rec_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_receipt_item') 
OR seq_product       in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug') 
OR seq_react         in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_reaction') 
OR seq_label           in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
OR seq_ae_association  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
WHERE 	TO_DATE(LS_DB_FACT_RELATED_LABEL_CNTRY.PROCESSING_DT) < TO_DATE(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.PROCESSING_DT)
AND LS_DB_FACT_RELATED_LABEL_CNTRY.INTEGRATION_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.INTEGRATION_ID
AND LS_DB_FACT_RELATED_LABEL_CNTRY.ari_rec_id = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ari_rec_id
AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.dr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.drli_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY.drre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.dr_date_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drli_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.drre_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( ARI_REC_ID  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_receipt_item') 
OR seq_product  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug') 
OR seq_react  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_reaction') 
OR seq_label  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_listedness')
OR seq_ae_association  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.FACT_RELATED_LABEL_CNTRY_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_FACT_RELATED_LABEL_CNTRY Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_FACT_RELATED_LABEL_CNTRY'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_FACT_RELATED_LABEL_CNTRY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
