


/*
call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_DER();
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_DRUG_THERAPY_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_PROD_DER';

delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_PROD_DER';


truncate table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER;
*/



CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_DER()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_PROD_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_PROD_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_PROD_DER'),
	'LSRA','Case','LS_DB_CASE_PROD_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_PROD_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_PROD_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_PROD_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  As 
select RECORD_ID,'lsmv_drug_therapy' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_drug' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')




),

lsmv_drug_therapy_SUBSET as
 (
 
 
select  ARI_REC_ID,fk_ad_rec_id As SEQ_PRODUCT,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,fk_ad_rec_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='lsmv_drug_therapy')
) where
REC_RANK=1
group by 1,2
) ,lsmv_drug_SUBSET as
(
select  ARI_REC_ID,record_id As SEQ_PRODUCT,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='lsmv_drug')
) where
REC_RANK=1
group by 1,2
),
LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET as
 (
 
 
select  ARI_REC_ID,FK_DEVICE_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,FK_DEVICE_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='LSMV_DEVICE_PROB_EVAL_IMDRF')
) where
REC_RANK=1
group by 1,2
) ,LSMV_DEVICE_SUBSET as
(
select  record_id,ARI_REC_ID,FK_AD_REC_ID As SEQ_PRODUCT,max(date_modified) date_modified  
FROM
(
select record_id,ARI_REC_ID,FK_AD_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='LSMV_DEVICE')
) where
REC_RANK=1
group by 1,2,3
)
,LSMV_DRUG_APPROVAL_SUBSET as
(
select  record_id,ARI_REC_ID,FK_DRUG_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id,ARI_REC_ID,FK_DRUG_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='LSMV_DRUG_APPROVAL')
) where
REC_RANK=1
group by 1,2,3
)
,LSMV_ACTIVE_SUBSTANCE_SUBSET as 
(
select  record_id,ARI_REC_ID,FK_AD_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id,ARI_REC_ID,FK_AD_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='LSMV_DRUG_APPROVAL')
) where
REC_RANK=1
group by 1,2,3
)

,LSMV_DRUG_INDICATION_SUBSET as 
(
select  record_id,ARI_REC_ID,FK_AD_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id,ARI_REC_ID,FK_AD_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP where table_name='LSMV_DRUG_INDICATION')
) where
REC_RANK=1
group by 1,2,3
)



 SELECT lsmv_drug_SUBSET.ARI_REC_ID,
 lsmv_drug_SUBSET.SEQ_PRODUCT,
 max(to_date(GREATEST(NVL(lsmv_drug_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_therapy_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEVICE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_APPROVAL_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACTIVE_SUBSTANCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_INDICATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(lsmv_drug_SUBSET.ARI_REC_ID,-1),'||',NVL(lsmv_drug_SUBSET.SEQ_PRODUCT,-1)) INTEGRATION_ID
,max(GREATEST(NVL(lsmv_drug_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_therapy_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEVICE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_APPROVAL_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_ACTIVE_SUBSTANCE_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_INDICATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)) as DATE_MODIFIED
FROM lsmv_drug_SUBSET left join lsmv_drug_therapy_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=lsmv_drug_therapy_SUBSET.ari_rec_id
and lsmv_drug_SUBSET.SEQ_PRODUCT=lsmv_drug_therapy_SUBSET.SEQ_PRODUCT
left join LSMV_DEVICE_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=LSMV_DEVICE_SUBSET.ari_rec_id
and lsmv_drug_SUBSET.SEQ_PRODUCT=LSMV_DEVICE_SUBSET.SEQ_PRODUCT
left join LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET.ari_rec_id
and LSMV_DEVICE_SUBSET.record_id=LSMV_DEVICE_PROB_EVAL_IMDRF_SUBSET.FK_DEVICE_REC_ID
left join LSMV_DRUG_APPROVAL_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=LSMV_DRUG_APPROVAL_SUBSET.ari_rec_id
and lsmv_drug_SUBSET.SEQ_PRODUCT=LSMV_DRUG_APPROVAL_SUBSET.FK_DRUG_REC_ID
left join LSMV_ACTIVE_SUBSTANCE_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=LSMV_ACTIVE_SUBSTANCE_SUBSET.ari_rec_id
and lsmv_drug_SUBSET.SEQ_PRODUCT=LSMV_ACTIVE_SUBSTANCE_SUBSET.FK_AD_REC_ID
left join LSMV_DRUG_INDICATION_SUBSET 
on lsmv_drug_SUBSET.ari_rec_id=LSMV_DRUG_INDICATION_SUBSET.ari_rec_id
and lsmv_drug_SUBSET.SEQ_PRODUCT=LSMV_DRUG_INDICATION_SUBSET.FK_AD_REC_ID
group by lsmv_drug_SUBSET.ARI_REC_ID,
 lsmv_drug_SUBSET.SEQ_PRODUCT
;


  
  

ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_THERAPY_REGIMEN_START_DATE 		TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_THERAPY_REGIMEN_END_DATE   		TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_MED_DEVICE_PROB_CONC_ANNEX_BCD  TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_MED_DEVICE_PROB_CONC_ANNEX_G    TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_FSCA_REF_NUM_CONC               TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_THERAPY_PERIOD_CONCAT           TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_LOT_NO TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_MDR_TYPE_CONC           TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_MED_DEVICE_PROB_CONC           TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_MIN_SEQ_ACT_SUB           TEXT(4000);    
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_FORM_STRENGTH_INFO TEXT(16000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_DAILY_DOSE_INFO  TEXT(16000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_INDICATIONS_PT_COMBINED              TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_INDICATION_LLT_COMBINED              TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_REMEDIAL_ACTION_CONC TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_REF_NUM_CONC TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP ADD COLUMN DER_ROUTE_ADMIN_INFO TEXT(4000);

  
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED AS
(with drug_subset as (

select ARI_REC_ID,RECORD_ID,BLINDED_PRODUCT_REC_ID
from (
SELECT LSMV_DRUG.ARI_REC_ID,RECORD_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER)
		) where rank=1 and	CDC_OPERATION_TYPE IN ('I','U')
)

SELECT DISTINCT LSMV_DRUG.ARI_REC_ID ,
  LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,
  AD.record_id AS SEQ_UNBLINDED
FROM drug_subset LSMV_DRUG				 
LEFT JOIN
  drug_subset AD
ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.RECORD_ID
);



    



drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP as 
select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID
  ,PRODUCT_TYPE
  from 
			(select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,
                PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,PRODUCT_TYPE,
                row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;
                  
                  

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ACTIVE_SUBSTANCE_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ACTIVE_SUBSTANCE_SUBSET_TMP as 
	  select record_id,ARI_REC_ID,ACTIVESUBSTANCENAME,FK_AD_REC_ID
  from 
			(select record_id,ARI_REC_ID,ACTIVESUBSTANCENAME,FK_AD_REC_ID,
                row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_ACTIVE_SUBSTANCE 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;



drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP as 
	  select *
  from 
			(select record_id,ARI_REC_ID,FK_AD_REC_ID,FORM_STRENGTH,FORM_STRENGTH_UNIT,
			DRUGADMINISTRATIONROUTE,DRUGADMINISTRATIONROUTE_NF,DRUGADMINISTRATIONROUTE_SF,
			CASE WHEN COALESCE(LSMV_DRUG_THERAPY.SPR_ID,'')='' then '-9999' else LSMV_DRUG_THERAPY.SPR_ID END AS SPR_ID,
                row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE,DAILY_DOSE,DAILY_DOSE_UNIT
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;



 drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp ;
create temp table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
 
 select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE,r3_code from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE,r3_code
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9947','9948','9949','10007','9946','9070','1018','1020','9037')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,EMDR_CODE,r3_code,
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


	

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_REGIMEN_START_DATE=case when LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_START_DATE is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_START_DATE)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_START_DATE,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_START_DATE end
	 else null end
  

FROM (
SELECT ARI_REC_ID,LISTAGG(DER_THERAPY_REGIMEN_START_DATE,'\n') within group (order by ARI_REC_ID,class desc) DER_THERAPY_REGIMEN_START_DATE,
SEQ_PRODUCT
from
(
select ARI_REC_ID,case when class='BN' then '@\n'||listagg(DER_THERAPY_REGIMEN_START_DATE,'|') within group (order by ARI_REC_ID,class)
else listagg(DER_THERAPY_REGIMEN_START_DATE,'|') within group (order by ARI_REC_ID,class) end DER_THERAPY_REGIMEN_START_DATE,
SEQ_PRODUCT,class from (
(
SELECT
	ARI_REC_ID,
	SEQ_PRODUCT,
	LISTAGG( START_THERAPY_DATE,'\r\n')within group (order by SEQ_THERAPY) DER_THERAPY_REGIMEN_START_DATE,
	'UN' class
FROM 
(
select 
APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.START_THERAPY_DATE,'-') As START_THERAPY_DATE
	,MIN(APT.SEQ_THERAPY) SEQ_THERAPY

FROM
	(SELECT DISTINCT ari_rec_id ari_rec_id,record_id SEQ_THERAPY,FK_AD_REC_ID SEQ_PRODUCT,
		CASE
		   WHEN DRUGSTARTDATEFMT  IS NULL
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT  in (0,102)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT  in (1,602)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYY')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT in (2,610)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMM')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT  in (3,4,5,6,7,8,9,204,611,203) 
		   THEN 
		     case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		 END START_THERAPY_DATE
    FROM 
		(select record_id,FK_AD_REC_ID,ARI_REC_ID,DRUGSTARTDATE,DRUGSTARTDATEFMT,DRUGSTARTDATE_NF,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY
			where ARI_REC_ID in (select DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
	)	APT
	LEFT OUTER JOIN       (SELECT SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE FROM 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
	) AP
	ON (APT.ARI_REC_ID=AP.ARI_REC_ID and APT.SEQ_PRODUCT=AP.SEQ_PRODUCT)
	WHERE 
		 AP.ARI_REC_ID||'-'||AP.SEQ_PRODUCT  NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
GROUP BY APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.START_THERAPY_DATE,'-')
) final_output	
GROUP BY 
	ARI_REC_ID,
	SEQ_PRODUCT
)

UNION ALL

(
SELECT
	ARI_REC_ID,
	SEQ_PRODUCT,
	LISTAGG( START_THERAPY_DATE,'\r\n')within group (order by SEQ_THERAPY) DER_THERAPY_REGIMEN_START_DATE,
	'BN' class
FROM 
(
select 
APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.START_THERAPY_DATE,'-') As START_THERAPY_DATE
	,MIN(APT.SEQ_THERAPY) SEQ_THERAPY

FROM
	(SELECT DISTINCT ari_rec_id ari_rec_id,record_id SEQ_THERAPY,FK_AD_REC_ID SEQ_PRODUCT,
		CASE
		   WHEN DRUGSTARTDATEFMT IS NULL
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT in (0,102)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT in (1,602)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYY')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT in (2,610)
		   THEN 
		   case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMM')
				else DRUGSTARTDATE_NF
		   end
		   WHEN DRUGSTARTDATEFMT in (3,4,5,6,7,8,9,204,611,203) 
		   THEN 
		     case
				when DRUGSTARTDATE is not null
				then TO_CHAR((DRUGSTARTDATE),'YYYYMMDD')
				else DRUGSTARTDATE_NF
		   end
		 END START_THERAPY_DATE
    FROM 
		(select record_id,FK_AD_REC_ID,ARI_REC_ID,DRUGSTARTDATE,DRUGSTARTDATEFMT,DRUGSTARTDATE_NF,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY
			where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
   ) APT
	LEFT OUTER JOIN 

      (SELECT SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE FROM 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
	)	AP 
	ON (APT.ARI_REC_ID=AP.ARI_REC_ID and APT.SEQ_PRODUCT=AP.SEQ_PRODUCT)
	WHERE 
	 AP.ARI_REC_ID||'-'||AP.SEQ_PRODUCT  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
	
GROUP BY APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.START_THERAPY_DATE,'-')
) final_output	
GROUP BY 
	ARI_REC_ID,
	SEQ_PRODUCT
)
)
  group by ARI_REC_ID,class ,SEQ_PRODUCT
  order by  ARI_REC_ID,class desc
  )
 group by ARI_REC_ID,SEQ_PRODUCT
  
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


-- DER_THERAPY_REGIMEN_END_DATE

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_REGIMEN_END_DATE=
case when LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_END_DATE is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_END_DATE)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_END_DATE,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_REGIMEN_END_DATE end
	 else null end 
  
FROM (
SELECT ARI_REC_ID,
  LISTAGG(DER_THERAPY_REGIMEN_END_DATE,'\n') within group (order by ARI_REC_ID,class desc) DER_THERAPY_REGIMEN_END_DATE,
SEQ_PRODUCT
from
(
select ARI_REC_ID,case when class='BN' then '@\n'||listagg(DER_THERAPY_REGIMEN_END_DATE,'|') within group (order by ARI_REC_ID,class)
else listagg(DER_THERAPY_REGIMEN_END_DATE,'|') within group (order by ARI_REC_ID,class) end DER_THERAPY_REGIMEN_END_DATE,
SEQ_PRODUCT,class from (
(
SELECT
	ARI_REC_ID,
	SEQ_PRODUCT,
	LISTAGG( END_THERAPY_DATE,'\r\n')within group (order by SEQ_THERAPY) DER_THERAPY_REGIMEN_END_DATE,
	'UN' class
FROM 
(
select 
APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.END_THERAPY_DATE,'-') As END_THERAPY_DATE
	,MIN(APT.SEQ_THERAPY) SEQ_THERAPY

FROM
	(SELECT DISTINCT ari_rec_id ari_rec_id,record_id SEQ_THERAPY,FK_AD_REC_ID SEQ_PRODUCT,
		CASE
		   WHEN DRUGENDDATEFMT  IS NULL
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT  in (0,102)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT  in (1,602)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYY')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT in (2,610)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMM')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT  in (3,4,5,6,7,8,9,204,611,203) 
		   THEN 
		     case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		 END END_THERAPY_DATE
	FROM (select record_id,FK_AD_REC_ID,ARI_REC_ID,DRUGENDDATE,DRUGENDDATEFMT,DRUGENDDATE_NF,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY
			where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
   ) APT
	LEFT OUTER JOIN       (SELECT SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE FROM 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') )
	AP
	ON (APT.ARI_REC_ID=AP.ARI_REC_ID and APT.SEQ_PRODUCT=AP.SEQ_PRODUCT)
	WHERE 
		 AP.ARI_REC_ID||'-'||AP.SEQ_PRODUCT  NOT IN
    (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
GROUP BY APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.END_THERAPY_DATE,'-')
) final_output	
GROUP BY 
	ARI_REC_ID,
	SEQ_PRODUCT
)

UNION ALL

(
SELECT
	ARI_REC_ID,
	SEQ_PRODUCT,
	LISTAGG( END_THERAPY_DATE,'\r\n')within group (order by SEQ_THERAPY) DER_THERAPY_REGIMEN_END_DATE,
	'BN' class
FROM 
(
select 
APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.END_THERAPY_DATE,'-') As END_THERAPY_DATE
	,MIN(APT.SEQ_THERAPY) SEQ_THERAPY

FROM
	(SELECT DISTINCT ari_rec_id ari_rec_id,record_id SEQ_THERAPY,FK_AD_REC_ID SEQ_PRODUCT,
		CASE
		   WHEN DRUGENDDATEFMT IS NULL
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT in (0,102)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT in (1,602)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYY')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT in (2,610)
		   THEN 
		   case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMM')
				else DRUGENDDATE_NF
		   end
		   WHEN DRUGENDDATEFMT in (3,4,5,6,7,8,9,204,611,203) 
		   THEN 
		     case
				when DRUGENDDATE is not null
				then TO_CHAR((DRUGENDDATE),'YYYYMMDD')
				else DRUGENDDATE_NF
		   end
		 END END_THERAPY_DATE
	FROM (select record_id,FK_AD_REC_ID,ARI_REC_ID,DRUGENDDATE,DRUGENDDATE_NF,DRUGENDDATEFMT,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY
			where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
   ) APT
	LEFT OUTER JOIN 

      (SELECT SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE FROM 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') )
	AP 
	ON (APT.ARI_REC_ID=AP.ARI_REC_ID and APT.SEQ_PRODUCT=AP.SEQ_PRODUCT)
	WHERE 
	 AP.ARI_REC_ID||'-'||AP.SEQ_PRODUCT  IN
    (select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is null) 
	
GROUP BY APT.ARI_REC_ID,
	APT.SEQ_PRODUCT,
	COALESCE(APT.END_THERAPY_DATE,'-')
) final_output	
GROUP BY 
	ARI_REC_ID,
	SEQ_PRODUCT
)
)
  group by ARI_REC_ID,class ,SEQ_PRODUCT
  order by  ARI_REC_ID,class desc
  )
 group by ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp ;
create temp table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp AS 
 
   select APD.RECORD_ID,ARI_REC_ID,EVALUATION_TYPE_IMDRF,EVALUATION_VALUE_IMDRF,FK_DEVICE_REC_ID,
             EVALUATION_TYPE_IMDRF_SF,EVALUATION_VALUE_IMDRF_SF,FK_AD_REC_ID,SPR_ID
	FROM 		 
      (select RECORD_ID ,ARI_REC_ID,EVALUATION_TYPE_IMDRF,EVALUATION_VALUE_IMDRF,FK_DEVICE_REC_ID,
             EVALUATION_TYPE_IMDRF_SF,EVALUATION_VALUE_IMDRF_SF,CDC_OPERATION_TYPE,COALESCE(SPR_ID,'-9999') AS SPR_ID,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD JOIN 
		(select RECORD_ID ,FK_AD_REC_ID,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        ) AGX_DEVICE ON	AGX_DEVICE.RECORD_ID= APD.FK_DEVICE_REC_ID
		
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') 
	AND AGX_DEVICE.rank=1 AND AGX_DEVICE.CDC_OPERATION_TYPE IN ('I','U')
;


drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 ;
create temp table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 AS 
   SELECT SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE FROM 
		(select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,PRODUCT_TYPE,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
			
        )  APD
	where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U')  
	and APD.ARI_REC_ID||'-'||APD.SEQ_PRODUCT not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null
		)
 ;
 
 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD=
case when LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD end
	 else null end 
  
FROM (
 SELECT ARI_REC_ID,SEQ_PRODUCT
,LISTAGG(DER_MED_DEVICE_PROB_CONC_ANNEX_BCD,'| \n') within group (order by rk) AS DER_MED_DEVICE_PROB_CONC_ANNEX_BCD
FROM 
(  SELECT 
ARI_REC_ID,SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_BCD As DER_MED_DEVICE_PROB_CONC_ANNEX_BCD
,min(EVALUATION_TYPE_IMDRF||'-'||EVALUATION_VALUE_IMDRF) rk
	
FROM
( SELECT A.ARI_REC_ID,
			B.SEQ_PRODUCT,
			A.RECORD_ID,
			A.EVALUATION_TYPE_IMDRF,
			A.EVALUATION_VALUE_IMDRF,
  	CASE WHEN CODELIST_ID=9947 then 'Annex B: '
  WHEN CODELIST_ID=9948 then 'Annex C: '
  WHEN CODELIST_ID=9949 then 'Annex D: ' end|| D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')' AS EVALUATION_VALUE_ANNEX_BCD,
  A.SPR_ID
  
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp A  join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 B
  ON B.SEQ_PRODUCT = A.FK_AD_REC_ID  and B.ARI_REC_ID = A.ARI_REC_ID 
  JOIN  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D
  ON A.EVALUATION_VALUE_IMDRF||'-'||CASE WHEN EVALUATION_TYPE_IMDRF='2' then '9947'
  WHEN EVALUATION_TYPE_IMDRF='3' then '9948'
  WHEN EVALUATION_TYPE_IMDRF='4' then '9949' END  =
  D.CODE||'-'||CODELIST_ID AND A.SPR_ID=D.SPR_ID
  where D.CODELIST_ID             in (9947,9948,9949)
  AND A.EVALUATION_TYPE_IMDRF	in ('2','3','4')
  and	B.DRUGCHARACTERIZATION IN ('1','3','10') AND B.PRODUCT_TYPE in ('4','02')	
  
  -- select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp

UNION
  SELECT A.ARI_REC_ID,
		B.SEQ_PRODUCT,
		A.RECORD_ID,
		A.EVALUATION_TYPE_IMDRF_SF,
		A.EVALUATION_VALUE_IMDRF_SF,
		A.EVALUATION_VALUE_IMDRF_SF as EVALUATION_VALUE_ANNEX_BCD,
		A.SPR_ID
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp A  join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 B
  ON B.SEQ_PRODUCT = A.FK_AD_REC_ID and B.ARI_REC_ID = A.ARI_REC_ID 
  where A.EVALUATION_TYPE_IMDRF_SF is not null and A.EVALUATION_VALUE_IMDRF_SF is not null 
   and	B.DRUGCHARACTERIZATION IN ('1','3','10') AND B.PRODUCT_TYPE in ('4','02')	
)

GROUP BY	
ari_rec_id,SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_BCD
) final_output	
group by ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

-- DER_MED_DEVICE_PROB_CONC_ANNEX_G

 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_G=
case when LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_G is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_G)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_G,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_G end
	 else null end 
  
FROM (
SELECT ARI_REC_ID,SEQ_PRODUCT
,LISTAGG(EVALUATION_VALUE_ANNEX_G,'\r\n') within group (order by record_id) AS DER_MED_DEVICE_PROB_CONC_ANNEX_G
FROM 
(  SELECT 
ARI_REC_ID,SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_G As EVALUATION_VALUE_ANNEX_G
,min(record_id) record_id
	
FROM
( SELECT A.ARI_REC_ID,
			B.SEQ_PRODUCT,
			A.RECORD_ID,
			A.EVALUATION_TYPE_IMDRF,
			A.EVALUATION_VALUE_IMDRF,
  	        D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')' AS EVALUATION_VALUE_ANNEX_G,
  A.SPR_ID
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp A  join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 B
  ON B.SEQ_PRODUCT = A.FK_AD_REC_ID  AND B.ARI_REC_ID = A.ARI_REC_ID
  JOIN  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D
  ON A.EVALUATION_VALUE_IMDRF =D.CODE AND A.SPR_ID=D.SPR_ID
  where  D.CODELIST_ID             =10007
  AND A.EVALUATION_TYPE_IMDRF	='5'
  and	B.DRUGCHARACTERIZATION IN ('1','3','10') AND B.PRODUCT_TYPE in ('4','02')	
  

UNION
  SELECT A.ARI_REC_ID,
		B.SEQ_PRODUCT,
		A.RECORD_ID,
		A.EVALUATION_TYPE_IMDRF_SF,
		A.EVALUATION_VALUE_IMDRF_SF,
		A.EVALUATION_VALUE_IMDRF_SF as EVALUATION_VALUE_ANNEX_G,
		A.SPR_ID
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp A  join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 B
  ON B.SEQ_PRODUCT = A.FK_AD_REC_ID AND B.ARI_REC_ID = A.ARI_REC_ID
  where A.EVALUATION_TYPE_IMDRF_SF is not null and A.EVALUATION_VALUE_IMDRF_SF is not null 
  and	B.DRUGCHARACTERIZATION IN ('1','3','10') AND B.PRODUCT_TYPE in ('4','02')	
)

group  BY	
ari_rec_id,SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_G
) final_output	
group by ari_rec_id,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_FSCA_REF_NUM_CONC=
case when LS_DB_CASE_PROD_DER_FINAL.DER_FSCA_REF_NUM_CONC is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_FSCA_REF_NUM_CONC)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_FSCA_REF_NUM_CONC,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_FSCA_REF_NUM_CONC end
	 else null end 
  
FROM (
select ARI_REC_ID,SEQ_PRODUCT
,LISTAGG(DER_FSCA_REF_NUM_CONC,'; \n')within group (order by SEQ_DEVICE) as DER_FSCA_REF_NUM_CONC
from (
select ARI_REC_ID,SEQ_PRODUCT,SEQ_DEVICE
,case when length(pp)=0 or pp=null then null else pp end as DER_FSCA_REF_NUM_CONC
from
(
select ARI_REC_ID,SEQ_PRODUCT,SEQ_DEVICE,
case when substring(pp,length(pp)-1,1)='|'  then substring(pp,1,length(pp)-3) else pp end as pp
from
(
select  
ARI_REC_ID,SEQ_PRODUCT,SEQ_DEVICE,
ltrim(replace(coalesce(NCA_LOCAL_REF_NUM,'')||' | '||coalesce(EUDAMED_FSCA_REF_NUM,'')||' | '||coalesce(MANUFACTURE_FSCA_REF_NUM,''),'|  |','|'),' | ') as PP
from (
		SELECT DISTINCT
		LSMV_DRUG_SUBSET_tmp1.SEQ_PRODUCT AS SEQ_PRODUCT,
		LSMV_DEVICE.RECORD_ID AS SEQ_DEVICE,
		LSMV_DEVICE.ARI_REC_ID AS ARI_REC_ID,
		LSMV_DEVICE.NCA_LOCAL_REF_NUM,
		LSMV_DEVICE.EUDAMED_FSCA_REF_NUM,
		LSMV_DEVICE.manufacture_fsca_ref_num
		FROM
				(
				select RECORD_ID,FK_AD_REC_ID,ARI_REC_ID,NCA_LOCAL_REF_NUM,EUDAMED_FSCA_REF_NUM,manufacture_fsca_ref_num,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
				) LSMV_DEVICE
		INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_tmp1 ON
		LSMV_DRUG_SUBSET_tmp1.SEQ_PRODUCT =LSMV_DEVICE.FK_AD_REC_ID
		where LSMV_DEVICE.rank=1 AND LSMV_DEVICE.CDC_OPERATION_TYPE IN ('I','U')  
	  )
)
) where pp is not null and length(PP)>0 
group by ARI_REC_ID,SEQ_PRODUCT,SEQ_DEVICE
,case when length(pp)=0 or pp=null then null else pp end

) final_out
group by ARI_REC_ID,SEQ_PRODUCT) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
                                   
                                   

 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP   
SET LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_PERIOD_CONCAT=case when LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_PERIOD_CONCAT is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_PERIOD_CONCAT)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_PERIOD_CONCAT,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_THERAPY_PERIOD_CONCAT end
	 else null end 

FROM (SELECT 
ARI_REC_ID,SEQ_PRODUCT,LISTAGG(DER_THERAPY_PERIOD_CONCAT,' | ')within group (order by SEQ_THERAPY) AS DER_THERAPY_PERIOD_CONCAT
FROM
(
SELECT 
A.ARI_REC_ID,
A.SEQ_PRODUCT,
CASE WHEN A.THERPAY_DATE = '-' AND A.DURATION = '-' THEN '-'
ELSE A.THERPAY_DATE ||chr(10)||A.DURATION 
END As DER_THERAPY_PERIOD_CONCAT,
MIN(A.SEQ_THERAPY) AS SEQ_THERAPY
FROM
(
SELECT
APT.ARI_REC_ID,
APT.SEQ_THERAPY,
APT.SEQ_PRODUCT,
CASE 
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NULL AND APT.END_THERAPY_DATE IS NULL AND 
END_THERAPY_DATE_NF  IS NULL THEN '-'
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NULL AND 
APT.END_THERAPY_DATE IS NOT NULL AND END_THERAPY_DATE_NF  IS NULL THEN '-' ||' to '||APT.END_THERAPY_DATE
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NOT NULL AND 
APT.END_THERAPY_DATE IS NOT NULL AND END_THERAPY_DATE_NF  IS NULL THEN START_THERAPY_DATE_NF ||' to '||APT.END_THERAPY_DATE
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NULL AND 
APT.END_THERAPY_DATE IS  NULL AND END_THERAPY_DATE_NF  IS NOT NULL THEN '-' ||' to '||END_THERAPY_DATE_NF
WHEN APT.START_THERAPY_DATE IS NOT  NULL AND START_THERAPY_DATE_NF IS NULL AND 
 APT.END_THERAPY_DATE IS  NULL  AND END_THERAPY_DATE_NF  IS NULL THEN APT.START_THERAPY_DATE ||' to '|| '-'
 WHEN APT.START_THERAPY_DATE IS NOT  NULL AND START_THERAPY_DATE_NF IS NULL AND 
 APT.END_THERAPY_DATE IS  NULL  AND END_THERAPY_DATE_NF  IS NOT NULL THEN APT.START_THERAPY_DATE ||' to '|| END_THERAPY_DATE_NF
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NOT NULL AND 
 APT.END_THERAPY_DATE IS  NULL  AND END_THERAPY_DATE_NF  IS NULL THEN  START_THERAPY_DATE_NF ||' to '|| '-'
WHEN APT.START_THERAPY_DATE IS NULL AND START_THERAPY_DATE_NF IS NOT NULL AND 
 APT.END_THERAPY_DATE IS  NULL  AND END_THERAPY_DATE_NF  IS NOT NULL THEN  START_THERAPY_DATE_NF ||' to '|| END_THERAPY_DATE_NF
ELSE APT.START_THERAPY_DATE ||' to '||APT.END_THERAPY_DATE
END THERPAY_DATE,
CASE WHEN APT.THERAPY_DURATION IS NULL AND DC.DECODE IS NULL THEN '-'
WHEN APT.THERAPY_DURATION IS NULL AND DC.DECODE IS NOT NULL THEN '-' ||' '||DC.DECODE
WHEN APT.THERAPY_DURATION IS NOT NULL AND DC.DECODE IS NULL THEN APT.THERAPY_DURATION ||' '|| '-'
ELSE APT.THERAPY_DURATION ||' '||DC.DECODE
END DURATION

FROM
 (
SELECT
	DISTINCT ARI_REC_ID ARI_REC_ID,
	FK_AD_REC_ID AS SEQ_PRODUCT,
	RECORD_ID AS SEQ_THERAPY,
	CASE
       WHEN DRUGSTARTDATEFMT IS NULL     THEN TO_CHAR((DRUGSTARTDATE),'DD-Mon-YYYY')
       WHEN DRUGSTARTDATEFMT in (0,102)  THEN TO_CHAR((DRUGSTARTDATE),'DD-Mon-YYYY')
       WHEN DRUGSTARTDATEFMT in (1,602)  THEN TO_CHAR((DRUGSTARTDATE),'YYYY')
       WHEN DRUGSTARTDATEFMT in (2,610)  THEN TO_CHAR((DRUGSTARTDATE),'Mon-YYYY')
       --WHEN DRUGSTARTDATEFMT IN(3,4,5,6,7,8,9)
	   WHEN DRUGSTARTDATEFMT in (3,4,5,6,7,8,9,204,611,203) THEN TO_CHAR((DRUGSTARTDATE),'DD-Mon-YYYY')
     END START_THERAPY_DATE,
	CASE WHEN DRUGENDDATEFMT IS NULL  THEN TO_CHAR((DRUGENDDATE),'DD-Mon-YYYY')
       WHEN   DRUGENDDATEFMT in (0,102) THEN TO_CHAR((DRUGENDDATE),'DD-Mon-YYYY')
       WHEN   DRUGENDDATEFMT in (1,602) THEN TO_CHAR((DRUGENDDATE),'YYYY')
       WHEN   DRUGENDDATEFMT in (2,610) THEN TO_CHAR((DRUGENDDATE),'Mon-YYYY')
       --WHEN DRUGENDDATEFMT IN(3,4,5,6,7,8,9)
	   WHEN DRUGENDDATEFMT in (3,4,5,6,7,8,9,204,611,203) THEN TO_CHAR((DRUGENDDATE),'DD-Mon-YYYY')
     END END_THERAPY_DATE ,
	DRUGSTARTDATE_NF AS START_THERAPY_DATE_NF,
	DRUGENDDATE_NF AS END_THERAPY_DATE_NF,
	DRUGADMINDURATION AS THERAPY_DURATION,
	DRUGADMINDURATIONUNIT AS THERAPY_DURATION_UNIT,
	SPR_ID
FROM
			(select ARI_REC_ID,FK_AD_REC_ID,RECORD_ID,DRUGSTARTDATEFMT,DRUGSTARTDATE,DRUGENDDATEFMT,DRUGENDDATE
				,DRUGSTARTDATE_NF,DRUGENDDATE_NF,DRUGADMINDURATION,DRUGADMINDURATIONUNIT,CDC_OPERATION_TYPE
				,COALESCE(SPR_ID,'-9999') As SPR_ID
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
			FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
			where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP)
			) Where Rank=1	and CDC_OPERATION_TYPE IN ('I','U')
    ) APT
	LEFT OUTER JOIN (select CODE,DECODE,SPR_Id from 
							(
								SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
								,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
												order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1017')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,COALESCE(SPR_ID,'-9999') as SPR_ID
																									 ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					) DC
	 ON APT.THERAPY_DURATION_UNIT=DC.CODE and APT.SPR_ID=DC.SPR_ID
	 ORDER BY
	 APT.ari_rec_id,
	APT.SEQ_PRODUCT,
	APT.SEQ_THERAPY
)A
where DER_THERAPY_PERIOD_CONCAT IS NOT NULL
GROUP BY  
A.ari_rec_id,
A.SEQ_PRODUCT,
CASE WHEN A.THERPAY_DATE = '-' AND A.DURATION = '-' THEN '-'
ELSE A.THERPAY_DATE ||chr(10)||A.DURATION 
END 
) final_out
Group by 	ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_LOT_NO=case when LS_DB_CASE_PROD_DER_FINAL.DER_LOT_NO is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_LOT_NO)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_LOT_NO,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_LOT_NO end
	 else null end 
FROM (
SELECT ARI_REC_ID,SEQ_PRODUCT,LISTAGG(LOT_NO,' | ')within group (order by SEQ_PRODUCT) DER_LOT_NO
FROM 
(
SELECT P.ARI_REC_ID,P.SEQ_PRODUCT,P.LOT_NO
  FROM
	
	(select 
		ARI_REC_ID,DRUGCHARACTERIZATION,
		RECORD_ID 
		from 
		(SELECT RECORD_ID,ARI_REC_ID,DRUGCHARACTERIZATION,CDC_OPERATION_TYPE
		,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
			FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP)
		) where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
	)	D JOIN	
	(
	select Distinct PL.ARI_REC_ID,PL.FK_AD_REC_ID SEQ_PRODUCT
	,COALESCE (COALESCE(PL.LOT_NUMBER,PL.LOT_NUMBER_NF),A.DE) LOT_NO
	FROM
		(
		SELECT ARI_REC_ID,FK_AD_REC_ID,LOT_NUMBER,LOT_NUMBER_NF,SPR_ID
		FROM
			(select ARI_REC_ID,FK_AD_REC_ID,LOT_NUMBER,LOT_NUMBER_NF,COALESCE(SPR_ID,'-9999') As SPR_ID,CDC_OPERATION_TYPE
			,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
			FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
			where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP)
			
		    ) Where Rank=1	and CDC_OPERATION_TYPE IN ('I','U')	
		)	PL LEFT JOIN
		
		(select CODE CD,DECODE DE ,SPR_ID from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('350')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,Coalesce(SPR_ID,'-9999') SPR_ID
									,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					
		)A ON   NVL(PL.LOT_NUMBER_NF,'-')=A.CD AND PL.SPR_ID=A.SPR_Id
	)	P ON P.ARI_REC_ID = D.ARI_REC_ID 
			AND P.SEQ_PRODUCT = D.RECORD_ID 
			and D.DRUGCHARACTERIZATION IN ('1','3') 
group by 		P.ARI_REC_ID,P.SEQ_PRODUCT,P.LOT_NO
)final_output
group by ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 
                  
                                
                                   
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC=case when LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_MED_DEVICE_PROB_CONC end
	 else null end  
FROM(
 SELECT ARI_REC_ID,SEQ_PRODUCT,
LISTAGG(EVALUATION_VALUE_ANNEX_A,'\r\n')within group (order by SEQ_IMDRF)  AS DER_MED_DEVICE_PROB_CONC
FROM
(SELECT P.ARI_REC_ID,P.SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_A,min(A.SEQ_IMDRF) AS SEQ_IMDRF

FROM
	(SELECT ari_rec_id ARI_REC_ID,record_id SEQ_PRODUCT FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP
	WHERE drugcharacterization in ('1','3','10') AND PRODUCT_TYPE='4'								
	AND ari_rec_id||record_id NOT IN 
	(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)) P,
	(  SELECT A.ARI_REC_ID,A.SEQ_PRODUCT,A.SEQ_IMDRF,A.EVALUATION_TYPE_IMDRF, A.EVALUATION_VALUE_IMDRF,
  	D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')' AS EVALUATION_VALUE_ANNEX_A,A.SPR_ID
  FROM (SELECT 
	ARI_REC_ID AS ARI_REC_ID,
	RECORD_ID AS SEQ_IMDRF,
	FK_DEVICE_REC_ID AS SEQ_DEVICE,
    FK_AD_REC_ID AS SEQ_PRODUCT,
	EVALUATION_TYPE_IMDRF,
	EVALUATION_VALUE_IMDRF,
    SPR_Id                               
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp) A ,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D 
  WHERE A.EVALUATION_VALUE_IMDRF =D.CODE
  AND D.CODELIST_ID             =9946
  AND A.EVALUATION_TYPE_IMDRF	='1'
   AND A.SPR_ID =D.SPR_ID
      UNION
  SELECT A.ARI_REC_ID,
    A.FK_AD_REC_ID SEQ_PRODUCT,
    A.RECORD_ID SEQ_IMDRF ,
    A.EVALUATION_TYPE_IMDRF_SF,
    A.EVALUATION_VALUE_IMDRF_SF,
    A.EVALUATION_VALUE_IMDRF_SF as EVALUATION_VALUE_ANNEX_A,
    A.SPR_ID
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_PROB_EVAL_IMDRF_subset_tmp A where EVALUATION_TYPE_IMDRF_SF is not null and EVALUATION_VALUE_IMDRF_SF is not null 

	)A where 	P.ARI_REC_ID = A.ARI_REC_ID 
	AND P.SEQ_PRODUCT = A.SEQ_PRODUCT
	
GROUP BY 	
P.ARI_REC_ID,P.SEQ_PRODUCT,EVALUATION_VALUE_ANNEX_A	                                  
 )  group by ARI_REC_ID,SEQ_PRODUCT) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_MDR_TYPE_CONC=case when LS_DB_CASE_PROD_DER_FINAL.DER_MDR_TYPE_CONC is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_MDR_TYPE_CONC)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_MDR_TYPE_CONC,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_MDR_TYPE_CONC end
	 else null end 
FROM (
SELECT ARI_REC_ID,SEQ_PRODUCT
,LISTAGG( intd,'\r\n')within group (order by seq_approval_no) as DER_MDR_TYPE_CONC
FROM 
(
SELECT 
P.ARI_REC_ID,P.SEQ_PRODUCT
,B.intd
,MIN(B.seq_approval_no) as seq_approval_no

FROM
                (SELECT ARI_REC_ID,record_id SEQ_PRODUCT FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP
					WHERE DRUGCHARACTERIZATION IN ('1','3') AND PRODUCT_TYPE in ('4','02')                                                                                                                   
					 AND ARI_REC_ID||'-'||record_id NOT IN 
						(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null)
				) P,
                (select ARI_REC_ID,SEQ_PRODUCT,seq_approval_no,case when length(intd)>0 then substring(intd,1,length(intd)-1) else intd end as intd
                FROM
                (
					SELECT DISTINCT B.ARI_REC_Id,B.FK_DRUG_REC_ID SEQ_PRODUCT,B.record_id seq_approval_no,
									coalesce((case when B.IMPLANTABLE='1' then 'Implantable' else null end) || ' | ' ,'')
						||coalesce((case when B.ACTIVE_DEVICE ='1' then 'Active Device' else null end) || ' | ' ,'')
						||coalesce((case when B.INTENDED_MEDICAL_PROD ='1' then 'Intended to administer and/or remove a medicinal product' else null end) || ' | ' ,'')
						||coalesce((case when B.STERILE_CONDITION ='1' then 'Sterile conditions' else null end) || ' | ' ,'')
						||coalesce((case when B.MEASUREING_FUCNTION ='1' then 'Measuring function' else null end) || ' | ' ,'')
						||coalesce((case when B.REUSABE_INSTRUMENTS ='1' then 'Reusable surgical instruments' else null end) || ' | ' ,'')
						||coalesce((case when B.SOFTWARE_MDR ='1' then 'Software' else null end) || ' | ' ,'')
						||coalesce((case when B.MDR_SYSTEM ='1' then 'Systems' else null end) || ' | ' ,'')
						||coalesce((case when B.PROCEDURE_PACKS ='1' then 'Procedure packs' else null end) || ' | ' ,'')
						||coalesce((case when B.NON_MEDICAL_PURPOSE ='1' then 'Non-medical purpose' else null end) || ' | ' ,'')
						||coalesce((case when B.CUSTOM_MADE ='1' then 'Custom-made' else null end) || ' | ' ,'') intd
									FROM 
							(select RECORD_ID ,ARI_REC_ID,IMPLANTABLE,ACTIVE_DEVICE,INTENDED_MEDICAL_PROD,STERILE_CONDITION,MEASUREING_FUCNTION,
							REUSABE_INSTRUMENTS,SOFTWARE_MDR,MDR_SYSTEM,PROCEDURE_PACKS,NON_MEDICAL_PURPOSE,CUSTOM_MADE,device_class,FK_DRUG_REC_ID
							
							,CDC_OPERATION_TYPE,
								row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
									FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_APPROVAL 
								 where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
								
							)  B
						where B.rank=1 AND B.CDC_OPERATION_TYPE IN ('I','U')
						and  B.device_class in ('10','11','12','13')
			)  A
        )B where  P.ARI_REC_ID = B.ARI_REC_ID 
                AND P.SEQ_PRODUCT = B.SEQ_PRODUCT
group by P.ARI_REC_ID,P.SEQ_PRODUCT
,B.intd
) final_output                
GROUP BY    ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_MIN_SEQ_ACT_SUB=LS_DB_CASE_PROD_DER_FINAL.DER_MIN_SEQ_ACT_SUB
FROM 
(
SELECT 
	IQ.AER_ID,IQ.SEQ_PRODUCT,IQ.ACTIVESUBSTANCENAME as DER_MIN_SEQ_ACT_SUB
FROM
	(
	SELECT DISTINCT
		AP.ARI_REC_ID as AER_ID,AP.RECORD_ID as SEQ_PRODUCT,API.RECORD_ID as SEQ_PRODUCT_INGREDIENT,API.ACTIVESUBSTANCENAME,
		DENSE_RANK() OVER(PARTITION BY AP.ARI_REC_ID,AP.RECORD_ID ORDER BY  AP.ARI_REC_ID,AP.RECORD_ID,API.RECORD_ID  ) PP
	FROM
	    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP AP,${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ACTIVE_SUBSTANCE_SUBSET_TMP API
	    where AP.ARI_REC_ID=API.ARI_REC_ID(+)
	    and   AP.RECORD_ID=API.FK_AD_REC_ID(+)
		and API.ACTIVESUBSTANCENAME is not null and AP.DRUGCHARACTERIZATION in ('1','3','4')
		and  
		 AP.ARI_REC_ID||'-'||AP.RECORD_ID  NOT IN
		(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null) 
	ORDER BY  
		AP.ARI_REC_ID,AP.RECORD_ID,API.RECORD_ID
	) IQ
WHERE PP= 1
) LS_DB_CASE_PROD_DER_FINAL
    WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.AER_ID	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
  


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_FORM_STRENGTH_INFO=case when LS_DB_CASE_PROD_DER_FINAL.DER_FORM_STRENGTH_INFO is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_FORM_STRENGTH_INFO)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_FORM_STRENGTH_INFO,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_FORM_STRENGTH_INFO end
	 else null end 
FROM (
SELECT ARI_REC_ID,FK_AD_REC_ID,LISTAGG(DER_FORM_STRENGTH_INFO,' | ')within group (order by FK_AD_REC_ID) DER_FORM_STRENGTH_INFO
FROM 
(
SELECT
APT.ARI_REC_ID,
APT.FK_AD_REC_ID,
'T'||DENSE_RANK() OVER (PARTITION BY ARI_REC_ID,FK_AD_REC_ID ORDER BY APT.RECORD_ID)||': '||COALESCE(APT.FORM_STRENGTH,'-') || ' ' || COALESCE(CODELIST_SUBSET_TMP.DECODE,'-')  AS DER_FORM_STRENGTH_INFO
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP  APT
LEFT OUTER JOIN  (SELECT CODELIST_ID,CODE,DECODE,SPR_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE CODELIST_ID =9070) AS CODELIST_SUBSET_TMP
		ON APT.FORM_STRENGTH_UNIT =CODELIST_SUBSET_TMP.CODE 
		AND APT.SPR_ID=CODELIST_SUBSET_TMP.SPR_ID
		--WHERE  CODELIST_SUBSET_TMP.CODELIST_ID =9070
ORDER BY
APT.ARI_REC_ID,APT.FK_AD_REC_ID,APT.RECORD_ID
) GROUP BY ARI_REC_ID,FK_AD_REC_ID
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.FK_AD_REC_ID
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_DAILY_DOSE_INFO=case when LS_DB_CASE_PROD_DER_FINAL.DER_DAILY_DOSE_INFO is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_DAILY_DOSE_INFO)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_DAILY_DOSE_INFO,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_DAILY_DOSE_INFO end
	 else null end 
FROM (
SELECT ARI_REC_ID,FK_AD_REC_ID,LISTAGG(DER_DAILY_DOSE_INFO,' | ')within group (order by FK_AD_REC_ID) DER_DAILY_DOSE_INFO
FROM 
(
SELECT
APT.ARI_REC_ID,
APT.FK_AD_REC_ID,
'T'||DENSE_RANK() OVER (PARTITION BY ARI_REC_ID,FK_AD_REC_ID ORDER BY APT.RECORD_ID)||': '||COALESCE(APT.DAILY_DOSE,'-') || ' ' || COALESCE(CODELIST_SUBSET_TMP.DECODE,'-')  AS DER_DAILY_DOSE_INFO
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP  APT
LEFT OUTER JOIN  (SELECT CODELIST_ID,CODE,DECODE,SPR_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_TMP WHERE CODELIST_ID =1018) AS CODELIST_SUBSET_TMP
		ON APT.DAILY_DOSE_UNIT =CODELIST_SUBSET_TMP.CODE 
		AND APT.SPR_ID=CODELIST_SUBSET_TMP.SPR_ID
		--WHERE  CODELIST_SUBSET_TMP.CODELIST_ID =1018
ORDER BY
APT.ARI_REC_ID,APT.FK_AD_REC_ID,APT.RECORD_ID
) GROUP BY ARI_REC_ID,FK_AD_REC_ID
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.FK_AD_REC_ID
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 
 
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_INDICATIONS_PT_COMBINED=case when LS_DB_CASE_PROD_DER_FINAL.DER_INDICATIONS_PT_COMBINED is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_INDICATIONS_PT_COMBINED)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_INDICATIONS_PT_COMBINED,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_INDICATIONS_PT_COMBINED end
	 else null end 
from (
select
  	APD.ARI_REC_ID,
	APD.SEQ_PRODUCT,
	LISTAGG(PT.PT_NAME,' | ')within group (order by APD.RECORD_ID)  DER_INDICATIONS_PT_COMBINED
from
    (
     select * from 
		(select RECORD_ID,ARI_REC_ID,FK_AD_REC_ID as SEQ_PRODUCT,DRGINDCD_LLTCODE as PT_CODE,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION
											 where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')  and SEQ_PRODUCT is not null
     ) APD,
	(select * from 
		(select RECORD_ID,PT_NAME,PT_CODE,MEDDRA_VERSION,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U') 
    and MEDDRA_VERSION = (SELECT 'v.'||MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
	) PT
where
	TRY_TO_NUMBER(APD.PT_CODE)=PT.PT_CODE
GROUP BY 
	APD.ARI_REC_ID,
	APD.SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_INDICATION_LLT_COMBINED=case when LS_DB_CASE_PROD_DER_FINAL.DER_INDICATION_LLT_COMBINED is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_INDICATION_LLT_COMBINED)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_INDICATION_LLT_COMBINED,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_INDICATION_LLT_COMBINED end
	 else null end 
from (
select
  	APD.ARI_REC_ID,
	APD.SEQ_PRODUCT,
	LISTAGG(PT.LLT_NAME,' | ')within group (order by APD.RECORD_ID)  DER_INDICATION_LLT_COMBINED
from
    (
     select * from 
		(select RECORD_ID,ARI_REC_ID,FK_AD_REC_ID as SEQ_PRODUCT,DRUGINDICATION_CODE as LLT_CODE,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_INDICATION
											 where ARI_REC_ID in (select  DISTINCT  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')  and SEQ_PRODUCT is not null
     ) APD,
	(select * from 
		(select RECORD_ID,LLT_NAME,LLT_CODE,MEDDRA_VERSION,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_LOW_LEVEL_TERM
		)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U') 
    and MEDDRA_VERSION = (SELECT 'v.'||MEDDRA_VERSION from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION WHERE EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'))
	) PT
where
	TRY_TO_NUMBER(APD.LLT_CODE)=PT.LLT_CODE
GROUP BY 
	APD.ARI_REC_ID,
	APD.SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

	
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_REMEDIAL_ACTION_CONC=case when LS_DB_CASE_PROD_DER_FINAL.DER_REMEDIAL_ACTION_CONC is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_REMEDIAL_ACTION_CONC)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_REMEDIAL_ACTION_CONC,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_REMEDIAL_ACTION_CONC end
	 else null end 
from (	
SELECT ARI_REC_ID,SEQ_PRODUCT
,LISTAGG(case when length(DER_REMEDIAL_ACTION_CONC)>0 then substring(DER_REMEDIAL_ACTION_CONC,1,length(DER_REMEDIAL_ACTION_CONC)-1) else DER_REMEDIAL_ACTION_CONC end,'\n')within group (order by SEQ_DEVICE) as DER_REMEDIAL_ACTION_CONC
FROM 
(
SELECT
P.ARI_REC_ID,P.SEQ_PRODUCT
,case when length(B.DER_REMEDIAL_ACTION_CONC)>0 then substring(B.DER_REMEDIAL_ACTION_CONC,1,length(B.DER_REMEDIAL_ACTION_CONC)-2) else B.DER_REMEDIAL_ACTION_CONC end as DER_REMEDIAL_ACTION_CONC
,min (B.SEQ_DEVICE) As SEQ_DEVICE

FROM
	(SELECT ARI_REC_ID,record_id SEQ_PRODUCT FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP
	WHERE DRUGCHARACTERIZATION IN ('1','3','10') AND PRODUCT_TYPE in ('4','02')								
	AND ARI_REC_ID||record_id NOT IN 
	(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
      where SEQ_UNBLINDED is not null)) P,
	(SELECT A.ARI_REC_ID,A.SEQ_PRODUCT,A.SEQ_DEVICE,
	(NVL(A.Recall || ' | ' ,'') || '' || NVL(A.Repair|| ' | ','') || '' || NVL(A.Repl|| ' | ','') || '' || NVL(A.Relabeling|| ' | ','') || '' ||  NVL(A.Other1|| ' | ','') || '' ||
	NVL(A.Notification|| ' | ','') || '' || NVL(A.Inspection|| ' | ','') || '' ||
	NVL(A.Patient_Monitoring|| ' | ','') || '' || NVL( A.Modification_Adjustment|| ' | ','')
	) as DER_REMEDIAL_ACTION_CONC
	FROM
	(
	SELECT DISTINCT A.ARI_REC_ID,A.SEQ_PRODUCT,A.SEQ_DEVICE,
	CASE WHEN A.REMEDIAL_ACTION_RECALL = '1' THEN 'Recall' end as Recall,
	CASE WHEN A.REMEDIAL_ACTION_REPAIR = '1' THEN 'Repair' end as Repair,
	CASE WHEN A.REMEDIAL_ACTION_REPLACE = '1' THEN 'Replace' end as Repl,
	CASE WHEN A.REMEDIAL_ACTION_RELABEL = '1' THEN 'Relabeling' end as Relabeling,
	CASE WHEN A.REMEDIAL_OTHER = '1' THEN ('Other - ' || COALESCE(A.REMEDIAL_ACTION_OTHER,'')) end as Other1,
	CASE WHEN A.REMEDIAL_ACTION_NOTIFY = '1' THEN 'Notification' end as Notification,
	CASE WHEN A.REMEDIAL_ACTION_INSPECTION = '1' THEN 'Inspection' end as Inspection,
	CASE WHEN A.REMEDIAL_ACTION_PAT_MONITOR = '1' THEN 'Patient Monitoring' end as Patient_Monitoring,
	CASE WHEN A.REMEDIAL_ACTION_MODIFY_ADJUST = '1' THEN 'Modification/Adjustment' end as Modification_Adjustment
	
	
  FROM (
				select FK_AD_REC_ID AS SEQ_PRODUCT,
		         RECORD_ID AS SEQ_DEVICE,
		         ARI_REC_ID AS ARI_REC_ID,
                REMEDIAL_ACTION_RECALL
				,REMEDIAL_ACTION_REPAIR 
				,REMEDIAL_ACTION_REPLACE
				,REMEDIAL_ACTION_RELABEL
				,REMEDIAL_OTHER
                ,REMEDIAL_ACTION_OTHER
				,REMEDIAL_ACTION_NOTIFY
				,REMEDIAL_ACTION_INSPECTION
				,REMEDIAL_ACTION_PAT_MONITOR
				,REMEDIAL_ACTION_MODIFY_ADJUST,CDC_OPERATION_TYPE,
							row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
				) A 
)  A
)B WHERE	P.ARI_REC_ID = B.ARI_REC_ID 
	AND P.SEQ_PRODUCT = B.SEQ_PRODUCT
GROUP BY 
P.ARI_REC_ID,P.SEQ_PRODUCT
,case when length(B.DER_REMEDIAL_ACTION_CONC)>0 then substring(B.DER_REMEDIAL_ACTION_CONC,1,length(B.DER_REMEDIAL_ACTION_CONC)-2) else B.DER_REMEDIAL_ACTION_CONC end
) Final_output

GROUP BY
	ARI_REC_ID,SEQ_PRODUCT	
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

    
      
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_REF_NUM_CONC=case when LS_DB_CASE_PROD_DER_FINAL.DER_REF_NUM_CONC is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_REF_NUM_CONC)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_REF_NUM_CONC,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_REF_NUM_CONC end
	 else null end 
from (		
select ARI_REC_ID,SEQ_PRODUCT
,LISTAGG( case when length(pp)=0 or pp=null then null else pp end,'; \n')within group (order by SEQ_DEVICE) as DER_REF_NUM_CONC
from
(select ARI_REC_ID,SEQ_PRODUCT,SEQ_DEVICE,
case when substring(pp,length(pp)-1,1)='|'  then substring(pp,1,length(pp)-3) else pp end as pp
from
(
SELECT DISTINCT
		FK_AD_REC_ID AS SEQ_PRODUCT,
		RECORD_ID AS SEQ_DEVICE,
		ARI_REC_ID AS ARI_REC_ID,
		NCA_FSCA_REF_NUM,
		EUDAMED_REF_NUM,
		ltrim(replace(coalesce(AER_NUMBER,'')||' | '||coalesce(NCA_FSCA_REF_NUM,'')||' | '||coalesce(EUDAMED_REF_NUM,''),'|  |','|'),' | ') as PP

		FROM
				(
				select RECORD_ID,FK_AD_REC_ID,ARI_REC_ID,NCA_FSCA_REF_NUM,EUDAMED_REF_NUM,CDC_OPERATION_TYPE,AER_NUMBER,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
						FROM ${stage_db_name}.${stage_schema_name}.LSMV_DEVICE 
					where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP) 
				) LSMV_DEVICE

         order by ARI_REC_ID,RECORD_ID
) 
) where pp is not null and length(PP)>0
GROUP BY ARI_REC_ID,SEQ_PRODUCT

) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

       
                  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP  
SET LS_DB_CASE_PROD_DER_TMP.DER_ROUTE_ADMIN_INFO=case when LS_DB_CASE_PROD_DER_FINAL.DER_ROUTE_ADMIN_INFO is not null 
     then case when length(LS_DB_CASE_PROD_DER_FINAL.DER_ROUTE_ADMIN_INFO)>=4000 
               then substring(LS_DB_CASE_PROD_DER_FINAL.DER_ROUTE_ADMIN_INFO,0,3996)||' ...' else LS_DB_CASE_PROD_DER_FINAL.DER_ROUTE_ADMIN_INFO end
	 else null end 
from (
select ARI_REC_ID,SEQ_PRODUCT
,LISTAGG( DER_ROUTE_ADMIN_INFO,' | ')within group (order by SEQ_THERAPY) as DER_ROUTE_ADMIN_INFO
from (SELECT
APT.ARI_REC_ID,
APT.FK_AD_REC_ID as SEQ_PRODUCT,
APT.record_id as SEQ_THERAPY,
'T'||DENSE_RANK() OVER (PARTITION BY ARI_REC_ID,FK_AD_REC_ID ORDER BY record_id)||': '||COALESCE(ROUTE_ADMIN_CODE.DECODE,coalesce(DRUGADMINISTRATIONROUTE_SF,'-')) AS DER_ROUTE_ADMIN_INFO
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP APT,
(SELECT 
		COALESCE(DC.CODE,'-1') CODE, 
		DC.DECODE ,
				DC.R3_CODE,DC.SPR_ID 
	FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp DC
	WHERE
	  DC.CODELIST_ID in (1020,9037)
	 ) ROUTE_ADMIN_CODE
WHERE 
COALESCE(APT.DRUGADMINISTRATIONROUTE,APT.DRUGADMINISTRATIONROUTE_NF)= ROUTE_ADMIN_CODE.CODE (+)
and APT.SPR_ID = ROUTE_ADMIN_CODE.SPR_ID (+)
ORDER BY
APT.ARI_REC_ID,APT.FK_AD_REC_ID,APT.record_id)
group by ARI_REC_ID,SEQ_PRODUCT
) LS_DB_CASE_PROD_DER_FINAL
WHERE LS_DB_CASE_PROD_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_FINAL.SEQ_PRODUCT
	AND LS_DB_CASE_PROD_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP)
where target_table_name='LS_DB_CASE_PROD_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                  where target_table_name='LS_DB_CASE_PROD_DER'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER   
SET LS_DB_CASE_PROD_DER.ARI_REC_ID = LS_DB_CASE_PROD_DER_TMP.ARI_REC_ID,
LS_DB_CASE_PROD_DER.SEQ_PRODUCT = LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT,
LS_DB_CASE_PROD_DER.PROCESSING_DT = LS_DB_CASE_PROD_DER_TMP.PROCESSING_DT,
LS_DB_CASE_PROD_DER.expiry_date    =LS_DB_CASE_PROD_DER_TMP.expiry_date,
LS_DB_CASE_PROD_DER.date_modified    =LS_DB_CASE_PROD_DER_TMP.date_modified,
LS_DB_CASE_PROD_DER.load_ts    =LS_DB_CASE_PROD_DER_TMP.load_ts,
LS_DB_CASE_PROD_DER.DER_THERAPY_REGIMEN_START_DATE     =LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_REGIMEN_START_DATE,  
LS_DB_CASE_PROD_DER.DER_THERAPY_REGIMEN_END_DATE     =LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_REGIMEN_END_DATE  ,
LS_DB_CASE_PROD_DER.DER_MED_DEVICE_PROB_CONC_ANNEX_G     =LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_G,  
LS_DB_CASE_PROD_DER.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD     =LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_BCD ,
LS_DB_CASE_PROD_DER.DER_FSCA_REF_NUM_CONC   =LS_DB_CASE_PROD_DER_TMP.DER_FSCA_REF_NUM_CONC ,
LS_DB_CASE_PROD_DER.DER_THERAPY_PERIOD_CONCAT=LS_DB_CASE_PROD_DER_TMP.DER_THERAPY_PERIOD_CONCAT ,
LS_DB_CASE_PROD_DER.DER_LOT_NO=LS_DB_CASE_PROD_DER_TMP.DER_LOT_NO ,
LS_DB_CASE_PROD_DER.DER_MDR_TYPE_CONC=LS_DB_CASE_PROD_DER_TMP.DER_MDR_TYPE_CONC,
LS_DB_CASE_PROD_DER.DER_MIN_SEQ_ACT_SUB=LS_DB_CASE_PROD_DER_TMP.DER_MIN_SEQ_ACT_SUB,
LS_DB_CASE_PROD_DER.DER_FORM_STRENGTH_INFO=LS_DB_CASE_PROD_DER_TMP.DER_FORM_STRENGTH_INFO,
LS_DB_CASE_PROD_DER.DER_DAILY_DOSE_INFO=LS_DB_CASE_PROD_DER_TMP.DER_DAILY_DOSE_INFO,
LS_DB_CASE_PROD_DER.DER_INDICATIONS_PT_COMBINED=LS_DB_CASE_PROD_DER_TMP.DER_INDICATIONS_PT_COMBINED,
LS_DB_CASE_PROD_DER.DER_INDICATION_LLT_COMBINED=LS_DB_CASE_PROD_DER_TMP.DER_INDICATION_LLT_COMBINED,
LS_DB_CASE_PROD_DER.DER_REMEDIAL_ACTION_CONC =LS_DB_CASE_PROD_DER_TMP.DER_REMEDIAL_ACTION_CONC,
LS_DB_CASE_PROD_DER.DER_REF_NUM_CONC         =LS_DB_CASE_PROD_DER_TMP.DER_REF_NUM_CONC,
LS_DB_CASE_PROD_DER.DER_ROUTE_ADMIN_INFO     =LS_DB_CASE_PROD_DER_TMP.DER_ROUTE_ADMIN_INFO,
LS_DB_CASE_PROD_DER.DER_MED_DEVICE_PROB_CONC =LS_DB_CASE_PROD_DER_TMP.DER_MED_DEVICE_PROB_CONC                 
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP 
WHERE 	LS_DB_CASE_PROD_DER.INTEGRATION_ID = LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_PROD_DER_TMP.PROCESSING_DT = LS_DB_CASE_PROD_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_PROD_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_PROD_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_PROD_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_PROD_DER.SEQ_PRODUCT ,LS_DB_CASE_PROD_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP 
ON LS_DB_CASE_PROD_DER.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT
AND LS_DB_CASE_PROD_DER.INTEGRATION_ID = LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_PROD_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_PROD_DER.SEQ_PRODUCT in (select SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP )
) TMP where LS_DB_CASE_PROD_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_PROD_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_PROD_DER.SEQ_PRODUCT ,LS_DB_CASE_PROD_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP 
    ON LS_DB_CASE_PROD_DER.SEQ_PRODUCT=LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT
    AND LS_DB_CASE_PROD_DER.INTEGRATION_ID = LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_PROD_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_PROD_DER.SEQ_PRODUCT in (select SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER
( ARI_REC_ID    ,
SEQ_PRODUCT       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_THERAPY_REGIMEN_START_DATE,
DER_THERAPY_REGIMEN_END_DATE,
DER_MED_DEVICE_PROB_CONC_ANNEX_G,
DER_MED_DEVICE_PROB_CONC_ANNEX_BCD,
DER_FSCA_REF_NUM_CONC,
DER_THERAPY_PERIOD_CONCAT  ,
DER_LOT_NO,
DER_MDR_TYPE_CONC,
DER_MIN_SEQ_ACT_SUB,
DER_FORM_STRENGTH_INFO,
DER_DAILY_DOSE_INFO,
DER_INDICATIONS_PT_COMBINED,
DER_INDICATION_LLT_COMBINED,
DER_REMEDIAL_ACTION_CONC,
DER_REF_NUM_CONC        ,
DER_ROUTE_ADMIN_INFO    ,
DER_MED_DEVICE_PROB_CONC                  
)
SELECT 
  ARI_REC_ID    ,
SEQ_PRODUCT       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_THERAPY_REGIMEN_START_DATE,
DER_THERAPY_REGIMEN_END_DATE,
DER_MED_DEVICE_PROB_CONC_ANNEX_G,
DER_MED_DEVICE_PROB_CONC_ANNEX_BCD,
DER_FSCA_REF_NUM_CONC,
DER_THERAPY_PERIOD_CONCAT,
DER_LOT_NO,
DER_MDR_TYPE_CONC,
DER_MIN_SEQ_ACT_SUB,
DER_FORM_STRENGTH_INFO,
DER_DAILY_DOSE_INFO,
DER_INDICATIONS_PT_COMBINED,
DER_INDICATION_LLT_COMBINED,
DER_REMEDIAL_ACTION_CONC,
DER_REF_NUM_CONC        ,
DER_ROUTE_ADMIN_INFO  ,
DER_MED_DEVICE_PROB_CONC
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


/*
DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

*/

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_PROD_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_PROD_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_PROD_DER.INTEGRATION_ID = LS_DB_CASE_PROD_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_PROD_DER.SEQ_PRODUCT = LS_DB_CASE_PROD_DER_TMP.SEQ_PRODUCT
AND LS_DB_CASE_PROD_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
 AND MD5(NVL(LS_DB_CASE_PROD_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_PROD_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

/*

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');
*/

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_PROD_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_PROD_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_PROD_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_PROD_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_PROD_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;
  

