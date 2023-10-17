--delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_REACT_DER';

--truncate table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER;

--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_REACT_DER()

CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_REACT_DER()
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
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_REACT_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_REACT_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_REACT_DER'),
	'LSRA','Case','LS_DB_CASE_REACT_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_REACT_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_REACT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_REACT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;






DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  As 
select RECORD_ID,'LSMV_REACTION' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'LSMV_IMRDF_EVALUATION' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT ARI_REC_ID FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 
select DISTINCT ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 
select DISTINCT ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')



),

LSMV_REACTION_SUBSET as
 (
 
 
select  ARI_REC_ID,record_id As SEQ_REACT,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP where table_name='LSMV_REACTION')
) where
REC_RANK=1
group by 1,2
) ,LSMV_IMRDF_EVALUATION_SUBSET as
(
select  ARI_REC_ID,FK_AD_REC_ID As SEQ_REACT,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,FK_AD_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP where table_name='LSMV_IMRDF_EVALUATION')
) where
REC_RANK=1
group by 1,2
),lsmv_drug_therapy_SUBSET as
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
),lsmv_receipt_item_SUBSET as
 (
 
 
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id as ARI_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE 
    record_id in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
   -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}_LSRA.LS_DB_CASE_DER_DELETION_TMP where table_name='lsmv_receipt_item')
) where REC_RANK=1
group by 1
),lsmv_drug_react_listedness_SUBSET as
 (
 
 
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id as ARI_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness WHERE 
    record_id in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
   -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}_LSRA.LS_DB_CASE_DER_DELETION_TMP where table_name='lsmv_drug_react_listedness')
) where REC_RANK=1
group by 1
),LSMV_DRUG_REACT_RELATEDNESS_SUBSET as
 (
 
 
select  ARI_REC_ID,SEQ_REACT,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,FK_AR_REC_ID SEQ_REACT,date_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS WHERE 
    record_id in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
   -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}_LSRA.LS_DB_CASE_DER_DELETION_TMP where table_name='LSMV_DRUG_REACT_RELATEDNESS')
) where REC_RANK=1
group by 1,2
)


 SELECT LSMV_REACTION_SUBSET.ARI_REC_ID,
 LSMV_REACTION_SUBSET.SEQ_REACT,
 max(to_date(GREATEST(NVL(LSMV_REACTION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_IMRDF_EVALUATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_therapy_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_react_listedness_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_RELATEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))

)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(LSMV_REACTION_SUBSET.ARI_REC_ID,-1),'||',NVL(LSMV_REACTION_SUBSET.SEQ_REACT,-1)) INTEGRATION_ID
,max(GREATEST(NVL(LSMV_REACTION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_IMRDF_EVALUATION_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_therapy_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_drug_react_listedness_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_DRUG_REACT_RELATEDNESS_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)) as DATE_MODIFIED
FROM LSMV_REACTION_SUBSET left join LSMV_IMRDF_EVALUATION_SUBSET 
on LSMV_REACTION_SUBSET.ari_rec_id=LSMV_IMRDF_EVALUATION_SUBSET.ari_rec_id
and LSMV_REACTION_SUBSET.SEQ_REACT=LSMV_IMRDF_EVALUATION_SUBSET.SEQ_REACT
left JOIN lsmv_drug_SUBSET
ON LSMV_REACTION_SUBSET.ari_rec_id=lsmv_drug_SUBSET.ari_rec_id
 left join lsmv_drug_therapy_SUBSET on lsmv_drug_SUBSET.SEQ_PRODUCT=lsmv_drug_therapy_SUBSET.SEQ_PRODUCT
 left join lsmv_drug_react_listedness_SUBSET on LSMV_REACTION_SUBSET.ARI_REC_ID=lsmv_drug_react_listedness_SUBSET.ARI_REC_ID
 left join LSMV_DRUG_REACT_RELATEDNESS_SUBSET on LSMV_REACTION_SUBSET.ARI_REC_ID=LSMV_DRUG_REACT_RELATEDNESS_SUBSET.ARI_REC_ID
  AND LSMV_REACTION_SUBSET.SEQ_REACT=LSMV_DRUG_REACT_RELATEDNESS_SUBSET.SEQ_REACT

  group by LSMV_REACTION_SUBSET.ARI_REC_ID,
 LSMV_REACTION_SUBSET.SEQ_REACT
;


										
  

ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_MED_DEVICE_PROB_CONC_ANNEX_E TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_MED_DEVICE_PROB_CONC_ANNEX_F TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_TIME_OFF_DRUG_EVENT TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_TIME_OFF_DRUG_DAYS TEXT(4000); 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_TIME_TO_ONSET_NORM TEXT(4000); 
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_TIME_OFF_DRUG_NORMAL  TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_SER_CRITERIA_BRAZIL_CODE int;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP ADD COLUMN DER_TIME_OFF_DRUG_NORM TEXT(4000);
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP	 ADD COLUMN DER_EVENT_SERIOUSNESS_CRITERIA       TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP	 ADD COLUMN DER_REP_CAUS_COMB                    TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP	 ADD COLUMN DER_COMP_CAUS_COMB                   TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP	 ADD COLUMN DER_VERBATERMS_PT                   TEXT;
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP	 ADD COLUMN DER_LLT_PT                   TEXT;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED AS
(with drug_subset as (

select ARI_REC_ID,RECORD_ID,BLINDED_PRODUCT_REC_ID
from (
SELECT LSMV_DRUG.ARI_REC_ID,RECORD_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG --where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
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
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9996','9997','9062')
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

                  

				  
drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP as 
select * from 
(select RECORD_ID,ARI_REC_ID,LIFETHREATENING,LIFETHREATENING_NF,HOSPITALIZATION,HOSPITALIZATION_NF
                      ,CONGENITALANOMALY_NF,DISABILITY,DISABILITY_NF,SERIOUSNESS,REACTSTARTDATE,REACTSTARTDATEFMT,
                      DRUG_INTERACTION,HOSP_PROLONGED,DEATH,DEATH_NF,CONGENITALANOMALY,INTERVENTIONREQUIRED,INTERVENTIONREQUIRED_NF,
					  NONSERIOUS,NONSERIOUS_NF,REACTMEDDRALLT_CODE,REACTMEDDRAPT_CODE,REACTIONTERM,  CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
											 
										)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
;




drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP as 
select record_id,ARI_REC_ID,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID from 
			(select record_id,ARI_REC_ID,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;


drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP as 
select record_id,ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATE,DRUGENDDATEFMT from 
			(select record_id,ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATE,DRUGENDDATEFMT,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;





--- DER_MED_DEVICE_PROB_CONC_ANNEX_E


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_E=LS_DB_CASE_REACT_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_E
FROM (
SELECT ARI_REC_ID,SEQ_REACT,
LISTAGG( EVALUATION_VALUE_ANNEX_E,' | \n')within group (order by SEQ_EVALUATION) AS DER_MED_DEVICE_PROB_CONC_ANNEX_E
FROM
(
SELECT 
LSMV_REACTION_SUBSET.ARI_REC_ID,LSMV_REACTION_SUBSET.SEQ_REACT, SEQ_EVALUATION,
D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')' AS EVALUATION_VALUE_ANNEX_E
FROM	
(select SEQ_REACT,ARI_REC_ID FROM
(
	select RECORD_ID As SEQ_REACT,ARI_REC_ID,CDC_OPERATION_TYPE,EVENT_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
			-- where ARI_REC_ID in (select distinct ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			
 )APD where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') AND EVENT_TYPE IN ('2','3') and ARI_REC_ID is not null
)  LSMV_REACTION_SUBSET JOIN

(select ARI_REC_ID,IMRDF_EVALUATION_TYPE,IMRDF_EVALUATION_CODES,SEQ_EVALUATION,FK_AD_REC_ID,SPR_ID FROM
(
	select 	ARI_REC_ID, 	IMRDF_TYPE AS IMRDF_EVALUATION_TYPE,	
	IMRDF_CODES AS IMRDF_EVALUATION_CODES,RECORD_ID AS SEQ_EVALUATION,FK_AD_REC_ID,
	CASE WHEN COALESCE(SPR_ID,'')='' then '-9999' else SPR_ID END AS SPR_ID,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION
			where ARI_REC_ID in (select distinct ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			
 )APD where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U')
 )LSMV_IMRDF_EVALUATION_SUBSET ON LSMV_REACTION_SUBSET.ARI_REC_ID=LSMV_IMRDF_EVALUATION_SUBSET.ARI_REC_ID 
 AND LSMV_REACTION_SUBSET.SEQ_REACT=LSMV_IMRDF_EVALUATION_SUBSET.FK_AD_REC_ID
 JOIN  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D
  ON LSMV_IMRDF_EVALUATION_SUBSET.IMRDF_EVALUATION_CODES =D.CODE AND LSMV_IMRDF_EVALUATION_SUBSET.SPR_ID=D.SPR_ID
  where  D.CODELIST_ID             =9996
  AND IMRDF_EVALUATION_TYPE ='01'
GROUP BY  LSMV_REACTION_SUBSET.ARI_REC_ID,LSMV_REACTION_SUBSET.SEQ_REACT,SEQ_EVALUATION,D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')'
  )
GROUP BY
ARI_REC_ID,SEQ_REACT
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
  
  
--  DER_MED_DEVICE_PROB_CONC_ANNEX_F

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_F=LS_DB_CASE_REACT_DER_FINAL.DER_MED_DEVICE_PROB_CONC_ANNEX_F
FROM (

SELECT ARI_REC_ID,SEQ_REACT,
LISTAGG( EVALUATION_VALUE_ANNEX_F,' | \n')within group (order by SEQ_EVALUATION) AS DER_MED_DEVICE_PROB_CONC_ANNEX_F
FROM
(
SELECT 
LSMV_REACTION_SUBSET.ARI_REC_ID,LSMV_REACTION_SUBSET.SEQ_REACT, SEQ_EVALUATION,
D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')' AS EVALUATION_VALUE_ANNEX_F
FROM	
(select SEQ_REACT,ARI_REC_ID FROM
(
	select RECORD_ID As SEQ_REACT,ARI_REC_ID,CDC_OPERATION_TYPE,EVENT_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			where ARI_REC_ID in (select distinct ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
			
 )APD where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U') AND EVENT_TYPE IN ('2','3')
)LSMV_REACTION_SUBSET JOIN

(select ARI_REC_ID,IMRDF_EVALUATION_TYPE,IMRDF_EVALUATION_CODES,SEQ_EVALUATION,FK_AD_REC_ID,SPR_ID FROM
(
	select 	ARI_REC_ID, 	IMRDF_TYPE AS IMRDF_EVALUATION_TYPE,	
	IMRDF_CODES AS IMRDF_EVALUATION_CODES,RECORD_ID AS SEQ_EVALUATION,FK_AD_REC_ID,
	CASE WHEN COALESCE(SPR_ID,'')='' then '-9999' else SPR_ID END AS SPR_ID,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_IMRDF_EVALUATION
			where ARI_REC_ID in (select distinct ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
			
 )APD where APD.rank=1 AND APD.CDC_OPERATION_TYPE IN ('I','U')
 )LSMV_IMRDF_EVALUATION_SUBSET  ON LSMV_REACTION_SUBSET.ARI_REC_ID=LSMV_IMRDF_EVALUATION_SUBSET.ARI_REC_ID
 AND LSMV_REACTION_SUBSET.SEQ_REACT=LSMV_IMRDF_EVALUATION_SUBSET.FK_AD_REC_ID
 JOIN  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D
  ON LSMV_IMRDF_EVALUATION_SUBSET.IMRDF_EVALUATION_CODES =D.CODE AND LSMV_IMRDF_EVALUATION_SUBSET.SPR_ID=D.SPR_ID
  where  D.CODELIST_ID             =9997
  AND IMRDF_EVALUATION_TYPE ='02'
GROUP BY  LSMV_REACTION_SUBSET.ARI_REC_ID,LSMV_REACTION_SUBSET.SEQ_REACT,SEQ_EVALUATION,D.DECODE || '(' || D.CODE || ')' || '(' || D.EMDR_CODE || ')'
  )
GROUP BY
ARI_REC_ID,SEQ_REACT


) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_EVENT=case when LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_EVENT is not null 
     then case when length(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_EVENT)>=4000 
               then substring(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_EVENT,0,3996)||' ...' else LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_EVENT end
	 else null end 
FROM (
SELECT ARI_REC_ID ,  SEQ_REACT 
  ,  LISTAGG(DER_TIME_OFF_DRUG_EVENT,'\r\n')within group (order by AUTO_RANK)  AS DER_TIME_OFF_DRUG_EVENT
 from (
SELECT R.ARI_REC_ID ,
   R.RECORD_ID AS SEQ_REACT ,
  T.AUTO_RANK,
  'D'|| T.AUTO_RANK || ': ' || NVL(CASE
      WHEN R.REACTENDDATE  IS NULL
      OR T.MAX_END_THERAPY_DATE IS NULL
      THEN NULL
      	WHEN R.REACTENDDATEFMT  IN (610,602)
      THEN NULL
      WHEN datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTENDDATE)) < 0
	  THEN ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTENDDATE)) as  VARCHAR), 1,5))
      ELSE ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTENDDATE))as VARCHAR ), 1,4))
    END,'-') as DER_TIME_OFF_DRUG_EVENT
  FROM ( select ARI_REC_ID,REACTENDDATE,REACTENDDATEFMT,RECORD_ID
			
		     FROM
		   (SELECT ARI_REC_ID,REACTENDDATE,REACTENDDATEFMT,RECORD_ID ,CDC_OPERATION_TYPE
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
				
				) LSMV_REACTION_SUBSET		
				WHERE  rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		 ) R,
    (SELECT A.ARI_REC_ID ,
      A.SEQ_PRODUCT,
      MAX(B.END_THERAPY_DATE) MAX_END_THERAPY_DATE,
      A.AUTO_RANK
    FROM
      (SELECT ARI_REC_ID,
        SEQ_PRODUCT,
        SEQ_THERAPY,
        END_THERAPY_DATE
      FROM 
        ( select ARI_REC_ID,SEQ_PRODUCT,DRUGENDDATE END_THERAPY_DATE,RECORD_ID SEQ_THERAPY
			,Row_NUMBER() over(Partition by ARI_REC_ID,SEQ_PRODUCT  order by RECORD_ID desc) SEQ_order  
		     FROM
		   (SELECT ARI_REC_ID,FK_AD_REC_ID  SEQ_PRODUCT,RECORD_ID ,DRUGENDDATE,DRUGENDDATEFMT,CDC_OPERATION_TYPE
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM
				${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
				
				) LSMV_DRUG_THERAPY_SUBSET		
				WHERE DRUGENDDATEFMT NOT IN (610,602) and rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		 ) APTS WHERE SEQ_order=1
		
      ) B , 
		(
			select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER As AUTO_RANK,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			
		) A
		
         WHERE 

		 A.ARI_REC_ID=B.ARI_REC_ID (+)
		AND A.SEQ_PRODUCT=B.SEQ_PRODUCT (+)
		AND A.DRUGCHARACTERIZATION IN ('1','3')
		AND  A.rank=1 AND A.CDC_OPERATION_TYPE IN ('I','U') and A.ARI_REC_ID||'-'||A.SEQ_PRODUCT not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
		)
         GROUP BY A.ARI_REC_ID , A.SEQ_PRODUCT,A.AUTO_RANK 
    ) T WHERE T.ARI_REC_ID=R.ARI_REC_ID  AND  R.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
		
 
ORDER BY R.ARI_REC_ID, R.RECORD_ID,T.AUTO_RANK
)
GROUP BY ARI_REC_ID ,  SEQ_REACT
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID = LS_DB_CASE_REACT_DER_FINAL.ARI_REC_ID	
        AND LS_DB_CASE_REACT_DER_TMP.seq_react = LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_TIME_TO_ONSET_NORM=case when LS_DB_CASE_REACT_DER_FINAL.DER_TIME_TO_ONSET_NORM is not null 
     then case when length(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_TO_ONSET_NORM)>=4000 
               then substring(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_TO_ONSET_NORM,0,3996)||' ...' else LS_DB_CASE_REACT_DER_FINAL.DER_TIME_TO_ONSET_NORM end
	 else null end
FROM (SELECT  ARI_REC_ID , SEQ_REACT,LISTAGG( DER_TIME_TO_ONSET_NORM,'\r\n')within group (order by AUTO_RANK)   AS DER_TIME_TO_ONSET_NORM 

 FROM
	(
SELECT  ARI_REC_ID , SEQ_REACT,AUTO_RANK,'D' || AUTO_RANK  || ': ' || NVL(TIME_TO_ONSET,'-')  AS DER_TIME_TO_ONSET_NORM FROM
	(
		SELECT R.ARI_REC_ID , R.RECORD_ID AS SEQ_REACT ,T.SEQ_PRODUCT,R.REACTSTARTDATE as AE_ONSET_DATE,
		T.MIN_START_THERAPY_DATE ,
		CASE WHEN   R.REACTSTARTDATE IS NULL or T.MIN_START_THERAPY_DATE IS NULL THEN NULL
		WHEN   R.REACTSTARTDATEFMT  IN (610,602)  THEN NULL
		WHEN   datediff(DAY, date_trunc('DAY',T.MIN_START_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE )) < 0  
          THEN ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MIN_START_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE )) as VARCHAR), 1,5)) 
		ELSE ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MIN_START_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE )) as VARCHAR), 1,4))  
		END AS TIME_TO_ONSET, T.AUTO_RANK FROM 
        (select ARI_REC_ID,RECORD_ID,REACTSTARTDATE,REACTSTARTDATEFMT from 
		     (SELECT ARI_REC_ID,RECORD_ID,REACTSTARTDATE,REACTSTARTDATEFMT,CDC_OPERATION_TYPE
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
				
				) LSMV_REACTION		
				WHERE rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		)

		R, 
		(
		SELECT A.ARI_REC_ID , A.SEQ_PRODUCT,min(START_THERAPY_DATE) As MIN_START_THERAPY_DATE,A.AUTO_RANK
        
		FROM
		
		(select ARI_REC_ID,SEQ_PRODUCT,START_THERAPY_DATE from 
		     (SELECT ARI_REC_ID,FK_AD_REC_ID  SEQ_PRODUCT,DRUGSTARTDATE START_THERAPY_DATE,DRUGSTARTDATEFMT,CDC_OPERATION_TYPE
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM
				${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
				
				) LSMV_DRUG_THERAPY_SUBSET		
				WHERE DRUGSTARTDATEFMT NOT IN (610,602) and rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		) B , 
		(
			select RECORD_ID As SEQ_PRODUCT,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER As AUTO_RANK,CDC_OPERATION_TYPE,
			row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
			where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
			
		) A
		
         WHERE 

		 A.ARI_REC_ID=B.ARI_REC_ID (+)
		AND A.SEQ_PRODUCT=B.SEQ_PRODUCT (+)
		AND A.DRUGCHARACTERIZATION IN ('1','3')
		AND  A.rank=1 
		AND A.CDC_OPERATION_TYPE IN ('I','U')
		and A.ARI_REC_ID||'-'||A.SEQ_PRODUCT not in 
		(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
		)
         GROUP BY A.ARI_REC_ID , A.SEQ_PRODUCT,A.AUTO_RANK ) T
         WHERE T.ARI_REC_ID=R.ARI_REC_ID  AND  R.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
		
         
	)
ORDER BY ARI_REC_ID, AUTO_RANK
)
GROUP BY ARI_REC_ID , SEQ_REACT
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_NORMAL=LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_NORMAL   

FROM (SELECT distinct ARI_REC_ID,SEQ_REACT,
LISTAGG(DER_TIME_OFF_DRUG_NORM,'\r\n')within group (ORDER BY AUTO_RANK) AS DER_TIME_OFF_DRUG_NORMAL

FROM (
SELECT R.ARI_REC_ID ,
	  R.RECORD_ID AS SEQ_REACT,
	  T.AUTO_RANK,
	  'D'
	  || T.AUTO_RANK
	  || ': '
	  ||NVL(CASE
        WHEN R.REACTSTARTDATE      IS NULL
        OR T.MAX_END_THERAPY_DATE IS NULL
        THEN NULL
        WHEN R.REACTSTARTDATEFMT IN ( 610,602)
        THEN NULL
        WHEN datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) < 0
        THEN ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) AS VARCHAR), 1,5))
        ELSE ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) AS VARCHAR), 1,4))
      END,'-') AS DER_TIME_OFF_DRUG_NORM
		FROM (select RECORD_ID,ARI_REC_ID,REACTSTARTDATE,REACTSTARTDATEFMT,CDC_OPERATION_TYPE
				,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
											 
										) R,
		  (SELECT A.ARI_REC_ID ,
			A.RECORD_ID,A.RANK_ORDER as AUTO_RANK,
			MAX(B.END_THERAPY_DATE) MAX_END_THERAPY_DATE
			
		  FROM
			(SELECT APT.ARI_REC_ID,
			  APT.SEQ_PRODUCT,
			  APTS.RECORD_ID AS SEQ_THERAPY,
			  APTS.DRUGENDDATE AS END_THERAPY_DATE
			FROM 
				(select ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATEFMT,DRUGENDDATE,RECORD_ID,CDC_OPERATION_TYPE
					,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
					where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
					
				) APTS,
			  (SELECT ARI_REC_ID,
				FK_AD_REC_ID AS SEQ_PRODUCT,
				MAX(RECORD_ID) SEQ_THERAPY
			  FROM
				(select ARI_REC_ID,FK_AD_REC_ID,DRUGENDDATEFMT,RECORD_ID,CDC_OPERATION_TYPE
					,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
					where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
					
				) Where Rank=1	AND CDC_OPERATION_TYPE IN ('I','U')
				and DRUGENDDATEFMT NOT IN ( 610,602)
			  GROUP BY 1,2
			  ) APT
			WHERE APTS.rank=1 AND APTS.CDC_OPERATION_TYPE IN ('I','U') and APTS.ARI_REC_ID   =APT.ARI_REC_ID
			AND APTS.FK_AD_REC_ID=APT.SEQ_PRODUCT
			AND APTS.RECORD_ID=APT.SEQ_THERAPY
			
			) B ,
			(select RECORD_ID,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER,CDC_OPERATION_TYPE,
				row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
											 
										) A 
		  WHERE A.ARI_REC_ID      =B.ARI_REC_ID (+)
		  AND A.RECORD_ID   =B.SEQ_PRODUCT (+)
		  AND A.DRUGCHARACTERIZATION  IN ('1','3')
		  AND A.rank=1 AND A.CDC_OPERATION_TYPE IN ('I','U') AND A.CDC_OPERATION_TYPE IN ('I','U')
		  AND A.ARI_REC_ID||'-'||A.RECORD_ID NOT IN
			(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
		)
			
		  GROUP BY A.ARI_REC_ID,
		  A.RECORD_ID,
		  A.RANK_ORDER
		  ) T
		WHERE  T.ARI_REC_ID      =R.ARI_REC_ID and R.rank=1 AND R.CDC_OPERATION_TYPE IN ('I','U')
		
	ORDER BY R.ARI_REC_ID,
	  T.AUTO_RANK
	  )
	  
GROUP BY ARI_REC_ID,SEQ_REACT
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

                              
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_DAYS=LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_DAYS
FROM (


SELECT  ARI_REC_ID , RECORD_ID
         ,LISTAGG('D' || AUTO_RANK  || ': ' ||DER_TIME_OFF_DRUG_DAYS,'\r\n')within group (order by AUTO_RANK) AS DER_TIME_OFF_DRUG_DAYS 
FROM
(

   SELECT  R.ARI_REC_ID , R.RECORD_ID,
         NVL(CASE WHEN   R.REACTSTARTDATE IS NULL or T.MIN_END_THERAPY_DATE IS NULL THEN NULL
				WHEN   R.REACTSTARTDATEFMT  IN ( 610,602)  THEN NULL
        WHEN   datediff(DAY, date_trunc('DAY',R.REACTSTARTDATE ), date_trunc('DAY',T.MIN_END_THERAPY_DATE)) < 0  
          THEN ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',R.REACTSTARTDATE ), date_trunc('DAY',T.MIN_END_THERAPY_DATE)) as VARCHAR), 1,5)) 
		ELSE ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',R.REACTSTARTDATE ), date_trunc('DAY',T.MIN_END_THERAPY_DATE))  as VARCHAR), 1,4))  
		END,'-') AS DER_TIME_OFF_DRUG_DAYS 
		,T.AUTO_RANK AS AUTO_RANK
	
		FROM (select RECORD_ID,ARI_REC_ID,REACTSTARTDATE,REACTSTARTDATEFMT
				FROM
					(select RECORD_ID,ARI_REC_ID,REACTSTARTDATE,REACTSTARTDATEFMT,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
											 
										)		where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')						
											
											
			 ) R, 
		(
		SELECT A.ARI_REC_ID , A.RECORD_ID,MIN(B.END_THERAPY_DATE) MIN_END_THERAPY_DATE,A.AUTO_RANK FROM 
		(select ARI_REC_ID,SEQ_PRODUCT,DRUGENDDATE As END_THERAPY_DATE FROM
				(
					SELECT ARI_REC_ID,FK_AD_REC_ID AS SEQ_PRODUCT,DRUGENDDATE,DRUGENDDATEFMT,CDC_OPERATION_TYPE
							,ROW_NUMBER () over(PARTITION BY ARI_REC_ID,FK_AD_REC_ID order by CDC_OPERATION_TIME desc) rank 
						FROM
						${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY  where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
				) As LSMV_DRUG_THERAPY	WHERE rank=1 and DRUGENDDATEFMT NOT IN ( 610,602)  AND CDC_OPERATION_TYPE IN ('I','U')
		) B , 
		(select RECORD_ID,ARI_REC_ID,DRUGCHARACTERIZATION,RANK_ORDER As AUTO_RANK,CDC_OPERATION_TYPE
			,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
		where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
		) A 
		WHERE A.ARI_REC_ID=B.ARI_REC_ID (+)
		AND A.RECORD_ID=B.SEQ_PRODUCT (+)
		AND A.DRUGCHARACTERIZATION IN ('1','3')
		AND A.RANK=1  AND A.CDC_OPERATION_TYPE IN ('I','U')
		
		AND  A.ARI_REC_ID||'-'||A.RECORD_ID  NOT  IN
					(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null)
			
         GROUP BY A.ARI_REC_ID , A.RECORD_ID,A.AUTO_RANK
		 ) T
         WHERE T.ARI_REC_ID=R.ARI_REC_ID
         
	) final_out 		 
group by  ARI_REC_ID , RECORD_ID ) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.RECORD_ID
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_SER_CRITERIA_BRAZIL_CODE=LS_DB_CASE_REACT_DER_FINAL.DER_SER_CRITERIA_BRAZIL_CODE
FROM (
select 
ARI_REC_ID , REACT_RECORD_ID ,
case 
when der is not null then der
when der is null and SERIOUSNESS ='1' then 6
else null end as DER_SER_CRITERIA_BRAZIL_CODE
from
(
SELECT ARI_REC_ID,
       REACT_RECORD_ID,
       SERIOUSNESS,
       CASE
         WHEN DEATH = '1' THEN 1
         WHEN LIFE_THREATENING = '1' THEN 2
         WHEN HOSP_REQUIRED = '1' THEN 3
         WHEN CONGENITAL_ANOMALY = '1' THEN 4
         WHEN DISABILITY = '1' THEN 5
       END AS der
FROM ( SELECT A.ARI_REC_ID AS ARI_REC_ID,
             CAST(A.RECORD_ID AS VARCHAR(255)) AS REACT_RECORD_ID,
             A.RECORD_ID AS SEQ_AE_CHAR,
             CASE
               WHEN CONGENITALANOMALY IS NULL THEN CONGENITALANOMALY_NF
               ELSE CONGENITALANOMALY
             END AS CONGENITAL_ANOMALY,
             2 AS DATA_LEVEL,
             CASE
               WHEN DEATH IS NULL THEN DEATH_NF
               ELSE DEATH
             END AS DEATH,
             CASE
               WHEN DISABILITY IS NULL THEN DISABILITY_NF
               ELSE DISABILITY
             END AS DISABILITY,
             DRUG_INTERACTION AS DRUG_INTERACTION,
             HOSP_PROLONGED AS HOSP_PROLONGED,
             CASE
               WHEN HOSPITALIZATION IS NULL THEN HOSPITALIZATION_NF
               ELSE HOSPITALIZATION
             END AS HOSP_REQUIRED, 
             CASE
               WHEN LIFETHREATENING IS NULL THEN LIFETHREATENING_NF
               ELSE LIFETHREATENING
             END AS LIFE_THREATENING,
             A.RECORD_ID AS RECORD_ID,
             SERIOUSNESS AS SERIOUSNESS
      FROM 		(select RECORD_ID,ARI_REC_ID,LIFETHREATENING,LIFETHREATENING_NF,HOSPITALIZATION,HOSPITALIZATION_NF
                      ,CONGENITALANOMALY_NF,DISABILITY,DISABILITY_NF,SERIOUSNESS,
                      DRUG_INTERACTION,HOSP_PROLONGED,DEATH,DEATH_NF,CONGENITALANOMALY,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
											 
										)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')						
											
			
		) TMP
)
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.REACT_RECORD_ID
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_NORM=CASE
        WHEN LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_NORM IS NOT NULL THEN 
            SUBSTRING(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_NORM, 1, 3996) ||
            CASE WHEN LENGTH(LS_DB_CASE_REACT_DER_FINAL.DER_TIME_OFF_DRUG_NORM) > 4000 THEN ' ...' ELSE '' END
        ELSE '(-)'
    END
FROM (


  SELECT ARI_REC_ID,SEQ_REACT,LISTAGG(DER_TIME_OFF_DRUG_NORM,'\r\n')within group (ORDER BY RANK_ORDER) AS DER_TIME_OFF_DRUG_NORM

FROM (
SELECT R.ARI_REC_ID ,
	  R.record_id as SEQ_REACT,
	  T.RANK_ORDER,
	  'D'
	  || T.RANK_ORDER
	  || ': '
	  ||NVL(CASE
        WHEN R.REACTSTARTDATE      IS NULL
        OR T.MAX_END_THERAPY_DATE IS NULL
        THEN NULL
        WHEN R.REACTSTARTDATEFMT IN ( 610,602)
        THEN NULL
        WHEN datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) < 0
        THEN ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) AS VARCHAR), 1,5))
        ELSE ( SUBSTRING(CAST( datediff(DAY, date_trunc('DAY',T.MAX_END_THERAPY_DATE), date_trunc('DAY',R.REACTSTARTDATE)) AS VARCHAR), 1,4))
      END,'-') AS DER_TIME_OFF_DRUG_NORM

		FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP R,
		  (SELECT A.ARI_REC_ID ,
			A.record_id,
			MAX(B.END_THERAPY_DATE) MAX_END_THERAPY_DATE,
			A.RANK_ORDER
		  FROM
			(SELECT APT.ARI_REC_ID,
			  APT.SEQ_PRODUCT,
			  APTS.RECORD_ID AS SEQ_THERAPY,
			  APTS.DRUGENDDATE as END_THERAPY_DATE
			FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP APTS,
			  (     SELECT
                        ARI_REC_ID AS ARI_REC_ID,
                        FK_AD_REC_ID AS SEQ_PRODUCT,
                        MAX(RECORD_ID) AS SEQ_THERAPY
                    FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_THERAPY_SUBSET_TMP
                    WHERE DRUGENDDATEFMT NOT IN (610, 602)
                    GROUP BY ARI_REC_ID, SEQ_PRODUCT
			  ) APT
			WHERE APTS.ARI_REC_ID   =APT.ARI_REC_ID
			AND APTS.FK_AD_REC_ID=APT.SEQ_PRODUCT
			AND APTS.RECORD_ID=APT.SEQ_THERAPY
			) B ,
			${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP A WHERE
		   A.ARI_REC_ID      =B.ARI_REC_ID (+)
		  AND A.record_id   =B.SEQ_PRODUCT (+)
		  AND A.DRUGCHARACTERIZATION  IN ('1','3')
		  AND A.ARI_REC_ID||'-'||A.record_id NOT IN
			(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
			)
		  GROUP BY A.ARI_REC_ID,
		  A.record_id,
		  A.RANK_ORDER
		  ) T
		WHERE  T.ARI_REC_ID      =R.ARI_REC_ID
	ORDER BY R.ARI_REC_ID,
	  T.RANK_ORDER
	  )
	  
GROUP BY ARI_REC_ID,SEQ_REACT) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_EVENT_SERIOUSNESS_CRITERIA=LS_DB_CASE_REACT_DER_FINAL.DER_EVENT_SERIOUSNESS_CRITERIA
from (
SELECT  A.ARI_REC_ID,
       A.record_id SEQ_REACT,      
       RTRIM( NVL(case when COALESCE(CONGENITALANOMALY,CONGENITALANOMALY_NF,'')='1' then 'C|' end,'')||
					NVL( CASE WHEN COALESCE(DEATH,DEATH_NF,'')='1' THEN 'F|' END,'')||
					NVL( CASE WHEN COALESCE(DISABILITY,DISABILITY_NF,'')='1' THEN 'D|' END,'')||
					NVL( case when COALESCE(HOSPITALIZATION,HOSPITALIZATION_NF,'')='1' then 'H|' end,'')||
					NVL(case when COALESCE(LIFETHREATENING,LIFETHREATENING_NF,'')='1' then 'L|' end,'')||
					NVL(case when COALESCE(NONSERIOUS,NONSERIOUS_NF,'')='1' then 'O|' end,'')||
					NVL(case when COALESCE(INTERVENTIONREQUIRED,INTERVENTIONREQUIRED_NF,'')='1' then 'I' end,''),'|') DER_EVENT_SERIOUSNESS_CRITERIA
			FROM lsmv_reaction_SUBSET_TMP A

) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_REACT_DER_FINAL.ari_rec_id	
	and LS_DB_CASE_REACT_DER_TMP.SEQ_REACT=LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_COMP_CAUS_COMB=case when LS_DB_CASE_REACT_DER_TMP_TMP.DER_COMP_CAUS_COMB is not null 
     then case when length(LS_DB_CASE_REACT_DER_TMP_TMP.DER_COMP_CAUS_COMB)>=4000 
               then substring(LS_DB_CASE_REACT_DER_TMP_TMP.DER_COMP_CAUS_COMB,0,3996)||' ...' else LS_DB_CASE_REACT_DER_TMP_TMP.DER_COMP_CAUS_COMB end
	 else null end
FROM (


SELECT ARI_REC_ID,LISTAGG(DER_COMP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,class desc)  DER_COMP_CAUS_COMB,seq_react
from
(
select ARI_REC_ID,case when class='BN' then '@\n'||listagg(DER_COMP_CAUS_COMB,'|') within group (order by ARI_REC_ID,AUTO_RANK,class)
else listagg(DER_COMP_CAUS_COMB,'|') within group (order by ARI_REC_ID,AUTO_RANK,class) end DER_COMP_CAUS_COMB,
seq_react,class from 

(

SELECT ARI_REC_ID,
  seq_react,listagg(DER_COMP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,AUTO_RANK) DER_COMP_CAUS_COMB,
  'UN' class,
  AUTO_RANK
FROM
(
SELECT  APR.ARI_REC_ID, 
   APR.seq_react,
  'D'
  ||AUTO_RANK
  ||': '
  ||nvl(DECODE,' -') As DER_COMP_CAUS_COMB,
  AUTO_RANK
FROM
  (SELECT AP.ARI_REC_ID ,
    AP.record_id as SEQ_PRODUCT,
    AR.RECORD_ID as SEQ_REACT,
    AP.RANK_ORDER AUTO_RANK
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP AP ,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP AR
  WHERE AP.ARI_REC_ID       =AR.ARI_REC_ID(+)
 AND AP.DRUGCHARACTERIZATION   IN ('1','3')
    and AP.ARI_REC_ID||'-'||AP.record_id not in 
		( select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
		)
   ) APR,
   (
    
	select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from
	(
	SELECT ARI_REC_ID,
		FK_AR_REC_ID as SEQ_REACT,
		FK_DRUG_REC_ID AS SEQ_PRODUCT ,
		COMPANY_CAUSALITY,
		Coalesce(SPR_ID,'-9999') SPR_ID,
		CDC_OPERATION_TYPE,
		row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	from ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS
	where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA Join
	(select CODE,DECODE,SPR_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where codelist_id IN ('9062')
					) D ON  APAA.COMPANY_CAUSALITY =D.CODE AND APAA.SPR_ID =D.SPR_ID
					where APAA.rank=1 AND APAA.CDC_OPERATION_TYPE IN ('I','U') 
  UNION
    select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from 
	(SELECT ARI_REC_ID,
    FK_AR_REC_ID SEQ_REACT,
    FK_DRUG_REC_ID AS SEQ_PRODUCT ,
    COMPANY_CAUSALITY_FT as decode,
	CDC_OPERATION_TYPE,
	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS where COMPANY_CAUSALITY_FT is not null
    and ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA where rank=1 AND APAA.CDC_OPERATION_TYPE IN ('I','U') 
	
  ) CAUS where  APR.ARI_REC_ID   =CAUS.ARI_REC_ID(+)
AND APR.SEQ_PRODUCT=CAUS.SEQ_PRODUCT(+)
AND APR.SEQ_REACT  =CAUS.SEQ_REACT(+)
) final_output
group by ARI_REC_ID,SEQ_REACT,AUTO_RANK

union 

SELECT ARI_REC_ID,
  seq_react,listagg(DER_COMP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,AUTO_RANK) DER_COMP_CAUS_COMB,
  'BN' class,
  AUTO_RANK
FROM
(
SELECT  APR.ARI_REC_ID, 
   APR.seq_react,
  'D'
  ||AUTO_RANK
  ||': '
  ||nvl(DECODE,' -') As DER_COMP_CAUS_COMB,
  AUTO_RANK
FROM
  (SELECT AP.ARI_REC_ID ,
    AP.record_id as SEQ_PRODUCT,
    AR.RECORD_ID as SEQ_REACT,
    AP.RANK_ORDER as AUTO_RANK
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP AP ,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP AR
  WHERE AP.ARI_REC_ID       =AR.ARI_REC_ID(+)
 AND AP.DRUGCHARACTERIZATION   IN ('1','3')
    and AP.ARI_REC_ID||'-'||AP.record_id  in 
		( select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is  null
		)
   ) APR,
   (
    
	select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from
	(
	SELECT ARI_REC_ID,
		FK_AR_REC_ID as SEQ_REACT,
		FK_DRUG_REC_ID AS SEQ_PRODUCT ,
		COMPANY_CAUSALITY,
		Coalesce(SPR_ID,'-9999') SPR_ID,
		CDC_OPERATION_TYPE,
		row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	from ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS
	where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA Join
	(select CODE,DECODE,SPR_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where codelist_id IN ('9062')
					) D ON  APAA.COMPANY_CAUSALITY =D.CODE AND APAA.SPR_ID =D.SPR_ID
					where APAA.rank=1 AND APAA.CDC_OPERATION_TYPE IN ('I','U') 
  UNION
    select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from 
	(SELECT ARI_REC_ID,
    FK_AR_REC_ID SEQ_REACT,
    FK_DRUG_REC_ID AS SEQ_PRODUCT ,
    COMPANY_CAUSALITY_FT as decode,
	CDC_OPERATION_TYPE,
	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS where COMPANY_CAUSALITY_FT is not null
    and ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA where rank=1 AND APAA.CDC_OPERATION_TYPE IN ('I','U') 
	
  ) CAUS where  APR.ARI_REC_ID   =CAUS.ARI_REC_ID(+)
AND APR.SEQ_PRODUCT=CAUS.SEQ_PRODUCT(+)
AND APR.SEQ_REACT  =CAUS.SEQ_REACT(+)
) final_output
group by ARI_REC_ID,SEQ_REACT,AUTO_RANK
)
  group by ARI_REC_ID,class , seq_react
  order by  ARI_REC_ID,class desc
  )
  group by ARI_REC_ID , seq_react


) LS_DB_CASE_REACT_DER_TMP_TMP
    WHERE LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID = LS_DB_CASE_REACT_DER_TMP_TMP.ARI_REC_ID	
     AND LS_DB_CASE_REACT_DER_TMP.SEQ_REACT = LS_DB_CASE_REACT_DER_TMP_TMP.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_REP_CAUS_COMB=case when LS_DB_CASE_REACT_DER_TMP_TMP.DER_REP_CAUS_COMB is not null 
                                   then case when length(LS_DB_CASE_REACT_DER_TMP_TMP.DER_REP_CAUS_COMB)>=4000 
                                  then substring(LS_DB_CASE_REACT_DER_TMP_TMP.DER_REP_CAUS_COMB,0,3996)||' ...' else LS_DB_CASE_REACT_DER_TMP_TMP.DER_REP_CAUS_COMB end
                                   else null end
FROM 
(


SELECT ARI_REC_ID,LISTAGG(DER_REP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,class desc)  DER_REP_CAUS_COMB,seq_react
from
(
select ARI_REC_ID,case when class='BN' then '@\n'||listagg(DER_REP_CAUS_COMB,'|') within group (order by ARI_REC_ID,AUTO_RANK,class)
else listagg(DER_REP_CAUS_COMB,'|') within group (order by ARI_REC_ID,AUTO_RANK,class) end DER_REP_CAUS_COMB,
seq_react,class from 

(
SELECT ARI_REC_ID,
  seq_react,listagg(DER_REP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,AUTO_RANK) DER_REP_CAUS_COMB  ,
  'UN' class,
  AUTO_RANK
FROM
(
SELECT  APR.ARI_REC_ID, 
   APR.seq_react,
  'D'
  ||AUTO_RANK
  ||': '
  ||nvl(DECODE,' -') As DER_REP_CAUS_COMB,
  AUTO_RANK
FROM
  (SELECT AP.ARI_REC_ID ,
    AP.record_id SEQ_PRODUCT,
    AR.record_id SEQ_REACT,
    AP.RANK_ORDER AUTO_RANK
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP AP ,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP AR
  WHERE AP.ARI_REC_ID       =AR.ARI_REC_ID(+)
 AND AP.DRUGCHARACTERIZATION   IN ('1','3')
  and AP.ARI_REC_ID||'-'||AP.record_id not in 
		(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null
		)
   ) APR,  
  (
    
	select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	FROM
	(
	SELECT ARI_REC_ID,
		FK_AR_REC_ID as SEQ_REACT,
		FK_DRUG_REC_ID AS SEQ_PRODUCT ,
		REPORTER_CAUSALITY,
		Coalesce(SPR_ID,'-9999') SPR_ID,CDC_OPERATION_TYPE,
		row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	from ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS
	where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA Join
	(select CODE,DECODE,SPR_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where codelist_id IN ('9062')
					) D ON  APAA.REPORTER_CAUSALITY =D.CODE AND APAA.SPR_ID =D.SPR_ID
					where rank=1 and APAA.CDC_OPERATION_TYPE IN ('I','U')
  UNION
    select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from 
	(SELECT ARI_REC_ID,
    FK_AR_REC_ID SEQ_REACT,
    FK_DRUG_REC_ID AS SEQ_PRODUCT ,
    REPORTER_CAUSALITY_FT as decode,
	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank ,
	CDC_OPERATION_TYPE
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS where REPORTER_CAUSALITY_FT is not null
    and ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA where rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
	
  ) CAUS where  APR.ARI_REC_ID   =CAUS.ARI_REC_ID(+)
AND APR.SEQ_PRODUCT=CAUS.SEQ_PRODUCT(+)
AND APR.SEQ_REACT  =CAUS.SEQ_REACT(+)
) final_output
group by ARI_REC_ID,SEQ_REACT,AUTO_RANK

UNION 

SELECT ARI_REC_ID,
  seq_react,listagg(DER_REP_CAUS_COMB,'\n') within group (order by ARI_REC_ID,AUTO_RANK) DER_REP_CAUS_COMB  ,
  'BN' class,
  AUTO_RANK
FROM
(
SELECT  APR.ARI_REC_ID, 
   APR.seq_react,
  'D'
  ||AUTO_RANK
  ||': '
  ||nvl(DECODE,' -') As DER_REP_CAUS_COMB,
  AUTO_RANK
FROM
  (SELECT AP.ARI_REC_ID ,
    AP.record_id SEQ_PRODUCT,
    AR.record_id SEQ_REACT,
    AP.RANK_ORDER AUTO_RANK
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP AP ,
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP AR
  WHERE AP.ARI_REC_ID       =AR.ARI_REC_ID(+)
 AND AP.DRUGCHARACTERIZATION   IN ('1','3')
  and AP.ARI_REC_ID||'-'||AP.record_id  in 
		(select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is  null
		)
   ) APR,  
  (
    
	select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	FROM
	(
	SELECT ARI_REC_ID,
		FK_AR_REC_ID as SEQ_REACT,
		FK_DRUG_REC_ID AS SEQ_PRODUCT ,
		REPORTER_CAUSALITY,
		Coalesce(SPR_ID,'-9999') SPR_ID,CDC_OPERATION_TYPE,
		row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
	from ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS
	where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA Join
	(select CODE,DECODE,SPR_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp where codelist_id IN ('9062')
					) D ON  APAA.REPORTER_CAUSALITY =D.CODE AND APAA.SPR_ID =D.SPR_ID
					where rank=1 and APAA.CDC_OPERATION_TYPE IN ('I','U')
  UNION
    select ARI_REC_ID,SEQ_REACT,SEQ_PRODUCT,decode
	from 
	(SELECT ARI_REC_ID,
    FK_AR_REC_ID SEQ_REACT,
    FK_DRUG_REC_ID AS SEQ_PRODUCT ,
    REPORTER_CAUSALITY_FT as decode,
	row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank ,
	CDC_OPERATION_TYPE
	FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS where REPORTER_CAUSALITY_FT is not null
    and ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP) 
	) APAA where rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
	
  ) CAUS where  APR.ARI_REC_ID   =CAUS.ARI_REC_ID(+)
AND APR.SEQ_PRODUCT=CAUS.SEQ_PRODUCT(+)
AND APR.SEQ_REACT  =CAUS.SEQ_REACT(+)
) final_output
group by ARI_REC_ID,SEQ_REACT,AUTO_RANK
)
  group by ARI_REC_ID,class , seq_react
  order by  ARI_REC_ID,class desc
  )
  group by ARI_REC_ID , seq_react



) LS_DB_CASE_REACT_DER_TMP_TMP
    WHERE LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID = LS_DB_CASE_REACT_DER_TMP_TMP.ARI_REC_ID	
        AND LS_DB_CASE_REACT_DER_TMP.SEQ_REACT = LS_DB_CASE_REACT_DER_TMP_TMP.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

   --                                select ARI_REC_ID,SEQ_REACT,DER_VERBATERMS_PT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER where ari_rec_id=196376 and DER_VERBATERMS_PT is not null;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP   
SET LS_DB_CASE_REACT_DER_TMP.DER_VERBATERMS_PT=LS_DB_CASE_REACT_DER_final.DER_VERBATERMS_PT,
LS_DB_CASE_REACT_DER_TMP.DER_LLT_PT=LS_DB_CASE_REACT_DER_final.DER_LLT_PT

from ( select 
	AR.ARI_REC_ID,
	AR.record_id as SEQ_REACT,
	COALESCE(AR.REACTIONTERM,'-') ||' ('|| COALESCE(D.PT_NAME,'-') ||')' AS DER_VERBATERMS_PT,
	COALESCE(D.LLT_NAME,'-') ||' ('|| COALESCE(D.PT_NAME,'-') ||')' AS DER_LLT_PT
 FROM 
	 ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP AR,
	(
		SELECT distinct LLT_NAME,
		LLT_CODE,
		pt_name,
		PT_CODE
		FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD 
		where meddra_version in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')
		AND LS_DB_MEDDRA_ICD.language_wid= (SELECT SK_LANGUAGE_WID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE
		WHERE CODE =(SELECT LANGUAGE_CODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_LANGUAGE_PARAMETERS WHERE DEFAULT_LANGUAGE='Y'))
		AND UPPER(PRIMARY_SOC_FG)='Y'
	) D
    WHERE   COALESCE(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),-1)=COALESCE(D.PT_CODE(+),-1)
	AND COALESCE(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),COALESCE(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),-1))=COALESCE(D.LLT_CODE(+),-1)
) LS_DB_CASE_REACT_DER_FINAL
    WHERE LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID = LS_DB_CASE_REACT_DER_FINAL.ARI_REC_ID	
        AND LS_DB_CASE_REACT_DER_TMP.SEQ_REACT = LS_DB_CASE_REACT_DER_FINAL.SEQ_REACT
	AND LS_DB_CASE_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP)
where target_table_name='LS_DB_CASE_REACT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                  where target_table_name='LS_DB_CASE_REACT_DER'
					and LOAD_STATUS = 'In Progress') 
; 




UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER   
SET LS_DB_CASE_REACT_DER.ARI_REC_ID = LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID,
LS_DB_CASE_REACT_DER.SEQ_REACT = LS_DB_CASE_REACT_DER_TMP.SEQ_REACT,
LS_DB_CASE_REACT_DER.PROCESSING_DT = LS_DB_CASE_REACT_DER_TMP.PROCESSING_DT,
LS_DB_CASE_REACT_DER.expiry_date    =LS_DB_CASE_REACT_DER_TMP.expiry_date,
LS_DB_CASE_REACT_DER.date_modified    =LS_DB_CASE_REACT_DER_TMP.date_modified,
LS_DB_CASE_REACT_DER.load_ts    =LS_DB_CASE_REACT_DER_TMP.load_ts,
LS_DB_CASE_REACT_DER.DER_MED_DEVICE_PROB_CONC_ANNEX_E     =LS_DB_CASE_REACT_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_E,  
LS_DB_CASE_REACT_DER.DER_MED_DEVICE_PROB_CONC_ANNEX_F     =LS_DB_CASE_REACT_DER_TMP.DER_MED_DEVICE_PROB_CONC_ANNEX_F,
LS_DB_CASE_REACT_DER.DER_TIME_OFF_DRUG_EVENT        =LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_EVENT,
LS_DB_CASE_REACT_DER.DER_TIME_OFF_DRUG_DAYS   = LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_DAYS  ,
LS_DB_CASE_REACT_DER.DER_TIME_TO_ONSET_NORM   = LS_DB_CASE_REACT_DER_TMP.DER_TIME_TO_ONSET_NORM  ,
LS_DB_CASE_REACT_DER.DER_TIME_OFF_DRUG_NORMAL = LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_NORMAL,
LS_DB_CASE_REACT_DER.DER_SER_CRITERIA_BRAZIL_CODE = LS_DB_CASE_REACT_DER_TMP.DER_SER_CRITERIA_BRAZIL_CODE,
LS_DB_CASE_REACT_DER.DER_TIME_OFF_DRUG_NORM   =LS_DB_CASE_REACT_DER_TMP.DER_TIME_OFF_DRUG_NORM,
LS_DB_CASE_REACT_DER.DER_EVENT_SERIOUSNESS_CRITERIA   =LS_DB_CASE_REACT_DER_TMP.DER_EVENT_SERIOUSNESS_CRITERIA ,
LS_DB_CASE_REACT_DER.DER_REP_CAUS_COMB   =LS_DB_CASE_REACT_DER_TMP.DER_REP_CAUS_COMB ,
LS_DB_CASE_REACT_DER.DER_COMP_CAUS_COMB   =LS_DB_CASE_REACT_DER_TMP.DER_COMP_CAUS_COMB   ,
LS_DB_CASE_REACT_DER.DER_VERBATERMS_PT=LS_DB_CASE_REACT_DER_TMP.DER_VERBATERMS_PT,
LS_DB_CASE_REACT_DER.DER_LLT_PT=LS_DB_CASE_REACT_DER_TMP.DER_LLT_PT
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP 
WHERE 	LS_DB_CASE_REACT_DER.INTEGRATION_ID = LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_REACT_DER_TMP.PROCESSING_DT = LS_DB_CASE_REACT_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_REACT_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_REACT_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
        
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_REACT_DER.ARI_REC_ID ,LS_DB_CASE_REACT_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP 
ON LS_DB_CASE_REACT_DER.ARI_REC_ID=LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID
AND LS_DB_CASE_REACT_DER.INTEGRATION_ID = LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_REACT_DER.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP )
) TMP where LS_DB_CASE_REACT_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_REACT_DER.ARI_REC_ID ,LS_DB_CASE_REACT_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP 
    ON LS_DB_CASE_REACT_DER.ARI_REC_ID=LS_DB_CASE_REACT_DER_TMP.ARI_REC_ID
    AND LS_DB_CASE_REACT_DER.INTEGRATION_ID = LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_REACT_DER.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

 


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER
( ARI_REC_ID    ,
SEQ_REACT       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_MED_DEVICE_PROB_CONC_ANNEX_E,
DER_MED_DEVICE_PROB_CONC_ANNEX_F,
DER_TIME_OFF_DRUG_EVENT,
DER_TIME_OFF_DRUG_DAYS , 
DER_TIME_TO_ONSET_NORM , 
DER_TIME_OFF_DRUG_NORMAL,
DER_SER_CRITERIA_BRAZIL_CODE ,
DER_TIME_OFF_DRUG_NORM,
DER_EVENT_SERIOUSNESS_CRITERIA,
DER_REP_CAUS_COMB,
DER_COMP_CAUS_COMB ,
DER_VERBATERMS_PT,
DER_LLT_PT
)
SELECT 
  ARI_REC_ID    ,
SEQ_REACT       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_MED_DEVICE_PROB_CONC_ANNEX_E,
DER_MED_DEVICE_PROB_CONC_ANNEX_F,
DER_TIME_OFF_DRUG_EVENT,
DER_TIME_OFF_DRUG_DAYS , 
DER_TIME_TO_ONSET_NORM , 
DER_TIME_OFF_DRUG_NORMAL ,
DER_SER_CRITERIA_BRAZIL_CODE ,
DER_TIME_OFF_DRUG_NORM,
DER_EVENT_SERIOUSNESS_CRITERIA  ,
DER_REP_CAUS_COMB,
DER_COMP_CAUS_COMB ,
DER_VERBATERMS_PT,
DER_LLT_PT                                  
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


/*
DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

*/

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_REACT_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_REACT_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_REACT_DER.INTEGRATION_ID = LS_DB_CASE_REACT_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CASE_REACT_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_REACT_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

/*

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy') OR dgthbstdoc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_best_doctor') OR dgthvac_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_therapy_vaccine')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');
*/

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_REACT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_REACT_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_REACT_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_REACT_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_REACT_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;
