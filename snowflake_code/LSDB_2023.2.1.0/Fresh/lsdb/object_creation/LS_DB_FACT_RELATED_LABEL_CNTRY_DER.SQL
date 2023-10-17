CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY_DER()
 RETURNS VARCHAR NOT NULL 
 LANGUAGE SQL AS $$ DECLARE CURRENT_TS_VAR TIMESTAMP;
BEGIN CURRENT_TS_VAR := CURRENT_TIMESTAMP();
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_FACT_RELATED_LABEL_CNTRY_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_FACT_RELATED_LABEL_CNTRY_DER';
-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY_DER() ;
/*

*/
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (
  ROW_WID, TARGET_TABLE_NAME, PARAM_NAME
) 
select 
  ROW_WID, 
  TARGET_TABLE_NAME, 
  PARAM_NAME 
from 
  (
    select 
      coalesce(
        (
          select 
            max(ROW_WID) 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
        )+ 1, 
        1
      ) ROW_WID, 
      'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' AS TARGET_TABLE_NAME, 
      'CDC_EXTRACT_TS_LB' PARAM_NAME 
    union all 
    select 
      coalesce(
        (
          select 
            max(ROW_WID) 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
        )+ 2, 
        1
      ), 
      'LS_DB_FACT_RELATED_LABEL_CNTRY_DER', 
      'CDC_EXTRACT_TS_UB'
  ) 
where 
  TARGET_TABLE_NAME not in (
    select 
      TARGET_TABLE_NAME 
    from 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
  );
INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(
  ROW_WID, FUNCTIONAL_AREA, ENTITY_NAME, 
  TARGET_TABLE_NAME, LOAD_TS, LOAD_START_TS, 
  LOAD_END_TS, REC_READ_CNT, REC_PROCESSED_CNT, 
  ERROR_REC_CNT, ERROR_DETAILS, LOAD_STATUS, 
  CHANGED_REC_SET
) 
SELECT 
  (
    select 
      nvl(
        max(row_wid)+ 1, 
        1
      ) 
    from 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
    where 
      target_table_name = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER'
  ), 
  'LSRA', 
  'Case', 
  'LS_DB_FACT_RELATED_LABEL_CNTRY_DER', 
  null, 
  CURRENT_TIMESTAMP, 
  null, 
  null, 
  null, 
  null, 
  null, 
  'In Progress', 
  null;
UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
SET 
  PARAM_VALUE = nvl(
    (
      SELECT 
        LOAD_TS 
      from 
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
      WHERE 
        TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
        AND LOAD_STATUS = 'Completed' 
      ORDER BY 
        ROW_WID DESC 
      limit 
        1
    ), 
    to_timestamp('1900-01-01', 'YYYY-MM-DD')
  ) 
WHERE 
  TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
  AND PARAM_NAME = 'CDC_EXTRACT_TS_LB' --AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
  ;
UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
SET 
  PARAM_VALUE = (
    select 
      max(load_ts) 
    from 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
  ) 
WHERE 
  TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
  AND PARAM_NAME = 'CDC_EXTRACT_TS_UB' --AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
  ;
DROP 
  TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC AS 
select 
  DISTINCT RECORD_ID record_id, 
  0 common_parent_key, 
  ARI_REC_ID 
FROM 
  ${stage_db_name}.${stage_schema_name}.lsmv_drug 
WHERE 
  CDC_OPERATION_TIME >(
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_LB'
  ) 
  AND CDC_OPERATION_TIME <= (
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_UB'
  ) 
UNION 
select 
  DISTINCT RECORD_ID record_id, 
  0 common_parent_key, 
  ARI_REC_ID 
FROM 
  ${stage_db_name}.${stage_schema_name}.lsmv_reaction 
WHERE 
  CDC_OPERATION_TIME >(
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_LB'
  ) 
  AND CDC_OPERATION_TIME <= (
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_UB'
  ) 
UNION 
select 
  DISTINCT record_id, 
  0 common_parent_key, 
  ARI_REC_ID 
FROM 
  ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness 
WHERE 
  CDC_OPERATION_TIME >(
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_LB'
  ) 
  AND CDC_OPERATION_TIME <= (
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_UB'
  ) 
UNION 
select 
  DISTINCT RECORD_ID record_id, 
  0 common_parent_key, 
  ARI_REC_ID 
FROM 
  ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness 
WHERE 
  CDC_OPERATION_TIME >(
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_LB'
  ) 
  AND CDC_OPERATION_TIME <= (
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_UB'
  ) 
UNION 
select 
  DISTINCT record_id, 
  0 common_parent_key, 
  RECORD_ID ARI_REC_ID 
FROM 
  ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item 
WHERE 
  CDC_OPERATION_TIME >(
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_LB'
  ) 
  AND CDC_OPERATION_TIME <= (
    SELECT 
      PARAM_VALUE 
    FROM 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
    WHERE 
      TARGET_TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      AND PARAM_NAME = 'CDC_EXTRACT_TS_UB'
  );
/*( select UNBLINDED_REC from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_UNBLINDED
    )*/
DROP 
  TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_UNBLINDED;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_UNBLINDED AS 
select 
  LSMV_DRUG.ARI_REC_ID || '-' || AD.SEQ_PRODUCT AS UNBLINDED_REC 
from 
  (
    SELECT 
      LSMV_DRUG.ARI_REC_ID, 
      BLINDED_PRODUCT_REC_ID, 
      CDC_OPERATION_TYPE, 
      row_number() over (
        partition by LSMV_DRUG.RECORD_ID 
        order by 
          CDC_OPERATION_TIME desc
      ) rank 
    FROM 
      ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
    where 
      (
        ARI_REC_ID in (
          select 
            ARI_REC_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        ) 
        or RECORD_ID in (
          select 
            RECORD_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        )
      ) 
      AND CDC_OPERATION_TYPE IN ('I', 'U')
  ) LSMV_DRUG 
  JOIN (
    SELECT 
      LSMV_DRUG.ARI_REC_ID, 
      LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT, 
      CDC_OPERATION_TYPE, 
      row_number() over (
        partition by LSMV_DRUG.RECORD_ID 
        order by 
          CDC_OPERATION_TIME desc
      ) rank 
    FROM 
      ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
    where 
      (
        ARI_REC_ID in (
          select 
            ARI_REC_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        ) 
        or RECORD_ID in (
          select 
            RECORD_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        )
      ) 
      AND CDC_OPERATION_TYPE IN ('I', 'U')
  ) AD ON LSMV_DRUG.ARI_REC_ID = AD.ARI_REC_ID 
  AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT 
  AND LSMV_DRUG.rank = 1 
  AND AD.rank = 1 
  AND LSMV_DRUG.CDC_OPERATION_TYPE IN ('I', 'U') 
  AND AD.CDC_OPERATION_TYPE IN ('I', 'U');
drop 
  table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_NON_BLINDED_RECORD_FLAG;
create temporary table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_NON_BLINDED_RECORD_FLAG as 
SELECT 
  ARI_REC_ID, 
  RECORD_ID, 
  DER_NON_BLINDED_RECORD_FLAG, 
  RANK, 
  CDC_OPERATION_TYPE, 
  DRUGCHARACTERIZATION 
FROM 
  (
    SELECT 
      ARI_REC_ID ARI_REC_ID, 
      RECORD_ID, 
      1 DER_NON_BLINDED_RECORD_FLAG, 
      ROW_NUMBER() OVER (
        PARTITION BY LSMV_DRUG.RECORD_ID 
        ORDER BY 
          CDC_OPERATION_TIME DESC
      ) RANK, 
      CDC_OPERATION_TYPE, 
      DRUGCHARACTERIZATION 
    FROM 
      ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
    WHERE 
      ARI_REC_ID || RECORD_ID NOT IN (
        select 
          ari_rec_id || BLINDED_PRODUCT_REC_ID 
        from 
          (
            SELECT 
              B.ari_rec_id, 
              B.BLINDED_PRODUCT_REC_ID, 
              ROW_NUMBER() OVER (
                PARTITION BY B.RECORD_ID 
                ORDER BY 
                  CDC_OPERATION_TIME DESC
              ) RANK_BLINDED, 
              CDC_OPERATION_TYPE 
            FROM 
              ${stage_db_name}.${stage_schema_name}.LSMV_DRUG B 
            WHERE 
              BLINDED_PRODUCT_REC_ID IS NOT NULL 
              AND (
                ARI_REC_ID in (
                  select 
                    ARI_REC_ID 
                  from 
                    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                ) 
                or RECORD_ID in (
                  select 
                    RECORD_ID 
                  from 
                    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                )
              )
          ) 
        where 
          RANK_BLINDED = 1 
          and CDC_OPERATION_TYPE IN ('I', 'U')
      ) 
      AND (
        ARI_REC_ID in (
          select 
            ARI_REC_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        ) 
        or RECORD_ID in (
          select 
            RECORD_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        )
      )
  ) A 
where 
  RANK = 1 
  AND CDC_OPERATION_TYPE IN ('I', 'U') 
  AND DRUGCHARACTERIZATION IN ('1', '3');
  
  
  
 drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP as 
select * from 
			(select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE,PRODUCT_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
				where ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;
                  

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP;
create TEMPORARY table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP as 
select * from 
(select RECORD_ID,ARI_REC_ID,LIFETHREATENING,LIFETHREATENING_NF,HOSPITALIZATION,HOSPITALIZATION_NF
                      ,CONGENITALANOMALY_NF,DISABILITY,DISABILITY_NF,SERIOUSNESS,
                      DRUG_INTERACTION,HOSP_PROLONGED,DEATH,DEATH_NF,CONGENITALANOMALY,event_type,CDC_OPERATION_TYPE
						,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
											FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
											where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC) 
											 
										)	A	where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED AS
(with drug_subset as (

select ARI_REC_ID,RECORD_ID,BLINDED_PRODUCT_REC_ID
from (
SELECT LSMV_DRUG.ARI_REC_ID,RECORD_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG --where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
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
 

UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
SET 
  LS_DB_FACT_RELATED_LABEL_CNTRY.DER_EXPECT_INTERNATIONAL = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.DER_EXPECT_INTERNATIONAL 
FROM 
  (
    SELECT 
      DISTINCT ARI_REC_ID, 
      SEQ_PRODUCT, 
      SEQ_REACT, 
      AUTO_RANK, 
      CASE WHEN DER_EXPECT_INTERNATIONAL_DSUR IS NOT NULL THEN DER_EXPECT_INTERNATIONAL_DSUR WHEN DER_EXPECT_INTERNATIONAL_IB IS NOT NULL THEN DER_EXPECT_INTERNATIONAL_IB WHEN DER_EXPECT_INTERNATIONAL_CORE IS NOT NULL THEN DER_EXPECT_INTERNATIONAL_CORE ELSE NULL END AS DER_EXPECT_INTERNATIONAL 
    FROM 
      (
        SELECT 
          ARI_REC_ID, 
          SEQ_REACT, 
          SEQ_PRODUCT, 
          AUTO_RANK, 
          MIN(DER_EXPECT_INTERNATIONAL_DSUR) AS DER_EXPECT_INTERNATIONAL_DSUR, 
          MIN(DER_EXPECT_INTERNATIONAL_IB) AS DER_EXPECT_INTERNATIONAL_IB, 
          MIN(DER_EXPECT_INTERNATIONAL_CORE) AS DER_EXPECT_INTERNATIONAL_CORE 
        FROM 
          (
            SELECT 
              DRUG_REACT_LISTEDNESS_SUBSET.ARI_REC_ID, 
              DRUG_REACT_LISTEDNESS_SUBSET.SEQ_REACT, 
              DRUG_REACT_LISTEDNESS_SUBSET.SEQ_PRODUCT, 
              DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE, 
              DRUG_REACT_LISTEDNESS_SUBSET.LABELLED, 
              DRUG_SUBSET.AUTO_RANK, 
              CASE WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('4') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED = '1' THEN 'Listed' WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('4') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED IN ('0', '2') THEN 'Unlisted' END DER_EXPECT_INTERNATIONAL_DSUR, 
              CASE WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('2') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED = '1' THEN 'Listed' WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('2') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED IN ('0', '2') THEN 'Unlisted' END DER_EXPECT_INTERNATIONAL_IB, 
              CASE WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('1') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED = '1' THEN 'Listed' WHEN DRUG_REACT_LISTEDNESS_SUBSET.COUNTRY_CODE IN ('1') 
              AND DRUG_REACT_LISTEDNESS_SUBSET.LABELLED IN ('0', '2') THEN 'Unlisted' END DER_EXPECT_INTERNATIONAL_CORE 
            FROM 
              (
                SELECT 
                  LSMV_DRUG_REACT_LISTEDNESS.ari_rec_id, 
                  fk_drug_rec_id AS seq_product, 
                  fk_ar_rec_id AS seq_react, 
                  is_listed AS LABELLED, 
                  country AS COUNTRY_CODE 
                FROM 
                  (
                    SELECT 
                      DISTINCT ARI_REC_ID, 
                      record_id, 
                      fk_adrr_rec_id, 
                      country, 
                      is_listed, 
                      record_id 
                    FROM 
                      (
                        SELECT 
                          record_id, 
                          ARI_REC_ID, 
                          fk_adrr_rec_id, 
                          CDC_OPERATION_TYPE, 
                          country, 
                          is_listed, 
                          ROW_NUMBER() OVER (
                            PARTITION BY LSMV_DRUG_REACT_LISTEDNESS.RECORD_ID 
                            ORDER BY 
                              CDC_OPERATION_TIME DESC
                          ) RANK 
                        FROM 
                          ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_LISTEDNESS 
                        WHERE 
                          (
                            ARI_REC_ID in (
                              select 
                                ARI_REC_ID 
                              from 
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                            ) 
                            or RECORD_ID in (
                              select 
                                RECORD_ID 
                              from 
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                            )
                          ) 
                          AND CDC_OPERATION_TYPE IN ('I', 'U')
                      ) A 
                    WHERE 
                      RANK = 1 
                      AND CDC_OPERATION_TYPE IN ('I', 'U')
                  ) LSMV_DRUG_REACT_LISTEDNESS 
                  JOIN (
                    SELECT 
                      DISTINCT ARI_REC_ID, 
                      fk_drug_rec_id, 
                      fk_ar_rec_id, 
                      record_id 
                    FROM 
                      (
                        SELECT 
                          record_id, 
                          ARI_REC_ID, 
                          fk_drug_rec_id, 
                          fk_ar_rec_id, 
                          CDC_OPERATION_TYPE, 
                          ROW_NUMBER() OVER (
                            PARTITION BY LSMV_DRUG_REACT_RELATEDNESS.RECORD_ID 
                            ORDER BY 
                              CDC_OPERATION_TIME DESC
                          ) RANK 
                        FROM 
                          ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
                        WHERE 
                          (
                            ARI_REC_ID in (
                              select 
                                ARI_REC_ID 
                              from 
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                            ) 
                            or RECORD_ID in (
                              select 
                                RECORD_ID 
                              from 
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                            )
                          )
                      ) A 
                    WHERE 
                      RANK = 1 
                      AND CDC_OPERATION_TYPE IN ('I', 'U')
                  ) LSMV_DRUG_REACT_RELATEDNESS ON coalesce(LSMV_DRUG_REACT_LISTEDNESS.fk_adrr_rec_id,-1) = coalesce(LSMV_DRUG_REACT_RELATEDNESS.record_id,-1) 
                WHERE 
                  country IN ('1', '2', '4')
              ) DRUG_REACT_LISTEDNESS_SUBSET 
              LEFT JOIN (
                SELECT 
                  DISTINCT ARI_REC_ID, 
                  record_id AS seq_product, 
                  RANK_ORDER AS AUTO_RANK 
                FROM 
                  (
                    SELECT 
                      record_id, 
                      ARI_REC_ID, 
                      DRUGCHARACTERIZATION, 
                      CDC_OPERATION_TYPE, 
                      RANK_ORDER, 
                      ROW_NUMBER() OVER (
                        PARTITION BY LSMV_DRUG.RECORD_ID 
                        ORDER BY 
                          CDC_OPERATION_TIME DESC
                      ) RANK 
                    FROM 
                      ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
                    WHERE 
                      (
                        ARI_REC_ID in (
                          select 
                            ARI_REC_ID 
                          from 
                            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        ) 
                        or RECORD_ID in (
                          select 
                            RECORD_ID 
                          from 
                            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      ) 
                      AND CDC_OPERATION_TYPE IN ('I', 'U')
                  ) A 
                WHERE 
                  RANK = 1 
                  AND CDC_OPERATION_TYPE IN ('I', 'U') 
                  AND DRUGCHARACTERIZATION IN ('1', '3') 
                  AND ARI_REC_ID || '-' || record_id NOT IN (
                    SELECT 
                      UNBLINDED_REC 
                    FROM 
                      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_UNBLINDED
                  )
              ) DRUG_SUBSET ON coalesce(DRUG_REACT_LISTEDNESS_SUBSET.ARI_REC_ID,-1) = coalesce(DRUG_SUBSET.ARI_REC_ID,-1) 
              AND coalesce(DRUG_REACT_LISTEDNESS_SUBSET.SEQ_PRODUCT,-1) = coalesce(DRUG_SUBSET.SEQ_PRODUCT,-1) 
              LEFT JOIN (
                SELECT 
                  record_id AS SEQ_REACT, 
                  ARI_REC_ID 
                FROM 
                  (
                    SELECT 
                      record_id, 
                      ARI_REC_ID, 
                      CDC_OPERATION_TYPE, 
                      ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID 
                        ORDER BY 
                          CDC_OPERATION_TIME DESC
                      ) RANK 
                    FROM 
                      ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
                    WHERE 
                      (
                        ARI_REC_ID in (
                          select 
                            ARI_REC_ID 
                          from 
                            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        ) 
                        or RECORD_ID in (
                          select 
                            RECORD_ID 
                          from 
                            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )
                  ) 
                WHERE 
                  RANK = 1 
                  AND CDC_OPERATION_TYPE IN ('I', 'U')
              ) REACTION_SUBSET ON coalesce(DRUG_REACT_LISTEDNESS_SUBSET.ARI_REC_ID,-1) = coalesce(REACTION_SUBSET.ARI_REC_ID,-1) 
              AND coalesce(DRUG_REACT_LISTEDNESS_SUBSET.SEQ_REACT,-1) = coalesce(REACTION_SUBSET.SEQ_REACT,-1)
          ) Z 
        GROUP BY 
          ARI_REC_ID, 
          SEQ_REACT, 
          SEQ_PRODUCT, 
          AUTO_RANK
      )
  ) LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
WHERE 
  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.ARI_REC_ID 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.SEQ_REACT 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.SEQ_PRODUCT 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

  
  UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
SET 
  LS_DB_FACT_RELATED_LABEL_CNTRY.DER_COMBINED_RELATEDNESS = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.DER_COMBINED_RELATEDNESS 
FROM 
  (   select 
      AER_ID, 
      SEQ_PRODUCT, 
      SEQ_REACT, 
      SEQ_LABEL, 
      der_combined_relatedness 
    from 
      (
        with AER_S as (
          SELECT 
            AER_ID, 
            aer_no, 
            aer_version_no, 
            aer_approval_status 
          FROM 
            (
              SELECT 
                coalesce(lsmv_receipt_item.RECORD_ID,-1) aer_id, 
                aer_no, 
                lsmv_aer_info.aer_version_no, 
                lsmv_message.aer_approval_status, 
                ROW_NUMBER() OVER (
                  PARTITION BY lsmv_receipt_item.RECORD_ID 
                  ORDER BY 
                    lsmv_receipt_item.CDC_OPERATION_TIME DESC
                ) RANK, 
                lsmv_receipt_item.CDC_OPERATION_TYPE 
              FROM 
                ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item 
                INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID = lsmv_receipt_item.RECORD_ID 
                left JOIN ${stage_db_name}.${stage_schema_name}.lsmv_aer_info ON lsmv_receipt_item.record_id = lsmv_aer_info.ari_rec_id 
                LEFT JOIN ${stage_db_name}.${stage_schema_name}.lsmv_message ON lsmv_receipt_item.record_id = lsmv_message.ari_rec_id 
                /*WHERE
                lsmv_message.aer_approval_status ='1'*/
                ) 
          WHERE 
            RANK = 1 
            AND CDC_OPERATION_TYPE IN ('I', 'U')
        ), 
        AER_PRODUCT_S as (
          SELECT 
            A.AER_ID, 
            A.SEQ_PRODUCT 
          FROM 
            (
              SELECT 
                AER_ID, 
                SEQ_PRODUCT, 
                blinded_product_rec_id 
              FROM 
                (
                  SELECT 
                    DISTINCT coalesce(lsmv_drug.ari_rec_id,-1) AS aer_id, 
                    coalesce(lsmv_drug.record_id,-1) AS seq_product, 
                    ROW_NUMBER() OVER (
                      PARTITION BY lsmv_drug.RECORD_ID 
                      ORDER BY 
                        CDC_OPERATION_TIME DESC
                    ) RANK, 
                    CDC_OPERATION_TYPE, 
                    blinded_product_rec_id 
                  FROM 
                    ${stage_db_name}.${stage_schema_name}.lsmv_drug
                ) 
              where 
                RANK = 1 
                AND CDC_OPERATION_TYPE IN ('I', 'U')
            ) A 
            left JOIN (
              select 
                ari_rec_id, 
                seq_product, 
                record_id 
              from 
                (
                  SELECT 
                    lsmv_drug.ari_rec_id, 
                    lsmv_drug.record_id AS seq_product, 
                    lsmv_drug.record_id, 
                    lsmv_drug.blinded_product_rec_id, 
                    ROW_NUMBER() OVER (
                      PARTITION BY lsmv_drug.RECORD_ID 
                      ORDER BY 
                        CDC_OPERATION_TIME DESC
                    ) RANK, 
                    CDC_OPERATION_TYPE 
                  FROM 
                    ${stage_db_name}.${stage_schema_name}.lsmv_drug 
                    INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID = lsmv_drug.ARI_REC_ID
                ) 
              where 
                RANK = 1 
                AND CDC_OPERATION_TYPE IN ('I', 'U')
            ) ad ON A.AER_ID = ad.ari_rec_id 
            AND A.blinded_product_rec_id = ad.record_id 
          WHERE 
            A.AER_ID IS NOT NULL
        ), 
        AER_REACT_S as (
          SELECT 
            AER_ID, 
            SEQ_REACT 
          FROM 
            (
              SELECT 
                DISTINCT coalesce(LSMV_REACTION.ARI_REC_ID,-1) AS AER_ID, 
                coalesce(LSMV_REACTION.RECORD_ID,-1) AS SEQ_REACT, 
                ROW_NUMBER() OVER (
                  PARTITION BY LSMV_REACTION.RECORD_ID 
                  ORDER BY 
                    CDC_OPERATION_TIME DESC
                ) RANK, 
                CDC_OPERATION_TYPE 
              FROM 
                ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
                INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID = LSMV_REACTION.ARI_REC_ID 
              WHERE 
                LSMV_REACTION.ARI_REC_ID IS NOT NULL
            ) 
          where 
            RANK = 1 
            and CDC_OPERATION_TYPE IN ('I', 'U')
        ), 
        AER_LABELLING_S as (
          SELECT 
            aer_id, 
            fk_ar_rec_id, 
            fk_drug_rec_id, 
            seq_label, 
            seq_product, 
            seq_react, 
            country_code, 
            labelled 
          FROM 
            (
              SELECT 
                DISTINCT coalesce(
                  ls_drug_react_listedness.ari_rec_id, 
                  -1
                ) AS aer_id, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id, 
                  -1
                ) as fk_ar_rec_id, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id, 
                  -1
                ) as fk_drug_rec_id, 
                coalesce(
                  ls_drug_react_listedness.record_id, 
                  -1
                ) AS seq_label, 
                coalesce(
                  CASE WHEN LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id IS NULL THEN NULL ELSE LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id END, 
                  -1
                ) AS seq_product, 
                coalesce(
                  CASE WHEN LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id IS NULL THEN NULL ELSE LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id END, 
                  -1
                ) AS seq_react, 
                ls_drug_react_listedness.country AS country_code, 
                ls_drug_react_listedness.is_listed AS labelled, 
                ROW_NUMBER() OVER (
                  PARTITION BY ls_drug_react_listedness.RECORD_ID 
                  ORDER BY 
                    ls_drug_react_listedness.CDC_OPERATION_TIME DESC
                ) RANK, 
                ls_drug_react_listedness.CDC_OPERATION_TYPE 
              FROM 
                ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness ls_drug_react_listedness 
                INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID,-1) = coalesce(ls_drug_react_listedness.ARI_REC_ID,-1) 
                INNER JOIN ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS ON coalesce(ls_drug_react_listedness.fk_adrr_rec_id,-1) = coalesce(LSMV_DRUG_REACT_RELATEDNESS.record_id,-1) 
              WHERE 
                ls_drug_react_listedness.ari_rec_id IS NOT NULL
            ) 
          where 
            RANK = 1 
            and CDC_OPERATION_TYPE IN ('I', 'U')
        ), 
        AER_PRODUCT_AE_ASSOCIATION_S as (
          SELECT 
            AER_ID, 
            FK_DRUG_REC_ID, 
            FK_AR_REC_ID, 
            SEQ_AE_ASSOCIATION, 
            SEQ_PRODUCT, 
            SEQ_REACT, 
            COMPANY_CAUSALITY, 
            REPORTER_CAUSALITY, 
            REPORTER_CAUSALITY_FT, 
            COMPANY_CAUSALITY_FT, 
            SPR_ID 
          FROM 
            (
              SELECT 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.ARI_REC_ID, 
                  -1
                ) AS AER_ID, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.FK_DRUG_REC_ID, 
                  -1
                ) as FK_DRUG_REC_ID, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.FK_AR_REC_ID, 
                  -1
                ) as FK_AR_REC_ID, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.RECORD_ID, 
                  -1
                ) AS SEQ_AE_ASSOCIATION, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.FK_DRUG_REC_ID, 
                  -1
                ) AS SEQ_PRODUCT, 
                coalesce(
                  LSMV_DRUG_REACT_RELATEDNESS.FK_AR_REC_ID, 
                  -1
                ) AS SEQ_REACT, 
                LSMV_DRUG_REACT_RELATEDNESS.COMPANY_CAUSALITY, 
                LSMV_DRUG_REACT_RELATEDNESS.REPORTER_CAUSALITY, 
                ROW_NUMBER() OVER (
                  PARTITION BY LSMV_DRUG_REACT_RELATEDNESS.RECORD_ID 
                  ORDER BY 
                    LSMV_DRUG_REACT_RELATEDNESS.CDC_OPERATION_TIME DESC
                ) RANK, 
                LSMV_DRUG_REACT_RELATEDNESS.CDC_OPERATION_TYPE, 
                LSMV_DRUG_REACT_RELATEDNESS.REPORTER_CAUSALITY_FT, 
                LSMV_DRUG_REACT_RELATEDNESS.COMPANY_CAUSALITY_FT, 
                LSMV_DRUG_REACT_RELATEDNESS.SPR_ID 
              FROM 
                ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
                INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID,-1) = coalesce(LSMV_DRUG_REACT_RELATEDNESS.ARI_REC_ID,-1) 
                LEFT OUTER JOIN ${stage_db_name}.${stage_schema_name}.LSMV_DRUG ON coalesce(LSMV_DRUG_REACT_RELATEDNESS.FK_DRUG_REC_ID,-1) = coalesce(LSMV_DRUG.RECORD_ID,-1) 
              WHERE 
                LSMV_DRUG_REACT_RELATEDNESS.ARI_REC_ID IS NOT NULL
            ) 
          where 
            RANK = 1 
            and CDC_OPERATION_TYPE IN ('I', 'U')
        ), 
ARIS_RELATED_MAPPING_S as (
          select 
            value || '-' || coalesce(SPR_ID, '-9999') as related_causality 
          from 
            (
              select 
                preference_value, 
                spr_id 
              from 
                (
                  select 
                    *, 
                    row_number() over (
                      partition by lsmv_agx_appl_pref.RECORD_ID 
                      order by 
                        DATE_MODIFIED desc
                    ) rank 
                  FROM 
                    ${stage_db_name}.${stage_schema_name}.lsmv_agx_appl_pref 
					where 
					upper (preference_name) like 'CASUALITY_RESULTS'
                ) 
              where 
                rank = 1 
                AND CDC_OPERATION_TYPE IN ('I', 'U')
            ) lsmv_agx_appl_pref, 
            lateral STRTOK_SPLIT_TO_TABLE(
              lsmv_agx_appl_pref.preference_value, 
              ','
            )
        )         
        SELECT 
          distinct AE.AER_ID, 
          IQ.SEQ_PRODUCT, 
          IQ.SEQ_REACT, 
          IQ.SEQ_LABEL, 
          IQ.der_combined_relatedness 
        FROM 
          AER_S AE 
          LEFT JOIN (
            SELECT 
              APR.AER_ID_PROD, 
              APR.AER_ID_EVENT, 
              AL.AER_ID AER_ID_LABEL, 
              APAA.AER_ID AER_ID_APAA, 
              APR.SEQ_PRODUCT, 
              APR.SEQ_REACT, 
              AL.SEQ_LABEL, 
              --       AL.COUNTRY_CODE,
              APAA.SEQ_AE_ASSOCIATION, 
              APAA.SEQ_PRODUCT SEQ_PRODUCT1, 
              APAA.SEQ_REACT SEQ_REACT1, 
              AL.country_code, 
              AL.labelled, 
              CASE
    WHEN (APAA.REPORTER_CAUSALITY  IS NULL
    AND APAA.REPORTER_CAUSALITY_FT IS NOT NULL )
    AND APAA.COMPANY_CAUSALITY     IS NULL
    THEN APAA.REPORTER_CAUSALITY_FT
    WHEN (APAA.COMPANY_CAUSALITY  IS NULL
    AND APAA.COMPANY_CAUSALITY_FT IS NOT NULL )
    AND APAA.REPORTER_CAUSALITY   IS NULL
    THEN APAA.COMPANY_CAUSALITY_FT
    ELSE
      CASE
        WHEN (SELECT trim(upper(default_value_char))
          FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
          WHERE column_name='DER_COMBINED_RELATEDNESS'
          AND table_name   ='LS_DB_FACT_RELATED_LABEL_CNTRY') = 'COMPANY'
        THEN (
          CASE
            WHEN APAA.COMPANY_CAUSALITY IS NULL
            THEN
              (SELECT trim((default_value_char))
              FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
              WHERE parameter_name='RELATED_COMPANY'
              AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
              )
            WHEN APAA.COMPANY_CAUSALITY
              ||'-'
              ||COALESCE(APAA.SPR_ID,'-9999') IN
              (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
              )
            THEN 'Related'
            ELSE 'Not related'
          END)
        ELSE
          CASE
            WHEN (SELECT trim(upper(default_value_char))
              FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
              WHERE column_name='DER_COMBINED_RELATEDNESS'
              AND table_name   ='LS_DB_FACT_RELATED_LABEL_CNTRY') = 'REPORTER'
            THEN (
              CASE
                WHEN APAA.REPORTER_CAUSALITY IS NULL
                THEN
                  (SELECT trim((default_value_char))
                  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                  WHERE parameter_name='RELATED_REPORTER'
                  AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
                  )
                WHEN APAA.REPORTER_CAUSALITY
                  ||'-'
                  ||COALESCE(APAA.SPR_ID,'-9999') IN
                  (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                  )
                THEN 'Related'
                ELSE 'Not related'
              END )
            ELSE
              CASE
                WHEN (SELECT trim(upper(default_value_char))
                  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                  WHERE column_name='DER_COMBINED_RELATEDNESS'
                  AND table_name   ='LS_DB_FACT_RELATED_LABEL_CNTRY') IN ( 'COMPANY OR REPORTER' , 'REPORTER OR COMPANY' )
                THEN
                  CASE
                    WHEN ((SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_REPORTER'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NOT NULL
                    AND (SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_COMPANY'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NOT NULL)
                    THEN (
                      CASE
				      WHEN APAA.REPORTER_CAUSALITY IS NULL
                        THEN (
                          (SELECT trim((default_value_char))
                          FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                          WHERE parameter_name='RELATED_REPORTER'
                          AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
                          ))
                        WHEN APAA.COMPANY_CAUSALITY IS NULL
                        THEN (
                          (SELECT trim((default_value_char))
                          FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                          WHERE parameter_name='RELATED_COMPANY'
                          AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
                          ))
                        WHEN COALESCE(APAA.REPORTER_CAUSALITY,APAA.COMPANY_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        OR COALESCE(APAA.COMPANY_CAUSALITY,APAA.REPORTER_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        THEN 'Related'
                        ELSE 'Not related'
                      END)
					 WHEN 
					 (
					 (SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_REPORTER'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NULL
                    AND (SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_COMPANY'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NOT NULL)
                    THEN (
                      CASE
					  WHEN APAA.COMPANY_CAUSALITY IS NULL
                        THEN (
                          (SELECT trim((default_value_char))
                          FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                          WHERE parameter_name='RELATED_COMPANY'
                          AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
                          ))
                        WHEN COALESCE(APAA.REPORTER_CAUSALITY,APAA.COMPANY_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        OR COALESCE(APAA.COMPANY_CAUSALITY,APAA.REPORTER_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        THEN 'Related'
                        ELSE 'Not related'
                      END)
					  WHEN ((SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_REPORTER'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NOT NULL
                    AND (SELECT trim((default_value_char))
                      FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                      WHERE parameter_name='RELATED_COMPANY'
                      AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY') IS NULL)
                    THEN (
                      CASE
					  	WHEN APAA.REPORTER_CAUSALITY IS NULL 
                        THEN (
                          (SELECT trim((default_value_char))
                          FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                          WHERE parameter_name='RELATED_REPORTER'
                          AND table_name      ='LS_DB_FACT_RELATED_LABEL_CNTRY'
                          ))
                        WHEN COALESCE(APAA.REPORTER_CAUSALITY,APAA.COMPANY_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        OR COALESCE(APAA.COMPANY_CAUSALITY,APAA.REPORTER_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        THEN 'Related'
                        ELSE 'Not related'
                      END)
                    ELSE (
                      CASE
                        WHEN APAA.REPORTER_CAUSALITY IS NULL
                        AND APAA.COMPANY_CAUSALITY   IS NULL
                        THEN NULL
                        WHEN COALESCE(APAA.REPORTER_CAUSALITY,APAA.COMPANY_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        OR COALESCE(APAA.COMPANY_CAUSALITY,APAA.REPORTER_CAUSALITY)
                          ||'-'
                          ||COALESCE(APAA.SPR_ID,'-9999') IN
                          (SELECT related_causality FROM ARIS_RELATED_MAPPING_S
                          )
                        THEN 'Related'
                        ELSE 'Not related'
                      END)
                  END
              END
          END
      END
  END DER_COMBINED_RELATEDNESS
            FROM 
              (
                SELECT 
                  AP.AER_ID AER_ID_PROD, 
                  AR.AER_ID AER_ID_EVENT, 
                  AP.SEQ_PRODUCT, 
                  AR.SEQ_REACT 
                FROM 
                  AER_PRODUCT_S AP FULL 
                  OUTER JOIN AER_REACT_S AR ON AP.AER_ID = AR.AER_ID
              ) APR FULL 
              OUTER JOIN AER_LABELLING_S AL ON COALESCE(
                APR.AER_ID_PROD, 
                COALESCE(APR.AER_ID_EVENT,-1)
              )= AL.AER_ID 
              AND COALESCE(APR.SEQ_PRODUCT,-1)= COALESCE(AL.SEQ_PRODUCT,-1) 
              AND COALESCE(APR.SEQ_REACT,-1)= COALESCE(AL.SEQ_REACT,-1) 
              LEFT OUTER JOIN AER_PRODUCT_AE_ASSOCIATION_S APAA ON COALESCE(
                APR.AER_ID_PROD, 
                COALESCE(APR.AER_ID_EVENT,-1)
              )= APAA.AER_ID 
              AND COALESCE(APR.SEQ_PRODUCT,-1)= COALESCE(APAA.SEQ_PRODUCT,-1) 
              AND COALESCE(APR.SEQ_REACT,-1)= COALESCE(APAA.SEQ_REACT,-1)
          ) IQ ON AE.AER_ID = COALESCE(
            IQ.AER_ID_PROD, 
            COALESCE(IQ.AER_ID_EVENT,-1)
          )
      ) FL
  ) LS_DB_FACT_RELATED_LABEL_CNTRY_TMP 
WHERE 
  coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID, 
    -1
  ) = coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.AER_ID, 
    -1
  ) --and coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_AE_ASSOCIATION,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.RECORD_ID,-1) 
  and coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT, 
    -1
  ) = coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.SEQ_PRODUCT, 
    -1
  ) 
  and coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT, 
    -1
  ) = coalesce(
    LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.SEQ_REACT, 
    -1
  ) 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM');
/*UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
SET 
  LS_DB_FACT_RELATED_LABEL_CNTRY.DER_CASE_EVENT_SERIOUSNESS = LS_DB_FACT_RELATED_LABEL_CNTRY_TMP.DER_CASE_EVENT_SERIOUSNESS 
FROM
*/
DROP 
  TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS AS 
SELECT 
  ARI_REC_ID, 
  RECORD_ID, 
  DER_CASE_EVENT_SERIOUSNESS 
FROM 
  (
    SELECT 
      A.ARI_REC_ID AS ARI_REC_ID, 
      A.RECORD_ID AS RECORD_ID, 
      CASE WHEN (
        SELECT 
          TRIM(
            UPPER(default_value_char)
          ) 
        FROM 
          ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
        WHERE 
          column_name = 'DER_CASE_EVENT_SERIOUSNESS' 
          AND table_name = 'B_CASE_EVENT_W'
      ) = 'COMPANY' THEN (
        CASE WHEN SERIOUSNESS_COMPANY IS NULL THEN NULL WHEN (
          SERIOUSNESS_COMPANY = '01' 
          OR SERIOUSNESS_COMPANY = '1'
        ) THEN 'Serious' WHEN (
          SERIOUSNESS_COMPANY = '02' 
          OR SERIOUSNESS_COMPANY = '2'
        ) THEN 'Non-serious' END
      ) ELSE CASE WHEN (
        SELECT 
          TRIM(
            UPPER(default_value_char)
          ) 
        FROM 
          ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
        WHERE 
          column_name = 'DER_CASE_EVENT_SERIOUSNESS' 
          AND table_name = 'B_CASE_EVENT_W'
      ) = 'REPORTER' THEN (
        CASE WHEN SERIOUSNESS IS NULL THEN NULL WHEN (
          SERIOUSNESS = '01' 
          OR SERIOUSNESS = '1'
        ) THEN 'Serious' WHEN (
          SERIOUSNESS = '02' 
          OR SERIOUSNESS = '2'
        ) THEN 'Non-serious' END
      ) ELSE CASE WHEN (
        SELECT 
          TRIM(
            UPPER(default_value_char)
          ) 
        FROM 
          ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER 
        WHERE 
          column_name = 'DER_CASE_EVENT_SERIOUSNESS' 
          AND table_name = 'B_CASE_EVENT_W'
      ) IN (
        'COMPANY OR REPORTER', 'REPORTER OR COMPANY'
      ) THEN (
        CASE WHEN (
          SERIOUSNESS IS NULL 
          AND SERIOUSNESS_COMPANY IS NULL
        ) THEN NULL WHEN (
          SERIOUSNESS IS NULL 
          AND (
            SERIOUSNESS_COMPANY = '02' 
            OR SERIOUSNESS_COMPANY = '2'
          )
        ) THEN 'Non-serious' WHEN (
          (
            SERIOUSNESS = '02' 
            OR SERIOUSNESS = '2'
          ) 
          AND SERIOUSNESS_COMPANY IS NULL
        ) THEN 'Non-serious' WHEN (
          (
            SERIOUSNESS = '01' 
            OR SERIOUSNESS = '1'
          ) 
          OR (
            SERIOUSNESS_COMPANY = '01' 
            OR SERIOUSNESS_COMPANY = '1'
          )
        ) THEN 'Serious' WHEN (
          (
            SERIOUSNESS = '02' 
            OR SERIOUSNESS = '2'
          ) 
          AND (
            SERIOUSNESS_COMPANY = '02' 
            OR SERIOUSNESS_COMPANY = '2'
          )
        ) THEN 'Non-serious' END
      ) END END END DER_CASE_EVENT_SERIOUSNESS, 
      ROW_NUMBER() OVER (
        PARTITION BY A.RECORD_ID 
        ORDER BY 
          CDC_OPERATION_TIME DESC
      ) RANK, 
      CDC_OPERATION_TYPE 
    FROM 
      ${stage_db_name}.${stage_schema_name}.LSMV_REACTION A 
    WHERE 
      (
        A.ARI_REC_ID in (
          select 
            ARI_REC_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        ) 
        or A.RECORD_ID in (
          select 
            RECORD_ID 
          from 
            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
        )
      )
  ) A 
WHERE 
  RANK = 1 
  AND CDC_OPERATION_TYPE IN ('I', 'U');
update 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
set 
  DER_CASE_SRL_RANK = t.DER_CASE_SRL_RANK, 
  DER_CASE_SRL_H_RANK = t.DER_CASE_SRL_H_RANK, 
  DER_CASE_SRL_RANK_DECODE = t.DER_CASE_SRL_RANK_DECODE 
from 
  (
    select 
      B.ARI_REC_ID, 
      DER_CASE_SRL_RANK, 
      DER_CASE_SRL_H_RANK, 
      case when DER_CASE_SRL_RANK = 1 then 'Non-serious + Unrelated + Listed' when DER_CASE_SRL_RANK = 2 then 'Non-serious + Unrelated + Unlisted' when DER_CASE_SRL_RANK = 3 then 'Non-serious + Related + Listed' when DER_CASE_SRL_RANK = 4 then 'Non-serious + Related + Unlisted' when DER_CASE_SRL_RANK = 5 then 'Serious + Unrelated + Listed' when DER_CASE_SRL_RANK = 6 then 'Serious + Unrelated + Unlisted' when DER_CASE_SRL_RANK = 7 then 'Serious + Related + Listed' when DER_CASE_SRL_RANK = 8 then 'Serious + Related + Unlisted' else null end DER_CASE_SRL_RANK_DECODE 
    from 
      (
        SELECT 
          A.ARI_REC_ID, 
          A.RNK as DER_CASE_SRL_RANK, 
          CASE WHEN RNK = 1 THEN 105 WHEN RNK = 2 THEN 255 WHEN RNK = 3 THEN 273 WHEN RNK = 4 THEN 663 WHEN RNK = 5 THEN 385 WHEN RNK = 6 THEN 935 WHEN RNK = 7 THEN 1001 WHEN RNK = 8 THEN 2431 ELSE NULL END AS DER_CASE_SRL_H_RANK 
        FROM 
          (
            SELECT 
              ARI_REC_ID, 
              MAX(DER_PPD_SRL_RANK) AS RNK 
            FROM 
              (
                select 
                  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID, 
                  SEQ_PRODUCT, 
                  SEQ_REACT, 
                  SEQ_label, 
                  case when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 1 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 2 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 3 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 4 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 5 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 6 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 7 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 8 else null end as DER_PPD_SRL_RANK 
                from 
                  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
                  LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_CASE_EVENT_SERIOUSNESS.ARI_REC_ID,-1) 
				  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1)=coalesce(DER_CASE_EVENT_SERIOUSNESS.RECORD_ID,-1) 
                  LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_NON_BLINDED_RECORD_FLAG ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_NON_BLINDED_RECORD_FLAG.ARI_REC_ID,-1) --and PRODUCT_FLAG_CE in ('1','3')
				  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1)=coalesce(DER_NON_BLINDED_RECORD_FLAG.RECORD_ID,-1) 
                  INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                  ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1)=coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID,-1)
                WHERE 
                  LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM') --and DER_NON_BLINDED_RECORD_FLAG=1
                  --and (PRODUCT_IS_DELETED=0  or  PRODUCT_IS_DELETED is null )
                  ) 
            GROUP BY 
              ARI_REC_ID
          ) A
      ) B
  ) t 
where 
  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID = t.ARI_REC_ID 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM');

update 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
set 
  DER_PPD_SRL_RANK_DECODE = t.DER_PPD_SRL_RANK_DECODE, 
  DER_PPD_SRL_RANK = t.DER_PPD_SRL_RANK, 
  DER_CASE_PROD_EVENT_SRL_H_RANK = t.DER_CASE_PROD_EVENT_SRL_H_RANK 
from 
  (
    select 
      ARI_REC_ID, 
      SEQ_PRODUCT, 
      SEQ_REACT, 
      SEQ_label, 
      DER_PPD_SRL_RANK_DECODE, 
      DER_PPD_SRL_RANK, 
      DER_CASE_PROD_EVENT_SRL_H_RANK 
    FROM 
      (
        select 
          ARI_REC_ID, 
          SEQ_PRODUCT, 
          SEQ_REACT, 
          SEQ_label, 
          case when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 105 then 'Non-serious + Unrelated + Listed' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 255 then 'Non-serious + Unrelated + Unlisted' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 273 then 'Non-serious + Related + Listed' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 663 then 'Non-serious + Related + Unlisted' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 385 then 'Serious + Unrelated + Listed' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 935 then 'Serious + Unrelated + Unlisted' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 1001 then 'Serious + Related + Listed' when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 2431 then 'Serious + Related + Unlisted' else 'Non-serious + Unrelated + Listed' end as DER_PPD_SRL_RANK_DECODE, 
          case when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 105 then 1 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 255 then 2 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 273 then 3 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 663 then 4 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 385 then 5 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 935 then 6 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 1001 then 7 when A.DER_CASE_PROD_EVENT_SRL_H_RANK = 2431 then 8 else Null end DER_PPD_SRL_RANK, 
          DER_CASE_PROD_EVENT_SRL_H_RANK 
        FROM 
          (
            select 
              LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID, 
              SEQ_PRODUCT, 
              SEQ_REACT, 
              SEQ_label, 
              case when (
                DER_CASE_EVENT_SERIOUSNESS is null 
                or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS is null 
                or DER_COMBINED_RELATEDNESS = 'Not related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL is null 
                or DER_EXPECT_INTERNATIONAL = 'Listed'
              ) then 105 when (
                DER_CASE_EVENT_SERIOUSNESS is null 
                or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS is null 
                or DER_COMBINED_RELATEDNESS = 'Not related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL = 'Unlisted'
              ) then 255 when (
                DER_CASE_EVENT_SERIOUSNESS is null 
                or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS = 'Related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL is null 
                or DER_EXPECT_INTERNATIONAL = 'Listed'
              ) then 273 when (
                DER_CASE_EVENT_SERIOUSNESS is null 
                or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS = 'Related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL = 'Unlisted'
              ) then 663 when (
                DER_CASE_EVENT_SERIOUSNESS = 'Serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS is null 
                or DER_COMBINED_RELATEDNESS = 'Not related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL is null 
                or DER_EXPECT_INTERNATIONAL = 'Listed'
              ) then 385 when (
                DER_CASE_EVENT_SERIOUSNESS = 'Serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS is null 
                or DER_COMBINED_RELATEDNESS = 'Not related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL = 'Unlisted'
              ) then 935 when (
                DER_CASE_EVENT_SERIOUSNESS = 'Serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS = 'Related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL is null 
                or DER_EXPECT_INTERNATIONAL = 'Listed'
              ) then 1001 when (
                DER_CASE_EVENT_SERIOUSNESS = 'Serious'
              ) 
              and (
                DER_COMBINED_RELATEDNESS = 'Related'
              ) 
              and (
                DER_EXPECT_INTERNATIONAL = 'Unlisted'
              ) then 2431 else null end as DER_CASE_PROD_EVENT_SRL_H_RANK 
            from 
              ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
              LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_CASE_EVENT_SERIOUSNESS.ARI_REC_ID,-1) 
			  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1)=coalesce(DER_CASE_EVENT_SERIOUSNESS.RECORD_ID,-1) 
              LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_NON_BLINDED_RECORD_FLAG ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_NON_BLINDED_RECORD_FLAG.ARI_REC_ID,-1) 
			  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1)=coalesce(DER_NON_BLINDED_RECORD_FLAG.RECORD_ID,-1) 
			  --and PRODUCT_FLAG_CE in ('1','3')
              --and DER_NON_BLINDED_RECORD_FLAG=1
              --and (PRODUCT_IS_DELETED=0  or  PRODUCT_IS_DELETED is null )
              INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
              ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1)=coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID,-1)
            WHERE 
              LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
          ) A
      ) B
  ) t 
where 
  coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID,-1) 
  AND coalesce( LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1) = coalesce(t.SEQ_REACT,-1) 
  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1) = coalesce(t.SEQ_PRODUCT,-1) 
  AND nvl(
    LS_DB_FACT_RELATED_LABEL_CNTRY.seq_label, 
    -1
  )= nvl(t.seq_label,-1) 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM');
  
  
update 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
set 
  DER_CASE_PROD_SRL_RANK = t.DER_CASE_PROD_SRL_RANK, 
  DER_CASE_PROD_SRL_H_RANK = t.DER_CASE_PROD_SRL_H_RANK, 
  DER_CASE_PROD_SRL_RANK_DECODE = t.DER_CASE_PROD_SRL_RANK_DECODE 
from 
  (
    select 
      B.ARI_REC_ID, 
      B.DER_CASE_PROD_SRL_RANK, 
      B.SEQ_PRODUCT, 
      B.DER_CASE_PROD_SRL_H_RANK, 
      case when B.DER_CASE_PROD_SRL_RANK = 1 then 'Non-serious + Unrelated + Listed' when B.DER_CASE_PROD_SRL_RANK = 2 then 'Non-serious + Unrelated + Unlisted' when B.DER_CASE_PROD_SRL_RANK = 3 then 'Non-serious + Related + Listed' when B.DER_CASE_PROD_SRL_RANK = 4 then 'Non-serious + Related + Unlisted' when B.DER_CASE_PROD_SRL_RANK = 5 then 'Serious + Unrelated + Listed' when B.DER_CASE_PROD_SRL_RANK = 6 then 'Serious + Unrelated + Unlisted' when B.DER_CASE_PROD_SRL_RANK = 7 then 'Serious + Related + Listed' when B.DER_CASE_PROD_SRL_RANK = 8 then 'Serious + Related + Unlisted' else 'Non-serious + Unrelated + Unlisted' end DER_CASE_PROD_SRL_RANK_DECODE 
    from 
      (
        SELECT 
          A.ARI_REC_ID, 
          A.SEQ_PRODUCT, 
          A.RNK as DER_CASE_PROD_SRL_RANK, 
          CASE WHEN A.RNK = 1 THEN 105 WHEN A.RNK = 2 THEN 255 WHEN A.RNK = 3 THEN 273 WHEN A.RNK = 4 THEN 663 WHEN A.RNK = 5 THEN 385 WHEN A.RNK = 6 THEN 935 WHEN A.RNK = 7 THEN 1001 WHEN A.RNK = 8 THEN 2431 ELSE 105 END AS DER_CASE_PROD_SRL_H_RANK 
        FROM 
          (
            SELECT 
              ARI_REC_ID, 
              SEQ_PRODUCT, 
              MAX(DER_PPD_SRL_RANK) AS RNK 
            FROM 
              (
                select 
                  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID, 
                  SEQ_PRODUCT, 
                  SEQ_REACT, 
                  SEQ_label, 
                  case when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 1 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 2 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 3 when (
                    DER_CASE_EVENT_SERIOUSNESS is null 
                    or DER_CASE_EVENT_SERIOUSNESS = 'Non-serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 4 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 5 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS is null 
                    or DER_COMBINED_RELATEDNESS = 'Not related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 6 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL is null 
                    or DER_EXPECT_INTERNATIONAL = 'Listed'
                  ) then 7 when (
                    DER_CASE_EVENT_SERIOUSNESS = 'Serious'
                  ) 
                  and (
                    DER_COMBINED_RELATEDNESS = 'Related'
                  ) 
                  and (
                    DER_EXPECT_INTERNATIONAL = 'Unlisted'
                  ) then 8 else null end as DER_PPD_SRL_RANK 
                from 
                  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
                  LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_CASE_EVENT_SERIOUSNESS.ARI_REC_ID,-1)
				  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1)=coalesce(DER_CASE_EVENT_SERIOUSNESS.RECORD_ID,-1) 
																	
                  LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_NON_BLINDED_RECORD_FLAG ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(DER_NON_BLINDED_RECORD_FLAG.ARI_REC_ID,-1) --and PRODUCT_FLAG_CE in ('1','3')
				  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1)=coalesce(DER_NON_BLINDED_RECORD_FLAG.RECORD_ID,-1) 
                  INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC ON coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC.ARI_REC_ID,-1) 
                WHERE 
                  LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM') --and DER_NON_BLINDED_RECORD_FLAG=1
                  --and (PRODUCT_IS_DELETED=0  or  PRODUCT_IS_DELETED is null )
                  ) 
            GROUP BY 
              ARI_REC_ID, 
              SEQ_PRODUCT
          ) A
      ) B
  ) t 
where 
  coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID,-1) 
  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1) = coalesce(t.SEQ_PRODUCT,-1) 
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;



UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	DER_MEDICATION_ERROR_FLAG = CASE
			WHEN t.ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END
FROM
	(
	SELECT
		ARI_REC_ID,
		CASE
			WHEN ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END AS DER_MEDICATION_ERROR_FLAG
	FROM
		(
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				AND DM.EXPIRY_DATE = TO_DATE('9999-31-12',
				'YYYY-DD-MM')
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),
				-1)= NVL(DW.PT_CODE,
				-1)
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),
				TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE))= NVL(DW.LLT_CODE,
				DW.PT_CODE)
				AND (DW.HLT_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
					AND PARAMETER_NAME = 'MEF_HLT')
				OR DW.SOC_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
					AND PARAMETER_NAME = 'MEF_SOC'))
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	
		)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			WHERE
				TRY_TO_NUMBER(REACTMEDDRAPT_CODE )=(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
					AND PARAMETER_NAME = 'MEF_PT')
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				AR.ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query DW,
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR 
,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				--	AND DM.VERSION_FLAG = 1
				AND  
TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE) = DW.TERM_CODE
				AND TERM_LEVEL = 4
				--	AND AR.RECORD_DELETED = 0
				AND SMQ_CODE IN 
(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
					AND PARAMETER_NAME IN ('MEF_SMQ', 'MEF_CMQ'))
					AND DW.TERM_SCOPE IN (
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '1'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '1'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
				UNION
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '2'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_MEDICATION_ERROR_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '2'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
  )
					AND 
                      (
                        ARI_REC_ID IN (
					SELECT
						ARI_REC_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
					OR RECORD_ID IN (
					SELECT
						RECORD_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

  
  )
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')

  )
  )
  T
WHERE
	coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID(+),-1)
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12',
	'YYYY-DD-MM')
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID IN (
	SELECT
				ARI_REC_ID
	FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        );


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY   
SET LS_DB_FACT_RELATED_LABEL_CNTRY.DER_SUSPECT_PPD_BLIND=case when LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSPECT_PPD_BLIND is not null then 
substring(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSPECT_PPD_BLIND,1,3996) ||
case when length(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSPECT_PPD_BLIND)>4000 then ' ...' else '' end
else '(-)'
end
FROM
( select ARI_REC_ID, listagg(DER_SUSPECT_PPD_BLIND,'\r\n') DER_SUSPECT_PPD_BLIND
from 
( 
select ARI_REC_ID,case when class='BN' then '@\n'||listagg(DER_SUSPECT_PPD_BLIND,'|') within group (order by ARI_REC_ID,RANK_ORDER,class)
else listagg(DER_SUSPECT_PPD_BLIND,'|') within group (order by ARI_REC_ID,RANK_ORDER,class) end DER_SUSPECT_PPD_BLIND from (
(
SELECT 
  APD.ARI_REC_ID,
  CASE 
    WHEN APD.PREFERED_PRODUCT_description IS NULL THEN 'D'||APD.RANK_ORDER||': '||'(-)'
	WHEN APD.INVESTIGATIONAL_PROD_BLINDED ='1' THEN 'D'||APD.RANK_ORDER||': '||APD.PREFERED_PRODUCT_description || ' (BLIND)'
    ELSE  'D'||APD.RANK_ORDER||': '||APD.PREFERED_PRODUCT_description 
  END AS DER_SUSPECT_PPD_BLIND,
  'UN' class,
  RANK_ORDER
FROM 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
WHERE
  APD.DRUGCHARACTERIZATION IN ('1','3')
  AND APD.ARI_REC_ID||'-'||APD.record_id  NOT  IN
  (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null )
ORDER BY
  APD.RANK_ORDER
 )
 
 UNION ALL
 
 (
SELECT 
  APD.ARI_REC_ID,
  CASE 
    WHEN APD.PREFERED_PRODUCT_description IS NULL THEN 'D'||APD.RANK_ORDER||': '||'(-)'
	WHEN APD.INVESTIGATIONAL_PROD_BLINDED ='1' THEN 'D'||APD.RANK_ORDER||': '||APD.PREFERED_PRODUCT_description || ' (BLIND)'
    ELSE  'D'||APD.RANK_ORDER||': '||APD.PREFERED_PRODUCT_description 
  END AS DER_SUSPECT_PPD_BLIND,
  'BN' class,
  RANK_ORDER
FROM 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP APD
WHERE
  APD.DRUGCHARACTERIZATION IN ('1','3')
  AND APD.ARI_REC_ID||'-'||APD.record_id IN
  (select ARI_REC_ID||'-'||seq_product from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is  null )
ORDER BY
  APD.RANK_ORDER
 )
   )
  group by ARI_REC_ID,class 
  order by  ARI_REC_ID,class desc
 ) group by 1
  )  LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL
    WHERE LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID = LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.ARI_REC_ID	
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');  



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY   
SET LS_DB_FACT_RELATED_LABEL_CNTRY.DER_SUSAR_EVENT=CASE
         WHEN LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSAR_EVENT IS NOT NULL THEN SUBSTRING(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSAR_EVENT,1,3996) ||
           CASE
             WHEN LENGTH(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SUSAR_EVENT) > 4000 THEN ' ...'
             ELSE ''
           END 
         ELSE '(-)'
       END
FROM ( SELECT ARI_REC_ID,
             seq_react,
             listagg(DER_SUSAR_EVENT,'\n') WITHIN GROUP(ORDER BY ARI_REC_ID,AUTO_RANK) DER_SUSAR_EVENT
      FROM (SELECT ARI_REC_ID,
                   SEQ_REACT,
                   'D' ||AUTO_RANK|| ': ' || DER_SUSAR_EVENT AS DER_SUSAR_EVENT,
                   AUTO_RANK
            FROM (SELECT R.ARI_REC_ID ARI_REC_ID,
                         R.RECORD_ID SEQ_REACT,
                         CASE
                           WHEN FL_SUSAR = '1' THEN 'Yes'
                           ELSE 'No'
                         END DER_SUSAR_EVENT,
                         T.AUTO_RANK
                  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACTION_SUBSET_TMP R,
                       (SELECT A.ARI_REC_ID ARI_REC_ID,
                               A.RECORD_ID SEQ_PRODUCT,
                               C.RECORD_ID SEQ_REACT,
                               MAX(FL_SUSAR) FL_SUSAR,
                               A.RANK_ORDER AUTO_RANK
                        FROM (SELECT ARI_REC_ID,RECORD_ID,RANK_ORDER,DRUGCHARACTERIZATION FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_SUBSET_TMP) A,
                             (SELECT RECORD_ID,ARI_REC_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACTION_SUBSET_TMP) C,
                             (  SELECT          LSMV_DRUG_REACT_LISTEDNESS.ARI_REC_ID,
                                                LSMV_DRUG_REACT_RELATEDNESS.FK_DRUG_REC_ID SEQ_PRODUCT,
                                                LSMV_DRUG_REACT_RELATEDNESS.FK_AR_REC_ID AS SEQ_REACT,
                                                MAX(CASE WHEN SUSAR = '1' THEN 1 ELSE 0 END) as FL_SUSAR

                                        FROM 
                                        (select *
                                                FROM 
                                                (select record_id,ARI_REC_ID,FK_ADRR_REC_ID,
                                                SUSAR,
                                                CDC_OPERATION_TYPE,
                                                row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
                                                        FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_LISTEDNESS 
                                                        where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC) 
                                                ) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
                                        )       
                                        LSMV_DRUG_REACT_LISTEDNESS
                                        INNER JOIN 
                                        (select *
                                                FROM 
                                                (select ARI_REC_ID, 
                                                FK_DRUG_REC_ID,
                                                FK_AR_REC_ID ,
                                                RECORD_ID,CDC_OPERATION_TYPE,
                                                row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
                                                        FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
                                                        where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC) 
                                                ) where rank=1 and  CDC_OPERATION_TYPE IN ('I','U')
                                        ) LSMV_DRUG_REACT_RELATEDNESS ON LSMV_DRUG_REACT_LISTEDNESS.FK_ADRR_REC_ID = LSMV_DRUG_REACT_RELATEDNESS.RECORD_ID
        
                                    group by 1,2,3  
                                        
                                        
                              ) B
                        WHERE A.DRUGCHARACTERIZATION IN ('1','3')
                        AND   B.ARI_REC_ID = A.ARI_REC_ID(+)
                        AND   B.SEQ_PRODUCT = A.RECORD_ID(+)
                        AND   B.ARI_REC_ID = C.ARI_REC_ID(+)
                        AND   B.SEQ_REACT = C.RECORD_ID(+)
                        AND   A.ARI_REC_ID ||'-'||A.RECORD_ID NOT IN (select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
                    where SEQ_UNBLINDED is not null)
                        GROUP BY A.ARI_REC_ID,
                                 A.RECORD_ID,
                                 A.RANK_ORDER,
                                 C.RECORD_ID) T
                  WHERE T.ARI_REC_ID = R.ARI_REC_ID
                  AND   T.SEQ_REACT = R.RECORD_ID)
            ORDER BY ARI_REC_ID,
                     AUTO_RANK)
      GROUP BY ARI_REC_ID,
               seq_react) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL
    WHERE coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ari_rec_id,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.ari_rec_id,-1) 
    and coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1)=coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.seq_react,-1)
    AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	DER_ABUSE_FLAG = CASE
			WHEN t.ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END

FROM
	(
	SELECT
		ARI_REC_ID,
		CASE
			WHEN ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END AS DER_ABUSE_FLAG
	FROM
		(
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				AND DM.EXPIRY_DATE = TO_DATE('9999-31-12',
				'YYYY-DD-MM')
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),
				-1)= NVL(DW.PT_CODE,
				-1)
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),
				TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE))= NVL(DW.LLT_CODE,
				DW.PT_CODE)
				AND (DW.HLT_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_ABUSE_FLAG'
					AND PARAMETER_NAME = 'AF_HLT')
				OR DW.SOC_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_ABUSE_FLAG'
					AND PARAMETER_NAME = 'AF_SOC'))
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	
		)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			WHERE
				TRY_TO_NUMBER(REACTMEDDRAPT_CODE) =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_ABUSE_FLAG'
					AND PARAMETER_NAME = 'AF_PT')
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				AR.ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query DW,
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR 
,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				--	AND DM.VERSION_FLAG = 1
				AND  
TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE) = DW.TERM_CODE
				AND TERM_LEVEL = 4
				--	AND AR.RECORD_DELETED = 0
				AND SMQ_CODE IN 
(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_ABUSE_FLAG'
					AND PARAMETER_NAME IN ('AF_SMQ', 'AF_CMQ'))
					AND DW.TERM_SCOPE IN (
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_ABUSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '1'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_ABUSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '1'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
				UNION
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_ABUSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '2'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_ABUSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '2'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
  )
					AND 
                      (
                        ARI_REC_ID IN (
					SELECT
						ARI_REC_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
					OR RECORD_ID IN (
					SELECT
						RECORD_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

  
  )
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')

  )
  )
  T
WHERE
	coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID(+),-1)
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12',
	'YYYY-DD-MM')
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID IN (
	SELECT
				ARI_REC_ID
	FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        );
UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	DER_INTERACTION_FLAG = CASE
			WHEN t.ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END
FROM
	(
	SELECT
		ARI_REC_ID
	FROM
		(
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				AND DM.EXPIRY_DATE = TO_DATE('9999-31-12',
				'YYYY-DD-MM')
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),
				-1)= NVL(DW.PT_CODE,
				-1)
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),
				TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE))= NVL(DW.LLT_CODE,
				DW.PT_CODE)
				AND (DW.HLT_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
					AND PARAMETER_NAME = 'INT_HLT')
				OR DW.SOC_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
					AND PARAMETER_NAME = 'INT_SOC'))
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	
		)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			WHERE
				TRY_TO_NUMBER(REACTMEDDRAPT_CODE) =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
					AND PARAMETER_NAME = 'INT_PT')
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				AR.ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query DW,
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR 
,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				--	AND DM.VERSION_FLAG = 1
				AND  
TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE) = DW.TERM_CODE
				AND TERM_LEVEL = 4
				--	AND AR.RECORD_DELETED = 0
				AND SMQ_CODE IN 
(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
					AND PARAMETER_NAME IN ('INT_SMQ', 'INT_CMQ'))
					AND DW.TERM_SCOPE IN (
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '1'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '1'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
				UNION
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '2'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_INTERACTION_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '2'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
  )
					AND 
                      (
                        ARI_REC_ID IN (
					SELECT
						ARI_REC_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
					OR RECORD_ID IN (
					SELECT
						RECORD_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

  
  )
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')

  )
  )
  T
WHERE
	coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID(+),-1)
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12',
	'YYYY-DD-MM')
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID IN (
	SELECT
				ARI_REC_ID
	FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        );

UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	DER_LACK_OF_EFFECT_FLAG = CASE
			WHEN t.ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END

FROM
	(
	SELECT
		ARI_REC_ID
	FROM
		(
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				AND DM.EXPIRY_DATE = TO_DATE('9999-31-12',
				'YYYY-DD-MM')
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),
				-1)= NVL(DW.PT_CODE,
				-1)
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),
				TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE))= NVL(DW.LLT_CODE,
				DW.PT_CODE)
				AND (DW.HLT_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
					AND PARAMETER_NAME = 'LOE_HLT')
				OR DW.SOC_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
					AND PARAMETER_NAME = 'LOE_SOC'))
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	
		)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			WHERE
				TRY_TO_NUMBER(REACTMEDDRAPT_CODE) =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
					AND PARAMETER_NAME = 'LOE_PT')
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				AR.ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query DW,
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR 
,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				--	AND DM.VERSION_FLAG = 1
				AND  
TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE) = DW.TERM_CODE
				AND TERM_LEVEL = 4
				--	AND AR.RECORD_DELETED = 0
				AND SMQ_CODE IN 
(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
					AND PARAMETER_NAME IN ('LOE_SMQ', 'LOE_CMQ'))
					AND DW.TERM_SCOPE IN (
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '1'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '1'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
				UNION
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '2'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_LACK_OF_EFFECT_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '2'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
  )
					AND 
                      (
                        ARI_REC_ID IN (
					SELECT
						ARI_REC_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
					OR RECORD_ID IN (
					SELECT
						RECORD_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

  
  )
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')

  )
  )
  T
WHERE
	coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1)= coalesce(t.ARI_REC_ID(+),-1)
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12',
	'YYYY-DD-MM')
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID IN (
	SELECT
				ARI_REC_ID
	FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        );

UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	DER_OVERDOSE_FLAG = CASE
			WHEN t.ARI_REC_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END
FROM
	(
	SELECT
		ARI_REC_ID
	FROM
		(
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				AND DM.EXPIRY_DATE = TO_DATE('9999-31-12',
				'YYYY-DD-MM')
				AND NVL(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),
				-1)= NVL(DW.PT_CODE,
				-1)
				AND NVL(AR.REACTMEDDRALLT_CODE,
				AR.REACTMEDDRAPT_CODE)= NVL(DW.LLT_CODE,
				DW.PT_CODE)
				AND (DW.HLT_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
					AND PARAMETER_NAME = 'OD_HLT')
				OR DW.SOC_CODE =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
					AND PARAMETER_NAME = 'OD_SOC'))
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	
		)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY LSMV_REACTION.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION
			WHERE
				TRY_TO_NUMBER(REACTMEDDRAPT_CODE) =(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
					AND PARAMETER_NAME = 'OD_PT')
				AND 
                      (
                        ARI_REC_ID IN (
				SELECT
					ARI_REC_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
				OR RECORD_ID IN (
				SELECT
					RECORD_ID
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

)
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')
	UNION
		SELECT
			ARI_REC_ID
		FROM
			(
			SELECT
				AR.ARI_REC_ID,
				CDC_OPERATION_TYPE,
				ROW_NUMBER() OVER (
                        PARTITION BY AR.RECORD_ID
			ORDER BY
				CDC_OPERATION_TIME DESC
                      ) RANK
			FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_query DW,
				${stage_db_name}.${stage_schema_name}.LSMV_REACTION AR 
,
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
			WHERE
				DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
				--	AND DM.VERSION_FLAG = 1
				AND  
TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE) = DW.TERM_CODE
				AND TERM_LEVEL = 4
				--	AND AR.RECORD_DELETED = 0
				AND SMQ_CODE IN 
(
				SELECT
					DEFAULT_VALUE_INT
				FROM
					${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
				WHERE
					TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
					AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
					AND PARAMETER_NAME IN ('OD_SMQ', 'OD_CMQ'))
					AND DW.TERM_SCOPE IN (
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '1'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '1'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
				UNION
					SELECT
						CASE
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'BROAD' 
	THEN '2'
							WHEN 
	COALESCE(DEFAULT_VALUE_CHAR,
							(
							SELECT
								DEFAULT_VALUE_CHAR
							FROM
								${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
							WHERE
								TABLE_NAME = 'LS_DB_FACT_RELATED_LABEL_CNTRY'
								AND COLUMN_NAME = 'DER_OVERDOSE_FLAG'
								AND PARAMETER_NAME = 'BROAD_NARROW')) = 'NARROW' 
	THEN '2'
							ELSE '2'
						END AS BROAD_NARROW
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
					WHERE
						PARAMETER_NAME = 'GENERIC_BROAD_NARROW'
  )
					AND 
                      (
                        ARI_REC_ID IN (
					SELECT
						ARI_REC_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
					OR RECORD_ID IN (
					SELECT
						RECORD_ID
					FROM
						${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        )
                      )	

  
  )
		WHERE
			RANK = 1
			AND CDC_OPERATION_TYPE IN ('I', 'U')

  )
  )
  T
WHERE
	coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(t.ARI_REC_ID(+),-1)
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12',
	'YYYY-DD-MM')
	AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID IN (
	SELECT
				ARI_REC_ID
	FROM
				${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC
                        );


UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	LS_DB_FACT_RELATED_LABEL_CNTRY.DERIVED_EXPECT_INT_COMBINED=LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DERIVED_EXPECT_INT_COMBINED
FROM (	
	
WITh AER_LABELLING_S as (
SELECT DISTINCT
    ls_drug_react_listedness.ari_rec_id ,
    LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id AS seq_product,
    LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id   AS seq_react,
    ls_drug_react_listedness.country         AS country_code,
    ls_drug_react_listedness.is_listed       AS labelled
FROM
    ( select ari_rec_id,country,is_listed,fk_adrr_rec_id from 
	    (select ari_rec_id,country,is_listed,fk_adrr_rec_id,CDC_OPERATION_TYPE
			, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
		FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness 
         where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
		
		)
	    WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U') and ari_rec_id IS NOT NULL and country in ('1','2','4') 
	) ls_drug_react_listedness
    INNER JOIN 
	( select record_id,fk_drug_rec_id,fk_ar_rec_id from 
	    (select record_id,fk_drug_rec_id,fk_ar_rec_id,CDC_OPERATION_TYPE
			, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
		FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
         where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
		
		)
	    WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
	) LSMV_DRUG_REACT_RELATEDNESS ON coalesce(ls_drug_react_listedness.fk_adrr_rec_id,-1) = coalesce(LSMV_DRUG_REACT_RELATEDNESS.record_id,-1)


)


SELECT ari_rec_id,LISTAGG(DERIVED_EXPECT_INTERNATIONAL,'\n') within group (order by ari_rec_id,class desc)  DERIVED_EXPECT_INT_COMBINED
              ,seq_react
from
(
select ari_rec_id,case when class='BN' then '@\n'||listagg(DERIVED_EXPECT_INTERNATIONAL,'|') within group (order by ari_rec_id,RANK_ORDER,class)
else listagg(DERIVED_EXPECT_INTERNATIONAL,'|') within group (order by ari_rec_id,RANK_ORDER,class) end DERIVED_EXPECT_INTERNATIONAL,
SEQ_REACT,class
 from (
(
SELECT
		ari_rec_id,SEQ_REACT,LISTAGG( DERIVED_EXPECT_INTERNATIONAL,'\r\n')within group (order by ari_rec_id,SEQ_REACT,RANK_ORDER) as DERIVED_EXPECT_INTERNATIONAL ,
		RANK_ORDER,
		'UN' class		
FROM
(		
SELECT
		ari_rec_id,SEQ_REACT,		
		 DERIVED_EXPECT_INTERNATIONAL,RANK_ORDER
		FROM
		(
			SELECT DISTINCT
				ari_rec_id , SEQ_PRODUCT,SEQ_REACT ,
				RANK_ORDER,
				CASE WHEN T.DERIVED_EXPECT_INTERNATIONAL_DSUR IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_DSUR 
				WHEN T.DERIVED_EXPECT_INTERNATIONAL_IB IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_IB
				WHEN T.DERIVED_EXPECT_INTERNATIONAL_CORE IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_CORE
				ELSE 'D'||T.RANK_ORDER||': '||'-'
				END AS DERIVED_EXPECT_INTERNATIONAL 
			FROM
			(
			 SELECT ari_rec_id, SEQ_REACT, SEQ_PRODUCT,RANK_ORDER,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_DSUR) AS DERIVED_EXPECT_INTERNATIONAL_DSUR,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_IB) AS DERIVED_EXPECT_INTERNATIONAL_IB,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_CORE) AS DERIVED_EXPECT_INTERNATIONAL_CORE
			 FROM
			 (
				SELECT DISTINCT 
					C.ari_rec_id, C.SEQ_REACT, C.SEQ_PRODUCT,C.COUNTRY_CODE, C.LABELLED, A.RANK_ORDER,		
				CASE
				   when  C.COUNTRY_CODE IN ('4') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('4') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				   end DERIVED_EXPECT_INTERNATIONAL_DSUR,
				CASE
				   when  C.COUNTRY_CODE IN ('2') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('2') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				   END DERIVED_EXPECT_INTERNATIONAL_IB,
				CASE
				   when  C.COUNTRY_CODE IN ('1') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('1') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				  END DERIVED_EXPECT_INTERNATIONAL_CORE
				FROM
					(SELECT ari_rec_id,record_id SEQ_PRODUCT,rank_order FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP 
					WHERE DRUGCHARACTERIZATION IN ('1','3') AND ari_rec_id||'-'||record_id NOT IN 
					(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is not null)
                     )A,
					(SELECT ari_rec_id,record_id as SEQ_REACT FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP ) B,
					AER_LABELLING_S C
					 WHERE 
					C.ari_rec_id=A.ari_rec_id(+)
					AND C.ari_rec_id=B.ari_rec_id(+)
					AND C.SEQ_REACT=B.SEQ_REACT(+)
					AND C.SEQ_PRODUCT=A.SEQ_PRODUCT(+)
			) Z GROUP BY ari_rec_id, SEQ_REACT, SEQ_PRODUCT,RANK_ORDER
			) T ORDER BY ari_rec_id,RANK_ORDER
		) A
	WHERE DERIVED_EXPECT_INTERNATIONAL IS NOT NULL 
	group by  ari_rec_id,SEQ_REACT,		
		 DERIVED_EXPECT_INTERNATIONAL,RANK_ORDER
) final_out
group by ari_rec_id,SEQ_REACT,RANK_ORDER
)

UNION ALL

(
SELECT
		ari_rec_id,SEQ_REACT,LISTAGG( DERIVED_EXPECT_INTERNATIONAL,'\r\n')within group (order by ari_rec_id,SEQ_REACT,RANK_ORDER) as DERIVED_EXPECT_INTERNATIONAL ,
		RANK_ORDER,
		'BN' class		
FROM
(		
SELECT
		ari_rec_id,SEQ_REACT,		
		 DERIVED_EXPECT_INTERNATIONAL,RANK_ORDER
		FROM
		(
			SELECT DISTINCT
				ari_rec_id , SEQ_PRODUCT,SEQ_REACT ,
				RANK_ORDER,
				CASE WHEN T.DERIVED_EXPECT_INTERNATIONAL_DSUR IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_DSUR 
				WHEN T.DERIVED_EXPECT_INTERNATIONAL_IB IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_IB
				WHEN T.DERIVED_EXPECT_INTERNATIONAL_CORE IS NOT NULL THEN DERIVED_EXPECT_INTERNATIONAL_CORE
				ELSE 'D'||T.RANK_ORDER||': '||'-'
				END AS DERIVED_EXPECT_INTERNATIONAL 
			FROM
			(
			 SELECT ari_rec_id, SEQ_REACT, SEQ_PRODUCT,RANK_ORDER,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_DSUR) AS DERIVED_EXPECT_INTERNATIONAL_DSUR,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_IB) AS DERIVED_EXPECT_INTERNATIONAL_IB,
			 MIN(DERIVED_EXPECT_INTERNATIONAL_CORE) AS DERIVED_EXPECT_INTERNATIONAL_CORE
			 FROM
			 (
				SELECT DISTINCT 
					C.ari_rec_id, C.SEQ_REACT, C.SEQ_PRODUCT,C.COUNTRY_CODE, C.LABELLED, A.RANK_ORDER,		
				CASE
				   when  C.COUNTRY_CODE IN ('4') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('4') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				   end DERIVED_EXPECT_INTERNATIONAL_DSUR,
				CASE
				   when  C.COUNTRY_CODE IN ('2') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('2') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				   END DERIVED_EXPECT_INTERNATIONAL_IB,
				CASE
				   when  C.COUNTRY_CODE IN ('1') and C.LABELLED='1' then 'D'||A.RANK_ORDER||': '||'Listed'
				   when C.COUNTRY_CODE IN ('1') and C.LABELLED IN ('0','2') then 'D'||A.RANK_ORDER||': '||'Unlisted'
				  END DERIVED_EXPECT_INTERNATIONAL_CORE
				FROM
					(SELECT ari_rec_id,record_id SEQ_PRODUCT,rank_order FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP 
					WHERE DRUGCHARACTERIZATION IN ('1','3') AND ari_rec_id||'-'||record_id  IN 
					(select ARI_REC_ID||'-'||SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
					where SEQ_UNBLINDED is null)) A,
					(SELECT ari_rec_id,record_id as SEQ_REACT FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP ) B,
					AER_LABELLING_S C
					WHERE  C.ari_rec_id=A.ari_rec_id(+)
					AND C.ari_rec_id=B.ari_rec_id(+)
					AND C.SEQ_REACT=B.SEQ_REACT(+)
					AND C.SEQ_PRODUCT=A.SEQ_PRODUCT(+)
			) Z GROUP BY ari_rec_id, SEQ_REACT, SEQ_PRODUCT,RANK_ORDER
			) T ORDER BY ari_rec_id,RANK_ORDER
		) A
	WHERE DERIVED_EXPECT_INTERNATIONAL IS NOT NULL 
	group by  ari_rec_id,SEQ_REACT,		
		 DERIVED_EXPECT_INTERNATIONAL,RANK_ORDER
) final_out
group by ari_rec_id,SEQ_REACT,RANK_ORDER
)
 )
  group by ari_rec_id,class ,SEQ_REACT
  order by  ari_rec_id,class desc
   )
  group by ari_rec_id ,SEQ_REACT

  ) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL 
WHERE 
  coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.ARI_REC_ID,-1) 
  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_REACT,-1)  
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD COLUMN DER_SERIOUSNESS_LISTEDNESS TEXT(10);
  
  
UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	LS_DB_FACT_RELATED_LABEL_CNTRY.DER_SERIOUSNESS_LISTEDNESS=LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SERIOUSNESS_LISTEDNESS
FROM (
SELECT
                AE.ARI_REC_ID,
                AE.SEQ_REACT,
                AE.SEQ_PRODUCT,
                AL.SEQ_LABEL,
                CASE
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED='0' THEN 'S/UL'
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED='1' THEN 'S/L'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED='0' THEN 'NS/UL'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED='1' THEN 'NS/L'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED IS NULL THEN 'NS'
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED IS NULL THEN 'S'
                                WHEN AE.SERIOUSNESS IS NULL AND AL.LABELLED='1' THEN 'L'
                                WHEN AE.SERIOUSNESS IS NULL AND AL.LABELLED='0' THEN 'UL'
                                ELSE NULL
                END DER_SERIOUSNESS_LISTEDNESS
FROM
                (
                SELECT A.ARI_REC_ID,P.SEQ_PRODUCT,R.record_id SEQ_REACT,R.SERIOUSNESS
                FROM
                                ( select ARI_REC_ID from 
									(select record_id ARI_REC_ID,CDC_OPERATION_TYPE
										, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
									FROM ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM where 
									record_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
									
									)
									WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
								) A,
                                (SELECT ari_rec_id,record_id SEQ_PRODUCT,rank_order FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP 
									WHERE DRUGCHARACTERIZATION IN ('1','3') AND ari_rec_id||'-'||record_id NOT IN 
									(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
										where SEQ_UNBLINDED is not null
										)
								) P,
                                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP R
                WHERE
                                A.ari_rec_id=P.ari_rec_id (+) AND
                                A.ari_rec_id=R.ari_rec_id(+) 
                               
                ) AE,       
                (
						SELECT DISTINCT
							ls_drug_react_listedness.ari_rec_id ,
							LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id AS seq_product,
							LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id  AS seq_react,
							ls_drug_react_listedness.country         AS country_code,
							ls_drug_react_listedness.is_listed       AS labelled,
							seq_label
						FROM
							( select record_id as seq_label,ari_rec_id,country,is_listed,fk_adrr_rec_id from 
								(select record_id,ari_rec_id,country,is_listed,fk_adrr_rec_id,CDC_OPERATION_TYPE
									, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
								FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness 
								where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
								
								)
								WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U') and ari_rec_id IS NOT NULL and country in ('US') 
							) ls_drug_react_listedness
							INNER JOIN 
							( select record_id,fk_drug_rec_id,fk_ar_rec_id from 
								(select record_id,fk_drug_rec_id,fk_ar_rec_id,CDC_OPERATION_TYPE
									, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
								FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
								where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
								
								)
								WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
							) LSMV_DRUG_REACT_RELATEDNESS ON ls_drug_react_listedness.fk_adrr_rec_id = LSMV_DRUG_REACT_RELATEDNESS.record_id
						) AL
				WHERE AE.ARI_REC_ID=AL.ARI_REC_ID(+) AND
                AE.SEQ_PRODUCT=AL.SEQ_PRODUCT(+) AND
                AE.SEQ_REACT=AL.SEQ_REACT(+)
	) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL 
WHERE 
	  LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID  = LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.ARI_REC_ID  
	AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_REACT,-1)  
  AND  coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_PRODUCT,-1)
  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_LABEL,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_LABEL,-1)  
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;


----------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD DER_CASEPROD_MAX_SRL_FLAG BIGINT ;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY SET DER_CASEPROD_MAX_SRL_FLAG=
CASE WHEN DER_CASEPROD_MAX_SRL_SUBSET.DER_CASEPROD_MAX_SRL=LS_DB_FACT_RELATED_LABEL_CNTRY.DER_PPD_SRL_RANK THEN 0 ELSE 1 END
  FROM
  (
   SELECT ARI_REC_ID,SEQ_PRODUCT,max(DER_PPD_SRL_RANK) DER_CASEPROD_MAX_SRL 
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY WHERE DER_PPD_SRL_RANK IS NOT NULL 
  and
              LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  AND            LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
  
  GROUP BY ARI_REC_ID,SEQ_PRODUCT
  ) DER_CASEPROD_MAX_SRL_SUBSET
WHERE   LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID =DER_CASEPROD_MAX_SRL_SUBSET.ARI_REC_ID(+)
AND 	LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT =DER_CASEPROD_MAX_SRL_SUBSET.SEQ_PRODUCT(+)
AND     LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
AND     LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC);          
    



	
----------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD DER_PRD_LISTED BIGINT ;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY SET DER_PRD_LISTED=
CASE WHEN DER_PRD_LISTED_SUBSET.ARI_REC_ID IS NOT NULL  THEN 1 ELSE 0 END
  FROM
  (
  SELECT ARI_REC_ID,SEQ_PRODUCT,INTEGRATION_ID 
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
  WHERE SEQ_PRODUCT IS NOT NULL 
  AND DER_EXPECT_INTERNATIONAL='Unlisted'
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
) DER_PRD_LISTED_SUBSET
WHERE LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID =DER_PRD_LISTED_SUBSET.ARI_REC_ID(+)
AND     LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT =DER_PRD_LISTED_SUBSET.SEQ_PRODUCT(+)
AND     LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
AND     LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC);          


--------------------------------------------------------------------------------------------------------------------------------------

--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD DER_CASE_SER BIGINT ;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY SET DER_CASE_SER=
CASE WHEN DER_CASE_SER_SUBSET.ARI_REC_ID IS NOT NULL  THEN 1 ELSE 0 END
FROM
(
SELECT ARI_REC_ID,RECORD_ID FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.DER_CASE_EVENT_SERIOUSNESS WHERE DER_CASE_EVENT_SERIOUSNESS='Serious') DER_CASE_SER_SUBSET
WHERE LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID =DER_CASE_SER_SUBSET.ARI_REC_ID(+)
AND   LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
AND   LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC);    


------------------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD DER_PRD_REL BIGINT ;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY SET DER_PRD_REL=
CASE WHEN DER_PRD_REL_SUBSET.ARI_REC_ID IS NOT NULL  THEN 1 ELSE 0 END
  FROM
  (
  SELECT ARI_REC_ID,SEQ_PRODUCT,INTEGRATION_ID 
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY 
  WHERE SEQ_PRODUCT IS NOT NULL 
  AND DER_COMBINED_RELATEDNESS='Related'
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
) DER_PRD_REL_SUBSET
WHERE LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID =DER_PRD_REL_SUBSET.ARI_REC_ID(+)
AND   LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT =DER_PRD_REL_SUBSET.SEQ_PRODUCT(+)  
AND   LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
AND   LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC);          



------------------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD COLUMN DER_PRODUCT_ID BIGINT;

UPDATE
              ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
LS_DB_FACT_RELATED_LABEL_CNTRY.DER_PRODUCT_ID=LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_PRODUCT_ID
FROM (SELECT DISTINCT
                                                          LSMV_DRUG.record_id as SEQ_PRODUCT,LSMV_PRODUCT.product_id as DER_PRODUCT_ID
                                           FROM
                                                          ( select record_id,product_id from 
                                                                        (select record_id,product_id,CDC_OPERATION_TYPE
                                                                                      , row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
                                                                        FROM ${stage_db_name}.${stage_schema_name}.lsmv_product
                                  --                                      where   ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
                                                                        
                                                                        )
                                                                        WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U') 
                                                          ) LSMV_PRODUCT
                                                          INNER JOIN 
                                                          ( select ARI_REC_ID, record_id,product_record_id from 
                                                                        (select ARI_REC_ID,record_id,product_record_id,CDC_OPERATION_TYPE
                                                                                      , row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
                                                                        FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
                                                                        where   ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
                                                                        
                                                                        )
                                                                        WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
                                                          ) LSMV_DRUG ON LSMV_PRODUCT.record_id = LSMV_DRUG.product_record_id
              ) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL 
WHERE  LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT = LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_PRODUCT
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;




------------------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD COLUMN DER_PRIMARY_EVENT_WID BIGINT;

UPDATE
              ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
LS_DB_FACT_RELATED_LABEL_CNTRY.DER_PRIMARY_EVENT_WID=LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_PRIMARY_EVENT_WID
FROM (
                                           SELECT distinct 
                                                          SEQ_REACT,BK_MEDDRA_ICD_WID as DER_PRIMARY_EVENT_WID

                                           FROM (select ARI_REC_ID,record_id as SEQ_REACT,REACTMEDDRALLT_CODE,REACTMEDDRAPT_CODE,CDC_OPERATION_TYPE
                                                                                      , row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
                                                                        FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
                                                                        where  ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
                                                          )AR,
                                                          ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD DW,
                                                         ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION DM
                                           WHERE
                                                          DW.MEDDRA_VERSION = DM.MEDDRA_VERSION
                                                          AND DM.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') and DW.PRIMARY_SOC_FG='Y'
                                                          AND try_to_number(COALESCE(TRY_TO_NUMBER(AR.REACTMEDDRALLT_CODE),TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE)),38)=try_to_number(DW.LLT_CODE,38) 
                                                          and try_to_number(TRY_TO_NUMBER(AR.REACTMEDDRAPT_CODE),38)=try_to_number(DW.PT_CODE,38)
                                                          AND ARI_REC_ID IN (      SELECT              ARI_REC_ID FROM            ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC   )
                                     and RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
              ) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL 
WHERE  LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT = LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_REACT
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

    

    
UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
SET
	LS_DB_FACT_RELATED_LABEL_CNTRY.DER_SL_EVENT_INCIDENT=LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.DER_SL_EVENT_INCIDENT
FROM (


SELECT ARI_REC_ID , SEQ_REACT , SEQ_PRODUCT, MAX(DER_SL_EVENT_INCIDENT) DER_SL_EVENT_INCIDENT FROM
(
SELECT
                AE.ARI_REC_ID,
                AE.SEQ_REACT,
                AE.SEQ_PRODUCT,
                AL.SEQ_LABEL,
                CASE
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED='0' THEN 'S/UL'
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED='1' THEN 'S/L'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED='0' THEN 'NS/UL'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED='1' THEN 'NS/L'
                                WHEN AE.SERIOUSNESS='2' AND AL.LABELLED IS NULL THEN 'NS'
                                WHEN AE.SERIOUSNESS='1' AND AL.LABELLED IS NULL THEN 'S'
                                WHEN AE.SERIOUSNESS IS NULL AND AL.LABELLED='1' THEN 'L'
                                WHEN AE.SERIOUSNESS IS NULL AND AL.LABELLED='0' THEN 'UL'
                                ELSE NULL
                END DER_SL_EVENT_INCIDENT
FROM
                (
                SELECT A.ARI_REC_ID,P.SEQ_PRODUCT,R.record_id SEQ_REACT,R.SERIOUSNESS
                FROM
                                ( select ARI_REC_ID from 
									(select record_id ARI_REC_ID,CDC_OPERATION_TYPE
										, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
									FROM ${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM where 
									record_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
									
									)
									WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
								) A,
                                (SELECT ari_rec_id,record_id SEQ_PRODUCT,rank_order FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP 
									WHERE DRUGCHARACTERIZATION IN ('1','3','10') AND  PRODUCT_TYPE='4'  AND ari_rec_id||'-'||record_id NOT IN 
									(select ARI_REC_ID||'-'||SEQ_UNBLINDED from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CASE_UNBLINDED
										where SEQ_UNBLINDED is not null
										)
								) P,
                                (select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_reaction_SUBSET_TMP where event_type='1') R
                WHERE
                                A.ari_rec_id=P.ari_rec_id (+) AND
                                A.ari_rec_id=R.ari_rec_id(+) 
                               
                ) AE,       
                (
						SELECT DISTINCT
							ls_drug_react_listedness.ari_rec_id ,
							LSMV_DRUG_REACT_RELATEDNESS.fk_drug_rec_id AS seq_product,
							LSMV_DRUG_REACT_RELATEDNESS.fk_ar_rec_id  AS seq_react,
							ls_drug_react_listedness.country         AS country_code,
							ls_drug_react_listedness.is_listed       AS labelled,
							seq_label
						FROM
							( select record_id as seq_label,ari_rec_id,country,is_listed,fk_adrr_rec_id from 
								(select record_id,ari_rec_id,country,is_listed,fk_adrr_rec_id,CDC_OPERATION_TYPE
									, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
								FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_listedness 
								where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
								
								)
								WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U') and ari_rec_id IS NOT NULL and country in ('US') 
							) ls_drug_react_listedness
							INNER JOIN 
							( select record_id,fk_drug_rec_id,fk_ar_rec_id from 
								(select record_id,fk_drug_rec_id,fk_ar_rec_id,CDC_OPERATION_TYPE
									, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
								FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_REACT_RELATEDNESS 
								where	ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
								
								)
								WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
							) LSMV_DRUG_REACT_RELATEDNESS ON ls_drug_react_listedness.fk_adrr_rec_id = LSMV_DRUG_REACT_RELATEDNESS.record_id
						) AL
				WHERE AE.ARI_REC_ID=AL.ARI_REC_ID(+) AND
                AE.SEQ_PRODUCT=AL.SEQ_PRODUCT(+) AND
                AE.SEQ_REACT=AL.SEQ_REACT(+)
	)
	group by 1,2,3
	
	
	) LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL 
WHERE 
   LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID  = LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.ARI_REC_ID  
  AND coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_REACT,-1)  
  AND  coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_PRODUCT,-1) = coalesce(LS_DB_FACT_RELATED_LABEL_CNTRY_FINAL.SEQ_PRODUCT,-1)
  AND LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

  
--ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY ADD COLUMN DER_ADVERSE_EXPERIENCE VARCHAR(4000);
  

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY   
SET LS_DB_FACT_RELATED_LABEL_CNTRY.DER_ADVERSE_EXPERIENCE=T.DER_ADVERSE_EXPERIENCE
FROM
(
SELECT distinct
    AE.ari_rec_id
    ,APRL.SEQ_REACT
    ,APRL.LLT_CODE||' '||'('||APRL.PT_CODE||')' as DER_ADVERSE_EXPERIENCE
FROM
    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC AE,
       (
		SELECT  
		AP.AER_ID AER_ID_PROD,
		AR.AER_ID AER_ID_EVENT,
		AP.SEQ_PRODUCT,
		AR.SEQ_REACT,
		AR.PT_CODE,
		AR.LLT_CODE,
		AR.AUTO_RANK
		FROM 
			(SELECT ARI_REC_ID as AER_ID,record_id as SEQ_PRODUCT from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP ) AP 
			FULL OUTER JOIN
		(SELECT ARI_REC_ID as AER_ID,RECORD_ID as SEQ_REACT,REACTMEDDRAPT_CODE as PT_CODE,REACTMEDDRALLT_CODE as LLT_CODE,RANK_ORDER AS AUTO_RANK
		FROM
		(SELECT ARI_REC_ID,RECORD_ID,RANK_ORDER,REACTMEDDRAPT_CODE,REACTMEDDRALLT_CODE,ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,CDC_OPERATION_TYPE FROM ${stage_db_name}.${stage_schema_name}.LSMV_REACTION
		WHERE ARI_REC_ID IN (SELECT ari_rec_id FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY_CASE_QFC)
		) AR_INNER 
		WHERE AR_INNER.RANK=1 AND AR_INNER.CDC_OPERATION_TYPE IN ('I','U')
		)AR
		ON
			AP.AER_ID=AR.AER_ID
       ) APRL 
      WHERE
	  AE.ari_rec_id=COALESCE(APRL.AER_ID_PROD(+),COALESCE(APRL.AER_ID_EVENT(+),-1))
	  ) T
	  WHERE LS_DB_FACT_RELATED_LABEL_CNTRY.ARI_REC_ID  = T.ARI_REC_ID 
	  AND   LS_DB_FACT_RELATED_LABEL_CNTRY.SEQ_REACT   = T.SEQ_REACT
	  AND   LS_DB_FACT_RELATED_LABEL_CNTRY.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM');

------------------------------------------------------------------------------------------------------------------------------------------
UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SET 
  LOAD_END_TS = current_timestamp, 
  LOAD_TS =(
    select 
      max(LOAD_TS) 
    from 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_FACT_RELATED_LABEL_CNTRY
  ), 
  LOAD_STATUS = 'Completed' 
where 
  target_table_name = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
  and LOAD_STATUS = 'In Progress' 
  and LOAD_START_TS =(
    select 
      max(LOAD_START_TS) 
    from 
      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
    where 
      target_table_name = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
      and LOAD_STATUS = 'In Progress'
  );
RETURN 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER Update completed';
EXCEPTION WHEN OTHER THEN LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
set 
  ERROR_DETAILS =: line, 
  LOAD_STATUS = 'Error' 
WHERE 
  target_table_name = 'LS_DB_FACT_RELATED_LABEL_CNTRY_DER' 
  and LOAD_STATUS = 'In Progress';
END;
$$;
