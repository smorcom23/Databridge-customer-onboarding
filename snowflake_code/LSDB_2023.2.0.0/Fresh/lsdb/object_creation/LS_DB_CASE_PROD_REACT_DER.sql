

/*
--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_REACT_DER()



*/
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_REACT_DER()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

--delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_PROD_REACT_DER';

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_PROD_REACT_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_PROD_REACT_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_PROD_REACT_DER'),
	'LSRA','Case','LS_DB_CASE_PROD_REACT_DER',null,CURRENT_TIMESTAMP(),null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_PROD_REACT_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_PROD_REACT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP()
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_PROD_REACT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  As 
select RECORD_ID,'lsmv_drug' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_reaction' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_reaction WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_receipt_item' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE CDC_OPERATION_TYPE IN ('D') ;

--select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP  AS
with LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
 WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_reaction  WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 


select DISTINCT record_id, 0 common_parent_key,  RECORD_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_receipt_item WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')

UNION 


select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_therapy 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_PROD_REACT_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')



)
 ,
lsmv_receipt_item_SUBSET AS 
(
select * from 
    (SELECT  record_id as ri_record_id,
    date_modified  as ri_date_modified,
	record_id as ari_rec_id  ,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_receipt_item 
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
record_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_receipt_item')
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
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
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
 --AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction')
  ) where REC_RANK=1  )
, LSMV_DRUG_THERAPY_SUBSET AS 
(
select * from 
    (SELECT  record_id,
    date_modified ,
	ari_rec_id  ,FK_AD_REC_ID,
	row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY  
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ari_rec_id IN (SELECT ari_rec_id FROM LSMV_CASE_NO_SUBSET))
 --AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='LSMV_DRUG_THERAPY')

  ) where REC_RANK=1  )  
 
 SELECT DISTINCT  
	to_date(GREATEST(NVL(lsmv_receipt_item_SUBSET.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(dr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(LSMV_DRUG_THERAPY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP() LOAD_TS	 
,lsmv_receipt_item_SUBSET.ri_record_id AS ARI_REC_ID
,apr.seq_product
,apr.seq_react
,GREATEST(NVL(lsmv_receipt_item_SUBSET.ri_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(dr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(rc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
	NVL(LSMV_DRUG_THERAPY_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
) date_modified
,CONCAT( NVL(lsmv_receipt_item_SUBSET.ri_RECORD_ID,-1),'||',
         NVL(apr.seq_product,-1),'||',
		 NVL(apr.seq_react,-1)
		 ) INTEGRATION_ID
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
	LEFT Join LSMV_DRUG_THERAPY_SUBSET 
	ON lsmv_receipt_item_SUBSET.ri_record_id=LSMV_DRUG_THERAPY_SUBSET.ARI_REC_ID
	AND APR.SEQ_PRODUCT = LSMV_DRUG_THERAPY_SUBSET.FK_AD_REC_ID 
    
;
 
 
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
create TEMPORARY table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE from 
    (
              SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE
              ,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
                                                                        order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
                                                                                       FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
                                                                                                                                 ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('1018','1014','1020','9037','805')
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
   
 
 

ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP ADD COLUMN DER_TREATMENT_DURATION_NORMAL TEXT;
 
 
UPDATE 
  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP 
SET 
  LS_DB_CASE_PROD_REACT_DER_TMP.DER_TREATMENT_DURATION_NORMAL = COALESCE(
    LS_DB_CASE_PROD_REACT_DER_FINAL.DER_TREATMENT_DURATION_NORMAL, 
    LS_DB_CASE_PROD_REACT_DER_FINAL.DURATION_ROUND
  ) 
FROM 
  (
    select 
      ARI_REC_ID, 
      SEQ_PRODUCT, 
      SEQ_REACT, 
      ceil(
        MAX(DURATION)
      ) DURATION_ROUND, 
      cast (
        case when ROUND(
          MAX(
            to_date(
              to_char(END_THERAPY_DATE, 'dd-mm-yyyy'), 
              'dd-mm-yyyy'
            )
          )-(
            MIN(
              to_date(
                to_char(
                  START_THERAPY_DATE, 'dd-mm-yyyy'
                ), 
                'dd-mm-yyyy'
              )
            )
          )
        ) >= 0 then ROUND(
          MAX(
            to_date(
              to_char(END_THERAPY_DATE, 'dd-mm-yyyy'), 
              'dd-mm-yyyy'
            )
          )-(
            MIN(
              to_date(
                to_char(
                  START_THERAPY_DATE, 'dd-mm-yyyy'
                ), 
                'dd-mm-yyyy'
              )
            )
          )
        ) when ROUND(
          MAX(
            to_date(
              to_char(END_THERAPY_DATE, 'dd-mm-yyyy'), 
              'dd-mm-yyyy'
            )
          )-(
            MIN(
              to_date(
                to_char(
                  START_THERAPY_DATE, 'dd-mm-yyyy'
                ), 
                'dd-mm-yyyy'
              )
            )
          )
        )< 0 then null end as integer
      ) DER_TREATMENT_DURATION_NORMAL 
    from 
      (
        SELECT 
          APT.ARI_REC_ID, 
          APT.SEQ_PRODUCT, 
          AR.SEQ_REACT, 
          case when APT.END_THERAPY_DATE_PRECISION not in (1, 2, 610, 602) then APT.END_THERAPY_DATE else null end END_THERAPY_DATE, 
          case when APT.START_THERAPY_DATE_PRECISION not in (1, 2, 610, 602) then APT.START_THERAPY_DATE else null end START_THERAPY_DATE, 
          case when APT.THERAPY_DURATION_UNIT in ('804') then cast (APT.THERAPY_DURATION as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('803') then cast (APT.THERAPY_DURATION * 7 as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('802') then cast (APT.THERAPY_DURATION * 30 as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('801') then cast (APT.THERAPY_DURATION * 365 as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('805') then cast (APT.THERAPY_DURATION / 24 as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('806') then cast (APT.THERAPY_DURATION * 0.000694444 as numeric
          ) when APT.THERAPY_DURATION_UNIT in ('807') then cast (APT.THERAPY_DURATION * 0.0000115740666667 as numeric
          ) else cast (null as numeric) end DURATION 
        FROM 
          (
            SELECT 
              DISTINCT ARI_REC_ID ARI_REC_ID, 
              FK_AD_REC_ID AS SEQ_PRODUCT, 
              RECORD_ID AS SEQ_THERAPY, 
              DRUGSTARTDATEFMT START_THERAPY_DATE_PRECISION, 
              DRUGSTARTDATE START_THERAPY_DATE, 
              DRUGENDDATEFMT END_THERAPY_DATE_PRECISION, 
              DRUGENDDATE END_THERAPY_DATE, 
              DRUGSTARTDATE_NF AS START_THERAPY_DATE_NF, 
              DRUGENDDATE_NF AS END_THERAPY_DATE_NF, 
              regexp_replace(DRUGADMINDURATION,'[a-z/-/A-Z/.]', '') AS THERAPY_DURATION, 
              DRUGADMINDURATIONUNIT AS THERAPY_DURATION_UNIT 
            FROM 
              (
                select 
                  ARI_REC_ID, 
                  FK_AD_REC_ID, 
                  RECORD_ID, 
                  DRUGSTARTDATEFMT, 
                  DRUGSTARTDATE, 
                  DRUGENDDATEFMT, 
                  DRUGENDDATE, 
                  DRUGSTARTDATE_NF, 
                  DRUGENDDATE_NF, 
                  DRUGADMINDURATION, 
                  DRUGADMINDURATIONUNIT, 
                  CDC_OPERATION_TYPE, 
                  row_number() over (
                    partition by RECORD_ID 
                    order by 
                      CDC_OPERATION_TIME desc
                  ) rank 
                FROM 
                  ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
                where 
                  ARI_REC_ID in (
                    select 
                      ari_rec_id 
                    from 
                      ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP
                  )
              ) 
            Where 
              Rank = 1 
              and CDC_OPERATION_TYPE IN ('I', 'U')
          ) APT, 
          (
            select 
              RECORD_ID as SEQ_PRODUCT, 
              ARI_REC_ID, 
              DRUGCHARACTERIZATION, 
              RANK_ORDER, 
              CDC_OPERATION_TYPE, 
              row_number() over (
                partition by RECORD_ID 
                order by 
                  CDC_OPERATION_TIME desc
              ) rank 
            FROM 
              ${stage_db_name}.${stage_schema_name}.LSMV_DRUG 
            where 
              ARI_REC_ID in (
                select 
                  ARI_REC_ID 
                from 
                  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP
              ) 
              AND CDC_OPERATION_TYPE IN ('I', 'U')
          ) AP, 
          (
            SELECT 
              ARI_REC_ID, 
              RECORD_ID AS SEQ_REACT, 
              CDC_OPERATION_TYPE, 
              row_number() over (
                partition by RECORD_ID 
                order by 
                  CDC_OPERATION_TIME desc
              ) rank 
            FROM 
              ${stage_db_name}.${stage_schema_name}.LSMV_REACTION 
            where 
              ARI_REC_ID in (
                select 
                  ARI_REC_ID 
                from 
                  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP
              )
          ) AR 
        WHERE 
          AP.DRUGCHARACTERIZATION IN ('1', '3') 
          AND ap.rank = 1 
          AND AR.CDC_OPERATION_TYPE IN ('I', 'U') 
          AND AP.CDC_OPERATION_TYPE IN ('I', 'U') --AND AP.PRODUCT_TYPE='01'
          AND APT.ARI_REC_ID = AP.ARI_REC_ID 
          AND APT.SEQ_PRODUCT = AP.SEQ_PRODUCT 
          AND APT.ARI_REC_ID = AR.ARI_REC_ID 
          AND AR.rank = 1
      ) 
    GROUP BY 
      ARI_REC_ID, 
      SEQ_PRODUCT, 
      SEQ_REACT
  ) LS_DB_CASE_PROD_REACT_DER_FINAL 
WHERE 
  LS_DB_CASE_PROD_REACT_DER_TMP.ari_rec_id = LS_DB_CASE_PROD_REACT_DER_FINAL.ari_rec_id 
  and LS_DB_CASE_PROD_REACT_DER_TMP.SEQ_PRODUCT = LS_DB_CASE_PROD_REACT_DER_FINAL.SEQ_PRODUCT 
  and LS_DB_CASE_PROD_REACT_DER_TMP.SEQ_REACT = LS_DB_CASE_PROD_REACT_DER_FINAL.SEQ_REACT 
  AND LS_DB_CASE_PROD_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM');
 
 alter table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP ADD column DER_COMB_THERAPY_INFO TEXT;

UPDATE
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP
SET
	LS_DB_CASE_PROD_REACT_DER_TMP.DER_COMB_THERAPY_INFO=LS_DB_CASE_PROD_REACT_DER_FINAL.DER_COMB_THERAPY_INFO
FROM (
SELECT
		ARI_REC_ID,
		SEQ_PRODUCT,
		LISTAGG(DER_COMB_THERAPY_INFO,'\r\n')within group (order by SEQ_THERAPY) AS DER_COMB_THERAPY_INFO
FROM   
 (

	SELECT
		APT.ARI_REC_ID,
		APT.SEQ_PRODUCT,NVL(APT.DRUGSTRUCTUREDOSAGENUMB,'-')||' '|| NVL(UDU.DECODE,'-')||' | ('||NVL(APT.DRUGSEPARATEDOSAGENUMB,'-')||', '||NVL(APT.DRUGINTDOSAGEUNITNUMB,'-')||', '||NVL(FTU.DECODE,'-')||') | '||NVL(APT.DAILY_DOSE,'-')||' '||NVL(DDU.DECODE,'-')||' | '||NVL(NVL(ROA.DECODE,APT.DRUGADMINISTRATIONROUTE_SF),'-')||' | '||NVL(NVL(FOA.DECODE,APT.DRUGDOSAGEFORM_SF),'-') 
		AS DER_COMB_THERAPY_INFO
		,MIN(APT.record_id) AS SEQ_THERAPY
	FROM
		(select  record_id,ARI_REC_ID,FK_AD_REC_ID As SEQ_PRODUCT,DRUGSTRUCTUREDOSAGEUNIT,DRUGINTDOSAGEUNITDEF
			,COALESCE(DRUGDOSAGEFORM,DRUGDOSAGEFORM_NF) DRUGDOSAGEFORM,COALESCE(DRUGADMINISTRATIONROUTE,DRUGADMINISTRATIONROUTE_NF) DRUGADMINISTRATIONROUTE,DAILY_DOSE_UNIT
			,DRUGSTRUCTUREDOSAGENUMB,DRUGSEPARATEDOSAGENUMB,DRUGINTDOSAGEUNITNUMB,DAILY_DOSE,DRUGADMINISTRATIONROUTE_SF,DRUGDOSAGEFORM_SF
			from 
			(select record_id,ARI_REC_ID,FK_AD_REC_ID,DRUGSTRUCTUREDOSAGEUNIT,DRUGINTDOSAGEUNITDEF
			,DRUGDOSAGEFORM,DRUGDOSAGEFORM_NF,DRUGADMINISTRATIONROUTE,DRUGADMINISTRATIONROUTE_NF,DAILY_DOSE_UNIT
			,DRUGSTRUCTUREDOSAGENUMB,DRUGSEPARATEDOSAGENUMB,DRUGINTDOSAGEUNITNUMB,DAILY_DOSE,DRUGADMINISTRATIONROUTE_SF,DRUGDOSAGEFORM_SF
			,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG_THERAPY 
				 where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U')
		) APT,               
		(SELECT CODE,DECODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp WHERE CODELIST_ID=1018 ) UDU,
		(SELECT CODE,DECODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp WHERE CODELIST_ID=1014) FTU,
		(SELECT CODE,DECODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp WHERE CODELIST_ID=1018) DDU,
		(SELECT CODE,DECODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp WHERE CODELIST_ID IN (1020,9037)) ROA,
		(SELECT CODE,DECODE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp WHERE CODELIST_ID IN (805,9037) ) FOA
		WHERE	APT.DAILY_DOSE_UNIT=DDU.CODE(+) AND
		APT.DRUGSTRUCTUREDOSAGEUNIT=UDU.CODE(+) AND
		APT.DRUGINTDOSAGEUNITDEF=FTU.CODE(+) AND
		APT.DRUGDOSAGEFORM=FOA.CODE(+) AND
		APT.DRUGADMINISTRATIONROUTE=ROA.CODE(+)
	GROUP BY 
	APT.ARI_REC_ID,
		APT.SEQ_PRODUCT,NVL(APT.DRUGSTRUCTUREDOSAGENUMB,'-')||' '|| NVL(UDU.DECODE,'-')||' | ('||NVL(APT.DRUGSEPARATEDOSAGENUMB,'-')||', '||NVL(APT.DRUGINTDOSAGEUNITNUMB,'-')||', '||NVL(FTU.DECODE,'-')||') | '||NVL(APT.DAILY_DOSE,'-')||' '||NVL(DDU.DECODE,'-')||' | '||NVL(NVL(ROA.DECODE,APT.DRUGADMINISTRATIONROUTE_SF),'-')||' | '||NVL(NVL(FOA.DECODE,APT.DRUGDOSAGEFORM_SF),'-') 
	) Final_OutPut
	GROUP BY
		ARI_REC_ID,
		SEQ_PRODUCT) LS_DB_CASE_PROD_REACT_DER_FINAL 
WHERE 
  LS_DB_CASE_PROD_REACT_DER_TMP.ARI_REC_ID = LS_DB_CASE_PROD_REACT_DER_FINAL.ARI_REC_ID 
  AND LS_DB_CASE_PROD_REACT_DER_TMP.SEQ_PRODUCT = LS_DB_CASE_PROD_REACT_DER_FINAL.SEQ_PRODUCT
  AND LS_DB_CASE_PROD_REACT_DER_TMP.EXPIRY_DATE = TO_DATE('9999-31-12', 'YYYY-DD-MM')
  ;

 
 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP)
where target_table_name='LS_DB_CASE_PROD_REACT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_PROD_REACT_DER'
					and LOAD_STATUS = 'In Progress') 
; 


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER   
SET LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID = LS_DB_CASE_PROD_REACT_DER_TMP.ARI_REC_ID,
LS_DB_CASE_PROD_REACT_DER.seq_product = LS_DB_CASE_PROD_REACT_DER_TMP.seq_product,
LS_DB_CASE_PROD_REACT_DER.seq_react = LS_DB_CASE_PROD_REACT_DER_TMP.seq_react,
LS_DB_CASE_PROD_REACT_DER.DATE_MODIFIED = LS_DB_CASE_PROD_REACT_DER_TMP.DATE_MODIFIED,
LS_DB_CASE_PROD_REACT_DER.PROCESSING_DT = LS_DB_CASE_PROD_REACT_DER_TMP.PROCESSING_DT,
LS_DB_CASE_PROD_REACT_DER.expiry_date    =LS_DB_CASE_PROD_REACT_DER_TMP.expiry_date       ,
LS_DB_CASE_PROD_REACT_DER.load_ts        =LS_DB_CASE_PROD_REACT_DER_TMP.load_ts   ,
LS_DB_CASE_PROD_REACT_DER.DER_TREATMENT_DURATION_NORMAL   = LS_DB_CASE_PROD_REACT_DER_TMP.DER_TREATMENT_DURATION_NORMAL ,
LS_DB_CASE_PROD_REACT_DER.DER_COMB_THERAPY_INFO = LS_DB_CASE_PROD_REACT_DER_TMP.DER_COMB_THERAPY_INFO 
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP 
WHERE 	LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID = LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_PROD_REACT_DER_TMP.PROCESSING_DT = LS_DB_CASE_PROD_REACT_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_PROD_REACT_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CASE_PROD_REACT_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_PROD_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID ,LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP 
ON LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID=LS_DB_CASE_PROD_REACT_DER_TMP.ARI_REC_ID
AND LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID = LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_PROD_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP )
) TMP where LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_PROD_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID ,LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP 
    ON LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID=LS_DB_CASE_PROD_REACT_DER_TMP.ARI_REC_ID
    AND LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID = LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_PROD_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_PROD_REACT_DER.ARI_REC_ID in (select ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER
( 
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,
ari_rec_id,
seq_product,
seq_react,
DATE_MODIFIED,
DER_TREATMENT_DURATION_NORMAL,
DER_COMB_THERAPY_INFO

)
SELECT 
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,
ari_rec_id,
seq_product,
seq_react,
DATE_MODIFIED,
DER_TREATMENT_DURATION_NORMAL,
DER_COMB_THERAPY_INFO
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER TGT
WHERE  ( ari_rec_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_receipt_item') 
OR seq_product       in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug') 
OR seq_react         in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction') 
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_PROD_REACT_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_PROD_REACT_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_PROD_REACT_DER.INTEGRATION_ID = LS_DB_CASE_PROD_REACT_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_PROD_REACT_DER.ari_rec_id = LS_DB_CASE_PROD_REACT_DER_TMP.ari_rec_id
AND LS_DB_CASE_PROD_REACT_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CASE_PROD_REACT_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CASE_PROD_REACT_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( ARI_REC_ID  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_receipt_item') 
OR seq_product  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug') 
OR seq_react  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction') 
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_PROD_REACT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_PROD_REACT_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_PROD_REACT_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_PROD_REACT_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_PROD_REACT_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;