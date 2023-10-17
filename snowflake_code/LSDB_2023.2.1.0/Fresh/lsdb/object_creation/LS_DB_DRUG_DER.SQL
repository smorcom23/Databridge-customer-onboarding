-- call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_DER()

-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();


-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_DRUG_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_DRUG_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG ADD COLUMN DER_DRUG_NAME_CONCOMITANT TEXT;
*/






insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_DRUG_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_DRUG_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_DER'),
	'LSRA','Case','LS_DB_DRUG_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_DRUG_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG)
WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC AS
select distinct ari_rec_id as ari_rec_id
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_DRUG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


/*( select UNBLINDED_REC from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_UNBLINDED
		)*/


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID||'-'||AD.SEQ_PRODUCT AS UNBLINDED_REC
				from 
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
				
				) LSMV_DRUG 
				JOIN
				(
				SELECT LSMV_DRUG.ARI_REC_ID,
					LSMV_DRUG.RECORD_ID AS SEQ_PRODUCT,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
				 
				) AD
				ON LSMV_DRUG.ARI_REC_ID              = AD.ARI_REC_ID
				AND LSMV_DRUG.BLINDED_PRODUCT_REC_ID = AD.SEQ_PRODUCT
				AND LSMV_DRUG.rank=1 AND AD.rank=1 AND LSMV_DRUG.CDC_OPERATION_TYPE IN ('I','U') AND AD.CDC_OPERATION_TYPE IN ('I','U')

;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp;
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
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9120','8008')
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


drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP;
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP as 
select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,STUDY_PRODUCT_TYPE,SPR_ID from 
			(select record_id,ARI_REC_ID,INVESTIGATIONAL_PROD_BLINDED,DRUGCHARACTERIZATION,MEDICINALPRODUCT,RANK_ORDER,PREFERED_PRODUCT_description,BLINDED_PRODUCT_REC_ID,row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank 
				,CDC_OPERATION_TYPE,STUDY_PRODUCT_TYPE,CASE WHEN COALESCE(SPR_ID,'')= '' THEN '-9999' ELSE SPR_ID END AS SPR_ID
				FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug 
				where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC) 
			) where rank=1 AND CDC_OPERATION_TYPE IN ('I','U') ;
                  


 UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG   
SET LS_DB_DRUG.DER_BLINDED_PPD=LS_DB_DRUG_TMP.DER_BLINDED_PPD
FROM (
    SELECT
        non.record_id,
        blind.record_id   blind_record_id,
        blind.ari_rec_id ari_rec_id,
        blind.PREFERED_PRODUCT_DESCRIPTION as DER_BLINDED_PPD
FROM
    (select record_id,ari_rec_id,PREFERED_PRODUCT_DESCRIPTION,
     ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,
     CDC_OPERATION_TYPE
     from ${stage_db_name}.${stage_schema_name}.lsmv_drug where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC))  blind,
    (select record_id,ari_rec_id,blinded_product_rec_id,
     ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,
     CDC_OPERATION_TYPE
     from ${stage_db_name}.${stage_schema_name}.lsmv_drug where ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)) non
WHERE 
    blind.record_id = non.blinded_product_rec_id
    AND blind.ari_rec_id = non.ari_rec_id
    and blind.RANK=1 and non.RANK=1
    and blind.CDC_OPERATION_TYPE IN ('I','U') and non.CDC_OPERATION_TYPE IN ('I','U')
    ) LS_DB_DRUG_TMP
    WHERE LS_DB_DRUG.ARI_REC_ID = LS_DB_DRUG_TMP.ARI_REC_ID
        and LS_DB_DRUG.RECORD_ID=LS_DB_DRUG_TMP.record_id
    AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');                 
                  
                 
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG   
SET LS_DB_DRUG.DER_NON_BLINDED_RECORD_FLAG=LS_DB_DRUG_TMP.DER_NON_BLINDED_RECORD_FLAG
FROM (
  SELECT ARI_REC_ID,RECORD_ID,DER_NON_BLINDED_RECORD_FLAG,RANK,
CDC_OPERATION_TYPE
FROM
(
SELECT ARI_REC_ID,RECORD_ID,DER_NON_BLINDED_RECORD_FLAG,RANK,
CDC_OPERATION_TYPE
FROM
(
SELECT ARI_REC_ID ARI_REC_ID,
             RECORD_ID ,
             1 DER_NON_BLINDED_RECORD_FLAG,
             ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,
             CDC_OPERATION_TYPE 
      FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
      WHERE ARI_REC_ID||RECORD_ID NOT IN (SELECT B.ari_rec_id||B.BLINDED_PRODUCT_REC_ID
                                          FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG B
                                          WHERE BLINDED_PRODUCT_REC_ID IS NOT NULL 
                                          AND  ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
                                          )
       AND ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
       ) A where RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')
UNION ALL
SELECT ARI_REC_ID,RECORD_ID,DER_NON_BLINDED_RECORD_FLAG,RANK,CDC_OPERATION_TYPE
FROM
(     
 SELECT ARI_REC_ID,
             RECORD_ID,
             0 DER_NON_BLINDED_RECORD_FLAG,
             ROW_NUMBER() OVER (PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK,
             CDC_OPERATION_TYPE 
      FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG
      WHERE ARI_REC_ID||RECORD_ID IN (SELECT B.ari_rec_id||B.BLINDED_PRODUCT_REC_ID
                                      FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG B
                                      WHERE BLINDED_PRODUCT_REC_ID IS NOT NULL 
                                      AND ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
                                      )
                                      
      AND ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
) B where RANK=1 AND CDC_OPERATION_TYPE IN ('I','U')
)

    ) LS_DB_DRUG_TMP
    WHERE LS_DB_DRUG.ARI_REC_ID = LS_DB_DRUG_TMP.ARI_REC_ID
        and LS_DB_DRUG.RECORD_ID=LS_DB_DRUG_TMP.record_id
    AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



update ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG
set DER_PPD_WITH_CODING_CLASS=case when T.DER_PPD_WITH_CODING_CLASS is not null 
                                   then case when length(T.DER_PPD_WITH_CODING_CLASS)>=4000 
                                  then substring(T.DER_PPD_WITH_CODING_CLASS,0,3996)||' ...' else T.DER_PPD_WITH_CODING_CLASS end
                                   else null end
from
(
SELECT ARI_REC_ID,RECORD_ID,PREFERED_PRODUCT_DESCRIPTION,DER_PPD_WITH_CODING_CLASS,SPR_ID,CDC_OPERATION_TYPE
FROM
(
select 
	LSMV_DRUG.ARI_REC_ID ,LSMV_DRUG.RECORD_ID ,LSMV_DRUG.PREFERED_PRODUCT_DESCRIPTION,
  case  when  (LSMV_DRUG.PREFERED_PRODUCT_DESCRIPTION  is null or LSMV_DRUG.CODING_CLASS is null) then  null
        else  (LSMV_DRUG.PREFERED_PRODUCT_DESCRIPTION||' ('||DC.DECODE||')')
  end DER_PPD_WITH_CODING_CLASS,
  CASE WHEN COALESCE(LSMV_DRUG.SPR_ID,'')='' then '-9999' else LSMV_DRUG.SPR_ID END AS SPR_ID,
  row_number() over (partition by LSMV_DRUG.RECORD_ID order by CDC_OPERATION_TIME desc) rank,
  CDC_OPERATION_TYPE 	
from 
	${stage_db_name}.${stage_schema_name}.LSMV_DRUG ,
  (select CODE,DECODE,CASE WHEN COALESCE(CODELIST_SUBSET_tmp.SPR_ID,'')='' then '-9999' else CODELIST_SUBSET_tmp.SPR_ID END AS SPR_ID,
  CODELIST_ID,EMDR_CODE from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp  where CODELIST_ID=9120) DC
where
	LSMV_DRUG.CODING_CLASS=DC.CODE(+)
and	CASE WHEN COALESCE(LSMV_DRUG.SPR_ID,'')='' then '-9999' else LSMV_DRUG.SPR_ID END=DC.SPR_ID(+)
and LSMV_DRUG.ari_rec_id in (select ari_Rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC)
) where RANK=1 AND CDC_OPERATION_TYPE IN ('I', 'U')  
) T
where LS_DB_DRUG.ari_rec_id=t.ARI_REC_ID 
and LS_DB_DRUG.record_id=t.record_id
AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');

  --ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG ADD COLUMN DER_RECORD_BLINDED NUMBER(38);
  
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG
SET DER_RECORD_BLINDED= CASE
    WHEN LS_DB_DRUG.INVESTIGATIONAL_PROD_BLINDED = '2'
    THEN '0'
    ELSE LS_DB_DRUG.INVESTIGATIONAL_PROD_BLINDED
 END where LS_DB_DRUG.ari_rec_id in (select ari_Rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_CASE_QFC) and LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');
 
 --ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG ADD COLUMN DER_PPD_STUDY_PRDCT_TYPE TEXT(250);
 
update ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG
set DER_PPD_STUDY_PRDCT_TYPE=case when T.DER_PPD_STUDY_PRDCT_TYPE is not null 
                                   then case when length(T.DER_PPD_STUDY_PRDCT_TYPE)>=4000 
                                  then substring(T.DER_PPD_STUDY_PRDCT_TYPE,0,3996)||' ...' else T.DER_PPD_STUDY_PRDCT_TYPE end
                                   else null end
from
( 
select
	APD.ari_rec_id,
	APD.SEQ_PRODUCT,
	LISTAGG(APD.INT_TRADENAME||' ('||nvl(DC.DECODE,' ')||') ', ' | ') within group (order by APD.RANK_ORDER) as DER_PPD_STUDY_PRDCT_TYPE
from
	(SELECT ARI_REC_ID,RECORD_ID SEQ_PRODUCT,PREFERED_PRODUCT_DESCRIPTION INT_TRADENAME,DRUGCHARACTERIZATION PRODUCT_FLAG,RANK_ORDER,STUDY_PRODUCT_TYPE,
	coalesce(SPR_ID,'-9999') as SPR_ID
		FROM	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_drug_SUBSET_TMP
		where PRODUCT_FLAG IN ('1','3')
		and INT_TRADENAME is not null
     and ARI_REC_ID is not null
	) APD,	(select * from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp
	where codelist_id='8008'
	) DC
where
	APD.STUDY_PRODUCT_TYPE=DC.CODE(+) 
    and APD.SPR_ID=DC.SPR_ID(+) 
group by
	APD.ari_rec_id,
	APD.SEQ_PRODUCT
) t
where LS_DB_DRUG.ari_rec_id=t.ARI_REC_ID 
and LS_DB_DRUG.record_id=t.SEQ_PRODUCT
AND LS_DB_DRUG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_DRUG_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_DRUG_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	

 
 
 
 