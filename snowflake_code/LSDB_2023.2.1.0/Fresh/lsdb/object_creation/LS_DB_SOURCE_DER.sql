--	call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE_DER();
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_SOURCE_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_SOURCE_DER';
/*
ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE ADD COLUMN DER_ALL_SOURCE TEXT;
*/






insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_SOURCE_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_SOURCE_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE_DER'),
	'LSRA','Case','LS_DB_SOURCE_DER',null,CURRENT_TIMESTAMP(),null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_SOURCE_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SOURCE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE)
WHERE TARGET_TABLE_NAME = 'LS_DB_SOURCE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_CASE_QFC;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_CASE_QFC AS
select distinct ari_rec_id
FROM 
	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SOURCE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_SOURCE_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_SOURCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


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







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE   
SET LS_DB_SOURCE.DER_ALL_SOURCE=LS_DB_SOURCE_TMP.DER_ALL_SOURCE
FROM (
WITH TEMP AS
(
  SELECT DISTINCT D1.CODE,
         CASE
           WHEN (SELECT UPPER(DEFAULT_VALUE_CHAR)
                 FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER
                 WHERE PARAMETER_NAME = 'E2B_PRIMARY_SOURCE') = 'YES' THEN D2.DECODE
           ELSE D1.DECODE
         END AS DECODE,
		 COALESCE(D1.SPR_ID,'-9999') SPR_ID
  FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp D1,
  (select SPR_ID,language_code from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_LANGUAGE_PARAMETERS) L,
       (SELECT *
        FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp
        WHERE CODELIST_ID = '1019'
       -- AND   CODELIST_TYPE_FLAG = '2'
        and 1=2
) D2
  WHERE D1.CODELIST_ID = '346'
 -- AND   D1.CODELIST_TYPE_FLAG = '1'
  AND   D1.LANGUAGE_CODE = D2.LANGUAGE_CODE(+)
  AND   D1.LANGUAGE_CODE = L.LANGUAGE_CODE(+)
  AND   D1.SPR_ID = L.SPR_ID(+)
  AND   D1.E2B_CODE = D2.CODE(+)
  AND   D1.SPR_ID = D2.SPR_ID(+)
)

                                   
      SELECT ARI_REC_ID
      ,LISTAGG(decode,'\r\n')within group (order by record_id) AS DER_ALL_SOURCE
                                   from 
  (                                 
select   ARI_REC_ID,decode ,record_id                                   
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE left outer join temp on 
CODE=SOURCE
AND TEMP.SPR_ID=COALESCE(LS_DB_SOURCE.SPR_ID,'-9999')
WHERE  ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_CASE_QFC)
	and   EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM') AND SOURCE IS NOT NULL 
ORDER BY ARI_REC_ID,
         record_id)
 group by 1                                  
                                   
) LS_DB_SOURCE_TMP
    WHERE LS_DB_SOURCE.ari_rec_id = LS_DB_SOURCE_TMP.ari_rec_id	
	AND LS_DB_SOURCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'); 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE   
SET LS_DB_SOURCE.der_primary_source_flag_de_ml=LS_DB_SOURCE_TMP.der_primary_source_flag_de_ml,
LS_DB_SOURCE.der_primary_source_flag_code=LS_DB_SOURCE_TMP.der_primary_source_flag_code
FROM (
WITH cd_with AS (
    SELECT
        cd_id,
        cd,
        de,
        ln
    FROM
        (
            SELECT DISTINCT
                lsmv_codelist_name.codelist_id     cd_id,
                lsmv_codelist_code.code            cd,
                lsmv_codelist_decode.decode        de,
                lsmv_codelist_decode.language_code ln,
                ROW_NUMBER()
                OVER(PARTITION BY lsmv_codelist_name.codelist_id, lsmv_codelist_code.code, lsmv_codelist_decode.language_code
                     ORDER BY
                         lsmv_codelist_name.cdc_operation_time DESC, lsmv_codelist_code.cdc_operation_time DESC, lsmv_codelist_decode.
                         cdc_operation_time DESC
                )                                  rank
            FROM
                     (
                    SELECT
                        record_id,
                        codelist_id,
                        cdc_operation_time,
                        ROW_NUMBER()
                        OVER(PARTITION BY record_id
                             ORDER BY
                                 cdc_operation_time DESC
                        ) rank
                    FROM
                        ${stage_db_name}.${stage_schema_name}.lsmv_codelist_name
                    WHERE
                        codelist_id IN ( '7077' )
                ) lsmv_codelist_name
                JOIN (
                    SELECT
                        record_id,
                        code,
                        fk_cl_name_rec_id,
                        cdc_operation_time,
                        ROW_NUMBER()
                        OVER(PARTITION BY record_id
                             ORDER BY
                                 cdc_operation_time DESC
                        ) rank
                    FROM
                        ${stage_db_name}.${stage_schema_name}.lsmv_codelist_code
                ) lsmv_codelist_code ON lsmv_codelist_name.record_id = lsmv_codelist_code.fk_cl_name_rec_id
                                        AND lsmv_codelist_name.rank = 1
                                        AND lsmv_codelist_code.rank = 1
                JOIN (
                    SELECT
                        record_id,
                        language_code,
                        decode,
                        fk_cl_code_rec_id,
                        cdc_operation_time,
                        ROW_NUMBER()
                        OVER(PARTITION BY record_id
                             ORDER BY
                                 cdc_operation_time DESC
                        ) rank
                    FROM
                        ${stage_db_name}.${stage_schema_name}.lsmv_codelist_decode
                ) lsmv_codelist_decode ON lsmv_codelist_code.record_id = lsmv_codelist_decode.fk_cl_code_rec_id
                                          AND lsmv_codelist_decode.rank = 1
        )
    WHERE
        rank = 1
)                                                                            
        SELECT record_id,
            (SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST((CASE 
        WHEN primary_source_flag = '1' THEN '1'
        ELSE '0'
    END) AS VARCHAR(100)) ) der_primary_source_flag_de_ml,
	CASE 
        WHEN primary_source_flag = '1' THEN '1'
        ELSE '0'
    END der_primary_source_flag_code
	
 from ${stage_db_name}.${stage_schema_name}.LSMV_SOURCE where 
  ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE_CASE_QFC)
) LS_DB_SOURCE_TMP
    WHERE LS_DB_SOURCE.record_id = LS_DB_SOURCE_TMP.record_id	
	AND LS_DB_SOURCE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM'); 


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOURCE),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SOURCE_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SOURCE_DER'
and LOAD_STATUS = 'In Progress') ;	
 


 RETURN 'LS_DB_SOURCE_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SOURCE_DER'
and LOAD_STATUS = 'In Progress'
;


  RETURN 'LS_DB_SOURCE_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;