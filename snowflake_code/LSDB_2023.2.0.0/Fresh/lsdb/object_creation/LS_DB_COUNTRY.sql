/*
drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COUNTRY;

CREATE OR REPLACE TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COUNTRY (
    COUNTRY_NAME VARCHAR,
    COUNTRY_ISO_ALPHA2 VARCHAR,
    COUNTRY_ISO_ALPHA3 VARCHAR,
    LANGUAGE_CODE VARCHAR,
    CODE VARCHAR,
    DECODE VARCHAR,
    DECODE_ABBR VARCHAR,
    CODE_TYPE VARCHAR,
    PROCESSING_DT DATE,
    REGION_NAME VARCHAR,
    EXPIRY_DATE DATE,
    load_ts TIMESTAMP
);
*/

CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_COUNTRY()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COUNTRY 
(
    COUNTRY_NAME,
    COUNTRY_ISO_ALPHA2,
    COUNTRY_ISO_ALPHA3,
    LANGUAGE_CODE,
    CODE,
    DECODE,
    DECODE_ABBR,
    CODE_TYPE,
    PROCESSING_DT,
    REGION_NAME,
    EXPIRY_DATE,
    load_ts
)

WITH ProcessedData AS (
   select CD_ID,
CASE 
    WHEN LN='en' THEN '001'
    WHEN LN='de' THEN '002'
    WHEN LN='es' THEN '003'
    WHEN LN='fr' THEN '005'
    WHEN LN='it' THEN '007'
    WHEN LN='ja' THEN '008'
    WHEN LN='cn' THEN '009'
    WHEN LN='po' THEN '010'
  END LANGUAGE_CODE,
  CASE 
    WHEN CD='127' THEN '001'
    WHEN CD='110' THEN '002'
    WHEN CD='404' THEN '003'
    WHEN CD='142' THEN '005'
    WHEN CD='205' THEN '007'
    WHEN CD='208' THEN '008'
    WHEN CD='81' THEN '009'
    WHEN CD='351' THEN '010'
                ELSE CD END CODE,
              DE AS DECODE ,
    DECODE_ABBR,
	  CASE WHEN CD_ID = 1015 THEN '1'
                             WHEN CD_ID = 9744 THEN '2'
                END AS CODE_TYPE,
    SPR_ID,
    PROCESSING_DT,
    TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE, 
    current_timestamp
    from 
(
SELECT distinct 
  to_date(GREATEST(
NVL(LSMV_CODELIST_NAME.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_CODELIST_CODE.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_CODELIST_DECODE.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))    
))
PROCESSING_DT , LSMV_CODELIST_NAME.SPR_ID, 
  LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,LSMV_CODELIST_DECODE.DECODE_ABBR
, row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
FROM
    (
                   SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,date_modified,SPR_ID,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (1015,9744)
    ) LSMV_CODELIST_NAME JOIN
    (
                   SELECT RECORD_ID, CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,date_modified  ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
    ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
    AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
    JOIN 
    (
                   SELECT RECORD_ID,LANGUAGE_CODE, DECODE,FK_CL_CODE_REC_ID,DECODE_ABBR ,CDC_OPERATION_TIME,date_modified,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE in ('en','ja')
    ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
    AND LSMV_CODELIST_DECODE.RANK=1) where rank=1)
SELECT
    PD.DECODE COUNTRY_NAME,
    CMD.COUNTRY_ISO_ALPHA2,
    CMD.COUNTRY_ISO_ALPHA3,
    PD.LANGUAGE_CODE,
    PD.CODE,
    PD.DECODE,
    PD.DECODE_ABBR,
    PD.CODE_TYPE,
    PD.PROCESSING_DT,
    CMD.REGION_NAME,
    PD.EXPIRY_DATE,
    PD.CURRENT_TIMESTAMP
FROM
    ProcessedData PD
left JOIN
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COUNTRY_REGION CMD    
    ON CMD.COUNTRY_NAME = PD.DECODE
where PD.CODE not in ('MSK','ASKU','UNKO','NASK')
;

commit;


RETURN 'LS_DB_COUNTRY Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_COUNTRY'
and LOAD_STATUS = 'In Progress'
;


  RETURN 'LS_DB_COUNTRY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;