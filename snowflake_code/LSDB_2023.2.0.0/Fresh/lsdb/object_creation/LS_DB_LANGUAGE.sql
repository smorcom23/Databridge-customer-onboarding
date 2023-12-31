--call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LANGUAGE();
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LANGUAGE()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE 
(codelist_id,language_code,code,language_name,language_name_abbr,spr_id,PROCESSING_DT,EXPIRY_DATE,load_ts)

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
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9065')
    ) LSMV_CODELIST_NAME JOIN
    (
                   SELECT RECORD_ID, CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,date_modified  ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
    ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
    AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
    JOIN 
    (
                   SELECT RECORD_ID,LANGUAGE_CODE, DECODE,FK_CL_CODE_REC_ID,DECODE_ABBR ,CDC_OPERATION_TIME,date_modified,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE --where LANGUAGE_CODE='en'
    ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
    AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 ;


  RETURN 'LS_DB_LANGUAGE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_LANGUAGE'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_LANGUAGE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;
