
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ST_LABELLED_PROD_EVENTS()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;

CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP (
ROW_WID           NUMBER(38,0),
FUNCTIONAL_AREA        VARCHAR(25),
ENTITY_NAME   VARCHAR(25),
TARGET_TABLE_NAME   VARCHAR(100),
LOAD_TS              TIMESTAMP_NTZ(9),
LOAD_START_TS               TIMESTAMP_NTZ(9),
LOAD_END_TS   TIMESTAMP_NTZ(9),
REC_READ_CNT NUMBER(38,0),
REC_PROCESSED_CNT    NUMBER(38,0),
ERROR_REC_CNT              NUMBER(38,0),
ERROR_DETAILS VARCHAR(8000),
LOAD_STATUS   VARCHAR(15),
CHANGED_REC_SET        VARIANT);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ST_LABELLED_PROD_EVENTS'),
                'LSDB','Case','LS_DB_ST_LABELLED_PROD_EVENTS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_ST_LABELLED_PROD_EVENTS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_ST_LABELLED_PROD_EVENTS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_LABELLED_PROD_EVENTS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_LABELLED_PROD_EVENTS_DELETION_TMP  As select RECORD_ID,'lsmv_st_labelled_prod_events' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_labelled_prod_events WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('7077')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
                                                                                ), D_MEDDRA_ICD_SUBSET AS 
( select distinct BK_MEDDRA_ICD_WID,LLT_CODE,PT_CODE,PRIMARY_SOC_FG from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
(

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_labelled_prod_events WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ST_LABELLED_PROD_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_labelled_prod_events WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ST_LABELLED_PROD_EVENTS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_st_labelled_prod_events_SUBSET AS 
(
select * from 
    (SELECT  
    broad_narrow  broad_narrow,comments  comments,conditional_labeling  conditional_labeling,conditional_labeling_comments  conditional_labeling_comments,date_created  date_created,date_modified  date_modified,eff_labeling_date  eff_labeling_date,event  event,fk_aplg_rec_id  fk_aplg_rec_id,is_coded  is_coded,is_fatal  is_fatal,is_labeled  is_labeled,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(is_labeled AS VARCHAR(100)) )is_labeled_de_ml , is_lifethreatening  is_lifethreatening,is_oth_seriouness  is_oth_seriouness,labelling_version  labelling_version,llt_code  llt_code,medra_version  medra_version,pt_code  pt_code,record_id  record_id,smqcmq_code  smqcmq_code,smqcmq_term  smqcmq_term,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_labelled_prod_events
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_LABELLED_PROD_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_labelled_prod_events')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_st_labelled_prod_events_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_st_labelled_prod_events_SUBSET.USER_CREATED CREATED_BY,lsmv_st_labelled_prod_events_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_labelled_prod_events_SUBSET.user_modified  ,lsmv_st_labelled_prod_events_SUBSET.user_created  ,lsmv_st_labelled_prod_events_SUBSET.spr_id  ,lsmv_st_labelled_prod_events_SUBSET.smqcmq_term  ,lsmv_st_labelled_prod_events_SUBSET.smqcmq_code  ,lsmv_st_labelled_prod_events_SUBSET.record_id  ,lsmv_st_labelled_prod_events_SUBSET.pt_code  ,lsmv_st_labelled_prod_events_SUBSET.medra_version  ,lsmv_st_labelled_prod_events_SUBSET.llt_code  ,lsmv_st_labelled_prod_events_SUBSET.labelling_version  ,lsmv_st_labelled_prod_events_SUBSET.is_oth_seriouness  ,lsmv_st_labelled_prod_events_SUBSET.is_lifethreatening  ,lsmv_st_labelled_prod_events_SUBSET.is_labeled_de_ml  ,lsmv_st_labelled_prod_events_SUBSET.is_labeled  ,lsmv_st_labelled_prod_events_SUBSET.is_fatal  ,lsmv_st_labelled_prod_events_SUBSET.is_coded  ,lsmv_st_labelled_prod_events_SUBSET.fk_aplg_rec_id  ,lsmv_st_labelled_prod_events_SUBSET.event  ,lsmv_st_labelled_prod_events_SUBSET.eff_labeling_date  ,lsmv_st_labelled_prod_events_SUBSET.date_modified  ,lsmv_st_labelled_prod_events_SUBSET.date_created  ,lsmv_st_labelled_prod_events_SUBSET.conditional_labeling_comments  ,lsmv_st_labelled_prod_events_SUBSET.conditional_labeling  ,lsmv_st_labelled_prod_events_SUBSET.comments  ,lsmv_st_labelled_prod_events_SUBSET.broad_narrow ,CONCAT( NVL(lsmv_st_labelled_prod_events_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_labelled_prod_events_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP)
where target_table_name='LS_DB_ST_LABELLED_PROD_EVENTS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS   
SET LS_DB_ST_LABELLED_PROD_EVENTS.user_modified = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.user_modified,LS_DB_ST_LABELLED_PROD_EVENTS.user_created = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.user_created,LS_DB_ST_LABELLED_PROD_EVENTS.spr_id = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.spr_id,LS_DB_ST_LABELLED_PROD_EVENTS.smqcmq_term = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.smqcmq_term,LS_DB_ST_LABELLED_PROD_EVENTS.smqcmq_code = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.smqcmq_code,LS_DB_ST_LABELLED_PROD_EVENTS.record_id = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.record_id,LS_DB_ST_LABELLED_PROD_EVENTS.pt_code = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.pt_code,LS_DB_ST_LABELLED_PROD_EVENTS.medra_version = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.medra_version,LS_DB_ST_LABELLED_PROD_EVENTS.llt_code = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.llt_code,LS_DB_ST_LABELLED_PROD_EVENTS.labelling_version = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.labelling_version,LS_DB_ST_LABELLED_PROD_EVENTS.is_oth_seriouness = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_oth_seriouness,LS_DB_ST_LABELLED_PROD_EVENTS.is_lifethreatening = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_lifethreatening,LS_DB_ST_LABELLED_PROD_EVENTS.is_labeled_de_ml = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_labeled_de_ml,LS_DB_ST_LABELLED_PROD_EVENTS.is_labeled = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_labeled,LS_DB_ST_LABELLED_PROD_EVENTS.is_fatal = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_fatal,LS_DB_ST_LABELLED_PROD_EVENTS.is_coded = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.is_coded,LS_DB_ST_LABELLED_PROD_EVENTS.fk_aplg_rec_id = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.fk_aplg_rec_id,LS_DB_ST_LABELLED_PROD_EVENTS.event = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.event,LS_DB_ST_LABELLED_PROD_EVENTS.eff_labeling_date = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.eff_labeling_date,LS_DB_ST_LABELLED_PROD_EVENTS.date_modified = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.date_modified,LS_DB_ST_LABELLED_PROD_EVENTS.date_created = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.date_created,LS_DB_ST_LABELLED_PROD_EVENTS.conditional_labeling_comments = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.conditional_labeling_comments,LS_DB_ST_LABELLED_PROD_EVENTS.conditional_labeling = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.conditional_labeling,LS_DB_ST_LABELLED_PROD_EVENTS.comments = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.comments,LS_DB_ST_LABELLED_PROD_EVENTS.broad_narrow = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.broad_narrow,
LS_DB_ST_LABELLED_PROD_EVENTS.PROCESSING_DT = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.PROCESSING_DT ,
LS_DB_ST_LABELLED_PROD_EVENTS.expiry_date    =LS_DB_ST_LABELLED_PROD_EVENTS_TMP.expiry_date       ,
LS_DB_ST_LABELLED_PROD_EVENTS.created_by     =LS_DB_ST_LABELLED_PROD_EVENTS_TMP.created_by        ,
LS_DB_ST_LABELLED_PROD_EVENTS.created_dt     =LS_DB_ST_LABELLED_PROD_EVENTS_TMP.created_dt        ,
LS_DB_ST_LABELLED_PROD_EVENTS.load_ts        =LS_DB_ST_LABELLED_PROD_EVENTS_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP 
WHERE LS_DB_ST_LABELLED_PROD_EVENTS.INTEGRATION_ID = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_ST_LABELLED_PROD_EVENTS_TMP.PROCESSING_DT = LS_DB_ST_LABELLED_PROD_EVENTS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
smqcmq_term,
smqcmq_code,
record_id,
pt_code,
medra_version,
llt_code,
labelling_version,
is_oth_seriouness,
is_lifethreatening,
is_labeled_de_ml,
is_labeled,
is_fatal,
is_coded,
fk_aplg_rec_id,
event,
eff_labeling_date,
date_modified,
date_created,
conditional_labeling_comments,
conditional_labeling,
comments,
broad_narrow)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
smqcmq_term,
smqcmq_code,
record_id,
pt_code,
medra_version,
llt_code,
labelling_version,
is_oth_seriouness,
is_lifethreatening,
is_labeled_de_ml,
is_labeled,
is_fatal,
is_coded,
fk_aplg_rec_id,
event,
eff_labeling_date,
date_modified,
date_created,
conditional_labeling_comments,
conditional_labeling,
comments,
broad_narrow
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP 
WHERE TO_DATE(LS_DB_ST_LABELLED_PROD_EVENTS.PROCESSING_DT) < TO_DATE(LS_DB_ST_LABELLED_PROD_EVENTS_TMP.PROCESSING_DT)
AND LS_DB_ST_LABELLED_PROD_EVENTS.INTEGRATION_ID = LS_DB_ST_LABELLED_PROD_EVENTS_TMP.INTEGRATION_ID
AND LS_DB_ST_LABELLED_PROD_EVENTS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_LABELLED_PROD_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_labelled_prod_events')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ST_LABELLED_PROD_EVENTS_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_labelled_prod_events')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ST_LABELLED_PROD_EVENTS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_ST_LABELLED_PROD_EVENTS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_ST_LABELLED_PROD_EVENTS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ST_LABELLED_PROD_EVENTS'),
                'LSDB','Case','LS_DB_ST_LABELLED_PROD_EVENTS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_ST_LABELLED_PROD_EVENTS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
