
-- USE SCHEMA ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_JDD_JPN()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;

CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP (
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

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_JDD_JPN'),
                'LSDB','Case','LS_DB_CC_JDD_JPN',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_CC_JDD_JPN' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CC_JDD_JPN' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_JDD_JPN_DELETION_TMP;
CREATE TEMPORARY TABLE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_JDD_JPN_DELETION_TMP  As select RECORD_ID,'lsmv_cc_jdd_jpn' AS TABLE_NAME FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_jdd_jpn WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP;
CREATE TEMPORARY TABLE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP  AS  WITH 
LSMV_CASE_NO_SUBSET as
(

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_jdd_jpn WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CC_JDD_JPN' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_jdd_jpn WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_CC_JDD_JPN' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_cc_jdd_jpn_SUBSET AS 
(
select * from 
    (SELECT  
    basic_name_code  basic_name_code,cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,date_created  date_created,date_modified  date_modified,dictionary_version  dictionary_version,dosage_route  dosage_route,dosage_route_code  dosage_route_code,drug_code_class1  drug_code_class1,drug_code_class2  drug_code_class2,form_code  form_code,gen_use_class  gen_use_class,generic_name  generic_name,generic_name_2  generic_name_2,jpn_drug_code  jpn_drug_code,maintenance_flag  maintenance_flag,maintenance_seq  maintenance_seq,maintenance_yymm  maintenance_yymm,manufacturer_code  manufacturer_code,manufacturer_name  manufacturer_name,prdouct_name_2  prdouct_name_2,product_name  product_name,record_id  record_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.lsmv_cc_jdd_jpn
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_JDD_JPN_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_jdd_jpn')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_cc_jdd_jpn_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_cc_jdd_jpn_SUBSET.USER_CREATED CREATED_BY,lsmv_cc_jdd_jpn_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_cc_jdd_jpn_SUBSET.user_modified  ,lsmv_cc_jdd_jpn_SUBSET.user_created  ,lsmv_cc_jdd_jpn_SUBSET.spr_id  ,lsmv_cc_jdd_jpn_SUBSET.record_id  ,lsmv_cc_jdd_jpn_SUBSET.product_name  ,lsmv_cc_jdd_jpn_SUBSET.prdouct_name_2  ,lsmv_cc_jdd_jpn_SUBSET.manufacturer_name  ,lsmv_cc_jdd_jpn_SUBSET.manufacturer_code  ,lsmv_cc_jdd_jpn_SUBSET.maintenance_yymm  ,lsmv_cc_jdd_jpn_SUBSET.maintenance_seq  ,lsmv_cc_jdd_jpn_SUBSET.maintenance_flag  ,lsmv_cc_jdd_jpn_SUBSET.jpn_drug_code  ,lsmv_cc_jdd_jpn_SUBSET.generic_name_2  ,lsmv_cc_jdd_jpn_SUBSET.generic_name  ,lsmv_cc_jdd_jpn_SUBSET.gen_use_class  ,lsmv_cc_jdd_jpn_SUBSET.form_code  ,lsmv_cc_jdd_jpn_SUBSET.drug_code_class2  ,lsmv_cc_jdd_jpn_SUBSET.drug_code_class1  ,lsmv_cc_jdd_jpn_SUBSET.dosage_route_code  ,lsmv_cc_jdd_jpn_SUBSET.dosage_route  ,lsmv_cc_jdd_jpn_SUBSET.dictionary_version  ,lsmv_cc_jdd_jpn_SUBSET.date_modified  ,lsmv_cc_jdd_jpn_SUBSET.date_created  ,lsmv_cc_jdd_jpn_SUBSET.cdc_operation_type  ,lsmv_cc_jdd_jpn_SUBSET.cdc_operation_time  ,lsmv_cc_jdd_jpn_SUBSET.basic_name_code ,CONCAT( NVL(lsmv_cc_jdd_jpn_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_cc_jdd_jpn_SUBSET  WHERE 1=1  
;



UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP)
where target_table_name='LS_DB_CC_JDD_JPN'

; 






UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN   
SET LS_DB_CC_JDD_JPN.user_modified = LS_DB_CC_JDD_JPN_TMP.user_modified,LS_DB_CC_JDD_JPN.user_created = LS_DB_CC_JDD_JPN_TMP.user_created,LS_DB_CC_JDD_JPN.spr_id = LS_DB_CC_JDD_JPN_TMP.spr_id,LS_DB_CC_JDD_JPN.record_id = LS_DB_CC_JDD_JPN_TMP.record_id,LS_DB_CC_JDD_JPN.product_name = LS_DB_CC_JDD_JPN_TMP.product_name,LS_DB_CC_JDD_JPN.prdouct_name_2 = LS_DB_CC_JDD_JPN_TMP.prdouct_name_2,LS_DB_CC_JDD_JPN.manufacturer_name = LS_DB_CC_JDD_JPN_TMP.manufacturer_name,LS_DB_CC_JDD_JPN.manufacturer_code = LS_DB_CC_JDD_JPN_TMP.manufacturer_code,LS_DB_CC_JDD_JPN.maintenance_yymm = LS_DB_CC_JDD_JPN_TMP.maintenance_yymm,LS_DB_CC_JDD_JPN.maintenance_seq = LS_DB_CC_JDD_JPN_TMP.maintenance_seq,LS_DB_CC_JDD_JPN.maintenance_flag = LS_DB_CC_JDD_JPN_TMP.maintenance_flag,LS_DB_CC_JDD_JPN.jpn_drug_code = LS_DB_CC_JDD_JPN_TMP.jpn_drug_code,LS_DB_CC_JDD_JPN.generic_name_2 = LS_DB_CC_JDD_JPN_TMP.generic_name_2,LS_DB_CC_JDD_JPN.generic_name = LS_DB_CC_JDD_JPN_TMP.generic_name,LS_DB_CC_JDD_JPN.gen_use_class = LS_DB_CC_JDD_JPN_TMP.gen_use_class,LS_DB_CC_JDD_JPN.form_code = LS_DB_CC_JDD_JPN_TMP.form_code,LS_DB_CC_JDD_JPN.drug_code_class2 = LS_DB_CC_JDD_JPN_TMP.drug_code_class2,LS_DB_CC_JDD_JPN.drug_code_class1 = LS_DB_CC_JDD_JPN_TMP.drug_code_class1,LS_DB_CC_JDD_JPN.dosage_route_code = LS_DB_CC_JDD_JPN_TMP.dosage_route_code,LS_DB_CC_JDD_JPN.dosage_route = LS_DB_CC_JDD_JPN_TMP.dosage_route,LS_DB_CC_JDD_JPN.dictionary_version = LS_DB_CC_JDD_JPN_TMP.dictionary_version,LS_DB_CC_JDD_JPN.date_modified = LS_DB_CC_JDD_JPN_TMP.date_modified,LS_DB_CC_JDD_JPN.date_created = LS_DB_CC_JDD_JPN_TMP.date_created,LS_DB_CC_JDD_JPN.cdc_operation_type = LS_DB_CC_JDD_JPN_TMP.cdc_operation_type,LS_DB_CC_JDD_JPN.cdc_operation_time = LS_DB_CC_JDD_JPN_TMP.cdc_operation_time,LS_DB_CC_JDD_JPN.basic_name_code = LS_DB_CC_JDD_JPN_TMP.basic_name_code,
LS_DB_CC_JDD_JPN.PROCESSING_DT = LS_DB_CC_JDD_JPN_TMP.PROCESSING_DT ,
LS_DB_CC_JDD_JPN.expiry_date    =LS_DB_CC_JDD_JPN_TMP.expiry_date       ,
LS_DB_CC_JDD_JPN.created_by     =LS_DB_CC_JDD_JPN_TMP.created_by        ,
LS_DB_CC_JDD_JPN.created_dt     =LS_DB_CC_JDD_JPN_TMP.created_dt        ,
LS_DB_CC_JDD_JPN.load_ts        =LS_DB_CC_JDD_JPN_TMP.load_ts         
FROM    ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP 
WHERE LS_DB_CC_JDD_JPN.INTEGRATION_ID = LS_DB_CC_JDD_JPN_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CC_JDD_JPN_TMP.PROCESSING_DT = LS_DB_CC_JDD_JPN.PROCESSING_DT,1=1);


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
product_name,
prdouct_name_2,
manufacturer_name,
manufacturer_code,
maintenance_yymm,
maintenance_seq,
maintenance_flag,
jpn_drug_code,
generic_name_2,
generic_name,
gen_use_class,
form_code,
drug_code_class2,
drug_code_class1,
dosage_route_code,
dosage_route,
dictionary_version,
date_modified,
date_created,
cdc_operation_type,
cdc_operation_time,
basic_name_code)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
spr_id,
record_id,
product_name,
prdouct_name_2,
manufacturer_name,
manufacturer_code,
maintenance_yymm,
maintenance_seq,
maintenance_flag,
jpn_drug_code,
generic_name_2,
generic_name,
gen_use_class,
form_code,
drug_code_class2,
drug_code_class1,
dosage_route_code,
dosage_route,
dictionary_version,
date_modified,
date_created,
cdc_operation_type,
cdc_operation_time,
basic_name_code
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN TGT)
                                                                                ; 
COMMIT;



UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP 
WHERE TO_DATE(LS_DB_CC_JDD_JPN.PROCESSING_DT) < TO_DATE(LS_DB_CC_JDD_JPN_TMP.PROCESSING_DT)
AND LS_DB_CC_JDD_JPN.INTEGRATION_ID = LS_DB_CC_JDD_JPN_TMP.INTEGRATION_ID
AND LS_DB_CC_JDD_JPN.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_JDD_JPN_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_jdd_jpn')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LSMV_CC_JDD_JPN_DELETION_TMP  WHERE TABLE_NAME='lsmv_cc_jdd_jpn')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN where LOAD_TS= (select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_JDD_JPN_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CC_JDD_JPN'
;

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_CC_JDD_JPN Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CC_JDD_JPN'),
                'LSDB','Case','LS_DB_CC_JDD_JPN',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_CC_JDD_JPN not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
