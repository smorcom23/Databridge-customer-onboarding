
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PROD_APPROVAL_LIB()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_APPROVAL_LIB'),
                'LSDB','Case','LS_DB_PROD_APPROVAL_LIB',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PROD_APPROVAL_LIB' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PROD_APPROVAL_LIB' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PROD_APPROVAL_LIB_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PROD_APPROVAL_LIB_DELETION_TMP  As select RECORD_ID,'lsmv_product_approval' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_approval WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('8013')
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

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PROD_APPROVAL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_product_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PROD_APPROVAL_LIB' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_product_approval_SUBSET AS 
(
select * from 
    (SELECT  
    account_record_id_agent  account_record_id_agent,account_record_id_mah  account_record_id_mah,agent_name  agent_name,anda_no  anda_no,application_no  application_no,approval_date  approval_date,approval_no  approval_no,approval_start_date  approval_start_date,approval_status  approval_status,approval_submission_type  approval_submission_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8013' AND CD=CAST(approval_submission_type AS VARCHAR(100)) )approval_submission_type_de_ml , approval_type  approval_type,arisg_approval_id  arisg_approval_id,arisg_product_id  arisg_product_id,arisg_record_id  arisg_record_id,atc_code  atc_code,atc_vet_code  atc_vet_code,bio_ref_name  bio_ref_name,chemical_type  chemical_type,code_name  code_name,combination_product  combination_product,company_unit_record_id_mah  company_unit_record_id_mah,container_type  container_type,copy_approval_checked  copy_approval_checked,country_code  country_code,date_created  date_created,date_modified  date_modified,internal_prd_id  internal_prd_id,lead_center  lead_center,lead_review_codelist_id  lead_review_codelist_id,license_id  license_id,license_status  license_status,mah_name  mah_name,marketing_category  marketing_category,marketing_status  marketing_status,marketting_date  marketting_date,medicinal_product_name  medicinal_product_name,ndc_no  ndc_no,otc_altnte_id  otc_altnte_id,product_division  product_division,product_type  product_type,proprietary_name_suffix  proprietary_name_suffix,record_id  record_id,source  source,spr_id  spr_id,trade_id  trade_id,user_created  user_created,user_modified  user_modified,verified  verified,viewpdfflag  viewpdfflag,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_product_approval
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PROD_APPROVAL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_approval')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_product_approval_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_product_approval_SUBSET.USER_CREATED CREATED_BY,lsmv_product_approval_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_product_approval_SUBSET.viewpdfflag  ,lsmv_product_approval_SUBSET.verified  ,lsmv_product_approval_SUBSET.user_modified  ,lsmv_product_approval_SUBSET.user_created  ,lsmv_product_approval_SUBSET.trade_id  ,lsmv_product_approval_SUBSET.spr_id  ,lsmv_product_approval_SUBSET.source  ,lsmv_product_approval_SUBSET.record_id  ,lsmv_product_approval_SUBSET.proprietary_name_suffix  ,lsmv_product_approval_SUBSET.product_type  ,lsmv_product_approval_SUBSET.product_division  ,lsmv_product_approval_SUBSET.otc_altnte_id  ,lsmv_product_approval_SUBSET.ndc_no  ,lsmv_product_approval_SUBSET.medicinal_product_name  ,lsmv_product_approval_SUBSET.marketting_date  ,lsmv_product_approval_SUBSET.marketing_status  ,lsmv_product_approval_SUBSET.marketing_category  ,lsmv_product_approval_SUBSET.mah_name  ,lsmv_product_approval_SUBSET.license_status  ,lsmv_product_approval_SUBSET.license_id  ,lsmv_product_approval_SUBSET.lead_review_codelist_id  ,lsmv_product_approval_SUBSET.lead_center  ,lsmv_product_approval_SUBSET.internal_prd_id  ,lsmv_product_approval_SUBSET.date_modified  ,lsmv_product_approval_SUBSET.date_created  ,lsmv_product_approval_SUBSET.country_code  ,lsmv_product_approval_SUBSET.copy_approval_checked  ,lsmv_product_approval_SUBSET.container_type  ,lsmv_product_approval_SUBSET.company_unit_record_id_mah  ,lsmv_product_approval_SUBSET.combination_product  ,lsmv_product_approval_SUBSET.code_name  ,lsmv_product_approval_SUBSET.chemical_type  ,lsmv_product_approval_SUBSET.bio_ref_name  ,lsmv_product_approval_SUBSET.atc_vet_code  ,lsmv_product_approval_SUBSET.atc_code  ,lsmv_product_approval_SUBSET.arisg_record_id  ,lsmv_product_approval_SUBSET.arisg_product_id  ,lsmv_product_approval_SUBSET.arisg_approval_id  ,lsmv_product_approval_SUBSET.approval_type  ,lsmv_product_approval_SUBSET.approval_submission_type_de_ml  ,lsmv_product_approval_SUBSET.approval_submission_type  ,lsmv_product_approval_SUBSET.approval_status  ,lsmv_product_approval_SUBSET.approval_start_date  ,lsmv_product_approval_SUBSET.approval_no  ,lsmv_product_approval_SUBSET.approval_date  ,lsmv_product_approval_SUBSET.application_no  ,lsmv_product_approval_SUBSET.anda_no  ,lsmv_product_approval_SUBSET.agent_name  ,lsmv_product_approval_SUBSET.account_record_id_mah  ,lsmv_product_approval_SUBSET.account_record_id_agent ,CONCAT( NVL(lsmv_product_approval_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_product_approval_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP)
where target_table_name='LS_DB_PROD_APPROVAL_LIB'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB   
SET LS_DB_PROD_APPROVAL_LIB.viewpdfflag = LS_DB_PROD_APPROVAL_LIB_TMP.viewpdfflag,LS_DB_PROD_APPROVAL_LIB.verified = LS_DB_PROD_APPROVAL_LIB_TMP.verified,LS_DB_PROD_APPROVAL_LIB.user_modified = LS_DB_PROD_APPROVAL_LIB_TMP.user_modified,LS_DB_PROD_APPROVAL_LIB.user_created = LS_DB_PROD_APPROVAL_LIB_TMP.user_created,LS_DB_PROD_APPROVAL_LIB.trade_id = LS_DB_PROD_APPROVAL_LIB_TMP.trade_id,LS_DB_PROD_APPROVAL_LIB.spr_id = LS_DB_PROD_APPROVAL_LIB_TMP.spr_id,LS_DB_PROD_APPROVAL_LIB.source = LS_DB_PROD_APPROVAL_LIB_TMP.source,LS_DB_PROD_APPROVAL_LIB.record_id = LS_DB_PROD_APPROVAL_LIB_TMP.record_id,LS_DB_PROD_APPROVAL_LIB.proprietary_name_suffix = LS_DB_PROD_APPROVAL_LIB_TMP.proprietary_name_suffix,LS_DB_PROD_APPROVAL_LIB.product_type = LS_DB_PROD_APPROVAL_LIB_TMP.product_type,LS_DB_PROD_APPROVAL_LIB.product_division = LS_DB_PROD_APPROVAL_LIB_TMP.product_division,LS_DB_PROD_APPROVAL_LIB.otc_altnte_id = LS_DB_PROD_APPROVAL_LIB_TMP.otc_altnte_id,LS_DB_PROD_APPROVAL_LIB.ndc_no = LS_DB_PROD_APPROVAL_LIB_TMP.ndc_no,LS_DB_PROD_APPROVAL_LIB.medicinal_product_name = LS_DB_PROD_APPROVAL_LIB_TMP.medicinal_product_name,LS_DB_PROD_APPROVAL_LIB.marketting_date = LS_DB_PROD_APPROVAL_LIB_TMP.marketting_date,LS_DB_PROD_APPROVAL_LIB.marketing_status = LS_DB_PROD_APPROVAL_LIB_TMP.marketing_status,LS_DB_PROD_APPROVAL_LIB.marketing_category = LS_DB_PROD_APPROVAL_LIB_TMP.marketing_category,LS_DB_PROD_APPROVAL_LIB.mah_name = LS_DB_PROD_APPROVAL_LIB_TMP.mah_name,LS_DB_PROD_APPROVAL_LIB.license_status = LS_DB_PROD_APPROVAL_LIB_TMP.license_status,LS_DB_PROD_APPROVAL_LIB.license_id = LS_DB_PROD_APPROVAL_LIB_TMP.license_id,LS_DB_PROD_APPROVAL_LIB.lead_review_codelist_id = LS_DB_PROD_APPROVAL_LIB_TMP.lead_review_codelist_id,LS_DB_PROD_APPROVAL_LIB.lead_center = LS_DB_PROD_APPROVAL_LIB_TMP.lead_center,LS_DB_PROD_APPROVAL_LIB.internal_prd_id = LS_DB_PROD_APPROVAL_LIB_TMP.internal_prd_id,LS_DB_PROD_APPROVAL_LIB.date_modified = LS_DB_PROD_APPROVAL_LIB_TMP.date_modified,LS_DB_PROD_APPROVAL_LIB.date_created = LS_DB_PROD_APPROVAL_LIB_TMP.date_created,LS_DB_PROD_APPROVAL_LIB.country_code = LS_DB_PROD_APPROVAL_LIB_TMP.country_code,LS_DB_PROD_APPROVAL_LIB.copy_approval_checked = LS_DB_PROD_APPROVAL_LIB_TMP.copy_approval_checked,LS_DB_PROD_APPROVAL_LIB.container_type = LS_DB_PROD_APPROVAL_LIB_TMP.container_type,LS_DB_PROD_APPROVAL_LIB.company_unit_record_id_mah = LS_DB_PROD_APPROVAL_LIB_TMP.company_unit_record_id_mah,LS_DB_PROD_APPROVAL_LIB.combination_product = LS_DB_PROD_APPROVAL_LIB_TMP.combination_product,LS_DB_PROD_APPROVAL_LIB.code_name = LS_DB_PROD_APPROVAL_LIB_TMP.code_name,LS_DB_PROD_APPROVAL_LIB.chemical_type = LS_DB_PROD_APPROVAL_LIB_TMP.chemical_type,LS_DB_PROD_APPROVAL_LIB.bio_ref_name = LS_DB_PROD_APPROVAL_LIB_TMP.bio_ref_name,LS_DB_PROD_APPROVAL_LIB.atc_vet_code = LS_DB_PROD_APPROVAL_LIB_TMP.atc_vet_code,LS_DB_PROD_APPROVAL_LIB.atc_code = LS_DB_PROD_APPROVAL_LIB_TMP.atc_code,LS_DB_PROD_APPROVAL_LIB.arisg_record_id = LS_DB_PROD_APPROVAL_LIB_TMP.arisg_record_id,LS_DB_PROD_APPROVAL_LIB.arisg_product_id = LS_DB_PROD_APPROVAL_LIB_TMP.arisg_product_id,LS_DB_PROD_APPROVAL_LIB.arisg_approval_id = LS_DB_PROD_APPROVAL_LIB_TMP.arisg_approval_id,LS_DB_PROD_APPROVAL_LIB.approval_type = LS_DB_PROD_APPROVAL_LIB_TMP.approval_type,LS_DB_PROD_APPROVAL_LIB.approval_submission_type_de_ml = LS_DB_PROD_APPROVAL_LIB_TMP.approval_submission_type_de_ml,LS_DB_PROD_APPROVAL_LIB.approval_submission_type = LS_DB_PROD_APPROVAL_LIB_TMP.approval_submission_type,LS_DB_PROD_APPROVAL_LIB.approval_status = LS_DB_PROD_APPROVAL_LIB_TMP.approval_status,LS_DB_PROD_APPROVAL_LIB.approval_start_date = LS_DB_PROD_APPROVAL_LIB_TMP.approval_start_date,LS_DB_PROD_APPROVAL_LIB.approval_no = LS_DB_PROD_APPROVAL_LIB_TMP.approval_no,LS_DB_PROD_APPROVAL_LIB.approval_date = LS_DB_PROD_APPROVAL_LIB_TMP.approval_date,LS_DB_PROD_APPROVAL_LIB.application_no = LS_DB_PROD_APPROVAL_LIB_TMP.application_no,LS_DB_PROD_APPROVAL_LIB.anda_no = LS_DB_PROD_APPROVAL_LIB_TMP.anda_no,LS_DB_PROD_APPROVAL_LIB.agent_name = LS_DB_PROD_APPROVAL_LIB_TMP.agent_name,LS_DB_PROD_APPROVAL_LIB.account_record_id_mah = LS_DB_PROD_APPROVAL_LIB_TMP.account_record_id_mah,LS_DB_PROD_APPROVAL_LIB.account_record_id_agent = LS_DB_PROD_APPROVAL_LIB_TMP.account_record_id_agent,
LS_DB_PROD_APPROVAL_LIB.PROCESSING_DT = LS_DB_PROD_APPROVAL_LIB_TMP.PROCESSING_DT ,
LS_DB_PROD_APPROVAL_LIB.expiry_date    =LS_DB_PROD_APPROVAL_LIB_TMP.expiry_date       ,
LS_DB_PROD_APPROVAL_LIB.created_by     =LS_DB_PROD_APPROVAL_LIB_TMP.created_by        ,
LS_DB_PROD_APPROVAL_LIB.created_dt     =LS_DB_PROD_APPROVAL_LIB_TMP.created_dt        ,
LS_DB_PROD_APPROVAL_LIB.load_ts        =LS_DB_PROD_APPROVAL_LIB_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP 
WHERE LS_DB_PROD_APPROVAL_LIB.INTEGRATION_ID = LS_DB_PROD_APPROVAL_LIB_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PROD_APPROVAL_LIB_TMP.PROCESSING_DT = LS_DB_PROD_APPROVAL_LIB.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,viewpdfflag,
verified,
user_modified,
user_created,
trade_id,
spr_id,
source,
record_id,
proprietary_name_suffix,
product_type,
product_division,
otc_altnte_id,
ndc_no,
medicinal_product_name,
marketting_date,
marketing_status,
marketing_category,
mah_name,
license_status,
license_id,
lead_review_codelist_id,
lead_center,
internal_prd_id,
date_modified,
date_created,
country_code,
copy_approval_checked,
container_type,
company_unit_record_id_mah,
combination_product,
code_name,
chemical_type,
bio_ref_name,
atc_vet_code,
atc_code,
arisg_record_id,
arisg_product_id,
arisg_approval_id,
approval_type,
approval_submission_type_de_ml,
approval_submission_type,
approval_status,
approval_start_date,
approval_no,
approval_date,
application_no,
anda_no,
agent_name,
account_record_id_mah,
account_record_id_agent)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,viewpdfflag,
verified,
user_modified,
user_created,
trade_id,
spr_id,
source,
record_id,
proprietary_name_suffix,
product_type,
product_division,
otc_altnte_id,
ndc_no,
medicinal_product_name,
marketting_date,
marketing_status,
marketing_category,
mah_name,
license_status,
license_id,
lead_review_codelist_id,
lead_center,
internal_prd_id,
date_modified,
date_created,
country_code,
copy_approval_checked,
container_type,
company_unit_record_id_mah,
combination_product,
code_name,
chemical_type,
bio_ref_name,
atc_vet_code,
atc_code,
arisg_record_id,
arisg_product_id,
arisg_approval_id,
approval_type,
approval_submission_type_de_ml,
approval_submission_type,
approval_status,
approval_start_date,
approval_no,
approval_date,
application_no,
anda_no,
agent_name,
account_record_id_mah,
account_record_id_agent
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP 
WHERE TO_DATE(LS_DB_PROD_APPROVAL_LIB.PROCESSING_DT) < TO_DATE(LS_DB_PROD_APPROVAL_LIB_TMP.PROCESSING_DT)
AND LS_DB_PROD_APPROVAL_LIB.INTEGRATION_ID = LS_DB_PROD_APPROVAL_LIB_TMP.INTEGRATION_ID
AND LS_DB_PROD_APPROVAL_LIB.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PROD_APPROVAL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_approval')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PROD_APPROVAL_LIB_DELETION_TMP  WHERE TABLE_NAME='lsmv_product_approval')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PROD_APPROVAL_LIB_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PROD_APPROVAL_LIB'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PROD_APPROVAL_LIB Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PROD_APPROVAL_LIB'),
                'LSDB','Case','LS_DB_PROD_APPROVAL_LIB',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_PROD_APPROVAL_LIB not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
