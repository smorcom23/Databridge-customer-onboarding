
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INDICATION()
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
ROW_WID	NUMBER(38,0),
FUNCTIONAL_AREA	VARCHAR(25),
ENTITY_NAME	VARCHAR(25),
TARGET_TABLE_NAME	VARCHAR(100),
LOAD_TS	TIMESTAMP_NTZ(9),
LOAD_START_TS	TIMESTAMP_NTZ(9),
LOAD_END_TS	TIMESTAMP_NTZ(9),
REC_READ_CNT	NUMBER(38,0),
REC_PROCESSED_CNT	NUMBER(38,0),
ERROR_REC_CNT	NUMBER(38,0),
ERROR_DETAILS	VARCHAR(8000),
LOAD_STATUS	VARCHAR(15),
CHANGED_REC_SET	VARIANT);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INDICATION'),
	'LSDB','Case','LS_DB_DRUG_INDICATION',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_INDICATION' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_INDICATION' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INDICATION_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INDICATION_DELETION_TMP  As select RECORD_ID,'lsmv_drug_indication' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_indication WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_indication WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INDICATION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_indication WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_INDICATION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 ,
	 LSMV_COMMON_COLUMN_SUBSET as
 (   select RECORD_ID,common_parent_key,ARI_REC_ID,CASE_NO,AER_VERSION_NO,RECEIPT_ID,RECEIPT_NO,VERSION_NO 
              from     (
                                                          select LSMV_CASE_NO_SUBSET.RECORD_ID,LSMV_CASE_NO_SUBSET.common_parent_key,AER_INFO.ARI_REC_ID, AER_INFO.AER_NO CASE_NO, AER_INFO.AER_VERSION_NO, RECPT_ITM.RECORD_ID RECEIPT_ID,
                                                                                      RECPT_ITM.RECEIPT_NO RECEIPT_NO,RECPT_ITM.VERSION VERSION_NO , 
                                                                                      row_number () OVER ( PARTITION BY LSMV_CASE_NO_SUBSET.RECORD_ID,RECPT_ITM.RECORD_ID ORDER BY to_date(GREATEST(
                                                                                                                                                                             NVL(RECPT_ITM.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(AER_INFO.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
                                                                                                                                                                                            )) DESC ) REC_RANK
                                                                                      from ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO AER_INFO,${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_drug_indication_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,cessation_date  cessation_date,coding_comments  coding_comments,coding_type  coding_type,comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,drgidcn_lltdecode  drgidcn_lltdecode,try_to_number(drgindcd_lltcode,38)  drgindcd_lltcode,drgindcd_lltcode  drgindcd_lltcode_1 ,drugindication  drugindication,try_to_number(drugindication_code,38)  drugindication_code,drugindication_code  drugindication_code_1 ,drugindication_decode  drugindication_decode,drugindication_lang  drugindication_lang,drugindication_level  drugindication_level,drugindication_nf  drugindication_nf,drugindication_ptver  drugindication_ptver,drugindicationmeddraver  drugindicationmeddraver,drugindicationmeddraver_lang  drugindicationmeddraver_lang,drugindn_coded_flag  drugindn_coded_flag,e2b_r3_med_prodid  e2b_r3_med_prodid,e2b_r3_medprodid_date  e2b_r3_medprodid_date,e2b_r3_medprodid_date_fmt  e2b_r3_medprodid_date_fmt,e2b_r3_medprodid_datenumber  e2b_r3_medprodid_datenumber,e2b_r3_pharma_prodid  e2b_r3_pharma_prodid,e2b_r3_pharmaprodid_date  e2b_r3_pharmaprodid_date,e2b_r3_pharmaprodid_date_fmt  e2b_r3_pharmaprodid_date_fmt,e2b_r3_pharmaprodid_datenumber  e2b_r3_pharmaprodid_datenumber,entity_updated  entity_updated,fk_ad_rec_id  fk_ad_rec_id,indication_labelling  indication_labelling,indication_lang  indication_lang,inq_rec_id  inq_rec_id,onset_date  onset_date,record_id  record_id,reported_term  reported_term,spr_id  spr_id,user_created  user_created,user_modified  user_modified,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_indication
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INDICATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_indication')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_indication_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_indication_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_indication_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_indication_SUBSET.version  ,lsmv_drug_indication_SUBSET.user_modified  ,lsmv_drug_indication_SUBSET.user_created  ,lsmv_drug_indication_SUBSET.spr_id  ,lsmv_drug_indication_SUBSET.reported_term  ,lsmv_drug_indication_SUBSET.record_id  ,lsmv_drug_indication_SUBSET.onset_date  ,lsmv_drug_indication_SUBSET.inq_rec_id  ,lsmv_drug_indication_SUBSET.indication_lang  ,lsmv_drug_indication_SUBSET.indication_labelling  ,lsmv_drug_indication_SUBSET.fk_ad_rec_id  ,lsmv_drug_indication_SUBSET.entity_updated  ,lsmv_drug_indication_SUBSET.e2b_r3_pharmaprodid_datenumber  ,lsmv_drug_indication_SUBSET.e2b_r3_pharmaprodid_date_fmt  ,lsmv_drug_indication_SUBSET.e2b_r3_pharmaprodid_date  ,lsmv_drug_indication_SUBSET.e2b_r3_pharma_prodid  ,lsmv_drug_indication_SUBSET.e2b_r3_medprodid_datenumber  ,lsmv_drug_indication_SUBSET.e2b_r3_medprodid_date_fmt  ,lsmv_drug_indication_SUBSET.e2b_r3_medprodid_date  ,lsmv_drug_indication_SUBSET.e2b_r3_med_prodid  ,lsmv_drug_indication_SUBSET.drugindn_coded_flag  ,lsmv_drug_indication_SUBSET.drugindicationmeddraver_lang  ,lsmv_drug_indication_SUBSET.drugindicationmeddraver  ,lsmv_drug_indication_SUBSET.drugindication_ptver  ,lsmv_drug_indication_SUBSET.drugindication_nf  ,lsmv_drug_indication_SUBSET.drugindication_level  ,lsmv_drug_indication_SUBSET.drugindication_lang  ,lsmv_drug_indication_SUBSET.drugindication_decode  ,lsmv_drug_indication_SUBSET.drugindication_code ,lsmv_drug_indication_SUBSET.drugindication_code_1  ,lsmv_drug_indication_SUBSET.drugindication  ,lsmv_drug_indication_SUBSET.drgindcd_lltcode ,lsmv_drug_indication_SUBSET.drgindcd_lltcode_1  ,lsmv_drug_indication_SUBSET.drgidcn_lltdecode  ,lsmv_drug_indication_SUBSET.date_modified  ,lsmv_drug_indication_SUBSET.date_created  ,lsmv_drug_indication_SUBSET.comp_rec_id  ,lsmv_drug_indication_SUBSET.coding_type  ,lsmv_drug_indication_SUBSET.coding_comments  ,lsmv_drug_indication_SUBSET.cessation_date  ,lsmv_drug_indication_SUBSET.ari_rec_id  ,COALESCE(D_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,-1) DRGINDCD_LLTCODE_MD_BK ,CONCAT(NVL(lsmv_drug_indication_SUBSET.RECORD_ID,-1)) INTEGRATION_ID 
FROM lsmv_drug_indication_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_indication_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id
LEFT JOIN D_MEDDRA_ICD_SUBSET
ON try_to_number(lsmv_drug_indication_SUBSET.DRUGINDICATION_CODE,38) =try_to_number(D_MEDDRA_ICD_SUBSET.LLT_CODE ,38)
and try_to_number(lsmv_drug_indication_SUBSET.DRGINDCD_LLTCODE,38) =try_to_number(D_MEDDRA_ICD_SUBSET.PT_CODE ,38)
and D_MEDDRA_ICD_SUBSET.PRIMARY_SOC_FG='Y'
WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP)
where target_table_name='LS_DB_DRUG_INDICATION'

; 

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_drug_indication' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'drugindication_code' ,
drugindication_code_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on drugindication_code'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP
WHERE (drugindication_code is null and drugindication_code_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_drug_indication' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'drgindcd_lltcode' ,
drgindcd_lltcode_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on drgindcd_lltcode'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP
WHERE (drgindcd_lltcode is null and drgindcd_lltcode_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP
WHERE (drgindcd_lltcode is null and drgindcd_lltcode_1 is not null) or (drugindication_code is null and drugindication_code_1 is not null) 
and ARI_REC_ID is not null 
and CASE_NO is not null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION   
SET LS_DB_DRUG_INDICATION.version = LS_DB_DRUG_INDICATION_TMP.version,LS_DB_DRUG_INDICATION.user_modified = LS_DB_DRUG_INDICATION_TMP.user_modified,LS_DB_DRUG_INDICATION.user_created = LS_DB_DRUG_INDICATION_TMP.user_created,LS_DB_DRUG_INDICATION.spr_id = LS_DB_DRUG_INDICATION_TMP.spr_id,LS_DB_DRUG_INDICATION.reported_term = LS_DB_DRUG_INDICATION_TMP.reported_term,LS_DB_DRUG_INDICATION.record_id = LS_DB_DRUG_INDICATION_TMP.record_id,LS_DB_DRUG_INDICATION.onset_date = LS_DB_DRUG_INDICATION_TMP.onset_date,LS_DB_DRUG_INDICATION.inq_rec_id = LS_DB_DRUG_INDICATION_TMP.inq_rec_id,LS_DB_DRUG_INDICATION.indication_lang = LS_DB_DRUG_INDICATION_TMP.indication_lang,LS_DB_DRUG_INDICATION.indication_labelling = LS_DB_DRUG_INDICATION_TMP.indication_labelling,LS_DB_DRUG_INDICATION.fk_ad_rec_id = LS_DB_DRUG_INDICATION_TMP.fk_ad_rec_id,LS_DB_DRUG_INDICATION.entity_updated = LS_DB_DRUG_INDICATION_TMP.entity_updated,LS_DB_DRUG_INDICATION.e2b_r3_pharmaprodid_datenumber = LS_DB_DRUG_INDICATION_TMP.e2b_r3_pharmaprodid_datenumber,LS_DB_DRUG_INDICATION.e2b_r3_pharmaprodid_date_fmt = LS_DB_DRUG_INDICATION_TMP.e2b_r3_pharmaprodid_date_fmt,LS_DB_DRUG_INDICATION.e2b_r3_pharmaprodid_date = LS_DB_DRUG_INDICATION_TMP.e2b_r3_pharmaprodid_date,LS_DB_DRUG_INDICATION.e2b_r3_pharma_prodid = LS_DB_DRUG_INDICATION_TMP.e2b_r3_pharma_prodid,LS_DB_DRUG_INDICATION.e2b_r3_medprodid_datenumber = LS_DB_DRUG_INDICATION_TMP.e2b_r3_medprodid_datenumber,LS_DB_DRUG_INDICATION.e2b_r3_medprodid_date_fmt = LS_DB_DRUG_INDICATION_TMP.e2b_r3_medprodid_date_fmt,LS_DB_DRUG_INDICATION.e2b_r3_medprodid_date = LS_DB_DRUG_INDICATION_TMP.e2b_r3_medprodid_date,LS_DB_DRUG_INDICATION.e2b_r3_med_prodid = LS_DB_DRUG_INDICATION_TMP.e2b_r3_med_prodid,LS_DB_DRUG_INDICATION.drugindn_coded_flag = LS_DB_DRUG_INDICATION_TMP.drugindn_coded_flag,LS_DB_DRUG_INDICATION.drugindicationmeddraver_lang = LS_DB_DRUG_INDICATION_TMP.drugindicationmeddraver_lang,LS_DB_DRUG_INDICATION.drugindicationmeddraver = LS_DB_DRUG_INDICATION_TMP.drugindicationmeddraver,LS_DB_DRUG_INDICATION.drugindication_ptver = LS_DB_DRUG_INDICATION_TMP.drugindication_ptver,LS_DB_DRUG_INDICATION.drugindication_nf = LS_DB_DRUG_INDICATION_TMP.drugindication_nf,LS_DB_DRUG_INDICATION.drugindication_level = LS_DB_DRUG_INDICATION_TMP.drugindication_level,LS_DB_DRUG_INDICATION.drugindication_lang = LS_DB_DRUG_INDICATION_TMP.drugindication_lang,LS_DB_DRUG_INDICATION.drugindication_decode = LS_DB_DRUG_INDICATION_TMP.drugindication_decode,LS_DB_DRUG_INDICATION.drugindication_code = LS_DB_DRUG_INDICATION_TMP.drugindication_code,LS_DB_DRUG_INDICATION.drugindication = LS_DB_DRUG_INDICATION_TMP.drugindication,LS_DB_DRUG_INDICATION.drgindcd_lltcode = LS_DB_DRUG_INDICATION_TMP.drgindcd_lltcode,LS_DB_DRUG_INDICATION.drgidcn_lltdecode = LS_DB_DRUG_INDICATION_TMP.drgidcn_lltdecode,LS_DB_DRUG_INDICATION.date_modified = LS_DB_DRUG_INDICATION_TMP.date_modified,LS_DB_DRUG_INDICATION.date_created = LS_DB_DRUG_INDICATION_TMP.date_created,LS_DB_DRUG_INDICATION.comp_rec_id = LS_DB_DRUG_INDICATION_TMP.comp_rec_id,LS_DB_DRUG_INDICATION.coding_type = LS_DB_DRUG_INDICATION_TMP.coding_type,LS_DB_DRUG_INDICATION.coding_comments = LS_DB_DRUG_INDICATION_TMP.coding_comments,LS_DB_DRUG_INDICATION.cessation_date = LS_DB_DRUG_INDICATION_TMP.cessation_date,LS_DB_DRUG_INDICATION.ari_rec_id = LS_DB_DRUG_INDICATION_TMP.ari_rec_id,LS_DB_DRUG_INDICATION.DRGINDCD_LLTCODE_MD_BK = LS_DB_DRUG_INDICATION_TMP.DRGINDCD_LLTCODE_MD_BK,
LS_DB_DRUG_INDICATION.PROCESSING_DT = LS_DB_DRUG_INDICATION_TMP.PROCESSING_DT ,
LS_DB_DRUG_INDICATION.receipt_id     =LS_DB_DRUG_INDICATION_TMP.receipt_id        ,
LS_DB_DRUG_INDICATION.case_no        =LS_DB_DRUG_INDICATION_TMP.case_no           ,
LS_DB_DRUG_INDICATION.case_version   =LS_DB_DRUG_INDICATION_TMP.case_version      ,
LS_DB_DRUG_INDICATION.version_no     =LS_DB_DRUG_INDICATION_TMP.version_no        ,
LS_DB_DRUG_INDICATION.expiry_date    =LS_DB_DRUG_INDICATION_TMP.expiry_date       ,
LS_DB_DRUG_INDICATION.load_ts        =LS_DB_DRUG_INDICATION_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP 
WHERE 	LS_DB_DRUG_INDICATION.INTEGRATION_ID = LS_DB_DRUG_INDICATION_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_INDICATION_TMP.PROCESSING_DT = LS_DB_DRUG_INDICATION.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
spr_id,
reported_term,
record_id,
onset_date,
inq_rec_id,
indication_lang,
indication_labelling,
fk_ad_rec_id,
entity_updated,
e2b_r3_pharmaprodid_datenumber,
e2b_r3_pharmaprodid_date_fmt,
e2b_r3_pharmaprodid_date,
e2b_r3_pharma_prodid,
e2b_r3_medprodid_datenumber,
e2b_r3_medprodid_date_fmt,
e2b_r3_medprodid_date,
e2b_r3_med_prodid,
drugindn_coded_flag,
drugindicationmeddraver_lang,
drugindicationmeddraver,
drugindication_ptver,
drugindication_nf,
drugindication_level,
drugindication_lang,
drugindication_decode,
drugindication_code,
drugindication,
drgindcd_lltcode,
drgidcn_lltdecode,
date_modified,
date_created,
comp_rec_id,
coding_type,
coding_comments,
cessation_date,
ari_rec_id,
DRGINDCD_LLTCODE_MD_BK)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
user_modified,
user_created,
spr_id,
reported_term,
record_id,
onset_date,
inq_rec_id,
indication_lang,
indication_labelling,
fk_ad_rec_id,
entity_updated,
e2b_r3_pharmaprodid_datenumber,
e2b_r3_pharmaprodid_date_fmt,
e2b_r3_pharmaprodid_date,
e2b_r3_pharma_prodid,
e2b_r3_medprodid_datenumber,
e2b_r3_medprodid_date_fmt,
e2b_r3_medprodid_date,
e2b_r3_med_prodid,
drugindn_coded_flag,
drugindicationmeddraver_lang,
drugindicationmeddraver,
drugindication_ptver,
drugindication_nf,
drugindication_level,
drugindication_lang,
drugindication_decode,
drugindication_code,
drugindication,
drgindcd_lltcode,
drgidcn_lltdecode,
date_modified,
date_created,
comp_rec_id,
coding_type,
coding_comments,
cessation_date,
ari_rec_id,
DRGINDCD_LLTCODE_MD_BK
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_INDICATION.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_INDICATION_TMP.PROCESSING_DT)
AND LS_DB_DRUG_INDICATION.INTEGRATION_ID = LS_DB_DRUG_INDICATION_TMP.INTEGRATION_ID
AND LS_DB_DRUG_INDICATION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INDICATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_indication')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_INDICATION_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_indication')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_INDICATION_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_INDICATION'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_INDICATION Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_INDICATION'),
	'LSDB','Case','LS_DB_DRUG_INDICATION',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_INDICATION not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
