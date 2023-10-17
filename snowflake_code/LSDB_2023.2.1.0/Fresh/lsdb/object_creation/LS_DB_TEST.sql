/* CREATE TABLE IF NOT EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
(FUNCTIONAL_AREA	VARCHAR(25)
,ENTITY_NAME	VARCHAR(25)
,SRC_TABLE_NAME	VARCHAR(100)
,LOAD_TS	TIMESTAMP_NTZ(9)
,SRC_COLUMN_NAME	VARCHAR(100)
,VALUE	VARCHAR(4000)
,RECORD_ID	NUMBER(38,0)
,CASE_NO	VARCHAR(4000)
,ERROR_DETAILS	VARCHAR(4000)); */



-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

-- New code starts
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST'),
	'LSRA','Case','LS_DB_TEST',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_TEST' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_TEST' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 

-- New code ends

-- Remove below

/*
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_TEST' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_TEST'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_TEST'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

*/


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_TEST_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_TEST_DELETION_TMP  As select RECORD_ID,'lsmv_test' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_test WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','158','8138','9107','9970')
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
					),
LSMV_CASE_NO_SUBSET as
 (
 -- Replacing LS_DB_ETL_CONFIG with LS_DB_ETL_CONFIG_TMP table, upper bound is changed
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_test WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_TEST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_test WHERE CDC_OPERATION_TIME >( SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_TEST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_test_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,coding_comments  coding_comments,coding_type  coding_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='158' AND CD=CAST(coding_type AS VARCHAR(100)) )coding_type_de_ml , comments  comments,comp_rec_id  comp_rec_id,component  component,date_created  date_created,date_modified  date_modified,entity_updated  entity_updated,excel_import  excel_import,ext_clob_fld  ext_clob_fld,fk_apat_rec_id  fk_apat_rec_id,hightestrange  hightestrange,hightestrange_lang  hightestrange_lang,include_in_ang  include_in_ang,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9970' AND CD=CAST(include_in_ang AS VARCHAR(100)) )include_in_ang_de_ml , inq_rec_id  inq_rec_id,loinc  loinc,loinc_coded_flag  loinc_coded_flag,loinccode  loinccode,long_common_name  long_common_name,lowtestrange  lowtestrange,lowtestrange_lang  lowtestrange_lang,medicalepisode_code  medicalepisode_code,method_typ  method_typ,moreinformation  moreinformation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(moreinformation AS VARCHAR(100)) )moreinformation_de_ml , normal_value  normal_value,property  property,reactionterm  reactionterm,record_id  record_id,resultstestsprocedures  resultstestsprocedures,scale_typ  scale_typ,snomed  snomed,snomed_coded_flag  snomed_coded_flag,snomed_info  snomed_info,spr_id  spr_id,system  system,test_coded_flag  test_coded_flag,test_date_tz  test_date_tz,test_lang  test_lang,test_meddra_llt  test_meddra_llt,test_meddra_llt_code  test_meddra_llt_code,test_meddra_llt_decode  test_meddra_llt_decode,test_meddra_llt_level  test_meddra_llt_level,test_meddra_llt_version  test_meddra_llt_version,test_pt_code  test_pt_code,test_pt_decode  test_pt_decode,test_ptver  test_ptver,test_result_and_procedure  test_result_and_procedure,test_result_code  test_result_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9107' AND CD=CAST(test_result_code AS VARCHAR(100)) )test_result_code_de_ml , test_result_comp  test_result_comp,test_result_text  test_result_text,test_result_value  test_result_value,test_result_value_nf  test_result_value_nf,testdate  testdate,testdate_nf  testdate_nf,testdatefmt  testdatefmt,testname  testname,testname_lang  testname_lang,testresult  testresult,testresult_lang  testresult_lang,testresult_nf  testresult_nf,testunit  testunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8138' AND CD=CAST(testunit AS VARCHAR(100)) )testunit_de_ml , testunit_lang  testunit_lang,testunit_sf  testunit_sf,time_aspct  time_aspct,user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_test
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_TEST_DELETION_TMP  WHERE TABLE_NAME='lsmv_test')
  ) where REC_RANK=1 ), D_MEDDRA_ICD_SUBSET AS 
( select distinct BK_MEDDRA_ICD_WID,LLT_CODE,PT_CODE,PRIMARY_SOC_FG from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD
 -- WHERE MEDDRA_VERSION='26.0'
)
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_test_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_test_SUBSET.USER_CREATED CREATED_BY,lsmv_test_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_test_SUBSET.version  ,lsmv_test_SUBSET.uuid  ,lsmv_test_SUBSET.user_modified  ,lsmv_test_SUBSET.user_created  ,lsmv_test_SUBSET.time_aspct  ,lsmv_test_SUBSET.testunit_sf  ,lsmv_test_SUBSET.testunit_lang  ,lsmv_test_SUBSET.testunit_de_ml  ,lsmv_test_SUBSET.testunit  ,lsmv_test_SUBSET.testresult_nf  ,lsmv_test_SUBSET.testresult_lang  ,lsmv_test_SUBSET.testresult  ,lsmv_test_SUBSET.testname_lang  ,lsmv_test_SUBSET.testname  ,lsmv_test_SUBSET.testdatefmt  ,lsmv_test_SUBSET.testdate_nf  ,lsmv_test_SUBSET.testdate  ,lsmv_test_SUBSET.test_result_value_nf  ,lsmv_test_SUBSET.test_result_value  ,lsmv_test_SUBSET.test_result_text  ,lsmv_test_SUBSET.test_result_comp  ,lsmv_test_SUBSET.test_result_code_de_ml  ,lsmv_test_SUBSET.test_result_code  ,lsmv_test_SUBSET.test_result_and_procedure  ,lsmv_test_SUBSET.test_ptver  ,lsmv_test_SUBSET.test_pt_decode  ,try_to_number(lsmv_test_SUBSET.test_pt_code,38) test_pt_code  ,lsmv_test_SUBSET.test_meddra_llt_version  ,lsmv_test_SUBSET.test_meddra_llt_level  ,lsmv_test_SUBSET.test_meddra_llt_decode  ,lsmv_test_SUBSET.test_meddra_llt_code  ,lsmv_test_SUBSET.test_meddra_llt  ,lsmv_test_SUBSET.test_lang  ,lsmv_test_SUBSET.test_date_tz  ,lsmv_test_SUBSET.test_coded_flag  ,lsmv_test_SUBSET.system  ,lsmv_test_SUBSET.spr_id  ,lsmv_test_SUBSET.snomed_info  ,lsmv_test_SUBSET.snomed_coded_flag  ,lsmv_test_SUBSET.snomed  ,lsmv_test_SUBSET.scale_typ  ,lsmv_test_SUBSET.resultstestsprocedures  ,lsmv_test_SUBSET.record_id  ,lsmv_test_SUBSET.reactionterm  ,lsmv_test_SUBSET.property  ,lsmv_test_SUBSET.normal_value  ,lsmv_test_SUBSET.moreinformation_de_ml  ,lsmv_test_SUBSET.moreinformation  ,lsmv_test_SUBSET.method_typ  ,try_to_number(lsmv_test_SUBSET.medicalepisode_code,38) medicalepisode_code ,lsmv_test_SUBSET.lowtestrange_lang  ,lsmv_test_SUBSET.lowtestrange  ,lsmv_test_SUBSET.long_common_name  ,lsmv_test_SUBSET.loinccode  ,lsmv_test_SUBSET.loinc_coded_flag  ,lsmv_test_SUBSET.loinc  ,lsmv_test_SUBSET.inq_rec_id  ,lsmv_test_SUBSET.include_in_ang_de_ml  ,lsmv_test_SUBSET.include_in_ang  ,lsmv_test_SUBSET.hightestrange_lang  ,lsmv_test_SUBSET.hightestrange  ,lsmv_test_SUBSET.fk_apat_rec_id  ,lsmv_test_SUBSET.ext_clob_fld  ,lsmv_test_SUBSET.excel_import  ,lsmv_test_SUBSET.entity_updated  ,lsmv_test_SUBSET.date_modified  ,lsmv_test_SUBSET.date_created  ,lsmv_test_SUBSET.component  ,lsmv_test_SUBSET.comp_rec_id  ,lsmv_test_SUBSET.comments  ,lsmv_test_SUBSET.coding_type_de_ml  ,lsmv_test_SUBSET.coding_type  ,lsmv_test_SUBSET.coding_comments  ,lsmv_test_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_test_SUBSET.RECORD_ID,-1)) INTEGRATION_ID,test_pt_code test_pt_code_1,lsmv_test_SUBSET.medicalepisode_code medicalepisode_code_1,COALESCE(D_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,-1) As TEST_PT_CODE_MD_BK
 FROM lsmv_test_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_test_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  
 LEFT JOIN D_MEDDRA_ICD_SUBSET
ON try_to_number(lsmv_test_SUBSET.MEDICALEPISODE_CODE,38)=try_to_number(D_MEDDRA_ICD_SUBSET.LLT_CODE,38) 
and cast(lsmv_test_SUBSET.TEST_PT_CODE as varchar(100))=cast(D_MEDDRA_ICD_SUBSET.PT_CODE as varchar(100)) and D_MEDDRA_ICD_SUBSET.PRIMARY_SOC_FG='Y'
 WHERE 1=1  
;


-- _TMP table is used, Changes required to remove the commented code

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP)
where target_table_name='LS_DB_TEST'
-- and LOAD_STATUS = 'In Progress'
-- and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST'					and LOAD_STATUS = 'In Progress') 
; 

-- New code for data validation begins

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_test' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'TEST_PT_CODE' ,
TEST_PT_CODE_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on TEST_PT_CODE'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP
WHERE (TEST_PT_CODE is null and TEST_PT_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_test' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'MEDICALEPISODE_CODE' ,
MEDICALEPISODE_CODE_1,
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on MEDICALEPISODE_CODE'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP
WHERE (MEDICALEPISODE_CODE is null and MEDICALEPISODE_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP
WHERE (TEST_PT_CODE is null and TEST_PT_CODE_1 is not null)
or (MEDICALEPISODE_CODE is null and MEDICALEPISODE_CODE_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

-- -- New code for data validation ends

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST   
SET LS_DB_TEST.version = LS_DB_TEST_TMP.version,LS_DB_TEST.uuid = LS_DB_TEST_TMP.uuid,LS_DB_TEST.user_modified = LS_DB_TEST_TMP.user_modified,LS_DB_TEST.user_created = LS_DB_TEST_TMP.user_created,LS_DB_TEST.time_aspct = LS_DB_TEST_TMP.time_aspct,LS_DB_TEST.testunit_sf = LS_DB_TEST_TMP.testunit_sf,LS_DB_TEST.testunit_lang = LS_DB_TEST_TMP.testunit_lang,LS_DB_TEST.testunit_de_ml = LS_DB_TEST_TMP.testunit_de_ml,LS_DB_TEST.testunit = LS_DB_TEST_TMP.testunit,LS_DB_TEST.testresult_nf = LS_DB_TEST_TMP.testresult_nf,LS_DB_TEST.testresult_lang = LS_DB_TEST_TMP.testresult_lang,LS_DB_TEST.testresult = LS_DB_TEST_TMP.testresult,LS_DB_TEST.testname_lang = LS_DB_TEST_TMP.testname_lang,LS_DB_TEST.testname = LS_DB_TEST_TMP.testname,LS_DB_TEST.testdatefmt = LS_DB_TEST_TMP.testdatefmt,LS_DB_TEST.testdate_nf = LS_DB_TEST_TMP.testdate_nf,LS_DB_TEST.testdate = LS_DB_TEST_TMP.testdate,LS_DB_TEST.test_result_value_nf = LS_DB_TEST_TMP.test_result_value_nf,LS_DB_TEST.test_result_value = LS_DB_TEST_TMP.test_result_value,LS_DB_TEST.test_result_text = LS_DB_TEST_TMP.test_result_text,LS_DB_TEST.test_result_comp = LS_DB_TEST_TMP.test_result_comp,LS_DB_TEST.test_result_code_de_ml = LS_DB_TEST_TMP.test_result_code_de_ml,LS_DB_TEST.test_result_code = LS_DB_TEST_TMP.test_result_code,LS_DB_TEST.test_result_and_procedure = LS_DB_TEST_TMP.test_result_and_procedure,LS_DB_TEST.test_ptver = LS_DB_TEST_TMP.test_ptver,LS_DB_TEST.test_pt_decode = LS_DB_TEST_TMP.test_pt_decode,LS_DB_TEST.test_pt_code = LS_DB_TEST_TMP.test_pt_code,LS_DB_TEST.test_meddra_llt_version = LS_DB_TEST_TMP.test_meddra_llt_version,LS_DB_TEST.test_meddra_llt_level = LS_DB_TEST_TMP.test_meddra_llt_level,LS_DB_TEST.test_meddra_llt_decode = LS_DB_TEST_TMP.test_meddra_llt_decode,LS_DB_TEST.test_meddra_llt_code = LS_DB_TEST_TMP.test_meddra_llt_code,LS_DB_TEST.test_meddra_llt = LS_DB_TEST_TMP.test_meddra_llt,LS_DB_TEST.test_lang = LS_DB_TEST_TMP.test_lang,LS_DB_TEST.test_date_tz = LS_DB_TEST_TMP.test_date_tz,LS_DB_TEST.test_coded_flag = LS_DB_TEST_TMP.test_coded_flag,LS_DB_TEST.system = LS_DB_TEST_TMP.system,LS_DB_TEST.spr_id = LS_DB_TEST_TMP.spr_id,LS_DB_TEST.snomed_info = LS_DB_TEST_TMP.snomed_info,LS_DB_TEST.snomed_coded_flag = LS_DB_TEST_TMP.snomed_coded_flag,LS_DB_TEST.snomed = LS_DB_TEST_TMP.snomed,LS_DB_TEST.scale_typ = LS_DB_TEST_TMP.scale_typ,LS_DB_TEST.resultstestsprocedures = LS_DB_TEST_TMP.resultstestsprocedures,LS_DB_TEST.record_id = LS_DB_TEST_TMP.record_id,LS_DB_TEST.reactionterm = LS_DB_TEST_TMP.reactionterm,LS_DB_TEST.property = LS_DB_TEST_TMP.property,LS_DB_TEST.normal_value = LS_DB_TEST_TMP.normal_value,LS_DB_TEST.moreinformation_de_ml = LS_DB_TEST_TMP.moreinformation_de_ml,LS_DB_TEST.moreinformation = LS_DB_TEST_TMP.moreinformation,LS_DB_TEST.method_typ = LS_DB_TEST_TMP.method_typ,LS_DB_TEST.medicalepisode_code = LS_DB_TEST_TMP.medicalepisode_code,LS_DB_TEST.lowtestrange_lang = LS_DB_TEST_TMP.lowtestrange_lang,LS_DB_TEST.lowtestrange = LS_DB_TEST_TMP.lowtestrange,LS_DB_TEST.long_common_name = LS_DB_TEST_TMP.long_common_name,LS_DB_TEST.loinccode = LS_DB_TEST_TMP.loinccode,LS_DB_TEST.loinc_coded_flag = LS_DB_TEST_TMP.loinc_coded_flag,LS_DB_TEST.loinc = LS_DB_TEST_TMP.loinc,LS_DB_TEST.inq_rec_id = LS_DB_TEST_TMP.inq_rec_id,LS_DB_TEST.include_in_ang_de_ml = LS_DB_TEST_TMP.include_in_ang_de_ml,LS_DB_TEST.include_in_ang = LS_DB_TEST_TMP.include_in_ang,LS_DB_TEST.hightestrange_lang = LS_DB_TEST_TMP.hightestrange_lang,LS_DB_TEST.hightestrange = LS_DB_TEST_TMP.hightestrange,LS_DB_TEST.fk_apat_rec_id = LS_DB_TEST_TMP.fk_apat_rec_id,LS_DB_TEST.ext_clob_fld = LS_DB_TEST_TMP.ext_clob_fld,LS_DB_TEST.excel_import = LS_DB_TEST_TMP.excel_import,LS_DB_TEST.entity_updated = LS_DB_TEST_TMP.entity_updated,LS_DB_TEST.date_modified = LS_DB_TEST_TMP.date_modified,LS_DB_TEST.date_created = LS_DB_TEST_TMP.date_created,LS_DB_TEST.component = LS_DB_TEST_TMP.component,LS_DB_TEST.comp_rec_id = LS_DB_TEST_TMP.comp_rec_id,LS_DB_TEST.comments = LS_DB_TEST_TMP.comments,LS_DB_TEST.coding_type_de_ml = LS_DB_TEST_TMP.coding_type_de_ml,LS_DB_TEST.coding_type = LS_DB_TEST_TMP.coding_type,LS_DB_TEST.coding_comments = LS_DB_TEST_TMP.coding_comments,LS_DB_TEST.ari_rec_id = LS_DB_TEST_TMP.ari_rec_id,
LS_DB_TEST.PROCESSING_DT = LS_DB_TEST_TMP.PROCESSING_DT ,
LS_DB_TEST.receipt_id     =LS_DB_TEST_TMP.receipt_id        ,
LS_DB_TEST.case_no        =LS_DB_TEST_TMP.case_no           ,
LS_DB_TEST.case_version   =LS_DB_TEST_TMP.case_version      ,
LS_DB_TEST.version_no     =LS_DB_TEST_TMP.version_no        ,
LS_DB_TEST.expiry_date    =LS_DB_TEST_TMP.expiry_date       ,
LS_DB_TEST.load_ts        =LS_DB_TEST_TMP.load_ts           ,
LS_DB_TEST.TEST_PT_CODE_MD_BK=LS_DB_TEST.TEST_PT_CODE_MD_BK
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP 
WHERE 	LS_DB_TEST.INTEGRATION_ID = LS_DB_TEST_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_TEST_TMP.PROCESSING_DT = LS_DB_TEST.PROCESSING_DT,1=1)
;


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
uuid,
user_modified,
user_created,
time_aspct,
testunit_sf,
testunit_lang,
testunit_de_ml,
testunit,
testresult_nf,
testresult_lang,
testresult,
testname_lang,
testname,
testdatefmt,
testdate_nf,
testdate,
test_result_value_nf,
test_result_value,
test_result_text,
test_result_comp,
test_result_code_de_ml,
test_result_code,
test_result_and_procedure,
test_ptver,
test_pt_decode,
test_pt_code,
test_meddra_llt_version,
test_meddra_llt_level,
test_meddra_llt_decode,
test_meddra_llt_code,
test_meddra_llt,
test_lang,
test_date_tz,
test_coded_flag,
system,
spr_id,
snomed_info,
snomed_coded_flag,
snomed,
scale_typ,
resultstestsprocedures,
record_id,
reactionterm,
property,
normal_value,
moreinformation_de_ml,
moreinformation,
method_typ,
medicalepisode_code,
lowtestrange_lang,
lowtestrange,
long_common_name,
loinccode,
loinc_coded_flag,
loinc,
inq_rec_id,
include_in_ang_de_ml,
include_in_ang,
hightestrange_lang,
hightestrange,
fk_apat_rec_id,
ext_clob_fld,
excel_import,
entity_updated,
date_modified,
date_created,
component,
comp_rec_id,
comments,
coding_type_de_ml,
coding_type,
coding_comments,
ari_rec_id,
TEST_PT_CODE_MD_BK)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,version,
uuid,
user_modified,
user_created,
time_aspct,
testunit_sf,
testunit_lang,
testunit_de_ml,
testunit,
testresult_nf,
testresult_lang,
testresult,
testname_lang,
testname,
testdatefmt,
testdate_nf,
testdate,
test_result_value_nf,
test_result_value,
test_result_text,
test_result_comp,
test_result_code_de_ml,
test_result_code,
test_result_and_procedure,
test_ptver,
test_pt_decode,
test_pt_code,
test_meddra_llt_version,
test_meddra_llt_level,
test_meddra_llt_decode,
test_meddra_llt_code,
test_meddra_llt,
test_lang,
test_date_tz,
test_coded_flag,
system,
spr_id,
snomed_info,
snomed_coded_flag,
snomed,
scale_typ,
resultstestsprocedures,
record_id,
reactionterm,
property,
normal_value,
moreinformation_de_ml,
moreinformation,
method_typ,
medicalepisode_code,
lowtestrange_lang,
lowtestrange,
long_common_name,
loinccode,
loinc_coded_flag,
loinc,
inq_rec_id,
include_in_ang_de_ml,
include_in_ang,
hightestrange_lang,
hightestrange,
fk_apat_rec_id,
ext_clob_fld,
excel_import,
entity_updated,
date_modified,
date_created,
component,
comp_rec_id,
comments,
coding_type_de_ml,
coding_type,
coding_comments,
ari_rec_id,
TEST_PT_CODE_MD_BK
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST TGT)								
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP 
WHERE 	TO_DATE(LS_DB_TEST.PROCESSING_DT) < TO_DATE(LS_DB_TEST_TMP.PROCESSING_DT)
AND LS_DB_TEST.INTEGRATION_ID = LS_DB_TEST_TMP.INTEGRATION_ID
AND LS_DB_TEST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_TEST_DELETION_TMP  WHERE TABLE_NAME='lsmv_test')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_TEST_DELETION_TMP  WHERE TABLE_NAME='lsmv_test')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');
		   
-- Changed to _TMP and remove the commented lines

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_TEST_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_TEST'
-- and LOAD_STATUS = 'In Progress'
-- and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST' and LOAD_STATUS = 'In Progress') 
;

-- Below lines have undergone changes related to locking

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;
           
  RETURN 'LS_DB_TEST Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_TEST'),
	'LSRA','Case','LS_DB_TEST',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null; 
	
  RETURN 'LS_DB_TEST not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;
