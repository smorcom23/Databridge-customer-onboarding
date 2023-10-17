
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS'),
	'LSDB','Case','LS_DB_DRUG_REACT_RELATEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_REACT_RELATEDNESS' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_REACT_RELATEDNESS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_RELATEDNESS_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_RELATEDNESS_DELETION_TMP  As select RECORD_ID,'lsmv_drug_react_relatedness' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1012','1017','1017','1027','21','315','7077','7077','7077','7077','9054','9055','9062','9062','9062','9062','9641','9642','9941','9941')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_RELATEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_RELATEDNESS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_drug_react_relatedness_SUBSET AS 
(
select * from 
    (SELECT  
    access_relationship  access_relationship,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(access_relationship AS VARCHAR(100)) )access_relationship_de_ml , access_reletionship_manual  access_reletionship_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(access_reletionship_manual AS VARCHAR(100)) )access_reletionship_manual_de_ml , actiondrug  actiondrug,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='21' AND CD=CAST(actiondrug AS VARCHAR(100)) )actiondrug_de_ml , aesi_manual  aesi_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(aesi_manual AS VARCHAR(100)) )aesi_manual_de_ml , ari_rec_id  ari_rec_id,causality_company  causality_company,causality_reporter  causality_reporter,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='315' AND CD=CAST(causality_reporter AS VARCHAR(100)) )causality_reporter_de_ml , causality_source  causality_source,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9055' AND CD=CAST(causality_source AS VARCHAR(100)) )causality_source_de_ml , causalitysource_ft  causalitysource_ft,changeindosage  changeindosage,com_drugresult_free_text  com_drugresult_free_text,comp_rec_id  comp_rec_id,company_causality  company_causality,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(company_causality AS VARCHAR(100)) )company_causality_de_ml , company_causality_ft  company_causality_ft,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9642' AND CD=CAST(company_causality_ft AS VARCHAR(100)) )company_causality_ft_de_ml , company_drugresult  company_drugresult,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(company_drugresult AS VARCHAR(100)) )company_drugresult_de_ml , date_created  date_created,date_modified  date_modified,dechallenge  dechallenge,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1027' AND CD=CAST(dechallenge AS VARCHAR(100)) )dechallenge_de_ml , drug_assessed_name  drug_assessed_name,drugadditionalinfo  drugadditionalinfo,drugassessmentmethod  drugassessmentmethod,drugassessmentmethod_lang  drugassessmentmethod_lang,drugassessmentsource  drugassessmentsource,drugassessmentsource_lang  drugassessmentsource_lang,drugendlatency  drugendlatency,drugendlatencyunit  drugendlatencyunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(drugendlatencyunit AS VARCHAR(100)) )drugendlatencyunit_de_ml , drugreactasses  drugreactasses,drugreactasses_code  drugreactasses_code,drugreactasses_lang  drugreactasses_lang,drugreactassesmeddraver  drugreactassesmeddraver,drugreactassesmeddraver_lang  drugreactassesmeddraver_lang,drugreactrelatedness_lang  drugreactrelatedness_lang,drugrecurreadministration  drugrecurreadministration,drugresult  drugresult,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(drugresult AS VARCHAR(100)) )drugresult_de_ml , drugresult_decode  drugresult_decode,drugresult_lang  drugresult_lang,drugstartlatency  drugstartlatency,drugstartlatencyunit  drugstartlatencyunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(drugstartlatencyunit AS VARCHAR(100)) )drugstartlatencyunit_de_ml , end_latency_manual  end_latency_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(end_latency_manual AS VARCHAR(100)) )end_latency_manual_de_ml , entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fde_seq_reaction  fde_seq_reaction,fk_ad_rec_id  fk_ad_rec_id,fk_ar_rec_id  fk_ar_rec_id,fk_drug_rec_id  fk_drug_rec_id,inq_rec_id  inq_rec_id,isaesi  isaesi,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(isaesi AS VARCHAR(100)) )isaesi_de_ml , methods  methods,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9641' AND CD=CAST(methods AS VARCHAR(100)) )methods_de_ml , methods_ft  methods_ft,outcome_after_change  outcome_after_change,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1012' AND CD=CAST(outcome_after_change AS VARCHAR(100)) )outcome_after_change_de_ml , rechallenge  rechallenge,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9054' AND CD=CAST(rechallenge AS VARCHAR(100)) )rechallenge_de_ml , record_id  record_id,relatedness_json_text  relatedness_json_text,repo_drugreactrelatedness_id  repo_drugreactrelatedness_id,repo_event_id  repo_event_id,repo_reporter_comments  repo_reporter_comments,reporter_causality  reporter_causality,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(reporter_causality AS VARCHAR(100)) )reporter_causality_de_ml , reporter_causality_ft  reporter_causality_ft,result_ft  result_ft,spr_id  spr_id,start_latency_manual  start_latency_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(start_latency_manual AS VARCHAR(100)) )start_latency_manual_de_ml , susar  susar,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(susar AS VARCHAR(100)) )susar_de_ml , susar_manual  susar_manual,user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_react_relatedness
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_react_relatedness_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_react_relatedness_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_react_relatedness_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_react_relatedness_SUBSET.version  ,lsmv_drug_react_relatedness_SUBSET.uuid  ,lsmv_drug_react_relatedness_SUBSET.user_modified  ,lsmv_drug_react_relatedness_SUBSET.user_created  ,lsmv_drug_react_relatedness_SUBSET.susar_manual  ,lsmv_drug_react_relatedness_SUBSET.susar_de_ml  ,lsmv_drug_react_relatedness_SUBSET.susar  ,lsmv_drug_react_relatedness_SUBSET.start_latency_manual_de_ml  ,lsmv_drug_react_relatedness_SUBSET.start_latency_manual  ,lsmv_drug_react_relatedness_SUBSET.spr_id  ,lsmv_drug_react_relatedness_SUBSET.result_ft  ,lsmv_drug_react_relatedness_SUBSET.reporter_causality_ft  ,lsmv_drug_react_relatedness_SUBSET.reporter_causality_de_ml  ,lsmv_drug_react_relatedness_SUBSET.reporter_causality  ,lsmv_drug_react_relatedness_SUBSET.repo_reporter_comments  ,lsmv_drug_react_relatedness_SUBSET.repo_event_id  ,lsmv_drug_react_relatedness_SUBSET.repo_drugreactrelatedness_id  ,lsmv_drug_react_relatedness_SUBSET.relatedness_json_text  ,lsmv_drug_react_relatedness_SUBSET.record_id  ,lsmv_drug_react_relatedness_SUBSET.rechallenge_de_ml  ,lsmv_drug_react_relatedness_SUBSET.rechallenge  ,lsmv_drug_react_relatedness_SUBSET.outcome_after_change_de_ml  ,lsmv_drug_react_relatedness_SUBSET.outcome_after_change  ,lsmv_drug_react_relatedness_SUBSET.methods_ft  ,lsmv_drug_react_relatedness_SUBSET.methods_de_ml  ,lsmv_drug_react_relatedness_SUBSET.methods  ,lsmv_drug_react_relatedness_SUBSET.isaesi_de_ml  ,lsmv_drug_react_relatedness_SUBSET.isaesi  ,lsmv_drug_react_relatedness_SUBSET.inq_rec_id  ,lsmv_drug_react_relatedness_SUBSET.fk_drug_rec_id  ,lsmv_drug_react_relatedness_SUBSET.fk_ar_rec_id  ,lsmv_drug_react_relatedness_SUBSET.fk_ad_rec_id  ,lsmv_drug_react_relatedness_SUBSET.fde_seq_reaction  ,lsmv_drug_react_relatedness_SUBSET.ext_clob_fld  ,lsmv_drug_react_relatedness_SUBSET.entity_updated  ,lsmv_drug_react_relatedness_SUBSET.end_latency_manual_de_ml  ,lsmv_drug_react_relatedness_SUBSET.end_latency_manual  ,lsmv_drug_react_relatedness_SUBSET.drugstartlatencyunit_de_ml  ,lsmv_drug_react_relatedness_SUBSET.drugstartlatencyunit  ,lsmv_drug_react_relatedness_SUBSET.drugstartlatency  ,lsmv_drug_react_relatedness_SUBSET.drugresult_lang  ,lsmv_drug_react_relatedness_SUBSET.drugresult_decode  ,lsmv_drug_react_relatedness_SUBSET.drugresult_de_ml  ,lsmv_drug_react_relatedness_SUBSET.drugresult  ,lsmv_drug_react_relatedness_SUBSET.drugrecurreadministration  ,lsmv_drug_react_relatedness_SUBSET.drugreactrelatedness_lang  ,lsmv_drug_react_relatedness_SUBSET.drugreactassesmeddraver_lang  ,lsmv_drug_react_relatedness_SUBSET.drugreactassesmeddraver  ,lsmv_drug_react_relatedness_SUBSET.drugreactasses_lang  ,lsmv_drug_react_relatedness_SUBSET.drugreactasses_code  ,lsmv_drug_react_relatedness_SUBSET.drugreactasses  ,lsmv_drug_react_relatedness_SUBSET.drugendlatencyunit_de_ml  ,lsmv_drug_react_relatedness_SUBSET.drugendlatencyunit  ,lsmv_drug_react_relatedness_SUBSET.drugendlatency  ,lsmv_drug_react_relatedness_SUBSET.drugassessmentsource_lang  ,lsmv_drug_react_relatedness_SUBSET.drugassessmentsource  ,lsmv_drug_react_relatedness_SUBSET.drugassessmentmethod_lang  ,lsmv_drug_react_relatedness_SUBSET.drugassessmentmethod  ,lsmv_drug_react_relatedness_SUBSET.drugadditionalinfo  ,lsmv_drug_react_relatedness_SUBSET.drug_assessed_name  ,lsmv_drug_react_relatedness_SUBSET.dechallenge_de_ml  ,lsmv_drug_react_relatedness_SUBSET.dechallenge  ,lsmv_drug_react_relatedness_SUBSET.date_modified  ,lsmv_drug_react_relatedness_SUBSET.date_created  ,lsmv_drug_react_relatedness_SUBSET.company_drugresult_de_ml  ,lsmv_drug_react_relatedness_SUBSET.company_drugresult  ,lsmv_drug_react_relatedness_SUBSET.company_causality_ft_de_ml  ,lsmv_drug_react_relatedness_SUBSET.company_causality_ft  ,lsmv_drug_react_relatedness_SUBSET.company_causality_de_ml  ,lsmv_drug_react_relatedness_SUBSET.company_causality  ,lsmv_drug_react_relatedness_SUBSET.comp_rec_id  ,lsmv_drug_react_relatedness_SUBSET.com_drugresult_free_text  ,lsmv_drug_react_relatedness_SUBSET.changeindosage  ,lsmv_drug_react_relatedness_SUBSET.causalitysource_ft  ,lsmv_drug_react_relatedness_SUBSET.causality_source_de_ml  ,lsmv_drug_react_relatedness_SUBSET.causality_source  ,lsmv_drug_react_relatedness_SUBSET.causality_reporter_de_ml  ,lsmv_drug_react_relatedness_SUBSET.causality_reporter  ,lsmv_drug_react_relatedness_SUBSET.causality_company  ,lsmv_drug_react_relatedness_SUBSET.ari_rec_id  ,lsmv_drug_react_relatedness_SUBSET.aesi_manual_de_ml  ,lsmv_drug_react_relatedness_SUBSET.aesi_manual  ,lsmv_drug_react_relatedness_SUBSET.actiondrug_de_ml  ,lsmv_drug_react_relatedness_SUBSET.actiondrug  ,lsmv_drug_react_relatedness_SUBSET.access_reletionship_manual_de_ml  ,lsmv_drug_react_relatedness_SUBSET.access_reletionship_manual  ,lsmv_drug_react_relatedness_SUBSET.access_relationship_de_ml  ,lsmv_drug_react_relatedness_SUBSET.access_relationship ,CONCAT(NVL(lsmv_drug_react_relatedness_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_react_relatedness_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_react_relatedness_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP)
where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS   
SET LS_DB_DRUG_REACT_RELATEDNESS.version = LS_DB_DRUG_REACT_RELATEDNESS_TMP.version,LS_DB_DRUG_REACT_RELATEDNESS.uuid = LS_DB_DRUG_REACT_RELATEDNESS_TMP.uuid,LS_DB_DRUG_REACT_RELATEDNESS.user_modified = LS_DB_DRUG_REACT_RELATEDNESS_TMP.user_modified,LS_DB_DRUG_REACT_RELATEDNESS.user_created = LS_DB_DRUG_REACT_RELATEDNESS_TMP.user_created,LS_DB_DRUG_REACT_RELATEDNESS.susar_manual = LS_DB_DRUG_REACT_RELATEDNESS_TMP.susar_manual,LS_DB_DRUG_REACT_RELATEDNESS.susar_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.susar_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.susar = LS_DB_DRUG_REACT_RELATEDNESS_TMP.susar,LS_DB_DRUG_REACT_RELATEDNESS.start_latency_manual_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.start_latency_manual_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.start_latency_manual = LS_DB_DRUG_REACT_RELATEDNESS_TMP.start_latency_manual,LS_DB_DRUG_REACT_RELATEDNESS.spr_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.spr_id,LS_DB_DRUG_REACT_RELATEDNESS.result_ft = LS_DB_DRUG_REACT_RELATEDNESS_TMP.result_ft,LS_DB_DRUG_REACT_RELATEDNESS.reporter_causality_ft = LS_DB_DRUG_REACT_RELATEDNESS_TMP.reporter_causality_ft,LS_DB_DRUG_REACT_RELATEDNESS.reporter_causality_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.reporter_causality_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.reporter_causality = LS_DB_DRUG_REACT_RELATEDNESS_TMP.reporter_causality,LS_DB_DRUG_REACT_RELATEDNESS.repo_reporter_comments = LS_DB_DRUG_REACT_RELATEDNESS_TMP.repo_reporter_comments,LS_DB_DRUG_REACT_RELATEDNESS.repo_event_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.repo_event_id,LS_DB_DRUG_REACT_RELATEDNESS.repo_drugreactrelatedness_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.repo_drugreactrelatedness_id,LS_DB_DRUG_REACT_RELATEDNESS.relatedness_json_text = LS_DB_DRUG_REACT_RELATEDNESS_TMP.relatedness_json_text,LS_DB_DRUG_REACT_RELATEDNESS.record_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.record_id,LS_DB_DRUG_REACT_RELATEDNESS.rechallenge_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.rechallenge_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.rechallenge = LS_DB_DRUG_REACT_RELATEDNESS_TMP.rechallenge,LS_DB_DRUG_REACT_RELATEDNESS.outcome_after_change_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.outcome_after_change_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.outcome_after_change = LS_DB_DRUG_REACT_RELATEDNESS_TMP.outcome_after_change,LS_DB_DRUG_REACT_RELATEDNESS.methods_ft = LS_DB_DRUG_REACT_RELATEDNESS_TMP.methods_ft,LS_DB_DRUG_REACT_RELATEDNESS.methods_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.methods_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.methods = LS_DB_DRUG_REACT_RELATEDNESS_TMP.methods,LS_DB_DRUG_REACT_RELATEDNESS.isaesi_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.isaesi_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.isaesi = LS_DB_DRUG_REACT_RELATEDNESS_TMP.isaesi,LS_DB_DRUG_REACT_RELATEDNESS.inq_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.inq_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.fk_drug_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.fk_drug_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.fk_ar_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.fk_ar_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.fk_ad_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.fk_ad_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.fde_seq_reaction = LS_DB_DRUG_REACT_RELATEDNESS_TMP.fde_seq_reaction,LS_DB_DRUG_REACT_RELATEDNESS.ext_clob_fld = LS_DB_DRUG_REACT_RELATEDNESS_TMP.ext_clob_fld,LS_DB_DRUG_REACT_RELATEDNESS.entity_updated = LS_DB_DRUG_REACT_RELATEDNESS_TMP.entity_updated,LS_DB_DRUG_REACT_RELATEDNESS.end_latency_manual_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.end_latency_manual_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.end_latency_manual = LS_DB_DRUG_REACT_RELATEDNESS_TMP.end_latency_manual,LS_DB_DRUG_REACT_RELATEDNESS.drugstartlatencyunit_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugstartlatencyunit_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.drugstartlatencyunit = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugstartlatencyunit,LS_DB_DRUG_REACT_RELATEDNESS.drugstartlatency = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugstartlatency,LS_DB_DRUG_REACT_RELATEDNESS.drugresult_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugresult_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugresult_decode = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugresult_decode,LS_DB_DRUG_REACT_RELATEDNESS.drugresult_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugresult_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.drugresult = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugresult,LS_DB_DRUG_REACT_RELATEDNESS.drugrecurreadministration = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugrecurreadministration,LS_DB_DRUG_REACT_RELATEDNESS.drugreactrelatedness_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactrelatedness_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugreactassesmeddraver_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactassesmeddraver_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugreactassesmeddraver = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactassesmeddraver,LS_DB_DRUG_REACT_RELATEDNESS.drugreactasses_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactasses_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugreactasses_code = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactasses_code,LS_DB_DRUG_REACT_RELATEDNESS.drugreactasses = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugreactasses,LS_DB_DRUG_REACT_RELATEDNESS.drugendlatencyunit_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugendlatencyunit_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.drugendlatencyunit = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugendlatencyunit,LS_DB_DRUG_REACT_RELATEDNESS.drugendlatency = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugendlatency,LS_DB_DRUG_REACT_RELATEDNESS.drugassessmentsource_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugassessmentsource_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugassessmentsource = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugassessmentsource,LS_DB_DRUG_REACT_RELATEDNESS.drugassessmentmethod_lang = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugassessmentmethod_lang,LS_DB_DRUG_REACT_RELATEDNESS.drugassessmentmethod = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugassessmentmethod,LS_DB_DRUG_REACT_RELATEDNESS.drugadditionalinfo = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drugadditionalinfo,LS_DB_DRUG_REACT_RELATEDNESS.drug_assessed_name = LS_DB_DRUG_REACT_RELATEDNESS_TMP.drug_assessed_name,LS_DB_DRUG_REACT_RELATEDNESS.dechallenge_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.dechallenge_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.dechallenge = LS_DB_DRUG_REACT_RELATEDNESS_TMP.dechallenge,LS_DB_DRUG_REACT_RELATEDNESS.date_modified = LS_DB_DRUG_REACT_RELATEDNESS_TMP.date_modified,LS_DB_DRUG_REACT_RELATEDNESS.date_created = LS_DB_DRUG_REACT_RELATEDNESS_TMP.date_created,LS_DB_DRUG_REACT_RELATEDNESS.company_drugresult_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_drugresult_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.company_drugresult = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_drugresult,LS_DB_DRUG_REACT_RELATEDNESS.company_causality_ft_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_causality_ft_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.company_causality_ft = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_causality_ft,LS_DB_DRUG_REACT_RELATEDNESS.company_causality_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_causality_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.company_causality = LS_DB_DRUG_REACT_RELATEDNESS_TMP.company_causality,LS_DB_DRUG_REACT_RELATEDNESS.comp_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.comp_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.com_drugresult_free_text = LS_DB_DRUG_REACT_RELATEDNESS_TMP.com_drugresult_free_text,LS_DB_DRUG_REACT_RELATEDNESS.changeindosage = LS_DB_DRUG_REACT_RELATEDNESS_TMP.changeindosage,LS_DB_DRUG_REACT_RELATEDNESS.causalitysource_ft = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causalitysource_ft,LS_DB_DRUG_REACT_RELATEDNESS.causality_source_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causality_source_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.causality_source = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causality_source,LS_DB_DRUG_REACT_RELATEDNESS.causality_reporter_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causality_reporter_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.causality_reporter = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causality_reporter,LS_DB_DRUG_REACT_RELATEDNESS.causality_company = LS_DB_DRUG_REACT_RELATEDNESS_TMP.causality_company,LS_DB_DRUG_REACT_RELATEDNESS.ari_rec_id = LS_DB_DRUG_REACT_RELATEDNESS_TMP.ari_rec_id,LS_DB_DRUG_REACT_RELATEDNESS.aesi_manual_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.aesi_manual_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.aesi_manual = LS_DB_DRUG_REACT_RELATEDNESS_TMP.aesi_manual,LS_DB_DRUG_REACT_RELATEDNESS.actiondrug_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.actiondrug_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.actiondrug = LS_DB_DRUG_REACT_RELATEDNESS_TMP.actiondrug,LS_DB_DRUG_REACT_RELATEDNESS.access_reletionship_manual_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.access_reletionship_manual_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.access_reletionship_manual = LS_DB_DRUG_REACT_RELATEDNESS_TMP.access_reletionship_manual,LS_DB_DRUG_REACT_RELATEDNESS.access_relationship_de_ml = LS_DB_DRUG_REACT_RELATEDNESS_TMP.access_relationship_de_ml,LS_DB_DRUG_REACT_RELATEDNESS.access_relationship = LS_DB_DRUG_REACT_RELATEDNESS_TMP.access_relationship,
LS_DB_DRUG_REACT_RELATEDNESS.PROCESSING_DT = LS_DB_DRUG_REACT_RELATEDNESS_TMP.PROCESSING_DT ,
LS_DB_DRUG_REACT_RELATEDNESS.receipt_id     =LS_DB_DRUG_REACT_RELATEDNESS_TMP.receipt_id        ,
LS_DB_DRUG_REACT_RELATEDNESS.case_no        =LS_DB_DRUG_REACT_RELATEDNESS_TMP.case_no           ,
LS_DB_DRUG_REACT_RELATEDNESS.case_version   =LS_DB_DRUG_REACT_RELATEDNESS_TMP.case_version      ,
LS_DB_DRUG_REACT_RELATEDNESS.version_no     =LS_DB_DRUG_REACT_RELATEDNESS_TMP.version_no        ,
LS_DB_DRUG_REACT_RELATEDNESS.expiry_date    =LS_DB_DRUG_REACT_RELATEDNESS_TMP.expiry_date       ,
LS_DB_DRUG_REACT_RELATEDNESS.load_ts        =LS_DB_DRUG_REACT_RELATEDNESS_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP 
WHERE 	LS_DB_DRUG_REACT_RELATEDNESS.INTEGRATION_ID = LS_DB_DRUG_REACT_RELATEDNESS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_REACT_RELATEDNESS_TMP.PROCESSING_DT = LS_DB_DRUG_REACT_RELATEDNESS.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS
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
susar_manual,
susar_de_ml,
susar,
start_latency_manual_de_ml,
start_latency_manual,
spr_id,
result_ft,
reporter_causality_ft,
reporter_causality_de_ml,
reporter_causality,
repo_reporter_comments,
repo_event_id,
repo_drugreactrelatedness_id,
relatedness_json_text,
record_id,
rechallenge_de_ml,
rechallenge,
outcome_after_change_de_ml,
outcome_after_change,
methods_ft,
methods_de_ml,
methods,
isaesi_de_ml,
isaesi,
inq_rec_id,
fk_drug_rec_id,
fk_ar_rec_id,
fk_ad_rec_id,
fde_seq_reaction,
ext_clob_fld,
entity_updated,
end_latency_manual_de_ml,
end_latency_manual,
drugstartlatencyunit_de_ml,
drugstartlatencyunit,
drugstartlatency,
drugresult_lang,
drugresult_decode,
drugresult_de_ml,
drugresult,
drugrecurreadministration,
drugreactrelatedness_lang,
drugreactassesmeddraver_lang,
drugreactassesmeddraver,
drugreactasses_lang,
drugreactasses_code,
drugreactasses,
drugendlatencyunit_de_ml,
drugendlatencyunit,
drugendlatency,
drugassessmentsource_lang,
drugassessmentsource,
drugassessmentmethod_lang,
drugassessmentmethod,
drugadditionalinfo,
drug_assessed_name,
dechallenge_de_ml,
dechallenge,
date_modified,
date_created,
company_drugresult_de_ml,
company_drugresult,
company_causality_ft_de_ml,
company_causality_ft,
company_causality_de_ml,
company_causality,
comp_rec_id,
com_drugresult_free_text,
changeindosage,
causalitysource_ft,
causality_source_de_ml,
causality_source,
causality_reporter_de_ml,
causality_reporter,
causality_company,
ari_rec_id,
aesi_manual_de_ml,
aesi_manual,
actiondrug_de_ml,
actiondrug,
access_reletionship_manual_de_ml,
access_reletionship_manual,
access_relationship_de_ml,
access_relationship)
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
susar_manual,
susar_de_ml,
susar,
start_latency_manual_de_ml,
start_latency_manual,
spr_id,
result_ft,
reporter_causality_ft,
reporter_causality_de_ml,
reporter_causality,
repo_reporter_comments,
repo_event_id,
repo_drugreactrelatedness_id,
relatedness_json_text,
record_id,
rechallenge_de_ml,
rechallenge,
outcome_after_change_de_ml,
outcome_after_change,
methods_ft,
methods_de_ml,
methods,
isaesi_de_ml,
isaesi,
inq_rec_id,
fk_drug_rec_id,
fk_ar_rec_id,
fk_ad_rec_id,
fde_seq_reaction,
ext_clob_fld,
entity_updated,
end_latency_manual_de_ml,
end_latency_manual,
drugstartlatencyunit_de_ml,
drugstartlatencyunit,
drugstartlatency,
drugresult_lang,
drugresult_decode,
drugresult_de_ml,
drugresult,
drugrecurreadministration,
drugreactrelatedness_lang,
drugreactassesmeddraver_lang,
drugreactassesmeddraver,
drugreactasses_lang,
drugreactasses_code,
drugreactasses,
drugendlatencyunit_de_ml,
drugendlatencyunit,
drugendlatency,
drugassessmentsource_lang,
drugassessmentsource,
drugassessmentmethod_lang,
drugassessmentmethod,
drugadditionalinfo,
drug_assessed_name,
dechallenge_de_ml,
dechallenge,
date_modified,
date_created,
company_drugresult_de_ml,
company_drugresult,
company_causality_ft_de_ml,
company_causality_ft,
company_causality_de_ml,
company_causality,
comp_rec_id,
com_drugresult_free_text,
changeindosage,
causalitysource_ft,
causality_source_de_ml,
causality_source,
causality_reporter_de_ml,
causality_reporter,
causality_company,
ari_rec_id,
aesi_manual_de_ml,
aesi_manual,
actiondrug_de_ml,
actiondrug,
access_reletionship_manual_de_ml,
access_reletionship_manual,
access_relationship_de_ml,
access_relationship
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_REACT_RELATEDNESS.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_REACT_RELATEDNESS_TMP.PROCESSING_DT)
AND LS_DB_DRUG_REACT_RELATEDNESS.INTEGRATION_ID = LS_DB_DRUG_REACT_RELATEDNESS_TMP.INTEGRATION_ID
AND LS_DB_DRUG_REACT_RELATEDNESS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_RELATEDNESS_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_relatedness')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_RELATEDNESS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_REACT_RELATEDNESS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_RELATEDNESS'),
	'LSDB','Case','LS_DB_DRUG_REACT_RELATEDNESS',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_REACT_RELATEDNESS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
