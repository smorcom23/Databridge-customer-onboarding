
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_COMPONENT_PROCEDURES()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES'),
	'LSDB','Case','LS_DB_DRUG_REACT_COMPONENT_PROCEDURES',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_COMPONENT_PROCEDURES_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_COMPONENT_PROCEDURES_DELETION_TMP  As select RECORD_ID,'lsmv_drug_react_component_procedures' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_component_procedures WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1017','1017','7077','9054','9054','9062','9062')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_component_procedures WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug_react_component_procedures WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_drug_react_component_procedures_SUBSET AS 
(
select * from 
    (SELECT  
    aesi  aesi,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(aesi AS VARCHAR(100)) )aesi_de_ml , aesi_manual  aesi_manual,ari_rec_id  ari_rec_id,assess_relationship  assess_relationship,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(assess_relationship AS VARCHAR(100)) )assess_relationship_de_ml , assess_relationship_manual  assess_relationship_manual,casuality_date_fmt  casuality_date_fmt,causality_date  causality_date,causality_flag  causality_flag,company_assessment  company_assessment,company_causality  company_causality,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(company_causality AS VARCHAR(100)) )company_causality_de_ml , date_created  date_created,date_modified  date_modified,dechallenge  dechallenge,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9054' AND CD=CAST(dechallenge AS VARCHAR(100)) )dechallenge_de_ml , end_latency  end_latency,end_latency_manual  end_latency_manual,end_latency_unit  end_latency_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(end_latency_unit AS VARCHAR(100)) )end_latency_unit_de_ml , fk_device_recid  fk_device_recid,fk_drug_recid  fk_drug_recid,fk_reaction_recid  fk_reaction_recid,im_recid  im_recid,rechallenge  rechallenge,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9054' AND CD=CAST(rechallenge AS VARCHAR(100)) )rechallenge_de_ml , record_id  record_id,reporter_assessment  reporter_assessment,reporter_causality  reporter_causality,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(reporter_causality AS VARCHAR(100)) )reporter_causality_de_ml , spr_id  spr_id,start_latency  start_latency,start_latency_manual  start_latency_manual,start_latency_unit  start_latency_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(start_latency_unit AS VARCHAR(100)) )start_latency_unit_de_ml , temp_relationship  temp_relationship,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(temp_relationship AS VARCHAR(100)) )temp_relationship_de_ml , user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_drug_react_component_procedures
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_COMPONENT_PROCEDURES_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_component_procedures')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_drug_react_component_procedures_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_drug_react_component_procedures_SUBSET.USER_CREATED CREATED_BY,lsmv_drug_react_component_procedures_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_drug_react_component_procedures_SUBSET.user_modified  ,lsmv_drug_react_component_procedures_SUBSET.user_created  ,lsmv_drug_react_component_procedures_SUBSET.temp_relationship_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.temp_relationship  ,lsmv_drug_react_component_procedures_SUBSET.start_latency_unit_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.start_latency_unit  ,lsmv_drug_react_component_procedures_SUBSET.start_latency_manual  ,lsmv_drug_react_component_procedures_SUBSET.start_latency  ,lsmv_drug_react_component_procedures_SUBSET.spr_id  ,lsmv_drug_react_component_procedures_SUBSET.reporter_causality_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.reporter_causality  ,lsmv_drug_react_component_procedures_SUBSET.reporter_assessment  ,lsmv_drug_react_component_procedures_SUBSET.record_id  ,lsmv_drug_react_component_procedures_SUBSET.rechallenge_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.rechallenge  ,lsmv_drug_react_component_procedures_SUBSET.im_recid  ,lsmv_drug_react_component_procedures_SUBSET.fk_reaction_recid  ,lsmv_drug_react_component_procedures_SUBSET.fk_drug_recid  ,lsmv_drug_react_component_procedures_SUBSET.fk_device_recid  ,lsmv_drug_react_component_procedures_SUBSET.end_latency_unit_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.end_latency_unit  ,lsmv_drug_react_component_procedures_SUBSET.end_latency_manual  ,lsmv_drug_react_component_procedures_SUBSET.end_latency  ,lsmv_drug_react_component_procedures_SUBSET.dechallenge_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.dechallenge  ,lsmv_drug_react_component_procedures_SUBSET.date_modified  ,lsmv_drug_react_component_procedures_SUBSET.date_created  ,lsmv_drug_react_component_procedures_SUBSET.company_causality_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.company_causality  ,lsmv_drug_react_component_procedures_SUBSET.company_assessment  ,lsmv_drug_react_component_procedures_SUBSET.causality_flag  ,lsmv_drug_react_component_procedures_SUBSET.causality_date  ,lsmv_drug_react_component_procedures_SUBSET.casuality_date_fmt  ,lsmv_drug_react_component_procedures_SUBSET.assess_relationship_manual  ,lsmv_drug_react_component_procedures_SUBSET.assess_relationship_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.assess_relationship  ,lsmv_drug_react_component_procedures_SUBSET.ari_rec_id  ,lsmv_drug_react_component_procedures_SUBSET.aesi_manual  ,lsmv_drug_react_component_procedures_SUBSET.aesi_de_ml  ,lsmv_drug_react_component_procedures_SUBSET.aesi ,CONCAT(NVL(lsmv_drug_react_component_procedures_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_drug_react_component_procedures_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_drug_react_component_procedures_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP)
where target_table_name='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES   
SET LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.user_modified = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.user_modified,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.user_created = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.user_created,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.temp_relationship_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.temp_relationship_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.temp_relationship = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.temp_relationship,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.start_latency_unit_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.start_latency_unit_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.start_latency_unit = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.start_latency_unit,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.start_latency_manual = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.start_latency_manual,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.start_latency = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.start_latency,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.spr_id = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.spr_id,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.reporter_causality_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.reporter_causality_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.reporter_causality = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.reporter_causality,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.reporter_assessment = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.reporter_assessment,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.record_id = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.record_id,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.rechallenge_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.rechallenge_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.rechallenge = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.rechallenge,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.im_recid = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.im_recid,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.fk_reaction_recid = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.fk_reaction_recid,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.fk_drug_recid = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.fk_drug_recid,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.fk_device_recid = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.fk_device_recid,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.end_latency_unit_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.end_latency_unit_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.end_latency_unit = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.end_latency_unit,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.end_latency_manual = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.end_latency_manual,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.end_latency = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.end_latency,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.dechallenge_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.dechallenge_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.dechallenge = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.dechallenge,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.date_modified = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.date_modified,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.date_created = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.date_created,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.company_causality_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.company_causality_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.company_causality = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.company_causality,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.company_assessment = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.company_assessment,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.causality_flag = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.causality_flag,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.causality_date = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.causality_date,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.casuality_date_fmt = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.casuality_date_fmt,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.assess_relationship_manual = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.assess_relationship_manual,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.assess_relationship_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.assess_relationship_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.assess_relationship = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.assess_relationship,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.ari_rec_id = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.ari_rec_id,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.aesi_manual = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.aesi_manual,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.aesi_de_ml = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.aesi_de_ml,LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.aesi = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.aesi,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.PROCESSING_DT = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.PROCESSING_DT ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.receipt_id     =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.receipt_id        ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.case_no        =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.case_no           ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.case_version   =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.case_version      ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.version_no     =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.version_no        ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.expiry_date    =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.expiry_date       ,
LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.load_ts        =LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP 
WHERE 	LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.INTEGRATION_ID = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.PROCESSING_DT = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
temp_relationship_de_ml,
temp_relationship,
start_latency_unit_de_ml,
start_latency_unit,
start_latency_manual,
start_latency,
spr_id,
reporter_causality_de_ml,
reporter_causality,
reporter_assessment,
record_id,
rechallenge_de_ml,
rechallenge,
im_recid,
fk_reaction_recid,
fk_drug_recid,
fk_device_recid,
end_latency_unit_de_ml,
end_latency_unit,
end_latency_manual,
end_latency,
dechallenge_de_ml,
dechallenge,
date_modified,
date_created,
company_causality_de_ml,
company_causality,
company_assessment,
causality_flag,
causality_date,
casuality_date_fmt,
assess_relationship_manual,
assess_relationship_de_ml,
assess_relationship,
ari_rec_id,
aesi_manual,
aesi_de_ml,
aesi)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
temp_relationship_de_ml,
temp_relationship,
start_latency_unit_de_ml,
start_latency_unit,
start_latency_manual,
start_latency,
spr_id,
reporter_causality_de_ml,
reporter_causality,
reporter_assessment,
record_id,
rechallenge_de_ml,
rechallenge,
im_recid,
fk_reaction_recid,
fk_drug_recid,
fk_device_recid,
end_latency_unit_de_ml,
end_latency_unit,
end_latency_manual,
end_latency,
dechallenge_de_ml,
dechallenge,
date_modified,
date_created,
company_causality_de_ml,
company_causality,
company_assessment,
causality_flag,
causality_date,
casuality_date_fmt,
assess_relationship_manual,
assess_relationship_de_ml,
assess_relationship,
ari_rec_id,
aesi_manual,
aesi_de_ml,
aesi
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP 
WHERE 	TO_DATE(LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.PROCESSING_DT) < TO_DATE(LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.PROCESSING_DT)
AND LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.INTEGRATION_ID = LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP.INTEGRATION_ID
AND LS_DB_DRUG_REACT_COMPONENT_PROCEDURES.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_COMPONENT_PROCEDURES_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_component_procedures')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DRUG_REACT_COMPONENT_PROCEDURES_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug_react_component_procedures')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRUG_REACT_COMPONENT_PROCEDURES_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DRUG_REACT_COMPONENT_PROCEDURES'),
	'LSDB','Case','LS_DB_DRUG_REACT_COMPONENT_PROCEDURES',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
