
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT_MED_HIST_EPISODE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_MED_HIST_EPISODE'),
	'LSDB','Case','LS_DB_PATIENT_MED_HIST_EPISODE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PATIENT_MED_HIST_EPISODE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PATIENT_MED_HIST_EPISODE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE_DELETION_TMP  As select RECORD_ID,'lsmv_patient_med_hist_episode' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_med_hist_episode WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1017','1021','1021','158','9917','9970')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_med_hist_episode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_MED_HIST_EPISODE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_med_hist_episode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_MED_HIST_EPISODE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_patient_med_hist_episode_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,cmp_pat_medical_history_id  cmp_pat_medical_history_id,cmp_pat_medical_history_text  cmp_pat_medical_history_text,coding_comments  coding_comments,coding_type  coding_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='158' AND CD=CAST(coding_type AS VARCHAR(100)) )coding_type_de_ml , comp_rec_id  comp_rec_id,concomitant_therapies  concomitant_therapies,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(concomitant_therapies AS VARCHAR(100)) )concomitant_therapies_de_ml , condition_treated  condition_treated,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(condition_treated AS VARCHAR(100)) )condition_treated_de_ml , date_created  date_created,date_modified  date_modified,disease_term_meddra_llt_term  disease_term_meddra_llt_term,disease_type  disease_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9917' AND CD=CAST(disease_type AS VARCHAR(100)) )disease_type_de_ml , try_to_number(duration,38)  duration,duration  duration_1 ,duration_unit  duration_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(duration_unit AS VARCHAR(100)) )duration_unit_de_ml , e2b_r3_familyhist  e2b_r3_familyhist,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(e2b_r3_familyhist AS VARCHAR(100)) )e2b_r3_familyhist_de_ml , entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fk_apat_rec_id  fk_apat_rec_id,icd_code  icd_code,include_in_ang  include_in_ang,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9970' AND CD=CAST(include_in_ang AS VARCHAR(100)) )include_in_ang_de_ml , inq_rec_id  inq_rec_id,medepisodenamemedver  medepisodenamemedver,medepisodenamemedver_lang  medepisodenamemedver_lang,medhist_coded_flag  medhist_coded_flag,medicalcomment  medicalcomment,medicalcomment_lang  medicalcomment_lang,medicalcontinue  medicalcontinue,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(medicalcontinue AS VARCHAR(100)) )medicalcontinue_de_ml , medicalcontinue_nf  medicalcontinue_nf,medicalenddate  medicalenddate,medicalenddate_nf  medicalenddate_nf,medicalenddate_tz  medicalenddate_tz,medicalenddatefmt  medicalenddatefmt,medicalepi_llt  medicalepi_llt,try_to_number(medicalepisode_code,38)  medicalepisode_code,medicalepisode_code  medicalepisode_code_1 ,medicalepisode_decode  medicalepisode_decode,medicalepisodename  medicalepisodename,medicalepisodename_lang  medicalepisodename_lang,medicalepisodename_nf  medicalepisodename_nf,medicalepisodenamelevel  medicalepisodenamelevel,medicalhis_lltver  medicalhis_lltver,medicalhis_ptver  medicalhis_ptver,medicalhistepisode_lang  medicalhistepisode_lang,medicalstartdate  medicalstartdate,medicalstartdate_nf  medicalstartdate_nf,medicalstartdate_tz  medicalstartdate_tz,medicalstartdatefmt  medicalstartdatefmt,try_to_number(medihist_lltcode,38)  medihist_lltcode,medihist_lltcode  medihist_lltcode_1 ,not_relevant  not_relevant,patient_disease_type  patient_disease_type,patmedicalhistorytext  patmedicalhistorytext,patmedicalhistorytext_nf  patmedicalhistorytext_nf,record_id  record_id,repo_pat_medhist_episode_id  repo_pat_medhist_episode_id,spr_id  spr_id,user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_patient_med_hist_episode
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_med_hist_episode')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_patient_med_hist_episode_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_patient_med_hist_episode_SUBSET.USER_CREATED CREATED_BY,lsmv_patient_med_hist_episode_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_patient_med_hist_episode_SUBSET.version  ,lsmv_patient_med_hist_episode_SUBSET.uuid  ,lsmv_patient_med_hist_episode_SUBSET.user_modified  ,lsmv_patient_med_hist_episode_SUBSET.user_created  ,lsmv_patient_med_hist_episode_SUBSET.spr_id  ,lsmv_patient_med_hist_episode_SUBSET.repo_pat_medhist_episode_id  ,lsmv_patient_med_hist_episode_SUBSET.record_id  ,lsmv_patient_med_hist_episode_SUBSET.patmedicalhistorytext_nf  ,lsmv_patient_med_hist_episode_SUBSET.patmedicalhistorytext  ,lsmv_patient_med_hist_episode_SUBSET.patient_disease_type  ,lsmv_patient_med_hist_episode_SUBSET.not_relevant  ,lsmv_patient_med_hist_episode_SUBSET.medihist_lltcode ,lsmv_patient_med_hist_episode_SUBSET.medihist_lltcode_1  ,lsmv_patient_med_hist_episode_SUBSET.medicalstartdatefmt  ,lsmv_patient_med_hist_episode_SUBSET.medicalstartdate_tz  ,lsmv_patient_med_hist_episode_SUBSET.medicalstartdate_nf  ,lsmv_patient_med_hist_episode_SUBSET.medicalstartdate  ,lsmv_patient_med_hist_episode_SUBSET.medicalhistepisode_lang  ,lsmv_patient_med_hist_episode_SUBSET.medicalhis_ptver  ,lsmv_patient_med_hist_episode_SUBSET.medicalhis_lltver  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisodenamelevel  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisodename_nf  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisodename_lang  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisodename  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisode_decode  ,lsmv_patient_med_hist_episode_SUBSET.medicalepisode_code ,lsmv_patient_med_hist_episode_SUBSET.medicalepisode_code_1  ,lsmv_patient_med_hist_episode_SUBSET.medicalepi_llt  ,lsmv_patient_med_hist_episode_SUBSET.medicalenddatefmt  ,lsmv_patient_med_hist_episode_SUBSET.medicalenddate_tz  ,lsmv_patient_med_hist_episode_SUBSET.medicalenddate_nf  ,lsmv_patient_med_hist_episode_SUBSET.medicalenddate  ,lsmv_patient_med_hist_episode_SUBSET.medicalcontinue_nf  ,lsmv_patient_med_hist_episode_SUBSET.medicalcontinue_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.medicalcontinue  ,lsmv_patient_med_hist_episode_SUBSET.medicalcomment_lang  ,lsmv_patient_med_hist_episode_SUBSET.medicalcomment  ,lsmv_patient_med_hist_episode_SUBSET.medhist_coded_flag  ,lsmv_patient_med_hist_episode_SUBSET.medepisodenamemedver_lang  ,lsmv_patient_med_hist_episode_SUBSET.medepisodenamemedver  ,lsmv_patient_med_hist_episode_SUBSET.inq_rec_id  ,lsmv_patient_med_hist_episode_SUBSET.include_in_ang_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.include_in_ang  ,lsmv_patient_med_hist_episode_SUBSET.icd_code  ,lsmv_patient_med_hist_episode_SUBSET.fk_apat_rec_id  ,lsmv_patient_med_hist_episode_SUBSET.ext_clob_fld  ,lsmv_patient_med_hist_episode_SUBSET.entity_updated  ,lsmv_patient_med_hist_episode_SUBSET.e2b_r3_familyhist_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.e2b_r3_familyhist  ,lsmv_patient_med_hist_episode_SUBSET.duration_unit_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.duration_unit  ,lsmv_patient_med_hist_episode_SUBSET.duration ,lsmv_patient_med_hist_episode_SUBSET.duration_1  ,lsmv_patient_med_hist_episode_SUBSET.disease_type_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.disease_type  ,lsmv_patient_med_hist_episode_SUBSET.disease_term_meddra_llt_term  ,lsmv_patient_med_hist_episode_SUBSET.date_modified  ,lsmv_patient_med_hist_episode_SUBSET.date_created  ,lsmv_patient_med_hist_episode_SUBSET.condition_treated_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.condition_treated  ,lsmv_patient_med_hist_episode_SUBSET.concomitant_therapies_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.concomitant_therapies  ,lsmv_patient_med_hist_episode_SUBSET.comp_rec_id  ,lsmv_patient_med_hist_episode_SUBSET.coding_type_de_ml  ,lsmv_patient_med_hist_episode_SUBSET.coding_type  ,lsmv_patient_med_hist_episode_SUBSET.coding_comments  ,lsmv_patient_med_hist_episode_SUBSET.cmp_pat_medical_history_text  ,lsmv_patient_med_hist_episode_SUBSET.cmp_pat_medical_history_id  ,lsmv_patient_med_hist_episode_SUBSET.ari_rec_id  ,COALESCE(D_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,-1) MEDIHIST_LLTCODE_MD_BK ,CONCAT(NVL(lsmv_patient_med_hist_episode_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_patient_med_hist_episode_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_patient_med_hist_episode_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id 
LEFT JOIN D_MEDDRA_ICD_SUBSET
ON try_to_number(lsmv_patient_med_hist_episode_SUBSET.MEDICALEPISODE_CODE,38) =try_to_number(D_MEDDRA_ICD_SUBSET.LLT_CODE ,38)
and try_to_number(lsmv_patient_med_hist_episode_SUBSET.MEDIHIST_LLTCODE,38) =try_to_number(D_MEDDRA_ICD_SUBSET.PT_CODE,38) and D_MEDDRA_ICD_SUBSET.PRIMARY_SOC_FG='Y'
 WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP)
where target_table_name='LS_DB_PATIENT_MED_HIST_EPISODE'

; 

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient_med_hist_episode' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'medihist_lltcode' ,
medihist_lltcode_1, 
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on medihist_lltcode'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP
WHERE (medihist_lltcode is null and medihist_lltcode_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient_med_hist_episode' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'medicalepisode_code' ,
medicalepisode_code_1, 
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on medicalepisode_code'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP
WHERE (medicalepisode_code is null and medicalepisode_code_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient_med_hist_episode' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'duration' ,
duration_1, 
RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on duration'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP
WHERE (duration is null and duration_1 is not null)
and ARI_REC_ID is not null 
and CASE_NO is not null;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP
WHERE (duration is null and duration_1 is not null) or (medicalepisode_code is null and medicalepisode_code_1 is not null) or (medihist_lltcode is null and medihist_lltcode_1 is not null) 
and CASE_NO is not null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE   
SET LS_DB_PATIENT_MED_HIST_EPISODE.version = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.version,LS_DB_PATIENT_MED_HIST_EPISODE.uuid = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.uuid,LS_DB_PATIENT_MED_HIST_EPISODE.user_modified = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.user_modified,LS_DB_PATIENT_MED_HIST_EPISODE.user_created = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.user_created,LS_DB_PATIENT_MED_HIST_EPISODE.spr_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.spr_id,LS_DB_PATIENT_MED_HIST_EPISODE.repo_pat_medhist_episode_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.repo_pat_medhist_episode_id,LS_DB_PATIENT_MED_HIST_EPISODE.record_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.record_id,LS_DB_PATIENT_MED_HIST_EPISODE.patmedicalhistorytext_nf = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.patmedicalhistorytext_nf,LS_DB_PATIENT_MED_HIST_EPISODE.patmedicalhistorytext = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.patmedicalhistorytext,LS_DB_PATIENT_MED_HIST_EPISODE.patient_disease_type = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.patient_disease_type,LS_DB_PATIENT_MED_HIST_EPISODE.not_relevant = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.not_relevant,LS_DB_PATIENT_MED_HIST_EPISODE.medihist_lltcode = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medihist_lltcode,LS_DB_PATIENT_MED_HIST_EPISODE.medicalstartdatefmt = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalstartdatefmt,LS_DB_PATIENT_MED_HIST_EPISODE.medicalstartdate_tz = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalstartdate_tz,LS_DB_PATIENT_MED_HIST_EPISODE.medicalstartdate_nf = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalstartdate_nf,LS_DB_PATIENT_MED_HIST_EPISODE.medicalstartdate = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalstartdate,LS_DB_PATIENT_MED_HIST_EPISODE.medicalhistepisode_lang = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalhistepisode_lang,LS_DB_PATIENT_MED_HIST_EPISODE.medicalhis_ptver = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalhis_ptver,LS_DB_PATIENT_MED_HIST_EPISODE.medicalhis_lltver = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalhis_lltver,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisodenamelevel = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisodenamelevel,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisodename_nf = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisodename_nf,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisodename_lang = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisodename_lang,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisodename = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisodename,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisode_decode = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisode_decode,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepisode_code = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepisode_code,LS_DB_PATIENT_MED_HIST_EPISODE.medicalepi_llt = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalepi_llt,LS_DB_PATIENT_MED_HIST_EPISODE.medicalenddatefmt = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalenddatefmt,LS_DB_PATIENT_MED_HIST_EPISODE.medicalenddate_tz = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalenddate_tz,LS_DB_PATIENT_MED_HIST_EPISODE.medicalenddate_nf = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalenddate_nf,LS_DB_PATIENT_MED_HIST_EPISODE.medicalenddate = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalenddate,LS_DB_PATIENT_MED_HIST_EPISODE.medicalcontinue_nf = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalcontinue_nf,LS_DB_PATIENT_MED_HIST_EPISODE.medicalcontinue_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalcontinue_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.medicalcontinue = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalcontinue,LS_DB_PATIENT_MED_HIST_EPISODE.medicalcomment_lang = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalcomment_lang,LS_DB_PATIENT_MED_HIST_EPISODE.medicalcomment = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medicalcomment,LS_DB_PATIENT_MED_HIST_EPISODE.medhist_coded_flag = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medhist_coded_flag,LS_DB_PATIENT_MED_HIST_EPISODE.medepisodenamemedver_lang = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medepisodenamemedver_lang,LS_DB_PATIENT_MED_HIST_EPISODE.medepisodenamemedver = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.medepisodenamemedver,LS_DB_PATIENT_MED_HIST_EPISODE.inq_rec_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.inq_rec_id,LS_DB_PATIENT_MED_HIST_EPISODE.include_in_ang_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.include_in_ang_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.include_in_ang = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.include_in_ang,LS_DB_PATIENT_MED_HIST_EPISODE.icd_code = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.icd_code,LS_DB_PATIENT_MED_HIST_EPISODE.fk_apat_rec_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.fk_apat_rec_id,LS_DB_PATIENT_MED_HIST_EPISODE.ext_clob_fld = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.ext_clob_fld,LS_DB_PATIENT_MED_HIST_EPISODE.entity_updated = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.entity_updated,LS_DB_PATIENT_MED_HIST_EPISODE.e2b_r3_familyhist_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.e2b_r3_familyhist_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.e2b_r3_familyhist = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.e2b_r3_familyhist,LS_DB_PATIENT_MED_HIST_EPISODE.duration_unit_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.duration_unit_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.duration_unit = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.duration_unit,LS_DB_PATIENT_MED_HIST_EPISODE.duration = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.duration,LS_DB_PATIENT_MED_HIST_EPISODE.disease_type_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.disease_type_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.disease_type = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.disease_type,LS_DB_PATIENT_MED_HIST_EPISODE.disease_term_meddra_llt_term = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.disease_term_meddra_llt_term,LS_DB_PATIENT_MED_HIST_EPISODE.date_modified = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.date_modified,LS_DB_PATIENT_MED_HIST_EPISODE.date_created = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.date_created,LS_DB_PATIENT_MED_HIST_EPISODE.condition_treated_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.condition_treated_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.condition_treated = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.condition_treated,LS_DB_PATIENT_MED_HIST_EPISODE.concomitant_therapies_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.concomitant_therapies_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.concomitant_therapies = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.concomitant_therapies,LS_DB_PATIENT_MED_HIST_EPISODE.comp_rec_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.comp_rec_id,LS_DB_PATIENT_MED_HIST_EPISODE.coding_type_de_ml = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.coding_type_de_ml,LS_DB_PATIENT_MED_HIST_EPISODE.coding_type = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.coding_type,LS_DB_PATIENT_MED_HIST_EPISODE.coding_comments = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.coding_comments,LS_DB_PATIENT_MED_HIST_EPISODE.cmp_pat_medical_history_text = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.cmp_pat_medical_history_text,LS_DB_PATIENT_MED_HIST_EPISODE.cmp_pat_medical_history_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.cmp_pat_medical_history_id,LS_DB_PATIENT_MED_HIST_EPISODE.ari_rec_id = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.ari_rec_id,LS_DB_PATIENT_MED_HIST_EPISODE.MEDIHIST_LLTCODE_MD_BK = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.MEDIHIST_LLTCODE_MD_BK,
LS_DB_PATIENT_MED_HIST_EPISODE.PROCESSING_DT = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.PROCESSING_DT ,
LS_DB_PATIENT_MED_HIST_EPISODE.receipt_id     =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.receipt_id        ,
LS_DB_PATIENT_MED_HIST_EPISODE.case_no        =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.case_no           ,
LS_DB_PATIENT_MED_HIST_EPISODE.case_version   =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.case_version      ,
LS_DB_PATIENT_MED_HIST_EPISODE.version_no     =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.version_no        ,
LS_DB_PATIENT_MED_HIST_EPISODE.expiry_date    =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.expiry_date       ,
LS_DB_PATIENT_MED_HIST_EPISODE.load_ts        =LS_DB_PATIENT_MED_HIST_EPISODE_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP 
WHERE 	LS_DB_PATIENT_MED_HIST_EPISODE.INTEGRATION_ID = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PATIENT_MED_HIST_EPISODE_TMP.PROCESSING_DT = LS_DB_PATIENT_MED_HIST_EPISODE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE
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
spr_id,
repo_pat_medhist_episode_id,
record_id,
patmedicalhistorytext_nf,
patmedicalhistorytext,
patient_disease_type,
not_relevant,
medihist_lltcode,
medicalstartdatefmt,
medicalstartdate_tz,
medicalstartdate_nf,
medicalstartdate,
medicalhistepisode_lang,
medicalhis_ptver,
medicalhis_lltver,
medicalepisodenamelevel,
medicalepisodename_nf,
medicalepisodename_lang,
medicalepisodename,
medicalepisode_decode,
medicalepisode_code,
medicalepi_llt,
medicalenddatefmt,
medicalenddate_tz,
medicalenddate_nf,
medicalenddate,
medicalcontinue_nf,
medicalcontinue_de_ml,
medicalcontinue,
medicalcomment_lang,
medicalcomment,
medhist_coded_flag,
medepisodenamemedver_lang,
medepisodenamemedver,
inq_rec_id,
include_in_ang_de_ml,
include_in_ang,
icd_code,
fk_apat_rec_id,
ext_clob_fld,
entity_updated,
e2b_r3_familyhist_de_ml,
e2b_r3_familyhist,
duration_unit_de_ml,
duration_unit,
duration,
disease_type_de_ml,
disease_type,
disease_term_meddra_llt_term,
date_modified,
date_created,
condition_treated_de_ml,
condition_treated,
concomitant_therapies_de_ml,
concomitant_therapies,
comp_rec_id,
coding_type_de_ml,
coding_type,
coding_comments,
cmp_pat_medical_history_text,
cmp_pat_medical_history_id,
ari_rec_id,
MEDIHIST_LLTCODE_MD_BK)
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
spr_id,
repo_pat_medhist_episode_id,
record_id,
patmedicalhistorytext_nf,
patmedicalhistorytext,
patient_disease_type,
not_relevant,
medihist_lltcode,
medicalstartdatefmt,
medicalstartdate_tz,
medicalstartdate_nf,
medicalstartdate,
medicalhistepisode_lang,
medicalhis_ptver,
medicalhis_lltver,
medicalepisodenamelevel,
medicalepisodename_nf,
medicalepisodename_lang,
medicalepisodename,
medicalepisode_decode,
medicalepisode_code,
medicalepi_llt,
medicalenddatefmt,
medicalenddate_tz,
medicalenddate_nf,
medicalenddate,
medicalcontinue_nf,
medicalcontinue_de_ml,
medicalcontinue,
medicalcomment_lang,
medicalcomment,
medhist_coded_flag,
medepisodenamemedver_lang,
medepisodenamemedver,
inq_rec_id,
include_in_ang_de_ml,
include_in_ang,
icd_code,
fk_apat_rec_id,
ext_clob_fld,
entity_updated,
e2b_r3_familyhist_de_ml,
e2b_r3_familyhist,
duration_unit_de_ml,
duration_unit,
duration,
disease_type_de_ml,
disease_type,
disease_term_meddra_llt_term,
date_modified,
date_created,
condition_treated_de_ml,
condition_treated,
concomitant_therapies_de_ml,
concomitant_therapies,
comp_rec_id,
coding_type_de_ml,
coding_type,
coding_comments,
cmp_pat_medical_history_text,
cmp_pat_medical_history_id,
ari_rec_id,
MEDIHIST_LLTCODE_MD_BK
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP 
WHERE 	TO_DATE(LS_DB_PATIENT_MED_HIST_EPISODE.PROCESSING_DT) < TO_DATE(LS_DB_PATIENT_MED_HIST_EPISODE_TMP.PROCESSING_DT)
AND LS_DB_PATIENT_MED_HIST_EPISODE.INTEGRATION_ID = LS_DB_PATIENT_MED_HIST_EPISODE_TMP.INTEGRATION_ID
AND LS_DB_PATIENT_MED_HIST_EPISODE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_med_hist_episode')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_MED_HIST_EPISODE_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_med_hist_episode')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_MED_HIST_EPISODE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PATIENT_MED_HIST_EPISODE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PATIENT_MED_HIST_EPISODE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_MED_HIST_EPISODE'),
	'LSDB','Case','LS_DB_PATIENT_MED_HIST_EPISODE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_PATIENT_MED_HIST_EPISODE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
