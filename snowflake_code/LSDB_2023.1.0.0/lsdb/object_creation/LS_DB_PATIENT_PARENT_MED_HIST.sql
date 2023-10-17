
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT_PARENT_MED_HIST()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'),
	'LSRA','Case','LS_DB_PATIENT_PARENT_MED_HIST',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PATIENT_PARENT_MED_HIST' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_PARENT_MED_HIST'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_PARENT_MED_HIST'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PARENT_MED_HIST_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PARENT_MED_HIST_DELETION_TMP  As select RECORD_ID,'lsmv_parent_med_hist_episode' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_parent_med_hist_episode WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1017','158','9917')
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_DECODE
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_parent_med_hist_episode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PARENT_MED_HIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PARENT_MED_HIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_parent_med_hist_episode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PARENT_MED_HIST' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT_PARENT_MED_HIST' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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
                                                                                      from $$STG_DB_NAME.$$LSDB_RPL.LSMV_AER_INFO AER_INFO,$$STG_DB_NAME.$$LSDB_RPL.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_parent_med_hist_episode_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,coding_comments  coding_comments,coding_type  coding_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='158' AND CD=CAST(coding_type AS VARCHAR(100)) )coding_type_de_ml , comp_rec_id  comp_rec_id,date_created  date_created,date_modified  date_modified,disease_type  disease_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9917' AND CD=CAST(disease_type AS VARCHAR(100)) )disease_type_de_ml , duration  duration,duration_unit  duration_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(duration_unit AS VARCHAR(100)) )duration_unit_de_ml , entity_updated  entity_updated,ext_clob_fld  ext_clob_fld,fk_apar_rec_id  fk_apar_rec_id,inq_rec_id  inq_rec_id,medepisodenamemedver  medepisodenamemedver,medepisodenamemedver_lang  medepisodenamemedver_lang,medhist_coded_flag  medhist_coded_flag,medicalcomment  medicalcomment,medicalcomment_lang  medicalcomment_lang,medicalcontinue  medicalcontinue,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(medicalcontinue AS VARCHAR(100)) )medicalcontinue_de_ml , medicalcontinue_nf  medicalcontinue_nf,medicalenddate  medicalenddate,medicalenddate_nf  medicalenddate_nf,medicalenddate_tz  medicalenddate_tz,medicalenddatefmt  medicalenddatefmt,medicalepisodename  medicalepisodename,medicalepisodename_code  medicalepisodename_code,medicalepisodename_decode  medicalepisodename_decode,medicalepisodename_lang  medicalepisodename_lang,medicalepisodename_ptcode  medicalepisodename_ptcode,medicalepisodenamelevel  medicalepisodenamelevel,medicalhistepisode_lang  medicalhistepisode_lang,medicalstartdate  medicalstartdate,medicalstartdate_nf  medicalstartdate_nf,medicalstartdate_tz  medicalstartdate_tz,medicalstartdatefmt  medicalstartdatefmt,record_id  record_id,relevant_med_history  relevant_med_history,seq_hist_episode  seq_hist_episode,spr_id  spr_id,user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_parent_med_hist_episode
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PARENT_MED_HIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_med_hist_episode')
  ) where REC_RANK=1 ), D_MEDDRA_ICD_SUBSET AS 
( select distinct BK_MEDDRA_ICD_WID,LLT_CODE,PT_CODE,PRIMARY_SOC_FG from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD
 -- WHERE MEDDRA_VERSION='26.0'
)
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_parent_med_hist_episode_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_parent_med_hist_episode_SUBSET.USER_CREATED CREATED_BY,lsmv_parent_med_hist_episode_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_parent_med_hist_episode_SUBSET.version  ,lsmv_parent_med_hist_episode_SUBSET.uuid  ,lsmv_parent_med_hist_episode_SUBSET.user_modified  ,lsmv_parent_med_hist_episode_SUBSET.user_created  ,lsmv_parent_med_hist_episode_SUBSET.spr_id  ,lsmv_parent_med_hist_episode_SUBSET.seq_hist_episode  ,lsmv_parent_med_hist_episode_SUBSET.relevant_med_history  ,lsmv_parent_med_hist_episode_SUBSET.record_id  ,lsmv_parent_med_hist_episode_SUBSET.medicalstartdatefmt  ,lsmv_parent_med_hist_episode_SUBSET.medicalstartdate_tz  ,lsmv_parent_med_hist_episode_SUBSET.medicalstartdate_nf  ,lsmv_parent_med_hist_episode_SUBSET.medicalstartdate  ,lsmv_parent_med_hist_episode_SUBSET.medicalhistepisode_lang  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodenamelevel  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodename_ptcode  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodename_lang  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodename_decode  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodename_code  ,lsmv_parent_med_hist_episode_SUBSET.medicalepisodename  ,lsmv_parent_med_hist_episode_SUBSET.medicalenddatefmt  ,lsmv_parent_med_hist_episode_SUBSET.medicalenddate_tz  ,lsmv_parent_med_hist_episode_SUBSET.medicalenddate_nf  ,lsmv_parent_med_hist_episode_SUBSET.medicalenddate  ,lsmv_parent_med_hist_episode_SUBSET.medicalcontinue_nf  ,lsmv_parent_med_hist_episode_SUBSET.medicalcontinue_de_ml  ,lsmv_parent_med_hist_episode_SUBSET.medicalcontinue  ,lsmv_parent_med_hist_episode_SUBSET.medicalcomment_lang  ,lsmv_parent_med_hist_episode_SUBSET.medicalcomment  ,lsmv_parent_med_hist_episode_SUBSET.medhist_coded_flag  ,lsmv_parent_med_hist_episode_SUBSET.medepisodenamemedver_lang  ,lsmv_parent_med_hist_episode_SUBSET.medepisodenamemedver  ,lsmv_parent_med_hist_episode_SUBSET.inq_rec_id  ,lsmv_parent_med_hist_episode_SUBSET.fk_apar_rec_id  ,lsmv_parent_med_hist_episode_SUBSET.ext_clob_fld  ,lsmv_parent_med_hist_episode_SUBSET.entity_updated  ,lsmv_parent_med_hist_episode_SUBSET.duration_unit_de_ml  ,lsmv_parent_med_hist_episode_SUBSET.duration_unit  ,lsmv_parent_med_hist_episode_SUBSET.duration  ,lsmv_parent_med_hist_episode_SUBSET.disease_type_de_ml  ,lsmv_parent_med_hist_episode_SUBSET.disease_type  ,lsmv_parent_med_hist_episode_SUBSET.date_modified  ,lsmv_parent_med_hist_episode_SUBSET.date_created  ,lsmv_parent_med_hist_episode_SUBSET.comp_rec_id  ,lsmv_parent_med_hist_episode_SUBSET.coding_type_de_ml  ,lsmv_parent_med_hist_episode_SUBSET.coding_type  ,lsmv_parent_med_hist_episode_SUBSET.coding_comments  ,lsmv_parent_med_hist_episode_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_parent_med_hist_episode_SUBSET.RECORD_ID,-1)) INTEGRATION_ID
,COALESCE(D_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,-1) As MEDICALEPISODENAME_PTCODE_MD_BK
 FROM lsmv_parent_med_hist_episode_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_parent_med_hist_episode_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id
LEFT JOIN D_MEDDRA_ICD_SUBSET
ON lsmv_parent_med_hist_episode_SUBSET.MEDICALEPISODENAME_CODE=D_MEDDRA_ICD_SUBSET.LLT_CODE 
and lsmv_parent_med_hist_episode_SUBSET.MEDICALEPISODENAME_PTCODE=D_MEDDRA_ICD_SUBSET.PT_CODE 
and D_MEDDRA_ICD_SUBSET.PRIMARY_SOC_FG='Y'

  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP)
where target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST   
SET LS_DB_PATIENT_PARENT_MED_HIST.version = LS_DB_PATIENT_PARENT_MED_HIST_TMP.version,LS_DB_PATIENT_PARENT_MED_HIST.uuid = LS_DB_PATIENT_PARENT_MED_HIST_TMP.uuid,LS_DB_PATIENT_PARENT_MED_HIST.user_modified = LS_DB_PATIENT_PARENT_MED_HIST_TMP.user_modified,LS_DB_PATIENT_PARENT_MED_HIST.user_created = LS_DB_PATIENT_PARENT_MED_HIST_TMP.user_created,LS_DB_PATIENT_PARENT_MED_HIST.spr_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.spr_id,LS_DB_PATIENT_PARENT_MED_HIST.seq_hist_episode = LS_DB_PATIENT_PARENT_MED_HIST_TMP.seq_hist_episode,LS_DB_PATIENT_PARENT_MED_HIST.relevant_med_history = LS_DB_PATIENT_PARENT_MED_HIST_TMP.relevant_med_history,LS_DB_PATIENT_PARENT_MED_HIST.record_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.record_id,LS_DB_PATIENT_PARENT_MED_HIST.medicalstartdatefmt = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalstartdatefmt,LS_DB_PATIENT_PARENT_MED_HIST.medicalstartdate_tz = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalstartdate_tz,LS_DB_PATIENT_PARENT_MED_HIST.medicalstartdate_nf = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalstartdate_nf,LS_DB_PATIENT_PARENT_MED_HIST.medicalstartdate = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalstartdate,LS_DB_PATIENT_PARENT_MED_HIST.medicalhistepisode_lang = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalhistepisode_lang,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodenamelevel = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodenamelevel,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodename_ptcode = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodename_ptcode,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodename_lang = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodename_lang,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodename_decode = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodename_decode,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodename_code = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodename_code,LS_DB_PATIENT_PARENT_MED_HIST.medicalepisodename = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalepisodename,LS_DB_PATIENT_PARENT_MED_HIST.medicalenddatefmt = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalenddatefmt,LS_DB_PATIENT_PARENT_MED_HIST.medicalenddate_tz = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalenddate_tz,LS_DB_PATIENT_PARENT_MED_HIST.medicalenddate_nf = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalenddate_nf,LS_DB_PATIENT_PARENT_MED_HIST.medicalenddate = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalenddate,LS_DB_PATIENT_PARENT_MED_HIST.medicalcontinue_nf = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalcontinue_nf,LS_DB_PATIENT_PARENT_MED_HIST.medicalcontinue_de_ml = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalcontinue_de_ml,LS_DB_PATIENT_PARENT_MED_HIST.medicalcontinue = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalcontinue,LS_DB_PATIENT_PARENT_MED_HIST.medicalcomment_lang = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalcomment_lang,LS_DB_PATIENT_PARENT_MED_HIST.medicalcomment = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medicalcomment,LS_DB_PATIENT_PARENT_MED_HIST.medhist_coded_flag = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medhist_coded_flag,LS_DB_PATIENT_PARENT_MED_HIST.medepisodenamemedver_lang = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medepisodenamemedver_lang,LS_DB_PATIENT_PARENT_MED_HIST.medepisodenamemedver = LS_DB_PATIENT_PARENT_MED_HIST_TMP.medepisodenamemedver,LS_DB_PATIENT_PARENT_MED_HIST.inq_rec_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.inq_rec_id,LS_DB_PATIENT_PARENT_MED_HIST.fk_apar_rec_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.fk_apar_rec_id,LS_DB_PATIENT_PARENT_MED_HIST.ext_clob_fld = LS_DB_PATIENT_PARENT_MED_HIST_TMP.ext_clob_fld,LS_DB_PATIENT_PARENT_MED_HIST.entity_updated = LS_DB_PATIENT_PARENT_MED_HIST_TMP.entity_updated,LS_DB_PATIENT_PARENT_MED_HIST.duration_unit_de_ml = LS_DB_PATIENT_PARENT_MED_HIST_TMP.duration_unit_de_ml,LS_DB_PATIENT_PARENT_MED_HIST.duration_unit = LS_DB_PATIENT_PARENT_MED_HIST_TMP.duration_unit,LS_DB_PATIENT_PARENT_MED_HIST.duration = LS_DB_PATIENT_PARENT_MED_HIST_TMP.duration,LS_DB_PATIENT_PARENT_MED_HIST.disease_type_de_ml = LS_DB_PATIENT_PARENT_MED_HIST_TMP.disease_type_de_ml,LS_DB_PATIENT_PARENT_MED_HIST.disease_type = LS_DB_PATIENT_PARENT_MED_HIST_TMP.disease_type,LS_DB_PATIENT_PARENT_MED_HIST.date_modified = LS_DB_PATIENT_PARENT_MED_HIST_TMP.date_modified,LS_DB_PATIENT_PARENT_MED_HIST.date_created = LS_DB_PATIENT_PARENT_MED_HIST_TMP.date_created,LS_DB_PATIENT_PARENT_MED_HIST.comp_rec_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.comp_rec_id,LS_DB_PATIENT_PARENT_MED_HIST.coding_type_de_ml = LS_DB_PATIENT_PARENT_MED_HIST_TMP.coding_type_de_ml,LS_DB_PATIENT_PARENT_MED_HIST.coding_type = LS_DB_PATIENT_PARENT_MED_HIST_TMP.coding_type,LS_DB_PATIENT_PARENT_MED_HIST.coding_comments = LS_DB_PATIENT_PARENT_MED_HIST_TMP.coding_comments,LS_DB_PATIENT_PARENT_MED_HIST.ari_rec_id = LS_DB_PATIENT_PARENT_MED_HIST_TMP.ari_rec_id,
LS_DB_PATIENT_PARENT_MED_HIST.PROCESSING_DT = LS_DB_PATIENT_PARENT_MED_HIST_TMP.PROCESSING_DT ,
LS_DB_PATIENT_PARENT_MED_HIST.receipt_id     =LS_DB_PATIENT_PARENT_MED_HIST_TMP.receipt_id        ,
LS_DB_PATIENT_PARENT_MED_HIST.case_no        =LS_DB_PATIENT_PARENT_MED_HIST_TMP.case_no           ,
LS_DB_PATIENT_PARENT_MED_HIST.case_version   =LS_DB_PATIENT_PARENT_MED_HIST_TMP.case_version      ,
LS_DB_PATIENT_PARENT_MED_HIST.version_no     =LS_DB_PATIENT_PARENT_MED_HIST_TMP.version_no        ,
LS_DB_PATIENT_PARENT_MED_HIST.expiry_date    =LS_DB_PATIENT_PARENT_MED_HIST_TMP.expiry_date       ,
LS_DB_PATIENT_PARENT_MED_HIST.load_ts        =LS_DB_PATIENT_PARENT_MED_HIST_TMP.load_ts           ,
LS_DB_PATIENT_PARENT_MED_HIST.MEDICALEPISODENAME_PTCODE_MD_BK=LS_DB_PATIENT_PARENT_MED_HIST_TMP.MEDICALEPISODENAME_PTCODE_MD_BK
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP 
WHERE 	LS_DB_PATIENT_PARENT_MED_HIST.INTEGRATION_ID = LS_DB_PATIENT_PARENT_MED_HIST_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PATIENT_PARENT_MED_HIST_TMP.PROCESSING_DT = LS_DB_PATIENT_PARENT_MED_HIST.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST
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
seq_hist_episode,
relevant_med_history,
record_id,
medicalstartdatefmt,
medicalstartdate_tz,
medicalstartdate_nf,
medicalstartdate,
medicalhistepisode_lang,
medicalepisodenamelevel,
medicalepisodename_ptcode,
medicalepisodename_lang,
medicalepisodename_decode,
medicalepisodename_code,
medicalepisodename,
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
fk_apar_rec_id,
ext_clob_fld,
entity_updated,
duration_unit_de_ml,
duration_unit,
duration,
disease_type_de_ml,
disease_type,
date_modified,
date_created,
comp_rec_id,
coding_type_de_ml,
coding_type,
coding_comments,
ari_rec_id,
MEDICALEPISODENAME_PTCODE_MD_BK)
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
seq_hist_episode,
relevant_med_history,
record_id,
medicalstartdatefmt,
medicalstartdate_tz,
medicalstartdate_nf,
medicalstartdate,
medicalhistepisode_lang,
medicalepisodenamelevel,
medicalepisodename_ptcode,
medicalepisodename_lang,
medicalepisodename_decode,
medicalepisodename_code,
medicalepisodename,
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
fk_apar_rec_id,
ext_clob_fld,
entity_updated,
duration_unit_de_ml,
duration_unit,
duration,
disease_type_de_ml,
disease_type,
date_modified,
date_created,
comp_rec_id,
coding_type_de_ml,
coding_type,
coding_comments,
ari_rec_id,
MEDICALEPISODENAME_PTCODE_MD_BK
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP 
WHERE 	TO_DATE(LS_DB_PATIENT_PARENT_MED_HIST.PROCESSING_DT) < TO_DATE(LS_DB_PATIENT_PARENT_MED_HIST_TMP.PROCESSING_DT)
AND LS_DB_PATIENT_PARENT_MED_HIST.INTEGRATION_ID = LS_DB_PATIENT_PARENT_MED_HIST_TMP.INTEGRATION_ID
AND LS_DB_PATIENT_PARENT_MED_HIST.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PARENT_MED_HIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_med_hist_episode')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PATIENT_PARENT_MED_HIST_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent_med_hist_episode')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_PARENT_MED_HIST_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PATIENT_PARENT_MED_HIST Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PATIENT_PARENT_MED_HIST'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PATIENT_PARENT_MED_HIST not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
