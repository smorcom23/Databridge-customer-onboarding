
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_NARRATIVE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_NARRATIVE'),
	'LSDB','Case','LS_DB_NARRATIVE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_NARRATIVE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_NARRATIVE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  As select RECORD_ID,'lsmv_st_summary_rpt_comments' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_summary_rpt_comments WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_summary' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_summary_rpt_comments_lang' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary_rpt_comments_lang WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('7077','9065')
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_summary_rpt_comments WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_summary_rpt_comments WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary_rpt_comments_lang WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_summary_rpt_comments_lang WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_NARRATIVE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_summary_SUBSET AS 
(
select * from 
    (SELECT  
    add_manufacturer_narrative  smry_add_manufacturer_narrative,additional_information  smry_additional_information,addtional_comments  smry_addtional_comments,ari_rec_id  smry_ari_rec_id,comp_rec_id  smry_comp_rec_id,company_narrative  smry_company_narrative,corrected_data  smry_corrected_data,corrective_actions  smry_corrective_actions,corrective_prevn_narrative  smry_corrective_prevn_narrative,counter_medications  smry_counter_medications,date_created  smry_date_created,date_modified  smry_date_modified,do_not_use_ang  smry_do_not_use_ang,do_not_use_ang_ac  smry_do_not_use_ang_ac,do_not_use_ang_cr  smry_do_not_use_ang_cr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(do_not_use_ang AS VARCHAR(100)) )smry_do_not_use_ang_de_ml , do_not_use_ang_pc  smry_do_not_use_ang_pc,e2b_r3_rep_comments_text  smry_e2b_r3_rep_comments_text,entity_updated  smry_entity_updated,evaluation_comments  smry_evaluation_comments,evaluation_summary  smry_evaluation_summary,ext_clob_fld  smry_ext_clob_fld,formatted_event_desc_text  smry_formatted_event_desc_text,identified_acton_narrative  smry_identified_acton_narrative,ini_manufactur_analysis  smry_ini_manufactur_analysis,inq_rec_id  smry_inq_rec_id,jpn_counter_measure  smry_jpn_counter_measure,jpn_remarks1  smry_jpn_remarks1,jpn_remarks2  smry_jpn_remarks2,jpn_remarks3  smry_jpn_remarks3,jpn_remarks4  smry_jpn_remarks4,labelled  smry_labelled,llt_code  smry_llt_code,llt_decode  smry_llt_decode,local_narratvive  smry_local_narratvive,manufacturer_narrative  smry_manufacturer_narrative,narrative_generated  smry_narrative_generated,narrative_generated_ac  smry_narrative_generated_ac,narrative_generated_cr  smry_narrative_generated_cr,narrative_generated_pc  smry_narrative_generated_pc,narrative_map  smry_narrative_map,narrative_map_ac  smry_narrative_map_ac,narrative_map_cr  smry_narrative_map_cr,narrative_map_pc  smry_narrative_map_pc,narrative_with_html  smry_narrative_with_html,narrativeincludeclinical  smry_narrativeincludeclinical,narrativeincludeclinical_lang  smry_narrativeincludeclinical_lang,narrativeincludeclinical_lde  smry_narrativeincludeclinical_lde,other_comments  smry_other_comments,other_comments_jpn  smry_other_comments_jpn,other_remarks  smry_other_remarks,patient_during_event_desc  smry_patient_during_event_desc,product_complaint_details  smry_product_complaint_details,rat_not_reporting  smry_rat_not_reporting,record_id  smry_record_id,referencedate1  smry_referencedate1,referencedate10  smry_referencedate10,referencedate10fmt  smry_referencedate10fmt,referencedate11  smry_referencedate11,referencedate11fmt  smry_referencedate11fmt,referencedate12  smry_referencedate12,referencedate12fmt  smry_referencedate12fmt,referencedate13  smry_referencedate13,referencedate13fmt  smry_referencedate13fmt,referencedate14  smry_referencedate14,referencedate14fmt  smry_referencedate14fmt,referencedate15  smry_referencedate15,referencedate15fmt  smry_referencedate15fmt,referencedate16  smry_referencedate16,referencedate16fmt  smry_referencedate16fmt,referencedate17  smry_referencedate17,referencedate17fmt  smry_referencedate17fmt,referencedate18  smry_referencedate18,referencedate18fmt  smry_referencedate18fmt,referencedate19  smry_referencedate19,referencedate19fmt  smry_referencedate19fmt,referencedate1fmt  smry_referencedate1fmt,referencedate2  smry_referencedate2,referencedate20  smry_referencedate20,referencedate20fmt  smry_referencedate20fmt,referencedate21  smry_referencedate21,referencedate21fmt  smry_referencedate21fmt,referencedate22  smry_referencedate22,referencedate22fmt  smry_referencedate22fmt,referencedate23  smry_referencedate23,referencedate23fmt  smry_referencedate23fmt,referencedate24  smry_referencedate24,referencedate24fmt  smry_referencedate24fmt,referencedate25  smry_referencedate25,referencedate25fmt  smry_referencedate25fmt,referencedate26  smry_referencedate26,referencedate26fmt  smry_referencedate26fmt,referencedate27  smry_referencedate27,referencedate27fmt  smry_referencedate27fmt,referencedate28  smry_referencedate28,referencedate28fmt  smry_referencedate28fmt,referencedate29  smry_referencedate29,referencedate29fmt  smry_referencedate29fmt,referencedate2fmt  smry_referencedate2fmt,referencedate3  smry_referencedate3,referencedate30  smry_referencedate30,referencedate30fmt  smry_referencedate30fmt,referencedate31  smry_referencedate31,referencedate31fmt  smry_referencedate31fmt,referencedate32  smry_referencedate32,referencedate32fmt  smry_referencedate32fmt,referencedate33  smry_referencedate33,referencedate33fmt  smry_referencedate33fmt,referencedate34  smry_referencedate34,referencedate34fmt  smry_referencedate34fmt,referencedate35  smry_referencedate35,referencedate35fmt  smry_referencedate35fmt,referencedate36  smry_referencedate36,referencedate36fmt  smry_referencedate36fmt,referencedate37  smry_referencedate37,referencedate37fmt  smry_referencedate37fmt,referencedate38  smry_referencedate38,referencedate38fmt  smry_referencedate38fmt,referencedate39  smry_referencedate39,referencedate39fmt  smry_referencedate39fmt,referencedate3fmt  smry_referencedate3fmt,referencedate4  smry_referencedate4,referencedate40  smry_referencedate40,referencedate40fmt  smry_referencedate40fmt,referencedate4fmt  smry_referencedate4fmt,referencedate5  smry_referencedate5,referencedate5fmt  smry_referencedate5fmt,referencedate6  smry_referencedate6,referencedate6fmt  smry_referencedate6fmt,referencedate7  smry_referencedate7,referencedate7fmt  smry_referencedate7fmt,referencedate8  smry_referencedate8,referencedate8fmt  smry_referencedate8fmt,referencedate9  smry_referencedate9,referencedate9fmt  smry_referencedate9fmt,remarks  smry_remarks,report_summary_jpn  smry_report_summary_jpn,reportercomment  smry_reportercomment,reportercomment_lang  smry_reportercomment_lang,sendercomment  smry_sendercomment,sendercomment_lang  smry_sendercomment_lang,sendercomment_lde  smry_sendercomment_lde,senderdiagnosis  smry_senderdiagnosis,senderdiagnosis_code  smry_senderdiagnosis_code,senderdiagnosis_lang  smry_senderdiagnosis_lang,senderdiagnosislevel  smry_senderdiagnosislevel,senderdiagnosismeddraver  smry_senderdiagnosismeddraver,senderdiagnosismeddraver_lang  smry_senderdiagnosismeddraver_lang,spr_id  smry_spr_id,summary_description  smry_summary_description,summary_lang  smry_summary_lang,tests_data  smry_tests_data,treatment_following_event  smry_treatment_following_event,txt_jpn_mhlw_rem_1  smry_txt_jpn_mhlw_rem_1,txt_jpn_mhlw_rem_2  smry_txt_jpn_mhlw_rem_2,txt_jpn_mhlw_rem_3  smry_txt_jpn_mhlw_rem_3,txt_jpn_mhlw_rem_4  smry_txt_jpn_mhlw_rem_4,user_created  smry_user_created,user_modified  smry_user_modified,version  smry_version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_summary
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary')
  ) where REC_RANK=1 )
  , lsmv_st_summary_rpt_comments_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  stsmryrpt_ari_rec_id,case_sum_and_rep_comments  stsmryrpt_case_sum_and_rep_comments,comments_lang  stsmryrpt_comments_lang,date_created  stsmryrpt_date_created,date_modified  stsmryrpt_date_modified,fk_lsm_rec_id  stsmryrpt_fk_lsm_rec_id,record_id  stsmryrpt_record_id,spr_id  stsmryrpt_spr_id,user_created  stsmryrpt_user_created,user_modified  stsmryrpt_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_st_summary_rpt_comments
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_summary_rpt_comments')
  ) where REC_RANK=1 )
  , lsmv_summary_rpt_comments_lang_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  smryrptcmtl_ari_rec_id,comments_lang  smryrptcmtl_comments_lang,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9065' AND CD=CAST(comments_lang AS VARCHAR(100)) )smryrptcmtl_comments_lang_de_ml , comp_rec_id  smryrptcmtl_comp_rec_id,date_created  smryrptcmtl_date_created,date_modified  smryrptcmtl_date_modified,fk_summary_rec_id  smryrptcmtl_fk_summary_rec_id,inq_rec_id  smryrptcmtl_inq_rec_id,record_id  smryrptcmtl_record_id,reporter_comments  smryrptcmtl_reporter_comments,spr_id  smryrptcmtl_spr_id,user_created  smryrptcmtl_user_created,user_modified  smryrptcmtl_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_summary_rpt_comments_lang
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary_rpt_comments_lang')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_summary_SUBSET.smry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_summary_SUBSET.smry_USER_MODIFIED USER_MODIFIED,lsmv_summary_SUBSET.smry_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_summary_SUBSET.smry_USER_CREATED CREATED_BY,lsmv_summary_SUBSET.smry_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_summary_SUBSET.smry_version  ,lsmv_summary_SUBSET.smry_user_modified  ,lsmv_summary_SUBSET.smry_user_created  ,lsmv_summary_SUBSET.smry_txt_jpn_mhlw_rem_4  ,lsmv_summary_SUBSET.smry_txt_jpn_mhlw_rem_3  ,lsmv_summary_SUBSET.smry_txt_jpn_mhlw_rem_2  ,lsmv_summary_SUBSET.smry_txt_jpn_mhlw_rem_1  ,lsmv_summary_SUBSET.smry_treatment_following_event  ,lsmv_summary_SUBSET.smry_tests_data  ,lsmv_summary_SUBSET.smry_summary_lang  ,lsmv_summary_SUBSET.smry_summary_description  ,lsmv_summary_SUBSET.smry_spr_id  ,lsmv_summary_SUBSET.smry_senderdiagnosismeddraver_lang  ,lsmv_summary_SUBSET.smry_senderdiagnosismeddraver  ,lsmv_summary_SUBSET.smry_senderdiagnosislevel  ,lsmv_summary_SUBSET.smry_senderdiagnosis_lang  ,lsmv_summary_SUBSET.smry_senderdiagnosis_code  ,lsmv_summary_SUBSET.smry_senderdiagnosis  ,lsmv_summary_SUBSET.smry_sendercomment_lde  ,lsmv_summary_SUBSET.smry_sendercomment_lang  ,lsmv_summary_SUBSET.smry_sendercomment  ,lsmv_summary_SUBSET.smry_reportercomment_lang  ,lsmv_summary_SUBSET.smry_reportercomment  ,lsmv_summary_SUBSET.smry_report_summary_jpn  ,lsmv_summary_SUBSET.smry_remarks  ,lsmv_summary_SUBSET.smry_referencedate9fmt  ,lsmv_summary_SUBSET.smry_referencedate9  ,lsmv_summary_SUBSET.smry_referencedate8fmt  ,lsmv_summary_SUBSET.smry_referencedate8  ,lsmv_summary_SUBSET.smry_referencedate7fmt  ,lsmv_summary_SUBSET.smry_referencedate7  ,lsmv_summary_SUBSET.smry_referencedate6fmt  ,lsmv_summary_SUBSET.smry_referencedate6  ,lsmv_summary_SUBSET.smry_referencedate5fmt  ,lsmv_summary_SUBSET.smry_referencedate5  ,lsmv_summary_SUBSET.smry_referencedate4fmt  ,lsmv_summary_SUBSET.smry_referencedate40fmt  ,lsmv_summary_SUBSET.smry_referencedate40  ,lsmv_summary_SUBSET.smry_referencedate4  ,lsmv_summary_SUBSET.smry_referencedate3fmt  ,lsmv_summary_SUBSET.smry_referencedate39fmt  ,lsmv_summary_SUBSET.smry_referencedate39  ,lsmv_summary_SUBSET.smry_referencedate38fmt  ,lsmv_summary_SUBSET.smry_referencedate38  ,lsmv_summary_SUBSET.smry_referencedate37fmt  ,lsmv_summary_SUBSET.smry_referencedate37  ,lsmv_summary_SUBSET.smry_referencedate36fmt  ,lsmv_summary_SUBSET.smry_referencedate36  ,lsmv_summary_SUBSET.smry_referencedate35fmt  ,lsmv_summary_SUBSET.smry_referencedate35  ,lsmv_summary_SUBSET.smry_referencedate34fmt  ,lsmv_summary_SUBSET.smry_referencedate34  ,lsmv_summary_SUBSET.smry_referencedate33fmt  ,lsmv_summary_SUBSET.smry_referencedate33  ,lsmv_summary_SUBSET.smry_referencedate32fmt  ,lsmv_summary_SUBSET.smry_referencedate32  ,lsmv_summary_SUBSET.smry_referencedate31fmt  ,lsmv_summary_SUBSET.smry_referencedate31  ,lsmv_summary_SUBSET.smry_referencedate30fmt  ,lsmv_summary_SUBSET.smry_referencedate30  ,lsmv_summary_SUBSET.smry_referencedate3  ,lsmv_summary_SUBSET.smry_referencedate2fmt  ,lsmv_summary_SUBSET.smry_referencedate29fmt  ,lsmv_summary_SUBSET.smry_referencedate29  ,lsmv_summary_SUBSET.smry_referencedate28fmt  ,lsmv_summary_SUBSET.smry_referencedate28  ,lsmv_summary_SUBSET.smry_referencedate27fmt  ,lsmv_summary_SUBSET.smry_referencedate27  ,lsmv_summary_SUBSET.smry_referencedate26fmt  ,lsmv_summary_SUBSET.smry_referencedate26  ,lsmv_summary_SUBSET.smry_referencedate25fmt  ,lsmv_summary_SUBSET.smry_referencedate25  ,lsmv_summary_SUBSET.smry_referencedate24fmt  ,lsmv_summary_SUBSET.smry_referencedate24  ,lsmv_summary_SUBSET.smry_referencedate23fmt  ,lsmv_summary_SUBSET.smry_referencedate23  ,lsmv_summary_SUBSET.smry_referencedate22fmt  ,lsmv_summary_SUBSET.smry_referencedate22  ,lsmv_summary_SUBSET.smry_referencedate21fmt  ,lsmv_summary_SUBSET.smry_referencedate21  ,lsmv_summary_SUBSET.smry_referencedate20fmt  ,lsmv_summary_SUBSET.smry_referencedate20  ,lsmv_summary_SUBSET.smry_referencedate2  ,lsmv_summary_SUBSET.smry_referencedate1fmt  ,lsmv_summary_SUBSET.smry_referencedate19fmt  ,lsmv_summary_SUBSET.smry_referencedate19  ,lsmv_summary_SUBSET.smry_referencedate18fmt  ,lsmv_summary_SUBSET.smry_referencedate18  ,lsmv_summary_SUBSET.smry_referencedate17fmt  ,lsmv_summary_SUBSET.smry_referencedate17  ,lsmv_summary_SUBSET.smry_referencedate16fmt  ,lsmv_summary_SUBSET.smry_referencedate16  ,lsmv_summary_SUBSET.smry_referencedate15fmt  ,lsmv_summary_SUBSET.smry_referencedate15  ,lsmv_summary_SUBSET.smry_referencedate14fmt  ,lsmv_summary_SUBSET.smry_referencedate14  ,lsmv_summary_SUBSET.smry_referencedate13fmt  ,lsmv_summary_SUBSET.smry_referencedate13  ,lsmv_summary_SUBSET.smry_referencedate12fmt  ,lsmv_summary_SUBSET.smry_referencedate12  ,lsmv_summary_SUBSET.smry_referencedate11fmt  ,lsmv_summary_SUBSET.smry_referencedate11  ,lsmv_summary_SUBSET.smry_referencedate10fmt  ,lsmv_summary_SUBSET.smry_referencedate10  ,lsmv_summary_SUBSET.smry_referencedate1  ,lsmv_summary_SUBSET.smry_record_id  ,lsmv_summary_SUBSET.smry_rat_not_reporting  ,lsmv_summary_SUBSET.smry_product_complaint_details  ,lsmv_summary_SUBSET.smry_patient_during_event_desc  ,lsmv_summary_SUBSET.smry_other_remarks  ,lsmv_summary_SUBSET.smry_other_comments_jpn  ,lsmv_summary_SUBSET.smry_other_comments  ,lsmv_summary_SUBSET.smry_narrativeincludeclinical_lde  ,lsmv_summary_SUBSET.smry_narrativeincludeclinical_lang  ,lsmv_summary_SUBSET.smry_narrativeincludeclinical  ,lsmv_summary_SUBSET.smry_narrative_with_html  ,lsmv_summary_SUBSET.smry_narrative_map_pc  ,lsmv_summary_SUBSET.smry_narrative_map_cr  ,lsmv_summary_SUBSET.smry_narrative_map_ac  ,lsmv_summary_SUBSET.smry_narrative_map  ,lsmv_summary_SUBSET.smry_narrative_generated_pc  ,lsmv_summary_SUBSET.smry_narrative_generated_cr  ,lsmv_summary_SUBSET.smry_narrative_generated_ac  ,lsmv_summary_SUBSET.smry_narrative_generated  ,lsmv_summary_SUBSET.smry_manufacturer_narrative  ,lsmv_summary_SUBSET.smry_local_narratvive  ,lsmv_summary_SUBSET.smry_llt_decode  ,lsmv_summary_SUBSET.smry_llt_code  ,lsmv_summary_SUBSET.smry_labelled  ,lsmv_summary_SUBSET.smry_jpn_remarks4  ,lsmv_summary_SUBSET.smry_jpn_remarks3  ,lsmv_summary_SUBSET.smry_jpn_remarks2  ,lsmv_summary_SUBSET.smry_jpn_remarks1  ,lsmv_summary_SUBSET.smry_jpn_counter_measure  ,lsmv_summary_SUBSET.smry_inq_rec_id  ,lsmv_summary_SUBSET.smry_ini_manufactur_analysis  ,lsmv_summary_SUBSET.smry_identified_acton_narrative  ,lsmv_summary_SUBSET.smry_formatted_event_desc_text  ,lsmv_summary_SUBSET.smry_ext_clob_fld  ,lsmv_summary_SUBSET.smry_evaluation_summary  ,lsmv_summary_SUBSET.smry_evaluation_comments  ,lsmv_summary_SUBSET.smry_entity_updated  ,lsmv_summary_SUBSET.smry_e2b_r3_rep_comments_text  ,lsmv_summary_SUBSET.smry_do_not_use_ang_pc  ,lsmv_summary_SUBSET.smry_do_not_use_ang_de_ml  ,lsmv_summary_SUBSET.smry_do_not_use_ang_cr  ,lsmv_summary_SUBSET.smry_do_not_use_ang_ac  ,lsmv_summary_SUBSET.smry_do_not_use_ang  ,lsmv_summary_SUBSET.smry_date_modified  ,lsmv_summary_SUBSET.smry_date_created  ,lsmv_summary_SUBSET.smry_counter_medications  ,lsmv_summary_SUBSET.smry_corrective_prevn_narrative  ,lsmv_summary_SUBSET.smry_corrective_actions  ,lsmv_summary_SUBSET.smry_corrected_data  ,lsmv_summary_SUBSET.smry_company_narrative  ,lsmv_summary_SUBSET.smry_comp_rec_id  ,lsmv_summary_SUBSET.smry_ari_rec_id  ,lsmv_summary_SUBSET.smry_addtional_comments  ,lsmv_summary_SUBSET.smry_additional_information  ,lsmv_summary_SUBSET.smry_add_manufacturer_narrative  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_user_modified  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_user_created  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_spr_id  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_reporter_comments  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_record_id  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_inq_rec_id  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_fk_summary_rec_id  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_date_modified  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_date_created  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_comp_rec_id  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_comments_lang_de_ml  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_comments_lang  ,lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_ari_rec_id  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_user_modified  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_user_created  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_spr_id  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_record_id  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_fk_lsm_rec_id  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_date_modified  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_date_created  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_comments_lang  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_case_sum_and_rep_comments  ,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_ari_rec_id ,CONCAT( NVL(lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_RECORD_ID,-1),'||',NVL(lsmv_summary_SUBSET.smry_RECORD_ID,-1),'||',NVL(lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_summary_SUBSET  FULL OUTER JOIN lsmv_summary_rpt_comments_lang_SUBSET ON lsmv_summary_SUBSET.smry_ARI_REC_ID=lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_ARI_REC_ID
                         FULL OUTER JOIN lsmv_st_summary_rpt_comments_SUBSET ON lsmv_summary_SUBSET.smry_ARI_REC_ID=lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_ARI_REC_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_summary_rpt_comments_lang_SUBSET.smryrptcmtl_RECORD_ID,lsmv_summary_SUBSET.smry_RECORD_ID,lsmv_st_summary_rpt_comments_SUBSET.stsmryrpt_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP)
where target_table_name='LS_DB_NARRATIVE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE   
SET LS_DB_NARRATIVE.smry_version = LS_DB_NARRATIVE_TMP.smry_version,LS_DB_NARRATIVE.smry_user_modified = LS_DB_NARRATIVE_TMP.smry_user_modified,LS_DB_NARRATIVE.smry_user_created = LS_DB_NARRATIVE_TMP.smry_user_created,LS_DB_NARRATIVE.smry_txt_jpn_mhlw_rem_4 = LS_DB_NARRATIVE_TMP.smry_txt_jpn_mhlw_rem_4,LS_DB_NARRATIVE.smry_txt_jpn_mhlw_rem_3 = LS_DB_NARRATIVE_TMP.smry_txt_jpn_mhlw_rem_3,LS_DB_NARRATIVE.smry_txt_jpn_mhlw_rem_2 = LS_DB_NARRATIVE_TMP.smry_txt_jpn_mhlw_rem_2,LS_DB_NARRATIVE.smry_txt_jpn_mhlw_rem_1 = LS_DB_NARRATIVE_TMP.smry_txt_jpn_mhlw_rem_1,LS_DB_NARRATIVE.smry_treatment_following_event = LS_DB_NARRATIVE_TMP.smry_treatment_following_event,LS_DB_NARRATIVE.smry_tests_data = LS_DB_NARRATIVE_TMP.smry_tests_data,LS_DB_NARRATIVE.smry_summary_lang = LS_DB_NARRATIVE_TMP.smry_summary_lang,LS_DB_NARRATIVE.smry_summary_description = LS_DB_NARRATIVE_TMP.smry_summary_description,LS_DB_NARRATIVE.smry_spr_id = LS_DB_NARRATIVE_TMP.smry_spr_id,LS_DB_NARRATIVE.smry_senderdiagnosismeddraver_lang = LS_DB_NARRATIVE_TMP.smry_senderdiagnosismeddraver_lang,LS_DB_NARRATIVE.smry_senderdiagnosismeddraver = LS_DB_NARRATIVE_TMP.smry_senderdiagnosismeddraver,LS_DB_NARRATIVE.smry_senderdiagnosislevel = LS_DB_NARRATIVE_TMP.smry_senderdiagnosislevel,LS_DB_NARRATIVE.smry_senderdiagnosis_lang = LS_DB_NARRATIVE_TMP.smry_senderdiagnosis_lang,LS_DB_NARRATIVE.smry_senderdiagnosis_code = LS_DB_NARRATIVE_TMP.smry_senderdiagnosis_code,LS_DB_NARRATIVE.smry_senderdiagnosis = LS_DB_NARRATIVE_TMP.smry_senderdiagnosis,LS_DB_NARRATIVE.smry_sendercomment_lde = LS_DB_NARRATIVE_TMP.smry_sendercomment_lde,LS_DB_NARRATIVE.smry_sendercomment_lang = LS_DB_NARRATIVE_TMP.smry_sendercomment_lang,LS_DB_NARRATIVE.smry_sendercomment = LS_DB_NARRATIVE_TMP.smry_sendercomment,LS_DB_NARRATIVE.smry_reportercomment_lang = LS_DB_NARRATIVE_TMP.smry_reportercomment_lang,LS_DB_NARRATIVE.smry_reportercomment = LS_DB_NARRATIVE_TMP.smry_reportercomment,LS_DB_NARRATIVE.smry_report_summary_jpn = LS_DB_NARRATIVE_TMP.smry_report_summary_jpn,LS_DB_NARRATIVE.smry_remarks = LS_DB_NARRATIVE_TMP.smry_remarks,LS_DB_NARRATIVE.smry_referencedate9fmt = LS_DB_NARRATIVE_TMP.smry_referencedate9fmt,LS_DB_NARRATIVE.smry_referencedate9 = LS_DB_NARRATIVE_TMP.smry_referencedate9,LS_DB_NARRATIVE.smry_referencedate8fmt = LS_DB_NARRATIVE_TMP.smry_referencedate8fmt,LS_DB_NARRATIVE.smry_referencedate8 = LS_DB_NARRATIVE_TMP.smry_referencedate8,LS_DB_NARRATIVE.smry_referencedate7fmt = LS_DB_NARRATIVE_TMP.smry_referencedate7fmt,LS_DB_NARRATIVE.smry_referencedate7 = LS_DB_NARRATIVE_TMP.smry_referencedate7,LS_DB_NARRATIVE.smry_referencedate6fmt = LS_DB_NARRATIVE_TMP.smry_referencedate6fmt,LS_DB_NARRATIVE.smry_referencedate6 = LS_DB_NARRATIVE_TMP.smry_referencedate6,LS_DB_NARRATIVE.smry_referencedate5fmt = LS_DB_NARRATIVE_TMP.smry_referencedate5fmt,LS_DB_NARRATIVE.smry_referencedate5 = LS_DB_NARRATIVE_TMP.smry_referencedate5,LS_DB_NARRATIVE.smry_referencedate4fmt = LS_DB_NARRATIVE_TMP.smry_referencedate4fmt,LS_DB_NARRATIVE.smry_referencedate40fmt = LS_DB_NARRATIVE_TMP.smry_referencedate40fmt,LS_DB_NARRATIVE.smry_referencedate40 = LS_DB_NARRATIVE_TMP.smry_referencedate40,LS_DB_NARRATIVE.smry_referencedate4 = LS_DB_NARRATIVE_TMP.smry_referencedate4,LS_DB_NARRATIVE.smry_referencedate3fmt = LS_DB_NARRATIVE_TMP.smry_referencedate3fmt,LS_DB_NARRATIVE.smry_referencedate39fmt = LS_DB_NARRATIVE_TMP.smry_referencedate39fmt,LS_DB_NARRATIVE.smry_referencedate39 = LS_DB_NARRATIVE_TMP.smry_referencedate39,LS_DB_NARRATIVE.smry_referencedate38fmt = LS_DB_NARRATIVE_TMP.smry_referencedate38fmt,LS_DB_NARRATIVE.smry_referencedate38 = LS_DB_NARRATIVE_TMP.smry_referencedate38,LS_DB_NARRATIVE.smry_referencedate37fmt = LS_DB_NARRATIVE_TMP.smry_referencedate37fmt,LS_DB_NARRATIVE.smry_referencedate37 = LS_DB_NARRATIVE_TMP.smry_referencedate37,LS_DB_NARRATIVE.smry_referencedate36fmt = LS_DB_NARRATIVE_TMP.smry_referencedate36fmt,LS_DB_NARRATIVE.smry_referencedate36 = LS_DB_NARRATIVE_TMP.smry_referencedate36,LS_DB_NARRATIVE.smry_referencedate35fmt = LS_DB_NARRATIVE_TMP.smry_referencedate35fmt,LS_DB_NARRATIVE.smry_referencedate35 = LS_DB_NARRATIVE_TMP.smry_referencedate35,LS_DB_NARRATIVE.smry_referencedate34fmt = LS_DB_NARRATIVE_TMP.smry_referencedate34fmt,LS_DB_NARRATIVE.smry_referencedate34 = LS_DB_NARRATIVE_TMP.smry_referencedate34,LS_DB_NARRATIVE.smry_referencedate33fmt = LS_DB_NARRATIVE_TMP.smry_referencedate33fmt,LS_DB_NARRATIVE.smry_referencedate33 = LS_DB_NARRATIVE_TMP.smry_referencedate33,LS_DB_NARRATIVE.smry_referencedate32fmt = LS_DB_NARRATIVE_TMP.smry_referencedate32fmt,LS_DB_NARRATIVE.smry_referencedate32 = LS_DB_NARRATIVE_TMP.smry_referencedate32,LS_DB_NARRATIVE.smry_referencedate31fmt = LS_DB_NARRATIVE_TMP.smry_referencedate31fmt,LS_DB_NARRATIVE.smry_referencedate31 = LS_DB_NARRATIVE_TMP.smry_referencedate31,LS_DB_NARRATIVE.smry_referencedate30fmt = LS_DB_NARRATIVE_TMP.smry_referencedate30fmt,LS_DB_NARRATIVE.smry_referencedate30 = LS_DB_NARRATIVE_TMP.smry_referencedate30,LS_DB_NARRATIVE.smry_referencedate3 = LS_DB_NARRATIVE_TMP.smry_referencedate3,LS_DB_NARRATIVE.smry_referencedate2fmt = LS_DB_NARRATIVE_TMP.smry_referencedate2fmt,LS_DB_NARRATIVE.smry_referencedate29fmt = LS_DB_NARRATIVE_TMP.smry_referencedate29fmt,LS_DB_NARRATIVE.smry_referencedate29 = LS_DB_NARRATIVE_TMP.smry_referencedate29,LS_DB_NARRATIVE.smry_referencedate28fmt = LS_DB_NARRATIVE_TMP.smry_referencedate28fmt,LS_DB_NARRATIVE.smry_referencedate28 = LS_DB_NARRATIVE_TMP.smry_referencedate28,LS_DB_NARRATIVE.smry_referencedate27fmt = LS_DB_NARRATIVE_TMP.smry_referencedate27fmt,LS_DB_NARRATIVE.smry_referencedate27 = LS_DB_NARRATIVE_TMP.smry_referencedate27,LS_DB_NARRATIVE.smry_referencedate26fmt = LS_DB_NARRATIVE_TMP.smry_referencedate26fmt,LS_DB_NARRATIVE.smry_referencedate26 = LS_DB_NARRATIVE_TMP.smry_referencedate26,LS_DB_NARRATIVE.smry_referencedate25fmt = LS_DB_NARRATIVE_TMP.smry_referencedate25fmt,LS_DB_NARRATIVE.smry_referencedate25 = LS_DB_NARRATIVE_TMP.smry_referencedate25,LS_DB_NARRATIVE.smry_referencedate24fmt = LS_DB_NARRATIVE_TMP.smry_referencedate24fmt,LS_DB_NARRATIVE.smry_referencedate24 = LS_DB_NARRATIVE_TMP.smry_referencedate24,LS_DB_NARRATIVE.smry_referencedate23fmt = LS_DB_NARRATIVE_TMP.smry_referencedate23fmt,LS_DB_NARRATIVE.smry_referencedate23 = LS_DB_NARRATIVE_TMP.smry_referencedate23,LS_DB_NARRATIVE.smry_referencedate22fmt = LS_DB_NARRATIVE_TMP.smry_referencedate22fmt,LS_DB_NARRATIVE.smry_referencedate22 = LS_DB_NARRATIVE_TMP.smry_referencedate22,LS_DB_NARRATIVE.smry_referencedate21fmt = LS_DB_NARRATIVE_TMP.smry_referencedate21fmt,LS_DB_NARRATIVE.smry_referencedate21 = LS_DB_NARRATIVE_TMP.smry_referencedate21,LS_DB_NARRATIVE.smry_referencedate20fmt = LS_DB_NARRATIVE_TMP.smry_referencedate20fmt,LS_DB_NARRATIVE.smry_referencedate20 = LS_DB_NARRATIVE_TMP.smry_referencedate20,LS_DB_NARRATIVE.smry_referencedate2 = LS_DB_NARRATIVE_TMP.smry_referencedate2,LS_DB_NARRATIVE.smry_referencedate1fmt = LS_DB_NARRATIVE_TMP.smry_referencedate1fmt,LS_DB_NARRATIVE.smry_referencedate19fmt = LS_DB_NARRATIVE_TMP.smry_referencedate19fmt,LS_DB_NARRATIVE.smry_referencedate19 = LS_DB_NARRATIVE_TMP.smry_referencedate19,LS_DB_NARRATIVE.smry_referencedate18fmt = LS_DB_NARRATIVE_TMP.smry_referencedate18fmt,LS_DB_NARRATIVE.smry_referencedate18 = LS_DB_NARRATIVE_TMP.smry_referencedate18,LS_DB_NARRATIVE.smry_referencedate17fmt = LS_DB_NARRATIVE_TMP.smry_referencedate17fmt,LS_DB_NARRATIVE.smry_referencedate17 = LS_DB_NARRATIVE_TMP.smry_referencedate17,LS_DB_NARRATIVE.smry_referencedate16fmt = LS_DB_NARRATIVE_TMP.smry_referencedate16fmt,LS_DB_NARRATIVE.smry_referencedate16 = LS_DB_NARRATIVE_TMP.smry_referencedate16,LS_DB_NARRATIVE.smry_referencedate15fmt = LS_DB_NARRATIVE_TMP.smry_referencedate15fmt,LS_DB_NARRATIVE.smry_referencedate15 = LS_DB_NARRATIVE_TMP.smry_referencedate15,LS_DB_NARRATIVE.smry_referencedate14fmt = LS_DB_NARRATIVE_TMP.smry_referencedate14fmt,LS_DB_NARRATIVE.smry_referencedate14 = LS_DB_NARRATIVE_TMP.smry_referencedate14,LS_DB_NARRATIVE.smry_referencedate13fmt = LS_DB_NARRATIVE_TMP.smry_referencedate13fmt,LS_DB_NARRATIVE.smry_referencedate13 = LS_DB_NARRATIVE_TMP.smry_referencedate13,LS_DB_NARRATIVE.smry_referencedate12fmt = LS_DB_NARRATIVE_TMP.smry_referencedate12fmt,LS_DB_NARRATIVE.smry_referencedate12 = LS_DB_NARRATIVE_TMP.smry_referencedate12,LS_DB_NARRATIVE.smry_referencedate11fmt = LS_DB_NARRATIVE_TMP.smry_referencedate11fmt,LS_DB_NARRATIVE.smry_referencedate11 = LS_DB_NARRATIVE_TMP.smry_referencedate11,LS_DB_NARRATIVE.smry_referencedate10fmt = LS_DB_NARRATIVE_TMP.smry_referencedate10fmt,LS_DB_NARRATIVE.smry_referencedate10 = LS_DB_NARRATIVE_TMP.smry_referencedate10,LS_DB_NARRATIVE.smry_referencedate1 = LS_DB_NARRATIVE_TMP.smry_referencedate1,LS_DB_NARRATIVE.smry_record_id = LS_DB_NARRATIVE_TMP.smry_record_id,LS_DB_NARRATIVE.smry_rat_not_reporting = LS_DB_NARRATIVE_TMP.smry_rat_not_reporting,LS_DB_NARRATIVE.smry_product_complaint_details = LS_DB_NARRATIVE_TMP.smry_product_complaint_details,LS_DB_NARRATIVE.smry_patient_during_event_desc = LS_DB_NARRATIVE_TMP.smry_patient_during_event_desc,LS_DB_NARRATIVE.smry_other_remarks = LS_DB_NARRATIVE_TMP.smry_other_remarks,LS_DB_NARRATIVE.smry_other_comments_jpn = LS_DB_NARRATIVE_TMP.smry_other_comments_jpn,LS_DB_NARRATIVE.smry_other_comments = LS_DB_NARRATIVE_TMP.smry_other_comments,LS_DB_NARRATIVE.smry_narrativeincludeclinical_lde = LS_DB_NARRATIVE_TMP.smry_narrativeincludeclinical_lde,LS_DB_NARRATIVE.smry_narrativeincludeclinical_lang = LS_DB_NARRATIVE_TMP.smry_narrativeincludeclinical_lang,LS_DB_NARRATIVE.smry_narrativeincludeclinical = LS_DB_NARRATIVE_TMP.smry_narrativeincludeclinical,LS_DB_NARRATIVE.smry_narrative_with_html = LS_DB_NARRATIVE_TMP.smry_narrative_with_html,LS_DB_NARRATIVE.smry_narrative_map_pc = LS_DB_NARRATIVE_TMP.smry_narrative_map_pc,LS_DB_NARRATIVE.smry_narrative_map_cr = LS_DB_NARRATIVE_TMP.smry_narrative_map_cr,LS_DB_NARRATIVE.smry_narrative_map_ac = LS_DB_NARRATIVE_TMP.smry_narrative_map_ac,LS_DB_NARRATIVE.smry_narrative_map = LS_DB_NARRATIVE_TMP.smry_narrative_map,LS_DB_NARRATIVE.smry_narrative_generated_pc = LS_DB_NARRATIVE_TMP.smry_narrative_generated_pc,LS_DB_NARRATIVE.smry_narrative_generated_cr = LS_DB_NARRATIVE_TMP.smry_narrative_generated_cr,LS_DB_NARRATIVE.smry_narrative_generated_ac = LS_DB_NARRATIVE_TMP.smry_narrative_generated_ac,LS_DB_NARRATIVE.smry_narrative_generated = LS_DB_NARRATIVE_TMP.smry_narrative_generated,LS_DB_NARRATIVE.smry_manufacturer_narrative = LS_DB_NARRATIVE_TMP.smry_manufacturer_narrative,LS_DB_NARRATIVE.smry_local_narratvive = LS_DB_NARRATIVE_TMP.smry_local_narratvive,LS_DB_NARRATIVE.smry_llt_decode = LS_DB_NARRATIVE_TMP.smry_llt_decode,LS_DB_NARRATIVE.smry_llt_code = LS_DB_NARRATIVE_TMP.smry_llt_code,LS_DB_NARRATIVE.smry_labelled = LS_DB_NARRATIVE_TMP.smry_labelled,LS_DB_NARRATIVE.smry_jpn_remarks4 = LS_DB_NARRATIVE_TMP.smry_jpn_remarks4,LS_DB_NARRATIVE.smry_jpn_remarks3 = LS_DB_NARRATIVE_TMP.smry_jpn_remarks3,LS_DB_NARRATIVE.smry_jpn_remarks2 = LS_DB_NARRATIVE_TMP.smry_jpn_remarks2,LS_DB_NARRATIVE.smry_jpn_remarks1 = LS_DB_NARRATIVE_TMP.smry_jpn_remarks1,LS_DB_NARRATIVE.smry_jpn_counter_measure = LS_DB_NARRATIVE_TMP.smry_jpn_counter_measure,LS_DB_NARRATIVE.smry_inq_rec_id = LS_DB_NARRATIVE_TMP.smry_inq_rec_id,LS_DB_NARRATIVE.smry_ini_manufactur_analysis = LS_DB_NARRATIVE_TMP.smry_ini_manufactur_analysis,LS_DB_NARRATIVE.smry_identified_acton_narrative = LS_DB_NARRATIVE_TMP.smry_identified_acton_narrative,LS_DB_NARRATIVE.smry_formatted_event_desc_text = LS_DB_NARRATIVE_TMP.smry_formatted_event_desc_text,LS_DB_NARRATIVE.smry_ext_clob_fld = LS_DB_NARRATIVE_TMP.smry_ext_clob_fld,LS_DB_NARRATIVE.smry_evaluation_summary = LS_DB_NARRATIVE_TMP.smry_evaluation_summary,LS_DB_NARRATIVE.smry_evaluation_comments = LS_DB_NARRATIVE_TMP.smry_evaluation_comments,LS_DB_NARRATIVE.smry_entity_updated = LS_DB_NARRATIVE_TMP.smry_entity_updated,LS_DB_NARRATIVE.smry_e2b_r3_rep_comments_text = LS_DB_NARRATIVE_TMP.smry_e2b_r3_rep_comments_text,LS_DB_NARRATIVE.smry_do_not_use_ang_pc = LS_DB_NARRATIVE_TMP.smry_do_not_use_ang_pc,LS_DB_NARRATIVE.smry_do_not_use_ang_de_ml = LS_DB_NARRATIVE_TMP.smry_do_not_use_ang_de_ml,LS_DB_NARRATIVE.smry_do_not_use_ang_cr = LS_DB_NARRATIVE_TMP.smry_do_not_use_ang_cr,LS_DB_NARRATIVE.smry_do_not_use_ang_ac = LS_DB_NARRATIVE_TMP.smry_do_not_use_ang_ac,LS_DB_NARRATIVE.smry_do_not_use_ang = LS_DB_NARRATIVE_TMP.smry_do_not_use_ang,LS_DB_NARRATIVE.smry_date_modified = LS_DB_NARRATIVE_TMP.smry_date_modified,LS_DB_NARRATIVE.smry_date_created = LS_DB_NARRATIVE_TMP.smry_date_created,LS_DB_NARRATIVE.smry_counter_medications = LS_DB_NARRATIVE_TMP.smry_counter_medications,LS_DB_NARRATIVE.smry_corrective_prevn_narrative = LS_DB_NARRATIVE_TMP.smry_corrective_prevn_narrative,LS_DB_NARRATIVE.smry_corrective_actions = LS_DB_NARRATIVE_TMP.smry_corrective_actions,LS_DB_NARRATIVE.smry_corrected_data = LS_DB_NARRATIVE_TMP.smry_corrected_data,LS_DB_NARRATIVE.smry_company_narrative = LS_DB_NARRATIVE_TMP.smry_company_narrative,LS_DB_NARRATIVE.smry_comp_rec_id = LS_DB_NARRATIVE_TMP.smry_comp_rec_id,LS_DB_NARRATIVE.smry_ari_rec_id = LS_DB_NARRATIVE_TMP.smry_ari_rec_id,LS_DB_NARRATIVE.smry_addtional_comments = LS_DB_NARRATIVE_TMP.smry_addtional_comments,LS_DB_NARRATIVE.smry_additional_information = LS_DB_NARRATIVE_TMP.smry_additional_information,LS_DB_NARRATIVE.smry_add_manufacturer_narrative = LS_DB_NARRATIVE_TMP.smry_add_manufacturer_narrative,LS_DB_NARRATIVE.smryrptcmtl_user_modified = LS_DB_NARRATIVE_TMP.smryrptcmtl_user_modified,LS_DB_NARRATIVE.smryrptcmtl_user_created = LS_DB_NARRATIVE_TMP.smryrptcmtl_user_created,LS_DB_NARRATIVE.smryrptcmtl_spr_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_spr_id,LS_DB_NARRATIVE.smryrptcmtl_reporter_comments = LS_DB_NARRATIVE_TMP.smryrptcmtl_reporter_comments,LS_DB_NARRATIVE.smryrptcmtl_record_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_record_id,LS_DB_NARRATIVE.smryrptcmtl_inq_rec_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_inq_rec_id,LS_DB_NARRATIVE.smryrptcmtl_fk_summary_rec_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_fk_summary_rec_id,LS_DB_NARRATIVE.smryrptcmtl_date_modified = LS_DB_NARRATIVE_TMP.smryrptcmtl_date_modified,LS_DB_NARRATIVE.smryrptcmtl_date_created = LS_DB_NARRATIVE_TMP.smryrptcmtl_date_created,LS_DB_NARRATIVE.smryrptcmtl_comp_rec_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_comp_rec_id,LS_DB_NARRATIVE.smryrptcmtl_comments_lang_de_ml = LS_DB_NARRATIVE_TMP.smryrptcmtl_comments_lang_de_ml,LS_DB_NARRATIVE.smryrptcmtl_comments_lang = LS_DB_NARRATIVE_TMP.smryrptcmtl_comments_lang,LS_DB_NARRATIVE.smryrptcmtl_ari_rec_id = LS_DB_NARRATIVE_TMP.smryrptcmtl_ari_rec_id,LS_DB_NARRATIVE.stsmryrpt_user_modified = LS_DB_NARRATIVE_TMP.stsmryrpt_user_modified,LS_DB_NARRATIVE.stsmryrpt_user_created = LS_DB_NARRATIVE_TMP.stsmryrpt_user_created,LS_DB_NARRATIVE.stsmryrpt_spr_id = LS_DB_NARRATIVE_TMP.stsmryrpt_spr_id,LS_DB_NARRATIVE.stsmryrpt_record_id = LS_DB_NARRATIVE_TMP.stsmryrpt_record_id,LS_DB_NARRATIVE.stsmryrpt_fk_lsm_rec_id = LS_DB_NARRATIVE_TMP.stsmryrpt_fk_lsm_rec_id,LS_DB_NARRATIVE.stsmryrpt_date_modified = LS_DB_NARRATIVE_TMP.stsmryrpt_date_modified,LS_DB_NARRATIVE.stsmryrpt_date_created = LS_DB_NARRATIVE_TMP.stsmryrpt_date_created,LS_DB_NARRATIVE.stsmryrpt_comments_lang = LS_DB_NARRATIVE_TMP.stsmryrpt_comments_lang,LS_DB_NARRATIVE.stsmryrpt_case_sum_and_rep_comments = LS_DB_NARRATIVE_TMP.stsmryrpt_case_sum_and_rep_comments,LS_DB_NARRATIVE.stsmryrpt_ari_rec_id = LS_DB_NARRATIVE_TMP.stsmryrpt_ari_rec_id,
LS_DB_NARRATIVE.PROCESSING_DT = LS_DB_NARRATIVE_TMP.PROCESSING_DT,
LS_DB_NARRATIVE.receipt_id     =LS_DB_NARRATIVE_TMP.receipt_id    ,
LS_DB_NARRATIVE.case_no        =LS_DB_NARRATIVE_TMP.case_no           ,
LS_DB_NARRATIVE.case_version   =LS_DB_NARRATIVE_TMP.case_version      ,
LS_DB_NARRATIVE.version_no     =LS_DB_NARRATIVE_TMP.version_no        ,
LS_DB_NARRATIVE.user_modified  =LS_DB_NARRATIVE_TMP.user_modified     ,
LS_DB_NARRATIVE.date_modified  =LS_DB_NARRATIVE_TMP.date_modified     ,
LS_DB_NARRATIVE.expiry_date    =LS_DB_NARRATIVE_TMP.expiry_date       ,
LS_DB_NARRATIVE.created_by     =LS_DB_NARRATIVE_TMP.created_by        ,
LS_DB_NARRATIVE.created_dt     =LS_DB_NARRATIVE_TMP.created_dt        ,
LS_DB_NARRATIVE.load_ts        =LS_DB_NARRATIVE_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP 
WHERE 	LS_DB_NARRATIVE.INTEGRATION_ID = LS_DB_NARRATIVE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_NARRATIVE_TMP.PROCESSING_DT = LS_DB_NARRATIVE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_NARRATIVE.smryrptcmtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_NARRATIVE.smry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_NARRATIVE.stsmryrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_NARRATIVE_TMP.smryrptcmtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_NARRATIVE_TMP.smry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_NARRATIVE_TMP.stsmryrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_NARRATIVE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_NARRATIVE.smry_RECORD_ID ,LS_DB_NARRATIVE.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP 
ON LS_DB_NARRATIVE.smry_RECORD_ID=LS_DB_NARRATIVE_TMP.smry_RECORD_ID
AND LS_DB_NARRATIVE.INTEGRATION_ID = LS_DB_NARRATIVE_TMP.INTEGRATION_ID 
where LS_DB_NARRATIVE_TMP.INTEGRATION_ID  is null AND LS_DB_NARRATIVE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_NARRATIVE.smry_RECORD_ID in (select smry_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP )
) TMP where LS_DB_NARRATIVE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_NARRATIVE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_NARRATIVE.smry_RECORD_ID ,LS_DB_NARRATIVE.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP 
    ON LS_DB_NARRATIVE.smry_RECORD_ID=LS_DB_NARRATIVE_TMP.smry_RECORD_ID
    AND LS_DB_NARRATIVE.INTEGRATION_ID = LS_DB_NARRATIVE_TMP.INTEGRATION_ID 
    where LS_DB_NARRATIVE_TMP.INTEGRATION_ID  is null AND LS_DB_NARRATIVE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_NARRATIVE.smry_RECORD_ID in (select smry_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE
( receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,smry_version,
smry_user_modified,
smry_user_created,
smry_txt_jpn_mhlw_rem_4,
smry_txt_jpn_mhlw_rem_3,
smry_txt_jpn_mhlw_rem_2,
smry_txt_jpn_mhlw_rem_1,
smry_treatment_following_event,
smry_tests_data,
smry_summary_lang,
smry_summary_description,
smry_spr_id,
smry_senderdiagnosismeddraver_lang,
smry_senderdiagnosismeddraver,
smry_senderdiagnosislevel,
smry_senderdiagnosis_lang,
smry_senderdiagnosis_code,
smry_senderdiagnosis,
smry_sendercomment_lde,
smry_sendercomment_lang,
smry_sendercomment,
smry_reportercomment_lang,
smry_reportercomment,
smry_report_summary_jpn,
smry_remarks,
smry_referencedate9fmt,
smry_referencedate9,
smry_referencedate8fmt,
smry_referencedate8,
smry_referencedate7fmt,
smry_referencedate7,
smry_referencedate6fmt,
smry_referencedate6,
smry_referencedate5fmt,
smry_referencedate5,
smry_referencedate4fmt,
smry_referencedate40fmt,
smry_referencedate40,
smry_referencedate4,
smry_referencedate3fmt,
smry_referencedate39fmt,
smry_referencedate39,
smry_referencedate38fmt,
smry_referencedate38,
smry_referencedate37fmt,
smry_referencedate37,
smry_referencedate36fmt,
smry_referencedate36,
smry_referencedate35fmt,
smry_referencedate35,
smry_referencedate34fmt,
smry_referencedate34,
smry_referencedate33fmt,
smry_referencedate33,
smry_referencedate32fmt,
smry_referencedate32,
smry_referencedate31fmt,
smry_referencedate31,
smry_referencedate30fmt,
smry_referencedate30,
smry_referencedate3,
smry_referencedate2fmt,
smry_referencedate29fmt,
smry_referencedate29,
smry_referencedate28fmt,
smry_referencedate28,
smry_referencedate27fmt,
smry_referencedate27,
smry_referencedate26fmt,
smry_referencedate26,
smry_referencedate25fmt,
smry_referencedate25,
smry_referencedate24fmt,
smry_referencedate24,
smry_referencedate23fmt,
smry_referencedate23,
smry_referencedate22fmt,
smry_referencedate22,
smry_referencedate21fmt,
smry_referencedate21,
smry_referencedate20fmt,
smry_referencedate20,
smry_referencedate2,
smry_referencedate1fmt,
smry_referencedate19fmt,
smry_referencedate19,
smry_referencedate18fmt,
smry_referencedate18,
smry_referencedate17fmt,
smry_referencedate17,
smry_referencedate16fmt,
smry_referencedate16,
smry_referencedate15fmt,
smry_referencedate15,
smry_referencedate14fmt,
smry_referencedate14,
smry_referencedate13fmt,
smry_referencedate13,
smry_referencedate12fmt,
smry_referencedate12,
smry_referencedate11fmt,
smry_referencedate11,
smry_referencedate10fmt,
smry_referencedate10,
smry_referencedate1,
smry_record_id,
smry_rat_not_reporting,
smry_product_complaint_details,
smry_patient_during_event_desc,
smry_other_remarks,
smry_other_comments_jpn,
smry_other_comments,
smry_narrativeincludeclinical_lde,
smry_narrativeincludeclinical_lang,
smry_narrativeincludeclinical,
smry_narrative_with_html,
smry_narrative_map_pc,
smry_narrative_map_cr,
smry_narrative_map_ac,
smry_narrative_map,
smry_narrative_generated_pc,
smry_narrative_generated_cr,
smry_narrative_generated_ac,
smry_narrative_generated,
smry_manufacturer_narrative,
smry_local_narratvive,
smry_llt_decode,
smry_llt_code,
smry_labelled,
smry_jpn_remarks4,
smry_jpn_remarks3,
smry_jpn_remarks2,
smry_jpn_remarks1,
smry_jpn_counter_measure,
smry_inq_rec_id,
smry_ini_manufactur_analysis,
smry_identified_acton_narrative,
smry_formatted_event_desc_text,
smry_ext_clob_fld,
smry_evaluation_summary,
smry_evaluation_comments,
smry_entity_updated,
smry_e2b_r3_rep_comments_text,
smry_do_not_use_ang_pc,
smry_do_not_use_ang_de_ml,
smry_do_not_use_ang_cr,
smry_do_not_use_ang_ac,
smry_do_not_use_ang,
smry_date_modified,
smry_date_created,
smry_counter_medications,
smry_corrective_prevn_narrative,
smry_corrective_actions,
smry_corrected_data,
smry_company_narrative,
smry_comp_rec_id,
smry_ari_rec_id,
smry_addtional_comments,
smry_additional_information,
smry_add_manufacturer_narrative,
smryrptcmtl_user_modified,
smryrptcmtl_user_created,
smryrptcmtl_spr_id,
smryrptcmtl_reporter_comments,
smryrptcmtl_record_id,
smryrptcmtl_inq_rec_id,
smryrptcmtl_fk_summary_rec_id,
smryrptcmtl_date_modified,
smryrptcmtl_date_created,
smryrptcmtl_comp_rec_id,
smryrptcmtl_comments_lang_de_ml,
smryrptcmtl_comments_lang,
smryrptcmtl_ari_rec_id,
stsmryrpt_user_modified,
stsmryrpt_user_created,
stsmryrpt_spr_id,
stsmryrpt_record_id,
stsmryrpt_fk_lsm_rec_id,
stsmryrpt_date_modified,
stsmryrpt_date_created,
stsmryrpt_comments_lang,
stsmryrpt_case_sum_and_rep_comments,
stsmryrpt_ari_rec_id)
SELECT 
 receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,smry_version,
smry_user_modified,
smry_user_created,
smry_txt_jpn_mhlw_rem_4,
smry_txt_jpn_mhlw_rem_3,
smry_txt_jpn_mhlw_rem_2,
smry_txt_jpn_mhlw_rem_1,
smry_treatment_following_event,
smry_tests_data,
smry_summary_lang,
smry_summary_description,
smry_spr_id,
smry_senderdiagnosismeddraver_lang,
smry_senderdiagnosismeddraver,
smry_senderdiagnosislevel,
smry_senderdiagnosis_lang,
smry_senderdiagnosis_code,
smry_senderdiagnosis,
smry_sendercomment_lde,
smry_sendercomment_lang,
smry_sendercomment,
smry_reportercomment_lang,
smry_reportercomment,
smry_report_summary_jpn,
smry_remarks,
smry_referencedate9fmt,
smry_referencedate9,
smry_referencedate8fmt,
smry_referencedate8,
smry_referencedate7fmt,
smry_referencedate7,
smry_referencedate6fmt,
smry_referencedate6,
smry_referencedate5fmt,
smry_referencedate5,
smry_referencedate4fmt,
smry_referencedate40fmt,
smry_referencedate40,
smry_referencedate4,
smry_referencedate3fmt,
smry_referencedate39fmt,
smry_referencedate39,
smry_referencedate38fmt,
smry_referencedate38,
smry_referencedate37fmt,
smry_referencedate37,
smry_referencedate36fmt,
smry_referencedate36,
smry_referencedate35fmt,
smry_referencedate35,
smry_referencedate34fmt,
smry_referencedate34,
smry_referencedate33fmt,
smry_referencedate33,
smry_referencedate32fmt,
smry_referencedate32,
smry_referencedate31fmt,
smry_referencedate31,
smry_referencedate30fmt,
smry_referencedate30,
smry_referencedate3,
smry_referencedate2fmt,
smry_referencedate29fmt,
smry_referencedate29,
smry_referencedate28fmt,
smry_referencedate28,
smry_referencedate27fmt,
smry_referencedate27,
smry_referencedate26fmt,
smry_referencedate26,
smry_referencedate25fmt,
smry_referencedate25,
smry_referencedate24fmt,
smry_referencedate24,
smry_referencedate23fmt,
smry_referencedate23,
smry_referencedate22fmt,
smry_referencedate22,
smry_referencedate21fmt,
smry_referencedate21,
smry_referencedate20fmt,
smry_referencedate20,
smry_referencedate2,
smry_referencedate1fmt,
smry_referencedate19fmt,
smry_referencedate19,
smry_referencedate18fmt,
smry_referencedate18,
smry_referencedate17fmt,
smry_referencedate17,
smry_referencedate16fmt,
smry_referencedate16,
smry_referencedate15fmt,
smry_referencedate15,
smry_referencedate14fmt,
smry_referencedate14,
smry_referencedate13fmt,
smry_referencedate13,
smry_referencedate12fmt,
smry_referencedate12,
smry_referencedate11fmt,
smry_referencedate11,
smry_referencedate10fmt,
smry_referencedate10,
smry_referencedate1,
smry_record_id,
smry_rat_not_reporting,
smry_product_complaint_details,
smry_patient_during_event_desc,
smry_other_remarks,
smry_other_comments_jpn,
smry_other_comments,
smry_narrativeincludeclinical_lde,
smry_narrativeincludeclinical_lang,
smry_narrativeincludeclinical,
smry_narrative_with_html,
smry_narrative_map_pc,
smry_narrative_map_cr,
smry_narrative_map_ac,
smry_narrative_map,
smry_narrative_generated_pc,
smry_narrative_generated_cr,
smry_narrative_generated_ac,
smry_narrative_generated,
smry_manufacturer_narrative,
smry_local_narratvive,
smry_llt_decode,
smry_llt_code,
smry_labelled,
smry_jpn_remarks4,
smry_jpn_remarks3,
smry_jpn_remarks2,
smry_jpn_remarks1,
smry_jpn_counter_measure,
smry_inq_rec_id,
smry_ini_manufactur_analysis,
smry_identified_acton_narrative,
smry_formatted_event_desc_text,
smry_ext_clob_fld,
smry_evaluation_summary,
smry_evaluation_comments,
smry_entity_updated,
smry_e2b_r3_rep_comments_text,
smry_do_not_use_ang_pc,
smry_do_not_use_ang_de_ml,
smry_do_not_use_ang_cr,
smry_do_not_use_ang_ac,
smry_do_not_use_ang,
smry_date_modified,
smry_date_created,
smry_counter_medications,
smry_corrective_prevn_narrative,
smry_corrective_actions,
smry_corrected_data,
smry_company_narrative,
smry_comp_rec_id,
smry_ari_rec_id,
smry_addtional_comments,
smry_additional_information,
smry_add_manufacturer_narrative,
smryrptcmtl_user_modified,
smryrptcmtl_user_created,
smryrptcmtl_spr_id,
smryrptcmtl_reporter_comments,
smryrptcmtl_record_id,
smryrptcmtl_inq_rec_id,
smryrptcmtl_fk_summary_rec_id,
smryrptcmtl_date_modified,
smryrptcmtl_date_created,
smryrptcmtl_comp_rec_id,
smryrptcmtl_comments_lang_de_ml,
smryrptcmtl_comments_lang,
smryrptcmtl_ari_rec_id,
stsmryrpt_user_modified,
stsmryrpt_user_created,
stsmryrpt_spr_id,
stsmryrpt_record_id,
stsmryrpt_fk_lsm_rec_id,
stsmryrpt_date_modified,
stsmryrpt_date_created,
stsmryrpt_comments_lang,
stsmryrpt_case_sum_and_rep_comments,
stsmryrpt_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP 
WHERE 	TO_DATE(LS_DB_NARRATIVE.PROCESSING_DT) < TO_DATE(LS_DB_NARRATIVE_TMP.PROCESSING_DT)
AND LS_DB_NARRATIVE.INTEGRATION_ID = LS_DB_NARRATIVE_TMP.INTEGRATION_ID
AND LS_DB_NARRATIVE.smry_RECORD_ID = LS_DB_NARRATIVE_TMP.smry_RECORD_ID
AND LS_DB_NARRATIVE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_NARRATIVE.smryrptcmtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_NARRATIVE.smry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_NARRATIVE.stsmryrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_NARRATIVE_TMP.smryrptcmtl_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_NARRATIVE_TMP.smry_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_NARRATIVE_TMP.stsmryrpt_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE TGT
WHERE  ( stsmryrpt_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_summary_rpt_comments') OR smry_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary') OR smryrptcmtl_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary_rpt_comments_lang')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( stsmryrpt_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_summary_rpt_comments') OR smry_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary') OR smryrptcmtl_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_NARRATIVE_DELETION_TMP  WHERE TABLE_NAME='lsmv_summary_rpt_comments_lang')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_NARRATIVE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_NARRATIVE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_NARRATIVE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_NARRATIVE'),
	'LSDB','Case','LS_DB_NARRATIVE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_NARRATIVE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
