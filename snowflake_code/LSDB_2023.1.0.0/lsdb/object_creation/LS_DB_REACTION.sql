
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_REACTION()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION'),
	'LSRA','Case','LS_DB_REACTION',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_REACTION' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_REACTION'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REACTION_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REACTION_DELETION_TMP  As select RECORD_ID,'lsmv_reaction' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reaction WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10002','10004','10005','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','1002','10045','1011','1012','1012','10129','10130','10131','1015','1017','1017','1017','158','4','4','4','7077','7089','8106','8137','9065','9745','9864','9941','9941','9941','9941','9941','9941','9941','9941','9970')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reaction WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REACTION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REACTION' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_reaction WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REACTION' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REACTION' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_reaction_SUBSET AS 
(
select * from 
    (SELECT  
    ae_additional_information  ae_additional_information,always_down_grade_reason  always_down_grade_reason,anticipated_events  anticipated_events,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(anticipated_events AS VARCHAR(100)) )anticipated_events_de_ml , anticipated_events_manual  anticipated_events_manual,are_react_end_date_string  are_react_end_date_string,are_react_start_date_string  are_react_start_date_string,ari_rec_id  ari_rec_id,ase_seriousness_check  ase_seriousness_check,basic_id_desc  basic_id_desc,basic_identification  basic_identification,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10004' AND CD=CAST(basic_identification AS VARCHAR(100)) )basic_identification_de_ml , caused_by_lo_defect  caused_by_lo_defect,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(caused_by_lo_defect AS VARCHAR(100)) )caused_by_lo_defect_de_ml , coded_reactionterm  coded_reactionterm,coding_comments  coding_comments,coding_type  coding_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='158' AND CD=CAST(coding_type AS VARCHAR(100)) )coding_type_de_ml , comments  comments,comments_similar_incident  comments_similar_incident,comp_rec_id  comp_rec_id,condition  condition,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10129' AND CD=CAST(condition AS VARCHAR(100)) )condition_de_ml , condition_sf  condition_sf,congenitalanomaly  congenitalanomaly,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(congenitalanomaly AS VARCHAR(100)) )congenitalanomaly_de_ml , congenitalanomaly_nf  congenitalanomaly_nf,country_detection_manual  country_detection_manual,cr_ref_id  cr_ref_id,date_created  date_created,date_modified  date_modified,death  death,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(death AS VARCHAR(100)) )death_de_ml , death_nf  death_nf,detected_country  detected_country,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(detected_country AS VARCHAR(100)) )detected_country_de_ml , device_criteria  device_criteria,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10005' AND CD=CAST(device_criteria AS VARCHAR(100)) )device_criteria_de_ml , device_criteria_desc  device_criteria_desc,disability  disability,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(disability AS VARCHAR(100)) )disability_de_ml , disability_nf  disability_nf,discharge_date_fmt  discharge_date_fmt,down_grade_reason  down_grade_reason,drug_interaction  drug_interaction,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(drug_interaction AS VARCHAR(100)) )drug_interaction_de_ml , e2b_r3_primary_source_transln  e2b_r3_primary_source_transln,entity_updated  entity_updated,eudamed_ref_number  eudamed_ref_number,event_coded_flag  event_coded_flag,event_location  event_location,event_occur_location  event_occur_location,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9864' AND CD=CAST(event_occur_location AS VARCHAR(100)) )event_occur_location_de_ml , event_occure_during_incident  event_occure_during_incident,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10131' AND CD=CAST(event_occure_during_incident AS VARCHAR(100)) )event_occure_during_incident_de_ml , event_received_date  event_received_date,event_received_date_jpn  event_received_date_jpn,event_received_date_jpn_manual  event_received_date_jpn_manual,event_type  event_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9745' AND CD=CAST(event_type AS VARCHAR(100)) )event_type_de_ml , exempted_events  exempted_events,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(exempted_events AS VARCHAR(100)) )exempted_events_de_ml , exempted_events_manual  exempted_events_manual,explanation_form_missing_code_e  explanation_form_missing_code_e,explanation_form_missing_code_f  explanation_form_missing_code_f,ext_clob_fld  ext_clob_fld,failed_reason_or_other_info  failed_reason_or_other_info,fk_apat_rec_id  fk_apat_rec_id,fk_vac_rec_id  fk_vac_rec_id,hcf_name  hcf_name,hcf_number  hcf_number,hosp_prolonged  hosp_prolonged,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(hosp_prolonged AS VARCHAR(100)) )hosp_prolonged_de_ml , hosp_prolonged_nf  hosp_prolonged_nf,hospital_discharge_date  hospital_discharge_date,hospitalisation_date  hospitalisation_date,hospitalization  hospitalization,hospitalization_days  hospitalization_days,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(hospitalization AS VARCHAR(100)) )hospitalization_de_ml , hospitalization_nf  hospitalization_nf,hospitalize_date_fmt  hospitalize_date_fmt,imrdf_codes  imrdf_codes,imrdf_desc  imrdf_desc,imrdf_similar_codes  imrdf_similar_codes,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10002' AND CD=CAST(imrdf_similar_codes AS VARCHAR(100)) )imrdf_similar_codes_de_ml , imrdf_similar_desc  imrdf_similar_desc,imrdf_type  imrdf_type,in_history  in_history,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8137' AND CD=CAST(in_history AS VARCHAR(100)) )in_history_de_ml , incident_occurred_during  incident_occurred_during,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10045' AND CD=CAST(incident_occurred_during AS VARCHAR(100)) )incident_occurred_during_de_ml , inq_rec_id  inq_rec_id,interventionrequired  interventionrequired,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(interventionrequired AS VARCHAR(100)) )interventionrequired_de_ml , interventionrequired_nf  interventionrequired_nf,investigation_finding  investigation_finding,is_added_from_krjp  is_added_from_krjp,is_desig_medical_event  is_desig_medical_event,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(is_desig_medical_event AS VARCHAR(100)) )is_desig_medical_event_de_ml , is_dme  is_dme,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_dme AS VARCHAR(100)) )is_dme_de_ml , is_event_aesi  is_event_aesi,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_event_aesi AS VARCHAR(100)) )is_event_aesi_de_ml , is_event_manual  is_event_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_event_manual AS VARCHAR(100)) )is_event_manual_de_ml , is_event_related_to_pqc  is_event_related_to_pqc,is_ime  is_ime,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_ime AS VARCHAR(100)) )is_ime_de_ml , is_ime_manual  is_ime_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_ime_manual AS VARCHAR(100)) )is_ime_manual_de_ml , is_non_ae  is_non_ae,is_related_to_pc  is_related_to_pc,is_serious_event  is_serious_event,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(is_serious_event AS VARCHAR(100)) )is_serious_event_de_ml , lifethreatening  lifethreatening,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(lifethreatening AS VARCHAR(100)) )lifethreatening_de_ml , lifethreatening_nf  lifethreatening_nf,location  location,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10130' AND CD=CAST(location AS VARCHAR(100)) )location_de_ml , location_sf  location_sf,lot_number_related_to_pc  lot_number_related_to_pc,malfunction  malfunction,manual_duration  manual_duration,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(manual_duration AS VARCHAR(100)) )manual_duration_de_ml , measures_theadverse_event  measures_theadverse_event,measures_thisdrug  measures_thisdrug,medical_device_problem  medical_device_problem,medically_confirm  medically_confirm,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(medically_confirm AS VARCHAR(100)) )medically_confirm_de_ml , medically_confirm_manual  medically_confirm_manual,medically_confirm_nf  medically_confirm_nf,medicallysignificant  medicallysignificant,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(medicallysignificant AS VARCHAR(100)) )medicallysignificant_de_ml , medicallysignificant_nf  medicallysignificant_nf,municipality  municipality,near_incident  near_incident,near_incident_related  near_incident_related,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(near_incident_related AS VARCHAR(100)) )near_incident_related_de_ml , nonserious  nonserious,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(nonserious AS VARCHAR(100)) )nonserious_de_ml , nonserious_info  nonserious_info,nonserious_nf  nonserious_nf,not_reportable_jpn  not_reportable_jpn,other  other,other_code_desc  other_code_desc,other_code_flag  other_code_flag,other_event_occur_location  other_event_occur_location,other_medically_text  other_medically_text,outcome_date  outcome_date,outcome_details  outcome_details,pmreaction_description  pmreaction_description,pqc_unique_id_number  pqc_unique_id_number,primary_soc_code  primary_soc_code,primarysrcreaction  primarysrcreaction,primarysrcreaction_lang  primarysrcreaction_lang,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9065' AND CD=CAST(primarysrcreaction_lang AS VARCHAR(100)) )primarysrcreaction_lang_de_ml , problem_code  problem_code,problem_term  problem_term,rank_order  rank_order,react_end_date_tz  react_end_date_tz,react_end_manual_check  react_end_manual_check,react_manual_check  react_manual_check,react_start_date_tz  react_start_date_tz,reactduration  reactduration,reactduration_lang  reactduration_lang,reactdurationunit  reactdurationunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(reactdurationunit AS VARCHAR(100)) )reactdurationunit_de_ml , reactenddate  reactenddate,reactenddate_nf  reactenddate_nf,reactenddatefmt  reactenddatefmt,reactfirsttime  reactfirsttime,reactfirsttime_lang  reactfirsttime_lang,reactfirsttimeunit  reactfirsttimeunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(reactfirsttimeunit AS VARCHAR(100)) )reactfirsttimeunit_de_ml , reaction_description  reaction_description,reaction_json_text  reaction_json_text,reaction_lang  reaction_lang,reaction_sequence_id  reaction_sequence_id,reaction_site  reaction_site,reaction_type  reaction_type,reactionsitetext  reactionsitetext,reactionterm  reactionterm,reactionterm_jpn  reactionterm_jpn,reactionterm_lang  reactionterm_lang,reactionterm_translation  reactionterm_translation,reactlasttime  reactlasttime,reactlasttime_lang  reactlasttime_lang,reactlasttimeunit  reactlasttimeunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1017' AND CD=CAST(reactlasttimeunit AS VARCHAR(100)) )reactlasttimeunit_de_ml , reactmeddracodellt  reactmeddracodellt,reactmeddracodept  reactmeddracodept,reactmeddrallt  reactmeddrallt,reactmeddrallt_code  reactmeddrallt_code,reactmeddrallt_lang  reactmeddrallt_lang,reactmeddralltlevel  reactmeddralltlevel,reactmeddrapt  reactmeddrapt,reactmeddrapt_code  reactmeddrapt_code,reactmeddrapt_lang  reactmeddrapt_lang,reactmeddraptlevel  reactmeddraptlevel,reactmeddraverllt  reactmeddraverllt,reactmeddraverllt_lang  reactmeddraverllt_lang,reactmeddraverpt  reactmeddraverpt,reactmeddraverpt_lang  reactmeddraverpt_lang,reactothers  reactothers,reactoutcome  reactoutcome,reactoutcome_company  reactoutcome_company,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1012' AND CD=CAST(reactoutcome_company AS VARCHAR(100)) )reactoutcome_company_de_ml , (SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1012' AND CD=CAST(reactoutcome AS VARCHAR(100)) )reactoutcome_de_ml , reactstartdate  reactstartdate,reactstartdate_nf  reactstartdate_nf,reactstartdatefmt  reactstartdatefmt,record_id  record_id,related_drug_name  related_drug_name,related_product  related_product,repo_event_id  repo_event_id,repo_reaction_id  repo_reaction_id,root_cause_code  root_cause_code,root_cause_term  root_cause_term,ser_public_health_threat  ser_public_health_threat,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(ser_public_health_threat AS VARCHAR(100)) )ser_public_health_threat_de_ml , serious_injury  serious_injury,serious_injury_text  serious_injury_text,seriousness  seriousness,seriousness_company  seriousness_company,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(seriousness_company AS VARCHAR(100)) )seriousness_company_de_ml , (SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(seriousness AS VARCHAR(100)) )seriousness_de_ml , severity  severity,severity_company  severity_company,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8106' AND CD=CAST(severity AS VARCHAR(100)) )severity_de_ml , snomed  snomed,snomed_coded_flag  snomed_coded_flag,snomed_info  snomed_info,spr_id  spr_id,term_added_by  term_added_by,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9970' AND CD=CAST(term_added_by AS VARCHAR(100)) )term_added_by_de_ml , termhighlighted  termhighlighted,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1011' AND CD=CAST(termhighlighted AS VARCHAR(100)) )termhighlighted_de_ml , total_incidents_reported  total_incidents_reported,treatment_desc  treatment_desc,treatment_performed  treatment_performed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7089' AND CD=CAST(treatment_performed AS VARCHAR(100)) )treatment_performed_de_ml , type_ae_duration  type_ae_duration,un_anticipated_state_health  un_anticipated_state_health,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(un_anticipated_state_health AS VARCHAR(100)) )un_anticipated_state_health_de_ml , user_created  user_created,user_modified  user_modified,uuid  uuid,version  version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_reaction
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REACTION_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction')
  ) where REC_RANK=1 ), D_MEDDRA_ICD_SUBSET AS 
( select distinct BK_MEDDRA_ICD_WID,LLT_CODE,PT_CODE,PRIMARY_SOC_FG from $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD
 -- WHERE MEDDRA_VERSION='26.0'
)
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_reaction_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_reaction_SUBSET.USER_CREATED CREATED_BY,lsmv_reaction_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_reaction_SUBSET.version  ,lsmv_reaction_SUBSET.uuid  ,lsmv_reaction_SUBSET.user_modified  ,lsmv_reaction_SUBSET.user_created  ,lsmv_reaction_SUBSET.un_anticipated_state_health_de_ml  ,lsmv_reaction_SUBSET.un_anticipated_state_health  ,lsmv_reaction_SUBSET.type_ae_duration  ,lsmv_reaction_SUBSET.treatment_performed_de_ml  ,lsmv_reaction_SUBSET.treatment_performed  ,lsmv_reaction_SUBSET.treatment_desc  ,lsmv_reaction_SUBSET.total_incidents_reported  ,lsmv_reaction_SUBSET.termhighlighted_de_ml  ,lsmv_reaction_SUBSET.termhighlighted  ,lsmv_reaction_SUBSET.term_added_by_de_ml  ,lsmv_reaction_SUBSET.term_added_by  ,lsmv_reaction_SUBSET.spr_id  ,lsmv_reaction_SUBSET.snomed_info  ,lsmv_reaction_SUBSET.snomed_coded_flag  ,lsmv_reaction_SUBSET.snomed  ,lsmv_reaction_SUBSET.severity_de_ml  ,lsmv_reaction_SUBSET.severity_company  ,lsmv_reaction_SUBSET.severity  ,lsmv_reaction_SUBSET.seriousness_de_ml  ,lsmv_reaction_SUBSET.seriousness_company_de_ml  ,lsmv_reaction_SUBSET.seriousness_company  ,lsmv_reaction_SUBSET.seriousness  ,lsmv_reaction_SUBSET.serious_injury_text  ,lsmv_reaction_SUBSET.serious_injury  ,lsmv_reaction_SUBSET.ser_public_health_threat_de_ml  ,lsmv_reaction_SUBSET.ser_public_health_threat  ,lsmv_reaction_SUBSET.root_cause_term  ,lsmv_reaction_SUBSET.root_cause_code  ,lsmv_reaction_SUBSET.repo_reaction_id  ,lsmv_reaction_SUBSET.repo_event_id  ,lsmv_reaction_SUBSET.related_product  ,lsmv_reaction_SUBSET.related_drug_name  ,lsmv_reaction_SUBSET.record_id  ,lsmv_reaction_SUBSET.reactstartdatefmt  ,lsmv_reaction_SUBSET.reactstartdate_nf  ,lsmv_reaction_SUBSET.reactstartdate  ,lsmv_reaction_SUBSET.reactoutcome_de_ml  ,lsmv_reaction_SUBSET.reactoutcome_company_de_ml  ,lsmv_reaction_SUBSET.reactoutcome_company  ,lsmv_reaction_SUBSET.reactoutcome  ,lsmv_reaction_SUBSET.reactothers  ,lsmv_reaction_SUBSET.reactmeddraverpt_lang  ,lsmv_reaction_SUBSET.reactmeddraverpt  ,lsmv_reaction_SUBSET.reactmeddraverllt_lang  ,lsmv_reaction_SUBSET.reactmeddraverllt  ,lsmv_reaction_SUBSET.reactmeddraptlevel  ,lsmv_reaction_SUBSET.reactmeddrapt_lang  ,lsmv_reaction_SUBSET.reactmeddrapt_code  ,lsmv_reaction_SUBSET.reactmeddrapt  ,lsmv_reaction_SUBSET.reactmeddralltlevel  ,lsmv_reaction_SUBSET.reactmeddrallt_lang  ,lsmv_reaction_SUBSET.reactmeddrallt_code  ,lsmv_reaction_SUBSET.reactmeddrallt  ,lsmv_reaction_SUBSET.reactmeddracodept  ,lsmv_reaction_SUBSET.reactmeddracodellt  ,lsmv_reaction_SUBSET.reactlasttimeunit_de_ml  ,lsmv_reaction_SUBSET.reactlasttimeunit  ,lsmv_reaction_SUBSET.reactlasttime_lang  ,lsmv_reaction_SUBSET.reactlasttime  ,lsmv_reaction_SUBSET.reactionterm_translation  ,lsmv_reaction_SUBSET.reactionterm_lang  ,lsmv_reaction_SUBSET.reactionterm_jpn  ,lsmv_reaction_SUBSET.reactionterm  ,lsmv_reaction_SUBSET.reactionsitetext  ,lsmv_reaction_SUBSET.reaction_type  ,lsmv_reaction_SUBSET.reaction_site  ,lsmv_reaction_SUBSET.reaction_sequence_id  ,lsmv_reaction_SUBSET.reaction_lang  ,lsmv_reaction_SUBSET.reaction_json_text  ,lsmv_reaction_SUBSET.reaction_description  ,lsmv_reaction_SUBSET.reactfirsttimeunit_de_ml  ,lsmv_reaction_SUBSET.reactfirsttimeunit  ,lsmv_reaction_SUBSET.reactfirsttime_lang  ,lsmv_reaction_SUBSET.reactfirsttime  ,lsmv_reaction_SUBSET.reactenddatefmt  ,lsmv_reaction_SUBSET.reactenddate_nf  ,lsmv_reaction_SUBSET.reactenddate  ,lsmv_reaction_SUBSET.reactdurationunit_de_ml  ,lsmv_reaction_SUBSET.reactdurationunit  ,lsmv_reaction_SUBSET.reactduration_lang  ,lsmv_reaction_SUBSET.reactduration  ,lsmv_reaction_SUBSET.react_start_date_tz  ,lsmv_reaction_SUBSET.react_manual_check  ,lsmv_reaction_SUBSET.react_end_manual_check  ,lsmv_reaction_SUBSET.react_end_date_tz  ,lsmv_reaction_SUBSET.rank_order  ,lsmv_reaction_SUBSET.problem_term  ,lsmv_reaction_SUBSET.problem_code  ,lsmv_reaction_SUBSET.primarysrcreaction_lang_de_ml  ,lsmv_reaction_SUBSET.primarysrcreaction_lang  ,lsmv_reaction_SUBSET.primarysrcreaction  ,lsmv_reaction_SUBSET.primary_soc_code  ,lsmv_reaction_SUBSET.pqc_unique_id_number  ,lsmv_reaction_SUBSET.pmreaction_description  ,lsmv_reaction_SUBSET.outcome_details  ,lsmv_reaction_SUBSET.outcome_date  ,lsmv_reaction_SUBSET.other_medically_text  ,lsmv_reaction_SUBSET.other_event_occur_location  ,lsmv_reaction_SUBSET.other_code_flag  ,lsmv_reaction_SUBSET.other_code_desc  ,lsmv_reaction_SUBSET.other  ,lsmv_reaction_SUBSET.not_reportable_jpn  ,lsmv_reaction_SUBSET.nonserious_nf  ,lsmv_reaction_SUBSET.nonserious_info  ,lsmv_reaction_SUBSET.nonserious_de_ml  ,lsmv_reaction_SUBSET.nonserious  ,lsmv_reaction_SUBSET.near_incident_related_de_ml  ,lsmv_reaction_SUBSET.near_incident_related  ,lsmv_reaction_SUBSET.near_incident  ,lsmv_reaction_SUBSET.municipality  ,lsmv_reaction_SUBSET.medicallysignificant_nf  ,lsmv_reaction_SUBSET.medicallysignificant_de_ml  ,lsmv_reaction_SUBSET.medicallysignificant  ,lsmv_reaction_SUBSET.medically_confirm_nf  ,lsmv_reaction_SUBSET.medically_confirm_manual  ,lsmv_reaction_SUBSET.medically_confirm_de_ml  ,lsmv_reaction_SUBSET.medically_confirm  ,lsmv_reaction_SUBSET.medical_device_problem  ,lsmv_reaction_SUBSET.measures_thisdrug  ,lsmv_reaction_SUBSET.measures_theadverse_event  ,lsmv_reaction_SUBSET.manual_duration_de_ml  ,lsmv_reaction_SUBSET.manual_duration  ,lsmv_reaction_SUBSET.malfunction  ,lsmv_reaction_SUBSET.lot_number_related_to_pc  ,lsmv_reaction_SUBSET.location_sf  ,lsmv_reaction_SUBSET.location_de_ml  ,lsmv_reaction_SUBSET.location  ,lsmv_reaction_SUBSET.lifethreatening_nf  ,lsmv_reaction_SUBSET.lifethreatening_de_ml  ,lsmv_reaction_SUBSET.lifethreatening  ,lsmv_reaction_SUBSET.is_serious_event_de_ml  ,lsmv_reaction_SUBSET.is_serious_event  ,lsmv_reaction_SUBSET.is_related_to_pc  ,lsmv_reaction_SUBSET.is_non_ae  ,lsmv_reaction_SUBSET.is_ime_manual_de_ml  ,lsmv_reaction_SUBSET.is_ime_manual  ,lsmv_reaction_SUBSET.is_ime_de_ml  ,lsmv_reaction_SUBSET.is_ime  ,lsmv_reaction_SUBSET.is_event_related_to_pqc  ,lsmv_reaction_SUBSET.is_event_manual_de_ml  ,lsmv_reaction_SUBSET.is_event_manual  ,lsmv_reaction_SUBSET.is_event_aesi_de_ml  ,lsmv_reaction_SUBSET.is_event_aesi  ,lsmv_reaction_SUBSET.is_dme_de_ml  ,lsmv_reaction_SUBSET.is_dme  ,lsmv_reaction_SUBSET.is_desig_medical_event_de_ml  ,lsmv_reaction_SUBSET.is_desig_medical_event  ,lsmv_reaction_SUBSET.is_added_from_krjp  ,lsmv_reaction_SUBSET.investigation_finding  ,lsmv_reaction_SUBSET.interventionrequired_nf  ,lsmv_reaction_SUBSET.interventionrequired_de_ml  ,lsmv_reaction_SUBSET.interventionrequired  ,lsmv_reaction_SUBSET.inq_rec_id  ,lsmv_reaction_SUBSET.incident_occurred_during_de_ml  ,lsmv_reaction_SUBSET.incident_occurred_during  ,lsmv_reaction_SUBSET.in_history_de_ml  ,lsmv_reaction_SUBSET.in_history  ,lsmv_reaction_SUBSET.imrdf_type  ,lsmv_reaction_SUBSET.imrdf_similar_desc  ,lsmv_reaction_SUBSET.imrdf_similar_codes_de_ml  ,lsmv_reaction_SUBSET.imrdf_similar_codes  ,lsmv_reaction_SUBSET.imrdf_desc  ,lsmv_reaction_SUBSET.imrdf_codes  ,lsmv_reaction_SUBSET.hospitalize_date_fmt  ,lsmv_reaction_SUBSET.hospitalization_nf  ,lsmv_reaction_SUBSET.hospitalization_de_ml  ,lsmv_reaction_SUBSET.hospitalization_days  ,lsmv_reaction_SUBSET.hospitalization  ,lsmv_reaction_SUBSET.hospitalisation_date  ,lsmv_reaction_SUBSET.hospital_discharge_date  ,lsmv_reaction_SUBSET.hosp_prolonged_nf  ,lsmv_reaction_SUBSET.hosp_prolonged_de_ml  ,lsmv_reaction_SUBSET.hosp_prolonged  ,lsmv_reaction_SUBSET.hcf_number  ,lsmv_reaction_SUBSET.hcf_name  ,lsmv_reaction_SUBSET.fk_vac_rec_id  ,lsmv_reaction_SUBSET.fk_apat_rec_id  ,lsmv_reaction_SUBSET.failed_reason_or_other_info  ,lsmv_reaction_SUBSET.ext_clob_fld  ,lsmv_reaction_SUBSET.explanation_form_missing_code_f  ,lsmv_reaction_SUBSET.explanation_form_missing_code_e  ,lsmv_reaction_SUBSET.exempted_events_manual  ,lsmv_reaction_SUBSET.exempted_events_de_ml  ,lsmv_reaction_SUBSET.exempted_events  ,lsmv_reaction_SUBSET.event_type_de_ml  ,lsmv_reaction_SUBSET.event_type  ,lsmv_reaction_SUBSET.event_received_date_jpn_manual  ,lsmv_reaction_SUBSET.event_received_date_jpn  ,lsmv_reaction_SUBSET.event_received_date  ,lsmv_reaction_SUBSET.event_occure_during_incident_de_ml  ,lsmv_reaction_SUBSET.event_occure_during_incident  ,lsmv_reaction_SUBSET.event_occur_location_de_ml  ,lsmv_reaction_SUBSET.event_occur_location  ,lsmv_reaction_SUBSET.event_location  ,lsmv_reaction_SUBSET.event_coded_flag  ,lsmv_reaction_SUBSET.eudamed_ref_number  ,lsmv_reaction_SUBSET.entity_updated  ,lsmv_reaction_SUBSET.e2b_r3_primary_source_transln  ,lsmv_reaction_SUBSET.drug_interaction_de_ml  ,lsmv_reaction_SUBSET.drug_interaction  ,lsmv_reaction_SUBSET.down_grade_reason  ,lsmv_reaction_SUBSET.discharge_date_fmt  ,lsmv_reaction_SUBSET.disability_nf  ,lsmv_reaction_SUBSET.disability_de_ml  ,lsmv_reaction_SUBSET.disability  ,lsmv_reaction_SUBSET.device_criteria_desc  ,lsmv_reaction_SUBSET.device_criteria_de_ml  ,lsmv_reaction_SUBSET.device_criteria  ,lsmv_reaction_SUBSET.detected_country_de_ml  ,lsmv_reaction_SUBSET.detected_country  ,lsmv_reaction_SUBSET.death_nf  ,lsmv_reaction_SUBSET.death_de_ml  ,lsmv_reaction_SUBSET.death  ,lsmv_reaction_SUBSET.date_modified  ,lsmv_reaction_SUBSET.date_created  ,lsmv_reaction_SUBSET.cr_ref_id  ,lsmv_reaction_SUBSET.country_detection_manual  ,lsmv_reaction_SUBSET.congenitalanomaly_nf  ,lsmv_reaction_SUBSET.congenitalanomaly_de_ml  ,lsmv_reaction_SUBSET.congenitalanomaly  ,lsmv_reaction_SUBSET.condition_sf  ,lsmv_reaction_SUBSET.condition_de_ml  ,lsmv_reaction_SUBSET.condition  ,lsmv_reaction_SUBSET.comp_rec_id  ,lsmv_reaction_SUBSET.comments_similar_incident  ,lsmv_reaction_SUBSET.comments  ,lsmv_reaction_SUBSET.coding_type_de_ml  ,lsmv_reaction_SUBSET.coding_type  ,lsmv_reaction_SUBSET.coding_comments  ,lsmv_reaction_SUBSET.coded_reactionterm  ,lsmv_reaction_SUBSET.caused_by_lo_defect_de_ml  ,lsmv_reaction_SUBSET.caused_by_lo_defect  ,lsmv_reaction_SUBSET.basic_identification_de_ml  ,lsmv_reaction_SUBSET.basic_identification  ,lsmv_reaction_SUBSET.basic_id_desc  ,lsmv_reaction_SUBSET.ase_seriousness_check  ,lsmv_reaction_SUBSET.ari_rec_id  ,lsmv_reaction_SUBSET.are_react_start_date_string  ,lsmv_reaction_SUBSET.are_react_end_date_string  ,lsmv_reaction_SUBSET.anticipated_events_manual  ,lsmv_reaction_SUBSET.anticipated_events_de_ml  ,lsmv_reaction_SUBSET.anticipated_events  ,lsmv_reaction_SUBSET.always_down_grade_reason  ,lsmv_reaction_SUBSET.ae_additional_information ,CONCAT(NVL(lsmv_reaction_SUBSET.RECORD_ID,-1)) INTEGRATION_ID,COALESCE(D_MEDDRA_ICD_SUBSET.BK_MEDDRA_ICD_WID,-1) As REACTMEDDRAPT_CODE_MD_BK
,COALESCE(D_MEDDRA_ICD_SUBSET1.BK_MEDDRA_ICD_WID,-1) As REACTMEDDRAPT_CODE_MD_BK1
 FROM lsmv_reaction_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_reaction_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  
 LEFT JOIN D_MEDDRA_ICD_SUBSET
ON lsmv_reaction_SUBSET.REACTMEDDRALLT_CODE=D_MEDDRA_ICD_SUBSET.LLT_CODE and lsmv_reaction_SUBSET.REACTMEDDRAPT_CODE=D_MEDDRA_ICD_SUBSET.PT_CODE 
and D_MEDDRA_ICD_SUBSET.PRIMARY_SOC_FG='Y'
LEFT JOIN D_MEDDRA_ICD_SUBSET As D_MEDDRA_ICD_SUBSET1
ON lsmv_reaction_SUBSET.REACTMEDDRALLT_CODE=D_MEDDRA_ICD_SUBSET1.LLT_CODE and lsmv_reaction_SUBSET.REACTMEDDRAPT_CODE=D_MEDDRA_ICD_SUBSET1.PT_CODE 
 WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP)
where target_table_name='LS_DB_REACTION'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION   
SET LS_DB_REACTION.version = LS_DB_REACTION_TMP.version,LS_DB_REACTION.uuid = LS_DB_REACTION_TMP.uuid,LS_DB_REACTION.user_modified = LS_DB_REACTION_TMP.user_modified,LS_DB_REACTION.user_created = LS_DB_REACTION_TMP.user_created,LS_DB_REACTION.un_anticipated_state_health_de_ml = LS_DB_REACTION_TMP.un_anticipated_state_health_de_ml,LS_DB_REACTION.un_anticipated_state_health = LS_DB_REACTION_TMP.un_anticipated_state_health,LS_DB_REACTION.type_ae_duration = LS_DB_REACTION_TMP.type_ae_duration,LS_DB_REACTION.treatment_performed_de_ml = LS_DB_REACTION_TMP.treatment_performed_de_ml,LS_DB_REACTION.treatment_performed = LS_DB_REACTION_TMP.treatment_performed,LS_DB_REACTION.treatment_desc = LS_DB_REACTION_TMP.treatment_desc,LS_DB_REACTION.total_incidents_reported = LS_DB_REACTION_TMP.total_incidents_reported,LS_DB_REACTION.termhighlighted_de_ml = LS_DB_REACTION_TMP.termhighlighted_de_ml,LS_DB_REACTION.termhighlighted = LS_DB_REACTION_TMP.termhighlighted,LS_DB_REACTION.term_added_by_de_ml = LS_DB_REACTION_TMP.term_added_by_de_ml,LS_DB_REACTION.term_added_by = LS_DB_REACTION_TMP.term_added_by,LS_DB_REACTION.spr_id = LS_DB_REACTION_TMP.spr_id,LS_DB_REACTION.snomed_info = LS_DB_REACTION_TMP.snomed_info,LS_DB_REACTION.snomed_coded_flag = LS_DB_REACTION_TMP.snomed_coded_flag,LS_DB_REACTION.snomed = LS_DB_REACTION_TMP.snomed,LS_DB_REACTION.severity_de_ml = LS_DB_REACTION_TMP.severity_de_ml,LS_DB_REACTION.severity_company = LS_DB_REACTION_TMP.severity_company,LS_DB_REACTION.severity = LS_DB_REACTION_TMP.severity,LS_DB_REACTION.seriousness_de_ml = LS_DB_REACTION_TMP.seriousness_de_ml,LS_DB_REACTION.seriousness_company_de_ml = LS_DB_REACTION_TMP.seriousness_company_de_ml,LS_DB_REACTION.seriousness_company = LS_DB_REACTION_TMP.seriousness_company,LS_DB_REACTION.seriousness = LS_DB_REACTION_TMP.seriousness,LS_DB_REACTION.serious_injury_text = LS_DB_REACTION_TMP.serious_injury_text,LS_DB_REACTION.serious_injury = LS_DB_REACTION_TMP.serious_injury,LS_DB_REACTION.ser_public_health_threat_de_ml = LS_DB_REACTION_TMP.ser_public_health_threat_de_ml,LS_DB_REACTION.ser_public_health_threat = LS_DB_REACTION_TMP.ser_public_health_threat,LS_DB_REACTION.root_cause_term = LS_DB_REACTION_TMP.root_cause_term,LS_DB_REACTION.root_cause_code = LS_DB_REACTION_TMP.root_cause_code,LS_DB_REACTION.repo_reaction_id = LS_DB_REACTION_TMP.repo_reaction_id,LS_DB_REACTION.repo_event_id = LS_DB_REACTION_TMP.repo_event_id,LS_DB_REACTION.related_product = LS_DB_REACTION_TMP.related_product,LS_DB_REACTION.related_drug_name = LS_DB_REACTION_TMP.related_drug_name,LS_DB_REACTION.record_id = LS_DB_REACTION_TMP.record_id,LS_DB_REACTION.reactstartdatefmt = LS_DB_REACTION_TMP.reactstartdatefmt,LS_DB_REACTION.reactstartdate_nf = LS_DB_REACTION_TMP.reactstartdate_nf,LS_DB_REACTION.reactstartdate = LS_DB_REACTION_TMP.reactstartdate,LS_DB_REACTION.reactoutcome_de_ml = LS_DB_REACTION_TMP.reactoutcome_de_ml,LS_DB_REACTION.reactoutcome_company_de_ml = LS_DB_REACTION_TMP.reactoutcome_company_de_ml,LS_DB_REACTION.reactoutcome_company = LS_DB_REACTION_TMP.reactoutcome_company,LS_DB_REACTION.reactoutcome = LS_DB_REACTION_TMP.reactoutcome,LS_DB_REACTION.reactothers = LS_DB_REACTION_TMP.reactothers,LS_DB_REACTION.reactmeddraverpt_lang = LS_DB_REACTION_TMP.reactmeddraverpt_lang,LS_DB_REACTION.reactmeddraverpt = LS_DB_REACTION_TMP.reactmeddraverpt,LS_DB_REACTION.reactmeddraverllt_lang = LS_DB_REACTION_TMP.reactmeddraverllt_lang,LS_DB_REACTION.reactmeddraverllt = LS_DB_REACTION_TMP.reactmeddraverllt,LS_DB_REACTION.reactmeddraptlevel = LS_DB_REACTION_TMP.reactmeddraptlevel,LS_DB_REACTION.reactmeddrapt_lang = LS_DB_REACTION_TMP.reactmeddrapt_lang,LS_DB_REACTION.reactmeddrapt_code = LS_DB_REACTION_TMP.reactmeddrapt_code,LS_DB_REACTION.reactmeddrapt = LS_DB_REACTION_TMP.reactmeddrapt,LS_DB_REACTION.reactmeddralltlevel = LS_DB_REACTION_TMP.reactmeddralltlevel,LS_DB_REACTION.reactmeddrallt_lang = LS_DB_REACTION_TMP.reactmeddrallt_lang,LS_DB_REACTION.reactmeddrallt_code = LS_DB_REACTION_TMP.reactmeddrallt_code,LS_DB_REACTION.reactmeddrallt = LS_DB_REACTION_TMP.reactmeddrallt,LS_DB_REACTION.reactmeddracodept = LS_DB_REACTION_TMP.reactmeddracodept,LS_DB_REACTION.reactmeddracodellt = LS_DB_REACTION_TMP.reactmeddracodellt,LS_DB_REACTION.reactlasttimeunit_de_ml = LS_DB_REACTION_TMP.reactlasttimeunit_de_ml,LS_DB_REACTION.reactlasttimeunit = LS_DB_REACTION_TMP.reactlasttimeunit,LS_DB_REACTION.reactlasttime_lang = LS_DB_REACTION_TMP.reactlasttime_lang,LS_DB_REACTION.reactlasttime = LS_DB_REACTION_TMP.reactlasttime,LS_DB_REACTION.reactionterm_translation = LS_DB_REACTION_TMP.reactionterm_translation,LS_DB_REACTION.reactionterm_lang = LS_DB_REACTION_TMP.reactionterm_lang,LS_DB_REACTION.reactionterm_jpn = LS_DB_REACTION_TMP.reactionterm_jpn,LS_DB_REACTION.reactionterm = LS_DB_REACTION_TMP.reactionterm,LS_DB_REACTION.reactionsitetext = LS_DB_REACTION_TMP.reactionsitetext,LS_DB_REACTION.reaction_type = LS_DB_REACTION_TMP.reaction_type,LS_DB_REACTION.reaction_site = LS_DB_REACTION_TMP.reaction_site,LS_DB_REACTION.reaction_sequence_id = LS_DB_REACTION_TMP.reaction_sequence_id,LS_DB_REACTION.reaction_lang = LS_DB_REACTION_TMP.reaction_lang,LS_DB_REACTION.reaction_json_text = LS_DB_REACTION_TMP.reaction_json_text,LS_DB_REACTION.reaction_description = LS_DB_REACTION_TMP.reaction_description,LS_DB_REACTION.reactfirsttimeunit_de_ml = LS_DB_REACTION_TMP.reactfirsttimeunit_de_ml,LS_DB_REACTION.reactfirsttimeunit = LS_DB_REACTION_TMP.reactfirsttimeunit,LS_DB_REACTION.reactfirsttime_lang = LS_DB_REACTION_TMP.reactfirsttime_lang,LS_DB_REACTION.reactfirsttime = LS_DB_REACTION_TMP.reactfirsttime,LS_DB_REACTION.reactenddatefmt = LS_DB_REACTION_TMP.reactenddatefmt,LS_DB_REACTION.reactenddate_nf = LS_DB_REACTION_TMP.reactenddate_nf,LS_DB_REACTION.reactenddate = LS_DB_REACTION_TMP.reactenddate,LS_DB_REACTION.reactdurationunit_de_ml = LS_DB_REACTION_TMP.reactdurationunit_de_ml,LS_DB_REACTION.reactdurationunit = LS_DB_REACTION_TMP.reactdurationunit,LS_DB_REACTION.reactduration_lang = LS_DB_REACTION_TMP.reactduration_lang,LS_DB_REACTION.reactduration = LS_DB_REACTION_TMP.reactduration,LS_DB_REACTION.react_start_date_tz = LS_DB_REACTION_TMP.react_start_date_tz,LS_DB_REACTION.react_manual_check = LS_DB_REACTION_TMP.react_manual_check,LS_DB_REACTION.react_end_manual_check = LS_DB_REACTION_TMP.react_end_manual_check,LS_DB_REACTION.react_end_date_tz = LS_DB_REACTION_TMP.react_end_date_tz,LS_DB_REACTION.rank_order = LS_DB_REACTION_TMP.rank_order,LS_DB_REACTION.problem_term = LS_DB_REACTION_TMP.problem_term,LS_DB_REACTION.problem_code = LS_DB_REACTION_TMP.problem_code,LS_DB_REACTION.primarysrcreaction_lang_de_ml = LS_DB_REACTION_TMP.primarysrcreaction_lang_de_ml,LS_DB_REACTION.primarysrcreaction_lang = LS_DB_REACTION_TMP.primarysrcreaction_lang,LS_DB_REACTION.primarysrcreaction = LS_DB_REACTION_TMP.primarysrcreaction,LS_DB_REACTION.primary_soc_code = LS_DB_REACTION_TMP.primary_soc_code,LS_DB_REACTION.pqc_unique_id_number = LS_DB_REACTION_TMP.pqc_unique_id_number,LS_DB_REACTION.pmreaction_description = LS_DB_REACTION_TMP.pmreaction_description,LS_DB_REACTION.outcome_details = LS_DB_REACTION_TMP.outcome_details,LS_DB_REACTION.outcome_date = LS_DB_REACTION_TMP.outcome_date,LS_DB_REACTION.other_medically_text = LS_DB_REACTION_TMP.other_medically_text,LS_DB_REACTION.other_event_occur_location = LS_DB_REACTION_TMP.other_event_occur_location,LS_DB_REACTION.other_code_flag = LS_DB_REACTION_TMP.other_code_flag,LS_DB_REACTION.other_code_desc = LS_DB_REACTION_TMP.other_code_desc,LS_DB_REACTION.other = LS_DB_REACTION_TMP.other,LS_DB_REACTION.not_reportable_jpn = LS_DB_REACTION_TMP.not_reportable_jpn,LS_DB_REACTION.nonserious_nf = LS_DB_REACTION_TMP.nonserious_nf,LS_DB_REACTION.nonserious_info = LS_DB_REACTION_TMP.nonserious_info,LS_DB_REACTION.nonserious_de_ml = LS_DB_REACTION_TMP.nonserious_de_ml,LS_DB_REACTION.nonserious = LS_DB_REACTION_TMP.nonserious,LS_DB_REACTION.near_incident_related_de_ml = LS_DB_REACTION_TMP.near_incident_related_de_ml,LS_DB_REACTION.near_incident_related = LS_DB_REACTION_TMP.near_incident_related,LS_DB_REACTION.near_incident = LS_DB_REACTION_TMP.near_incident,LS_DB_REACTION.municipality = LS_DB_REACTION_TMP.municipality,LS_DB_REACTION.medicallysignificant_nf = LS_DB_REACTION_TMP.medicallysignificant_nf,LS_DB_REACTION.medicallysignificant_de_ml = LS_DB_REACTION_TMP.medicallysignificant_de_ml,LS_DB_REACTION.medicallysignificant = LS_DB_REACTION_TMP.medicallysignificant,LS_DB_REACTION.medically_confirm_nf = LS_DB_REACTION_TMP.medically_confirm_nf,LS_DB_REACTION.medically_confirm_manual = LS_DB_REACTION_TMP.medically_confirm_manual,LS_DB_REACTION.medically_confirm_de_ml = LS_DB_REACTION_TMP.medically_confirm_de_ml,LS_DB_REACTION.medically_confirm = LS_DB_REACTION_TMP.medically_confirm,LS_DB_REACTION.medical_device_problem = LS_DB_REACTION_TMP.medical_device_problem,LS_DB_REACTION.measures_thisdrug = LS_DB_REACTION_TMP.measures_thisdrug,LS_DB_REACTION.measures_theadverse_event = LS_DB_REACTION_TMP.measures_theadverse_event,LS_DB_REACTION.manual_duration_de_ml = LS_DB_REACTION_TMP.manual_duration_de_ml,LS_DB_REACTION.manual_duration = LS_DB_REACTION_TMP.manual_duration,LS_DB_REACTION.malfunction = LS_DB_REACTION_TMP.malfunction,LS_DB_REACTION.lot_number_related_to_pc = LS_DB_REACTION_TMP.lot_number_related_to_pc,LS_DB_REACTION.location_sf = LS_DB_REACTION_TMP.location_sf,LS_DB_REACTION.location_de_ml = LS_DB_REACTION_TMP.location_de_ml,LS_DB_REACTION.location = LS_DB_REACTION_TMP.location,LS_DB_REACTION.lifethreatening_nf = LS_DB_REACTION_TMP.lifethreatening_nf,LS_DB_REACTION.lifethreatening_de_ml = LS_DB_REACTION_TMP.lifethreatening_de_ml,LS_DB_REACTION.lifethreatening = LS_DB_REACTION_TMP.lifethreatening,LS_DB_REACTION.is_serious_event_de_ml = LS_DB_REACTION_TMP.is_serious_event_de_ml,LS_DB_REACTION.is_serious_event = LS_DB_REACTION_TMP.is_serious_event,LS_DB_REACTION.is_related_to_pc = LS_DB_REACTION_TMP.is_related_to_pc,LS_DB_REACTION.is_non_ae = LS_DB_REACTION_TMP.is_non_ae,LS_DB_REACTION.is_ime_manual_de_ml = LS_DB_REACTION_TMP.is_ime_manual_de_ml,LS_DB_REACTION.is_ime_manual = LS_DB_REACTION_TMP.is_ime_manual,LS_DB_REACTION.is_ime_de_ml = LS_DB_REACTION_TMP.is_ime_de_ml,LS_DB_REACTION.is_ime = LS_DB_REACTION_TMP.is_ime,LS_DB_REACTION.is_event_related_to_pqc = LS_DB_REACTION_TMP.is_event_related_to_pqc,LS_DB_REACTION.is_event_manual_de_ml = LS_DB_REACTION_TMP.is_event_manual_de_ml,LS_DB_REACTION.is_event_manual = LS_DB_REACTION_TMP.is_event_manual,LS_DB_REACTION.is_event_aesi_de_ml = LS_DB_REACTION_TMP.is_event_aesi_de_ml,LS_DB_REACTION.is_event_aesi = LS_DB_REACTION_TMP.is_event_aesi,LS_DB_REACTION.is_dme_de_ml = LS_DB_REACTION_TMP.is_dme_de_ml,LS_DB_REACTION.is_dme = LS_DB_REACTION_TMP.is_dme,LS_DB_REACTION.is_desig_medical_event_de_ml = LS_DB_REACTION_TMP.is_desig_medical_event_de_ml,LS_DB_REACTION.is_desig_medical_event = LS_DB_REACTION_TMP.is_desig_medical_event,LS_DB_REACTION.is_added_from_krjp = LS_DB_REACTION_TMP.is_added_from_krjp,LS_DB_REACTION.investigation_finding = LS_DB_REACTION_TMP.investigation_finding,LS_DB_REACTION.interventionrequired_nf = LS_DB_REACTION_TMP.interventionrequired_nf,LS_DB_REACTION.interventionrequired_de_ml = LS_DB_REACTION_TMP.interventionrequired_de_ml,LS_DB_REACTION.interventionrequired = LS_DB_REACTION_TMP.interventionrequired,LS_DB_REACTION.inq_rec_id = LS_DB_REACTION_TMP.inq_rec_id,LS_DB_REACTION.incident_occurred_during_de_ml = LS_DB_REACTION_TMP.incident_occurred_during_de_ml,LS_DB_REACTION.incident_occurred_during = LS_DB_REACTION_TMP.incident_occurred_during,LS_DB_REACTION.in_history_de_ml = LS_DB_REACTION_TMP.in_history_de_ml,LS_DB_REACTION.in_history = LS_DB_REACTION_TMP.in_history,LS_DB_REACTION.imrdf_type = LS_DB_REACTION_TMP.imrdf_type,LS_DB_REACTION.imrdf_similar_desc = LS_DB_REACTION_TMP.imrdf_similar_desc,LS_DB_REACTION.imrdf_similar_codes_de_ml = LS_DB_REACTION_TMP.imrdf_similar_codes_de_ml,LS_DB_REACTION.imrdf_similar_codes = LS_DB_REACTION_TMP.imrdf_similar_codes,LS_DB_REACTION.imrdf_desc = LS_DB_REACTION_TMP.imrdf_desc,LS_DB_REACTION.imrdf_codes = LS_DB_REACTION_TMP.imrdf_codes,LS_DB_REACTION.hospitalize_date_fmt = LS_DB_REACTION_TMP.hospitalize_date_fmt,LS_DB_REACTION.hospitalization_nf = LS_DB_REACTION_TMP.hospitalization_nf,LS_DB_REACTION.hospitalization_de_ml = LS_DB_REACTION_TMP.hospitalization_de_ml,LS_DB_REACTION.hospitalization_days = LS_DB_REACTION_TMP.hospitalization_days,LS_DB_REACTION.hospitalization = LS_DB_REACTION_TMP.hospitalization,LS_DB_REACTION.hospitalisation_date = LS_DB_REACTION_TMP.hospitalisation_date,LS_DB_REACTION.hospital_discharge_date = LS_DB_REACTION_TMP.hospital_discharge_date,LS_DB_REACTION.hosp_prolonged_nf = LS_DB_REACTION_TMP.hosp_prolonged_nf,LS_DB_REACTION.hosp_prolonged_de_ml = LS_DB_REACTION_TMP.hosp_prolonged_de_ml,LS_DB_REACTION.hosp_prolonged = LS_DB_REACTION_TMP.hosp_prolonged,LS_DB_REACTION.hcf_number = LS_DB_REACTION_TMP.hcf_number,LS_DB_REACTION.hcf_name = LS_DB_REACTION_TMP.hcf_name,LS_DB_REACTION.fk_vac_rec_id = LS_DB_REACTION_TMP.fk_vac_rec_id,LS_DB_REACTION.fk_apat_rec_id = LS_DB_REACTION_TMP.fk_apat_rec_id,LS_DB_REACTION.failed_reason_or_other_info = LS_DB_REACTION_TMP.failed_reason_or_other_info,LS_DB_REACTION.ext_clob_fld = LS_DB_REACTION_TMP.ext_clob_fld,LS_DB_REACTION.explanation_form_missing_code_f = LS_DB_REACTION_TMP.explanation_form_missing_code_f,LS_DB_REACTION.explanation_form_missing_code_e = LS_DB_REACTION_TMP.explanation_form_missing_code_e,LS_DB_REACTION.exempted_events_manual = LS_DB_REACTION_TMP.exempted_events_manual,LS_DB_REACTION.exempted_events_de_ml = LS_DB_REACTION_TMP.exempted_events_de_ml,LS_DB_REACTION.exempted_events = LS_DB_REACTION_TMP.exempted_events,LS_DB_REACTION.event_type_de_ml = LS_DB_REACTION_TMP.event_type_de_ml,LS_DB_REACTION.event_type = LS_DB_REACTION_TMP.event_type,LS_DB_REACTION.event_received_date_jpn_manual = LS_DB_REACTION_TMP.event_received_date_jpn_manual,LS_DB_REACTION.event_received_date_jpn = LS_DB_REACTION_TMP.event_received_date_jpn,LS_DB_REACTION.event_received_date = LS_DB_REACTION_TMP.event_received_date,LS_DB_REACTION.event_occure_during_incident_de_ml = LS_DB_REACTION_TMP.event_occure_during_incident_de_ml,LS_DB_REACTION.event_occure_during_incident = LS_DB_REACTION_TMP.event_occure_during_incident,LS_DB_REACTION.event_occur_location_de_ml = LS_DB_REACTION_TMP.event_occur_location_de_ml,LS_DB_REACTION.event_occur_location = LS_DB_REACTION_TMP.event_occur_location,LS_DB_REACTION.event_location = LS_DB_REACTION_TMP.event_location,LS_DB_REACTION.event_coded_flag = LS_DB_REACTION_TMP.event_coded_flag,LS_DB_REACTION.eudamed_ref_number = LS_DB_REACTION_TMP.eudamed_ref_number,LS_DB_REACTION.entity_updated = LS_DB_REACTION_TMP.entity_updated,LS_DB_REACTION.e2b_r3_primary_source_transln = LS_DB_REACTION_TMP.e2b_r3_primary_source_transln,LS_DB_REACTION.drug_interaction_de_ml = LS_DB_REACTION_TMP.drug_interaction_de_ml,LS_DB_REACTION.drug_interaction = LS_DB_REACTION_TMP.drug_interaction,LS_DB_REACTION.down_grade_reason = LS_DB_REACTION_TMP.down_grade_reason,LS_DB_REACTION.discharge_date_fmt = LS_DB_REACTION_TMP.discharge_date_fmt,LS_DB_REACTION.disability_nf = LS_DB_REACTION_TMP.disability_nf,LS_DB_REACTION.disability_de_ml = LS_DB_REACTION_TMP.disability_de_ml,LS_DB_REACTION.disability = LS_DB_REACTION_TMP.disability,LS_DB_REACTION.device_criteria_desc = LS_DB_REACTION_TMP.device_criteria_desc,LS_DB_REACTION.device_criteria_de_ml = LS_DB_REACTION_TMP.device_criteria_de_ml,LS_DB_REACTION.device_criteria = LS_DB_REACTION_TMP.device_criteria,LS_DB_REACTION.detected_country_de_ml = LS_DB_REACTION_TMP.detected_country_de_ml,LS_DB_REACTION.detected_country = LS_DB_REACTION_TMP.detected_country,LS_DB_REACTION.death_nf = LS_DB_REACTION_TMP.death_nf,LS_DB_REACTION.death_de_ml = LS_DB_REACTION_TMP.death_de_ml,LS_DB_REACTION.death = LS_DB_REACTION_TMP.death,LS_DB_REACTION.date_modified = LS_DB_REACTION_TMP.date_modified,LS_DB_REACTION.date_created = LS_DB_REACTION_TMP.date_created,LS_DB_REACTION.cr_ref_id = LS_DB_REACTION_TMP.cr_ref_id,LS_DB_REACTION.country_detection_manual = LS_DB_REACTION_TMP.country_detection_manual,LS_DB_REACTION.congenitalanomaly_nf = LS_DB_REACTION_TMP.congenitalanomaly_nf,LS_DB_REACTION.congenitalanomaly_de_ml = LS_DB_REACTION_TMP.congenitalanomaly_de_ml,LS_DB_REACTION.congenitalanomaly = LS_DB_REACTION_TMP.congenitalanomaly,LS_DB_REACTION.condition_sf = LS_DB_REACTION_TMP.condition_sf,LS_DB_REACTION.condition_de_ml = LS_DB_REACTION_TMP.condition_de_ml,LS_DB_REACTION.condition = LS_DB_REACTION_TMP.condition,LS_DB_REACTION.comp_rec_id = LS_DB_REACTION_TMP.comp_rec_id,LS_DB_REACTION.comments_similar_incident = LS_DB_REACTION_TMP.comments_similar_incident,LS_DB_REACTION.comments = LS_DB_REACTION_TMP.comments,LS_DB_REACTION.coding_type_de_ml = LS_DB_REACTION_TMP.coding_type_de_ml,LS_DB_REACTION.coding_type = LS_DB_REACTION_TMP.coding_type,LS_DB_REACTION.coding_comments = LS_DB_REACTION_TMP.coding_comments,LS_DB_REACTION.coded_reactionterm = LS_DB_REACTION_TMP.coded_reactionterm,LS_DB_REACTION.caused_by_lo_defect_de_ml = LS_DB_REACTION_TMP.caused_by_lo_defect_de_ml,LS_DB_REACTION.caused_by_lo_defect = LS_DB_REACTION_TMP.caused_by_lo_defect,LS_DB_REACTION.basic_identification_de_ml = LS_DB_REACTION_TMP.basic_identification_de_ml,LS_DB_REACTION.basic_identification = LS_DB_REACTION_TMP.basic_identification,LS_DB_REACTION.basic_id_desc = LS_DB_REACTION_TMP.basic_id_desc,LS_DB_REACTION.ase_seriousness_check = LS_DB_REACTION_TMP.ase_seriousness_check,LS_DB_REACTION.ari_rec_id = LS_DB_REACTION_TMP.ari_rec_id,LS_DB_REACTION.are_react_start_date_string = LS_DB_REACTION_TMP.are_react_start_date_string,LS_DB_REACTION.are_react_end_date_string = LS_DB_REACTION_TMP.are_react_end_date_string,LS_DB_REACTION.anticipated_events_manual = LS_DB_REACTION_TMP.anticipated_events_manual,LS_DB_REACTION.anticipated_events_de_ml = LS_DB_REACTION_TMP.anticipated_events_de_ml,LS_DB_REACTION.anticipated_events = LS_DB_REACTION_TMP.anticipated_events,LS_DB_REACTION.always_down_grade_reason = LS_DB_REACTION_TMP.always_down_grade_reason,LS_DB_REACTION.ae_additional_information = LS_DB_REACTION_TMP.ae_additional_information,
LS_DB_REACTION.PROCESSING_DT = LS_DB_REACTION_TMP.PROCESSING_DT ,
LS_DB_REACTION.receipt_id     =LS_DB_REACTION_TMP.receipt_id        ,
LS_DB_REACTION.case_no        =LS_DB_REACTION_TMP.case_no           ,
LS_DB_REACTION.case_version   =LS_DB_REACTION_TMP.case_version      ,
LS_DB_REACTION.version_no     =LS_DB_REACTION_TMP.version_no        ,
LS_DB_REACTION.expiry_date    =LS_DB_REACTION_TMP.expiry_date       ,
LS_DB_REACTION.load_ts        =LS_DB_REACTION_TMP.load_ts           ,
LS_DB_REACTION.REACTMEDDRAPT_CODE_MD_BK=LS_DB_REACTION.REACTMEDDRAPT_CODE_MD_BK,
LS_DB_REACTION.REACTMEDDRAPT_CODE_MD_BK1=LS_DB_REACTION.REACTMEDDRAPT_CODE_MD_BK1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP 
WHERE 	LS_DB_REACTION.INTEGRATION_ID = LS_DB_REACTION_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_REACTION_TMP.PROCESSING_DT = LS_DB_REACTION.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION
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
un_anticipated_state_health_de_ml,
un_anticipated_state_health,
type_ae_duration,
treatment_performed_de_ml,
treatment_performed,
treatment_desc,
total_incidents_reported,
termhighlighted_de_ml,
termhighlighted,
term_added_by_de_ml,
term_added_by,
spr_id,
snomed_info,
snomed_coded_flag,
snomed,
severity_de_ml,
severity_company,
severity,
seriousness_de_ml,
seriousness_company_de_ml,
seriousness_company,
seriousness,
serious_injury_text,
serious_injury,
ser_public_health_threat_de_ml,
ser_public_health_threat,
root_cause_term,
root_cause_code,
repo_reaction_id,
repo_event_id,
related_product,
related_drug_name,
record_id,
reactstartdatefmt,
reactstartdate_nf,
reactstartdate,
reactoutcome_de_ml,
reactoutcome_company_de_ml,
reactoutcome_company,
reactoutcome,
reactothers,
reactmeddraverpt_lang,
reactmeddraverpt,
reactmeddraverllt_lang,
reactmeddraverllt,
reactmeddraptlevel,
reactmeddrapt_lang,
reactmeddrapt_code,
reactmeddrapt,
reactmeddralltlevel,
reactmeddrallt_lang,
reactmeddrallt_code,
reactmeddrallt,
reactmeddracodept,
reactmeddracodellt,
reactlasttimeunit_de_ml,
reactlasttimeunit,
reactlasttime_lang,
reactlasttime,
reactionterm_translation,
reactionterm_lang,
reactionterm_jpn,
reactionterm,
reactionsitetext,
reaction_type,
reaction_site,
reaction_sequence_id,
reaction_lang,
reaction_json_text,
reaction_description,
reactfirsttimeunit_de_ml,
reactfirsttimeunit,
reactfirsttime_lang,
reactfirsttime,
reactenddatefmt,
reactenddate_nf,
reactenddate,
reactdurationunit_de_ml,
reactdurationunit,
reactduration_lang,
reactduration,
react_start_date_tz,
react_manual_check,
react_end_manual_check,
react_end_date_tz,
rank_order,
problem_term,
problem_code,
primarysrcreaction_lang_de_ml,
primarysrcreaction_lang,
primarysrcreaction,
primary_soc_code,
pqc_unique_id_number,
pmreaction_description,
outcome_details,
outcome_date,
other_medically_text,
other_event_occur_location,
other_code_flag,
other_code_desc,
other,
not_reportable_jpn,
nonserious_nf,
nonserious_info,
nonserious_de_ml,
nonserious,
near_incident_related_de_ml,
near_incident_related,
near_incident,
municipality,
medicallysignificant_nf,
medicallysignificant_de_ml,
medicallysignificant,
medically_confirm_nf,
medically_confirm_manual,
medically_confirm_de_ml,
medically_confirm,
medical_device_problem,
measures_thisdrug,
measures_theadverse_event,
manual_duration_de_ml,
manual_duration,
malfunction,
lot_number_related_to_pc,
location_sf,
location_de_ml,
location,
lifethreatening_nf,
lifethreatening_de_ml,
lifethreatening,
is_serious_event_de_ml,
is_serious_event,
is_related_to_pc,
is_non_ae,
is_ime_manual_de_ml,
is_ime_manual,
is_ime_de_ml,
is_ime,
is_event_related_to_pqc,
is_event_manual_de_ml,
is_event_manual,
is_event_aesi_de_ml,
is_event_aesi,
is_dme_de_ml,
is_dme,
is_desig_medical_event_de_ml,
is_desig_medical_event,
is_added_from_krjp,
investigation_finding,
interventionrequired_nf,
interventionrequired_de_ml,
interventionrequired,
inq_rec_id,
incident_occurred_during_de_ml,
incident_occurred_during,
in_history_de_ml,
in_history,
imrdf_type,
imrdf_similar_desc,
imrdf_similar_codes_de_ml,
imrdf_similar_codes,
imrdf_desc,
imrdf_codes,
hospitalize_date_fmt,
hospitalization_nf,
hospitalization_de_ml,
hospitalization_days,
hospitalization,
hospitalisation_date,
hospital_discharge_date,
hosp_prolonged_nf,
hosp_prolonged_de_ml,
hosp_prolonged,
hcf_number,
hcf_name,
fk_vac_rec_id,
fk_apat_rec_id,
failed_reason_or_other_info,
ext_clob_fld,
explanation_form_missing_code_f,
explanation_form_missing_code_e,
exempted_events_manual,
exempted_events_de_ml,
exempted_events,
event_type_de_ml,
event_type,
event_received_date_jpn_manual,
event_received_date_jpn,
event_received_date,
event_occure_during_incident_de_ml,
event_occure_during_incident,
event_occur_location_de_ml,
event_occur_location,
event_location,
event_coded_flag,
eudamed_ref_number,
entity_updated,
e2b_r3_primary_source_transln,
drug_interaction_de_ml,
drug_interaction,
down_grade_reason,
discharge_date_fmt,
disability_nf,
disability_de_ml,
disability,
device_criteria_desc,
device_criteria_de_ml,
device_criteria,
detected_country_de_ml,
detected_country,
death_nf,
death_de_ml,
death,
date_modified,
date_created,
cr_ref_id,
country_detection_manual,
congenitalanomaly_nf,
congenitalanomaly_de_ml,
congenitalanomaly,
condition_sf,
condition_de_ml,
condition,
comp_rec_id,
comments_similar_incident,
comments,
coding_type_de_ml,
coding_type,
coding_comments,
coded_reactionterm,
caused_by_lo_defect_de_ml,
caused_by_lo_defect,
basic_identification_de_ml,
basic_identification,
basic_id_desc,
ase_seriousness_check,
ari_rec_id,
are_react_start_date_string,
are_react_end_date_string,
anticipated_events_manual,
anticipated_events_de_ml,
anticipated_events,
always_down_grade_reason,
ae_additional_information,
REACTMEDDRAPT_CODE_MD_BK,
REACTMEDDRAPT_CODE_MD_BK1)
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
un_anticipated_state_health_de_ml,
un_anticipated_state_health,
type_ae_duration,
treatment_performed_de_ml,
treatment_performed,
treatment_desc,
total_incidents_reported,
termhighlighted_de_ml,
termhighlighted,
term_added_by_de_ml,
term_added_by,
spr_id,
snomed_info,
snomed_coded_flag,
snomed,
severity_de_ml,
severity_company,
severity,
seriousness_de_ml,
seriousness_company_de_ml,
seriousness_company,
seriousness,
serious_injury_text,
serious_injury,
ser_public_health_threat_de_ml,
ser_public_health_threat,
root_cause_term,
root_cause_code,
repo_reaction_id,
repo_event_id,
related_product,
related_drug_name,
record_id,
reactstartdatefmt,
reactstartdate_nf,
reactstartdate,
reactoutcome_de_ml,
reactoutcome_company_de_ml,
reactoutcome_company,
reactoutcome,
reactothers,
reactmeddraverpt_lang,
reactmeddraverpt,
reactmeddraverllt_lang,
reactmeddraverllt,
reactmeddraptlevel,
reactmeddrapt_lang,
reactmeddrapt_code,
reactmeddrapt,
reactmeddralltlevel,
reactmeddrallt_lang,
reactmeddrallt_code,
reactmeddrallt,
reactmeddracodept,
reactmeddracodellt,
reactlasttimeunit_de_ml,
reactlasttimeunit,
reactlasttime_lang,
reactlasttime,
reactionterm_translation,
reactionterm_lang,
reactionterm_jpn,
reactionterm,
reactionsitetext,
reaction_type,
reaction_site,
reaction_sequence_id,
reaction_lang,
reaction_json_text,
reaction_description,
reactfirsttimeunit_de_ml,
reactfirsttimeunit,
reactfirsttime_lang,
reactfirsttime,
reactenddatefmt,
reactenddate_nf,
reactenddate,
reactdurationunit_de_ml,
reactdurationunit,
reactduration_lang,
reactduration,
react_start_date_tz,
react_manual_check,
react_end_manual_check,
react_end_date_tz,
rank_order,
problem_term,
problem_code,
primarysrcreaction_lang_de_ml,
primarysrcreaction_lang,
primarysrcreaction,
primary_soc_code,
pqc_unique_id_number,
pmreaction_description,
outcome_details,
outcome_date,
other_medically_text,
other_event_occur_location,
other_code_flag,
other_code_desc,
other,
not_reportable_jpn,
nonserious_nf,
nonserious_info,
nonserious_de_ml,
nonserious,
near_incident_related_de_ml,
near_incident_related,
near_incident,
municipality,
medicallysignificant_nf,
medicallysignificant_de_ml,
medicallysignificant,
medically_confirm_nf,
medically_confirm_manual,
medically_confirm_de_ml,
medically_confirm,
medical_device_problem,
measures_thisdrug,
measures_theadverse_event,
manual_duration_de_ml,
manual_duration,
malfunction,
lot_number_related_to_pc,
location_sf,
location_de_ml,
location,
lifethreatening_nf,
lifethreatening_de_ml,
lifethreatening,
is_serious_event_de_ml,
is_serious_event,
is_related_to_pc,
is_non_ae,
is_ime_manual_de_ml,
is_ime_manual,
is_ime_de_ml,
is_ime,
is_event_related_to_pqc,
is_event_manual_de_ml,
is_event_manual,
is_event_aesi_de_ml,
is_event_aesi,
is_dme_de_ml,
is_dme,
is_desig_medical_event_de_ml,
is_desig_medical_event,
is_added_from_krjp,
investigation_finding,
interventionrequired_nf,
interventionrequired_de_ml,
interventionrequired,
inq_rec_id,
incident_occurred_during_de_ml,
incident_occurred_during,
in_history_de_ml,
in_history,
imrdf_type,
imrdf_similar_desc,
imrdf_similar_codes_de_ml,
imrdf_similar_codes,
imrdf_desc,
imrdf_codes,
hospitalize_date_fmt,
hospitalization_nf,
hospitalization_de_ml,
hospitalization_days,
hospitalization,
hospitalisation_date,
hospital_discharge_date,
hosp_prolonged_nf,
hosp_prolonged_de_ml,
hosp_prolonged,
hcf_number,
hcf_name,
fk_vac_rec_id,
fk_apat_rec_id,
failed_reason_or_other_info,
ext_clob_fld,
explanation_form_missing_code_f,
explanation_form_missing_code_e,
exempted_events_manual,
exempted_events_de_ml,
exempted_events,
event_type_de_ml,
event_type,
event_received_date_jpn_manual,
event_received_date_jpn,
event_received_date,
event_occure_during_incident_de_ml,
event_occure_during_incident,
event_occur_location_de_ml,
event_occur_location,
event_location,
event_coded_flag,
eudamed_ref_number,
entity_updated,
e2b_r3_primary_source_transln,
drug_interaction_de_ml,
drug_interaction,
down_grade_reason,
discharge_date_fmt,
disability_nf,
disability_de_ml,
disability,
device_criteria_desc,
device_criteria_de_ml,
device_criteria,
detected_country_de_ml,
detected_country,
death_nf,
death_de_ml,
death,
date_modified,
date_created,
cr_ref_id,
country_detection_manual,
congenitalanomaly_nf,
congenitalanomaly_de_ml,
congenitalanomaly,
condition_sf,
condition_de_ml,
condition,
comp_rec_id,
comments_similar_incident,
comments,
coding_type_de_ml,
coding_type,
coding_comments,
coded_reactionterm,
caused_by_lo_defect_de_ml,
caused_by_lo_defect,
basic_identification_de_ml,
basic_identification,
basic_id_desc,
ase_seriousness_check,
ari_rec_id,
are_react_start_date_string,
are_react_end_date_string,
anticipated_events_manual,
anticipated_events_de_ml,
anticipated_events,
always_down_grade_reason,
ae_additional_information,
REACTMEDDRAPT_CODE_MD_BK,
REACTMEDDRAPT_CODE_MD_BK1
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION TGT)
                                                                                ; 
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REACTION_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP 
WHERE 	TO_DATE(LS_DB_REACTION.PROCESSING_DT) < TO_DATE(LS_DB_REACTION_TMP.PROCESSING_DT)
AND LS_DB_REACTION.INTEGRATION_ID = LS_DB_REACTION_TMP.INTEGRATION_ID
AND LS_DB_REACTION.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REACTION_DELETION_TMP  WHERE TABLE_NAME='lsmv_reaction')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REACTION_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REACTION'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACTION'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_REACTION Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REACTION'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_REACTION not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
