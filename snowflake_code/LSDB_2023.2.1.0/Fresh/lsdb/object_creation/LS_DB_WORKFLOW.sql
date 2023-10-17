
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WORKFLOW()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WORKFLOW'),
	'LSDB','Case','LS_DB_WORKFLOW',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_WORKFLOW' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_WORKFLOW' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  As select RECORD_ID,'lsmv_parallel_wf_details' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_parallel_wf_details WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_wf_activity' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_activity WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_workflow' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_workflow_quality' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow_quality WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_activity WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_awf_rec_id record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_activity WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow_quality WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_awfa_rec_id record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_workflow_quality WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parallel_wf_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT wf_rec_id record_id, 0 common_parent_key,  0 ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parallel_wf_details WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_WORKFLOW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_workflow_quality_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  wfquality_date_created,date_modified  wfquality_date_modified,fk_awfa_rec_id  wfquality_fk_awfa_rec_id,form_recid  wfquality_form_recid,object_id  wfquality_object_id,quality_flag  wfquality_quality_flag,record_id  wfquality_record_id,script_record_id  wfquality_script_record_id,spr_id  wfquality_spr_id,threshold  wfquality_threshold,user_created  wfquality_user_created,user_modified  wfquality_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_workflow_quality
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_awfa_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_quality')
  ) where REC_RANK=1 )
  , lsmv_workflow_SUBSET AS 
(
select * from 
    (SELECT  
    active_wf  workflow_active_wf,child_wf  workflow_child_wf,date_created  workflow_date_created,date_modified  workflow_date_modified,default_wf  workflow_default_wf,description  workflow_description,is_std  workflow_is_std,lslp_wf_lang  workflow_lslp_wf_lang,multilingual_support  workflow_multilingual_support,notes_flag  workflow_notes_flag,object_id  workflow_object_id,parent_wf_rec_id  workflow_parent_wf_rec_id,process_definition  workflow_process_definition,record_id  workflow_record_id,rel_version  workflow_rel_version,spr_id  workflow_spr_id,total_activities  workflow_total_activities,user_created  workflow_user_created,user_modified  workflow_user_modified,version  workflow_version,wf_mxgraph_xml  workflow_wf_mxgraph_xml,wf_name  workflow_wf_name,wf_scheme  workflow_wf_scheme,wf_type  workflow_wf_type,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_workflow
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow')
  ) where REC_RANK=1 )
  , lsmv_wf_activity_SUBSET AS 
(
select * from 
    (SELECT  
    add_distribution_contact  wfactivity_add_distribution_contact,allow_auto_assessment  wfactivity_allow_auto_assessment,allow_deletion  wfactivity_allow_deletion,allow_e2b_auto_assessment  wfactivity_allow_e2b_auto_assessment,allow_icsr_export  wfactivity_allow_icsr_export,allow_manual_lock  wfactivity_allow_manual_lock,allow_redistribute  wfactivity_allow_redistribute,allow_reject_case  wfactivity_allow_reject_case,always_case_edit  wfactivity_always_case_edit,ang_additional_comments  wfactivity_ang_additional_comments,ang_additional_comments_rule  wfactivity_ang_additional_comments_rule,ang_pregnancy_comments  wfactivity_ang_pregnancy_comments,ang_pregnancy_comments_rule  wfactivity_ang_pregnancy_comments_rule,auto_compl_rule_id  wfactivity_auto_compl_rule_id,auto_complete  wfactivity_auto_complete,auto_data_assessment  wfactivity_auto_data_assessment,automated_causality_methods  wfactivity_automated_causality_methods,bfc_enter_check  wfactivity_bfc_enter_check,bulk_analyze  wfactivity_bulk_analyze,bulk_assignment  wfactivity_bulk_assignment,bulk_routing  wfactivity_bulk_routing,check_lateness  wfactivity_check_lateness,close_inquiry  wfactivity_close_inquiry,company_remarks  wfactivity_company_remarks,company_remarks_rule  wfactivity_company_remarks_rule,compare_reconcile_follow_up  wfactivity_compare_reconcile_follow_up,compl_rule_id  wfactivity_compl_rule_id,completion_validation  wfactivity_completion_validation,custom_activity_check  wfactivity_custom_activity_check,date_created  wfactivity_date_created,date_modified  wfactivity_date_modified,decision_db_function  wfactivity_decision_db_function,decision_rule_id  wfactivity_decision_rule_id,description  wfactivity_description,display_order  wfactivity_display_order,dispose_to_irt  wfactivity_dispose_to_irt,drug_due_date  wfactivity_drug_due_date,dup_check_on_ca  wfactivity_dup_check_on_ca,dup_check_on_save  wfactivity_dup_check_on_save,e2b_dtd  wfactivity_e2b_dtd,enable_aesi  wfactivity_enable_aesi,enable_approve  wfactivity_enable_approve,enable_lock  wfactivity_enable_lock,enable_sub_tracking  wfactivity_enable_sub_tracking,fk_awf_rec_id  wfactivity_fk_awf_rec_id,fk_jbpm_node_id  wfactivity_fk_jbpm_node_id,fk_wfd_rec_id  wfactivity_fk_wfd_rec_id,fu_case_auto_merge  wfactivity_fu_case_auto_merge,fu_case_auto_merge_rule  wfactivity_fu_case_auto_merge_rule,generate_case_score  wfactivity_generate_case_score,generate_case_summary  wfactivity_generate_case_summary,generate_cioms  wfactivity_generate_cioms,generate_narrative  wfactivity_generate_narrative,generate_narrative_rule  wfactivity_generate_narrative_rule,icsr_relevency_for_dispo  wfactivity_icsr_relevency_for_dispo,if_dup_e2b_case  wfactivity_if_dup_e2b_case,if_error_e2b_case  wfactivity_if_error_e2b_case,if_fu_e2b_case  wfactivity_if_fu_e2b_case,if_new_e2b_case  wfactivity_if_new_e2b_case,incl_wf_actvity  wfactivity_incl_wf_actvity,investigation_activity  wfactivity_investigation_activity,is_aose_search  wfactivity_is_aose_search,is_aose_search_on_save  wfactivity_is_aose_search_on_save,is_critical_path  wfactivity_is_critical_path,is_delete_activity  wfactivity_is_delete_activity,is_disposition  wfactivity_is_disposition,is_distribute_transit  wfactivity_is_distribute_transit,is_e2b_auto_assessment  wfactivity_is_e2b_auto_assessment,is_non_case_activity  wfactivity_is_non_case_activity,jpn_noncase  wfactivity_jpn_noncase,lateness_default_code  wfactivity_lateness_default_code,linked_status  wfactivity_linked_status,local_correspondence_view  wfactivity_local_correspondence_view,local_doc_view  wfactivity_local_doc_view,medical_history  wfactivity_medical_history,medical_history_rule  wfactivity_medical_history_rule,monitor_transitions  wfactivity_monitor_transitions,monitor_transitions_sub  wfactivity_monitor_transitions_sub,move_create_new_version  wfactivity_move_create_new_version,move_oow  wfactivity_move_oow,name  wfactivity_name,note_flag  wfactivity_note_flag,notify_fu_complete_activity  wfactivity_notify_fu_complete_activity,object_id  wfactivity_object_id,onfailure_transition_id  wfactivity_onfailure_transition_id,onsuccess_transition_id  wfactivity_onsuccess_transition_id,parallel_wf  wfactivity_parallel_wf,pharmacovigilance_cmt_rule  wfactivity_pharmacovigilance_cmt_rule,pharmacovigilance_comments  wfactivity_pharmacovigilance_comments,pre_investigation_activity  wfactivity_pre_investigation_activity,quality_check  wfactivity_quality_check,quality_check_form  wfactivity_quality_check_form,record_id  wfactivity_record_id,regenerate_follow_up  wfactivity_regenerate_follow_up,removed_rule_id  wfactivity_removed_rule_id,restrict_dp_access  wfactivity_restrict_dp_access,sampling_rule_id  wfactivity_sampling_rule_id,screen  wfactivity_screen,sequence  wfactivity_sequence,spr_id  wfactivity_spr_id,stop_sampling  wfactivity_stop_sampling,submit_uncoded_terms  wfactivity_submit_uncoded_terms,sync_flag  wfactivity_sync_flag,transition_validation  wfactivity_transition_validation,type  wfactivity_type,update_vta_synonyms  wfactivity_update_vta_synonyms,user_created  wfactivity_user_created,user_modified  wfactivity_user_modified,validation_enabled  wfactivity_validation_enabled,validations_on_exit  wfactivity_validations_on_exit,version  wfactivity_version,wait_for_parallel_wf  wfactivity_wait_for_parallel_wf,wf_activity_id  wfactivity_wf_activity_id,wf_activity_mandtry  wfactivity_wf_activity_mandtry,wf_activity_mxgraph_id  wfactivity_wf_activity_mxgraph_id,wf_affiliate_sub_due_date  wfactivity_wf_affiliate_sub_due_date,wfa_data_assessment_type  wfactivity_wfa_data_assessment_type,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_wf_activity
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_awf_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_activity')
  ) where REC_RANK=1 )
  , lsmv_parallel_wf_details_SUBSET AS 
(
select * from 
    (SELECT  
    aer_approval_date  parllwf_aer_approval_date,aer_approval_status  parllwf_aer_approval_status,date_created  parllwf_date_created,date_modified  parllwf_date_modified,im_rec_id  parllwf_im_rec_id,pwf_act_name  parllwf_pwf_act_name,pwf_act_recid  parllwf_pwf_act_recid,record_id  parllwf_record_id,spr_id  parllwf_spr_id,user_created  parllwf_user_created,user_modified  parllwf_user_modified,wf_completed  parllwf_wf_completed,wf_instance_rec_id  parllwf_wf_instance_rec_id,wf_rec_id  parllwf_wf_rec_id,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_parallel_wf_details
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
wf_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_parallel_wf_details')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_parallel_wf_details_SUBSET.parllwf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_workflow_quality_SUBSET.wfquality_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_workflow_SUBSET.workflow_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_wf_activity_SUBSET.wfactivity_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_workflow_SUBSET.workflow_USER_MODIFIED USER_MODIFIED,lsmv_workflow_SUBSET.workflow_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_workflow_SUBSET.workflow_USER_CREATED CREATED_BY,lsmv_workflow_SUBSET.workflow_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_workflow_SUBSET.workflow_wf_type  ,lsmv_workflow_SUBSET.workflow_wf_scheme  ,lsmv_workflow_SUBSET.workflow_wf_name  ,lsmv_workflow_SUBSET.workflow_wf_mxgraph_xml  ,lsmv_workflow_SUBSET.workflow_version  ,lsmv_workflow_SUBSET.workflow_user_modified  ,lsmv_workflow_SUBSET.workflow_user_created  ,lsmv_workflow_SUBSET.workflow_total_activities  ,lsmv_workflow_SUBSET.workflow_spr_id  ,lsmv_workflow_SUBSET.workflow_rel_version  ,lsmv_workflow_SUBSET.workflow_record_id  ,lsmv_workflow_SUBSET.workflow_process_definition  ,lsmv_workflow_SUBSET.workflow_parent_wf_rec_id  ,lsmv_workflow_SUBSET.workflow_object_id  ,lsmv_workflow_SUBSET.workflow_notes_flag  ,lsmv_workflow_SUBSET.workflow_multilingual_support  ,lsmv_workflow_SUBSET.workflow_lslp_wf_lang  ,lsmv_workflow_SUBSET.workflow_is_std  ,lsmv_workflow_SUBSET.workflow_description  ,lsmv_workflow_SUBSET.workflow_default_wf  ,lsmv_workflow_SUBSET.workflow_date_modified  ,lsmv_workflow_SUBSET.workflow_date_created  ,lsmv_workflow_SUBSET.workflow_child_wf  ,lsmv_workflow_SUBSET.workflow_active_wf  ,lsmv_workflow_quality_SUBSET.wfquality_user_modified  ,lsmv_workflow_quality_SUBSET.wfquality_user_created  ,lsmv_workflow_quality_SUBSET.wfquality_threshold  ,lsmv_workflow_quality_SUBSET.wfquality_spr_id  ,lsmv_workflow_quality_SUBSET.wfquality_script_record_id  ,lsmv_workflow_quality_SUBSET.wfquality_record_id  ,lsmv_workflow_quality_SUBSET.wfquality_quality_flag  ,lsmv_workflow_quality_SUBSET.wfquality_object_id  ,lsmv_workflow_quality_SUBSET.wfquality_form_recid  ,lsmv_workflow_quality_SUBSET.wfquality_fk_awfa_rec_id  ,lsmv_workflow_quality_SUBSET.wfquality_date_modified  ,lsmv_workflow_quality_SUBSET.wfquality_date_created  ,lsmv_wf_activity_SUBSET.wfactivity_wfa_data_assessment_type  ,lsmv_wf_activity_SUBSET.wfactivity_wf_affiliate_sub_due_date  ,lsmv_wf_activity_SUBSET.wfactivity_wf_activity_mxgraph_id  ,lsmv_wf_activity_SUBSET.wfactivity_wf_activity_mandtry  ,lsmv_wf_activity_SUBSET.wfactivity_wf_activity_id  ,lsmv_wf_activity_SUBSET.wfactivity_wait_for_parallel_wf  ,lsmv_wf_activity_SUBSET.wfactivity_version  ,lsmv_wf_activity_SUBSET.wfactivity_validations_on_exit  ,lsmv_wf_activity_SUBSET.wfactivity_validation_enabled  ,lsmv_wf_activity_SUBSET.wfactivity_user_modified  ,lsmv_wf_activity_SUBSET.wfactivity_user_created  ,lsmv_wf_activity_SUBSET.wfactivity_update_vta_synonyms  ,lsmv_wf_activity_SUBSET.wfactivity_type  ,lsmv_wf_activity_SUBSET.wfactivity_transition_validation  ,lsmv_wf_activity_SUBSET.wfactivity_sync_flag  ,lsmv_wf_activity_SUBSET.wfactivity_submit_uncoded_terms  ,lsmv_wf_activity_SUBSET.wfactivity_stop_sampling  ,lsmv_wf_activity_SUBSET.wfactivity_spr_id  ,lsmv_wf_activity_SUBSET.wfactivity_sequence  ,lsmv_wf_activity_SUBSET.wfactivity_screen  ,lsmv_wf_activity_SUBSET.wfactivity_sampling_rule_id  ,lsmv_wf_activity_SUBSET.wfactivity_restrict_dp_access  ,lsmv_wf_activity_SUBSET.wfactivity_removed_rule_id  ,lsmv_wf_activity_SUBSET.wfactivity_regenerate_follow_up  ,lsmv_wf_activity_SUBSET.wfactivity_record_id  ,lsmv_wf_activity_SUBSET.wfactivity_quality_check_form  ,lsmv_wf_activity_SUBSET.wfactivity_quality_check  ,lsmv_wf_activity_SUBSET.wfactivity_pre_investigation_activity  ,lsmv_wf_activity_SUBSET.wfactivity_pharmacovigilance_comments  ,lsmv_wf_activity_SUBSET.wfactivity_pharmacovigilance_cmt_rule  ,lsmv_wf_activity_SUBSET.wfactivity_parallel_wf  ,lsmv_wf_activity_SUBSET.wfactivity_onsuccess_transition_id  ,lsmv_wf_activity_SUBSET.wfactivity_onfailure_transition_id  ,lsmv_wf_activity_SUBSET.wfactivity_object_id  ,lsmv_wf_activity_SUBSET.wfactivity_notify_fu_complete_activity  ,lsmv_wf_activity_SUBSET.wfactivity_note_flag  ,lsmv_wf_activity_SUBSET.wfactivity_name  ,lsmv_wf_activity_SUBSET.wfactivity_move_oow  ,lsmv_wf_activity_SUBSET.wfactivity_move_create_new_version  ,lsmv_wf_activity_SUBSET.wfactivity_monitor_transitions_sub  ,lsmv_wf_activity_SUBSET.wfactivity_monitor_transitions  ,lsmv_wf_activity_SUBSET.wfactivity_medical_history_rule  ,lsmv_wf_activity_SUBSET.wfactivity_medical_history  ,lsmv_wf_activity_SUBSET.wfactivity_local_doc_view  ,lsmv_wf_activity_SUBSET.wfactivity_local_correspondence_view  ,lsmv_wf_activity_SUBSET.wfactivity_linked_status  ,lsmv_wf_activity_SUBSET.wfactivity_lateness_default_code  ,lsmv_wf_activity_SUBSET.wfactivity_jpn_noncase  ,lsmv_wf_activity_SUBSET.wfactivity_is_non_case_activity  ,lsmv_wf_activity_SUBSET.wfactivity_is_e2b_auto_assessment  ,lsmv_wf_activity_SUBSET.wfactivity_is_distribute_transit  ,lsmv_wf_activity_SUBSET.wfactivity_is_disposition  ,lsmv_wf_activity_SUBSET.wfactivity_is_delete_activity  ,lsmv_wf_activity_SUBSET.wfactivity_is_critical_path  ,lsmv_wf_activity_SUBSET.wfactivity_is_aose_search_on_save  ,lsmv_wf_activity_SUBSET.wfactivity_is_aose_search  ,lsmv_wf_activity_SUBSET.wfactivity_investigation_activity  ,lsmv_wf_activity_SUBSET.wfactivity_incl_wf_actvity  ,lsmv_wf_activity_SUBSET.wfactivity_if_new_e2b_case  ,lsmv_wf_activity_SUBSET.wfactivity_if_fu_e2b_case  ,lsmv_wf_activity_SUBSET.wfactivity_if_error_e2b_case  ,lsmv_wf_activity_SUBSET.wfactivity_if_dup_e2b_case  ,lsmv_wf_activity_SUBSET.wfactivity_icsr_relevency_for_dispo  ,lsmv_wf_activity_SUBSET.wfactivity_generate_narrative_rule  ,lsmv_wf_activity_SUBSET.wfactivity_generate_narrative  ,lsmv_wf_activity_SUBSET.wfactivity_generate_cioms  ,lsmv_wf_activity_SUBSET.wfactivity_generate_case_summary  ,lsmv_wf_activity_SUBSET.wfactivity_generate_case_score  ,lsmv_wf_activity_SUBSET.wfactivity_fu_case_auto_merge_rule  ,lsmv_wf_activity_SUBSET.wfactivity_fu_case_auto_merge  ,lsmv_wf_activity_SUBSET.wfactivity_fk_wfd_rec_id  ,lsmv_wf_activity_SUBSET.wfactivity_fk_jbpm_node_id  ,lsmv_wf_activity_SUBSET.wfactivity_fk_awf_rec_id  ,lsmv_wf_activity_SUBSET.wfactivity_enable_sub_tracking  ,lsmv_wf_activity_SUBSET.wfactivity_enable_lock  ,lsmv_wf_activity_SUBSET.wfactivity_enable_approve  ,lsmv_wf_activity_SUBSET.wfactivity_enable_aesi  ,lsmv_wf_activity_SUBSET.wfactivity_e2b_dtd  ,lsmv_wf_activity_SUBSET.wfactivity_dup_check_on_save  ,lsmv_wf_activity_SUBSET.wfactivity_dup_check_on_ca  ,lsmv_wf_activity_SUBSET.wfactivity_drug_due_date  ,lsmv_wf_activity_SUBSET.wfactivity_dispose_to_irt  ,lsmv_wf_activity_SUBSET.wfactivity_display_order  ,lsmv_wf_activity_SUBSET.wfactivity_description  ,lsmv_wf_activity_SUBSET.wfactivity_decision_rule_id  ,lsmv_wf_activity_SUBSET.wfactivity_decision_db_function  ,lsmv_wf_activity_SUBSET.wfactivity_date_modified  ,lsmv_wf_activity_SUBSET.wfactivity_date_created  ,lsmv_wf_activity_SUBSET.wfactivity_custom_activity_check  ,lsmv_wf_activity_SUBSET.wfactivity_completion_validation  ,lsmv_wf_activity_SUBSET.wfactivity_compl_rule_id  ,lsmv_wf_activity_SUBSET.wfactivity_compare_reconcile_follow_up  ,lsmv_wf_activity_SUBSET.wfactivity_company_remarks_rule  ,lsmv_wf_activity_SUBSET.wfactivity_company_remarks  ,lsmv_wf_activity_SUBSET.wfactivity_close_inquiry  ,lsmv_wf_activity_SUBSET.wfactivity_check_lateness  ,lsmv_wf_activity_SUBSET.wfactivity_bulk_routing  ,lsmv_wf_activity_SUBSET.wfactivity_bulk_assignment  ,lsmv_wf_activity_SUBSET.wfactivity_bulk_analyze  ,lsmv_wf_activity_SUBSET.wfactivity_bfc_enter_check  ,lsmv_wf_activity_SUBSET.wfactivity_automated_causality_methods  ,lsmv_wf_activity_SUBSET.wfactivity_auto_data_assessment  ,lsmv_wf_activity_SUBSET.wfactivity_auto_complete  ,lsmv_wf_activity_SUBSET.wfactivity_auto_compl_rule_id  ,lsmv_wf_activity_SUBSET.wfactivity_ang_pregnancy_comments_rule  ,lsmv_wf_activity_SUBSET.wfactivity_ang_pregnancy_comments  ,lsmv_wf_activity_SUBSET.wfactivity_ang_additional_comments_rule  ,lsmv_wf_activity_SUBSET.wfactivity_ang_additional_comments  ,lsmv_wf_activity_SUBSET.wfactivity_always_case_edit  ,lsmv_wf_activity_SUBSET.wfactivity_allow_reject_case  ,lsmv_wf_activity_SUBSET.wfactivity_allow_redistribute  ,lsmv_wf_activity_SUBSET.wfactivity_allow_manual_lock  ,lsmv_wf_activity_SUBSET.wfactivity_allow_icsr_export  ,lsmv_wf_activity_SUBSET.wfactivity_allow_e2b_auto_assessment  ,lsmv_wf_activity_SUBSET.wfactivity_allow_deletion  ,lsmv_wf_activity_SUBSET.wfactivity_allow_auto_assessment  ,lsmv_wf_activity_SUBSET.wfactivity_add_distribution_contact  ,lsmv_parallel_wf_details_SUBSET.parllwf_wf_rec_id  ,lsmv_parallel_wf_details_SUBSET.parllwf_wf_instance_rec_id  ,lsmv_parallel_wf_details_SUBSET.parllwf_wf_completed  ,lsmv_parallel_wf_details_SUBSET.parllwf_user_modified  ,lsmv_parallel_wf_details_SUBSET.parllwf_user_created  ,lsmv_parallel_wf_details_SUBSET.parllwf_spr_id  ,lsmv_parallel_wf_details_SUBSET.parllwf_record_id  ,lsmv_parallel_wf_details_SUBSET.parllwf_pwf_act_recid  ,lsmv_parallel_wf_details_SUBSET.parllwf_pwf_act_name  ,lsmv_parallel_wf_details_SUBSET.parllwf_im_rec_id  ,lsmv_parallel_wf_details_SUBSET.parllwf_date_modified  ,lsmv_parallel_wf_details_SUBSET.parllwf_date_created  ,lsmv_parallel_wf_details_SUBSET.parllwf_aer_approval_status  ,lsmv_parallel_wf_details_SUBSET.parllwf_aer_approval_date ,CONCAT( NVL(lsmv_parallel_wf_details_SUBSET.parllwf_RECORD_ID,-1),'||',NVL(lsmv_workflow_quality_SUBSET.wfquality_RECORD_ID,-1),'||',NVL(lsmv_workflow_SUBSET.workflow_RECORD_ID,-1),'||',NVL(lsmv_wf_activity_SUBSET.wfactivity_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_workflow_SUBSET  LEFT JOIN lsmv_parallel_wf_details_SUBSET ON lsmv_workflow_SUBSET.workflow_record_id=lsmv_parallel_wf_details_SUBSET.parllwf_wf_rec_id
                         LEFT JOIN lsmv_wf_activity_SUBSET ON lsmv_workflow_SUBSET.workflow_record_id=lsmv_wf_activity_SUBSET.wfactivity_fk_awf_rec_id
                         LEFT JOIN lsmv_workflow_quality_SUBSET ON lsmv_wf_activity_SUBSET.wfactivity_record_id=lsmv_workflow_quality_SUBSET.wfquality_fk_awfa_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_parallel_wf_details_SUBSET.parllwf_RECORD_ID,lsmv_workflow_quality_SUBSET.wfquality_RECORD_ID,lsmv_workflow_SUBSET.workflow_RECORD_ID,lsmv_wf_activity_SUBSET.wfactivity_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP)
where target_table_name='LS_DB_WORKFLOW'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW   
SET LS_DB_WORKFLOW.workflow_wf_type = LS_DB_WORKFLOW_TMP.workflow_wf_type,LS_DB_WORKFLOW.workflow_wf_scheme = LS_DB_WORKFLOW_TMP.workflow_wf_scheme,LS_DB_WORKFLOW.workflow_wf_name = LS_DB_WORKFLOW_TMP.workflow_wf_name,LS_DB_WORKFLOW.workflow_wf_mxgraph_xml = LS_DB_WORKFLOW_TMP.workflow_wf_mxgraph_xml,LS_DB_WORKFLOW.workflow_version = LS_DB_WORKFLOW_TMP.workflow_version,LS_DB_WORKFLOW.workflow_user_modified = LS_DB_WORKFLOW_TMP.workflow_user_modified,LS_DB_WORKFLOW.workflow_user_created = LS_DB_WORKFLOW_TMP.workflow_user_created,LS_DB_WORKFLOW.workflow_total_activities = LS_DB_WORKFLOW_TMP.workflow_total_activities,LS_DB_WORKFLOW.workflow_spr_id = LS_DB_WORKFLOW_TMP.workflow_spr_id,LS_DB_WORKFLOW.workflow_rel_version = LS_DB_WORKFLOW_TMP.workflow_rel_version,LS_DB_WORKFLOW.workflow_record_id = LS_DB_WORKFLOW_TMP.workflow_record_id,LS_DB_WORKFLOW.workflow_process_definition = LS_DB_WORKFLOW_TMP.workflow_process_definition,LS_DB_WORKFLOW.workflow_parent_wf_rec_id = LS_DB_WORKFLOW_TMP.workflow_parent_wf_rec_id,LS_DB_WORKFLOW.workflow_object_id = LS_DB_WORKFLOW_TMP.workflow_object_id,LS_DB_WORKFLOW.workflow_notes_flag = LS_DB_WORKFLOW_TMP.workflow_notes_flag,LS_DB_WORKFLOW.workflow_multilingual_support = LS_DB_WORKFLOW_TMP.workflow_multilingual_support,LS_DB_WORKFLOW.workflow_lslp_wf_lang = LS_DB_WORKFLOW_TMP.workflow_lslp_wf_lang,LS_DB_WORKFLOW.workflow_is_std = LS_DB_WORKFLOW_TMP.workflow_is_std,LS_DB_WORKFLOW.workflow_description = LS_DB_WORKFLOW_TMP.workflow_description,LS_DB_WORKFLOW.workflow_default_wf = LS_DB_WORKFLOW_TMP.workflow_default_wf,LS_DB_WORKFLOW.workflow_date_modified = LS_DB_WORKFLOW_TMP.workflow_date_modified,LS_DB_WORKFLOW.workflow_date_created = LS_DB_WORKFLOW_TMP.workflow_date_created,LS_DB_WORKFLOW.workflow_child_wf = LS_DB_WORKFLOW_TMP.workflow_child_wf,LS_DB_WORKFLOW.workflow_active_wf = LS_DB_WORKFLOW_TMP.workflow_active_wf,LS_DB_WORKFLOW.wfquality_user_modified = LS_DB_WORKFLOW_TMP.wfquality_user_modified,LS_DB_WORKFLOW.wfquality_user_created = LS_DB_WORKFLOW_TMP.wfquality_user_created,LS_DB_WORKFLOW.wfquality_threshold = LS_DB_WORKFLOW_TMP.wfquality_threshold,LS_DB_WORKFLOW.wfquality_spr_id = LS_DB_WORKFLOW_TMP.wfquality_spr_id,LS_DB_WORKFLOW.wfquality_script_record_id = LS_DB_WORKFLOW_TMP.wfquality_script_record_id,LS_DB_WORKFLOW.wfquality_record_id = LS_DB_WORKFLOW_TMP.wfquality_record_id,LS_DB_WORKFLOW.wfquality_quality_flag = LS_DB_WORKFLOW_TMP.wfquality_quality_flag,LS_DB_WORKFLOW.wfquality_object_id = LS_DB_WORKFLOW_TMP.wfquality_object_id,LS_DB_WORKFLOW.wfquality_form_recid = LS_DB_WORKFLOW_TMP.wfquality_form_recid,LS_DB_WORKFLOW.wfquality_fk_awfa_rec_id = LS_DB_WORKFLOW_TMP.wfquality_fk_awfa_rec_id,LS_DB_WORKFLOW.wfquality_date_modified = LS_DB_WORKFLOW_TMP.wfquality_date_modified,LS_DB_WORKFLOW.wfquality_date_created = LS_DB_WORKFLOW_TMP.wfquality_date_created,LS_DB_WORKFLOW.wfactivity_wfa_data_assessment_type = LS_DB_WORKFLOW_TMP.wfactivity_wfa_data_assessment_type,LS_DB_WORKFLOW.wfactivity_wf_affiliate_sub_due_date = LS_DB_WORKFLOW_TMP.wfactivity_wf_affiliate_sub_due_date,LS_DB_WORKFLOW.wfactivity_wf_activity_mxgraph_id = LS_DB_WORKFLOW_TMP.wfactivity_wf_activity_mxgraph_id,LS_DB_WORKFLOW.wfactivity_wf_activity_mandtry = LS_DB_WORKFLOW_TMP.wfactivity_wf_activity_mandtry,LS_DB_WORKFLOW.wfactivity_wf_activity_id = LS_DB_WORKFLOW_TMP.wfactivity_wf_activity_id,LS_DB_WORKFLOW.wfactivity_wait_for_parallel_wf = LS_DB_WORKFLOW_TMP.wfactivity_wait_for_parallel_wf,LS_DB_WORKFLOW.wfactivity_version = LS_DB_WORKFLOW_TMP.wfactivity_version,LS_DB_WORKFLOW.wfactivity_validations_on_exit = LS_DB_WORKFLOW_TMP.wfactivity_validations_on_exit,LS_DB_WORKFLOW.wfactivity_validation_enabled = LS_DB_WORKFLOW_TMP.wfactivity_validation_enabled,LS_DB_WORKFLOW.wfactivity_user_modified = LS_DB_WORKFLOW_TMP.wfactivity_user_modified,LS_DB_WORKFLOW.wfactivity_user_created = LS_DB_WORKFLOW_TMP.wfactivity_user_created,LS_DB_WORKFLOW.wfactivity_update_vta_synonyms = LS_DB_WORKFLOW_TMP.wfactivity_update_vta_synonyms,LS_DB_WORKFLOW.wfactivity_type = LS_DB_WORKFLOW_TMP.wfactivity_type,LS_DB_WORKFLOW.wfactivity_transition_validation = LS_DB_WORKFLOW_TMP.wfactivity_transition_validation,LS_DB_WORKFLOW.wfactivity_sync_flag = LS_DB_WORKFLOW_TMP.wfactivity_sync_flag,LS_DB_WORKFLOW.wfactivity_submit_uncoded_terms = LS_DB_WORKFLOW_TMP.wfactivity_submit_uncoded_terms,LS_DB_WORKFLOW.wfactivity_stop_sampling = LS_DB_WORKFLOW_TMP.wfactivity_stop_sampling,LS_DB_WORKFLOW.wfactivity_spr_id = LS_DB_WORKFLOW_TMP.wfactivity_spr_id,LS_DB_WORKFLOW.wfactivity_sequence = LS_DB_WORKFLOW_TMP.wfactivity_sequence,LS_DB_WORKFLOW.wfactivity_screen = LS_DB_WORKFLOW_TMP.wfactivity_screen,LS_DB_WORKFLOW.wfactivity_sampling_rule_id = LS_DB_WORKFLOW_TMP.wfactivity_sampling_rule_id,LS_DB_WORKFLOW.wfactivity_restrict_dp_access = LS_DB_WORKFLOW_TMP.wfactivity_restrict_dp_access,LS_DB_WORKFLOW.wfactivity_removed_rule_id = LS_DB_WORKFLOW_TMP.wfactivity_removed_rule_id,LS_DB_WORKFLOW.wfactivity_regenerate_follow_up = LS_DB_WORKFLOW_TMP.wfactivity_regenerate_follow_up,LS_DB_WORKFLOW.wfactivity_record_id = LS_DB_WORKFLOW_TMP.wfactivity_record_id,LS_DB_WORKFLOW.wfactivity_quality_check_form = LS_DB_WORKFLOW_TMP.wfactivity_quality_check_form,LS_DB_WORKFLOW.wfactivity_quality_check = LS_DB_WORKFLOW_TMP.wfactivity_quality_check,LS_DB_WORKFLOW.wfactivity_pre_investigation_activity = LS_DB_WORKFLOW_TMP.wfactivity_pre_investigation_activity,LS_DB_WORKFLOW.wfactivity_pharmacovigilance_comments = LS_DB_WORKFLOW_TMP.wfactivity_pharmacovigilance_comments,LS_DB_WORKFLOW.wfactivity_pharmacovigilance_cmt_rule = LS_DB_WORKFLOW_TMP.wfactivity_pharmacovigilance_cmt_rule,LS_DB_WORKFLOW.wfactivity_parallel_wf = LS_DB_WORKFLOW_TMP.wfactivity_parallel_wf,LS_DB_WORKFLOW.wfactivity_onsuccess_transition_id = LS_DB_WORKFLOW_TMP.wfactivity_onsuccess_transition_id,LS_DB_WORKFLOW.wfactivity_onfailure_transition_id = LS_DB_WORKFLOW_TMP.wfactivity_onfailure_transition_id,LS_DB_WORKFLOW.wfactivity_object_id = LS_DB_WORKFLOW_TMP.wfactivity_object_id,LS_DB_WORKFLOW.wfactivity_notify_fu_complete_activity = LS_DB_WORKFLOW_TMP.wfactivity_notify_fu_complete_activity,LS_DB_WORKFLOW.wfactivity_note_flag = LS_DB_WORKFLOW_TMP.wfactivity_note_flag,LS_DB_WORKFLOW.wfactivity_name = LS_DB_WORKFLOW_TMP.wfactivity_name,LS_DB_WORKFLOW.wfactivity_move_oow = LS_DB_WORKFLOW_TMP.wfactivity_move_oow,LS_DB_WORKFLOW.wfactivity_move_create_new_version = LS_DB_WORKFLOW_TMP.wfactivity_move_create_new_version,LS_DB_WORKFLOW.wfactivity_monitor_transitions_sub = LS_DB_WORKFLOW_TMP.wfactivity_monitor_transitions_sub,LS_DB_WORKFLOW.wfactivity_monitor_transitions = LS_DB_WORKFLOW_TMP.wfactivity_monitor_transitions,LS_DB_WORKFLOW.wfactivity_medical_history_rule = LS_DB_WORKFLOW_TMP.wfactivity_medical_history_rule,LS_DB_WORKFLOW.wfactivity_medical_history = LS_DB_WORKFLOW_TMP.wfactivity_medical_history,LS_DB_WORKFLOW.wfactivity_local_doc_view = LS_DB_WORKFLOW_TMP.wfactivity_local_doc_view,LS_DB_WORKFLOW.wfactivity_local_correspondence_view = LS_DB_WORKFLOW_TMP.wfactivity_local_correspondence_view,LS_DB_WORKFLOW.wfactivity_linked_status = LS_DB_WORKFLOW_TMP.wfactivity_linked_status,LS_DB_WORKFLOW.wfactivity_lateness_default_code = LS_DB_WORKFLOW_TMP.wfactivity_lateness_default_code,LS_DB_WORKFLOW.wfactivity_jpn_noncase = LS_DB_WORKFLOW_TMP.wfactivity_jpn_noncase,LS_DB_WORKFLOW.wfactivity_is_non_case_activity = LS_DB_WORKFLOW_TMP.wfactivity_is_non_case_activity,LS_DB_WORKFLOW.wfactivity_is_e2b_auto_assessment = LS_DB_WORKFLOW_TMP.wfactivity_is_e2b_auto_assessment,LS_DB_WORKFLOW.wfactivity_is_distribute_transit = LS_DB_WORKFLOW_TMP.wfactivity_is_distribute_transit,LS_DB_WORKFLOW.wfactivity_is_disposition = LS_DB_WORKFLOW_TMP.wfactivity_is_disposition,LS_DB_WORKFLOW.wfactivity_is_delete_activity = LS_DB_WORKFLOW_TMP.wfactivity_is_delete_activity,LS_DB_WORKFLOW.wfactivity_is_critical_path = LS_DB_WORKFLOW_TMP.wfactivity_is_critical_path,LS_DB_WORKFLOW.wfactivity_is_aose_search_on_save = LS_DB_WORKFLOW_TMP.wfactivity_is_aose_search_on_save,LS_DB_WORKFLOW.wfactivity_is_aose_search = LS_DB_WORKFLOW_TMP.wfactivity_is_aose_search,LS_DB_WORKFLOW.wfactivity_investigation_activity = LS_DB_WORKFLOW_TMP.wfactivity_investigation_activity,LS_DB_WORKFLOW.wfactivity_incl_wf_actvity = LS_DB_WORKFLOW_TMP.wfactivity_incl_wf_actvity,LS_DB_WORKFLOW.wfactivity_if_new_e2b_case = LS_DB_WORKFLOW_TMP.wfactivity_if_new_e2b_case,LS_DB_WORKFLOW.wfactivity_if_fu_e2b_case = LS_DB_WORKFLOW_TMP.wfactivity_if_fu_e2b_case,LS_DB_WORKFLOW.wfactivity_if_error_e2b_case = LS_DB_WORKFLOW_TMP.wfactivity_if_error_e2b_case,LS_DB_WORKFLOW.wfactivity_if_dup_e2b_case = LS_DB_WORKFLOW_TMP.wfactivity_if_dup_e2b_case,LS_DB_WORKFLOW.wfactivity_icsr_relevency_for_dispo = LS_DB_WORKFLOW_TMP.wfactivity_icsr_relevency_for_dispo,LS_DB_WORKFLOW.wfactivity_generate_narrative_rule = LS_DB_WORKFLOW_TMP.wfactivity_generate_narrative_rule,LS_DB_WORKFLOW.wfactivity_generate_narrative = LS_DB_WORKFLOW_TMP.wfactivity_generate_narrative,LS_DB_WORKFLOW.wfactivity_generate_cioms = LS_DB_WORKFLOW_TMP.wfactivity_generate_cioms,LS_DB_WORKFLOW.wfactivity_generate_case_summary = LS_DB_WORKFLOW_TMP.wfactivity_generate_case_summary,LS_DB_WORKFLOW.wfactivity_generate_case_score = LS_DB_WORKFLOW_TMP.wfactivity_generate_case_score,LS_DB_WORKFLOW.wfactivity_fu_case_auto_merge_rule = LS_DB_WORKFLOW_TMP.wfactivity_fu_case_auto_merge_rule,LS_DB_WORKFLOW.wfactivity_fu_case_auto_merge = LS_DB_WORKFLOW_TMP.wfactivity_fu_case_auto_merge,LS_DB_WORKFLOW.wfactivity_fk_wfd_rec_id = LS_DB_WORKFLOW_TMP.wfactivity_fk_wfd_rec_id,LS_DB_WORKFLOW.wfactivity_fk_jbpm_node_id = LS_DB_WORKFLOW_TMP.wfactivity_fk_jbpm_node_id,LS_DB_WORKFLOW.wfactivity_fk_awf_rec_id = LS_DB_WORKFLOW_TMP.wfactivity_fk_awf_rec_id,LS_DB_WORKFLOW.wfactivity_enable_sub_tracking = LS_DB_WORKFLOW_TMP.wfactivity_enable_sub_tracking,LS_DB_WORKFLOW.wfactivity_enable_lock = LS_DB_WORKFLOW_TMP.wfactivity_enable_lock,LS_DB_WORKFLOW.wfactivity_enable_approve = LS_DB_WORKFLOW_TMP.wfactivity_enable_approve,LS_DB_WORKFLOW.wfactivity_enable_aesi = LS_DB_WORKFLOW_TMP.wfactivity_enable_aesi,LS_DB_WORKFLOW.wfactivity_e2b_dtd = LS_DB_WORKFLOW_TMP.wfactivity_e2b_dtd,LS_DB_WORKFLOW.wfactivity_dup_check_on_save = LS_DB_WORKFLOW_TMP.wfactivity_dup_check_on_save,LS_DB_WORKFLOW.wfactivity_dup_check_on_ca = LS_DB_WORKFLOW_TMP.wfactivity_dup_check_on_ca,LS_DB_WORKFLOW.wfactivity_drug_due_date = LS_DB_WORKFLOW_TMP.wfactivity_drug_due_date,LS_DB_WORKFLOW.wfactivity_dispose_to_irt = LS_DB_WORKFLOW_TMP.wfactivity_dispose_to_irt,LS_DB_WORKFLOW.wfactivity_display_order = LS_DB_WORKFLOW_TMP.wfactivity_display_order,LS_DB_WORKFLOW.wfactivity_description = LS_DB_WORKFLOW_TMP.wfactivity_description,LS_DB_WORKFLOW.wfactivity_decision_rule_id = LS_DB_WORKFLOW_TMP.wfactivity_decision_rule_id,LS_DB_WORKFLOW.wfactivity_decision_db_function = LS_DB_WORKFLOW_TMP.wfactivity_decision_db_function,LS_DB_WORKFLOW.wfactivity_date_modified = LS_DB_WORKFLOW_TMP.wfactivity_date_modified,LS_DB_WORKFLOW.wfactivity_date_created = LS_DB_WORKFLOW_TMP.wfactivity_date_created,LS_DB_WORKFLOW.wfactivity_custom_activity_check = LS_DB_WORKFLOW_TMP.wfactivity_custom_activity_check,LS_DB_WORKFLOW.wfactivity_completion_validation = LS_DB_WORKFLOW_TMP.wfactivity_completion_validation,LS_DB_WORKFLOW.wfactivity_compl_rule_id = LS_DB_WORKFLOW_TMP.wfactivity_compl_rule_id,LS_DB_WORKFLOW.wfactivity_compare_reconcile_follow_up = LS_DB_WORKFLOW_TMP.wfactivity_compare_reconcile_follow_up,LS_DB_WORKFLOW.wfactivity_company_remarks_rule = LS_DB_WORKFLOW_TMP.wfactivity_company_remarks_rule,LS_DB_WORKFLOW.wfactivity_company_remarks = LS_DB_WORKFLOW_TMP.wfactivity_company_remarks,LS_DB_WORKFLOW.wfactivity_close_inquiry = LS_DB_WORKFLOW_TMP.wfactivity_close_inquiry,LS_DB_WORKFLOW.wfactivity_check_lateness = LS_DB_WORKFLOW_TMP.wfactivity_check_lateness,LS_DB_WORKFLOW.wfactivity_bulk_routing = LS_DB_WORKFLOW_TMP.wfactivity_bulk_routing,LS_DB_WORKFLOW.wfactivity_bulk_assignment = LS_DB_WORKFLOW_TMP.wfactivity_bulk_assignment,LS_DB_WORKFLOW.wfactivity_bulk_analyze = LS_DB_WORKFLOW_TMP.wfactivity_bulk_analyze,LS_DB_WORKFLOW.wfactivity_bfc_enter_check = LS_DB_WORKFLOW_TMP.wfactivity_bfc_enter_check,LS_DB_WORKFLOW.wfactivity_automated_causality_methods = LS_DB_WORKFLOW_TMP.wfactivity_automated_causality_methods,LS_DB_WORKFLOW.wfactivity_auto_data_assessment = LS_DB_WORKFLOW_TMP.wfactivity_auto_data_assessment,LS_DB_WORKFLOW.wfactivity_auto_complete = LS_DB_WORKFLOW_TMP.wfactivity_auto_complete,LS_DB_WORKFLOW.wfactivity_auto_compl_rule_id = LS_DB_WORKFLOW_TMP.wfactivity_auto_compl_rule_id,LS_DB_WORKFLOW.wfactivity_ang_pregnancy_comments_rule = LS_DB_WORKFLOW_TMP.wfactivity_ang_pregnancy_comments_rule,LS_DB_WORKFLOW.wfactivity_ang_pregnancy_comments = LS_DB_WORKFLOW_TMP.wfactivity_ang_pregnancy_comments,LS_DB_WORKFLOW.wfactivity_ang_additional_comments_rule = LS_DB_WORKFLOW_TMP.wfactivity_ang_additional_comments_rule,LS_DB_WORKFLOW.wfactivity_ang_additional_comments = LS_DB_WORKFLOW_TMP.wfactivity_ang_additional_comments,LS_DB_WORKFLOW.wfactivity_always_case_edit = LS_DB_WORKFLOW_TMP.wfactivity_always_case_edit,LS_DB_WORKFLOW.wfactivity_allow_reject_case = LS_DB_WORKFLOW_TMP.wfactivity_allow_reject_case,LS_DB_WORKFLOW.wfactivity_allow_redistribute = LS_DB_WORKFLOW_TMP.wfactivity_allow_redistribute,LS_DB_WORKFLOW.wfactivity_allow_manual_lock = LS_DB_WORKFLOW_TMP.wfactivity_allow_manual_lock,LS_DB_WORKFLOW.wfactivity_allow_icsr_export = LS_DB_WORKFLOW_TMP.wfactivity_allow_icsr_export,LS_DB_WORKFLOW.wfactivity_allow_e2b_auto_assessment = LS_DB_WORKFLOW_TMP.wfactivity_allow_e2b_auto_assessment,LS_DB_WORKFLOW.wfactivity_allow_deletion = LS_DB_WORKFLOW_TMP.wfactivity_allow_deletion,LS_DB_WORKFLOW.wfactivity_allow_auto_assessment = LS_DB_WORKFLOW_TMP.wfactivity_allow_auto_assessment,LS_DB_WORKFLOW.wfactivity_add_distribution_contact = LS_DB_WORKFLOW_TMP.wfactivity_add_distribution_contact,LS_DB_WORKFLOW.parllwf_wf_rec_id = LS_DB_WORKFLOW_TMP.parllwf_wf_rec_id,LS_DB_WORKFLOW.parllwf_wf_instance_rec_id = LS_DB_WORKFLOW_TMP.parllwf_wf_instance_rec_id,LS_DB_WORKFLOW.parllwf_wf_completed = LS_DB_WORKFLOW_TMP.parllwf_wf_completed,LS_DB_WORKFLOW.parllwf_user_modified = LS_DB_WORKFLOW_TMP.parllwf_user_modified,LS_DB_WORKFLOW.parllwf_user_created = LS_DB_WORKFLOW_TMP.parllwf_user_created,LS_DB_WORKFLOW.parllwf_spr_id = LS_DB_WORKFLOW_TMP.parllwf_spr_id,LS_DB_WORKFLOW.parllwf_record_id = LS_DB_WORKFLOW_TMP.parllwf_record_id,LS_DB_WORKFLOW.parllwf_pwf_act_recid = LS_DB_WORKFLOW_TMP.parllwf_pwf_act_recid,LS_DB_WORKFLOW.parllwf_pwf_act_name = LS_DB_WORKFLOW_TMP.parllwf_pwf_act_name,LS_DB_WORKFLOW.parllwf_im_rec_id = LS_DB_WORKFLOW_TMP.parllwf_im_rec_id,LS_DB_WORKFLOW.parllwf_date_modified = LS_DB_WORKFLOW_TMP.parllwf_date_modified,LS_DB_WORKFLOW.parllwf_date_created = LS_DB_WORKFLOW_TMP.parllwf_date_created,LS_DB_WORKFLOW.parllwf_aer_approval_status = LS_DB_WORKFLOW_TMP.parllwf_aer_approval_status,LS_DB_WORKFLOW.parllwf_aer_approval_date = LS_DB_WORKFLOW_TMP.parllwf_aer_approval_date,
LS_DB_WORKFLOW.PROCESSING_DT = LS_DB_WORKFLOW_TMP.PROCESSING_DT,
LS_DB_WORKFLOW.receipt_id     =LS_DB_WORKFLOW_TMP.receipt_id    ,
LS_DB_WORKFLOW.case_no        =LS_DB_WORKFLOW_TMP.case_no           ,
LS_DB_WORKFLOW.case_version   =LS_DB_WORKFLOW_TMP.case_version      ,
LS_DB_WORKFLOW.version_no     =LS_DB_WORKFLOW_TMP.version_no        ,
LS_DB_WORKFLOW.user_modified  =LS_DB_WORKFLOW_TMP.user_modified     ,
LS_DB_WORKFLOW.date_modified  =LS_DB_WORKFLOW_TMP.date_modified     ,
LS_DB_WORKFLOW.expiry_date    =LS_DB_WORKFLOW_TMP.expiry_date       ,
LS_DB_WORKFLOW.created_by     =LS_DB_WORKFLOW_TMP.created_by        ,
LS_DB_WORKFLOW.created_dt     =LS_DB_WORKFLOW_TMP.created_dt        ,
LS_DB_WORKFLOW.load_ts        =LS_DB_WORKFLOW_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP 
WHERE 	LS_DB_WORKFLOW.INTEGRATION_ID = LS_DB_WORKFLOW_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_WORKFLOW_TMP.PROCESSING_DT = LS_DB_WORKFLOW.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_WORKFLOW.parllwf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.wfquality_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.workflow_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.wfactivity_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_WORKFLOW_TMP.parllwf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.wfquality_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.workflow_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.wfactivity_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_WORKFLOW.workflow_RECORD_ID ,LS_DB_WORKFLOW.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP 
ON LS_DB_WORKFLOW.workflow_RECORD_ID=LS_DB_WORKFLOW_TMP.workflow_RECORD_ID
AND LS_DB_WORKFLOW.INTEGRATION_ID = LS_DB_WORKFLOW_TMP.INTEGRATION_ID 
where LS_DB_WORKFLOW_TMP.INTEGRATION_ID  is null AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_WORKFLOW.workflow_RECORD_ID in (select workflow_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP )
) TMP where LS_DB_WORKFLOW.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_WORKFLOW.workflow_RECORD_ID ,LS_DB_WORKFLOW.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP 
    ON LS_DB_WORKFLOW.workflow_RECORD_ID=LS_DB_WORKFLOW_TMP.workflow_RECORD_ID
    AND LS_DB_WORKFLOW.INTEGRATION_ID = LS_DB_WORKFLOW_TMP.INTEGRATION_ID 
    where LS_DB_WORKFLOW_TMP.INTEGRATION_ID  is null AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_WORKFLOW.workflow_RECORD_ID in (select workflow_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW
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
integration_id ,workflow_wf_type,
workflow_wf_scheme,
workflow_wf_name,
workflow_wf_mxgraph_xml,
workflow_version,
workflow_user_modified,
workflow_user_created,
workflow_total_activities,
workflow_spr_id,
workflow_rel_version,
workflow_record_id,
workflow_process_definition,
workflow_parent_wf_rec_id,
workflow_object_id,
workflow_notes_flag,
workflow_multilingual_support,
workflow_lslp_wf_lang,
workflow_is_std,
workflow_description,
workflow_default_wf,
workflow_date_modified,
workflow_date_created,
workflow_child_wf,
workflow_active_wf,
wfquality_user_modified,
wfquality_user_created,
wfquality_threshold,
wfquality_spr_id,
wfquality_script_record_id,
wfquality_record_id,
wfquality_quality_flag,
wfquality_object_id,
wfquality_form_recid,
wfquality_fk_awfa_rec_id,
wfquality_date_modified,
wfquality_date_created,
wfactivity_wfa_data_assessment_type,
wfactivity_wf_affiliate_sub_due_date,
wfactivity_wf_activity_mxgraph_id,
wfactivity_wf_activity_mandtry,
wfactivity_wf_activity_id,
wfactivity_wait_for_parallel_wf,
wfactivity_version,
wfactivity_validations_on_exit,
wfactivity_validation_enabled,
wfactivity_user_modified,
wfactivity_user_created,
wfactivity_update_vta_synonyms,
wfactivity_type,
wfactivity_transition_validation,
wfactivity_sync_flag,
wfactivity_submit_uncoded_terms,
wfactivity_stop_sampling,
wfactivity_spr_id,
wfactivity_sequence,
wfactivity_screen,
wfactivity_sampling_rule_id,
wfactivity_restrict_dp_access,
wfactivity_removed_rule_id,
wfactivity_regenerate_follow_up,
wfactivity_record_id,
wfactivity_quality_check_form,
wfactivity_quality_check,
wfactivity_pre_investigation_activity,
wfactivity_pharmacovigilance_comments,
wfactivity_pharmacovigilance_cmt_rule,
wfactivity_parallel_wf,
wfactivity_onsuccess_transition_id,
wfactivity_onfailure_transition_id,
wfactivity_object_id,
wfactivity_notify_fu_complete_activity,
wfactivity_note_flag,
wfactivity_name,
wfactivity_move_oow,
wfactivity_move_create_new_version,
wfactivity_monitor_transitions_sub,
wfactivity_monitor_transitions,
wfactivity_medical_history_rule,
wfactivity_medical_history,
wfactivity_local_doc_view,
wfactivity_local_correspondence_view,
wfactivity_linked_status,
wfactivity_lateness_default_code,
wfactivity_jpn_noncase,
wfactivity_is_non_case_activity,
wfactivity_is_e2b_auto_assessment,
wfactivity_is_distribute_transit,
wfactivity_is_disposition,
wfactivity_is_delete_activity,
wfactivity_is_critical_path,
wfactivity_is_aose_search_on_save,
wfactivity_is_aose_search,
wfactivity_investigation_activity,
wfactivity_incl_wf_actvity,
wfactivity_if_new_e2b_case,
wfactivity_if_fu_e2b_case,
wfactivity_if_error_e2b_case,
wfactivity_if_dup_e2b_case,
wfactivity_icsr_relevency_for_dispo,
wfactivity_generate_narrative_rule,
wfactivity_generate_narrative,
wfactivity_generate_cioms,
wfactivity_generate_case_summary,
wfactivity_generate_case_score,
wfactivity_fu_case_auto_merge_rule,
wfactivity_fu_case_auto_merge,
wfactivity_fk_wfd_rec_id,
wfactivity_fk_jbpm_node_id,
wfactivity_fk_awf_rec_id,
wfactivity_enable_sub_tracking,
wfactivity_enable_lock,
wfactivity_enable_approve,
wfactivity_enable_aesi,
wfactivity_e2b_dtd,
wfactivity_dup_check_on_save,
wfactivity_dup_check_on_ca,
wfactivity_drug_due_date,
wfactivity_dispose_to_irt,
wfactivity_display_order,
wfactivity_description,
wfactivity_decision_rule_id,
wfactivity_decision_db_function,
wfactivity_date_modified,
wfactivity_date_created,
wfactivity_custom_activity_check,
wfactivity_completion_validation,
wfactivity_compl_rule_id,
wfactivity_compare_reconcile_follow_up,
wfactivity_company_remarks_rule,
wfactivity_company_remarks,
wfactivity_close_inquiry,
wfactivity_check_lateness,
wfactivity_bulk_routing,
wfactivity_bulk_assignment,
wfactivity_bulk_analyze,
wfactivity_bfc_enter_check,
wfactivity_automated_causality_methods,
wfactivity_auto_data_assessment,
wfactivity_auto_complete,
wfactivity_auto_compl_rule_id,
wfactivity_ang_pregnancy_comments_rule,
wfactivity_ang_pregnancy_comments,
wfactivity_ang_additional_comments_rule,
wfactivity_ang_additional_comments,
wfactivity_always_case_edit,
wfactivity_allow_reject_case,
wfactivity_allow_redistribute,
wfactivity_allow_manual_lock,
wfactivity_allow_icsr_export,
wfactivity_allow_e2b_auto_assessment,
wfactivity_allow_deletion,
wfactivity_allow_auto_assessment,
wfactivity_add_distribution_contact,
parllwf_wf_rec_id,
parllwf_wf_instance_rec_id,
parllwf_wf_completed,
parllwf_user_modified,
parllwf_user_created,
parllwf_spr_id,
parllwf_record_id,
parllwf_pwf_act_recid,
parllwf_pwf_act_name,
parllwf_im_rec_id,
parllwf_date_modified,
parllwf_date_created,
parllwf_aer_approval_status,
parllwf_aer_approval_date)
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
integration_id ,workflow_wf_type,
workflow_wf_scheme,
workflow_wf_name,
workflow_wf_mxgraph_xml,
workflow_version,
workflow_user_modified,
workflow_user_created,
workflow_total_activities,
workflow_spr_id,
workflow_rel_version,
workflow_record_id,
workflow_process_definition,
workflow_parent_wf_rec_id,
workflow_object_id,
workflow_notes_flag,
workflow_multilingual_support,
workflow_lslp_wf_lang,
workflow_is_std,
workflow_description,
workflow_default_wf,
workflow_date_modified,
workflow_date_created,
workflow_child_wf,
workflow_active_wf,
wfquality_user_modified,
wfquality_user_created,
wfquality_threshold,
wfquality_spr_id,
wfquality_script_record_id,
wfquality_record_id,
wfquality_quality_flag,
wfquality_object_id,
wfquality_form_recid,
wfquality_fk_awfa_rec_id,
wfquality_date_modified,
wfquality_date_created,
wfactivity_wfa_data_assessment_type,
wfactivity_wf_affiliate_sub_due_date,
wfactivity_wf_activity_mxgraph_id,
wfactivity_wf_activity_mandtry,
wfactivity_wf_activity_id,
wfactivity_wait_for_parallel_wf,
wfactivity_version,
wfactivity_validations_on_exit,
wfactivity_validation_enabled,
wfactivity_user_modified,
wfactivity_user_created,
wfactivity_update_vta_synonyms,
wfactivity_type,
wfactivity_transition_validation,
wfactivity_sync_flag,
wfactivity_submit_uncoded_terms,
wfactivity_stop_sampling,
wfactivity_spr_id,
wfactivity_sequence,
wfactivity_screen,
wfactivity_sampling_rule_id,
wfactivity_restrict_dp_access,
wfactivity_removed_rule_id,
wfactivity_regenerate_follow_up,
wfactivity_record_id,
wfactivity_quality_check_form,
wfactivity_quality_check,
wfactivity_pre_investigation_activity,
wfactivity_pharmacovigilance_comments,
wfactivity_pharmacovigilance_cmt_rule,
wfactivity_parallel_wf,
wfactivity_onsuccess_transition_id,
wfactivity_onfailure_transition_id,
wfactivity_object_id,
wfactivity_notify_fu_complete_activity,
wfactivity_note_flag,
wfactivity_name,
wfactivity_move_oow,
wfactivity_move_create_new_version,
wfactivity_monitor_transitions_sub,
wfactivity_monitor_transitions,
wfactivity_medical_history_rule,
wfactivity_medical_history,
wfactivity_local_doc_view,
wfactivity_local_correspondence_view,
wfactivity_linked_status,
wfactivity_lateness_default_code,
wfactivity_jpn_noncase,
wfactivity_is_non_case_activity,
wfactivity_is_e2b_auto_assessment,
wfactivity_is_distribute_transit,
wfactivity_is_disposition,
wfactivity_is_delete_activity,
wfactivity_is_critical_path,
wfactivity_is_aose_search_on_save,
wfactivity_is_aose_search,
wfactivity_investigation_activity,
wfactivity_incl_wf_actvity,
wfactivity_if_new_e2b_case,
wfactivity_if_fu_e2b_case,
wfactivity_if_error_e2b_case,
wfactivity_if_dup_e2b_case,
wfactivity_icsr_relevency_for_dispo,
wfactivity_generate_narrative_rule,
wfactivity_generate_narrative,
wfactivity_generate_cioms,
wfactivity_generate_case_summary,
wfactivity_generate_case_score,
wfactivity_fu_case_auto_merge_rule,
wfactivity_fu_case_auto_merge,
wfactivity_fk_wfd_rec_id,
wfactivity_fk_jbpm_node_id,
wfactivity_fk_awf_rec_id,
wfactivity_enable_sub_tracking,
wfactivity_enable_lock,
wfactivity_enable_approve,
wfactivity_enable_aesi,
wfactivity_e2b_dtd,
wfactivity_dup_check_on_save,
wfactivity_dup_check_on_ca,
wfactivity_drug_due_date,
wfactivity_dispose_to_irt,
wfactivity_display_order,
wfactivity_description,
wfactivity_decision_rule_id,
wfactivity_decision_db_function,
wfactivity_date_modified,
wfactivity_date_created,
wfactivity_custom_activity_check,
wfactivity_completion_validation,
wfactivity_compl_rule_id,
wfactivity_compare_reconcile_follow_up,
wfactivity_company_remarks_rule,
wfactivity_company_remarks,
wfactivity_close_inquiry,
wfactivity_check_lateness,
wfactivity_bulk_routing,
wfactivity_bulk_assignment,
wfactivity_bulk_analyze,
wfactivity_bfc_enter_check,
wfactivity_automated_causality_methods,
wfactivity_auto_data_assessment,
wfactivity_auto_complete,
wfactivity_auto_compl_rule_id,
wfactivity_ang_pregnancy_comments_rule,
wfactivity_ang_pregnancy_comments,
wfactivity_ang_additional_comments_rule,
wfactivity_ang_additional_comments,
wfactivity_always_case_edit,
wfactivity_allow_reject_case,
wfactivity_allow_redistribute,
wfactivity_allow_manual_lock,
wfactivity_allow_icsr_export,
wfactivity_allow_e2b_auto_assessment,
wfactivity_allow_deletion,
wfactivity_allow_auto_assessment,
wfactivity_add_distribution_contact,
parllwf_wf_rec_id,
parllwf_wf_instance_rec_id,
parllwf_wf_completed,
parllwf_user_modified,
parllwf_user_created,
parllwf_spr_id,
parllwf_record_id,
parllwf_pwf_act_recid,
parllwf_pwf_act_name,
parllwf_im_rec_id,
parllwf_date_modified,
parllwf_date_created,
parllwf_aer_approval_status,
parllwf_aer_approval_date
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP 
WHERE 	TO_DATE(LS_DB_WORKFLOW.PROCESSING_DT) < TO_DATE(LS_DB_WORKFLOW_TMP.PROCESSING_DT)
AND LS_DB_WORKFLOW.INTEGRATION_ID = LS_DB_WORKFLOW_TMP.INTEGRATION_ID
AND LS_DB_WORKFLOW.workflow_RECORD_ID = LS_DB_WORKFLOW_TMP.workflow_RECORD_ID
AND LS_DB_WORKFLOW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_WORKFLOW.parllwf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.wfquality_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.workflow_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_WORKFLOW.wfactivity_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_WORKFLOW_TMP.parllwf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.wfquality_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.workflow_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_WORKFLOW_TMP.wfactivity_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW TGT
WHERE  ( parllwf_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_parallel_wf_details') OR wfactivity_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_activity') OR workflow_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow') OR wfquality_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_quality')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( parllwf_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_parallel_wf_details') OR wfactivity_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_activity') OR workflow_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow') OR wfquality_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WORKFLOW_DELETION_TMP  WHERE TABLE_NAME='lsmv_workflow_quality')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_WORKFLOW_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_WORKFLOW'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_WORKFLOW Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_WORKFLOW'),
	'LSDB','Case','LS_DB_WORKFLOW',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_WORKFLOW not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
