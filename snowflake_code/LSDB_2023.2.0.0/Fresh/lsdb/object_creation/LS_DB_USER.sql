
-- USE SCHEMA${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_USER()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_USER'),
                'LSDB','Case','LS_DB_USER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_USER' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_USER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_DELETION_TMP  As select RECORD_ID,'lsmv_user' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_user WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1','1','5014','5014')
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

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_user WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_USER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_user WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_USER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_user_SUBSET AS 
(
select * from 
    (SELECT  
    access_all_cu_ae  access_all_cu_ae,access_all_eligible_products  access_all_eligible_products,access_all_eligible_thera  access_all_eligible_thera,access_all_prod_ae  access_all_prod_ae,account_locked_date  account_locked_date,account_rec_id  account_rec_id,address  address,address1  address1,address2  address2,agrepo_documents  agrepo_documents,agrepo_faqs  agrepo_faqs,agrepo_search_documents  agrepo_search_documents,all_company_unit  all_company_unit,all_country_access  all_country_access,all_roles_list  all_roles_list,allow_chat  allow_chat,animate_charts  animate_charts,api_key  api_key,approval_country_for_cioms  approval_country_for_cioms,au_home_page_irt  au_home_page_irt,au_home_page_lslm  au_home_page_lslm,authorized_e2b_sender  authorized_e2b_sender,autosaveinterval  autosaveinterval,availability_status  availability_status,availability_status_ae  availability_status_ae,batch_print_limit  batch_print_limit,batch_print_running_count  batch_print_running_count,calllog_submitted_flag  calllog_submitted_flag,case_attributes_value  case_attributes_value,case_listing_access  case_listing_access,case_priority  case_priority,case_summary_sheet  case_summary_sheet,cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,chart_default_from_date  chart_default_from_date,chart_default_from_date_ae  chart_default_from_date_ae,chart_default_from_date_vet  chart_default_from_date_vet,chart_default_to_date  chart_default_to_date,chart_default_to_date_ae  chart_default_to_date_ae,chart_default_to_date_vet  chart_default_to_date_vet,chrt_dflt_frmdate_complaints  chrt_dflt_frmdate_complaints,chrt_dflt_todate_complaints  chrt_dflt_todate_complaints,city  city,code_list_ids  code_list_ids,company_unit_access_type  company_unit_access_type,company_unit_name  company_unit_name,company_unit_recid  company_unit_recid,complaint_assign_notify  complaint_assign_notify,complaint_default_activity  complaint_default_activity,complaints_contact_view  complaints_contact_view,consent_share  consent_share,countofsq  countofsq,country  country,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1' AND CD=CAST(country AS VARCHAR(100)) )country_de_ml , create_inquiries_on_behalf  create_inquiries_on_behalf,date_created  date_created,date_format  date_format,date_modified  date_modified,date_out_from  date_out_from,date_out_from_ae  date_out_from_ae,date_out_to  date_out_to,date_out_to_ae  date_out_to_ae,default_age_unit  default_age_unit,default_company_unit_rec_id  default_company_unit_rec_id,default_cu_rec_id  default_cu_rec_id,default_height_unit  default_height_unit,default_inq_activity  default_inq_activity,default_login_language  default_login_language,default_role  default_role,default_sender_id  default_sender_id,default_sub_contact_rec_id  default_sub_contact_rec_id,default_vet_activity  default_vet_activity,default_weight_unit  default_weight_unit,degree  degree,department  department,department_record_id  department_record_id,designation  designation,display_time_component  display_time_component,e2b_r2_tag  e2b_r2_tag,e2b_r3_tag  e2b_r3_tag,e_mail_id  e_mail_id,edit_vet_case  edit_vet_case,expand_ost_cases  expand_ost_cases,failed_access_count  failed_access_count,fax_area_code  fax_area_code,fax_cntry_code  fax_cntry_code,fax_number  fax_number,file_name  file_name,fk_acu_rec_id  fk_acu_rec_id,fk_als_rec_id  fk_als_rec_id,fk_company_unit_rec_id  fk_company_unit_rec_id,freeze_aer_num  freeze_aer_num,freeze_receipt_num  freeze_receipt_num,has_offline_access  has_offline_access,hmac_authenication  hmac_authenication,home_page  home_page,home_page_ae  home_page_ae,home_page_complaints  home_page_complaints,home_page_submisison  home_page_submisison,home_page_vaer  home_page_vaer,inactive_date  inactive_date,inbound_sort_order  inbound_sort_order,inbound_sort_order_type  inbound_sort_order_type,inquiry_contact_view  inquiry_contact_view,irt_default_wf_activity  irt_default_wf_activity,is_account_portfolio  is_account_portfolio,is_also_portal_user  is_also_portal_user,is_audit_required  is_audit_required,is_event_portfolio  is_event_portfolio,is_health_professional  is_health_professional,is_logged_in  is_logged_in,is_login_under_maintenance  is_login_under_maintenance,is_mobile_admin_user  is_mobile_admin_user,is_new_user  is_new_user,is_product_portfolio  is_product_portfolio,is_sso_user  is_sso_user,is_web_service_user  is_web_service_user,landing_page_pref  landing_page_pref,language_code  language_code,last_login_time  last_login_time,last_name  last_name,list_of_manager  list_of_manager,lit_das_sel_activity  lit_das_sel_activity,lit_das_sel_portfolio  lit_das_sel_portfolio,lit_das_sel_product  lit_das_sel_product,logon_not_allowed  logon_not_allowed,lslp_default_assign  lslp_default_assign,mail_notification  mail_notification,mail_notification_interval  mail_notification_interval,mail_notification_time  mail_notification_time,manager_flag  manager_flag,medical_info_offline_label  medical_info_offline_label,mhlw_device_tag  mhlw_device_tag,mhlw_regenerative_tag  mhlw_regenerative_tag,mi_assgin_notify  mi_assgin_notify,middle_name  middle_name,migration_flag  migration_flag,mobile_number  mobile_number,multidb_access  multidb_access,no_of_cases_per_day  no_of_cases_per_day,no_of_cases_per_day_ae  no_of_cases_per_day_ae,no_of_records_per_page  no_of_records_per_page,object_id  object_id,occupation  occupation,oe_default_sender_id  oe_default_sender_id,oe_form_rec_id  oe_form_rec_id,only_edit_filter  only_edit_filter,open_doc_auto  open_doc_auto,password  password,phone_area_code  phone_area_code,phone_cntry_code  phone_cntry_code,phone_no  phone_no,portal_ae_followup  portal_ae_followup,portal_ae_listing  portal_ae_listing,portal_case_details_view  portal_case_details_view,portal_delete_access  portal_delete_access,portal_landing_page  portal_landing_page,portal_req_rec_id  portal_req_rec_id,portal_study_manager  portal_study_manager,portal_vaer_followup  portal_vaer_followup,portal_vaer_listing  portal_vaer_listing,position  position,postal_code_brick  postal_code_brick,practice_name  practice_name,pri_sort_columns_complaints  pri_sort_columns_complaints,pri_sort_order_complaints  pri_sort_order_complaints,primary_sort_column_sub  primary_sort_column_sub,primary_sort_order_sub  primary_sort_order_sub,processing_unit_access_type  processing_unit_access_type,prod_portfolio  prod_portfolio,psw_reset_rmdr_sent_date  psw_reset_rmdr_sent_date,pv_case_draft_listing  pv_case_draft_listing,pv_commercial_user  pv_commercial_user,pv_global_user  pv_global_user,pwd_change_at_logon  pwd_change_at_logon,pwd_expiry_date  pwd_expiry_date,pwd_mail_sent_date  pwd_mail_sent_date,pwd_retry_count  pwd_retry_count,pwd_token  pwd_token,qc_review  qc_review,record_id  record_id,repo_privacy_policy  repo_privacy_policy,repo_termsof_use  repo_termsof_use,reporting_analytics_user  reporting_analytics_user,req_session_id  req_session_id,requester_category  requester_category,reset_pwd_sent_date  reset_pwd_sent_date,review_approve_notify  review_approve_notify,rptdefform  rptdefform,rptdeflang  rptdeflang,rptrecperpage  rptrecperpage,salt  salt,salutation  salutation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5014' AND CD=CAST(salutation AS VARCHAR(100)) )salutation_de_ml , sec_sort_columns_complaints  sec_sort_columns_complaints,sec_sort_order_complaints  sec_sort_order_complaints,secondary_sort_column  secondary_sort_column,secondary_sort_column_ae  secondary_sort_column_ae,secondary_sort_column_sub  secondary_sort_column_sub,secondary_sort_column_vet  secondary_sort_column_vet,secondary_sort_order  secondary_sort_order,secondary_sort_order_ae  secondary_sort_order_ae,secondary_sort_order_sub  secondary_sort_order_sub,secondary_sort_order_vet  secondary_sort_order_vet,security_answer  security_answer,security_question  security_question,signature  signature,signup_inquiry  signup_inquiry,signup_irt  signup_irt,signup_responder  signup_responder,signup_vaer  signup_vaer,sort_column_ae  sort_column_ae,sort_column_inquiry  sort_column_inquiry,sort_column_order_ae  sort_column_order_ae,sort_column_order_inquiry  sort_column_order_inquiry,sort_column_order_vet  sort_column_order_vet,sort_column_vet  sort_column_vet,spr_id  spr_id,sso_token_id  sso_token_id,state  state,sub_contact_id_access_type  sub_contact_id_access_type,sub_default_wf_activity  sub_default_wf_activity,sub_print_e2b_report  sub_print_e2b_report,sub_submitting_unit_access  sub_submitting_unit_access,submission_type  submission_type,temporary_lock  temporary_lock,ter_sort_columns_complaints  ter_sort_columns_complaints,ter_sort_order_complaints  ter_sort_order_complaints,tertiary_sort_column  tertiary_sort_column,tertiary_sort_column_ae  tertiary_sort_column_ae,tertiary_sort_column_sub  tertiary_sort_column_sub,tertiary_sort_column_vet  tertiary_sort_column_vet,tertiary_sort_order  tertiary_sort_order,tertiary_sort_order_ae  tertiary_sort_order_ae,tertiary_sort_order_sub  tertiary_sort_order_sub,tertiary_sort_order_vet  tertiary_sort_order_vet,time_format  time_format,time_zone  time_zone,user_active  user_active,user_auto_active  user_auto_active,user_created  user_created,user_employee_id  user_employee_id,user_id  user_id,user_job_role  user_job_role,user_modified  user_modified,user_name  user_name,user_name_in_kana  user_name_in_kana,user_name_in_kanji  user_name_in_kanji,user_region  user_region,user_type  user_type,version  version,vet_requester_category  vet_requester_category,view_all_calllogs  view_all_calllogs,view_all_inquiries  view_all_inquiries,zip_code  zip_code,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_user
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_DELETION_TMP  WHERE TABLE_NAME='lsmv_user')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_user_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_user_SUBSET.USER_CREATED CREATED_BY,lsmv_user_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_user_SUBSET.zip_code  ,lsmv_user_SUBSET.view_all_inquiries  ,lsmv_user_SUBSET.view_all_calllogs  ,lsmv_user_SUBSET.vet_requester_category  ,lsmv_user_SUBSET.version  ,lsmv_user_SUBSET.user_type  ,lsmv_user_SUBSET.user_region  ,lsmv_user_SUBSET.user_name_in_kanji  ,lsmv_user_SUBSET.user_name_in_kana  ,lsmv_user_SUBSET.user_name  ,lsmv_user_SUBSET.user_modified  ,lsmv_user_SUBSET.user_job_role  ,lsmv_user_SUBSET.user_id  ,lsmv_user_SUBSET.user_employee_id  ,lsmv_user_SUBSET.user_created  ,lsmv_user_SUBSET.user_auto_active  ,lsmv_user_SUBSET.user_active  ,lsmv_user_SUBSET.time_zone  ,lsmv_user_SUBSET.time_format  ,lsmv_user_SUBSET.tertiary_sort_order_vet  ,lsmv_user_SUBSET.tertiary_sort_order_sub  ,lsmv_user_SUBSET.tertiary_sort_order_ae  ,lsmv_user_SUBSET.tertiary_sort_order  ,lsmv_user_SUBSET.tertiary_sort_column_vet  ,lsmv_user_SUBSET.tertiary_sort_column_sub  ,lsmv_user_SUBSET.tertiary_sort_column_ae  ,lsmv_user_SUBSET.tertiary_sort_column  ,lsmv_user_SUBSET.ter_sort_order_complaints  ,lsmv_user_SUBSET.ter_sort_columns_complaints  ,lsmv_user_SUBSET.temporary_lock  ,lsmv_user_SUBSET.submission_type  ,lsmv_user_SUBSET.sub_submitting_unit_access  ,lsmv_user_SUBSET.sub_print_e2b_report  ,lsmv_user_SUBSET.sub_default_wf_activity  ,lsmv_user_SUBSET.sub_contact_id_access_type  ,lsmv_user_SUBSET.state  ,lsmv_user_SUBSET.sso_token_id  ,lsmv_user_SUBSET.spr_id  ,lsmv_user_SUBSET.sort_column_vet  ,lsmv_user_SUBSET.sort_column_order_vet  ,lsmv_user_SUBSET.sort_column_order_inquiry  ,lsmv_user_SUBSET.sort_column_order_ae  ,lsmv_user_SUBSET.sort_column_inquiry  ,lsmv_user_SUBSET.sort_column_ae  ,lsmv_user_SUBSET.signup_vaer  ,lsmv_user_SUBSET.signup_responder  ,lsmv_user_SUBSET.signup_irt  ,lsmv_user_SUBSET.signup_inquiry  ,lsmv_user_SUBSET.signature  ,lsmv_user_SUBSET.security_question  ,lsmv_user_SUBSET.security_answer  ,lsmv_user_SUBSET.secondary_sort_order_vet  ,lsmv_user_SUBSET.secondary_sort_order_sub  ,lsmv_user_SUBSET.secondary_sort_order_ae  ,lsmv_user_SUBSET.secondary_sort_order  ,lsmv_user_SUBSET.secondary_sort_column_vet  ,lsmv_user_SUBSET.secondary_sort_column_sub  ,lsmv_user_SUBSET.secondary_sort_column_ae  ,lsmv_user_SUBSET.secondary_sort_column  ,lsmv_user_SUBSET.sec_sort_order_complaints  ,lsmv_user_SUBSET.sec_sort_columns_complaints  ,lsmv_user_SUBSET.salutation_de_ml  ,lsmv_user_SUBSET.salutation  ,lsmv_user_SUBSET.salt  ,lsmv_user_SUBSET.rptrecperpage  ,lsmv_user_SUBSET.rptdeflang  ,lsmv_user_SUBSET.rptdefform  ,lsmv_user_SUBSET.review_approve_notify  ,lsmv_user_SUBSET.reset_pwd_sent_date  ,lsmv_user_SUBSET.requester_category  ,lsmv_user_SUBSET.req_session_id  ,lsmv_user_SUBSET.reporting_analytics_user  ,lsmv_user_SUBSET.repo_termsof_use  ,lsmv_user_SUBSET.repo_privacy_policy  ,lsmv_user_SUBSET.record_id  ,lsmv_user_SUBSET.qc_review  ,lsmv_user_SUBSET.pwd_token  ,lsmv_user_SUBSET.pwd_retry_count  ,lsmv_user_SUBSET.pwd_mail_sent_date  ,lsmv_user_SUBSET.pwd_expiry_date  ,lsmv_user_SUBSET.pwd_change_at_logon  ,lsmv_user_SUBSET.pv_global_user  ,lsmv_user_SUBSET.pv_commercial_user  ,lsmv_user_SUBSET.pv_case_draft_listing  ,lsmv_user_SUBSET.psw_reset_rmdr_sent_date  ,lsmv_user_SUBSET.prod_portfolio  ,lsmv_user_SUBSET.processing_unit_access_type  ,lsmv_user_SUBSET.primary_sort_order_sub  ,lsmv_user_SUBSET.primary_sort_column_sub  ,lsmv_user_SUBSET.pri_sort_order_complaints  ,lsmv_user_SUBSET.pri_sort_columns_complaints  ,lsmv_user_SUBSET.practice_name  ,lsmv_user_SUBSET.postal_code_brick  ,lsmv_user_SUBSET.position  ,lsmv_user_SUBSET.portal_vaer_listing  ,lsmv_user_SUBSET.portal_vaer_followup  ,lsmv_user_SUBSET.portal_study_manager  ,lsmv_user_SUBSET.portal_req_rec_id  ,lsmv_user_SUBSET.portal_landing_page  ,lsmv_user_SUBSET.portal_delete_access  ,lsmv_user_SUBSET.portal_case_details_view  ,lsmv_user_SUBSET.portal_ae_listing  ,lsmv_user_SUBSET.portal_ae_followup  ,lsmv_user_SUBSET.phone_no  ,lsmv_user_SUBSET.phone_cntry_code  ,lsmv_user_SUBSET.phone_area_code  ,lsmv_user_SUBSET.password  ,lsmv_user_SUBSET.open_doc_auto  ,lsmv_user_SUBSET.only_edit_filter  ,lsmv_user_SUBSET.oe_form_rec_id  ,lsmv_user_SUBSET.oe_default_sender_id  ,lsmv_user_SUBSET.occupation  ,lsmv_user_SUBSET.object_id  ,lsmv_user_SUBSET.no_of_records_per_page  ,lsmv_user_SUBSET.no_of_cases_per_day_ae  ,lsmv_user_SUBSET.no_of_cases_per_day  ,lsmv_user_SUBSET.multidb_access  ,lsmv_user_SUBSET.mobile_number  ,lsmv_user_SUBSET.migration_flag  ,lsmv_user_SUBSET.middle_name  ,lsmv_user_SUBSET.mi_assgin_notify  ,lsmv_user_SUBSET.mhlw_regenerative_tag  ,lsmv_user_SUBSET.mhlw_device_tag  ,lsmv_user_SUBSET.medical_info_offline_label  ,lsmv_user_SUBSET.manager_flag  ,lsmv_user_SUBSET.mail_notification_time  ,lsmv_user_SUBSET.mail_notification_interval  ,lsmv_user_SUBSET.mail_notification  ,lsmv_user_SUBSET.lslp_default_assign  ,lsmv_user_SUBSET.logon_not_allowed  ,lsmv_user_SUBSET.lit_das_sel_product  ,lsmv_user_SUBSET.lit_das_sel_portfolio  ,lsmv_user_SUBSET.lit_das_sel_activity  ,lsmv_user_SUBSET.list_of_manager  ,lsmv_user_SUBSET.last_name  ,lsmv_user_SUBSET.last_login_time  ,lsmv_user_SUBSET.language_code  ,lsmv_user_SUBSET.landing_page_pref  ,lsmv_user_SUBSET.is_web_service_user  ,lsmv_user_SUBSET.is_sso_user  ,lsmv_user_SUBSET.is_product_portfolio  ,lsmv_user_SUBSET.is_new_user  ,lsmv_user_SUBSET.is_mobile_admin_user  ,lsmv_user_SUBSET.is_login_under_maintenance  ,lsmv_user_SUBSET.is_logged_in  ,lsmv_user_SUBSET.is_health_professional  ,lsmv_user_SUBSET.is_event_portfolio  ,lsmv_user_SUBSET.is_audit_required  ,lsmv_user_SUBSET.is_also_portal_user  ,lsmv_user_SUBSET.is_account_portfolio  ,lsmv_user_SUBSET.irt_default_wf_activity  ,lsmv_user_SUBSET.inquiry_contact_view  ,lsmv_user_SUBSET.inbound_sort_order_type  ,lsmv_user_SUBSET.inbound_sort_order  ,lsmv_user_SUBSET.inactive_date  ,lsmv_user_SUBSET.home_page_vaer  ,lsmv_user_SUBSET.home_page_submisison  ,lsmv_user_SUBSET.home_page_complaints  ,lsmv_user_SUBSET.home_page_ae  ,lsmv_user_SUBSET.home_page  ,lsmv_user_SUBSET.hmac_authenication  ,lsmv_user_SUBSET.has_offline_access  ,lsmv_user_SUBSET.freeze_receipt_num  ,lsmv_user_SUBSET.freeze_aer_num  ,lsmv_user_SUBSET.fk_company_unit_rec_id  ,lsmv_user_SUBSET.fk_als_rec_id  ,lsmv_user_SUBSET.fk_acu_rec_id  ,lsmv_user_SUBSET.file_name  ,lsmv_user_SUBSET.fax_number  ,lsmv_user_SUBSET.fax_cntry_code  ,lsmv_user_SUBSET.fax_area_code  ,lsmv_user_SUBSET.failed_access_count  ,lsmv_user_SUBSET.expand_ost_cases  ,lsmv_user_SUBSET.edit_vet_case  ,lsmv_user_SUBSET.e_mail_id  ,lsmv_user_SUBSET.e2b_r3_tag  ,lsmv_user_SUBSET.e2b_r2_tag  ,lsmv_user_SUBSET.display_time_component  ,lsmv_user_SUBSET.designation  ,lsmv_user_SUBSET.department_record_id  ,lsmv_user_SUBSET.department  ,lsmv_user_SUBSET.degree  ,lsmv_user_SUBSET.default_weight_unit  ,lsmv_user_SUBSET.default_vet_activity  ,lsmv_user_SUBSET.default_sub_contact_rec_id  ,lsmv_user_SUBSET.default_sender_id  ,lsmv_user_SUBSET.default_role  ,lsmv_user_SUBSET.default_login_language  ,lsmv_user_SUBSET.default_inq_activity  ,lsmv_user_SUBSET.default_height_unit  ,lsmv_user_SUBSET.default_cu_rec_id  ,lsmv_user_SUBSET.default_company_unit_rec_id  ,lsmv_user_SUBSET.default_age_unit  ,lsmv_user_SUBSET.date_out_to_ae  ,lsmv_user_SUBSET.date_out_to  ,lsmv_user_SUBSET.date_out_from_ae  ,lsmv_user_SUBSET.date_out_from  ,lsmv_user_SUBSET.date_modified  ,lsmv_user_SUBSET.date_format  ,lsmv_user_SUBSET.date_created  ,lsmv_user_SUBSET.create_inquiries_on_behalf  ,lsmv_user_SUBSET.country_de_ml  ,lsmv_user_SUBSET.country  ,lsmv_user_SUBSET.countofsq  ,lsmv_user_SUBSET.consent_share  ,lsmv_user_SUBSET.complaints_contact_view  ,lsmv_user_SUBSET.complaint_default_activity  ,lsmv_user_SUBSET.complaint_assign_notify  ,lsmv_user_SUBSET.company_unit_recid  ,lsmv_user_SUBSET.company_unit_name  ,lsmv_user_SUBSET.company_unit_access_type  ,lsmv_user_SUBSET.code_list_ids  ,lsmv_user_SUBSET.city  ,lsmv_user_SUBSET.chrt_dflt_todate_complaints  ,lsmv_user_SUBSET.chrt_dflt_frmdate_complaints  ,lsmv_user_SUBSET.chart_default_to_date_vet  ,lsmv_user_SUBSET.chart_default_to_date_ae  ,lsmv_user_SUBSET.chart_default_to_date  ,lsmv_user_SUBSET.chart_default_from_date_vet  ,lsmv_user_SUBSET.chart_default_from_date_ae  ,lsmv_user_SUBSET.chart_default_from_date  ,lsmv_user_SUBSET.cdc_operation_type  ,lsmv_user_SUBSET.cdc_operation_time  ,lsmv_user_SUBSET.case_summary_sheet  ,lsmv_user_SUBSET.case_priority  ,lsmv_user_SUBSET.case_listing_access  ,lsmv_user_SUBSET.case_attributes_value  ,lsmv_user_SUBSET.calllog_submitted_flag  ,lsmv_user_SUBSET.batch_print_running_count  ,lsmv_user_SUBSET.batch_print_limit  ,lsmv_user_SUBSET.availability_status_ae  ,lsmv_user_SUBSET.availability_status  ,lsmv_user_SUBSET.autosaveinterval  ,lsmv_user_SUBSET.authorized_e2b_sender  ,lsmv_user_SUBSET.au_home_page_lslm  ,lsmv_user_SUBSET.au_home_page_irt  ,lsmv_user_SUBSET.approval_country_for_cioms  ,lsmv_user_SUBSET.api_key  ,lsmv_user_SUBSET.animate_charts  ,lsmv_user_SUBSET.allow_chat  ,lsmv_user_SUBSET.all_roles_list  ,lsmv_user_SUBSET.all_country_access  ,lsmv_user_SUBSET.all_company_unit  ,lsmv_user_SUBSET.agrepo_search_documents  ,lsmv_user_SUBSET.agrepo_faqs  ,lsmv_user_SUBSET.agrepo_documents  ,lsmv_user_SUBSET.address2  ,lsmv_user_SUBSET.address1  ,lsmv_user_SUBSET.address  ,lsmv_user_SUBSET.account_rec_id  ,lsmv_user_SUBSET.account_locked_date  ,lsmv_user_SUBSET.access_all_prod_ae  ,lsmv_user_SUBSET.access_all_eligible_thera  ,lsmv_user_SUBSET.access_all_eligible_products  ,lsmv_user_SUBSET.access_all_cu_ae ,CONCAT( NVL(lsmv_user_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_user_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP)
where target_table_name='LS_DB_USER'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER   
SET LS_DB_USER.zip_code = LS_DB_USER_TMP.zip_code,LS_DB_USER.view_all_inquiries = LS_DB_USER_TMP.view_all_inquiries,LS_DB_USER.view_all_calllogs = LS_DB_USER_TMP.view_all_calllogs,LS_DB_USER.vet_requester_category = LS_DB_USER_TMP.vet_requester_category,LS_DB_USER.version = LS_DB_USER_TMP.version,LS_DB_USER.user_type = LS_DB_USER_TMP.user_type,LS_DB_USER.user_region = LS_DB_USER_TMP.user_region,LS_DB_USER.user_name_in_kanji = LS_DB_USER_TMP.user_name_in_kanji,LS_DB_USER.user_name_in_kana = LS_DB_USER_TMP.user_name_in_kana,LS_DB_USER.user_name = LS_DB_USER_TMP.user_name,LS_DB_USER.user_modified = LS_DB_USER_TMP.user_modified,LS_DB_USER.user_job_role = LS_DB_USER_TMP.user_job_role,LS_DB_USER.user_id = LS_DB_USER_TMP.user_id,LS_DB_USER.user_employee_id = LS_DB_USER_TMP.user_employee_id,LS_DB_USER.user_created = LS_DB_USER_TMP.user_created,LS_DB_USER.user_auto_active = LS_DB_USER_TMP.user_auto_active,LS_DB_USER.user_active = LS_DB_USER_TMP.user_active,LS_DB_USER.time_zone = LS_DB_USER_TMP.time_zone,LS_DB_USER.time_format = LS_DB_USER_TMP.time_format,LS_DB_USER.tertiary_sort_order_vet = LS_DB_USER_TMP.tertiary_sort_order_vet,LS_DB_USER.tertiary_sort_order_sub = LS_DB_USER_TMP.tertiary_sort_order_sub,LS_DB_USER.tertiary_sort_order_ae = LS_DB_USER_TMP.tertiary_sort_order_ae,LS_DB_USER.tertiary_sort_order = LS_DB_USER_TMP.tertiary_sort_order,LS_DB_USER.tertiary_sort_column_vet = LS_DB_USER_TMP.tertiary_sort_column_vet,LS_DB_USER.tertiary_sort_column_sub = LS_DB_USER_TMP.tertiary_sort_column_sub,LS_DB_USER.tertiary_sort_column_ae = LS_DB_USER_TMP.tertiary_sort_column_ae,LS_DB_USER.tertiary_sort_column = LS_DB_USER_TMP.tertiary_sort_column,LS_DB_USER.ter_sort_order_complaints = LS_DB_USER_TMP.ter_sort_order_complaints,LS_DB_USER.ter_sort_columns_complaints = LS_DB_USER_TMP.ter_sort_columns_complaints,LS_DB_USER.temporary_lock = LS_DB_USER_TMP.temporary_lock,LS_DB_USER.submission_type = LS_DB_USER_TMP.submission_type,LS_DB_USER.sub_submitting_unit_access = LS_DB_USER_TMP.sub_submitting_unit_access,LS_DB_USER.sub_print_e2b_report = LS_DB_USER_TMP.sub_print_e2b_report,LS_DB_USER.sub_default_wf_activity = LS_DB_USER_TMP.sub_default_wf_activity,LS_DB_USER.sub_contact_id_access_type = LS_DB_USER_TMP.sub_contact_id_access_type,LS_DB_USER.state = LS_DB_USER_TMP.state,LS_DB_USER.sso_token_id = LS_DB_USER_TMP.sso_token_id,LS_DB_USER.spr_id = LS_DB_USER_TMP.spr_id,LS_DB_USER.sort_column_vet = LS_DB_USER_TMP.sort_column_vet,LS_DB_USER.sort_column_order_vet = LS_DB_USER_TMP.sort_column_order_vet,LS_DB_USER.sort_column_order_inquiry = LS_DB_USER_TMP.sort_column_order_inquiry,LS_DB_USER.sort_column_order_ae = LS_DB_USER_TMP.sort_column_order_ae,LS_DB_USER.sort_column_inquiry = LS_DB_USER_TMP.sort_column_inquiry,LS_DB_USER.sort_column_ae = LS_DB_USER_TMP.sort_column_ae,LS_DB_USER.signup_vaer = LS_DB_USER_TMP.signup_vaer,LS_DB_USER.signup_responder = LS_DB_USER_TMP.signup_responder,LS_DB_USER.signup_irt = LS_DB_USER_TMP.signup_irt,LS_DB_USER.signup_inquiry = LS_DB_USER_TMP.signup_inquiry,LS_DB_USER.signature = LS_DB_USER_TMP.signature,LS_DB_USER.security_question = LS_DB_USER_TMP.security_question,LS_DB_USER.security_answer = LS_DB_USER_TMP.security_answer,LS_DB_USER.secondary_sort_order_vet = LS_DB_USER_TMP.secondary_sort_order_vet,LS_DB_USER.secondary_sort_order_sub = LS_DB_USER_TMP.secondary_sort_order_sub,LS_DB_USER.secondary_sort_order_ae = LS_DB_USER_TMP.secondary_sort_order_ae,LS_DB_USER.secondary_sort_order = LS_DB_USER_TMP.secondary_sort_order,LS_DB_USER.secondary_sort_column_vet = LS_DB_USER_TMP.secondary_sort_column_vet,LS_DB_USER.secondary_sort_column_sub = LS_DB_USER_TMP.secondary_sort_column_sub,LS_DB_USER.secondary_sort_column_ae = LS_DB_USER_TMP.secondary_sort_column_ae,LS_DB_USER.secondary_sort_column = LS_DB_USER_TMP.secondary_sort_column,LS_DB_USER.sec_sort_order_complaints = LS_DB_USER_TMP.sec_sort_order_complaints,LS_DB_USER.sec_sort_columns_complaints = LS_DB_USER_TMP.sec_sort_columns_complaints,LS_DB_USER.salutation_de_ml = LS_DB_USER_TMP.salutation_de_ml,LS_DB_USER.salutation = LS_DB_USER_TMP.salutation,LS_DB_USER.salt = LS_DB_USER_TMP.salt,LS_DB_USER.rptrecperpage = LS_DB_USER_TMP.rptrecperpage,LS_DB_USER.rptdeflang = LS_DB_USER_TMP.rptdeflang,LS_DB_USER.rptdefform = LS_DB_USER_TMP.rptdefform,LS_DB_USER.review_approve_notify = LS_DB_USER_TMP.review_approve_notify,LS_DB_USER.reset_pwd_sent_date = LS_DB_USER_TMP.reset_pwd_sent_date,LS_DB_USER.requester_category = LS_DB_USER_TMP.requester_category,LS_DB_USER.req_session_id = LS_DB_USER_TMP.req_session_id,LS_DB_USER.reporting_analytics_user = LS_DB_USER_TMP.reporting_analytics_user,LS_DB_USER.repo_termsof_use = LS_DB_USER_TMP.repo_termsof_use,LS_DB_USER.repo_privacy_policy = LS_DB_USER_TMP.repo_privacy_policy,LS_DB_USER.record_id = LS_DB_USER_TMP.record_id,LS_DB_USER.qc_review = LS_DB_USER_TMP.qc_review,LS_DB_USER.pwd_token = LS_DB_USER_TMP.pwd_token,LS_DB_USER.pwd_retry_count = LS_DB_USER_TMP.pwd_retry_count,LS_DB_USER.pwd_mail_sent_date = LS_DB_USER_TMP.pwd_mail_sent_date,LS_DB_USER.pwd_expiry_date = LS_DB_USER_TMP.pwd_expiry_date,LS_DB_USER.pwd_change_at_logon = LS_DB_USER_TMP.pwd_change_at_logon,LS_DB_USER.pv_global_user = LS_DB_USER_TMP.pv_global_user,LS_DB_USER.pv_commercial_user = LS_DB_USER_TMP.pv_commercial_user,LS_DB_USER.pv_case_draft_listing = LS_DB_USER_TMP.pv_case_draft_listing,LS_DB_USER.psw_reset_rmdr_sent_date = LS_DB_USER_TMP.psw_reset_rmdr_sent_date,LS_DB_USER.prod_portfolio = LS_DB_USER_TMP.prod_portfolio,LS_DB_USER.processing_unit_access_type = LS_DB_USER_TMP.processing_unit_access_type,LS_DB_USER.primary_sort_order_sub = LS_DB_USER_TMP.primary_sort_order_sub,LS_DB_USER.primary_sort_column_sub = LS_DB_USER_TMP.primary_sort_column_sub,LS_DB_USER.pri_sort_order_complaints = LS_DB_USER_TMP.pri_sort_order_complaints,LS_DB_USER.pri_sort_columns_complaints = LS_DB_USER_TMP.pri_sort_columns_complaints,LS_DB_USER.practice_name = LS_DB_USER_TMP.practice_name,LS_DB_USER.postal_code_brick = LS_DB_USER_TMP.postal_code_brick,LS_DB_USER.position = LS_DB_USER_TMP.position,LS_DB_USER.portal_vaer_listing = LS_DB_USER_TMP.portal_vaer_listing,LS_DB_USER.portal_vaer_followup = LS_DB_USER_TMP.portal_vaer_followup,LS_DB_USER.portal_study_manager = LS_DB_USER_TMP.portal_study_manager,LS_DB_USER.portal_req_rec_id = LS_DB_USER_TMP.portal_req_rec_id,LS_DB_USER.portal_landing_page = LS_DB_USER_TMP.portal_landing_page,LS_DB_USER.portal_delete_access = LS_DB_USER_TMP.portal_delete_access,LS_DB_USER.portal_case_details_view = LS_DB_USER_TMP.portal_case_details_view,LS_DB_USER.portal_ae_listing = LS_DB_USER_TMP.portal_ae_listing,LS_DB_USER.portal_ae_followup = LS_DB_USER_TMP.portal_ae_followup,LS_DB_USER.phone_no = LS_DB_USER_TMP.phone_no,LS_DB_USER.phone_cntry_code = LS_DB_USER_TMP.phone_cntry_code,LS_DB_USER.phone_area_code = LS_DB_USER_TMP.phone_area_code,LS_DB_USER.password = LS_DB_USER_TMP.password,LS_DB_USER.open_doc_auto = LS_DB_USER_TMP.open_doc_auto,LS_DB_USER.only_edit_filter = LS_DB_USER_TMP.only_edit_filter,LS_DB_USER.oe_form_rec_id = LS_DB_USER_TMP.oe_form_rec_id,LS_DB_USER.oe_default_sender_id = LS_DB_USER_TMP.oe_default_sender_id,LS_DB_USER.occupation = LS_DB_USER_TMP.occupation,LS_DB_USER.object_id = LS_DB_USER_TMP.object_id,LS_DB_USER.no_of_records_per_page = LS_DB_USER_TMP.no_of_records_per_page,LS_DB_USER.no_of_cases_per_day_ae = LS_DB_USER_TMP.no_of_cases_per_day_ae,LS_DB_USER.no_of_cases_per_day = LS_DB_USER_TMP.no_of_cases_per_day,LS_DB_USER.multidb_access = LS_DB_USER_TMP.multidb_access,LS_DB_USER.mobile_number = LS_DB_USER_TMP.mobile_number,LS_DB_USER.migration_flag = LS_DB_USER_TMP.migration_flag,LS_DB_USER.middle_name = LS_DB_USER_TMP.middle_name,LS_DB_USER.mi_assgin_notify = LS_DB_USER_TMP.mi_assgin_notify,LS_DB_USER.mhlw_regenerative_tag = LS_DB_USER_TMP.mhlw_regenerative_tag,LS_DB_USER.mhlw_device_tag = LS_DB_USER_TMP.mhlw_device_tag,LS_DB_USER.medical_info_offline_label = LS_DB_USER_TMP.medical_info_offline_label,LS_DB_USER.manager_flag = LS_DB_USER_TMP.manager_flag,LS_DB_USER.mail_notification_time = LS_DB_USER_TMP.mail_notification_time,LS_DB_USER.mail_notification_interval = LS_DB_USER_TMP.mail_notification_interval,LS_DB_USER.mail_notification = LS_DB_USER_TMP.mail_notification,LS_DB_USER.lslp_default_assign = LS_DB_USER_TMP.lslp_default_assign,LS_DB_USER.logon_not_allowed = LS_DB_USER_TMP.logon_not_allowed,LS_DB_USER.lit_das_sel_product = LS_DB_USER_TMP.lit_das_sel_product,LS_DB_USER.lit_das_sel_portfolio = LS_DB_USER_TMP.lit_das_sel_portfolio,LS_DB_USER.lit_das_sel_activity = LS_DB_USER_TMP.lit_das_sel_activity,LS_DB_USER.list_of_manager = LS_DB_USER_TMP.list_of_manager,LS_DB_USER.last_name = LS_DB_USER_TMP.last_name,LS_DB_USER.last_login_time = LS_DB_USER_TMP.last_login_time,LS_DB_USER.language_code = LS_DB_USER_TMP.language_code,LS_DB_USER.landing_page_pref = LS_DB_USER_TMP.landing_page_pref,LS_DB_USER.is_web_service_user = LS_DB_USER_TMP.is_web_service_user,LS_DB_USER.is_sso_user = LS_DB_USER_TMP.is_sso_user,LS_DB_USER.is_product_portfolio = LS_DB_USER_TMP.is_product_portfolio,LS_DB_USER.is_new_user = LS_DB_USER_TMP.is_new_user,LS_DB_USER.is_mobile_admin_user = LS_DB_USER_TMP.is_mobile_admin_user,LS_DB_USER.is_login_under_maintenance = LS_DB_USER_TMP.is_login_under_maintenance,LS_DB_USER.is_logged_in = LS_DB_USER_TMP.is_logged_in,LS_DB_USER.is_health_professional = LS_DB_USER_TMP.is_health_professional,LS_DB_USER.is_event_portfolio = LS_DB_USER_TMP.is_event_portfolio,LS_DB_USER.is_audit_required = LS_DB_USER_TMP.is_audit_required,LS_DB_USER.is_also_portal_user = LS_DB_USER_TMP.is_also_portal_user,LS_DB_USER.is_account_portfolio = LS_DB_USER_TMP.is_account_portfolio,LS_DB_USER.irt_default_wf_activity = LS_DB_USER_TMP.irt_default_wf_activity,LS_DB_USER.inquiry_contact_view = LS_DB_USER_TMP.inquiry_contact_view,LS_DB_USER.inbound_sort_order_type = LS_DB_USER_TMP.inbound_sort_order_type,LS_DB_USER.inbound_sort_order = LS_DB_USER_TMP.inbound_sort_order,LS_DB_USER.inactive_date = LS_DB_USER_TMP.inactive_date,LS_DB_USER.home_page_vaer = LS_DB_USER_TMP.home_page_vaer,LS_DB_USER.home_page_submisison = LS_DB_USER_TMP.home_page_submisison,LS_DB_USER.home_page_complaints = LS_DB_USER_TMP.home_page_complaints,LS_DB_USER.home_page_ae = LS_DB_USER_TMP.home_page_ae,LS_DB_USER.home_page = LS_DB_USER_TMP.home_page,LS_DB_USER.hmac_authenication = LS_DB_USER_TMP.hmac_authenication,LS_DB_USER.has_offline_access = LS_DB_USER_TMP.has_offline_access,LS_DB_USER.freeze_receipt_num = LS_DB_USER_TMP.freeze_receipt_num,LS_DB_USER.freeze_aer_num = LS_DB_USER_TMP.freeze_aer_num,LS_DB_USER.fk_company_unit_rec_id = LS_DB_USER_TMP.fk_company_unit_rec_id,LS_DB_USER.fk_als_rec_id = LS_DB_USER_TMP.fk_als_rec_id,LS_DB_USER.fk_acu_rec_id = LS_DB_USER_TMP.fk_acu_rec_id,LS_DB_USER.file_name = LS_DB_USER_TMP.file_name,LS_DB_USER.fax_number = LS_DB_USER_TMP.fax_number,LS_DB_USER.fax_cntry_code = LS_DB_USER_TMP.fax_cntry_code,LS_DB_USER.fax_area_code = LS_DB_USER_TMP.fax_area_code,LS_DB_USER.failed_access_count = LS_DB_USER_TMP.failed_access_count,LS_DB_USER.expand_ost_cases = LS_DB_USER_TMP.expand_ost_cases,LS_DB_USER.edit_vet_case = LS_DB_USER_TMP.edit_vet_case,LS_DB_USER.e_mail_id = LS_DB_USER_TMP.e_mail_id,LS_DB_USER.e2b_r3_tag = LS_DB_USER_TMP.e2b_r3_tag,LS_DB_USER.e2b_r2_tag = LS_DB_USER_TMP.e2b_r2_tag,LS_DB_USER.display_time_component = LS_DB_USER_TMP.display_time_component,LS_DB_USER.designation = LS_DB_USER_TMP.designation,LS_DB_USER.department_record_id = LS_DB_USER_TMP.department_record_id,LS_DB_USER.department = LS_DB_USER_TMP.department,LS_DB_USER.degree = LS_DB_USER_TMP.degree,LS_DB_USER.default_weight_unit = LS_DB_USER_TMP.default_weight_unit,LS_DB_USER.default_vet_activity = LS_DB_USER_TMP.default_vet_activity,LS_DB_USER.default_sub_contact_rec_id = LS_DB_USER_TMP.default_sub_contact_rec_id,LS_DB_USER.default_sender_id = LS_DB_USER_TMP.default_sender_id,LS_DB_USER.default_role = LS_DB_USER_TMP.default_role,LS_DB_USER.default_login_language = LS_DB_USER_TMP.default_login_language,LS_DB_USER.default_inq_activity = LS_DB_USER_TMP.default_inq_activity,LS_DB_USER.default_height_unit = LS_DB_USER_TMP.default_height_unit,LS_DB_USER.default_cu_rec_id = LS_DB_USER_TMP.default_cu_rec_id,LS_DB_USER.default_company_unit_rec_id = LS_DB_USER_TMP.default_company_unit_rec_id,LS_DB_USER.default_age_unit = LS_DB_USER_TMP.default_age_unit,LS_DB_USER.date_out_to_ae = LS_DB_USER_TMP.date_out_to_ae,LS_DB_USER.date_out_to = LS_DB_USER_TMP.date_out_to,LS_DB_USER.date_out_from_ae = LS_DB_USER_TMP.date_out_from_ae,LS_DB_USER.date_out_from = LS_DB_USER_TMP.date_out_from,LS_DB_USER.date_modified = LS_DB_USER_TMP.date_modified,LS_DB_USER.date_format = LS_DB_USER_TMP.date_format,LS_DB_USER.date_created = LS_DB_USER_TMP.date_created,LS_DB_USER.create_inquiries_on_behalf = LS_DB_USER_TMP.create_inquiries_on_behalf,LS_DB_USER.country_de_ml = LS_DB_USER_TMP.country_de_ml,LS_DB_USER.country = LS_DB_USER_TMP.country,LS_DB_USER.countofsq = LS_DB_USER_TMP.countofsq,LS_DB_USER.consent_share = LS_DB_USER_TMP.consent_share,LS_DB_USER.complaints_contact_view = LS_DB_USER_TMP.complaints_contact_view,LS_DB_USER.complaint_default_activity = LS_DB_USER_TMP.complaint_default_activity,LS_DB_USER.complaint_assign_notify = LS_DB_USER_TMP.complaint_assign_notify,LS_DB_USER.company_unit_recid = LS_DB_USER_TMP.company_unit_recid,LS_DB_USER.company_unit_name = LS_DB_USER_TMP.company_unit_name,LS_DB_USER.company_unit_access_type = LS_DB_USER_TMP.company_unit_access_type,LS_DB_USER.code_list_ids = LS_DB_USER_TMP.code_list_ids,LS_DB_USER.city = LS_DB_USER_TMP.city,LS_DB_USER.chrt_dflt_todate_complaints = LS_DB_USER_TMP.chrt_dflt_todate_complaints,LS_DB_USER.chrt_dflt_frmdate_complaints = LS_DB_USER_TMP.chrt_dflt_frmdate_complaints,LS_DB_USER.chart_default_to_date_vet = LS_DB_USER_TMP.chart_default_to_date_vet,LS_DB_USER.chart_default_to_date_ae = LS_DB_USER_TMP.chart_default_to_date_ae,LS_DB_USER.chart_default_to_date = LS_DB_USER_TMP.chart_default_to_date,LS_DB_USER.chart_default_from_date_vet = LS_DB_USER_TMP.chart_default_from_date_vet,LS_DB_USER.chart_default_from_date_ae = LS_DB_USER_TMP.chart_default_from_date_ae,LS_DB_USER.chart_default_from_date = LS_DB_USER_TMP.chart_default_from_date,LS_DB_USER.cdc_operation_type = LS_DB_USER_TMP.cdc_operation_type,LS_DB_USER.cdc_operation_time = LS_DB_USER_TMP.cdc_operation_time,LS_DB_USER.case_summary_sheet = LS_DB_USER_TMP.case_summary_sheet,LS_DB_USER.case_priority = LS_DB_USER_TMP.case_priority,LS_DB_USER.case_listing_access = LS_DB_USER_TMP.case_listing_access,LS_DB_USER.case_attributes_value = LS_DB_USER_TMP.case_attributes_value,LS_DB_USER.calllog_submitted_flag = LS_DB_USER_TMP.calllog_submitted_flag,LS_DB_USER.batch_print_running_count = LS_DB_USER_TMP.batch_print_running_count,LS_DB_USER.batch_print_limit = LS_DB_USER_TMP.batch_print_limit,LS_DB_USER.availability_status_ae = LS_DB_USER_TMP.availability_status_ae,LS_DB_USER.availability_status = LS_DB_USER_TMP.availability_status,LS_DB_USER.autosaveinterval = LS_DB_USER_TMP.autosaveinterval,LS_DB_USER.authorized_e2b_sender = LS_DB_USER_TMP.authorized_e2b_sender,LS_DB_USER.au_home_page_lslm = LS_DB_USER_TMP.au_home_page_lslm,LS_DB_USER.au_home_page_irt = LS_DB_USER_TMP.au_home_page_irt,LS_DB_USER.approval_country_for_cioms = LS_DB_USER_TMP.approval_country_for_cioms,LS_DB_USER.api_key = LS_DB_USER_TMP.api_key,LS_DB_USER.animate_charts = LS_DB_USER_TMP.animate_charts,LS_DB_USER.allow_chat = LS_DB_USER_TMP.allow_chat,LS_DB_USER.all_roles_list = LS_DB_USER_TMP.all_roles_list,LS_DB_USER.all_country_access = LS_DB_USER_TMP.all_country_access,LS_DB_USER.all_company_unit = LS_DB_USER_TMP.all_company_unit,LS_DB_USER.agrepo_search_documents = LS_DB_USER_TMP.agrepo_search_documents,LS_DB_USER.agrepo_faqs = LS_DB_USER_TMP.agrepo_faqs,LS_DB_USER.agrepo_documents = LS_DB_USER_TMP.agrepo_documents,LS_DB_USER.address2 = LS_DB_USER_TMP.address2,LS_DB_USER.address1 = LS_DB_USER_TMP.address1,LS_DB_USER.address = LS_DB_USER_TMP.address,LS_DB_USER.account_rec_id = LS_DB_USER_TMP.account_rec_id,LS_DB_USER.account_locked_date = LS_DB_USER_TMP.account_locked_date,LS_DB_USER.access_all_prod_ae = LS_DB_USER_TMP.access_all_prod_ae,LS_DB_USER.access_all_eligible_thera = LS_DB_USER_TMP.access_all_eligible_thera,LS_DB_USER.access_all_eligible_products = LS_DB_USER_TMP.access_all_eligible_products,LS_DB_USER.access_all_cu_ae = LS_DB_USER_TMP.access_all_cu_ae,
LS_DB_USER.PROCESSING_DT = LS_DB_USER_TMP.PROCESSING_DT ,
LS_DB_USER.expiry_date    =LS_DB_USER_TMP.expiry_date       ,
LS_DB_USER.created_by     =LS_DB_USER_TMP.created_by        ,
LS_DB_USER.created_dt     =LS_DB_USER_TMP.created_dt        ,
LS_DB_USER.load_ts        =LS_DB_USER_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP 
WHERE LS_DB_USER.INTEGRATION_ID = LS_DB_USER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_USER_TMP.PROCESSING_DT = LS_DB_USER.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,zip_code,
view_all_inquiries,
view_all_calllogs,
vet_requester_category,
version,
user_type,
user_region,
user_name_in_kanji,
user_name_in_kana,
user_name,
user_modified,
user_job_role,
user_id,
user_employee_id,
user_created,
user_auto_active,
user_active,
time_zone,
time_format,
tertiary_sort_order_vet,
tertiary_sort_order_sub,
tertiary_sort_order_ae,
tertiary_sort_order,
tertiary_sort_column_vet,
tertiary_sort_column_sub,
tertiary_sort_column_ae,
tertiary_sort_column,
ter_sort_order_complaints,
ter_sort_columns_complaints,
temporary_lock,
submission_type,
sub_submitting_unit_access,
sub_print_e2b_report,
sub_default_wf_activity,
sub_contact_id_access_type,
state,
sso_token_id,
spr_id,
sort_column_vet,
sort_column_order_vet,
sort_column_order_inquiry,
sort_column_order_ae,
sort_column_inquiry,
sort_column_ae,
signup_vaer,
signup_responder,
signup_irt,
signup_inquiry,
signature,
security_question,
security_answer,
secondary_sort_order_vet,
secondary_sort_order_sub,
secondary_sort_order_ae,
secondary_sort_order,
secondary_sort_column_vet,
secondary_sort_column_sub,
secondary_sort_column_ae,
secondary_sort_column,
sec_sort_order_complaints,
sec_sort_columns_complaints,
salutation_de_ml,
salutation,
salt,
rptrecperpage,
rptdeflang,
rptdefform,
review_approve_notify,
reset_pwd_sent_date,
requester_category,
req_session_id,
reporting_analytics_user,
repo_termsof_use,
repo_privacy_policy,
record_id,
qc_review,
pwd_token,
pwd_retry_count,
pwd_mail_sent_date,
pwd_expiry_date,
pwd_change_at_logon,
pv_global_user,
pv_commercial_user,
pv_case_draft_listing,
psw_reset_rmdr_sent_date,
prod_portfolio,
processing_unit_access_type,
primary_sort_order_sub,
primary_sort_column_sub,
pri_sort_order_complaints,
pri_sort_columns_complaints,
practice_name,
postal_code_brick,
position,
portal_vaer_listing,
portal_vaer_followup,
portal_study_manager,
portal_req_rec_id,
portal_landing_page,
portal_delete_access,
portal_case_details_view,
portal_ae_listing,
portal_ae_followup,
phone_no,
phone_cntry_code,
phone_area_code,
password,
open_doc_auto,
only_edit_filter,
oe_form_rec_id,
oe_default_sender_id,
occupation,
object_id,
no_of_records_per_page,
no_of_cases_per_day_ae,
no_of_cases_per_day,
multidb_access,
mobile_number,
migration_flag,
middle_name,
mi_assgin_notify,
mhlw_regenerative_tag,
mhlw_device_tag,
medical_info_offline_label,
manager_flag,
mail_notification_time,
mail_notification_interval,
mail_notification,
lslp_default_assign,
logon_not_allowed,
lit_das_sel_product,
lit_das_sel_portfolio,
lit_das_sel_activity,
list_of_manager,
last_name,
last_login_time,
language_code,
landing_page_pref,
is_web_service_user,
is_sso_user,
is_product_portfolio,
is_new_user,
is_mobile_admin_user,
is_login_under_maintenance,
is_logged_in,
is_health_professional,
is_event_portfolio,
is_audit_required,
is_also_portal_user,
is_account_portfolio,
irt_default_wf_activity,
inquiry_contact_view,
inbound_sort_order_type,
inbound_sort_order,
inactive_date,
home_page_vaer,
home_page_submisison,
home_page_complaints,
home_page_ae,
home_page,
hmac_authenication,
has_offline_access,
freeze_receipt_num,
freeze_aer_num,
fk_company_unit_rec_id,
fk_als_rec_id,
fk_acu_rec_id,
file_name,
fax_number,
fax_cntry_code,
fax_area_code,
failed_access_count,
expand_ost_cases,
edit_vet_case,
e_mail_id,
e2b_r3_tag,
e2b_r2_tag,
display_time_component,
designation,
department_record_id,
department,
degree,
default_weight_unit,
default_vet_activity,
default_sub_contact_rec_id,
default_sender_id,
default_role,
default_login_language,
default_inq_activity,
default_height_unit,
default_cu_rec_id,
default_company_unit_rec_id,
default_age_unit,
date_out_to_ae,
date_out_to,
date_out_from_ae,
date_out_from,
date_modified,
date_format,
date_created,
create_inquiries_on_behalf,
country_de_ml,
country,
countofsq,
consent_share,
complaints_contact_view,
complaint_default_activity,
complaint_assign_notify,
company_unit_recid,
company_unit_name,
company_unit_access_type,
code_list_ids,
city,
chrt_dflt_todate_complaints,
chrt_dflt_frmdate_complaints,
chart_default_to_date_vet,
chart_default_to_date_ae,
chart_default_to_date,
chart_default_from_date_vet,
chart_default_from_date_ae,
chart_default_from_date,
cdc_operation_type,
cdc_operation_time,
case_summary_sheet,
case_priority,
case_listing_access,
case_attributes_value,
calllog_submitted_flag,
batch_print_running_count,
batch_print_limit,
availability_status_ae,
availability_status,
autosaveinterval,
authorized_e2b_sender,
au_home_page_lslm,
au_home_page_irt,
approval_country_for_cioms,
api_key,
animate_charts,
allow_chat,
all_roles_list,
all_country_access,
all_company_unit,
agrepo_search_documents,
agrepo_faqs,
agrepo_documents,
address2,
address1,
address,
account_rec_id,
account_locked_date,
access_all_prod_ae,
access_all_eligible_thera,
access_all_eligible_products,
access_all_cu_ae)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,zip_code,
view_all_inquiries,
view_all_calllogs,
vet_requester_category,
version,
user_type,
user_region,
user_name_in_kanji,
user_name_in_kana,
user_name,
user_modified,
user_job_role,
user_id,
user_employee_id,
user_created,
user_auto_active,
user_active,
time_zone,
time_format,
tertiary_sort_order_vet,
tertiary_sort_order_sub,
tertiary_sort_order_ae,
tertiary_sort_order,
tertiary_sort_column_vet,
tertiary_sort_column_sub,
tertiary_sort_column_ae,
tertiary_sort_column,
ter_sort_order_complaints,
ter_sort_columns_complaints,
temporary_lock,
submission_type,
sub_submitting_unit_access,
sub_print_e2b_report,
sub_default_wf_activity,
sub_contact_id_access_type,
state,
sso_token_id,
spr_id,
sort_column_vet,
sort_column_order_vet,
sort_column_order_inquiry,
sort_column_order_ae,
sort_column_inquiry,
sort_column_ae,
signup_vaer,
signup_responder,
signup_irt,
signup_inquiry,
signature,
security_question,
security_answer,
secondary_sort_order_vet,
secondary_sort_order_sub,
secondary_sort_order_ae,
secondary_sort_order,
secondary_sort_column_vet,
secondary_sort_column_sub,
secondary_sort_column_ae,
secondary_sort_column,
sec_sort_order_complaints,
sec_sort_columns_complaints,
salutation_de_ml,
salutation,
salt,
rptrecperpage,
rptdeflang,
rptdefform,
review_approve_notify,
reset_pwd_sent_date,
requester_category,
req_session_id,
reporting_analytics_user,
repo_termsof_use,
repo_privacy_policy,
record_id,
qc_review,
pwd_token,
pwd_retry_count,
pwd_mail_sent_date,
pwd_expiry_date,
pwd_change_at_logon,
pv_global_user,
pv_commercial_user,
pv_case_draft_listing,
psw_reset_rmdr_sent_date,
prod_portfolio,
processing_unit_access_type,
primary_sort_order_sub,
primary_sort_column_sub,
pri_sort_order_complaints,
pri_sort_columns_complaints,
practice_name,
postal_code_brick,
position,
portal_vaer_listing,
portal_vaer_followup,
portal_study_manager,
portal_req_rec_id,
portal_landing_page,
portal_delete_access,
portal_case_details_view,
portal_ae_listing,
portal_ae_followup,
phone_no,
phone_cntry_code,
phone_area_code,
password,
open_doc_auto,
only_edit_filter,
oe_form_rec_id,
oe_default_sender_id,
occupation,
object_id,
no_of_records_per_page,
no_of_cases_per_day_ae,
no_of_cases_per_day,
multidb_access,
mobile_number,
migration_flag,
middle_name,
mi_assgin_notify,
mhlw_regenerative_tag,
mhlw_device_tag,
medical_info_offline_label,
manager_flag,
mail_notification_time,
mail_notification_interval,
mail_notification,
lslp_default_assign,
logon_not_allowed,
lit_das_sel_product,
lit_das_sel_portfolio,
lit_das_sel_activity,
list_of_manager,
last_name,
last_login_time,
language_code,
landing_page_pref,
is_web_service_user,
is_sso_user,
is_product_portfolio,
is_new_user,
is_mobile_admin_user,
is_login_under_maintenance,
is_logged_in,
is_health_professional,
is_event_portfolio,
is_audit_required,
is_also_portal_user,
is_account_portfolio,
irt_default_wf_activity,
inquiry_contact_view,
inbound_sort_order_type,
inbound_sort_order,
inactive_date,
home_page_vaer,
home_page_submisison,
home_page_complaints,
home_page_ae,
home_page,
hmac_authenication,
has_offline_access,
freeze_receipt_num,
freeze_aer_num,
fk_company_unit_rec_id,
fk_als_rec_id,
fk_acu_rec_id,
file_name,
fax_number,
fax_cntry_code,
fax_area_code,
failed_access_count,
expand_ost_cases,
edit_vet_case,
e_mail_id,
e2b_r3_tag,
e2b_r2_tag,
display_time_component,
designation,
department_record_id,
department,
degree,
default_weight_unit,
default_vet_activity,
default_sub_contact_rec_id,
default_sender_id,
default_role,
default_login_language,
default_inq_activity,
default_height_unit,
default_cu_rec_id,
default_company_unit_rec_id,
default_age_unit,
date_out_to_ae,
date_out_to,
date_out_from_ae,
date_out_from,
date_modified,
date_format,
date_created,
create_inquiries_on_behalf,
country_de_ml,
country,
countofsq,
consent_share,
complaints_contact_view,
complaint_default_activity,
complaint_assign_notify,
company_unit_recid,
company_unit_name,
company_unit_access_type,
code_list_ids,
city,
chrt_dflt_todate_complaints,
chrt_dflt_frmdate_complaints,
chart_default_to_date_vet,
chart_default_to_date_ae,
chart_default_to_date,
chart_default_from_date_vet,
chart_default_from_date_ae,
chart_default_from_date,
cdc_operation_type,
cdc_operation_time,
case_summary_sheet,
case_priority,
case_listing_access,
case_attributes_value,
calllog_submitted_flag,
batch_print_running_count,
batch_print_limit,
availability_status_ae,
availability_status,
autosaveinterval,
authorized_e2b_sender,
au_home_page_lslm,
au_home_page_irt,
approval_country_for_cioms,
api_key,
animate_charts,
allow_chat,
all_roles_list,
all_country_access,
all_company_unit,
agrepo_search_documents,
agrepo_faqs,
agrepo_documents,
address2,
address1,
address,
account_rec_id,
account_locked_date,
access_all_prod_ae,
access_all_eligible_thera,
access_all_eligible_products,
access_all_cu_ae
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP 
WHERE TO_DATE(LS_DB_USER.PROCESSING_DT) < TO_DATE(LS_DB_USER_TMP.PROCESSING_DT)
AND LS_DB_USER.INTEGRATION_ID = LS_DB_USER_TMP.INTEGRATION_ID
AND LS_DB_USER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_DELETION_TMP  WHERE TABLE_NAME='lsmv_user')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_USER_DELETION_TMP  WHERE TABLE_NAME='lsmv_user')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_USER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_USER'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_USER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_USER'),
                'LSDB','Case','LS_DB_USER',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_USER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
