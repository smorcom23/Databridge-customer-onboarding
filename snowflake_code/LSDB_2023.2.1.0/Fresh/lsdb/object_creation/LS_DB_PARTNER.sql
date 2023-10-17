
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PARTNER()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PARTNER'),
                'LSDB','Case','LS_DB_PARTNER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PARTNER' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PARTNER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARTNER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARTNER_DELETION_TMP  As select RECORD_ID,'lsmv_partner' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_partner WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9')
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

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_partner WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARTNER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_partner WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PARTNER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_partner_SUBSET AS 
(
select * from 
    (SELECT  
    account_for_holidays  account_for_holidays,account_for_weekend  account_for_weekend,ack_comments_length  ack_comments_length,ack_expected  ack_expected,ack_responder  ack_responder,ack_sub_res  ack_sub_res,acknowledgement  acknowledgement,acknowledgement_subject  acknowledgement_subject,address  address,agrepo_language_code  agrepo_language_code,agrepo_search_documents  agrepo_search_documents,all_department_assign  all_department_assign,all_product_assign  all_product_assign,all_product_class_assign  all_product_class_assign,allow_chat_access  allow_chat_access,allow_chat_access_mi  allow_chat_access_mi,allow_due_date_synch  allow_due_date_synch,allow_e2b_classification  allow_e2b_classification,allow_lde_synch  allow_lde_synch,assi_emial_frmt_rspd  assi_emial_frmt_rspd,assignment_emial_format  assignment_emial_format,atn_date_format  atn_date_format,atn_literal  atn_literal,atn_num_separation_char  atn_num_separation_char,atn_padding  atn_padding,atn_seq_generator  atn_seq_generator,authority  authority,auto_complete_first_activity  auto_complete_first_activity,building_number  building_number,city  city,closing_remarks  closing_remarks,company_unit_footer  company_unit_footer,company_unit_header  company_unit_header,company_unit_subject  company_unit_subject,company_unit_subject_rspd  company_unit_subject_rspd,company_unit_type  company_unit_type,compounding_outsourcing  compounding_outsourcing,compression  compression,country  country,cu_date_format  cu_date_format,cu_time_zone  cu_time_zone,cunit_med_rep_subject  cunit_med_rep_subject,cunit_med_rep_subject_rspd  cunit_med_rep_subject_rspd,cunit_sec_res_subject  cunit_sec_res_subject,cunit_sec_res_subject_rspd  cunit_sec_res_subject_rspd,data_entry_site_code  data_entry_site_code,date_created  date_created,date_modified  date_modified,def_bcc_email_corresp  def_bcc_email_corresp,def_bcc_email_response  def_bcc_email_response,default_company_file_name  default_company_file_name,default_for_anonymous  default_for_anonymous,default_for_anonymous_ae  default_for_anonymous_ae,default_language_code  default_language_code,disclaimer  disclaimer,document_backup_path  document_backup_path,dtd_type  dtd_type,due_date_adv  due_date_adv,duns_id  duns_id,e2b_ack_export_backup_path  e2b_ack_export_backup_path,e2b_ack_export_path  e2b_ack_export_path,e2b_document_ack_backup_path  e2b_document_ack_backup_path,e2b_document_ack_path  e2b_document_ack_path,e2b_document_backup_path  e2b_document_backup_path,e2b_document_path  e2b_document_path,e2b_encoding_format  e2b_encoding_format,e2b_export_path  e2b_export_path,e2b_import_backup_path  e2b_import_backup_path,e2b_import_path  e2b_import_path,e2b_meddra  e2b_meddra,e2b_receiver  e2b_receiver,e2b_receiver_id  e2b_receiver_id,e2b_receiver_rec_id  e2b_receiver_rec_id,e2b_xml_export_backup_path  e2b_xml_export_backup_path,e2b_xml_import_backup_path  e2b_xml_import_backup_path,e2b_xml_import_path  e2b_xml_import_path,e_mail_id  e_mail_id,eudamed_number  eudamed_number,exclusion_account  exclusion_account,exclusion_partner  exclusion_partner,export_route  export_route,external_app_contact_name  external_app_contact_name,external_app_rec_id  external_app_rec_id,external_app_updated_date  external_app_updated_date,fax_area_code  fax_area_code,fax_country_code  fax_country_code,fax_number  fax_number,fda_attachment_access  fda_attachment_access,fk_ae_wf_rec_id  fk_ae_wf_rec_id,fk_air_rec_id  fk_air_rec_id,fk_aper_rec_id  fk_aper_rec_id,fk_cec_rec_id  fk_cec_rec_id,fk_cmp_wf_rec_id  fk_cmp_wf_rec_id,fk_fqc_rec_id  fk_fqc_rec_id,fk_inb_wf_rec_id  fk_inb_wf_rec_id,fk_oub_wf_rec_id  fk_oub_wf_rec_id,fk_st_wf_rec_id  fk_st_wf_rec_id,fk_vet_wf_rec_id  fk_vet_wf_rec_id,has_offline_access  has_offline_access,hc_assigned_company_id  hc_assigned_company_id,hc_assigned_estd_lic_no  hc_assigned_estd_lic_no,incoming_source_lang  incoming_source_lang,inq_cu_inc_seq  inq_cu_inc_seq,inq_cu_seq_reset  inq_cu_seq_reset,inquiry_cover_letter_id  inquiry_cover_letter_id,inquiry_cover_letter_name  inquiry_cover_letter_name,inquiry_cu_sequence  inquiry_cu_sequence,inquiry_date_format  inquiry_date_format,inquiry_form_id  inquiry_form_id,inquiry_form_name  inquiry_form_name,inquiry_id_from_cu  inquiry_id_from_cu,inquiry_num_separation_char  inquiry_num_separation_char,inquiry_prefix  inquiry_prefix,inquiry_template_id  inquiry_template_id,inquiry_template_name  inquiry_template_name,inv_due_days  inv_due_days,ird_lrd  ird_lrd,irt_country  irt_country,irt_country_sequence  irt_country_sequence,irt_date_format  irt_date_format,irt_date_sequence  irt_date_sequence,irt_local_labeling_required  irt_local_labeling_required,irt_num_separation_char  irt_num_separation_char,irt_padding  irt_padding,irt_report_type  irt_report_type,irt_report_type_sequence  irt_report_type_sequence,irt_seq_generator  irt_seq_generator,is_distribution_contact  is_distribution_contact,is_e2b_partner  is_e2b_partner,is_sign_up  is_sign_up,is_unregister_access  is_unregister_access,local_labelling_access  local_labelling_access,logo_attachment_rec_id  logo_attachment_rec_id,lrn_country  lrn_country,lrn_country_sequence  lrn_country_sequence,lrn_date_format  lrn_date_format,lrn_date_sequence  lrn_date_sequence,lrn_literal  lrn_literal,lrn_num_separation_char  lrn_num_separation_char,lrn_padding  lrn_padding,lrn_report_type  lrn_report_type,lrn_report_type_sequence  lrn_report_type_sequence,lrn_seq_generator  lrn_seq_generator,mask_exclusion_id  mask_exclusion_id,mask_id  mask_id,migration_flag  migration_flag,name  name,narrative_comment_access  narrative_comment_access,opening_remarks  opening_remarks,partner_id  partner_id,partner_mode  partner_mode,pharmacovigilance_contact_info  pharmacovigilance_contact_info,phone_area_code  phone_area_code,phone_cntry_code  phone_cntry_code,phone_no  phone_no,po_box  po_box,postal_brick  postal_brick,postal_code  postal_code,pri_inv_due_days  pri_inv_due_days,priority_level_high  priority_level_high,priority_level_medium  priority_level_medium,product_class_type  product_class_type,qc_review_date  qc_review_date,qc_review_days  qc_review_days,qc_review_percentage  qc_review_percentage,rct_literal  rct_literal,recalculate_safety_report_id  recalculate_safety_report_id,receiver_id  receiver_id,receiver_mail_id  receiver_mail_id,reconciliation  reconciliation,record_id  record_id,region  region,report_type  report_type,sender_comments_access  sender_comments_access,sender_id  sender_id,sender_organization  sender_organization,sender_organization_type  sender_organization_type,single_reg_num  single_reg_num,spr_id  spr_id,state  state,supplement_field_rec_id  supplement_field_rec_id,terms_of_users  terms_of_users,time_zone  time_zone,type  type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9' AND CD=CAST(type AS VARCHAR(100)) )type_de_ml , user_created  user_created,user_modified  user_modified,version  version,week_working_days  week_working_days,working_days  working_days,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_partner
WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARTNER_DELETION_TMP  WHERE TABLE_NAME='lsmv_partner')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_partner_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_partner_SUBSET.USER_CREATED CREATED_BY,lsmv_partner_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_partner_SUBSET.working_days  ,lsmv_partner_SUBSET.week_working_days  ,lsmv_partner_SUBSET.version  ,lsmv_partner_SUBSET.user_modified  ,lsmv_partner_SUBSET.user_created  ,lsmv_partner_SUBSET.type_de_ml  ,lsmv_partner_SUBSET.type  ,lsmv_partner_SUBSET.time_zone  ,lsmv_partner_SUBSET.terms_of_users  ,lsmv_partner_SUBSET.supplement_field_rec_id  ,lsmv_partner_SUBSET.state  ,lsmv_partner_SUBSET.spr_id  ,lsmv_partner_SUBSET.single_reg_num  ,lsmv_partner_SUBSET.sender_organization_type  ,lsmv_partner_SUBSET.sender_organization  ,lsmv_partner_SUBSET.sender_id  ,lsmv_partner_SUBSET.sender_comments_access  ,lsmv_partner_SUBSET.report_type  ,lsmv_partner_SUBSET.region  ,lsmv_partner_SUBSET.record_id  ,lsmv_partner_SUBSET.reconciliation  ,lsmv_partner_SUBSET.receiver_mail_id  ,lsmv_partner_SUBSET.receiver_id  ,lsmv_partner_SUBSET.recalculate_safety_report_id  ,lsmv_partner_SUBSET.rct_literal  ,lsmv_partner_SUBSET.qc_review_percentage  ,lsmv_partner_SUBSET.qc_review_days  ,lsmv_partner_SUBSET.qc_review_date  ,lsmv_partner_SUBSET.product_class_type  ,lsmv_partner_SUBSET.priority_level_medium  ,lsmv_partner_SUBSET.priority_level_high  ,lsmv_partner_SUBSET.pri_inv_due_days  ,lsmv_partner_SUBSET.postal_code  ,lsmv_partner_SUBSET.postal_brick  ,lsmv_partner_SUBSET.po_box  ,lsmv_partner_SUBSET.phone_no  ,lsmv_partner_SUBSET.phone_cntry_code  ,lsmv_partner_SUBSET.phone_area_code  ,lsmv_partner_SUBSET.pharmacovigilance_contact_info  ,lsmv_partner_SUBSET.partner_mode  ,lsmv_partner_SUBSET.partner_id  ,lsmv_partner_SUBSET.opening_remarks  ,lsmv_partner_SUBSET.narrative_comment_access  ,lsmv_partner_SUBSET.name  ,lsmv_partner_SUBSET.migration_flag  ,lsmv_partner_SUBSET.mask_id  ,lsmv_partner_SUBSET.mask_exclusion_id  ,lsmv_partner_SUBSET.lrn_seq_generator  ,lsmv_partner_SUBSET.lrn_report_type_sequence  ,lsmv_partner_SUBSET.lrn_report_type  ,lsmv_partner_SUBSET.lrn_padding  ,lsmv_partner_SUBSET.lrn_num_separation_char  ,lsmv_partner_SUBSET.lrn_literal  ,lsmv_partner_SUBSET.lrn_date_sequence  ,lsmv_partner_SUBSET.lrn_date_format  ,lsmv_partner_SUBSET.lrn_country_sequence  ,lsmv_partner_SUBSET.lrn_country  ,lsmv_partner_SUBSET.logo_attachment_rec_id  ,lsmv_partner_SUBSET.local_labelling_access  ,lsmv_partner_SUBSET.is_unregister_access  ,lsmv_partner_SUBSET.is_sign_up  ,lsmv_partner_SUBSET.is_e2b_partner  ,lsmv_partner_SUBSET.is_distribution_contact  ,lsmv_partner_SUBSET.irt_seq_generator  ,lsmv_partner_SUBSET.irt_report_type_sequence  ,lsmv_partner_SUBSET.irt_report_type  ,lsmv_partner_SUBSET.irt_padding  ,lsmv_partner_SUBSET.irt_num_separation_char  ,lsmv_partner_SUBSET.irt_local_labeling_required  ,lsmv_partner_SUBSET.irt_date_sequence  ,lsmv_partner_SUBSET.irt_date_format  ,lsmv_partner_SUBSET.irt_country_sequence  ,lsmv_partner_SUBSET.irt_country  ,lsmv_partner_SUBSET.ird_lrd  ,lsmv_partner_SUBSET.inv_due_days  ,lsmv_partner_SUBSET.inquiry_template_name  ,lsmv_partner_SUBSET.inquiry_template_id  ,lsmv_partner_SUBSET.inquiry_prefix  ,lsmv_partner_SUBSET.inquiry_num_separation_char  ,lsmv_partner_SUBSET.inquiry_id_from_cu  ,lsmv_partner_SUBSET.inquiry_form_name  ,lsmv_partner_SUBSET.inquiry_form_id  ,lsmv_partner_SUBSET.inquiry_date_format  ,lsmv_partner_SUBSET.inquiry_cu_sequence  ,lsmv_partner_SUBSET.inquiry_cover_letter_name  ,lsmv_partner_SUBSET.inquiry_cover_letter_id  ,lsmv_partner_SUBSET.inq_cu_seq_reset  ,lsmv_partner_SUBSET.inq_cu_inc_seq  ,lsmv_partner_SUBSET.incoming_source_lang  ,lsmv_partner_SUBSET.hc_assigned_estd_lic_no  ,lsmv_partner_SUBSET.hc_assigned_company_id  ,lsmv_partner_SUBSET.has_offline_access  ,lsmv_partner_SUBSET.fk_vet_wf_rec_id  ,lsmv_partner_SUBSET.fk_st_wf_rec_id  ,lsmv_partner_SUBSET.fk_oub_wf_rec_id  ,lsmv_partner_SUBSET.fk_inb_wf_rec_id  ,lsmv_partner_SUBSET.fk_fqc_rec_id  ,lsmv_partner_SUBSET.fk_cmp_wf_rec_id  ,lsmv_partner_SUBSET.fk_cec_rec_id  ,lsmv_partner_SUBSET.fk_aper_rec_id  ,lsmv_partner_SUBSET.fk_air_rec_id  ,lsmv_partner_SUBSET.fk_ae_wf_rec_id  ,lsmv_partner_SUBSET.fda_attachment_access  ,lsmv_partner_SUBSET.fax_number  ,lsmv_partner_SUBSET.fax_country_code  ,lsmv_partner_SUBSET.fax_area_code  ,lsmv_partner_SUBSET.external_app_updated_date  ,lsmv_partner_SUBSET.external_app_rec_id  ,lsmv_partner_SUBSET.external_app_contact_name  ,lsmv_partner_SUBSET.export_route  ,lsmv_partner_SUBSET.exclusion_partner  ,lsmv_partner_SUBSET.exclusion_account  ,lsmv_partner_SUBSET.eudamed_number  ,lsmv_partner_SUBSET.e_mail_id  ,lsmv_partner_SUBSET.e2b_xml_import_path  ,lsmv_partner_SUBSET.e2b_xml_import_backup_path  ,lsmv_partner_SUBSET.e2b_xml_export_backup_path  ,lsmv_partner_SUBSET.e2b_receiver_rec_id  ,lsmv_partner_SUBSET.e2b_receiver_id  ,lsmv_partner_SUBSET.e2b_receiver  ,lsmv_partner_SUBSET.e2b_meddra  ,lsmv_partner_SUBSET.e2b_import_path  ,lsmv_partner_SUBSET.e2b_import_backup_path  ,lsmv_partner_SUBSET.e2b_export_path  ,lsmv_partner_SUBSET.e2b_encoding_format  ,lsmv_partner_SUBSET.e2b_document_path  ,lsmv_partner_SUBSET.e2b_document_backup_path  ,lsmv_partner_SUBSET.e2b_document_ack_path  ,lsmv_partner_SUBSET.e2b_document_ack_backup_path  ,lsmv_partner_SUBSET.e2b_ack_export_path  ,lsmv_partner_SUBSET.e2b_ack_export_backup_path  ,lsmv_partner_SUBSET.duns_id  ,lsmv_partner_SUBSET.due_date_adv  ,lsmv_partner_SUBSET.dtd_type  ,lsmv_partner_SUBSET.document_backup_path  ,lsmv_partner_SUBSET.disclaimer  ,lsmv_partner_SUBSET.default_language_code  ,lsmv_partner_SUBSET.default_for_anonymous_ae  ,lsmv_partner_SUBSET.default_for_anonymous  ,lsmv_partner_SUBSET.default_company_file_name  ,lsmv_partner_SUBSET.def_bcc_email_response  ,lsmv_partner_SUBSET.def_bcc_email_corresp  ,lsmv_partner_SUBSET.date_modified  ,lsmv_partner_SUBSET.date_created  ,lsmv_partner_SUBSET.data_entry_site_code  ,lsmv_partner_SUBSET.cunit_sec_res_subject_rspd  ,lsmv_partner_SUBSET.cunit_sec_res_subject  ,lsmv_partner_SUBSET.cunit_med_rep_subject_rspd  ,lsmv_partner_SUBSET.cunit_med_rep_subject  ,lsmv_partner_SUBSET.cu_time_zone  ,lsmv_partner_SUBSET.cu_date_format  ,lsmv_partner_SUBSET.country  ,lsmv_partner_SUBSET.compression  ,lsmv_partner_SUBSET.compounding_outsourcing  ,lsmv_partner_SUBSET.company_unit_type  ,lsmv_partner_SUBSET.company_unit_subject_rspd  ,lsmv_partner_SUBSET.company_unit_subject  ,lsmv_partner_SUBSET.company_unit_header  ,lsmv_partner_SUBSET.company_unit_footer  ,lsmv_partner_SUBSET.closing_remarks  ,lsmv_partner_SUBSET.city  ,lsmv_partner_SUBSET.building_number  ,lsmv_partner_SUBSET.auto_complete_first_activity  ,lsmv_partner_SUBSET.authority  ,lsmv_partner_SUBSET.atn_seq_generator  ,lsmv_partner_SUBSET.atn_padding  ,lsmv_partner_SUBSET.atn_num_separation_char  ,lsmv_partner_SUBSET.atn_literal  ,lsmv_partner_SUBSET.atn_date_format  ,lsmv_partner_SUBSET.assignment_emial_format  ,lsmv_partner_SUBSET.assi_emial_frmt_rspd  ,lsmv_partner_SUBSET.allow_lde_synch  ,lsmv_partner_SUBSET.allow_e2b_classification  ,lsmv_partner_SUBSET.allow_due_date_synch  ,lsmv_partner_SUBSET.allow_chat_access_mi  ,lsmv_partner_SUBSET.allow_chat_access  ,lsmv_partner_SUBSET.all_product_class_assign  ,lsmv_partner_SUBSET.all_product_assign  ,lsmv_partner_SUBSET.all_department_assign  ,lsmv_partner_SUBSET.agrepo_search_documents  ,lsmv_partner_SUBSET.agrepo_language_code  ,lsmv_partner_SUBSET.address  ,lsmv_partner_SUBSET.acknowledgement_subject  ,lsmv_partner_SUBSET.acknowledgement  ,lsmv_partner_SUBSET.ack_sub_res  ,lsmv_partner_SUBSET.ack_responder  ,lsmv_partner_SUBSET.ack_expected  ,lsmv_partner_SUBSET.ack_comments_length  ,lsmv_partner_SUBSET.account_for_weekend  ,lsmv_partner_SUBSET.account_for_holidays ,CONCAT( NVL(lsmv_partner_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_partner_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP)
where target_table_name='LS_DB_PARTNER'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER   
SET LS_DB_PARTNER.working_days = LS_DB_PARTNER_TMP.working_days,LS_DB_PARTNER.week_working_days = LS_DB_PARTNER_TMP.week_working_days,LS_DB_PARTNER.version = LS_DB_PARTNER_TMP.version,LS_DB_PARTNER.user_modified = LS_DB_PARTNER_TMP.user_modified,LS_DB_PARTNER.user_created = LS_DB_PARTNER_TMP.user_created,LS_DB_PARTNER.type_de_ml = LS_DB_PARTNER_TMP.type_de_ml,LS_DB_PARTNER.type = LS_DB_PARTNER_TMP.type,LS_DB_PARTNER.time_zone = LS_DB_PARTNER_TMP.time_zone,LS_DB_PARTNER.terms_of_users = LS_DB_PARTNER_TMP.terms_of_users,LS_DB_PARTNER.supplement_field_rec_id = LS_DB_PARTNER_TMP.supplement_field_rec_id,LS_DB_PARTNER.state = LS_DB_PARTNER_TMP.state,LS_DB_PARTNER.spr_id = LS_DB_PARTNER_TMP.spr_id,LS_DB_PARTNER.single_reg_num = LS_DB_PARTNER_TMP.single_reg_num,LS_DB_PARTNER.sender_organization_type = LS_DB_PARTNER_TMP.sender_organization_type,LS_DB_PARTNER.sender_organization = LS_DB_PARTNER_TMP.sender_organization,LS_DB_PARTNER.sender_id = LS_DB_PARTNER_TMP.sender_id,LS_DB_PARTNER.sender_comments_access = LS_DB_PARTNER_TMP.sender_comments_access,LS_DB_PARTNER.report_type = LS_DB_PARTNER_TMP.report_type,LS_DB_PARTNER.region = LS_DB_PARTNER_TMP.region,LS_DB_PARTNER.record_id = LS_DB_PARTNER_TMP.record_id,LS_DB_PARTNER.reconciliation = LS_DB_PARTNER_TMP.reconciliation,LS_DB_PARTNER.receiver_mail_id = LS_DB_PARTNER_TMP.receiver_mail_id,LS_DB_PARTNER.receiver_id = LS_DB_PARTNER_TMP.receiver_id,LS_DB_PARTNER.recalculate_safety_report_id = LS_DB_PARTNER_TMP.recalculate_safety_report_id,LS_DB_PARTNER.rct_literal = LS_DB_PARTNER_TMP.rct_literal,LS_DB_PARTNER.qc_review_percentage = LS_DB_PARTNER_TMP.qc_review_percentage,LS_DB_PARTNER.qc_review_days = LS_DB_PARTNER_TMP.qc_review_days,LS_DB_PARTNER.qc_review_date = LS_DB_PARTNER_TMP.qc_review_date,LS_DB_PARTNER.product_class_type = LS_DB_PARTNER_TMP.product_class_type,LS_DB_PARTNER.priority_level_medium = LS_DB_PARTNER_TMP.priority_level_medium,LS_DB_PARTNER.priority_level_high = LS_DB_PARTNER_TMP.priority_level_high,LS_DB_PARTNER.pri_inv_due_days = LS_DB_PARTNER_TMP.pri_inv_due_days,LS_DB_PARTNER.postal_code = LS_DB_PARTNER_TMP.postal_code,LS_DB_PARTNER.postal_brick = LS_DB_PARTNER_TMP.postal_brick,LS_DB_PARTNER.po_box = LS_DB_PARTNER_TMP.po_box,LS_DB_PARTNER.phone_no = LS_DB_PARTNER_TMP.phone_no,LS_DB_PARTNER.phone_cntry_code = LS_DB_PARTNER_TMP.phone_cntry_code,LS_DB_PARTNER.phone_area_code = LS_DB_PARTNER_TMP.phone_area_code,LS_DB_PARTNER.pharmacovigilance_contact_info = LS_DB_PARTNER_TMP.pharmacovigilance_contact_info,LS_DB_PARTNER.partner_mode = LS_DB_PARTNER_TMP.partner_mode,LS_DB_PARTNER.partner_id = LS_DB_PARTNER_TMP.partner_id,LS_DB_PARTNER.opening_remarks = LS_DB_PARTNER_TMP.opening_remarks,LS_DB_PARTNER.narrative_comment_access = LS_DB_PARTNER_TMP.narrative_comment_access,LS_DB_PARTNER.name = LS_DB_PARTNER_TMP.name,LS_DB_PARTNER.migration_flag = LS_DB_PARTNER_TMP.migration_flag,LS_DB_PARTNER.mask_id = LS_DB_PARTNER_TMP.mask_id,LS_DB_PARTNER.mask_exclusion_id = LS_DB_PARTNER_TMP.mask_exclusion_id,LS_DB_PARTNER.lrn_seq_generator = LS_DB_PARTNER_TMP.lrn_seq_generator,LS_DB_PARTNER.lrn_report_type_sequence = LS_DB_PARTNER_TMP.lrn_report_type_sequence,LS_DB_PARTNER.lrn_report_type = LS_DB_PARTNER_TMP.lrn_report_type,LS_DB_PARTNER.lrn_padding = LS_DB_PARTNER_TMP.lrn_padding,LS_DB_PARTNER.lrn_num_separation_char = LS_DB_PARTNER_TMP.lrn_num_separation_char,LS_DB_PARTNER.lrn_literal = LS_DB_PARTNER_TMP.lrn_literal,LS_DB_PARTNER.lrn_date_sequence = LS_DB_PARTNER_TMP.lrn_date_sequence,LS_DB_PARTNER.lrn_date_format = LS_DB_PARTNER_TMP.lrn_date_format,LS_DB_PARTNER.lrn_country_sequence = LS_DB_PARTNER_TMP.lrn_country_sequence,LS_DB_PARTNER.lrn_country = LS_DB_PARTNER_TMP.lrn_country,LS_DB_PARTNER.logo_attachment_rec_id = LS_DB_PARTNER_TMP.logo_attachment_rec_id,LS_DB_PARTNER.local_labelling_access = LS_DB_PARTNER_TMP.local_labelling_access,LS_DB_PARTNER.is_unregister_access = LS_DB_PARTNER_TMP.is_unregister_access,LS_DB_PARTNER.is_sign_up = LS_DB_PARTNER_TMP.is_sign_up,LS_DB_PARTNER.is_e2b_partner = LS_DB_PARTNER_TMP.is_e2b_partner,LS_DB_PARTNER.is_distribution_contact = LS_DB_PARTNER_TMP.is_distribution_contact,LS_DB_PARTNER.irt_seq_generator = LS_DB_PARTNER_TMP.irt_seq_generator,LS_DB_PARTNER.irt_report_type_sequence = LS_DB_PARTNER_TMP.irt_report_type_sequence,LS_DB_PARTNER.irt_report_type = LS_DB_PARTNER_TMP.irt_report_type,LS_DB_PARTNER.irt_padding = LS_DB_PARTNER_TMP.irt_padding,LS_DB_PARTNER.irt_num_separation_char = LS_DB_PARTNER_TMP.irt_num_separation_char,LS_DB_PARTNER.irt_local_labeling_required = LS_DB_PARTNER_TMP.irt_local_labeling_required,LS_DB_PARTNER.irt_date_sequence = LS_DB_PARTNER_TMP.irt_date_sequence,LS_DB_PARTNER.irt_date_format = LS_DB_PARTNER_TMP.irt_date_format,LS_DB_PARTNER.irt_country_sequence = LS_DB_PARTNER_TMP.irt_country_sequence,LS_DB_PARTNER.irt_country = LS_DB_PARTNER_TMP.irt_country,LS_DB_PARTNER.ird_lrd = LS_DB_PARTNER_TMP.ird_lrd,LS_DB_PARTNER.inv_due_days = LS_DB_PARTNER_TMP.inv_due_days,LS_DB_PARTNER.inquiry_template_name = LS_DB_PARTNER_TMP.inquiry_template_name,LS_DB_PARTNER.inquiry_template_id = LS_DB_PARTNER_TMP.inquiry_template_id,LS_DB_PARTNER.inquiry_prefix = LS_DB_PARTNER_TMP.inquiry_prefix,LS_DB_PARTNER.inquiry_num_separation_char = LS_DB_PARTNER_TMP.inquiry_num_separation_char,LS_DB_PARTNER.inquiry_id_from_cu = LS_DB_PARTNER_TMP.inquiry_id_from_cu,LS_DB_PARTNER.inquiry_form_name = LS_DB_PARTNER_TMP.inquiry_form_name,LS_DB_PARTNER.inquiry_form_id = LS_DB_PARTNER_TMP.inquiry_form_id,LS_DB_PARTNER.inquiry_date_format = LS_DB_PARTNER_TMP.inquiry_date_format,LS_DB_PARTNER.inquiry_cu_sequence = LS_DB_PARTNER_TMP.inquiry_cu_sequence,LS_DB_PARTNER.inquiry_cover_letter_name = LS_DB_PARTNER_TMP.inquiry_cover_letter_name,LS_DB_PARTNER.inquiry_cover_letter_id = LS_DB_PARTNER_TMP.inquiry_cover_letter_id,LS_DB_PARTNER.inq_cu_seq_reset = LS_DB_PARTNER_TMP.inq_cu_seq_reset,LS_DB_PARTNER.inq_cu_inc_seq = LS_DB_PARTNER_TMP.inq_cu_inc_seq,LS_DB_PARTNER.incoming_source_lang = LS_DB_PARTNER_TMP.incoming_source_lang,LS_DB_PARTNER.hc_assigned_estd_lic_no = LS_DB_PARTNER_TMP.hc_assigned_estd_lic_no,LS_DB_PARTNER.hc_assigned_company_id = LS_DB_PARTNER_TMP.hc_assigned_company_id,LS_DB_PARTNER.has_offline_access = LS_DB_PARTNER_TMP.has_offline_access,LS_DB_PARTNER.fk_vet_wf_rec_id = LS_DB_PARTNER_TMP.fk_vet_wf_rec_id,LS_DB_PARTNER.fk_st_wf_rec_id = LS_DB_PARTNER_TMP.fk_st_wf_rec_id,LS_DB_PARTNER.fk_oub_wf_rec_id = LS_DB_PARTNER_TMP.fk_oub_wf_rec_id,LS_DB_PARTNER.fk_inb_wf_rec_id = LS_DB_PARTNER_TMP.fk_inb_wf_rec_id,LS_DB_PARTNER.fk_fqc_rec_id = LS_DB_PARTNER_TMP.fk_fqc_rec_id,LS_DB_PARTNER.fk_cmp_wf_rec_id = LS_DB_PARTNER_TMP.fk_cmp_wf_rec_id,LS_DB_PARTNER.fk_cec_rec_id = LS_DB_PARTNER_TMP.fk_cec_rec_id,LS_DB_PARTNER.fk_aper_rec_id = LS_DB_PARTNER_TMP.fk_aper_rec_id,LS_DB_PARTNER.fk_air_rec_id = LS_DB_PARTNER_TMP.fk_air_rec_id,LS_DB_PARTNER.fk_ae_wf_rec_id = LS_DB_PARTNER_TMP.fk_ae_wf_rec_id,LS_DB_PARTNER.fda_attachment_access = LS_DB_PARTNER_TMP.fda_attachment_access,LS_DB_PARTNER.fax_number = LS_DB_PARTNER_TMP.fax_number,LS_DB_PARTNER.fax_country_code = LS_DB_PARTNER_TMP.fax_country_code,LS_DB_PARTNER.fax_area_code = LS_DB_PARTNER_TMP.fax_area_code,LS_DB_PARTNER.external_app_updated_date = LS_DB_PARTNER_TMP.external_app_updated_date,LS_DB_PARTNER.external_app_rec_id = LS_DB_PARTNER_TMP.external_app_rec_id,LS_DB_PARTNER.external_app_contact_name = LS_DB_PARTNER_TMP.external_app_contact_name,LS_DB_PARTNER.export_route = LS_DB_PARTNER_TMP.export_route,LS_DB_PARTNER.exclusion_partner = LS_DB_PARTNER_TMP.exclusion_partner,LS_DB_PARTNER.exclusion_account = LS_DB_PARTNER_TMP.exclusion_account,LS_DB_PARTNER.eudamed_number = LS_DB_PARTNER_TMP.eudamed_number,LS_DB_PARTNER.e_mail_id = LS_DB_PARTNER_TMP.e_mail_id,LS_DB_PARTNER.e2b_xml_import_path = LS_DB_PARTNER_TMP.e2b_xml_import_path,LS_DB_PARTNER.e2b_xml_import_backup_path = LS_DB_PARTNER_TMP.e2b_xml_import_backup_path,LS_DB_PARTNER.e2b_xml_export_backup_path = LS_DB_PARTNER_TMP.e2b_xml_export_backup_path,LS_DB_PARTNER.e2b_receiver_rec_id = LS_DB_PARTNER_TMP.e2b_receiver_rec_id,LS_DB_PARTNER.e2b_receiver_id = LS_DB_PARTNER_TMP.e2b_receiver_id,LS_DB_PARTNER.e2b_receiver = LS_DB_PARTNER_TMP.e2b_receiver,LS_DB_PARTNER.e2b_meddra = LS_DB_PARTNER_TMP.e2b_meddra,LS_DB_PARTNER.e2b_import_path = LS_DB_PARTNER_TMP.e2b_import_path,LS_DB_PARTNER.e2b_import_backup_path = LS_DB_PARTNER_TMP.e2b_import_backup_path,LS_DB_PARTNER.e2b_export_path = LS_DB_PARTNER_TMP.e2b_export_path,LS_DB_PARTNER.e2b_encoding_format = LS_DB_PARTNER_TMP.e2b_encoding_format,LS_DB_PARTNER.e2b_document_path = LS_DB_PARTNER_TMP.e2b_document_path,LS_DB_PARTNER.e2b_document_backup_path = LS_DB_PARTNER_TMP.e2b_document_backup_path,LS_DB_PARTNER.e2b_document_ack_path = LS_DB_PARTNER_TMP.e2b_document_ack_path,LS_DB_PARTNER.e2b_document_ack_backup_path = LS_DB_PARTNER_TMP.e2b_document_ack_backup_path,LS_DB_PARTNER.e2b_ack_export_path = LS_DB_PARTNER_TMP.e2b_ack_export_path,LS_DB_PARTNER.e2b_ack_export_backup_path = LS_DB_PARTNER_TMP.e2b_ack_export_backup_path,LS_DB_PARTNER.duns_id = LS_DB_PARTNER_TMP.duns_id,LS_DB_PARTNER.due_date_adv = LS_DB_PARTNER_TMP.due_date_adv,LS_DB_PARTNER.dtd_type = LS_DB_PARTNER_TMP.dtd_type,LS_DB_PARTNER.document_backup_path = LS_DB_PARTNER_TMP.document_backup_path,LS_DB_PARTNER.disclaimer = LS_DB_PARTNER_TMP.disclaimer,LS_DB_PARTNER.default_language_code = LS_DB_PARTNER_TMP.default_language_code,LS_DB_PARTNER.default_for_anonymous_ae = LS_DB_PARTNER_TMP.default_for_anonymous_ae,LS_DB_PARTNER.default_for_anonymous = LS_DB_PARTNER_TMP.default_for_anonymous,LS_DB_PARTNER.default_company_file_name = LS_DB_PARTNER_TMP.default_company_file_name,LS_DB_PARTNER.def_bcc_email_response = LS_DB_PARTNER_TMP.def_bcc_email_response,LS_DB_PARTNER.def_bcc_email_corresp = LS_DB_PARTNER_TMP.def_bcc_email_corresp,LS_DB_PARTNER.date_modified = LS_DB_PARTNER_TMP.date_modified,LS_DB_PARTNER.date_created = LS_DB_PARTNER_TMP.date_created,LS_DB_PARTNER.data_entry_site_code = LS_DB_PARTNER_TMP.data_entry_site_code,LS_DB_PARTNER.cunit_sec_res_subject_rspd = LS_DB_PARTNER_TMP.cunit_sec_res_subject_rspd,LS_DB_PARTNER.cunit_sec_res_subject = LS_DB_PARTNER_TMP.cunit_sec_res_subject,LS_DB_PARTNER.cunit_med_rep_subject_rspd = LS_DB_PARTNER_TMP.cunit_med_rep_subject_rspd,LS_DB_PARTNER.cunit_med_rep_subject = LS_DB_PARTNER_TMP.cunit_med_rep_subject,LS_DB_PARTNER.cu_time_zone = LS_DB_PARTNER_TMP.cu_time_zone,LS_DB_PARTNER.cu_date_format = LS_DB_PARTNER_TMP.cu_date_format,LS_DB_PARTNER.country = LS_DB_PARTNER_TMP.country,LS_DB_PARTNER.compression = LS_DB_PARTNER_TMP.compression,LS_DB_PARTNER.compounding_outsourcing = LS_DB_PARTNER_TMP.compounding_outsourcing,LS_DB_PARTNER.company_unit_type = LS_DB_PARTNER_TMP.company_unit_type,LS_DB_PARTNER.company_unit_subject_rspd = LS_DB_PARTNER_TMP.company_unit_subject_rspd,LS_DB_PARTNER.company_unit_subject = LS_DB_PARTNER_TMP.company_unit_subject,LS_DB_PARTNER.company_unit_header = LS_DB_PARTNER_TMP.company_unit_header,LS_DB_PARTNER.company_unit_footer = LS_DB_PARTNER_TMP.company_unit_footer,LS_DB_PARTNER.closing_remarks = LS_DB_PARTNER_TMP.closing_remarks,LS_DB_PARTNER.city = LS_DB_PARTNER_TMP.city,LS_DB_PARTNER.building_number = LS_DB_PARTNER_TMP.building_number,LS_DB_PARTNER.auto_complete_first_activity = LS_DB_PARTNER_TMP.auto_complete_first_activity,LS_DB_PARTNER.authority = LS_DB_PARTNER_TMP.authority,LS_DB_PARTNER.atn_seq_generator = LS_DB_PARTNER_TMP.atn_seq_generator,LS_DB_PARTNER.atn_padding = LS_DB_PARTNER_TMP.atn_padding,LS_DB_PARTNER.atn_num_separation_char = LS_DB_PARTNER_TMP.atn_num_separation_char,LS_DB_PARTNER.atn_literal = LS_DB_PARTNER_TMP.atn_literal,LS_DB_PARTNER.atn_date_format = LS_DB_PARTNER_TMP.atn_date_format,LS_DB_PARTNER.assignment_emial_format = LS_DB_PARTNER_TMP.assignment_emial_format,LS_DB_PARTNER.assi_emial_frmt_rspd = LS_DB_PARTNER_TMP.assi_emial_frmt_rspd,LS_DB_PARTNER.allow_lde_synch = LS_DB_PARTNER_TMP.allow_lde_synch,LS_DB_PARTNER.allow_e2b_classification = LS_DB_PARTNER_TMP.allow_e2b_classification,LS_DB_PARTNER.allow_due_date_synch = LS_DB_PARTNER_TMP.allow_due_date_synch,LS_DB_PARTNER.allow_chat_access_mi = LS_DB_PARTNER_TMP.allow_chat_access_mi,LS_DB_PARTNER.allow_chat_access = LS_DB_PARTNER_TMP.allow_chat_access,LS_DB_PARTNER.all_product_class_assign = LS_DB_PARTNER_TMP.all_product_class_assign,LS_DB_PARTNER.all_product_assign = LS_DB_PARTNER_TMP.all_product_assign,LS_DB_PARTNER.all_department_assign = LS_DB_PARTNER_TMP.all_department_assign,LS_DB_PARTNER.agrepo_search_documents = LS_DB_PARTNER_TMP.agrepo_search_documents,LS_DB_PARTNER.agrepo_language_code = LS_DB_PARTNER_TMP.agrepo_language_code,LS_DB_PARTNER.address = LS_DB_PARTNER_TMP.address,LS_DB_PARTNER.acknowledgement_subject = LS_DB_PARTNER_TMP.acknowledgement_subject,LS_DB_PARTNER.acknowledgement = LS_DB_PARTNER_TMP.acknowledgement,LS_DB_PARTNER.ack_sub_res = LS_DB_PARTNER_TMP.ack_sub_res,LS_DB_PARTNER.ack_responder = LS_DB_PARTNER_TMP.ack_responder,LS_DB_PARTNER.ack_expected = LS_DB_PARTNER_TMP.ack_expected,LS_DB_PARTNER.ack_comments_length = LS_DB_PARTNER_TMP.ack_comments_length,LS_DB_PARTNER.account_for_weekend = LS_DB_PARTNER_TMP.account_for_weekend,LS_DB_PARTNER.account_for_holidays = LS_DB_PARTNER_TMP.account_for_holidays,
LS_DB_PARTNER.PROCESSING_DT = LS_DB_PARTNER_TMP.PROCESSING_DT ,
LS_DB_PARTNER.expiry_date    =LS_DB_PARTNER_TMP.expiry_date       ,
LS_DB_PARTNER.created_by     =LS_DB_PARTNER_TMP.created_by        ,
LS_DB_PARTNER.created_dt     =LS_DB_PARTNER_TMP.created_dt        ,
LS_DB_PARTNER.load_ts        =LS_DB_PARTNER_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP 
WHERE LS_DB_PARTNER.INTEGRATION_ID = LS_DB_PARTNER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PARTNER_TMP.PROCESSING_DT = LS_DB_PARTNER.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,working_days,
week_working_days,
version,
user_modified,
user_created,
type_de_ml,
type,
time_zone,
terms_of_users,
supplement_field_rec_id,
state,
spr_id,
single_reg_num,
sender_organization_type,
sender_organization,
sender_id,
sender_comments_access,
report_type,
region,
record_id,
reconciliation,
receiver_mail_id,
receiver_id,
recalculate_safety_report_id,
rct_literal,
qc_review_percentage,
qc_review_days,
qc_review_date,
product_class_type,
priority_level_medium,
priority_level_high,
pri_inv_due_days,
postal_code,
postal_brick,
po_box,
phone_no,
phone_cntry_code,
phone_area_code,
pharmacovigilance_contact_info,
partner_mode,
partner_id,
opening_remarks,
narrative_comment_access,
name,
migration_flag,
mask_id,
mask_exclusion_id,
lrn_seq_generator,
lrn_report_type_sequence,
lrn_report_type,
lrn_padding,
lrn_num_separation_char,
lrn_literal,
lrn_date_sequence,
lrn_date_format,
lrn_country_sequence,
lrn_country,
logo_attachment_rec_id,
local_labelling_access,
is_unregister_access,
is_sign_up,
is_e2b_partner,
is_distribution_contact,
irt_seq_generator,
irt_report_type_sequence,
irt_report_type,
irt_padding,
irt_num_separation_char,
irt_local_labeling_required,
irt_date_sequence,
irt_date_format,
irt_country_sequence,
irt_country,
ird_lrd,
inv_due_days,
inquiry_template_name,
inquiry_template_id,
inquiry_prefix,
inquiry_num_separation_char,
inquiry_id_from_cu,
inquiry_form_name,
inquiry_form_id,
inquiry_date_format,
inquiry_cu_sequence,
inquiry_cover_letter_name,
inquiry_cover_letter_id,
inq_cu_seq_reset,
inq_cu_inc_seq,
incoming_source_lang,
hc_assigned_estd_lic_no,
hc_assigned_company_id,
has_offline_access,
fk_vet_wf_rec_id,
fk_st_wf_rec_id,
fk_oub_wf_rec_id,
fk_inb_wf_rec_id,
fk_fqc_rec_id,
fk_cmp_wf_rec_id,
fk_cec_rec_id,
fk_aper_rec_id,
fk_air_rec_id,
fk_ae_wf_rec_id,
fda_attachment_access,
fax_number,
fax_country_code,
fax_area_code,
external_app_updated_date,
external_app_rec_id,
external_app_contact_name,
export_route,
exclusion_partner,
exclusion_account,
eudamed_number,
e_mail_id,
e2b_xml_import_path,
e2b_xml_import_backup_path,
e2b_xml_export_backup_path,
e2b_receiver_rec_id,
e2b_receiver_id,
e2b_receiver,
e2b_meddra,
e2b_import_path,
e2b_import_backup_path,
e2b_export_path,
e2b_encoding_format,
e2b_document_path,
e2b_document_backup_path,
e2b_document_ack_path,
e2b_document_ack_backup_path,
e2b_ack_export_path,
e2b_ack_export_backup_path,
duns_id,
due_date_adv,
dtd_type,
document_backup_path,
disclaimer,
default_language_code,
default_for_anonymous_ae,
default_for_anonymous,
default_company_file_name,
def_bcc_email_response,
def_bcc_email_corresp,
date_modified,
date_created,
data_entry_site_code,
cunit_sec_res_subject_rspd,
cunit_sec_res_subject,
cunit_med_rep_subject_rspd,
cunit_med_rep_subject,
cu_time_zone,
cu_date_format,
country,
compression,
compounding_outsourcing,
company_unit_type,
company_unit_subject_rspd,
company_unit_subject,
company_unit_header,
company_unit_footer,
closing_remarks,
city,
building_number,
auto_complete_first_activity,
authority,
atn_seq_generator,
atn_padding,
atn_num_separation_char,
atn_literal,
atn_date_format,
assignment_emial_format,
assi_emial_frmt_rspd,
allow_lde_synch,
allow_e2b_classification,
allow_due_date_synch,
allow_chat_access_mi,
allow_chat_access,
all_product_class_assign,
all_product_assign,
all_department_assign,
agrepo_search_documents,
agrepo_language_code,
address,
acknowledgement_subject,
acknowledgement,
ack_sub_res,
ack_responder,
ack_expected,
ack_comments_length,
account_for_weekend,
account_for_holidays)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,working_days,
week_working_days,
version,
user_modified,
user_created,
type_de_ml,
type,
time_zone,
terms_of_users,
supplement_field_rec_id,
state,
spr_id,
single_reg_num,
sender_organization_type,
sender_organization,
sender_id,
sender_comments_access,
report_type,
region,
record_id,
reconciliation,
receiver_mail_id,
receiver_id,
recalculate_safety_report_id,
rct_literal,
qc_review_percentage,
qc_review_days,
qc_review_date,
product_class_type,
priority_level_medium,
priority_level_high,
pri_inv_due_days,
postal_code,
postal_brick,
po_box,
phone_no,
phone_cntry_code,
phone_area_code,
pharmacovigilance_contact_info,
partner_mode,
partner_id,
opening_remarks,
narrative_comment_access,
name,
migration_flag,
mask_id,
mask_exclusion_id,
lrn_seq_generator,
lrn_report_type_sequence,
lrn_report_type,
lrn_padding,
lrn_num_separation_char,
lrn_literal,
lrn_date_sequence,
lrn_date_format,
lrn_country_sequence,
lrn_country,
logo_attachment_rec_id,
local_labelling_access,
is_unregister_access,
is_sign_up,
is_e2b_partner,
is_distribution_contact,
irt_seq_generator,
irt_report_type_sequence,
irt_report_type,
irt_padding,
irt_num_separation_char,
irt_local_labeling_required,
irt_date_sequence,
irt_date_format,
irt_country_sequence,
irt_country,
ird_lrd,
inv_due_days,
inquiry_template_name,
inquiry_template_id,
inquiry_prefix,
inquiry_num_separation_char,
inquiry_id_from_cu,
inquiry_form_name,
inquiry_form_id,
inquiry_date_format,
inquiry_cu_sequence,
inquiry_cover_letter_name,
inquiry_cover_letter_id,
inq_cu_seq_reset,
inq_cu_inc_seq,
incoming_source_lang,
hc_assigned_estd_lic_no,
hc_assigned_company_id,
has_offline_access,
fk_vet_wf_rec_id,
fk_st_wf_rec_id,
fk_oub_wf_rec_id,
fk_inb_wf_rec_id,
fk_fqc_rec_id,
fk_cmp_wf_rec_id,
fk_cec_rec_id,
fk_aper_rec_id,
fk_air_rec_id,
fk_ae_wf_rec_id,
fda_attachment_access,
fax_number,
fax_country_code,
fax_area_code,
external_app_updated_date,
external_app_rec_id,
external_app_contact_name,
export_route,
exclusion_partner,
exclusion_account,
eudamed_number,
e_mail_id,
e2b_xml_import_path,
e2b_xml_import_backup_path,
e2b_xml_export_backup_path,
e2b_receiver_rec_id,
e2b_receiver_id,
e2b_receiver,
e2b_meddra,
e2b_import_path,
e2b_import_backup_path,
e2b_export_path,
e2b_encoding_format,
e2b_document_path,
e2b_document_backup_path,
e2b_document_ack_path,
e2b_document_ack_backup_path,
e2b_ack_export_path,
e2b_ack_export_backup_path,
duns_id,
due_date_adv,
dtd_type,
document_backup_path,
disclaimer,
default_language_code,
default_for_anonymous_ae,
default_for_anonymous,
default_company_file_name,
def_bcc_email_response,
def_bcc_email_corresp,
date_modified,
date_created,
data_entry_site_code,
cunit_sec_res_subject_rspd,
cunit_sec_res_subject,
cunit_med_rep_subject_rspd,
cunit_med_rep_subject,
cu_time_zone,
cu_date_format,
country,
compression,
compounding_outsourcing,
company_unit_type,
company_unit_subject_rspd,
company_unit_subject,
company_unit_header,
company_unit_footer,
closing_remarks,
city,
building_number,
auto_complete_first_activity,
authority,
atn_seq_generator,
atn_padding,
atn_num_separation_char,
atn_literal,
atn_date_format,
assignment_emial_format,
assi_emial_frmt_rspd,
allow_lde_synch,
allow_e2b_classification,
allow_due_date_synch,
allow_chat_access_mi,
allow_chat_access,
all_product_class_assign,
all_product_assign,
all_department_assign,
agrepo_search_documents,
agrepo_language_code,
address,
acknowledgement_subject,
acknowledgement,
ack_sub_res,
ack_responder,
ack_expected,
ack_comments_length,
account_for_weekend,
account_for_holidays
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP 
WHERE TO_DATE(LS_DB_PARTNER.PROCESSING_DT) < TO_DATE(LS_DB_PARTNER_TMP.PROCESSING_DT)
AND LS_DB_PARTNER.INTEGRATION_ID = LS_DB_PARTNER_TMP.INTEGRATION_ID
AND LS_DB_PARTNER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARTNER_DELETION_TMP  WHERE TABLE_NAME='lsmv_partner')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PARTNER_DELETION_TMP  WHERE TABLE_NAME='lsmv_partner')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PARTNER'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PARTNER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PARTNER'),
                'LSDB','Case','LS_DB_PARTNER',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_PARTNER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
