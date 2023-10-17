
-- USE SCHEMA${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ROLE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ROLE'),
                'LSDB','Case','LS_DB_ROLE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_ROLE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_ROLE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ROLE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ROLE_DELETION_TMP  As select RECORD_ID,'lsmv_role' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_role WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
(

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_role WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ROLE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_role WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_ROLE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_role_SUBSET AS 
(
select * from 
    (SELECT  
    all_accounts_safety  all_accounts_safety,all_contacts_sub  all_contacts_sub,all_inquiry_category_class  all_inquiry_category_class,all_product_class  all_product_class,all_receivers_access  all_receivers_access,all_senders_access  all_senders_access,all_study_safety  all_study_safety,all_study_sub  all_study_sub,all_therapeutic_class  all_therapeutic_class,archive_edit_access  archive_edit_access,assign_all_products  assign_all_products,biblio_all_receiver_access  biblio_all_receiver_access,calllog_all_product_class  calllog_all_product_class,callog_all_products  callog_all_products,callog_all_receiver_access  callog_all_receiver_access,cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,complaints_all_products  complaints_all_products,complaints_all_receiver_access  complaints_all_receiver_access,cr_audit_trail  cr_audit_trail,cr_reports  cr_reports,data_loader  data_loader,date_created  date_created,date_modified  date_modified,default_sender_id  default_sender_id,description  description,dist_create_cr  dist_create_cr,dist_delete_cr  dist_delete_cr,dist_rule_setup  dist_rule_setup,dist_update_cr  dist_update_cr,dms_change_requst_form  dms_change_requst_form,dms_dist_rule_setup_form  dms_dist_rule_setup_form,eslint  eslint,final_activity_edit_access  final_activity_edit_access,final_activity_edit_access_vet  final_activity_edit_access_vet,final_atty_comp_edit_access  final_atty_comp_edit_access,incomplete_trans_edit_access  incomplete_trans_edit_access,irt_archive_arisg_comms  irt_archive_arisg_comms,irt_archive_edit_access  irt_archive_edit_access,monitoring_access  monitoring_access,name  name,oe_all_receivers_access  oe_all_receivers_access,oe_all_senders_access  oe_all_senders_access,oe_assign_all_products  oe_assign_all_products,oe_default_sender_id  oe_default_sender_id,oe_rank1_product_access  oe_rank1_product_access,outbound_all_receivers_access  outbound_all_receivers_access,outbound_all_senders_access  outbound_all_senders_access,outbound_assign_all_products  outbound_assign_all_products,product_read_access  product_read_access,product_type  product_type,rank1_product_access  rank1_product_access,readonly_partner_wfaccess  readonly_partner_wfaccess,record_id  record_id,safety_all_product_class  safety_all_product_class,spr_id  spr_id,sub_all_contacts_access  sub_all_contacts_access,sub_all_product_class  sub_all_product_class,sub_all_receivers_access  sub_all_receivers_access,sub_all_submit_unit_access  sub_all_submit_unit_access,sub_assign_all_products  sub_assign_all_products,system_alert_access  system_alert_access,user_created  user_created,user_modified  user_modified,version  version,vet_all_product_class  vet_all_product_class,view_dataprivacy_fields  view_dataprivacy_fields,view_private_data  view_private_data,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_role
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ROLE_DELETION_TMP  WHERE TABLE_NAME='lsmv_role')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_role_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_role_SUBSET.USER_CREATED CREATED_BY,lsmv_role_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_role_SUBSET.view_private_data  ,lsmv_role_SUBSET.view_dataprivacy_fields  ,lsmv_role_SUBSET.vet_all_product_class  ,lsmv_role_SUBSET.version  ,lsmv_role_SUBSET.user_modified  ,lsmv_role_SUBSET.user_created  ,lsmv_role_SUBSET.system_alert_access  ,lsmv_role_SUBSET.sub_assign_all_products  ,lsmv_role_SUBSET.sub_all_submit_unit_access  ,lsmv_role_SUBSET.sub_all_receivers_access  ,lsmv_role_SUBSET.sub_all_product_class  ,lsmv_role_SUBSET.sub_all_contacts_access  ,lsmv_role_SUBSET.spr_id  ,lsmv_role_SUBSET.safety_all_product_class  ,lsmv_role_SUBSET.record_id  ,lsmv_role_SUBSET.readonly_partner_wfaccess  ,lsmv_role_SUBSET.rank1_product_access  ,lsmv_role_SUBSET.product_type  ,lsmv_role_SUBSET.product_read_access  ,lsmv_role_SUBSET.outbound_assign_all_products  ,lsmv_role_SUBSET.outbound_all_senders_access  ,lsmv_role_SUBSET.outbound_all_receivers_access  ,lsmv_role_SUBSET.oe_rank1_product_access  ,lsmv_role_SUBSET.oe_default_sender_id  ,lsmv_role_SUBSET.oe_assign_all_products  ,lsmv_role_SUBSET.oe_all_senders_access  ,lsmv_role_SUBSET.oe_all_receivers_access  ,lsmv_role_SUBSET.name  ,lsmv_role_SUBSET.monitoring_access  ,lsmv_role_SUBSET.irt_archive_edit_access  ,lsmv_role_SUBSET.irt_archive_arisg_comms  ,lsmv_role_SUBSET.incomplete_trans_edit_access  ,lsmv_role_SUBSET.final_atty_comp_edit_access  ,lsmv_role_SUBSET.final_activity_edit_access_vet  ,lsmv_role_SUBSET.final_activity_edit_access  ,lsmv_role_SUBSET.eslint  ,lsmv_role_SUBSET.dms_dist_rule_setup_form  ,lsmv_role_SUBSET.dms_change_requst_form  ,lsmv_role_SUBSET.dist_update_cr  ,lsmv_role_SUBSET.dist_rule_setup  ,lsmv_role_SUBSET.dist_delete_cr  ,lsmv_role_SUBSET.dist_create_cr  ,lsmv_role_SUBSET.description  ,lsmv_role_SUBSET.default_sender_id  ,lsmv_role_SUBSET.date_modified  ,lsmv_role_SUBSET.date_created  ,lsmv_role_SUBSET.data_loader  ,lsmv_role_SUBSET.cr_reports  ,lsmv_role_SUBSET.cr_audit_trail  ,lsmv_role_SUBSET.complaints_all_receiver_access  ,lsmv_role_SUBSET.complaints_all_products  ,lsmv_role_SUBSET.cdc_operation_type  ,lsmv_role_SUBSET.cdc_operation_time  ,lsmv_role_SUBSET.callog_all_receiver_access  ,lsmv_role_SUBSET.callog_all_products  ,lsmv_role_SUBSET.calllog_all_product_class  ,lsmv_role_SUBSET.biblio_all_receiver_access  ,lsmv_role_SUBSET.assign_all_products  ,lsmv_role_SUBSET.archive_edit_access  ,lsmv_role_SUBSET.all_therapeutic_class  ,lsmv_role_SUBSET.all_study_sub  ,lsmv_role_SUBSET.all_study_safety  ,lsmv_role_SUBSET.all_senders_access  ,lsmv_role_SUBSET.all_receivers_access  ,lsmv_role_SUBSET.all_product_class  ,lsmv_role_SUBSET.all_inquiry_category_class  ,lsmv_role_SUBSET.all_contacts_sub  ,lsmv_role_SUBSET.all_accounts_safety ,CONCAT( NVL(lsmv_role_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_role_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP)
where target_table_name='LS_DB_ROLE'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE   
SET LS_DB_ROLE.view_private_data = LS_DB_ROLE_TMP.view_private_data,LS_DB_ROLE.view_dataprivacy_fields = LS_DB_ROLE_TMP.view_dataprivacy_fields,LS_DB_ROLE.vet_all_product_class = LS_DB_ROLE_TMP.vet_all_product_class,LS_DB_ROLE.version = LS_DB_ROLE_TMP.version,LS_DB_ROLE.user_modified = LS_DB_ROLE_TMP.user_modified,LS_DB_ROLE.user_created = LS_DB_ROLE_TMP.user_created,LS_DB_ROLE.system_alert_access = LS_DB_ROLE_TMP.system_alert_access,LS_DB_ROLE.sub_assign_all_products = LS_DB_ROLE_TMP.sub_assign_all_products,LS_DB_ROLE.sub_all_submit_unit_access = LS_DB_ROLE_TMP.sub_all_submit_unit_access,LS_DB_ROLE.sub_all_receivers_access = LS_DB_ROLE_TMP.sub_all_receivers_access,LS_DB_ROLE.sub_all_product_class = LS_DB_ROLE_TMP.sub_all_product_class,LS_DB_ROLE.sub_all_contacts_access = LS_DB_ROLE_TMP.sub_all_contacts_access,LS_DB_ROLE.spr_id = LS_DB_ROLE_TMP.spr_id,LS_DB_ROLE.safety_all_product_class = LS_DB_ROLE_TMP.safety_all_product_class,LS_DB_ROLE.record_id = LS_DB_ROLE_TMP.record_id,LS_DB_ROLE.readonly_partner_wfaccess = LS_DB_ROLE_TMP.readonly_partner_wfaccess,LS_DB_ROLE.rank1_product_access = LS_DB_ROLE_TMP.rank1_product_access,LS_DB_ROLE.product_type = LS_DB_ROLE_TMP.product_type,LS_DB_ROLE.product_read_access = LS_DB_ROLE_TMP.product_read_access,LS_DB_ROLE.outbound_assign_all_products = LS_DB_ROLE_TMP.outbound_assign_all_products,LS_DB_ROLE.outbound_all_senders_access = LS_DB_ROLE_TMP.outbound_all_senders_access,LS_DB_ROLE.outbound_all_receivers_access = LS_DB_ROLE_TMP.outbound_all_receivers_access,LS_DB_ROLE.oe_rank1_product_access = LS_DB_ROLE_TMP.oe_rank1_product_access,LS_DB_ROLE.oe_default_sender_id = LS_DB_ROLE_TMP.oe_default_sender_id,LS_DB_ROLE.oe_assign_all_products = LS_DB_ROLE_TMP.oe_assign_all_products,LS_DB_ROLE.oe_all_senders_access = LS_DB_ROLE_TMP.oe_all_senders_access,LS_DB_ROLE.oe_all_receivers_access = LS_DB_ROLE_TMP.oe_all_receivers_access,LS_DB_ROLE.name = LS_DB_ROLE_TMP.name,LS_DB_ROLE.monitoring_access = LS_DB_ROLE_TMP.monitoring_access,LS_DB_ROLE.irt_archive_edit_access = LS_DB_ROLE_TMP.irt_archive_edit_access,LS_DB_ROLE.irt_archive_arisg_comms = LS_DB_ROLE_TMP.irt_archive_arisg_comms,LS_DB_ROLE.incomplete_trans_edit_access = LS_DB_ROLE_TMP.incomplete_trans_edit_access,LS_DB_ROLE.final_atty_comp_edit_access = LS_DB_ROLE_TMP.final_atty_comp_edit_access,LS_DB_ROLE.final_activity_edit_access_vet = LS_DB_ROLE_TMP.final_activity_edit_access_vet,LS_DB_ROLE.final_activity_edit_access = LS_DB_ROLE_TMP.final_activity_edit_access,LS_DB_ROLE.eslint = LS_DB_ROLE_TMP.eslint,LS_DB_ROLE.dms_dist_rule_setup_form = LS_DB_ROLE_TMP.dms_dist_rule_setup_form,LS_DB_ROLE.dms_change_requst_form = LS_DB_ROLE_TMP.dms_change_requst_form,LS_DB_ROLE.dist_update_cr = LS_DB_ROLE_TMP.dist_update_cr,LS_DB_ROLE.dist_rule_setup = LS_DB_ROLE_TMP.dist_rule_setup,LS_DB_ROLE.dist_delete_cr = LS_DB_ROLE_TMP.dist_delete_cr,LS_DB_ROLE.dist_create_cr = LS_DB_ROLE_TMP.dist_create_cr,LS_DB_ROLE.description = LS_DB_ROLE_TMP.description,LS_DB_ROLE.default_sender_id = LS_DB_ROLE_TMP.default_sender_id,LS_DB_ROLE.date_modified = LS_DB_ROLE_TMP.date_modified,LS_DB_ROLE.date_created = LS_DB_ROLE_TMP.date_created,LS_DB_ROLE.data_loader = LS_DB_ROLE_TMP.data_loader,LS_DB_ROLE.cr_reports = LS_DB_ROLE_TMP.cr_reports,LS_DB_ROLE.cr_audit_trail = LS_DB_ROLE_TMP.cr_audit_trail,LS_DB_ROLE.complaints_all_receiver_access = LS_DB_ROLE_TMP.complaints_all_receiver_access,LS_DB_ROLE.complaints_all_products = LS_DB_ROLE_TMP.complaints_all_products,LS_DB_ROLE.cdc_operation_type = LS_DB_ROLE_TMP.cdc_operation_type,LS_DB_ROLE.cdc_operation_time = LS_DB_ROLE_TMP.cdc_operation_time,LS_DB_ROLE.callog_all_receiver_access = LS_DB_ROLE_TMP.callog_all_receiver_access,LS_DB_ROLE.callog_all_products = LS_DB_ROLE_TMP.callog_all_products,LS_DB_ROLE.calllog_all_product_class = LS_DB_ROLE_TMP.calllog_all_product_class,LS_DB_ROLE.biblio_all_receiver_access = LS_DB_ROLE_TMP.biblio_all_receiver_access,LS_DB_ROLE.assign_all_products = LS_DB_ROLE_TMP.assign_all_products,LS_DB_ROLE.archive_edit_access = LS_DB_ROLE_TMP.archive_edit_access,LS_DB_ROLE.all_therapeutic_class = LS_DB_ROLE_TMP.all_therapeutic_class,LS_DB_ROLE.all_study_sub = LS_DB_ROLE_TMP.all_study_sub,LS_DB_ROLE.all_study_safety = LS_DB_ROLE_TMP.all_study_safety,LS_DB_ROLE.all_senders_access = LS_DB_ROLE_TMP.all_senders_access,LS_DB_ROLE.all_receivers_access = LS_DB_ROLE_TMP.all_receivers_access,LS_DB_ROLE.all_product_class = LS_DB_ROLE_TMP.all_product_class,LS_DB_ROLE.all_inquiry_category_class = LS_DB_ROLE_TMP.all_inquiry_category_class,LS_DB_ROLE.all_contacts_sub = LS_DB_ROLE_TMP.all_contacts_sub,LS_DB_ROLE.all_accounts_safety = LS_DB_ROLE_TMP.all_accounts_safety,
LS_DB_ROLE.PROCESSING_DT = LS_DB_ROLE_TMP.PROCESSING_DT ,
LS_DB_ROLE.expiry_date    =LS_DB_ROLE_TMP.expiry_date       ,
LS_DB_ROLE.created_by     =LS_DB_ROLE_TMP.created_by        ,
LS_DB_ROLE.created_dt     =LS_DB_ROLE_TMP.created_dt        ,
LS_DB_ROLE.load_ts        =LS_DB_ROLE_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP 
WHERE LS_DB_ROLE.INTEGRATION_ID = LS_DB_ROLE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_ROLE_TMP.PROCESSING_DT = LS_DB_ROLE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,view_private_data,
view_dataprivacy_fields,
vet_all_product_class,
version,
user_modified,
user_created,
system_alert_access,
sub_assign_all_products,
sub_all_submit_unit_access,
sub_all_receivers_access,
sub_all_product_class,
sub_all_contacts_access,
spr_id,
safety_all_product_class,
record_id,
readonly_partner_wfaccess,
rank1_product_access,
product_type,
product_read_access,
outbound_assign_all_products,
outbound_all_senders_access,
outbound_all_receivers_access,
oe_rank1_product_access,
oe_default_sender_id,
oe_assign_all_products,
oe_all_senders_access,
oe_all_receivers_access,
name,
monitoring_access,
irt_archive_edit_access,
irt_archive_arisg_comms,
incomplete_trans_edit_access,
final_atty_comp_edit_access,
final_activity_edit_access_vet,
final_activity_edit_access,
eslint,
dms_dist_rule_setup_form,
dms_change_requst_form,
dist_update_cr,
dist_rule_setup,
dist_delete_cr,
dist_create_cr,
description,
default_sender_id,
date_modified,
date_created,
data_loader,
cr_reports,
cr_audit_trail,
complaints_all_receiver_access,
complaints_all_products,
cdc_operation_type,
cdc_operation_time,
callog_all_receiver_access,
callog_all_products,
calllog_all_product_class,
biblio_all_receiver_access,
assign_all_products,
archive_edit_access,
all_therapeutic_class,
all_study_sub,
all_study_safety,
all_senders_access,
all_receivers_access,
all_product_class,
all_inquiry_category_class,
all_contacts_sub,
all_accounts_safety)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,view_private_data,
view_dataprivacy_fields,
vet_all_product_class,
version,
user_modified,
user_created,
system_alert_access,
sub_assign_all_products,
sub_all_submit_unit_access,
sub_all_receivers_access,
sub_all_product_class,
sub_all_contacts_access,
spr_id,
safety_all_product_class,
record_id,
readonly_partner_wfaccess,
rank1_product_access,
product_type,
product_read_access,
outbound_assign_all_products,
outbound_all_senders_access,
outbound_all_receivers_access,
oe_rank1_product_access,
oe_default_sender_id,
oe_assign_all_products,
oe_all_senders_access,
oe_all_receivers_access,
name,
monitoring_access,
irt_archive_edit_access,
irt_archive_arisg_comms,
incomplete_trans_edit_access,
final_atty_comp_edit_access,
final_activity_edit_access_vet,
final_activity_edit_access,
eslint,
dms_dist_rule_setup_form,
dms_change_requst_form,
dist_update_cr,
dist_rule_setup,
dist_delete_cr,
dist_create_cr,
description,
default_sender_id,
date_modified,
date_created,
data_loader,
cr_reports,
cr_audit_trail,
complaints_all_receiver_access,
complaints_all_products,
cdc_operation_type,
cdc_operation_time,
callog_all_receiver_access,
callog_all_products,
calllog_all_product_class,
biblio_all_receiver_access,
assign_all_products,
archive_edit_access,
all_therapeutic_class,
all_study_sub,
all_study_safety,
all_senders_access,
all_receivers_access,
all_product_class,
all_inquiry_category_class,
all_contacts_sub,
all_accounts_safety
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP 
WHERE TO_DATE(LS_DB_ROLE.PROCESSING_DT) < TO_DATE(LS_DB_ROLE_TMP.PROCESSING_DT)
AND LS_DB_ROLE.INTEGRATION_ID = LS_DB_ROLE_TMP.INTEGRATION_ID
AND LS_DB_ROLE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ROLE_DELETION_TMP  WHERE TABLE_NAME='lsmv_role')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_ROLE_DELETION_TMP  WHERE TABLE_NAME='lsmv_role')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ROLE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_ROLE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_ROLE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ROLE'),
                'LSDB','Case','LS_DB_ROLE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_ROLE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
