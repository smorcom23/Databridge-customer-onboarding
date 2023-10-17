
CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LANGUAGE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LANGUAGE();

CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_RECURRENCE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_RECURRENCE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SERIOUS_EVENTS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SERIOUS_EVENTS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_TASKS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TASKS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_WF_NOTES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_WF_NOTES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_LOCAL_REACTION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_LOCAL_REACTION();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_AE_CASE_TAG
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AE_CASE_TAG();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SAFETY_LATENESS_ASSESSMENT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_LATENESS_ASSESSMENT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_LOCAL_LABELLING
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_LOCAL_LABELLING();



CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ASYNC_WORKFLOW_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ASYNC_WORKFLOW_QUEUE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REQUESTERS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REQUESTERS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ST_LABELLED_PROD_EVENTS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ST_LABELLED_PROD_EVENTS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PATIENT_PAST_DRUG
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT_PAST_DRUG();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_JPN_PRODUCT_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_JPN_PRODUCT_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_AE_SENDER_RECEIVER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AE_SENDER_RECEIVER();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PROD_ACTIVE_SUB_FORM
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PROD_ACTIVE_SUB_FORM();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_E2B_CASE_EXPORT_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_E2B_CASE_EXPORT_QUEUE();




CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_SUPPORT_DOC
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_SUPPORT_DOC();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_QUES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_QUES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WORKFLOW
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WORKFLOW();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LOCAL_LABELING
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LOCAL_LABELING();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_EMAIL_INTAKE_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_EMAIL_INTAKE_QUEUE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_INBOUND_DOCUMENT_DETAILS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_INBOUND_DOCUMENT_DETAILS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_EMAIL_INTAKE_RPT_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_EMAIL_INTAKE_RPT_QUEUE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE_HEALTH_RELATEDNESS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_HEALTH_RELATEDNESS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_APPROVAL_INDICATIONS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_APPROVAL_INDICATIONS();





CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASUALTY_ASSESSMENT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASUALTY_ASSESSMENT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_E2B_SUBMISSIONS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_E2B_SUBMISSIONS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CAPA
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CAPA();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SOURCE_DOCS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE_DOCS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_LOCAL_APPROVAL
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_LOCAL_APPROVAL();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SOURCE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SAFETY_CORRESPONDENCE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_CORRESPONDENCE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_J12_STUDIES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_J12_STUDIES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PARENT_PAST_DRUG
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PARENT_PAST_DRUG();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PROD_INDICATION_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PROD_INDICATION_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_RESEARCH_RPT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_RESEARCH_RPT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_FLAGS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FLAGS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LEBELED_MAPPED_COUNTY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LEBELED_MAPPED_COUNTY();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE_DEVICE_PROB_EVAL
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_DEVICE_PROB_EVAL();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PARTNER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PARTNER();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_THERAPY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY_DER();
END;
$$
;


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_E2B_IMPORT_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_E2B_IMPORT_QUEUE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ACCOUNTS_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ACCOUNTS_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_NARRATIVE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_NARRATIVE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LANG_REDUCTED
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LANG_REDUCTED();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PERSON
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PERSON();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WF_ASSIGNMENT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_ASSIGNMENT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_SERIES_DETAILS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_SERIES_DETAILS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DLIST
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DLIST();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REPORTDUPLICATE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORTDUPLICATE();




CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ACCOUNTS_SERIES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ACCOUNTS_SERIES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUPPORT_DOC
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUPPORT_DOC();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WF_INSTANCE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_INSTANCE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE_IDENTIFIER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_IDENTIFIER();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_STUDY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_STUDY();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PROD_APPROVAL_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PROD_APPROVAL_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_QUALITY_CHECK
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_QUALITY_CHECK();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_AER_CLINICAL_CLASSIFICATION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AER_CLINICAL_CLASSIFICATION();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_CASE_DETAILS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CASE_DETAILS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_CORRESPONDENCE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CORRESPONDENCE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_TEST
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_AER_ADDITIONAL_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AER_ADDITIONAL_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LINKED_REPORT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LINKED_REPORT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PATIENT_PARENT_MED_HIST
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT_PARENT_MED_HIST();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SIMILAR_PRODUCTS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SIMILAR_PRODUCTS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE_THERAPY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_THERAPY();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_HEALTH_DAMAGE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_HEALTH_DAMAGE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_REACT_RELATEDNESS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS_DER();
END;
$$
;


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PREGNANCY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PREGNANCY();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_CASE_REACTION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CASE_REACTION();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PROD_INGREDIENT_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PROD_INGREDIENT_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_STUDY_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_STUDY_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PRODUCT_THERAPEUTIC_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_THERAPEUTIC_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PRD_CHARACTERISTIC
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRD_CHARACTERISTIC();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PATIENT_MED_HIST_EPISODE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT_MED_HIST_EPISODE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SUBMISSION_CASE_PRODUCT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SUBMISSION_CASE_PRODUCT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_INDICATION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INDICATION();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_LOT_NUMBERS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_LOT_NUMBERS();




CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_REACT_LISTEDNESS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_LISTEDNESS();
 -- CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_LISTEDNESS_DER();
END;
$$
;


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_IMRDF_EVALUATION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_IMRDF_EVALUATION();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PRODUCT_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LIB();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CODELIST_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CODELIST_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_JPN_REVIEW
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_JPN_REVIEW();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_NOTES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_NOTES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PATIENT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REPORTER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORTER();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LITERATURE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LITERATURE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WF_NOTES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_NOTES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SENDER_DIAGNOSIS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SENDER_DIAGNOSIS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_AGX_ANG_HISTORY
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AGX_ANG_HISTORY();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_COMPONENT
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_COMPONENT();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_APPROVAL
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_APPROVAL();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DOCUMENT_STORE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DOCUMENT_STORE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_MESSAGE_INFM_AUTH
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ST_BATCH_SUB_QUEUE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ST_BATCH_SUB_QUEUE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_EVENT_GROUP
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_EVENT_GROUP();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REACT_VACCINE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACT_VACCINE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_ACTIVITY_LOG
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_ACTIVITY_LOG();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LINKED_AE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LINKED_AE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_ADDITIONAL_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_ADDITIONAL_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_VERBATIM_TERMS
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_VERBATIM_TERMS();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_RISK_FACTOR
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_RISK_FACTOR();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SIMILAR_INCIDENT_DEVICE
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SIMILAR_INCIDENT_DEVICE();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DEVICE_PROBLEM
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE_PROBLEM();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WF_TRACKER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_TRACKER();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_LOT_BATCH_INFO
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LOT_BATCH_INFO();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_REACT_COMPONENT_PROCEDURES
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_COMPONENT_PROCEDURES();


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PRODDEVICE_UDI
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
as Call  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODDEVICE_UDI();





 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DISTRIBUTION_FORMAT
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DISTRIBUTION_FORMAT();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DISTRIBUTION_FORMAT_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_INGREDIENT
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INGREDIENT();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_INGREDIENT_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_MESSAGE_INFM_AUTH
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MESSAGE_INFM_AUTH_DER();
END;
$$
;



 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_THERAPY
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_THERAPY_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_REACT_LISTEDNESS
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_LISTEDNESS();
  --CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_LISTEDNESS_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_DRUG_REACT_RELATEDNESS
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DRUG_REACT_RELATEDNESS_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PATIENT
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT();
  --CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REACTION
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACTION();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACTION_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SAFETY_MASTER
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_MASTER();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_MASTER_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SOURCE
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SOURCE_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_TEST
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_TEST_DER();
END;
$$
;


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REACTION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACTION();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACTION_DER();
END;
$$
;
CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REPORTER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORTER();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORTER_DER();
END;
$$
;


CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_SAFETY_MASTER
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_MASTER();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_SAFETY_MASTER_DER();
END;
$$
;


 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_WF_TRACKER
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_TRACKER();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_WF_TRACKER_DER();
END;
$$
;

 CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_FACT_RELATED_LABEL_CNTRY
  WAREHOUSE = ${task_warehouse_name}
  Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_FACT_RELATED_LABEL_CNTRY_DER();
END;
$$
;

CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_PRODUCT_LABEL_LIB
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LABEL_LIB();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PRODUCT_LABEL_LIB_DER();
END;
$$
;

CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_OVERALL_LATENESS_REASON
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_OVERALL_LATENESS_REASON();
END;
$$
;

CREATE OR REPLACE TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_REPORT_CLASSIFICATION
WAREHOUSE =${task_warehouse_name} 
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REPORT_CLASSIFICATION();
END;
$$
;



CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CMQ_SMQ
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CMQ_SMQ();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CMQ_SMQ_HIER_TERM_SCOPE();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_MEDDRA_QUERY();
END;
$$
;

CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_DER();
END;
$$
;


CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_INFM_AUTH_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_INFM_AUTH_DER();
END;
$$
;


CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_PROD_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_DER();
END;
$$
;


CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_PROD_REACT_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_PROD_REACT_DER();
END;
$$
;



CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_REACT_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_REACT_DER();
END;
$$
;



CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_ST_MSG_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_ST_MSG_DER();
END;
$$
;

CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_THERAPY_PROD_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_THERAPY_PROD_DER();
END;
$$
;

CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_LS_DB_CASE_WF_TRACKER_DER
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_WF_TRACKER_DER();
END;
$$
;

CREATE or replace TASK ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.TSK_PRC_LS_DB_COUNTRY
WAREHOUSE = ${task_warehouse_name}
Schedule = '${task_schedule_frequency}'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_COUNTRY_REGION();
  CALL ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_COUNTRY();
END;
$$
;

