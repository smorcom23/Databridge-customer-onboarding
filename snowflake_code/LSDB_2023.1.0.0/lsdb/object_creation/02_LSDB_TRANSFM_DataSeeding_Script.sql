USE SCHEMA LSDB_TRANSFM;

CREATE TABLE IF NOT EXISTS LS_DB_DATA_PROCESSING_DTL_TBL (
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


CREATE TABLE IF NOT EXISTS LS_DB_ETL_CONFIG(
ROW_WID	NUMBER(4,0),
TARGET_TABLE_NAME	VARCHAR(100),
PARAM_NAME	VARCHAR(50),
PARAM_VALUE	TIMESTAMP_NTZ(9),
CHAR_PARAM_VALUE	VARCHAR(50),
NUM_PARAM_VALUE	NUMBER(38,0));




insert into LS_DB_ETL_CONFIG 
select (select nvl((max(row_wid)+1),1) from LS_DB_ETL_CONFIG),'HISTORY_CONTROL','KEEP_HISTORY',null,'NO',null;

commit;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;




insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;



insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


