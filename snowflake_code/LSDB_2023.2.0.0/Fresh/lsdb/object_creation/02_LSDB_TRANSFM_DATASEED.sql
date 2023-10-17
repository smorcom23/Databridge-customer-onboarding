
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select nvl((max(row_wid)+1),1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'HISTORY_CONTROL','KEEP_HISTORY',null,'YES',null;


INSERT	INTO	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.etl_language_parameters(  language_code,	default_language,	spr_id	)
VALUES	
(	
  '001',	
  'Y',
   '-9999'	
);

commit;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUPPORT_DOC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SUPPORT_DOC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_INSTANCE ','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_INSTANCE ','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_TRACKER ','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_WF_TRACKER ','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_FACT_RELATED_LABEL_CNTRY ','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_FACT_RELATED_LABEL_CNTRY ','CDC_EXTRACT_TS_UB',current_timestamp,null,null;




INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION (SK_MEDDRA_VERSION_WID,
	MEDDRA_VERSION,
	PROCESSING_DT,
	EXPIRY_DATE,
	CREATED_BY,
	SPR_ID,
	LOAD_TS
)  select 1,'26.0',current_date(),'9999-12-31','LSDB_ETL_TEAM','-9999',current_date();





INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 00:00:00.000','9999-12-31 00:00:00.000',5,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_COMBINED_RELATEDNESS','Related','Company or reporter',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 00:00:00.000','9999-12-31 00:00:00.000',6,NULL,'ARISG','JB_B_CASE_EVENT_W','B_CASE_EVENT_W','DER_CASE_EVENT_SERIOUSNESS','SERIOUS','COMPANY OR REPORTER',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 00:00:00.000','9999-12-31 00:00:00.000',8,NULL,'ARISG','GENERIC','GENERIC','GENERIC','SERIOUSNESS','COMPANY OR REPORTER',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:06.520','2021-01-13 04:54:06.520',14,20000187,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_LACTATION_CASE_BROAD','SMQ_DLCB',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:07.429','9999-12-31 00:00:00.000',16,20000005,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_PREEX_HEPTC_IMPRMNT','HD',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:07.887','9999-12-31 00:00:00.000',17,NULL,'ARISg','GENERIC','GENERIC','E2B','E2B_PRIMARY_SOURCE','NO',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:08.807','9999-12-31 00:00:00.000',19,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','RELATEDNESS','RELATED_REPORTER','Related',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:09.293','9999-12-31 00:00:00.000',20,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','RELATEDNESS','RELATED_COMPANY','Related',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:19.437','9999-12-31 00:00:00.000',21,10022528,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','INT_HLT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:19.949','9999-12-31 00:00:00.000',22,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','INT_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:20.399','9999-12-31 00:00:00.000',23,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','INT_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:20.956','9999-12-31 00:00:00.000',24,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','INT_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:21.471','9999-12-31 00:00:00.000',25,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','INT_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:21.971','9999-12-31 00:00:00.000',26,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','LOE_HLT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:22.396','9999-12-31 00:00:00.000',27,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','LOE_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:23.225','9999-12-31 00:00:00.000',28,20000032,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','LOE_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:23.828','9999-12-31 00:00:00.000',29,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','LOE_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:24.334','9999-12-31 00:00:00.000',30,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','LOE_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:24.840','9999-12-31 00:00:00.000',31,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','MEF_HLT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:25.368','9999-12-31 00:00:00.000',32,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','MEF_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:25.878','9999-12-31 00:00:00.000',33,20000224,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','MEF_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:26.298','9999-12-31 00:00:00.000',34,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','MEF_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:26.803','9999-12-31 00:00:00.000',35,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','MEF_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:27.313','9999-12-31 00:00:00.000',36,10076292,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','OD_HLT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:27.828','9999-12-31 00:00:00.000',37,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','OD_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:28.360','9999-12-31 00:00:00.000',38,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','OD_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:28.815','9999-12-31 00:00:00.000',39,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','OD_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:29.356','9999-12-31 00:00:00.000',40,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','OD_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:29.857','9999-12-31 00:00:00.000',41,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','AF_HLT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:30.376','9999-12-31 00:00:00.000',42,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','AF_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:30.994','9999-12-31 00:00:00.000',43,20000101,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','AF_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:31.612','9999-12-31 00:00:00.000',44,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','AF_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:32.139','9999-12-31 00:00:00.000',45,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','AF_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:32.651','2021-01-13 05:10:27.446',46,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_INTERACTION_FLAG','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:33.145','2021-01-13 05:10:27.898',47,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_LACK_OF_EFFECT_FLAG','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:34.064','2021-01-13 05:10:28.350',48,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_MEDICATION_ERROR_FLAG','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:34.500','2021-01-13 05:10:28.832',49,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_OVERDOSE_FLAG','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-08-30 17:52:34.980','2021-01-13 05:10:29.290',50,NULL,'ARISg','LS_DB_FACT_RELATED_LABEL_CNTRY','LS_DB_FACT_RELATED_LABEL_CNTRY','DER_ABUSE_FLAG','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:23.148','9999-12-31 00:00:00.000',50,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','OLU_PT',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:23.634','9999-12-31 00:00:00.000',51,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','OLU_SMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:24.317','9999-12-31 00:00:00.000',52,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','OLU_CMQ',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 04:54:24.769','9999-12-31 00:00:00.000',53,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','OLU_SOC',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 05:10:25.100','2021-01-13 05:10:25.100',55,NULL,'ARISg','GENERIC','GENERIC','GENERIC','GENERIC_BROAD_NARROW','BROAD',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 05:10:25.100','2021-01-13 05:10:25.100',55,NULL,'ARISg','GENERIC','GENERIC','GENERIC','GENERIC_BROAD_NARROW','BROAD',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 05:10:25.563','2021-01-13 05:10:25.563',56,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_LACTATION_CASE_BROAD','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 05:10:26.008','2021-01-13 05:10:26.008',57,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_PREEX_HEPTC_IMPRMNT','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2021-01-13 05:10:26.981','2021-01-13 05:10:26.981',59,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-09-08 09:45:29.028','9999-12-31 00:00:00.000',62,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_PREEX_RNL_IMPRMNT','CKD_OR_ARF','20000213,20000003',NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-09-08 09:45:29.540','9999-12-31 00:00:00.000',62,NULL,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_PREEX_RNL_IMPRMNT','BROAD_NARROW',NULL,NULL);

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ETL_COLUMN_PARAMETER(
DEFAULT_VALUE_DATE,ETL_DATE_CREATED,ETL_DATE_MODIFIED,CP_ID,DEFAULT_VALUE_INT,SRC_APP_NAME,JOB_NAME,TABLE_NAME,COLUMN_NAME,PARAMETER_NAME,DEFAULT_VALUE_CHAR,SPR_ID) 
VALUES
(NULL,'2023-09-08 11:22:08.898','9999-12-31 00:00:00.000',62,10076291,'ARISg','LS_DB_CASE_DER','LS_DB_CASE_DER','DER_OFF_LABEL_USE_BROAD','OLU_HLT',NULL,NULL);


/*
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_THERAPY_PROD_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_REACT_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_REACT_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_INFM_AUTH_DER SET load_ts='2023-08-29 13:47:23.043'; 
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_PROD_DER SET load_ts='2023-08-29 13:47:23.043'; 
*/




Insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.MASTER_SRL_HUERISTIC_VALUES (SL_NO,DROP_DOWN,RANKS_FILTERED,VALUE) values (1,'Any','all ranks',255255), 
(2,'Serious','ranks 5 - 8',85085),
(3,'Non-serious','ranks 1 - 4',23205),
(4,'Related','ranks 3, 4, 7, 8',51051), 
(5,'Unrelated','ranks 1, 2, 5, 6',19635), 
(6,'Unlisted','ranks 2, 4, 6, 8',36465),  
(7,'Listed','ranks 1, 3, 5, 7',15015),
(8,'Serious + Related','ranks 7, 8',17017), 
(9,'Serious + Unrelated','ranks 5, 6',6545),  
(10,'Serious + Unlisted','ranks 6, 8',12155), 
(11,'Serious + Listed','ranks 5, 7',5005),
(12,'Serious + Related + Unlisted','rank 8',2431),
(13,'Serious + Related + Listed','rank 7',1001),  
(14,'Serious + Unrelated + Unlisted','rank 6',935), 
(15,'Serious + Unrelated + Listed','rank 5',385), 
(16,'Non-serious + Related','ranks 3, 4',4641), 
(17,'Non-serious + Unrelated','ranks 1, 2',1785), 
(18,'Non-serious + Unlisted','ranks 2, 4',3315),  
(19,'Non-serious + Listed','ranks 1, 3',1365),
(20,'Non-serious + Related + Unlisted','rank 4',663), 
(21,'Non-serious + Related + Listed','rank 3',273), 
(22,'Non-serious + Unrelated + Unlisted','rank 2',255), 
(23,'Non-serious + Unrelated + Listed','rank 1',105);


Insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CODELIST_METADATA (LS_DB_ENTITY_NAME,LS_DB_DECODE_ATTR_NAME,LS_DB_CODE_ATTR_NAME,CODELIST_ID) 
values ('LS_DB_DRUG','VACCINATION_MILITARY_FLAG_DE_ML','VACCINATION_MILITARY_FLAG','1002')
,('LS_DB_DRUG_REACT_RELATEDNESS','AESI_MANUAL_DE_ML','AESI_MANUAL','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','START_LATENCY_MANUAL_DE_ML','START_LATENCY_MANUAL','9941')
,('LS_DB_DRUG_THERAPY','DGTH_DRUG_THERAPY_ON_GOING_DE_ML','DGTH_DRUG_THERAPY_ON_GOING','1002')
,('LS_DB_DRUG_THERAPY','DGTH_TOTAL_DOSE_UNIT_DE_ML','DGTH_TOTAL_DOSE_UNIT','1018')
,('LS_DB_DRUG_THERAPY','DGTHVAC_VACCINATION_STATE_DE_ML','DGTHVAC_VACCINATION_STATE','9171')
,('LS_DB_JPN_REVIEW','JPPVREW_CASE_DUE_MANUAL_DE_ML','JPPVREW_CASE_DUE_MANUAL','7077')
,('LS_DB_CASUALTY_ASSESSMENT','ASSESS_RELSHIP_DE_ML','ASSESS_RELSHIP','1002')
,('LS_DB_CASUALTY_ASSESSMENT','CAUSALITY_SOURCE_DE_ML','CAUSALITY_SOURCE','9055')
,('LS_DB_DEVICE','DVC_FOLLOW_UP_TYPE_DE_ML','DVC_FOLLOW_UP_TYPE','9752')
,('LS_DB_DEVICE','DVC_IF_NOT_RETURNED_DE_ML','DVC_IF_NOT_RETURNED','10047')
,('LS_DB_DEVICE','DVC_IF_REMEDIAL_ACTION_INITIATED_DE_ML','DVC_IF_REMEDIAL_ACTION_INITIATED','9749')
,('LS_DB_DEVICE','DVC_SUBMITTER_OF_REPORT_DE_ML','DVC_SUBMITTER_OF_REPORT','9979')
,('LS_DB_DEVICE','DVC_WHEN_DID_DEVICE_PROBLEM_OCCUR_DE_ML','DVC_WHEN_DID_DEVICE_PROBLEM_OCCUR','829')
,('LS_DB_DEVICE_PROBLEM','SERIOUSNESS_DEVICE_PROBLEM_DE_ML','SERIOUSNESS_DEVICE_PROBLEM','1002')
,('LS_DB_DRUG','CAUSED_BY_LO_EFFECT_DE_ML','CAUSED_BY_LO_EFFECT','1002')
,('LS_DB_DRUG','DRUGADMINISTRATIONROUTE_DE_ML','DRUGADMINISTRATIONROUTE','1020')
,('LS_DB_DRUG','INVESTIGATIONAL_PROD_BLINDED_DE_ML','INVESTIGATIONAL_PROD_BLINDED','1002')
,('LS_DB_DRUG','PATERNAL_EXPOSURE_DE_ML','PATERNAL_EXPOSURE','4')
,('LS_DB_DRUG','PRIMARY_SUSPECT_DRUG_DE_ML','PRIMARY_SUSPECT_DRUG','1002')
,('LS_DB_REACTION','CONGENITALANOMALY_DE_ML','CONGENITALANOMALY','1002')
,('LS_DB_REACTION','DEATH_DE_ML','DEATH','1002')
,('LS_DB_REACTION','DETECTED_COUNTRY_DE_ML','DETECTED_COUNTRY','1015')
,('LS_DB_REACTION','DEVICE_CRITERIA_DE_ML','DEVICE_CRITERIA','10005')
,('LS_DB_REACTION','DRUG_INTERACTION_DE_ML','DRUG_INTERACTION','1002')
,('LS_DB_REACTION','EVENT_OCCUR_LOCATION_DE_ML','EVENT_OCCUR_LOCATION','9864')
,('LS_DB_REACTION','IS_DESIG_MEDICAL_EVENT_DE_ML','IS_DESIG_MEDICAL_EVENT','1002')
,('LS_DB_REACTION','IS_EVENT_AESI_DE_ML','IS_EVENT_AESI','9941')
,('LS_DB_REPORTER','IS_HEALTH_PROF_DE_ML','IS_HEALTH_PROF','1002')
,('LS_DB_REPORTER','REPORTER_SENT_TO_FDA_DE_ML','REPORTER_SENT_TO_FDA','1008')
,('LS_DB_RISK_FACTOR','RISK_FACTOR_DE_ML','RISK_FACTOR','9650')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATESUBCOMP_RCA_CAPA_REQUIRED_DE_ML','LATESUBCOMP_RCA_CAPA_REQUIRED','1002')
,('LS_DB_SAFETY_MASTER','AERINFO_SCRIPT_TYPE_DE_ML','AERINFO_SCRIPT_TYPE','703')
,('LS_DB_SAFETY_MASTER','MSG_INBOUND_ARCHIVE_DE_ML','MSG_INBOUND_ARCHIVE','9058')
,('LS_DB_SAFETY_MASTER','MSG_SOURCE_AE_FORMTYPE_DE_ML','MSG_SOURCE_AE_FORMTYPE','9034')
,('LS_DB_SAFETY_MASTER','RECPT_RECEIPT_CATEGORY_DE_ML','RECPT_RECEIPT_CATEGORY','8012')
,('LS_DB_LITERATURE','LITAUTH_AUTHOR_TITLE_DE_ML','LITAUTH_AUTHOR_TITLE','5014')
,('LS_DB_PATIENT','PAR_PARENTAGEUNIT_DE_ML','PAR_PARENTAGEUNIT','1016')
,('LS_DB_PATIENT','PAT_BIRTH_WEIGHT_UNIT_DE_ML','PAT_BIRTH_WEIGHT_UNIT','48')
,('LS_DB_PATIENT','PAT_GENDER_DE_ML','PAT_GENDER','10117')
,('LS_DB_PATIENT','PAT_PATHEIGHT_UNIT_CODE_DE_ML','PAT_PATHEIGHT_UNIT_CODE','347')
,('LS_DB_PATIENT','PAT_PATIENT_MILITARY_STATUS_DE_ML','PAT_PATIENT_MILITARY_STATUS','9169')
,('LS_DB_PATIENT','PAT_PATINITIAL_TYPE_CODE_DE_ML','PAT_PATINITIAL_TYPE_CODE','372')
,('LS_DB_PATIENT_MED_HIST_EPISODE','CODING_TYPE_DE_ML','CODING_TYPE','158')
,('LS_DB_PREGNANCY','NEON_WHICH_PREGNANCY_DE_ML','NEON_WHICH_PREGNANCY','9986')
,('LS_DB_PREGNANCY','PREG_CONTRACEPTIVE_FAILURE_DE_ML','PREG_CONTRACEPTIVE_FAILURE','1002')
,('LS_DB_AER_ADDITIONAL_INFO','OTHER_IDENTIFICATION_REF_DE_ML','OTHER_IDENTIFICATION_REF','2032')
,('LS_DB_CASUALTY_ASSESSMENT','MAH_INIT_REP_OR_EVAL_RES_CN_DE_ML','MAH_INIT_REP_OR_EVAL_RES_CN','10077')
,('LS_DB_DEVICE','DVC_DEVICE_EVALUATEBY_MANUFACTURER_DE_ML','DVC_DEVICE_EVALUATEBY_MANUFACTURER','9862')
,('LS_DB_DEVICE','DVC_IS_PROD_COMBINATION_DE_ML','DVC_IS_PROD_COMBINATION','7077')
,('LS_DB_DEVICE','DVC_MAL_FUNCTION_DE_ML','DVC_MAL_FUNCTION','9606')
,('LS_DB_DEVICE','DVC_THIRD_PARTY_SERVICE_DE_ML','DVC_THIRD_PARTY_SERVICE','1008')
,('LS_DB_DEVICE_PROBLEM','DEVICE_PROB_SUSPECT_RISK_DE_ML','DEVICE_PROB_SUSPECT_RISK','10063')
,('LS_DB_DRUG','COMPONENT_TYPE_DE_ML','COMPONENT_TYPE','10046')
,('LS_DB_DRUG','DECHALLENGE_DE_ML','DECHALLENGE','1027')
,('LS_DB_DRUG','DRUG_VIGILANCETYPE_DE_ML','DRUG_VIGILANCETYPE','10020')
,('LS_DB_DRUG','DRUGCHARACTERIZATION_DE_ML','DRUGCHARACTERIZATION','1013')
,('LS_DB_DRUG','DRUGSTARTPERIODUNIT_DE_ML','DRUGSTARTPERIODUNIT','17')
,('LS_DB_DRUG','IS_CODED_DE_ML','IS_CODED','7077')
,('LS_DB_DRUG','REACTGESTATIONPERIODUNIT_DE_ML','REACTGESTATIONPERIODUNIT','11')
,('LS_DB_DRUG','SPECIALIZED_PRODUCT_CATEGORY_DE_ML','SPECIALIZED_PRODUCT_CATEGORY','9164')
,('LS_DB_DRUG','TOLERATED_DE_ML','TOLERATED','1002')
,('LS_DB_DRUG_REACT_RELATEDNESS','ACCESS_RELATIONSHIP_DE_ML','ACCESS_RELATIONSHIP','1002')
,('LS_DB_DRUG_REACT_RELATEDNESS','OUTCOME_AFTER_CHANGE_DE_ML','OUTCOME_AFTER_CHANGE','1012')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DEVICE_PROBLEM_REPORT_OUTCOME_DE_ML','SAFTYRPT_DEVICE_PROBLEM_REPORT_OUTCOME','10068')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DUPLICATE_DE_ML','SAFTYRPT_DUPLICATE','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_LATEST_AER_DE_ML','SAFTYRPT_LATEST_AER','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_LIFETHREATENING_DE_ML','SAFTYRPT_LIFETHREATENING','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_MEDHIST_CODED_FLAG_DE_ML','SAFTYRPT_MEDHIST_CODED_FLAG','35')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REQUIRED_INTERVENATION_DE_ML','SAFTYRPT_REQUIRED_INTERVENATION','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SAFETY_TEAM_DE_ML','SAFTYRPT_SAFETY_TEAM','502')
,('LS_DB_SOURCE','SOURCE_DE_ML','SOURCE','346')
,('LS_DB_SOURCE_DOCS','CATEGORY_CODE_DE_ML','CATEGORY_CODE','316')
,('LS_DB_STUDY','STDY_BLINDING_TECHNIQUE_DE_ML','STDY_BLINDING_TECHNIQUE','134')
,('LS_DB_STUDY','STDY_OTHER_STUDY_DE_ML','STDY_OTHER_STUDY','9929')
,('LS_DB_STUDY','STDY_STUDY_SUBJECT_FATHER_DE_ML','STDY_STUDY_SUBJECT_FATHER','1002')
,('LS_DB_TEST','TEST_RESULT_CODE_DE_ML','TEST_RESULT_CODE','9107')
,('LS_DB_PREGNANCY','NEON_ADMISSION_DURATION_UNIT_DE_ML','NEON_ADMISSION_DURATION_UNIT','9987')
,('LS_DB_PREGNANCY','NEON_NICU_ADMISSION_DE_ML','NEON_NICU_ADMISSION','1002')
,('LS_DB_PREGNANCY','NEONCHILD_NEON_BIRTH_LEN_UNIT_DE_ML','NEONCHILD_NEON_BIRTH_LEN_UNIT','347')
,('LS_DB_PREGNANCY','PREG_CONTRACEPTIVES_USED_DE_ML','PREG_CONTRACEPTIVES_USED','1002')
,('LS_DB_PREGNANCY','PREG_PREGNANCY_CONFIRM_MODE_DE_ML','PREG_PREGNANCY_CONFIRM_MODE','8108')
,('LS_DB_PREGNANCY','PREPREGOCOM_PAST_PREGNANCY_OUTCOME_DE_ML','PREPREGOCOM_PAST_PREGNANCY_OUTCOME','8120')
,('LS_DB_REACTION','IS_IME_DE_ML','IS_IME','9941')
,('LS_DB_REACTION','LOCATION_DE_ML','LOCATION','10130')
,('LS_DB_REACTION','MEDICALLYSIGNIFICANT_DE_ML','MEDICALLYSIGNIFICANT','4')
,('LS_DB_REACTION','REACTFIRSTTIMEUNIT_DE_ML','REACTFIRSTTIMEUNIT','1017')
,('LS_DB_REACTION','REACTOUTCOME_COMPANY_DE_ML','REACTOUTCOME_COMPANY','1012')
,('LS_DB_REACTION','SER_PUBLIC_HEALTH_THREAT_DE_ML','SER_PUBLIC_HEALTH_THREAT','1002')
,('LS_DB_REACT_VACCINE','DEPARTMENT_DE_ML','DEPARTMENT','1002')
,('LS_DB_REPORTDUPLICATE','PREVIOUSLY_REPORTED_DE_ML','PREVIOUSLY_REPORTED','1002')
,('LS_DB_REPORTER','REPORTER_OR_CONTACT_DE_ML','REPORTER_OR_CONTACT','9740')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATEINFOINBO_RCA_CAPA_REQUIRED_DE_ML','LATEINFOINBO_RCA_CAPA_REQUIRED','1002')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATESUBCOMP_LATENESS_REASON_DE_ML','LATESUBCOMP_LATENESS_REASON','801')
,('LS_DB_SAFETY_MASTER','AERINFO_CALL_DIRECTION_DE_ML','AERINFO_CALL_DIRECTION','704')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGDOSAGEFORM_DE_ML','DGTH_DRUGDOSAGEFORM','805')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGLASTPERIODUNIT_DE_ML','DGTH_DRUGLASTPERIODUNIT','17')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGPARADMINISTRATION_DE_ML','DGTH_DRUGPARADMINISTRATION','1020')
,('LS_DB_HEALTH_DAMAGE','SERIOUSNESS_HEALATH_DAMAGE_DE_ML','SERIOUSNESS_HEALATH_DAMAGE','1002')
,('LS_DB_JPN_REVIEW','JPREVCON_AE_PREG_SHEET_RCD_DE_ML','JPREVCON_AE_PREG_SHEET_RCD','10034')
,('LS_DB_JPN_REVIEW','JPREVCON_AGGREMENT_FORM_TYPE_DE_ML','JPREVCON_AGGREMENT_FORM_TYPE','10030')
,('LS_DB_JPN_REVIEW','JPREVCON_STATUS_DE_ML','JPREVCON_STATUS','10029')
,('LS_DB_LANG_REDUCTED','DO_NOT_USE_ANG_DE_ML','DO_NOT_USE_ANG','7077')
,('LS_DB_PATIENT','PAR_PARENTSEX_DE_ML','PAR_PARENTSEX','1007')
,('LS_DB_PATIENT_MED_HIST_EPISODE','CONDITION_TREATED_DE_ML','CONDITION_TREATED','1002')
,('LS_DB_PATIENT_MED_HIST_EPISODE','E2B_R3_FAMILYHIST_DE_ML','E2B_R3_FAMILYHIST','1021')
,('LS_DB_CAPA','CAPLATE_MONITORING_TYPE_DE_ML','CAPLATE_MONITORING_TYPE','10103')
,('LS_DB_CASUALTY_ASSESSMENT','MAH_INIT_REP_OR_EVAL_SRC_CN_DE_ML','MAH_INIT_REP_OR_EVAL_SRC_CN','10076')
,('LS_DB_DEVICE','DVC_CLASS_OF_DEVICE_DE_ML','DVC_CLASS_OF_DEVICE','10057')
,('LS_DB_DEVICE','DVC_CONDITIONAL_TIME_LIMITED_AUTHORIZATION_DE_ML','DVC_CONDITIONAL_TIME_LIMITED_AUTHORIZATION','10118')
,('LS_DB_DEVICE','DVC_FOLLOW_UP_ADD_INFO_DE_ML','DVC_FOLLOW_UP_ADD_INFO','1021')
,('LS_DB_DEVICE','DVC_FOLLOW_UP_RESPONSE_TO_FDA_DE_ML','DVC_FOLLOW_UP_RESPONSE_TO_FDA','1021')
,('LS_DB_DEVICE','DVC_REASON_EVALUATION_NOT_PROVIDED_DE_ML','DVC_REASON_EVALUATION_NOT_PROVIDED','9863')
,('LS_DB_DEVICE','DVC_REPORT_SENT_TO_FDA_DE_ML','DVC_REPORT_SENT_TO_FDA','1002')
,('LS_DB_DEVICE','DVC_RISK_ASSESSMENT_REVIEWED_DE_ML','DVC_RISK_ASSESSMENT_REVIEWED','1002')
,('LS_DB_DEVICE_DEVICE_PROB_EVAL','DVCPRBEVALIMDRF_EVALUATION_TYPE_IMDRF_DE_ML','DVCPRBEVALIMDRF_EVALUATION_TYPE_IMDRF','9945')
,('LS_DB_DRUG','DRUGAUTHORIZATIONCOUNTRY_DE_ML','DRUGAUTHORIZATIONCOUNTRY','1015')
,('LS_DB_DRUG','NOT_REPORTABLE_JPN_DE_ML','NOT_REPORTABLE_JPN','9941')
,('LS_DB_DRUG','PROD_REF_REQUIRED_DE_ML','PROD_REF_REQUIRED','1002')
,('LS_DB_DRUG','PROTOCOL_PRODUCT_TYPE_DE_ML','PROTOCOL_PRODUCT_TYPE','8008')
,('LS_DB_SAFETY_MASTER','AERINFO_E2B_CLASSIFICATION_TYPE_CODE_DE_ML','AERINFO_E2B_CLASSIFICATION_TYPE_CODE','9136')
,('LS_DB_SAFETY_MASTER','AERINFO_PMAE_TYPE_CODE_DE_ML','AERINFO_PMAE_TYPE_CODE','314')
,('LS_DB_SAFETY_MASTER','MSG_AER_APPROVAL_STATUS_DE_ML','MSG_AER_APPROVAL_STATUS','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_ADDITIONAL_DOC_DE_ML','SAFTYRPT_ADDITIONAL_DOC','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CASE_CODED_DE_ML','SAFTYRPT_CASE_CODED','9143')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DRUG_MFG_CODED_FLAG_DE_ML','SAFTYRPT_DRUG_MFG_CODED_FLAG','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_OCCURCOUNTRY_DE_ML','SAFTYRPT_OCCURCOUNTRY','1015')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_OTHER_DE_ML','SAFTYRPT_OTHER','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_PMDA_DEVICE_REPORT_TYPE_DE_ML','SAFTYRPT_PMDA_DEVICE_REPORT_TYPE','10067')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REPORT_CLASSIFICATION_DE_ML','SAFTYRPT_REPORT_CLASSIFICATION','8140')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REPORTTYPE_DE_ML','SAFTYRPT_REPORTTYPE','1001')
,('LS_DB_STUDY','STDY_INVEST_PRODUCT_BLINDED_DE_ML','STDY_INVEST_PRODUCT_BLINDED','4')
,('LS_DB_STUDY','STDY_PATIENT_EXPOSURE_DE_ML','STDY_PATIENT_EXPOSURE','136')
,('LS_DB_STUDY','STDY_STUDY_PHASE_DE_ML','STDY_STUDY_PHASE','133')
,('LS_DB_STUDY','STDY_STUDY_SUBJECT_MOTHER_DE_ML','STDY_STUDY_SUBJECT_MOTHER','1002')
,('LS_DB_STUDY','STDY_TYPE_OF_DRUG_DE_ML','STDY_TYPE_OF_DRUG','340')
,('LS_DB_PATIENT_MED_HIST_EPISODE','CONCOMITANT_THERAPIES_DE_ML','CONCOMITANT_THERAPIES','1021')
,('LS_DB_PATIENT_MED_HIST_EPISODE','DURATION_UNIT_DE_ML','DURATION_UNIT','1017')
,('LS_DB_PATIENT_MED_HIST_EPISODE','MEDICALCONTINUE_DE_ML','MEDICALCONTINUE','1002')
,('LS_DB_PATIENT_PARENT_MED_HIST','DURATION_UNIT_DE_ML','DURATION_UNIT','1017')
,('LS_DB_PATIENT_PARENT_MED_HIST','MEDICALCONTINUE_DE_ML','MEDICALCONTINUE','1002')
,('LS_DB_PREGNANCY','NEON_CURRENT_PREGNANCY_CHECKBOX_DE_ML','NEON_CURRENT_PREGNANCY_CHECKBOX','9970')
,('LS_DB_PREGNANCY','NEON_GESTATIONAL_AGE_BIRTH_UNIT_DE_ML','NEON_GESTATIONAL_AGE_BIRTH_UNIT','11')
,('LS_DB_PREGNANCY','NEONCHILD_CHILD_UNIT_DE_ML','NEONCHILD_CHILD_UNIT','11')
,('LS_DB_PREGNANCY','NEONCHILD_HEAD_CIRCUM_BIRTH_UNIT_DE_ML','NEONCHILD_HEAD_CIRCUM_BIRTH_UNIT','347')
,('LS_DB_PREGNANCY','PREGOCOM_LIVE_BIRTH_COMPLICATIONS_DE_ML','PREGOCOM_LIVE_BIRTH_COMPLICATIONS','8105')
,('LS_DB_REACTION','CONDITION_DE_ML','CONDITION','10129')
,('LS_DB_REACTION','INTERVENTIONREQUIRED_DE_ML','INTERVENTIONREQUIRED','1002')
,('LS_DB_REACTION','IS_EVENT_MANUAL_DE_ML','IS_EVENT_MANUAL','9941')
,('LS_DB_REACTION','LIFETHREATENING_DE_ML','LIFETHREATENING','1002')
,('LS_DB_DRUG','VACCINE_GIVEN_DE_ML','VACCINE_GIVEN','9165')
,('LS_DB_DRUG_REACT_LISTEDNESS','IS_MANNUAL_DE_ML','IS_MANNUAL','7077')
,('LS_DB_DRUG_REACT_LISTEDNESS','SUSAR_MANUAL_DE_ML','SUSAR_MANUAL','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','DRUGRESULT_DE_ML','DRUGRESULT','9062')
,('LS_DB_DRUG_THERAPY','DGTH_OVERDOSE_RESULT_SAE_DE_ML','DGTH_OVERDOSE_RESULT_SAE','4')
,('LS_DB_DRUG_THERAPY','DGTH_THERAPHY_SITE_DE_ML','DGTH_THERAPHY_SITE','9992')
,('LS_DB_DRUG_THERAPY','DGTHVAC_VACCINATION_TYPE_DE_ML','DGTHVAC_VACCINATION_TYPE','9163')
,('LS_DB_JPN_REVIEW','JPPVREW_TEST_RESULT_RECEIVED_DE_ML','JPPVREW_TEST_RESULT_RECEIVED','7077')
,('LS_DB_NARRATIVE','SMRY_DO_NOT_USE_ANG_DE_ML','SMRY_DO_NOT_USE_ANG','7077')
,('LS_DB_NARRATIVE','SMRYRPTCMTL_COMMENTS_LANG_DE_ML','SMRYRPTCMTL_COMMENTS_LANG','9065')
,('LS_DB_PATIENT','PAR_PARENTHEIGHT_UNIT_DE_ML','PAR_PARENTHEIGHT_UNIT','347')
,('LS_DB_PATIENT','PAT_COUNTRY_CN_DE_ML','PAT_COUNTRY_CN','1015')
,('LS_DB_PATIENT','PAT_DONOTREPORTNAME_DE_ML','PAT_DONOTREPORTNAME','9941')
,('LS_DB_PATIENT','PAT_OK_TO_SHARE_DETAILS_DE_ML','PAT_OK_TO_SHARE_DETAILS','8101')
,('LS_DB_PATIENT','PAT_PATONSETAGEUNIT_DE_ML','PAT_PATONSETAGEUNIT','1016')
,('LS_DB_PATIENT','PAT_PATSEX_DE_ML','PAT_PATSEX','1007')
,('LS_DB_PATIENT','PAT_SPANISH_STATE_DE_ML','PAT_SPANISH_STATE','8147')
,('LS_DB_AE_SENDER_RECEIVER','E2BAECASESNDR_TITLE_DE_ML','E2BAECASESNDR_TITLE','5014')
,('LS_DB_CASUALTY_ASSESSMENT','DRUGRESULT_DE_ML','DRUGRESULT','9062')
,('LS_DB_CASUALTY_ASSESSMENT','METHODS_DE_ML','METHODS','9641')
,('LS_DB_DEVICE','DVC_CURRENT_DEV_LOCATION_DE_ML','DVC_CURRENT_DEV_LOCATION','9978')
,('LS_DB_DEVICE','DVC_NOMENCLATURE_SYSTEM_DE_ML','DVC_NOMENCLATURE_SYSTEM','9925')
,('LS_DB_DEVICE','DVC_RISK_ASSMENT_BEEN_REVIEWED_DE_ML','DVC_RISK_ASSMENT_BEEN_REVIEWED','1002')
,('LS_DB_REACTION','REACTLASTTIMEUNIT_DE_ML','REACTLASTTIMEUNIT','1017')
,('LS_DB_REACTION','TERM_ADDED_BY_DE_ML','TERM_ADDED_BY','9970')
,('LS_DB_REACT_VACCINE','HEALTHCARE_PROFESSIONAL_DE_ML','HEALTHCARE_PROFESSIONAL','1002')
,('LS_DB_REPORTER','OTHERHCP_KR_DE_ML','OTHERHCP_KR','9927')
,('LS_DB_REPORTER','SPANISH_STATE_DE_ML','SPANISH_STATE','8147')
,('LS_DB_REPORTER','TYPE_DE_ML','TYPE','8112')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATECASEACT_LATENESS_REASON_DE_ML','LATECASEACT_LATENESS_REASON','801')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATEINFOINBO_LATENESS_REASON_DE_ML','LATEINFOINBO_LATENESS_REASON','10100')
,('LS_DB_SAFETY_MASTER','AERINFO_ONLINE_STATUS_DE_ML','AERINFO_ONLINE_STATUS','701')
,('LS_DB_SAFETY_MASTER','MSG_PRIORITY_DE_ML','MSG_PRIORITY','145')
,('LS_DB_SAFETY_MASTER','MSG_REMOVE_REASON_CODE_DE_ML','MSG_REMOVE_REASON_CODE','9026')
,('LS_DB_SAFETY_MASTER','RECPT_ICSR_FLAG_DE_ML','RECPT_ICSR_FLAG','9131')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CASE_TYPE_DE_ML','SAFTYRPT_CASE_TYPE','9865')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_INITIAL_SENDER_DE_ML','SAFTYRPT_INITIAL_SENDER','9125')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_LATEST_AER_APPROVED_DE_ML','SAFTYRPT_LATEST_AER_APPROVED','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_MANUAL_FOR_CASE_SIGNIFICANCE_DE_ML','SAFTYRPT_MANUAL_FOR_CASE_SIGNIFICANCE','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REPORT_CLASSIFICATION_CN_DE_ML','SAFTYRPT_REPORT_CLASSIFICATION_CN','10073')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SERIOUSNESS_COMPANY_DE_ML','SAFTYRPT_SERIOUSNESS_COMPANY','1002')
,('LS_DB_STUDY','STDY_CODE_BROKEN_DE_ML','STDY_CODE_BROKEN','54')
,('LS_DB_STUDY','STDY_EUCT_REGULATION_DE_ML','STDY_EUCT_REGULATION','7077')
,('LS_DB_PATIENT','PAR_CONSENT_TO_CONTACT_PARENT_DE_ML','PAR_CONSENT_TO_CONTACT_PARENT','7073')
,('LS_DB_PATIENT','PAR_PARENT_AGE_AT_VACCINE_UNIT_DE_ML','PAR_PARENT_AGE_AT_VACCINE_UNIT','1016')
,('LS_DB_PATIENT','PAT_NATIONALITIES_CN_DE_ML','PAT_NATIONALITIES_CN','10078')
,('LS_DB_PATIENT','PAT_PREGNANT_AT_TIME_OF_VACCINE_DE_ML','PAT_PREGNANT_AT_TIME_OF_VACCINE','1002')
,('LS_DB_PATIENT_MED_HIST_EPISODE','DISEASE_TYPE_DE_ML','DISEASE_TYPE','9917')
,('LS_DB_PATIENT_MED_HIST_EPISODE','INCLUDE_IN_ANG_DE_ML','INCLUDE_IN_ANG','9970')
,('LS_DB_PREGNANCY','PREG_EXPOSURE_STATUS_DE_ML','PREG_EXPOSURE_STATUS','8119')
,('LS_DB_PREGNANCY','PREGOCOM_METHOD_OF_DELIVERY_DE_ML','PREGOCOM_METHOD_OF_DELIVERY','951')
,('LS_DB_PREGNANCY','PREGOCOM_NEON_BIRT_LENG_UNIT_DE_ML','PREGOCOM_NEON_BIRT_LENG_UNIT','347')
,('LS_DB_DRUG_ADDITIONAL_INFO','ADDITIONAL_INFO_CODE_DE_ML','ADDITIONAL_INFO_CODE','9063')
,('LS_DB_DRUG','ACTIONDRUG_DE_ML','ACTIONDRUG','21')
,('LS_DB_DRUG','BIO_FATHER_DRUG_DE_ML','BIO_FATHER_DRUG','1002')
,('LS_DB_DRUG','MOST_SUSPECT_PRODUCT_DE_ML','MOST_SUSPECT_PRODUCT','7077')
,('LS_DB_DRUG','OBTAINDRUGCOUNTRY_DE_ML','OBTAINDRUGCOUNTRY','1015')
,('LS_DB_DRUG','PRODUCT_TYPE_DE_ML','PRODUCT_TYPE','5015')
,('LS_DB_DRUG_APPROVAL','TYPE_OF_DEVICE_DE_ML','TYPE_OF_DEVICE','10127')
,('LS_DB_DRUG_THERAPY','DGTH_ACTIONDRUG_DE_ML','DGTH_ACTIONDRUG','21')
,('LS_DB_DRUG_THERAPY','DGTH_IS_SAMPLE_AVAILABLE_DE_ML','DGTH_IS_SAMPLE_AVAILABLE','1002')
,('LS_DB_HEALTH_DAMAGE','REGENERATIVE_SERIOUSNESS_CRITERIA_DE_ML','REGENERATIVE_SERIOUSNESS_CRITERIA','10120')
,('LS_DB_HEALTH_DAMAGE','SERIOUSNESS_HEALATH_OUTCOME_DE_ML','SERIOUSNESS_HEALATH_OUTCOME','10068')
,('LS_DB_JPN_REVIEW','JPPVREW_PMDA_SUBMISIION_DUE_MANUAL_DE_ML','JPPVREW_PMDA_SUBMISIION_DUE_MANUAL','7077')
,('LS_DB_JPN_REVIEW','JPREVCON_CONTRACT_EXECUTION_DE_ML','JPREVCON_CONTRACT_EXECUTION','10031')
,('LS_DB_LITERATURE','LIT_RETAINLITERATUREREFERENCE_DE_ML','LIT_RETAINLITERATUREREFERENCE','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_PARTIAL_CASE_CODED_DE_ML','SAFTYRPT_PARTIAL_CASE_CODED','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REPORT_CLASSIFICATION_CATEGORY_DE_ML','SAFTYRPT_REPORT_CLASSIFICATION_CATEGORY','9747')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SAFETY_REPORT_TYPE_DE_ML','SAFTYRPT_SAFETY_REPORT_TYPE','10050')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SUSAR_DE_ML','SAFTYRPT_SUSAR','7077')
,('LS_DB_STUDY','STDY_STUDY_CODE_BROKEN_DE_ML','STDY_STUDY_CODE_BROKEN','54')
,('LS_DB_TEST','MOREINFORMATION_DE_ML','MOREINFORMATION','1002')
,('LS_DB_AER_CLINICAL_CLASSIFICATION','CLINICAL_YES_NO_JPN_DE_ML','CLINICAL_YES_NO_JPN','9958')
,('LS_DB_CASUALTY_ASSESSMENT','ASSESS_METHOD_KR_DE_ML','ASSESS_METHOD_KR','9930')
,('LS_DB_REACTION','ANTICIPATED_EVENTS_DE_ML','ANTICIPATED_EVENTS','9941')
,('LS_DB_REACTION','EVENT_TYPE_DE_ML','EVENT_TYPE','9745')
,('LS_DB_REACTION','HOSP_PROLONGED_DE_ML','HOSP_PROLONGED','4')
,('LS_DB_REACTION','MANUAL_DURATION_DE_ML','MANUAL_DURATION','7077')
,('LS_DB_REPORTER','CONSENT_CONTACT_DE_ML','CONSENT_CONTACT','1002')
,('LS_DB_REPORTER','E2B_R3_REGULATORY_PURPOSE_DE_ML','E2B_R3_REGULATORY_PURPOSE','4')
,('LS_DB_REPORTER','PRESCRIBER_CODE_DE_ML','PRESCRIBER_CODE','15')
,('LS_DB_REPORTER','REPORTER_INFORMED_TO_MFR_DE_ML','REPORTER_INFORMED_TO_MFR','10003')
,('LS_DB_REPORTER','REPORTERCOUNTRY_DE_ML','REPORTERCOUNTRY','1015')
,('LS_DB_REPORTER','SPECIALTY_DE_ML','SPECIALTY','345')
,('LS_DB_SAFETY_MASTER','AERINFO_CALLER_TYPE_DE_ML','AERINFO_CALLER_TYPE','702')
,('LS_DB_SAFETY_MASTER','MSG_AUTOCOMPLETE_FAILED_DE_ML','MSG_AUTOCOMPLETE_FAILED','7077')
,('LS_DB_SAFETY_MASTER','MSG_DMI_PRODUCT_CHARACTERIZATION_DE_ML','MSG_DMI_PRODUCT_CHARACTERIZATION','9970')
,('LS_DB_SAFETY_MASTER','MSG_FU_NOTIFICATION_STATE_DE_ML','MSG_FU_NOTIFICATION_STATE','10028')
,('LS_DB_SAFETY_MASTER','MSG_NON_CASE_REASON_DE_ML','MSG_NON_CASE_REASON','9924')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CASE_VERSION_SIGNIFICANCE_DE_ML','SAFTYRPT_CASE_VERSION_SIGNIFICANCE','9605')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CORRECTIVE_ACTION_CATEGORY_DE_ML','SAFTYRPT_CORRECTIVE_ACTION_CATEGORY','10056')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DEVICE_PROBLEM_LABELING_DE_ML','SAFTYRPT_DEVICE_PROBLEM_LABELING','10092')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_NON_AE_DE_ML','SAFTYRPT_NON_AE','1002')
,('LS_DB_PATIENT','PAR_PAR_ETHNIC_ORIGIN_DE_ML','PAR_PAR_ETHNIC_ORIGIN','1022')
,('LS_DB_PATIENT','PAT_ANY_RELEVANT_INFO_FROM_PARTNER_DE_ML','PAT_ANY_RELEVANT_INFO_FROM_PARTNER','4')
,('LS_DB_PATIENT','PAT_GESTATATIONPERIODUNIT_DE_ML','PAT_GESTATATIONPERIODUNIT','11')
,('LS_DB_PATIENT','PAT_INFORM_PATIENT_DE_ML','PAT_INFORM_PATIENT','4')
,('LS_DB_PATIENT','PAT_PATWEIGHT_UNIT_CODE_DE_ML','PAT_PATWEIGHT_UNIT_CODE','1026')
,('LS_DB_PATIENT','PAT_STUDY_PROGRAM_DE_ML','PAT_STUDY_PROGRAM','4')
,('LS_DB_PREGNANCY','NEON_CHILD_SEX_DE_ML','NEON_CHILD_SEX','10080')
,('LS_DB_PREGNANCY','NEON_CONGENITAL_ANOMALY_TYPE_DE_ML','NEON_CONGENITAL_ANOMALY_TYPE','9988')
,('LS_DB_PREGNANCY','PREG_MOTHER_EXP_ANY_MEDICAL_PROBLEM_DE_ML','PREG_MOTHER_EXP_ANY_MEDICAL_PROBLEM','15')
,('LS_DB_PREGNANCY','PREG_TRIMESTER_OF_EXPOSURE_DE_ML','PREG_TRIMESTER_OF_EXPOSURE','9969')
,('LS_DB_DEVICE','DVC_DEVICE_USED_FOR_DE_ML','DVC_DEVICE_USED_FOR','823')
,('LS_DB_DEVICE','DVC_DURATION_OF_IMPLANTATION_UNIT_DE_ML','DVC_DURATION_OF_IMPLANTATION_UNIT','10')
,('LS_DB_DEVICE','DVC_EVENT_CHANGE_STATUS_DE_ML','DVC_EVENT_CHANGE_STATUS','15')
,('LS_DB_DEVICE','DVC_FOLLOW_UP_CORRECTION_DE_ML','DVC_FOLLOW_UP_CORRECTION','1021')
,('LS_DB_DEVICE','DVC_FOLLOW_UP_DEVICE_EVALUATION_DE_ML','DVC_FOLLOW_UP_DEVICE_EVALUATION','1021')
,('LS_DB_DEVICE','DVC_OPERATOR_OF_DEVICE_DE_ML','DVC_OPERATOR_OF_DEVICE','9918')
,('LS_DB_DEVICE','DVC_PRODUCT_CHARECTERISATION_DE_ML','DVC_PRODUCT_CHARECTERISATION','1013')
,('LS_DB_DEVICE','DVC_REPORTABLE_EVENT_TYPE_DE_ML','DVC_REPORTABLE_EVENT_TYPE','2011')
,('LS_DB_DEVICE','DVC_USAGE_OF_DEVICE_DE_ML','DVC_USAGE_OF_DEVICE','823')
,('LS_DB_DRUG','COMPANY_PRODUCT_DE_ML','COMPANY_PRODUCT','1002')
,('LS_DB_DRUG','DRUGINTDOSAGEUNITDEF_DE_ML','DRUGINTDOSAGEUNITDEF','9029')
,('LS_DB_DRUG','DRUGRECURREADMINISTRATION_DE_ML','DRUGRECURREADMINISTRATION','1024')
,('LS_DB_DRUG','GENERIC_DE_ML','GENERIC','7077')
,('LS_DB_DRUG','PRODUCT_AVAILABLE_FLAG_DE_ML','PRODUCT_AVAILABLE_FLAG','1002')
,('LS_DB_DRUG','STUDY_PRODUCT_TYPE_DE_ML','STUDY_PRODUCT_TYPE','8008')
,('LS_DB_DRUG','VACCINE_TYPE_DE_ML','VACCINE_TYPE','10069')
,('LS_DB_DRUG_APPROVAL','DEMOGRAPHIC_WAS_DESIGNED_DE_ML','DEMOGRAPHIC_WAS_DESIGNED','10128')
,('LS_DB_DRUG_APPROVAL','DRUGAUTHORIZATIONTYPE_DE_ML','DRUGAUTHORIZATIONTYPE','709')
,('LS_DB_DRUG_REACT_RELATEDNESS','CAUSALITY_SOURCE_DE_ML','CAUSALITY_SOURCE','9055')
,('LS_DB_DRUG_REACT_RELATEDNESS','RECHALLENGE_DE_ML','RECHALLENGE','9054')
,('LS_DB_DRUG_THERAPY','DGTH_ANATOMICAL_APPROACH_SITE_DE_ML','DGTH_ANATOMICAL_APPROACH_SITE','9162')
,('LS_DB_JPN_REVIEW','JPPVREW_NECESSITY_OF_GSD_DE_ML','JPPVREW_NECESSITY_OF_GSD','10021')
,('LS_DB_JPN_REVIEW','JPPVREW_ORIGINAL_LOC_DE_ML','JPPVREW_ORIGINAL_LOC','10025')
,('LS_DB_JPN_REVIEW','JPREVDOC_REGISTERED_DE_ML','JPREVDOC_REGISTERED','7077')
,('LS_DB_SAFETY_MASTER','MSG_FOLLOW_UP_SIGNIFICANCE_DE_ML','MSG_FOLLOW_UP_SIGNIFICANCE','9605')
,('LS_DB_SAFETY_MASTER','MSG_PUBLISH_REPORT_DE_ML','MSG_PUBLISH_REPORT','1002')
,('LS_DB_SAFETY_MASTER','MSG_SOLICITED_REPORT_DE_ML','MSG_SOLICITED_REPORT','1002')
,('LS_DB_SAFETY_MASTER','RECPT_PRIORITY_DE_ML','RECPT_PRIORITY','145')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CASEAMENDMENT_DE_ML','SAFTYRPT_CASEAMENDMENT','4')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CASENULLIFICATION_DE_ML','SAFTYRPT_CASENULLIFICATION','9604')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CONGENITALANOMALI_DE_ML','SAFTYRPT_CONGENITALANOMALI','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_CONT_SURVEY_JPN_DE_ML','SAFTYRPT_CONT_SURVEY_JPN','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DRUG_CODED_FLAG_DE_ML','SAFTYRPT_DRUG_CODED_FLAG','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_E2B_R3_REPORTER_COUNTRY_DE_ML','SAFTYRPT_E2B_R3_REPORTER_COUNTRY','1')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_FULFILLEXPEDITECRITERIA_DE_ML','SAFTYRPT_FULFILLEXPEDITECRITERIA','9748')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_RELATION_TO_PATIENT_DE_ML','SAFTYRPT_RELATION_TO_PATIENT','8142')
,('LS_DB_SOURCE','PRIMARY_SOURCE_FLAG_DE_ML','PRIMARY_SOURCE_FLAG','7077')
,('LS_DB_SOURCE','REFERENCE_TYPE_DE_ML','REFERENCE_TYPE','9060')
,('LS_DB_STUDY','STDY_PRIOR_TYPE_OF_DRUG_DE_ML','STDY_PRIOR_TYPE_OF_DRUG','341')
,('LS_DB_STUDY','STDY_SAFETY_REPORTING_RESPONSIBILITY_DE_ML','STDY_SAFETY_REPORTING_RESPONSIBILITY','10125')
,('LS_DB_STUDY','STDY_STUDY_DESIGN_DE_ML','STDY_STUDY_DESIGN','132')
,('LS_DB_REACTION','EVENT_OCCURE_DURING_INCIDENT_DE_ML','EVENT_OCCURE_DURING_INCIDENT','10131')
,('LS_DB_REACTION','IS_IME_MANUAL_DE_ML','IS_IME_MANUAL','9941')
,('LS_DB_REACTION','SERIOUSNESS_DE_ML','SERIOUSNESS','1002')
,('LS_DB_REACTION','SERIOUSNESS_COMPANY_DE_ML','SERIOUSNESS_COMPANY','4')
,('LS_DB_REACTION','TREATMENT_PERFORMED_DE_ML','TREATMENT_PERFORMED','7089')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATECASEACT_RCA_CAPA_REQUIRED_DE_ML','LATECASEACT_RCA_CAPA_REQUIRED','1002')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATEINFOINBO_STATUS_DE_ML','LATEINFOINBO_STATUS','10144')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATESUBCOMP_REASON_FOR_SUBMISSION_DELAY_DE_ML','LATESUBCOMP_REASON_FOR_SUBMISSION_DELAY','9662')
,('LS_DB_JPN_REVIEW','JPREVCON_MOU_EXECUTION_DE_ML','JPREVCON_MOU_EXECUTION','10032')
,('LS_DB_JPN_REVIEW','JPREVDLST_REGISTERED_DE_ML','JPREVDLST_REGISTERED','7077')
,('LS_DB_JPN_REVIEW','JPREVDLST_TEST_RESULT_RECEIVED_DE_ML','JPREVDLST_TEST_RESULT_RECEIVED','7077')
,('LS_DB_CASUALTY_ASSESSMENT','KRCTRESULT_KR_DE_ML','KRCTRESULT_KR','9943')
,('LS_DB_DEVICE','DVC_CURRENT_LOCATION_OF_DEVICE_DE_ML','DVC_CURRENT_LOCATION_OF_DEVICE','824')
,('LS_DB_DEVICE','DVC_DEVICE_AGE_UNIT_DE_ML','DVC_DEVICE_AGE_UNIT','1016')
,('LS_DB_DEVICE','DVC_EVENT_OCCUR_LOCATION_DE_ML','DVC_EVENT_OCCUR_LOCATION','9864')
,('LS_DB_DEVICE','DVC_EVENT_OCCUR_STATUS_DE_ML','DVC_EVENT_OCCUR_STATUS','15')
,('LS_DB_DEVICE','DVC_LABELLED_FOR_SINGLE_USE_DE_ML','DVC_LABELLED_FOR_SINGLE_USE','9606')
,('LS_DB_DEVICE','DVC_MANUFACTURER_DE_ML','DVC_MANUFACTURER','2031')
,('LS_DB_DEVICE','DVC_NEEDLE_TYPE_DE_ML','DVC_NEEDLE_TYPE','10008')
,('LS_DB_DEVICE','DVC_REPORT_SENT_TO_MANUF_DE_ML','DVC_REPORT_SENT_TO_MANUF','1002')
,('LS_DB_DEVICE_DEVICE_PROB_EVAL','DVCPRBEVAL_EVALUATION_TYPE_DE_ML','DVCPRBEVAL_EVALUATION_TYPE','9851')
,('LS_DB_DRUG','CODING_CLASS_DE_ML','CODING_CLASS','9120')
,('LS_DB_DRUG','DRUGCUMULATIVEDOSAGEUNIT_DE_ML','DRUGCUMULATIVEDOSAGEUNIT','1018')
,('LS_DB_DRUG_APPROVAL','DEVICE_CLASS_DE_ML','DEVICE_CLASS','5040')
,('LS_DB_DRUG_APPROVAL','DEVICE_MARKETED_BEFORE_DE_ML','DEVICE_MARKETED_BEFORE','7077')
,('LS_DB_DRUG_APPROVAL','PRODUCT_TYPE_DE_ML','PRODUCT_TYPE','5015')
,('LS_DB_DRUG_REACT_RELATEDNESS','ACCESS_RELETIONSHIP_MANUAL_DE_ML','ACCESS_RELETIONSHIP_MANUAL','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','COMPANY_CAUSALITY_DE_ML','COMPANY_CAUSALITY','9062')
,('LS_DB_DRUG_REACT_RELATEDNESS','DECHALLENGE_DE_ML','DECHALLENGE','1027')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGADMINISTRATIONROUTE_DE_ML','DGTH_DRUGADMINISTRATIONROUTE','1020')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGCUMULATIVEDOSAGEUNIT_DE_ML','DGTH_DRUGCUMULATIVEDOSAGEUNIT','1018')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGINTDOSAGEUNITDEF_DE_ML','DGTH_DRUGINTDOSAGEUNITDEF','1014')
,('LS_DB_DRUG_THERAPY','DGTH_THERAPY_CONTINUED_DE_ML','DGTH_THERAPY_CONTINUED','1002')
,('LS_DB_REACTION','CODING_TYPE_DE_ML','CODING_TYPE','158')
,('LS_DB_REACTION','IN_HISTORY_DE_ML','IN_HISTORY','8137')
,('LS_DB_REACTION','INCIDENT_OCCURRED_DURING_DE_ML','INCIDENT_OCCURRED_DURING','10045')
,('LS_DB_REACTION','MEDICALLY_CONFIRM_DE_ML','MEDICALLY_CONFIRM','1002')
,('LS_DB_REACTION','NONSERIOUS_DE_ML','NONSERIOUS','1002')
,('LS_DB_REACTION','TERMHIGHLIGHTED_DE_ML','TERMHIGHLIGHTED','1011')
,('LS_DB_REACT_VACCINE','AE_OUTCOME_DE_ML','AE_OUTCOME','1002')
,('LS_DB_REPORTER','IS_COORDINATOR_DE_ML','IS_COORDINATOR','4')
,('LS_DB_REPORTER','OBSERVESTUDYTYPE_DE_ML','OBSERVESTUDYTYPE','122')
,('LS_DB_REPORTER','OCCUPATION_DE_ML','OCCUPATION','1028')
,('LS_DB_REPORTER','PRIMARY_REPORTER_DE_ML','PRIMARY_REPORTER','1021')
,('LS_DB_REPORTER','QUALIFICATION_DE_ML','QUALIFICATION','6')
,('LS_DB_REPORTER','REPORTER_TYPE_DE_ML','REPORTER_TYPE','1003')
,('LS_DB_RISK_FACTOR','EVALUATION_DE_ML','EVALUATION','9916')
,('LS_DB_SAFETY_MASTER','MSG_INTIALORFALLOWUPBASIC_DE_ML','MSG_INTIALORFALLOWUPBASIC','2029')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_EVENT_CODED_FLAG_DE_ML','SAFTYRPT_EVENT_CODED_FLAG','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_HOSPITALIZATION_DE_ML','SAFTYRPT_HOSPITALIZATION','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_OVERALL_DIST_STATUS_DE_ML','SAFTYRPT_OVERALL_DIST_STATUS','194')
,('LS_DB_PATIENT','PAR_PARENTWEIGHT_UNIT_DE_ML','PAR_PARENTWEIGHT_UNIT','48')
,('LS_DB_PATIENT','PAT_DOB_UNKNOWN_CODE_DE_ML','PAT_DOB_UNKNOWN_CODE','4')
,('LS_DB_PATIENT','PAT_OK_TO_CONTACT_DR_DE_ML','PAT_OK_TO_CONTACT_DR','8101')
,('LS_DB_PATIENT_PARENT_MED_HIST','CODING_TYPE_DE_ML','CODING_TYPE','158')
,('LS_DB_PATIENT_PARENT_MED_HIST','DISEASE_TYPE_DE_ML','DISEASE_TYPE','9917')
,('LS_DB_PREGNANCY','NEON_BIRTH_OUTCOME_DE_ML','NEON_BIRTH_OUTCOME','9994')
,('LS_DB_PREGNANCY','NEON_CONGENITAL_ANOMALY_DE_ML','NEON_CONGENITAL_ANOMALY','1002')
,('LS_DB_PREGNANCY','PREG_CONSENT_TO_CONTACT_DE_ML','PREG_CONSENT_TO_CONTACT','7073')
,('LS_DB_PREGNANCY','PREG_CONSENT_TO_PREG_REGISTER_DE_ML','PREG_CONSENT_TO_PREG_REGISTER','4')
,('LS_DB_PREGNANCY','PREG_MOTHER_BEFORE_PREGNANT_DE_ML','PREG_MOTHER_BEFORE_PREGNANT','15')
,('LS_DB_PREGNANCY','PREG_PLANNED_PREGNANCY_DE_ML','PREG_PLANNED_PREGNANCY','1002')
,('LS_DB_PREGNANCY','PREG_PRE_PREGNANCY_WEIGHT_UNIT_DE_ML','PREG_PRE_PREGNANCY_WEIGHT_UNIT','9991')
,('LS_DB_PREGNANCY','PREGOCOM_CHILDWEIGHT_UNIT_CODE_DE_ML','PREGOCOM_CHILDWEIGHT_UNIT_CODE','48')
,('LS_DB_PREGNANCY','PREGOCOM_DELIVERY_METHOD_DE_ML','PREGOCOM_DELIVERY_METHOD','820')
,('LS_DB_PREGNANCY','PREGOCOM_FETUS_CLINICAL_CONDITION_DE_ML','PREGOCOM_FETUS_CLINICAL_CONDITION','821')
,('LS_DB_PREGNANCY','PREGOCOM_PREGNANCY_CLINICAL_STATUS_DE_ML','PREGOCOM_PREGNANCY_CLINICAL_STATUS','8120')
,('LS_DB_DRUG_REACT_LISTEDNESS','IS_LISTED_DE_ML','IS_LISTED','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','ACTIONDRUG_DE_ML','ACTIONDRUG','21')
,('LS_DB_DRUG_REACT_RELATEDNESS','DRUGSTARTLATENCYUNIT_DE_ML','DRUGSTARTLATENCYUNIT','1017')
,('LS_DB_DRUG_REACT_RELATEDNESS','ISAESI_DE_ML','ISAESI','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','METHODS_DE_ML','METHODS','9641')
,('LS_DB_DRUG_REACT_RELATEDNESS','SUSAR_DE_ML','SUSAR','7077')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGADMINDURATIONUNIT_DE_ML','DGTH_DRUGADMINDURATIONUNIT','1017')
,('LS_DB_DRUG_THERAPY','DGTHBSTDOC_TITLE_DE_ML','DGTHBSTDOC_TITLE','5014')
,('LS_DB_JPN_REVIEW','JPPVREW_DATA_CORRECTION_DE_ML','JPPVREW_DATA_CORRECTION','7077')
,('LS_DB_JPN_REVIEW','JPPVREW_MERGE_CASE_DE_ML','JPPVREW_MERGE_CASE','7077')
,('LS_DB_LANG_REDUCTED','LANGUAGE_CODE_DE_ML','LANGUAGE_CODE','9065')
,('LS_DB_LITERATURE','LITAUTH_QUALIFICATION_DE_ML','LITAUTH_QUALIFICATION','1003')
,('LS_DB_CASUALTY_ASSESSMENT','WHOUMCRESULT_KR_DE_ML','WHOUMCRESULT_KR','9931')
,('LS_DB_DEVICE','DVC_ASSOCIATED_WITH_DISTRIBUTOR_DE_ML','DVC_ASSOCIATED_WITH_DISTRIBUTOR','2015')
,('LS_DB_DEVICE','DVC_BIOLOGICAL_DEVICE_DE_ML','DVC_BIOLOGICAL_DEVICE','10058')
,('LS_DB_DEVICE','DVC_DEVICE_REPROCESSED_AND_REUSED_DE_ML','DVC_DEVICE_REPROCESSED_AND_REUSED','1008')
,('LS_DB_DEVICE','DVC_PRIMARY_DEVICE_FLAG_DE_ML','DVC_PRIMARY_DEVICE_FLAG','7077')
,('LS_DB_DRUG','BIOSIMILAR_DE_ML','BIOSIMILAR','7077')
,('LS_DB_DRUG','DRUGLASTPERIODUNIT_DE_ML','DRUGLASTPERIODUNIT','1017')
,('LS_DB_DRUG','PRODUCT_FORMULATION_DE_ML','PRODUCT_FORMULATION','815')
,('LS_DB_REACTION','IS_DME_DE_ML','IS_DME','9941')
,('LS_DB_REACTION','NEAR_INCIDENT_RELATED_DE_ML','NEAR_INCIDENT_RELATED','1002')
,('LS_DB_REACTION','PRIMARYSRCREACTION_LANG_DE_ML','PRIMARYSRCREACTION_LANG','9065')
,('LS_DB_REACTION','UN_ANTICIPATED_STATE_HEALTH_DE_ML','UN_ANTICIPATED_STATE_HEALTH','1002')
,('LS_DB_SAFETY_LATENESS_ASSESSMENT','LATESUBCOMP_STATUS_DE_ML','LATESUBCOMP_STATUS','10144')
,('LS_DB_SAFETY_MASTER','AERINFO_CLASSIFICATION_TYPE_CODE_DE_ML','AERINFO_CLASSIFICATION_TYPE_CODE','9135')
,('LS_DB_SAFETY_MASTER','MSG_CASE_PRIORITY_DE_ML','MSG_CASE_PRIORITY','9742')
,('LS_DB_SAFETY_MASTER','MSG_OVERALL_LATENESS_REASON_DE_ML','MSG_OVERALL_LATENESS_REASON','9922')
,('LS_DB_SAFETY_MASTER','RECPT_MEDIUM_CODE_DE_ML','RECPT_MEDIUM_CODE','501')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_COMBINATION_PRODUCT_REPORT_DE_ML','SAFTYRPT_COMBINATION_PRODUCT_REPORT','4')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DISABLING_DE_ML','SAFTYRPT_DISABLING','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DRUG_INDICATION_DE_ML','SAFTYRPT_DRUG_INDICATION','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_REPORT_SOURCE_CN_DE_ML','SAFTYRPT_REPORT_SOURCE_CN','10074')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SAFETY_CLASSIFICATION_DE_ML','SAFTYRPT_SAFETY_CLASSIFICATION','10062')
,('LS_DB_PARENT_PAST_DRUG','PARPDGSUB_SUBSTANCE_STRENGTH_UNIT_DE_ML','PARPDGSUB_SUBSTANCE_STRENGTH_UNIT','9070')
,('LS_DB_PATIENT','PAT_AGE_AT_TIME_OF_VACCINE_UNIT_DE_ML','PAT_AGE_AT_TIME_OF_VACCINE_UNIT','1016')
,('LS_DB_PATIENT','PAT_CONCOMITANT_THERAPIES_DE_ML','PAT_CONCOMITANT_THERAPIES','1021')
,('LS_DB_PATIENT','PAT_ETHNIC_ORIGIN_CN_DE_ML','PAT_ETHNIC_ORIGIN_CN','10075')
,('LS_DB_PATIENT','PAT_HEALTH_DAMAGE_TYPE_DE_ML','PAT_HEALTH_DAMAGE_TYPE','10064')
,('LS_DB_PATIENT','PAT_IS_PREGNANT_CODE_DE_ML','PAT_IS_PREGNANT_CODE','2000')
,('LS_DB_PATIENT','PAT_MANUAL_AGE_ENTRY_DE_ML','PAT_MANUAL_AGE_ENTRY','7077')
,('LS_DB_PATIENT','PATDTH_PATAUTOPSYYESNO_DE_ML','PATDTH_PATAUTOPSYYESNO','1002')
,('LS_DB_PREGNANCY','NEONCHILD_NEON_WEIGHT_UNIT_DE_ML','NEONCHILD_NEON_WEIGHT_UNIT','48')
,('LS_DB_PREGNANCY','PREG_BIRTH_DEFECTS_HISTORY_DE_ML','PREG_BIRTH_DEFECTS_HISTORY','1002')
,('LS_DB_DRUG','EQUIVALENT_DRUG_DE_ML','EQUIVALENT_DRUG','1002')
,('LS_DB_DRUG','FORM_STRENGTH_UNIT_DE_ML','FORM_STRENGTH_UNIT','9070')
,('LS_DB_DRUG','PRIOR_USE_DE_ML','PRIOR_USE','1002')
,('LS_DB_DRUG','PRODUCT_COMPOUNDED_DE_ML','PRODUCT_COMPOUNDED','7077')
,('LS_DB_DRUG','PRODUCT_OVER_COUNTER_DE_ML','PRODUCT_OVER_COUNTER','9861')
,('LS_DB_DRUG_APPROVAL','DRUGAUTHORIZATIONCOUNTRY_DE_ML','DRUGAUTHORIZATIONCOUNTRY','1015')
,('LS_DB_DRUG_INGREDIENT','ACTSUB_SUBSTANCE_STRENGTH_UNIT_DE_ML','ACTSUB_SUBSTANCE_STRENGTH_UNIT','9070')
,('LS_DB_DRUG_REACT_LISTEDNESS','SUSAR_DE_ML','SUSAR','7077')
,('LS_DB_DRUG_REACT_RELATEDNESS','CAUSALITY_REPORTER_DE_ML','CAUSALITY_REPORTER','315')
,('LS_DB_DRUG_REACT_RELATEDNESS','COMPANY_DRUGRESULT_DE_ML','COMPANY_DRUGRESULT','9062')
,('LS_DB_DRUG_REACT_RELATEDNESS','DRUGENDLATENCYUNIT_DE_ML','DRUGENDLATENCYUNIT','1017')
,('LS_DB_DRUG_REACT_RELATEDNESS','END_LATENCY_MANUAL_DE_ML','END_LATENCY_MANUAL','9941')
,('LS_DB_DRUG_REACT_RELATEDNESS','REPORTER_CAUSALITY_DE_ML','REPORTER_CAUSALITY','9062')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGSTARTPERIODUNIT_DE_ML','DGTH_DRUGSTARTPERIODUNIT','17')
,('LS_DB_DRUG_THERAPY','DGTH_DRUGSTRUCTUREDOSAGEUNIT_DE_ML','DGTH_DRUGSTRUCTUREDOSAGEUNIT','1018')
,('LS_DB_DRUG_THERAPY','DGTH_FREQUENCY_DE_ML','DGTH_FREQUENCY','9031')
,('LS_DB_HEALTH_DAMAGE','SUSPECT_RISK_DE_ML','SUSPECT_RISK','10063')
,('LS_DB_IMRDF_EVALUATION','IMRDF_TYPE_DE_ML','IMRDF_TYPE','9995')
,('LS_DB_JPN_REVIEW','JPPVREW_PRESENCE_DE_ML','JPPVREW_PRESENCE','10024')
,('LS_DB_JPN_REVIEW','JPPVREW_REASON_FOR_EXL_GSD_DE_ML','JPPVREW_REASON_FOR_EXL_GSD','10022')
,('LS_DB_JPN_REVIEW','JPREVCON_INVOICE_TYPE_DE_ML','JPREVCON_INVOICE_TYPE','10033')
,('LS_DB_JPN_REVIEW','JPREVDOC_DOC_TYPE_DE_ML','JPREVDOC_DOC_TYPE','10023')
,('LS_DB_LITERATURE','LITAUTH_COUNTRY_DE_ML','LITAUTH_COUNTRY','1015')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_SERIOUS_DE_ML','SAFTYRPT_SERIOUS','4')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_TEST_CODED_FLAG_DE_ML','SAFTYRPT_TEST_CODED_FLAG','7077')
,('LS_DB_STUDY','STDY_PRIOR_PATIENT_STUDY_DE_ML','STDY_PRIOR_PATIENT_STUDY','15')
,('LS_DB_STUDY','STDY_STUDY_COMPLETION_STATUS_DE_ML','STDY_STUDY_COMPLETION_STATUS','53')
,('LS_DB_TEST','CODING_TYPE_DE_ML','CODING_TYPE','158')
,('LS_DB_TEST','INCLUDE_IN_ANG_DE_ML','INCLUDE_IN_ANG','9970')
,('LS_DB_CAPA','CAPLATE_STATUS_DE_ML','CAPLATE_STATUS','10102')
,('LS_DB_DEVICE','DVC_IS_ROUTE_CAUSED_CONFIRMED_DE_ML','DVC_IS_ROUTE_CAUSED_CONFIRMED','1002')
,('LS_DB_DEVICE','DVC_PRODUCT_AVAILABLE_FOR_EVA_DE_ML','DVC_PRODUCT_AVAILABLE_FOR_EVA','10048')
,('LS_DB_DEVICE','DVC_PRODUCT_FLAG_DE_ML','DVC_PRODUCT_FLAG','5015')
,('LS_DB_DEVICE','DVC_USER_FACILITY_OR_DISTRIBUTOR_DE_ML','DVC_USER_FACILITY_OR_DISTRIBUTOR','827')
,('LS_DB_DRUG','CLASSIFY_PRODUCT_DE_ML','CLASSIFY_PRODUCT','9970')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','ASSESS_RELATIONSHIP_DE_ML','ASSESS_RELATIONSHIP','1002')
,('LS_DB_INBOUND_DOCUMENT_DETAILS','IS_DOCUMENT_INCLUDED_DE_ML','IS_DOCUMENT_INCLUDED','7077')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','END_LATENCY_UNIT_DE_ML','END_LATENCY_UNIT','1017')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','REPORTER_CAUSALITY_DE_ML','REPORTER_CAUSALITY','9062')
,('LS_DB_FLAGS','OCR_APPLIED_DE_ML','OCR_APPLIED','7077')
,('LS_DB_INBOUND_DOCUMENT_DETAILS','ADDITIONAL_DOC_DE_ML','ADDITIONAL_DOC','1002')
,('LS_DB_J12_STUDIES','PATIENT_UNDER_TREATMENT_DE_ML','PATIENT_UNDER_TREATMENT','9941')
,('LS_DB_SUPPORT_DOC','CATEGORY_CODE_DE_ML','CATEGORY_CODE','316')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','TEMP_RELATIONSHIP_DE_ML','TEMP_RELATIONSHIP','1002')
,('LS_DB_FLAGS','NLP_CASE_COMPARE_FLAG_DE_ML','NLP_CASE_COMPARE_FLAG','7077')
,('LS_DB_FLAGS','TURN_OFF_TOUCHLESS_FLAG_DE_ML','TURN_OFF_TOUCHLESS_FLAG','7077')
,('LS_DB_FLAGS','FOLLOWUP_NOTIFY_FLAG_DE_ML','FOLLOWUP_NOTIFY_FLAG','7077')
,('LS_DB_FLAGS','TRANSLATE_REQ_FLAG_DE_ML','TRANSLATE_REQ_FLAG','7077')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','COMPANY_CAUSALITY_DE_ML','COMPANY_CAUSALITY','9062')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','DECHALLENGE_DE_ML','DECHALLENGE','9054')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','RECHALLENGE_DE_ML','RECHALLENGE','9054')
,('LS_DB_FLAGS','LINK_AE_FLAG_DE_ML','LINK_AE_FLAG','7077')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','AESI_DE_ML','AESI','7077')
,('LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','START_LATENCY_UNIT_DE_ML','START_LATENCY_UNIT','1017')
,('LS_DB_USER','SALUTATION_DE_ML','SALUTATION','5014')
,('LS_DB_MESSAGE_INFM_AUTH','INFMAUTH_REPORTING_STATUS_DE_ML','INFMAUTH_REPORTING_STATUS','139')
,('LS_DB_MESSAGE_INFM_AUTH','INFMAUTH_TYPE_OF_ACK_RECEIVED_DE_ML','INFMAUTH_TYPE_OF_ACK_RECEIVED','9647')
,('LS_DB_MESSAGE_INFM_AUTH','STMSG_SUBMISSION_STATUS_DE_ML','STMSG_SUBMISSION_STATUS','9643')
,('LS_DB_MESSAGE_INFM_AUTH','STMSG_RECEPIENT_COUNTRY_DE_ML','STMSG_RECEPIENT_COUNTRY','1015')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_PRIMARY_SRC_COUNTRY_DE_ML','SAFTYRPT_PRIMARY_SRC_COUNTRY','1015')
,('LS_DB_PRODUCT_LIB','PRD_PRODUCT_TYPE_DE_ML','PRD_PRODUCT_TYPE','5015')
,('LS_DB_ST_LABELLED_PROD_EVENTS','IS_LABELED_DE_ML','IS_LABELED','9614')
,('LS_DB_DRUG_REACT_RELATEDNESS','COMPANY_CAUSALITY_FT_DE_ML','COMPANY_CAUSALITY_FT','9642')
,('LS_DB_ACCOUNTS_INFO','ACC_HEALTH_AUTHORITY_DE_ML','ACC_HEALTH_AUTHORITY','42')
,('LS_DB_MESSAGE_INFM_AUTH','INFMAUTH_AUTHORITY_NAME_DE_ML','INFMAUTH_AUTHORITY_NAME','9638')
,('LS_DB_PATIENT','PAT_ETHNIC_ORIGIN_DE_ML','PAT_ETHNIC_ORIGIN','1022')
,('LS_DB_WF_TRACKER','AUTOCOMPLETE_DE_ML','AUTOCOMPLETE','7077')
,('LS_DB_USER','COUNTRY_DE_ML','COUNTRY','1')
,('LS_DB_PATIENT','PAT_MOTHER_EXP_ANY_MEDICAL_PROBLEM_DE_ML','PAT_MOTHER_EXP_ANY_MEDICAL_PROBLEM','15')
,('LS_DB_PATIENT','PAT_PATAGEGROUP_DE_ML','PAT_PATAGEGROUP','1006')
,('LS_DB_PATIENT','PAT_PATIENT_IDENTIFY_DE_ML','PAT_PATIENT_IDENTIFY','7077')
,('LS_DB_PATIENT','PAT_PATIENT_PREGNANT_DE_ML','PAT_PATIENT_PREGNANT','1008')
,('LS_DB_PATIENT','PAT_TITLE_DE_ML','PAT_TITLE','5014')
,('LS_DB_PATIENT','PATPTNR_BIOLOGICAL_FATHER_AGE_UNIT_DE_ML','PATPTNR_BIOLOGICAL_FATHER_AGE_UNIT','1016')
,('LS_DB_PATIENT_PAST_DRUG','PATPDGTH_PATIENT_AGE_AT_VACCINE_UNIT_DE_ML','PATPDGTH_PATIENT_AGE_AT_VACCINE_UNIT','1016')
,('LS_DB_PATIENT_PAST_DRUG','PATPDGSUB_SUBSTANCE_STRENGTH_UNIT_DE_ML','PATPDGSUB_SUBSTANCE_STRENGTH_UNIT','9070')
,('LS_DB_PREGNANCY','NEON_RESUSCITATED_DE_ML','NEON_RESUSCITATED','1002')
,('LS_DB_PREGNANCY','PREG_ANY_RELEVANT_INFO_FROM_PARTNER_DE_ML','PREG_ANY_RELEVANT_INFO_FROM_PARTNER','4')
,('LS_DB_PREGNANCY','PREG_GESTATATIONPERIODUNIT_DE_ML','PREG_GESTATATIONPERIODUNIT','11')
,('LS_DB_REACTION','BASIC_IDENTIFICATION_DE_ML','BASIC_IDENTIFICATION','10004')
,('LS_DB_REACTION','CAUSED_BY_LO_DEFECT_DE_ML','CAUSED_BY_LO_DEFECT','1002')
,('LS_DB_REACTION','DISABILITY_DE_ML','DISABILITY','1002')
,('LS_DB_REACTION','EXEMPTED_EVENTS_DE_ML','EXEMPTED_EVENTS','9941')
,('LS_DB_REACTION','HOSPITALIZATION_DE_ML','HOSPITALIZATION','1002')
,('LS_DB_REACTION','IMRDF_SIMILAR_CODES_DE_ML','IMRDF_SIMILAR_CODES','10002')
,('LS_DB_REACTION','IS_SERIOUS_EVENT_DE_ML','IS_SERIOUS_EVENT','9941')
,('LS_DB_REACTION','REACTDURATIONUNIT_DE_ML','REACTDURATIONUNIT','1017')
,('LS_DB_REACTION','REACTOUTCOME_DE_ML','REACTOUTCOME','1012')
,('LS_DB_REACTION','SEVERITY_DE_ML','SEVERITY','8106')
,('LS_DB_REACT_VACCINE','HOSPITALIZATION_REQUIRED_DE_ML','HOSPITALIZATION_REQUIRED','1002')
,('LS_DB_REACT_VACCINE','RESULT_PROLONG_HOSPITAL_DE_ML','RESULT_PROLONG_HOSPITAL','1002')
,('LS_DB_REPORTER','DONOTREPORTNAME_DE_ML','DONOTREPORTNAME','9941')
,('LS_DB_REPORTER','HCP_CLASSIFICATION_KR_DE_ML','HCP_CLASSIFICATION_KR','9928')
,('LS_DB_SAFETY_MASTER','MSG_COMPLETION_FLAG_DE_ML','MSG_COMPLETION_FLAG','7077')
,('LS_DB_SAFETY_MASTER','MSG_COMPLIANCE_TRACKER_STATUS_DE_ML','MSG_COMPLIANCE_TRACKER_STATUS','10104')
,('LS_DB_SAFETY_MASTER','MSG_OVERRIDE_AUTO_CALUCATION_RULE_DE_ML','MSG_OVERRIDE_AUTO_CALUCATION_RULE','1002')
,('LS_DB_SAFETY_MASTER','RECPT_INCOMPLETE_R3_EXPORT_DE_ML','RECPT_INCOMPLETE_R3_EXPORT','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_DEATH_DE_ML','SAFTYRPT_DEATH','1002')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_INBOUND_LATENESS_REASON_DE_ML','SAFTYRPT_INBOUND_LATENESS_REASON','10100')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_INITIAL_REPORT_MEDIUM_CODE_DE_ML','SAFTYRPT_INITIAL_REPORT_MEDIUM_CODE','335')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_ISAESI_DE_ML','SAFTYRPT_ISAESI','7077')
,('LS_DB_SAFETY_MASTER','SAFTYRPT_MEDICALLYCONFIRM_DE_ML','SAFTYRPT_MEDICALLYCONFIRM','1002')
,('LS_DB_STUDY','STDY_STUDY_DISCONT_REASON_CODE_DE_ML','STDY_STUDY_DISCONT_REASON_CODE','112')
,('LS_DB_STUDY','STDY_STUDY_TYPE_DE_ML','STDY_STUDY_TYPE','1004')
,('LS_DB_TEST','TESTUNIT_DE_ML','TESTUNIT','8138')
,('LS_DB_PRODUCT_LIB','PRD_MADE_BY_DE_ML','PRD_MADE_BY','816')
,('LS_DB_PRODUCT_LIB','PRD_PRODUCT_GROUP_DE_ML','PRD_PRODUCT_GROUP','9741')
,('LS_DB_MESSAGE_INFM_AUTH','INFMAUTH_TYPE_OF_REPORT_DE_ML','INFMAUTH_TYPE_OF_REPORT','9623');
