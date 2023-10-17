
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select nvl((max(row_wid)+1),1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'HISTORY_CONTROL','KEEP_HISTORY',null,'YES',null;

commit;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACCOUNTS_SERIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ACTIVITY_LOG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AE_CASE_TAG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AE_SENDER_RECEIVER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AER_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AER_CLINICAL_CLASSIFICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_AGX_ANG_HISTORY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ASYNC_WORKFLOW_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CAPA','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASE_QUES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASE_SERIES_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CASUALTY_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_CODELIST_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_COMPONENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_DEVICE_PROB_EVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_HEALTH_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_IDENTIFIER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_PROBLEM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DEVICE_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DISTRIBUTION_FORMAT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DLIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DOCUMENT_STORE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INDICATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_INGREDIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_LOT_NUMBERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_COMPONENT_PROCEDURES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_LISTEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_REACT_RELATEDNESS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_RECURRENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_DRUG_THERAPY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_CASE_EXPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_IMPORT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_E2B_SUBMISSIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EMAIL_INTAKE_RPT_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_EVENT_GROUP','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_FLAGS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_HEALTH_DAMAGE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_IMRDF_EVALUATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_INBOUND_DOCUMENT_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_J12_STUDIES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_JPN_PRODUCT_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_JPN_REVIEW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LANG_REDUCTED','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LEBELED_MAPPED_COUNTY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LINKED_AE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LITERATURE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LOCAL_LABELING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_LOT_BATCH_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_MESSAGE_INFM_AUTH','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_NARRATIVE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PARENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PARTNER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_MED_HIST_EPISODE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PARENT_MED_HIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PATIENT_PAST_DRUG','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PERSON','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRD_CHARACTERISTIC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PREGNANCY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_ACTIVE_SUB_FORM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_APPROVAL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_INDICATION_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PROD_INGREDIENT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODDEVICE_UDI','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_ADDITIONAL_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LABEL_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_PRODUCT_THERAPEUTIC_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_QUALITY_CHECK','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REACT_VACCINE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REPORTDUPLICATE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REPORTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_REQUESTERS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_RESEARCH_RPT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_RISK_FACTOR','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_LATENESS_ASSESSMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SAFETY_MASTER','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SENDER_DIAGNOSIS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SERIOUS_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_INCIDENT_DEVICE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SIMILAR_PRODUCTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SOURCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SOURCE_DOCS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ST_BATCH_SUB_QUEUE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_ST_LABELLED_PROD_EVENTS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_STUDY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_STUDY_LIB','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_DETAILS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_PRODUCT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CASE_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_CORRESPONDENCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_APPROVAL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_LABELLING','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_LOCAL_REACTION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_SUPPORT_DOC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUBMISSION_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUPPORT_DOC','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_SUPPORT_DOC','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_TASKS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_TEST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_VERBATIM_TERMS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_ASSIGNMENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_NOTES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WORKFLOW','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LSMV_APPROVAL_INDICATIONS','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_INSTANCE ','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_INSTANCE ','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_TRACKER ','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG),'LS_DB_WF_TRACKER ','CDC_EXTRACT_TS_UB',current_timestamp,null,null;




INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_MEDDRA_VERSION (MEDDRA_VERSION	
       , PROCESSING_DT	
       , EXPIRY_DATE
       , CREATED_BY  
)  select '25.1',current_date(),'9999-12-31','LSDB_ETL_TEAM';


insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_LANGUAGE 
(codelist_id,language_code,code,language_name,language_name_abbr,spr_id,PROCESSING_DT,EXPIRY_DATE)

select CD_ID,
CASE 
    WHEN LN='en' THEN '001'
    WHEN LN='de' THEN '002'
    WHEN LN='es' THEN '003'
    WHEN LN='fr' THEN '005'
    WHEN LN='it' THEN '007'
    WHEN LN='ja' THEN '008'
    WHEN LN='cn' THEN '009'
    WHEN LN='po' THEN '010'
  END LANGUAGE_CODE,
  CASE 
    WHEN CD='127' THEN '001'
    WHEN CD='110' THEN '002'
    WHEN CD='404' THEN '003'
    WHEN CD='142' THEN '005'
    WHEN CD='205' THEN '007'
    WHEN CD='208' THEN '008'
    WHEN CD='81' THEN '009'
    WHEN CD='351' THEN '010'
                ELSE CD END CODE,
	DE AS DECODE ,
    DECODE_ABBR,
    SPR_ID,
    PROCESSING_DT,
    TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
    
    from 
(
SELECT distinct 
  to_date(GREATEST(
NVL(LSMV_CODELIST_NAME.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_CODELIST_CODE.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(LSMV_CODELIST_DECODE.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))    
))
PROCESSING_DT , LSMV_CODELIST_NAME.SPR_ID, 
  LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,LSMV_CODELIST_DECODE.DECODE_ABBR
, row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
FROM
    (
                   SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,date_modified,SPR_ID,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('9065')
    ) LSMV_CODELIST_NAME JOIN
    (
                   SELECT RECORD_ID, CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,date_modified  ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM LSDB_RPL.LSMV_CODELIST_CODE
    ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
    AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
    JOIN 
    (
                   SELECT RECORD_ID,LANGUAGE_CODE, DECODE,FK_CL_CODE_REC_ID,DECODE_ABBR ,CDC_OPERATION_TIME,date_modified,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                   FROM LSDB_RPL.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
    ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
    AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 ;




INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.etl_column_parameter (
	default_value_date,
	etl_date_created,
	etl_date_modified,
	cp_id,
	default_value_int,
	src_app_name,
	job_name,
	table_name,
	column_name,
	parameter_name,
	default_value_char
) VALUES (
	NULL,
	'2021-01-13 00:00:00',
	'9999-12-31 00:00:00',
	5,
	NULL,
	'ARISg',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'DER_COMBINED_RELATEDNESS',
	'Related',
	'Company or reporter'
);



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.etl_column_parameter (
	default_value_date,
	etl_date_created,
	etl_date_modified,
	cp_id,
	default_value_int,
	src_app_name,
	job_name,
	table_name,
	column_name,
	parameter_name,
	default_value_char
) VALUES (
	NULL,
	'2021-01-13 04:54:08.807685',
	'9999-12-31 00:00:00',
	19,
	NULL,
	'ARISg',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'RELATEDNESS',
	'RELATED_REPORTER',
	'Related'
);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.etl_column_parameter (
	default_value_date,
	etl_date_created,
	etl_date_modified,
	cp_id,
	default_value_int,
	src_app_name,
	job_name,
	table_name,
	column_name,
	parameter_name,
	default_value_char
) VALUES (
	NULL,
	'2021-01-13 04:54:09.293587',
	'9999-12-31 00:00:00',
	20,
	NULL,
	'ARISg',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'LS_DB_DRUG_REACT_RELATEDNESS',
	'RELATEDNESS',
	'RELATED_COMPANY',
	'Related'
);

