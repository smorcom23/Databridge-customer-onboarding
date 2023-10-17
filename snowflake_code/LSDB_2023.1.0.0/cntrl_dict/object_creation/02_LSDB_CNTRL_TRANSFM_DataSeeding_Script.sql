USE SCHEMA LSDB_CNTRL_TRANSFM;

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
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_ATC_CODES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_ATC_CODES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_ATC_RELATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_ATC_RELATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_INGREDIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_INGREDIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_MFR','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_MFR','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_SUBSTANCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_DRL_SUBSTANCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HLGT_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HLGT_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HLT_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_HLT_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;
insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOW_LEVEL_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_LOW_LEVEL_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_MD_HIERARCHY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_MD_HIERARCHY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOC_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;

insert into LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from LS_DB_ETL_CONFIG),'LS_DB_SOC_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;