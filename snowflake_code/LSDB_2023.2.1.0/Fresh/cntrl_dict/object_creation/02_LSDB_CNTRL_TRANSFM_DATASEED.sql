insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select nvl((max(row_wid)+1),1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'HISTORY_CONTROL','KEEP_HISTORY',null,'YES',null;


insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_LIST','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_ATC_CODES','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_ATC_CODES','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_ATC_RELATION','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_ATC_RELATION','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_INGREDIENT','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_DRUG_INGREDIENT','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_MFR','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_MFR','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_SUBSTANCE','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_DRL_SUBSTANCE','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HLGT_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HLGT_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HLT_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_HLT_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOW_LEVEL_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_LOW_LEVEL_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_MD_HIERARCHY','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_MD_HIERARCHY','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PREF_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_PREF_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOC_TERM','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_SOC_TERM','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_JDD_JPN','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_JDD_JPN','CDC_EXTRACT_TS_UB',current_timestamp,null,null;

insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_TERMS_INFO','CDC_EXTRACT_TS_LB','1900-01-01 00:00:01.000',null,null;
insert into ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG 
select (select max(row_wid)+1 from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_ETL_CONFIG),'LS_DB_CC_SMQ_CMQ_TERMS_INFO','CDC_EXTRACT_TS_UB',current_timestamp,null,null;


INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_VERSION (DICTIONARY_VERSION
       , PROCESSING_DT	
       , EXPIRY_DATE
       , CREATED_BY  
)  select 'B3 MAR 2020',current_date(),'9999-12-31','LSDB_ETL_TEAM';

INSERT INTO ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.ETL_SOC_ORDER (SLNO,SOC_NAME,SOC_CODE,INTERNATIONAL_SOC_ORDER,SOC_ORDER)

  
 
 select SLNO,SOC_NAME,SOC_CODE,INTERNATIONAL_SOC_ORDER,SOC_ORDER from ( 
 select  1 AS SLNO,'Blood and lymphatic system disorders' AS SOC_NAME,10005329 AS SOC_CODE,3 AS INTERNATIONAL_SOC_ORDER,1 AS SOC_ORDER
union all select  2,'Cardiac disorders',10007541,11,2
union all select  3,'Congenital, familial and genetic disorders',10010331,21,3
union all select  4,'Ear and labyrinth disorders',10013993,10,4
union all select  5,'Endocrine disorders',10014698,5,5
union all select  6,'Eye disorders',10015919,9,6
union all select  7,'Gastrointestinal disorders',10017947,14,7
union all select  8,'General disorders and administration site conditions',10018065,22,8
union all select  9,'Hepatobiliary disorders',10019805,15,9
union all select  10,'Immune system disorders',10021428,4,10
union all select  11,'Infections and infestations',10021881,1,11
union all select  12,'Injury, poisoning and procedural complications',10022117,24,12
union all select  13,'Investigations',10022891,23,13
union all select  14,'Metabolism and nutrition disorders',10027433,6,14
union all select  15,'Musculoskeletal and connective tissue disorders',10028395,17,15
union all select  16,'Neoplasms benign, malignant and unspecified (incl cysts and polyps)',10029104,2,16
union all select  17,'Nervous system disorders',10029205,8,17
union all select  18,'Pregnancy, puerperium and perinatal conditions',10036585,19,18
union all select  19,'Psychiatric disorders',10037175,7,19
union all select  20,'Renal and urinary disorders',10038359,18,20
union all select  21,'Reproductive system and breast disorders',10038604,20,21
union all select  22,'Respiratory, thoracic and mediastinal disorders',10038738,13,22
union all select  23,'Skin and subcutaneous tissue disorders',10040785,16,23
union all select  24,'Social circumstances',10041244,26,24
union all select  25,'Surgical and medical procedures',10042613,25,25
union all select  26,'Vascular disorders',10047065,12,26
union all select  27,'Product issues',10077536,27,27
)  where coalesce(soc_code,-1) not in (select coalesce(soc_code,-1) from ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.ETL_SOC_ORDER);


