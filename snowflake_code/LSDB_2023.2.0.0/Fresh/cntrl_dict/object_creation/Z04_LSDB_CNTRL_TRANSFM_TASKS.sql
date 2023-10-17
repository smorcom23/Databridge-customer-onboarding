CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_MEDDRA_ICD
  WAREHOUSE = DATABRIDGE_QA_WH
  Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_MEDDRA_ICD();


CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL_ATC_CODES
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_ATC_CODES();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL_DRUG_ATC_RELATION
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_DRUG_ATC_RELATION();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL_MFR
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_MFR();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_MD_HIERARCHY
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_MD_HIERARCHY();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_HLGT_PREF_TERM
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_HLGT_PREF_TERM();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_PREF_TERM
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_PREF_TERM();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_HLT_PREF_TERM
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_HLT_PREF_TERM();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_SOC_TERM
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_SOC_TERM();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_CC_SMQ_CMQ_LIST
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_SMQ_CMQ_LIST();


CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_LOW_LEVEL_TERM
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_LOW_LEVEL_TERM();


CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL_DRUG_INGREDIENT
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_DRUG_INGREDIENT();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_DRL_SUBSTANCE
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_DRL_SUBSTANCE();


CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_CC_SMQ_CMQ_TERMS_INFO
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_SMQ_CMQ_TERMS_INFO();

CREATE OR REPLACE TASK ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.TSK_LS_DB_CC_JDD_JPN
WAREHOUSE =DATABRIDGE_QA_WH
Schedule = '200 minute'
as Call  ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.PRC_LS_DB_CC_JDD_JPN();







