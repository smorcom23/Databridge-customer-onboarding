CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_MFR (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    name text(70),
    mfr_code text(7),
    dictionary_version text(100),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    country text(11),
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_SOC_TERM (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    soc_whoart_code text(7),
    soc_order number (32,0),
    soc_name_cn text(250),
    soc_name text(250),
    soc_kanji text(100),
    soc_jart_code text(6),
    soc_icd9cm_code text(8),
    soc_icd9_code text(8),
    soc_icd10_code text(8),
    soc_harts_code number (32,0),
    soc_costart_sym text(21),
    soc_code number (32,0),
    soc_abbrev text(5),
    record_id number (38,0),
    meddra_version text(60),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MD_HIERARCHY (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    soc_name_cn text(250),
    soc_name text(250),
    soc_code number (32,0),
    soc_abbrev text(5),
    record_id number (38,0),
    pt_soc_code number (32,0),
    pt_name_cn text(250),
    pt_name text(250),
    pt_code number (32,0),
    primary_soc_fg text(1),
    null_field text(1),
    meddra_version text(60),
    hlt_name_cn text(250),
    hlt_name text(250),
    hlt_code number (32,0),
    hlgt_name_cn text(250),
    hlgt_name text(250),
    hlgt_code number (32,0),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_HLGT_PREF_TERM (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    meddra_version text(60),
    hlgt_whoart_code text(7),
    hlgt_name_cn text(250),
    hlgt_name text(250),
    hlgt_kanji text(100),
    hlgt_jart_code text(6),
    hlgt_icd9cm_code text(8),
    hlgt_icd9_code text(8),
    hlgt_icd10_code text(8),
    hlgt_harts_code number (32,0),
    hlgt_costart_sym text(21),
    hlgt_code number (32,0),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_ATC_CODES (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    dictionary_version text(100),
    decode text(255),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    atc_level number (32,0),
    atc_code text(7),
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_ATC_RELATION (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    quarter_amended text(3),
    preferred_code text(8),
    dictionary_version text(100),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    atc_code text(7),
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_CC_SMQ_CMQ_LIST (user_modified text(400),
    user_id text(400),
    user_created text(400),
    term_from text(4),
    status text(4),
    spr_id text(800),
    smq_source text(16777216),
    smq_note text(16777216),
    smq_name_j text(1200),
    smq_name text(1200),
    smq_level text(4),
    smq_description_j text(16777216),
    smq_description text(16777216),
    smq_code number (38,0),
    smq_algorithm text(800),
    record_id number (38,0),
    private_query_flag text(4),
    portfolio_flag text(36),
    meddra_version text(36),
    list_source text(4),
    language_code text(36),
    dbid text(240),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    approval_state text(4),
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_HLT_PREF_TERM (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    meddra_version text(60),
    hlt_whoart_code text(7),
    hlt_name_cn text(250),
    hlt_name text(250),
    hlt_kanji text(100),
    hlt_jart_code text(6),
    hlt_icd9cm_code text(8),
    hlt_icd9_code text(8),
    hlt_icd10_code text(8),
    hlt_harts_code number (32,0),
    hlt_costart_sym text(21),
    hlt_code number (32,0),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL (user_modified text(100),
    user_created text(100),
    tradename_chinese text(2500),
    tradename text(2000),
    spr_id text(200),
    record_id number (38,0),
    quarter_introduced text(3),
    preferred_code text(8),
    name_source text(5),
    mfr_code text(7),
    inactive_flag text(1),
    drl_code text(11),
    dictionary_version text(100),
    designation text(1),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_PREF_TERM (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    pt_whoart_code text(7),
    pt_soc_code number (32,0),
    pt_name_cn text(250),
    pt_name text(250),
    pt_kanji text(100),
    pt_jart_code text(6),
    pt_icd9cm_code text(8),
    pt_icd9_code text(8),
    pt_icd10_code text(8),
    pt_harts_code number (32,0),
    pt_costart_sym text(21),
    pt_code number (32,0),
    null_field text(1),
    meddra_version text(60),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_LOW_LEVEL_TERM (user_modified text(100),
    user_created text(100),
    spr_id text(200),
    record_id number (38,0),
    pt_code number (32,0),
    meddra_version text(60),
    llt_whoart_code text(7),
    llt_name_cn text(250),
    llt_name text(250),
    llt_kanji text(140),
    llt_jcurr text(1),
    llt_jart_code text(6),
    llt_icd9cm_code text(8),
    llt_icd9_code text(8),
    llt_icd10_code text(8),
    llt_harts_code number (32,0),
    llt_currency text(1),
    llt_costart_sym text(21),
    llt_code number (32,0),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_DRUG_INGREDIENT (user_modified text(100),
    user_created text(100),
    substance_code text(10),
    spr_id text(200),
    record_id number (38,0),
    preferred_code text(8),
    dictionary_version text(100),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;



CREATE TABLE if not Exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_SUBSTANCE (user_modified text(100),
    user_created text(100),
    substance_name text(250),
    substance_code text(10),
    substance_chinese text(250),
    spr_id text(200),
    record_id number (38,0),
    inactive_flag text(1),
    dictionary_version text(100),
    date_modified timestamp_ntz,
    date_created timestamp_ntz,
    processing_dt DATE, 
EXPIRY_DATE DATE , 
created_by TEXT(400),
created_dt TIMESTAMP_NTZ, 
load_ts TIMESTAMP_NTZ, 
integration_id TEXT(400)) CHANGE_TRACKING = TRUE;


drop table if exists $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD;


CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_MEDDRA_ICD
(   BK_MEDDRA_ICD_WID BIGINT  
    ,PROCESSING_DT DATE
	,EXPIRY_DATE DATE 
	,CREATED_BY TEXT(400)
	,CREATED_DT TIMESTAMP_NTZ 
	,LOAD_TS TIMESTAMP_NTZ
	,INTEGRATION_ID TEXT(400)
    ,MDHI_RECORD_ID BIGINT
    ,LOWLE_RECORD_ID  BIGINT
    ,ICD_CODE VARCHAR(112)  
	,LLT_CODE VARCHAR(250) 
	,LLT_NAME VARCHAR(1000)  
	,PT_CODE VARCHAR(250)  
	,HLT_CODE INTEGER  
	,HLGT_CODE INTEGER  
	,SOC_CODE INTEGER  
	,PT_NAME VARCHAR(1000)  
	,HLT_NAME VARCHAR(1000)  
	,HLGT_NAME VARCHAR(1000)  
	,SOC_NAME VARCHAR(1000) 
	,MEDDRA_VERSION VARCHAR(36)  
	,LLT_NAME_OTHER VARCHAR(1000)  
	,IS_MEDDRA_FLAG INTEGER  
	,IS_ICD_FLAG INTEGER  
	,LANGUAGE_CODE VARCHAR(112)
	,INTERNATIONAL_SOC_ORDER INTEGER  
	,SOC_ORDER INTEGER  
	,PRIMARY_SOC_NAME VARCHAR(1020)  
	,INTERNATIONAL_PRIMARY_ORDER INTEGER  
    ,HLGT_NAME_OTHER VARCHAR(1000)  
	,HLT_NAME_OTHER VARCHAR(1000)  
	,PT_NAME_OTHER VARCHAR(1000)  
	,SOC_NAME_OTHER VARCHAR(1000)  	
	,PT_SOC_CODE INTEGER  
	,PRIMARY_SOC_FG VARCHAR(4)  
)
 CHANGE_TRACKING = TRUE;

drop table  if exists  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.TAB_DMIW;


CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.TAB_DMIW
(
	BK_MEDDRA_ICD_WID BIGINT  IDENTITY(1,1)
	,LLT_CODE VARCHAR(250)  
	,PT_CODE VARCHAR(250)
	,SOC_CODE INTEGER  
	,ICD_CODE VARCHAR(112)  
	,SPR_ID VARCHAR(800) 

) CHANGE_TRACKING = TRUE;

drop table  if exists  $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.ETL_SOC_ORDER;

CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.ETL_SOC_ORDER
(
	SLNO INTEGER  
	,SOC_CODE INTEGER  
	,INTERNATIONAL_SOC_ORDER INTEGER  
	,SOC_ORDER INTEGER  
	,SOC_NAME VARCHAR(1020)  
	,SPR_ID VARCHAR(800) 
)  CHANGE_TRACKING = TRUE;

CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DRL_VERSION
(
 DICTIONARY_VERSION VARCHAR(240)
  ,PROCESSING_DT DATE
  ,EXPIRY_DATE DATE 
  ,CREATED_BY TEXT(400)	
)
CHANGE_TRACKING = TRUE;



CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL (
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
CHANGED_REC_SET	VARIANT) CHANGE_TRACKING = TRUE;


CREATE TABLE IF NOT EXISTS $$TGT_CNTRL_DB_NAME.$$LSDB_CNTRL_TRANSFM.LS_DB_ETL_CONFIG(
ROW_WID	NUMBER(4,0),
TARGET_TABLE_NAME	VARCHAR(100),
PARAM_NAME	VARCHAR(50),
PARAM_VALUE	TIMESTAMP_NTZ(9),
CHAR_PARAM_VALUE	VARCHAR(50),
NUM_PARAM_VALUE	NUMBER(38,0)) CHANGE_TRACKING = TRUE;




