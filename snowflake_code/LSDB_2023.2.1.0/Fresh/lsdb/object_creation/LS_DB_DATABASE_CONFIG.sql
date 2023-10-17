
-- USE SCHEMA${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DATABASE_CONFIG()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DATABASE_CONFIG'),
                'LSDB','Case','LS_DB_DATABASE_CONFIG',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DATABASE_CONFIG' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DATABASE_CONFIG' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DATABASE_CONFIG_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DATABASE_CONFIG_DELETION_TMP  As select RECORD_ID,'lsmv_database_config' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_database_config WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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

select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_database_config WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DATABASE_CONFIG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_database_config WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DATABASE_CONFIG' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
, lsmv_database_config_SUBSET AS 
(
select * from 
    (SELECT  
    applied_db  applied_db,aris_eviction_idle_time  aris_eviction_idle_time,aris_eviction_run_intrvl  aris_eviction_run_intrvl,aris_init_pool_size  aris_init_pool_size,aris_max_idle_pool_size  aris_max_idle_pool_size,aris_max_pool_size  aris_max_pool_size,aris_min_idle_pool_size  aris_min_idle_pool_size,arisg_company_id  arisg_company_id,arisg_db_hostname  arisg_db_hostname,arisg_db_id  arisg_db_id,arisg_db_password  arisg_db_password,arisg_db_portno  arisg_db_portno,arisg_db_sid  arisg_db_sid,arisg_db_user_name  arisg_db_user_name,arisg_integrated  arisg_integrated,cache_prep_stmts  cache_prep_stmts,cdc_operation_time  cdc_operation_time,cdc_operation_type  cdc_operation_type,config_file  config_file,config_file_name  config_file_name,data_privacy_key  data_privacy_key,database_driver  database_driver,database_ip  database_ip,database_port  database_port,database_pwd  database_pwd,database_schema  database_schema,database_service  database_service,database_url  database_url,database_user  database_user,datasource_id  datasource_id,datasource_name  datasource_name,date_created  date_created,date_modified  date_modified,db_eviction_idle_time  db_eviction_idle_time,db_eviction_run_intrvl  db_eviction_run_intrvl,db_init_pool_size  db_init_pool_size,db_maintenance_mode  db_maintenance_mode,db_max_idle_pool_size  db_max_idle_pool_size,db_max_pool_size  db_max_pool_size,db_max_pool_size_scheduler  db_max_pool_size_scheduler,db_min_idle_pool_size  db_min_idle_pool_size,download_logs_flag  download_logs_flag,edqm_sync_enable  edqm_sync_enable,edqm_sync_status  edqm_sync_status,elastic_search_arisg_index  elastic_search_arisg_index,elastic_search_authentication  elastic_search_authentication,elastic_search_ct_index  elastic_search_ct_index,elastic_search_e2b_xml_index  elastic_search_e2b_xml_index,elastic_search_password  elastic_search_password,elastic_search_port  elastic_search_port,elastic_search_protocol  elastic_search_protocol,elastic_search_sub_index  elastic_search_sub_index,elastic_search_url  elastic_search_url,elastic_search_username  elastic_search_username,enable_data_prvacy  enable_data_prvacy,enc_type  enc_type,es_arisg_no_of_replicas  es_arisg_no_of_replicas,es_arisg_no_of_shards  es_arisg_no_of_shards,es_ct_no_of_replicas  es_ct_no_of_replicas,es_ct_no_of_shards  es_ct_no_of_shards,es_e2b_xml_no_of_replicas  es_e2b_xml_no_of_replicas,es_e2b_xml_no_of_shards  es_e2b_xml_no_of_shards,es_sub_no_of_replicas  es_sub_no_of_replicas,es_sub_no_of_shards  es_sub_no_of_shards,initialize_schema_flag  initialize_schema_flag,is_cro_db  is_cro_db,key_updated  key_updated,license_file  license_file,license_file_name  license_file_name,mhash_k  mhash_k,monitor_srvc_active  monitor_srvc_active,override_agx_mapping  override_agx_mapping,prep_stmt_cache_size  prep_stmt_cache_size,prep_stmt_sql_limit  prep_stmt_sql_limit,record_id  record_id,ro_database_driver  ro_database_driver,ro_database_ip  ro_database_ip,ro_database_port  ro_database_port,ro_database_pwd  ro_database_pwd,ro_database_schema  ro_database_schema,ro_database_service  ro_database_service,ro_database_url  ro_database_url,ro_database_user  ro_database_user,schd_eviction_idle_time  schd_eviction_idle_time,schd_eviction_run_intrvl  schd_eviction_run_intrvl,schd_init_pool_size  schd_init_pool_size,schd_max_idle_pool_size  schd_max_idle_pool_size,schd_min_idle_pool_size  schd_min_idle_pool_size,scheduler_active  scheduler_active,schedulers_status  schedulers_status,spr_id  spr_id,target_db  target_db,user_created  user_created,user_modified  user_modified,wf_process_retry_count  wf_process_retry_count,workflow_async  workflow_async,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_database_config
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DATABASE_CONFIG_DELETION_TMP  WHERE TABLE_NAME='lsmv_database_config')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_database_config_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_database_config_SUBSET.USER_CREATED CREATED_BY,lsmv_database_config_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_database_config_SUBSET.workflow_async  ,lsmv_database_config_SUBSET.wf_process_retry_count  ,lsmv_database_config_SUBSET.user_modified  ,lsmv_database_config_SUBSET.user_created  ,lsmv_database_config_SUBSET.target_db  ,lsmv_database_config_SUBSET.spr_id  ,lsmv_database_config_SUBSET.schedulers_status  ,lsmv_database_config_SUBSET.scheduler_active  ,lsmv_database_config_SUBSET.schd_min_idle_pool_size  ,lsmv_database_config_SUBSET.schd_max_idle_pool_size  ,lsmv_database_config_SUBSET.schd_init_pool_size  ,lsmv_database_config_SUBSET.schd_eviction_run_intrvl  ,lsmv_database_config_SUBSET.schd_eviction_idle_time  ,lsmv_database_config_SUBSET.ro_database_user  ,lsmv_database_config_SUBSET.ro_database_url  ,lsmv_database_config_SUBSET.ro_database_service  ,lsmv_database_config_SUBSET.ro_database_schema  ,lsmv_database_config_SUBSET.ro_database_pwd  ,lsmv_database_config_SUBSET.ro_database_port  ,lsmv_database_config_SUBSET.ro_database_ip  ,lsmv_database_config_SUBSET.ro_database_driver  ,lsmv_database_config_SUBSET.record_id  ,lsmv_database_config_SUBSET.prep_stmt_sql_limit  ,lsmv_database_config_SUBSET.prep_stmt_cache_size  ,lsmv_database_config_SUBSET.override_agx_mapping  ,lsmv_database_config_SUBSET.monitor_srvc_active  ,lsmv_database_config_SUBSET.mhash_k  ,lsmv_database_config_SUBSET.license_file_name  ,lsmv_database_config_SUBSET.license_file  ,lsmv_database_config_SUBSET.key_updated  ,lsmv_database_config_SUBSET.is_cro_db  ,lsmv_database_config_SUBSET.initialize_schema_flag  ,lsmv_database_config_SUBSET.es_sub_no_of_shards  ,lsmv_database_config_SUBSET.es_sub_no_of_replicas  ,lsmv_database_config_SUBSET.es_e2b_xml_no_of_shards  ,lsmv_database_config_SUBSET.es_e2b_xml_no_of_replicas  ,lsmv_database_config_SUBSET.es_ct_no_of_shards  ,lsmv_database_config_SUBSET.es_ct_no_of_replicas  ,lsmv_database_config_SUBSET.es_arisg_no_of_shards  ,lsmv_database_config_SUBSET.es_arisg_no_of_replicas  ,lsmv_database_config_SUBSET.enc_type  ,lsmv_database_config_SUBSET.enable_data_prvacy  ,lsmv_database_config_SUBSET.elastic_search_username  ,lsmv_database_config_SUBSET.elastic_search_url  ,lsmv_database_config_SUBSET.elastic_search_sub_index  ,lsmv_database_config_SUBSET.elastic_search_protocol  ,lsmv_database_config_SUBSET.elastic_search_port  ,lsmv_database_config_SUBSET.elastic_search_password  ,lsmv_database_config_SUBSET.elastic_search_e2b_xml_index  ,lsmv_database_config_SUBSET.elastic_search_ct_index  ,lsmv_database_config_SUBSET.elastic_search_authentication  ,lsmv_database_config_SUBSET.elastic_search_arisg_index  ,lsmv_database_config_SUBSET.edqm_sync_status  ,lsmv_database_config_SUBSET.edqm_sync_enable  ,lsmv_database_config_SUBSET.download_logs_flag  ,lsmv_database_config_SUBSET.db_min_idle_pool_size  ,lsmv_database_config_SUBSET.db_max_pool_size_scheduler  ,lsmv_database_config_SUBSET.db_max_pool_size  ,lsmv_database_config_SUBSET.db_max_idle_pool_size  ,lsmv_database_config_SUBSET.db_maintenance_mode  ,lsmv_database_config_SUBSET.db_init_pool_size  ,lsmv_database_config_SUBSET.db_eviction_run_intrvl  ,lsmv_database_config_SUBSET.db_eviction_idle_time  ,lsmv_database_config_SUBSET.date_modified  ,lsmv_database_config_SUBSET.date_created  ,lsmv_database_config_SUBSET.datasource_name  ,lsmv_database_config_SUBSET.datasource_id  ,lsmv_database_config_SUBSET.database_user  ,lsmv_database_config_SUBSET.database_url  ,lsmv_database_config_SUBSET.database_service  ,lsmv_database_config_SUBSET.database_schema  ,lsmv_database_config_SUBSET.database_pwd  ,lsmv_database_config_SUBSET.database_port  ,lsmv_database_config_SUBSET.database_ip  ,lsmv_database_config_SUBSET.database_driver  ,lsmv_database_config_SUBSET.data_privacy_key  ,lsmv_database_config_SUBSET.config_file_name  ,lsmv_database_config_SUBSET.config_file  ,lsmv_database_config_SUBSET.cdc_operation_type  ,lsmv_database_config_SUBSET.cdc_operation_time  ,lsmv_database_config_SUBSET.cache_prep_stmts  ,lsmv_database_config_SUBSET.arisg_integrated  ,lsmv_database_config_SUBSET.arisg_db_user_name  ,lsmv_database_config_SUBSET.arisg_db_sid  ,lsmv_database_config_SUBSET.arisg_db_portno  ,lsmv_database_config_SUBSET.arisg_db_password  ,lsmv_database_config_SUBSET.arisg_db_id  ,lsmv_database_config_SUBSET.arisg_db_hostname  ,lsmv_database_config_SUBSET.arisg_company_id  ,lsmv_database_config_SUBSET.aris_min_idle_pool_size  ,lsmv_database_config_SUBSET.aris_max_pool_size  ,lsmv_database_config_SUBSET.aris_max_idle_pool_size  ,lsmv_database_config_SUBSET.aris_init_pool_size  ,lsmv_database_config_SUBSET.aris_eviction_run_intrvl  ,lsmv_database_config_SUBSET.aris_eviction_idle_time  ,lsmv_database_config_SUBSET.applied_db ,CONCAT( NVL(lsmv_database_config_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_database_config_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP)
where target_table_name='LS_DB_DATABASE_CONFIG'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG   
SET LS_DB_DATABASE_CONFIG.workflow_async = LS_DB_DATABASE_CONFIG_TMP.workflow_async,LS_DB_DATABASE_CONFIG.wf_process_retry_count = LS_DB_DATABASE_CONFIG_TMP.wf_process_retry_count,LS_DB_DATABASE_CONFIG.user_modified = LS_DB_DATABASE_CONFIG_TMP.user_modified,LS_DB_DATABASE_CONFIG.user_created = LS_DB_DATABASE_CONFIG_TMP.user_created,LS_DB_DATABASE_CONFIG.target_db = LS_DB_DATABASE_CONFIG_TMP.target_db,LS_DB_DATABASE_CONFIG.spr_id = LS_DB_DATABASE_CONFIG_TMP.spr_id,LS_DB_DATABASE_CONFIG.schedulers_status = LS_DB_DATABASE_CONFIG_TMP.schedulers_status,LS_DB_DATABASE_CONFIG.scheduler_active = LS_DB_DATABASE_CONFIG_TMP.scheduler_active,LS_DB_DATABASE_CONFIG.schd_min_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.schd_min_idle_pool_size,LS_DB_DATABASE_CONFIG.schd_max_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.schd_max_idle_pool_size,LS_DB_DATABASE_CONFIG.schd_init_pool_size = LS_DB_DATABASE_CONFIG_TMP.schd_init_pool_size,LS_DB_DATABASE_CONFIG.schd_eviction_run_intrvl = LS_DB_DATABASE_CONFIG_TMP.schd_eviction_run_intrvl,LS_DB_DATABASE_CONFIG.schd_eviction_idle_time = LS_DB_DATABASE_CONFIG_TMP.schd_eviction_idle_time,LS_DB_DATABASE_CONFIG.ro_database_user = LS_DB_DATABASE_CONFIG_TMP.ro_database_user,LS_DB_DATABASE_CONFIG.ro_database_url = LS_DB_DATABASE_CONFIG_TMP.ro_database_url,LS_DB_DATABASE_CONFIG.ro_database_service = LS_DB_DATABASE_CONFIG_TMP.ro_database_service,LS_DB_DATABASE_CONFIG.ro_database_schema = LS_DB_DATABASE_CONFIG_TMP.ro_database_schema,LS_DB_DATABASE_CONFIG.ro_database_pwd = LS_DB_DATABASE_CONFIG_TMP.ro_database_pwd,LS_DB_DATABASE_CONFIG.ro_database_port = LS_DB_DATABASE_CONFIG_TMP.ro_database_port,LS_DB_DATABASE_CONFIG.ro_database_ip = LS_DB_DATABASE_CONFIG_TMP.ro_database_ip,LS_DB_DATABASE_CONFIG.ro_database_driver = LS_DB_DATABASE_CONFIG_TMP.ro_database_driver,LS_DB_DATABASE_CONFIG.record_id = LS_DB_DATABASE_CONFIG_TMP.record_id,LS_DB_DATABASE_CONFIG.prep_stmt_sql_limit = LS_DB_DATABASE_CONFIG_TMP.prep_stmt_sql_limit,LS_DB_DATABASE_CONFIG.prep_stmt_cache_size = LS_DB_DATABASE_CONFIG_TMP.prep_stmt_cache_size,LS_DB_DATABASE_CONFIG.override_agx_mapping = LS_DB_DATABASE_CONFIG_TMP.override_agx_mapping,LS_DB_DATABASE_CONFIG.monitor_srvc_active = LS_DB_DATABASE_CONFIG_TMP.monitor_srvc_active,LS_DB_DATABASE_CONFIG.mhash_k = LS_DB_DATABASE_CONFIG_TMP.mhash_k,LS_DB_DATABASE_CONFIG.license_file_name = LS_DB_DATABASE_CONFIG_TMP.license_file_name,LS_DB_DATABASE_CONFIG.license_file = LS_DB_DATABASE_CONFIG_TMP.license_file,LS_DB_DATABASE_CONFIG.key_updated = LS_DB_DATABASE_CONFIG_TMP.key_updated,LS_DB_DATABASE_CONFIG.is_cro_db = LS_DB_DATABASE_CONFIG_TMP.is_cro_db,LS_DB_DATABASE_CONFIG.initialize_schema_flag = LS_DB_DATABASE_CONFIG_TMP.initialize_schema_flag,LS_DB_DATABASE_CONFIG.es_sub_no_of_shards = LS_DB_DATABASE_CONFIG_TMP.es_sub_no_of_shards,LS_DB_DATABASE_CONFIG.es_sub_no_of_replicas = LS_DB_DATABASE_CONFIG_TMP.es_sub_no_of_replicas,LS_DB_DATABASE_CONFIG.es_e2b_xml_no_of_shards = LS_DB_DATABASE_CONFIG_TMP.es_e2b_xml_no_of_shards,LS_DB_DATABASE_CONFIG.es_e2b_xml_no_of_replicas = LS_DB_DATABASE_CONFIG_TMP.es_e2b_xml_no_of_replicas,LS_DB_DATABASE_CONFIG.es_ct_no_of_shards = LS_DB_DATABASE_CONFIG_TMP.es_ct_no_of_shards,LS_DB_DATABASE_CONFIG.es_ct_no_of_replicas = LS_DB_DATABASE_CONFIG_TMP.es_ct_no_of_replicas,LS_DB_DATABASE_CONFIG.es_arisg_no_of_shards = LS_DB_DATABASE_CONFIG_TMP.es_arisg_no_of_shards,LS_DB_DATABASE_CONFIG.es_arisg_no_of_replicas = LS_DB_DATABASE_CONFIG_TMP.es_arisg_no_of_replicas,LS_DB_DATABASE_CONFIG.enc_type = LS_DB_DATABASE_CONFIG_TMP.enc_type,LS_DB_DATABASE_CONFIG.enable_data_prvacy = LS_DB_DATABASE_CONFIG_TMP.enable_data_prvacy,LS_DB_DATABASE_CONFIG.elastic_search_username = LS_DB_DATABASE_CONFIG_TMP.elastic_search_username,LS_DB_DATABASE_CONFIG.elastic_search_url = LS_DB_DATABASE_CONFIG_TMP.elastic_search_url,LS_DB_DATABASE_CONFIG.elastic_search_sub_index = LS_DB_DATABASE_CONFIG_TMP.elastic_search_sub_index,LS_DB_DATABASE_CONFIG.elastic_search_protocol = LS_DB_DATABASE_CONFIG_TMP.elastic_search_protocol,LS_DB_DATABASE_CONFIG.elastic_search_port = LS_DB_DATABASE_CONFIG_TMP.elastic_search_port,LS_DB_DATABASE_CONFIG.elastic_search_password = LS_DB_DATABASE_CONFIG_TMP.elastic_search_password,LS_DB_DATABASE_CONFIG.elastic_search_e2b_xml_index = LS_DB_DATABASE_CONFIG_TMP.elastic_search_e2b_xml_index,LS_DB_DATABASE_CONFIG.elastic_search_ct_index = LS_DB_DATABASE_CONFIG_TMP.elastic_search_ct_index,LS_DB_DATABASE_CONFIG.elastic_search_authentication = LS_DB_DATABASE_CONFIG_TMP.elastic_search_authentication,LS_DB_DATABASE_CONFIG.elastic_search_arisg_index = LS_DB_DATABASE_CONFIG_TMP.elastic_search_arisg_index,LS_DB_DATABASE_CONFIG.edqm_sync_status = LS_DB_DATABASE_CONFIG_TMP.edqm_sync_status,LS_DB_DATABASE_CONFIG.edqm_sync_enable = LS_DB_DATABASE_CONFIG_TMP.edqm_sync_enable,LS_DB_DATABASE_CONFIG.download_logs_flag = LS_DB_DATABASE_CONFIG_TMP.download_logs_flag,LS_DB_DATABASE_CONFIG.db_min_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.db_min_idle_pool_size,LS_DB_DATABASE_CONFIG.db_max_pool_size_scheduler = LS_DB_DATABASE_CONFIG_TMP.db_max_pool_size_scheduler,LS_DB_DATABASE_CONFIG.db_max_pool_size = LS_DB_DATABASE_CONFIG_TMP.db_max_pool_size,LS_DB_DATABASE_CONFIG.db_max_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.db_max_idle_pool_size,LS_DB_DATABASE_CONFIG.db_maintenance_mode = LS_DB_DATABASE_CONFIG_TMP.db_maintenance_mode,LS_DB_DATABASE_CONFIG.db_init_pool_size = LS_DB_DATABASE_CONFIG_TMP.db_init_pool_size,LS_DB_DATABASE_CONFIG.db_eviction_run_intrvl = LS_DB_DATABASE_CONFIG_TMP.db_eviction_run_intrvl,LS_DB_DATABASE_CONFIG.db_eviction_idle_time = LS_DB_DATABASE_CONFIG_TMP.db_eviction_idle_time,LS_DB_DATABASE_CONFIG.date_modified = LS_DB_DATABASE_CONFIG_TMP.date_modified,LS_DB_DATABASE_CONFIG.date_created = LS_DB_DATABASE_CONFIG_TMP.date_created,LS_DB_DATABASE_CONFIG.datasource_name = LS_DB_DATABASE_CONFIG_TMP.datasource_name,LS_DB_DATABASE_CONFIG.datasource_id = LS_DB_DATABASE_CONFIG_TMP.datasource_id,LS_DB_DATABASE_CONFIG.database_user = LS_DB_DATABASE_CONFIG_TMP.database_user,LS_DB_DATABASE_CONFIG.database_url = LS_DB_DATABASE_CONFIG_TMP.database_url,LS_DB_DATABASE_CONFIG.database_service = LS_DB_DATABASE_CONFIG_TMP.database_service,LS_DB_DATABASE_CONFIG.database_schema = LS_DB_DATABASE_CONFIG_TMP.database_schema,LS_DB_DATABASE_CONFIG.database_pwd = LS_DB_DATABASE_CONFIG_TMP.database_pwd,LS_DB_DATABASE_CONFIG.database_port = LS_DB_DATABASE_CONFIG_TMP.database_port,LS_DB_DATABASE_CONFIG.database_ip = LS_DB_DATABASE_CONFIG_TMP.database_ip,LS_DB_DATABASE_CONFIG.database_driver = LS_DB_DATABASE_CONFIG_TMP.database_driver,LS_DB_DATABASE_CONFIG.data_privacy_key = LS_DB_DATABASE_CONFIG_TMP.data_privacy_key,LS_DB_DATABASE_CONFIG.config_file_name = LS_DB_DATABASE_CONFIG_TMP.config_file_name,LS_DB_DATABASE_CONFIG.config_file = LS_DB_DATABASE_CONFIG_TMP.config_file,LS_DB_DATABASE_CONFIG.cdc_operation_type = LS_DB_DATABASE_CONFIG_TMP.cdc_operation_type,LS_DB_DATABASE_CONFIG.cdc_operation_time = LS_DB_DATABASE_CONFIG_TMP.cdc_operation_time,LS_DB_DATABASE_CONFIG.cache_prep_stmts = LS_DB_DATABASE_CONFIG_TMP.cache_prep_stmts,LS_DB_DATABASE_CONFIG.arisg_integrated = LS_DB_DATABASE_CONFIG_TMP.arisg_integrated,LS_DB_DATABASE_CONFIG.arisg_db_user_name = LS_DB_DATABASE_CONFIG_TMP.arisg_db_user_name,LS_DB_DATABASE_CONFIG.arisg_db_sid = LS_DB_DATABASE_CONFIG_TMP.arisg_db_sid,LS_DB_DATABASE_CONFIG.arisg_db_portno = LS_DB_DATABASE_CONFIG_TMP.arisg_db_portno,LS_DB_DATABASE_CONFIG.arisg_db_password = LS_DB_DATABASE_CONFIG_TMP.arisg_db_password,LS_DB_DATABASE_CONFIG.arisg_db_id = LS_DB_DATABASE_CONFIG_TMP.arisg_db_id,LS_DB_DATABASE_CONFIG.arisg_db_hostname = LS_DB_DATABASE_CONFIG_TMP.arisg_db_hostname,LS_DB_DATABASE_CONFIG.arisg_company_id = LS_DB_DATABASE_CONFIG_TMP.arisg_company_id,LS_DB_DATABASE_CONFIG.aris_min_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.aris_min_idle_pool_size,LS_DB_DATABASE_CONFIG.aris_max_pool_size = LS_DB_DATABASE_CONFIG_TMP.aris_max_pool_size,LS_DB_DATABASE_CONFIG.aris_max_idle_pool_size = LS_DB_DATABASE_CONFIG_TMP.aris_max_idle_pool_size,LS_DB_DATABASE_CONFIG.aris_init_pool_size = LS_DB_DATABASE_CONFIG_TMP.aris_init_pool_size,LS_DB_DATABASE_CONFIG.aris_eviction_run_intrvl = LS_DB_DATABASE_CONFIG_TMP.aris_eviction_run_intrvl,LS_DB_DATABASE_CONFIG.aris_eviction_idle_time = LS_DB_DATABASE_CONFIG_TMP.aris_eviction_idle_time,LS_DB_DATABASE_CONFIG.applied_db = LS_DB_DATABASE_CONFIG_TMP.applied_db,
LS_DB_DATABASE_CONFIG.PROCESSING_DT = LS_DB_DATABASE_CONFIG_TMP.PROCESSING_DT ,
LS_DB_DATABASE_CONFIG.expiry_date    =LS_DB_DATABASE_CONFIG_TMP.expiry_date       ,
LS_DB_DATABASE_CONFIG.created_by     =LS_DB_DATABASE_CONFIG_TMP.created_by        ,
LS_DB_DATABASE_CONFIG.created_dt     =LS_DB_DATABASE_CONFIG_TMP.created_dt        ,
LS_DB_DATABASE_CONFIG.load_ts        =LS_DB_DATABASE_CONFIG_TMP.load_ts         
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP 
WHERE LS_DB_DATABASE_CONFIG.INTEGRATION_ID = LS_DB_DATABASE_CONFIG_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DATABASE_CONFIG_TMP.PROCESSING_DT = LS_DB_DATABASE_CONFIG.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,workflow_async,
wf_process_retry_count,
user_modified,
user_created,
target_db,
spr_id,
schedulers_status,
scheduler_active,
schd_min_idle_pool_size,
schd_max_idle_pool_size,
schd_init_pool_size,
schd_eviction_run_intrvl,
schd_eviction_idle_time,
ro_database_user,
ro_database_url,
ro_database_service,
ro_database_schema,
ro_database_pwd,
ro_database_port,
ro_database_ip,
ro_database_driver,
record_id,
prep_stmt_sql_limit,
prep_stmt_cache_size,
override_agx_mapping,
monitor_srvc_active,
mhash_k,
license_file_name,
license_file,
key_updated,
is_cro_db,
initialize_schema_flag,
es_sub_no_of_shards,
es_sub_no_of_replicas,
es_e2b_xml_no_of_shards,
es_e2b_xml_no_of_replicas,
es_ct_no_of_shards,
es_ct_no_of_replicas,
es_arisg_no_of_shards,
es_arisg_no_of_replicas,
enc_type,
enable_data_prvacy,
elastic_search_username,
elastic_search_url,
elastic_search_sub_index,
elastic_search_protocol,
elastic_search_port,
elastic_search_password,
elastic_search_e2b_xml_index,
elastic_search_ct_index,
elastic_search_authentication,
elastic_search_arisg_index,
edqm_sync_status,
edqm_sync_enable,
download_logs_flag,
db_min_idle_pool_size,
db_max_pool_size_scheduler,
db_max_pool_size,
db_max_idle_pool_size,
db_maintenance_mode,
db_init_pool_size,
db_eviction_run_intrvl,
db_eviction_idle_time,
date_modified,
date_created,
datasource_name,
datasource_id,
database_user,
database_url,
database_service,
database_schema,
database_pwd,
database_port,
database_ip,
database_driver,
data_privacy_key,
config_file_name,
config_file,
cdc_operation_type,
cdc_operation_time,
cache_prep_stmts,
arisg_integrated,
arisg_db_user_name,
arisg_db_sid,
arisg_db_portno,
arisg_db_password,
arisg_db_id,
arisg_db_hostname,
arisg_company_id,
aris_min_idle_pool_size,
aris_max_pool_size,
aris_max_idle_pool_size,
aris_init_pool_size,
aris_eviction_run_intrvl,
aris_eviction_idle_time,
applied_db)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,workflow_async,
wf_process_retry_count,
user_modified,
user_created,
target_db,
spr_id,
schedulers_status,
scheduler_active,
schd_min_idle_pool_size,
schd_max_idle_pool_size,
schd_init_pool_size,
schd_eviction_run_intrvl,
schd_eviction_idle_time,
ro_database_user,
ro_database_url,
ro_database_service,
ro_database_schema,
ro_database_pwd,
ro_database_port,
ro_database_ip,
ro_database_driver,
record_id,
prep_stmt_sql_limit,
prep_stmt_cache_size,
override_agx_mapping,
monitor_srvc_active,
mhash_k,
license_file_name,
license_file,
key_updated,
is_cro_db,
initialize_schema_flag,
es_sub_no_of_shards,
es_sub_no_of_replicas,
es_e2b_xml_no_of_shards,
es_e2b_xml_no_of_replicas,
es_ct_no_of_shards,
es_ct_no_of_replicas,
es_arisg_no_of_shards,
es_arisg_no_of_replicas,
enc_type,
enable_data_prvacy,
elastic_search_username,
elastic_search_url,
elastic_search_sub_index,
elastic_search_protocol,
elastic_search_port,
elastic_search_password,
elastic_search_e2b_xml_index,
elastic_search_ct_index,
elastic_search_authentication,
elastic_search_arisg_index,
edqm_sync_status,
edqm_sync_enable,
download_logs_flag,
db_min_idle_pool_size,
db_max_pool_size_scheduler,
db_max_pool_size,
db_max_idle_pool_size,
db_maintenance_mode,
db_init_pool_size,
db_eviction_run_intrvl,
db_eviction_idle_time,
date_modified,
date_created,
datasource_name,
datasource_id,
database_user,
database_url,
database_service,
database_schema,
database_pwd,
database_port,
database_ip,
database_driver,
data_privacy_key,
config_file_name,
config_file,
cdc_operation_type,
cdc_operation_time,
cache_prep_stmts,
arisg_integrated,
arisg_db_user_name,
arisg_db_sid,
arisg_db_portno,
arisg_db_password,
arisg_db_id,
arisg_db_hostname,
arisg_company_id,
aris_min_idle_pool_size,
aris_max_pool_size,
aris_max_idle_pool_size,
aris_init_pool_size,
aris_eviction_run_intrvl,
aris_eviction_idle_time,
applied_db
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
                                                                                                                                                NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                                                                                                                                                                                                                                                                                                THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP 
WHERE TO_DATE(LS_DB_DATABASE_CONFIG.PROCESSING_DT) < TO_DATE(LS_DB_DATABASE_CONFIG_TMP.PROCESSING_DT)
AND LS_DB_DATABASE_CONFIG.INTEGRATION_ID = LS_DB_DATABASE_CONFIG_TMP.INTEGRATION_ID
AND LS_DB_DATABASE_CONFIG.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DATABASE_CONFIG_DELETION_TMP  WHERE TABLE_NAME='lsmv_database_config')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DATABASE_CONFIG_DELETION_TMP  WHERE TABLE_NAME='lsmv_database_config')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATABASE_CONFIG_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DATABASE_CONFIG'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DATABASE_CONFIG Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DATABASE_CONFIG'),
                'LSDB','Case','LS_DB_DATABASE_CONFIG',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_DATABASE_CONFIG not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
