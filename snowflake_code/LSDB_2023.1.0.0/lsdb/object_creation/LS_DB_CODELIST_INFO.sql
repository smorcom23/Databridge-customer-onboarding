
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_CODELIST_INFO()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CODELIST_INFO'),
	'LSRA','Case','LS_DB_CODELIST_INFO',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CODELIST_INFO' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CODELIST_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_CODELIST_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  As select RECORD_ID,'lsmv_codelist_code' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_codelist_decode' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_codelist_name' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_codelist_stdcode' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
                                                                                      ) LSMV_CODELIST_NAME JOIN
                                                                                      (
                                                                                                     SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME              ,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_CODE
                                                                                      ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                                                                      AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                                                                      JOIN 
                                                                                      (
                                                                                                     SELECT RECORD_ID,LANGUAGE_CODE, DECODE,            FK_CL_CODE_REC_ID              ,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_DECODE
                                                                                      ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                                                                      AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
					),
LSMV_CASE_NO_SUBSET_1 as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_cl_name_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_cl_code_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_cl_name_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CODELIST_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
),LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_cl_name_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_cl_code_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1)
UNION 

select DISTINCT fk_cl_name_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET_1) 
)
 , lsmv_codelist_name_SUBSET AS 
(
select * from 
    (SELECT  
    add_allowed  cdnm_add_allowed,app_id  cdnm_app_id,arisg_codelist_id  cdnm_arisg_codelist_id,cl_sql  cdnm_cl_sql,codelist_id  cdnm_codelist_id,codelist_name  cdnm_codelist_name,date_created  cdnm_date_created,date_modified  cdnm_date_modified,e2b_r2_tag  cdnm_e2b_r2_tag,e2b_r3_tag  cdnm_e2b_r3_tag,fk_parent_codelist_id  cdnm_fk_parent_codelist_id,initial_synch_status  cdnm_initial_synch_status,is_before_abbr  cdnm_is_before_abbr,max_length  cdnm_max_length,record_id  cdnm_record_id,spr_id  cdnm_spr_id,sync_codelist  cdnm_sync_codelist,time_stamp  cdnm_time_stamp,update_allowed  cdnm_update_allowed,use_display_order  cdnm_use_display_order,user_created  cdnm_user_created,user_modified  cdnm_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_name
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_name')
  ) where REC_RANK=1 )
  , lsmv_codelist_decode_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  cdde_date_created,date_modified  cdde_date_modified,decode  cdde_decode,decode_abbr  cdde_decode_abbr,fk_cl_code_rec_id  cdde_fk_cl_code_rec_id,language_code  cdde_language_code,record_id  cdde_record_id,spr_id  cdde_spr_id,time_stamp  cdde_time_stamp,user_created  cdde_user_created,user_modified  cdde_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_decode
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_cl_code_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_decode')
  ) where REC_RANK=1 )
  , lsmv_codelist_stdcode_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  cdstd_date_created,date_modified  cdstd_date_modified,fk_cl_name_rec_id  cdstd_fk_cl_name_rec_id,fk_parent_std_code  cdstd_fk_parent_std_code,multi_usage  cdstd_multi_usage,record_id  cdstd_record_id,spr_id  cdstd_spr_id,std_code  cdstd_std_code,std_decode  cdstd_std_decode,time_stamp  cdstd_time_stamp,user_created  cdstd_user_created,user_modified  cdstd_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_stdcode
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_cl_name_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_stdcode')
  ) where REC_RANK=1 )
  , lsmv_codelist_code_SUBSET AS 
(
select * from 
    (SELECT  
    app_code1  cdce_app_code1,app_code2  cdce_app_code2,app_code3  cdce_app_code3,app_code4  cdce_app_code4,app_code5  cdce_app_code5,arisg_code_status  cdce_arisg_code_status,cl_std_code  cdce_cl_std_code,code  cdce_code,code_status  cdce_code_status,code_type  cdce_code_type,codelist_code_color  cdce_codelist_code_color,cvmp_code  cdce_cvmp_code,cvmp_code_default  cdce_cvmp_code_default,date_created  cdce_date_created,date_modified  cdce_date_modified,date_term_version  cdce_date_term_version,disabled_for_data_entry  cdce_disabled_for_data_entry,display_order  cdce_display_order,edqm_code  cdce_edqm_code,emdr_code  cdce_emdr_code,fda_code  cdce_fda_code,fk_cl_name_rec_id  cdce_fk_cl_name_rec_id,pmda_code  cdce_pmda_code,r2_code  cdce_r2_code,r3_code  cdce_r3_code,record_id  cdce_record_id,spr_id  cdce_spr_id,term_version  cdce_term_version,time_stamp  cdce_time_stamp,user_created  cdce_user_created,user_modified  cdce_user_modified,vich_code  cdce_vich_code,vich_code_default  cdce_vich_code_default,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_codelist_code
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_cl_name_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_code')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_codelist_stdcode_SUBSET.cdstd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_codelist_decode_SUBSET.cdde_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_codelist_code_SUBSET.cdce_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_codelist_name_SUBSET.cdnm_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_codelist_name_SUBSET.cdnm_USER_MODIFIED USER_MODIFIED,lsmv_codelist_name_SUBSET.cdnm_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_codelist_name_SUBSET.cdnm_USER_CREATED CREATED_BY,lsmv_codelist_name_SUBSET.cdnm_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_codelist_stdcode_SUBSET.cdstd_user_modified  ,lsmv_codelist_stdcode_SUBSET.cdstd_user_created  ,lsmv_codelist_stdcode_SUBSET.cdstd_time_stamp  ,lsmv_codelist_stdcode_SUBSET.cdstd_std_decode  ,lsmv_codelist_stdcode_SUBSET.cdstd_std_code  ,lsmv_codelist_stdcode_SUBSET.cdstd_spr_id  ,lsmv_codelist_stdcode_SUBSET.cdstd_record_id  ,lsmv_codelist_stdcode_SUBSET.cdstd_multi_usage  ,lsmv_codelist_stdcode_SUBSET.cdstd_fk_parent_std_code  ,lsmv_codelist_stdcode_SUBSET.cdstd_fk_cl_name_rec_id  ,lsmv_codelist_stdcode_SUBSET.cdstd_date_modified  ,lsmv_codelist_stdcode_SUBSET.cdstd_date_created  ,lsmv_codelist_name_SUBSET.cdnm_user_modified  ,lsmv_codelist_name_SUBSET.cdnm_user_created  ,lsmv_codelist_name_SUBSET.cdnm_use_display_order  ,lsmv_codelist_name_SUBSET.cdnm_update_allowed  ,lsmv_codelist_name_SUBSET.cdnm_time_stamp  ,lsmv_codelist_name_SUBSET.cdnm_sync_codelist  ,lsmv_codelist_name_SUBSET.cdnm_spr_id  ,lsmv_codelist_name_SUBSET.cdnm_record_id  ,lsmv_codelist_name_SUBSET.cdnm_max_length  ,lsmv_codelist_name_SUBSET.cdnm_is_before_abbr  ,lsmv_codelist_name_SUBSET.cdnm_initial_synch_status  ,lsmv_codelist_name_SUBSET.cdnm_fk_parent_codelist_id  ,lsmv_codelist_name_SUBSET.cdnm_e2b_r3_tag  ,lsmv_codelist_name_SUBSET.cdnm_e2b_r2_tag  ,lsmv_codelist_name_SUBSET.cdnm_date_modified  ,lsmv_codelist_name_SUBSET.cdnm_date_created  ,lsmv_codelist_name_SUBSET.cdnm_codelist_name  ,lsmv_codelist_name_SUBSET.cdnm_codelist_id  ,lsmv_codelist_name_SUBSET.cdnm_cl_sql  ,lsmv_codelist_name_SUBSET.cdnm_arisg_codelist_id  ,lsmv_codelist_name_SUBSET.cdnm_app_id  ,lsmv_codelist_name_SUBSET.cdnm_add_allowed  ,lsmv_codelist_decode_SUBSET.cdde_user_modified  ,lsmv_codelist_decode_SUBSET.cdde_user_created  ,lsmv_codelist_decode_SUBSET.cdde_time_stamp  ,lsmv_codelist_decode_SUBSET.cdde_spr_id  ,lsmv_codelist_decode_SUBSET.cdde_record_id  ,lsmv_codelist_decode_SUBSET.cdde_language_code  ,lsmv_codelist_decode_SUBSET.cdde_fk_cl_code_rec_id  ,lsmv_codelist_decode_SUBSET.cdde_decode_abbr  ,lsmv_codelist_decode_SUBSET.cdde_decode  ,lsmv_codelist_decode_SUBSET.cdde_date_modified  ,lsmv_codelist_decode_SUBSET.cdde_date_created  ,lsmv_codelist_code_SUBSET.cdce_vich_code_default  ,lsmv_codelist_code_SUBSET.cdce_vich_code  ,lsmv_codelist_code_SUBSET.cdce_user_modified  ,lsmv_codelist_code_SUBSET.cdce_user_created  ,lsmv_codelist_code_SUBSET.cdce_time_stamp  ,lsmv_codelist_code_SUBSET.cdce_term_version  ,lsmv_codelist_code_SUBSET.cdce_spr_id  ,lsmv_codelist_code_SUBSET.cdce_record_id  ,lsmv_codelist_code_SUBSET.cdce_r3_code  ,lsmv_codelist_code_SUBSET.cdce_r2_code  ,lsmv_codelist_code_SUBSET.cdce_pmda_code  ,lsmv_codelist_code_SUBSET.cdce_fk_cl_name_rec_id  ,lsmv_codelist_code_SUBSET.cdce_fda_code  ,lsmv_codelist_code_SUBSET.cdce_emdr_code  ,lsmv_codelist_code_SUBSET.cdce_edqm_code  ,lsmv_codelist_code_SUBSET.cdce_display_order  ,lsmv_codelist_code_SUBSET.cdce_disabled_for_data_entry  ,lsmv_codelist_code_SUBSET.cdce_date_term_version  ,lsmv_codelist_code_SUBSET.cdce_date_modified  ,lsmv_codelist_code_SUBSET.cdce_date_created  ,lsmv_codelist_code_SUBSET.cdce_cvmp_code_default  ,lsmv_codelist_code_SUBSET.cdce_cvmp_code  ,lsmv_codelist_code_SUBSET.cdce_codelist_code_color  ,lsmv_codelist_code_SUBSET.cdce_code_type  ,lsmv_codelist_code_SUBSET.cdce_code_status  ,lsmv_codelist_code_SUBSET.cdce_code  ,lsmv_codelist_code_SUBSET.cdce_cl_std_code  ,lsmv_codelist_code_SUBSET.cdce_arisg_code_status  ,lsmv_codelist_code_SUBSET.cdce_app_code5  ,lsmv_codelist_code_SUBSET.cdce_app_code4  ,lsmv_codelist_code_SUBSET.cdce_app_code3  ,lsmv_codelist_code_SUBSET.cdce_app_code2  ,lsmv_codelist_code_SUBSET.cdce_app_code1 ,CONCAT(NVL(lsmv_codelist_stdcode_SUBSET.cdstd_RECORD_ID,-1),'||',NVL(lsmv_codelist_decode_SUBSET.cdde_RECORD_ID,-1),'||',NVL(lsmv_codelist_code_SUBSET.cdce_RECORD_ID,-1),'||',NVL(lsmv_codelist_name_SUBSET.cdnm_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_codelist_name_SUBSET  LEFT JOIN lsmv_codelist_code_SUBSET ON lsmv_codelist_name_SUBSET.cdnm_record_id=lsmv_codelist_code_SUBSET.cdce_fk_cl_name_rec_id
                         LEFT JOIN lsmv_codelist_decode_SUBSET ON lsmv_codelist_code_SUBSET.cdce_record_id=lsmv_codelist_decode_SUBSET.cdde_fk_cl_code_rec_id
                         LEFT JOIN lsmv_codelist_stdcode_SUBSET ON lsmv_codelist_name_SUBSET.cdnm_record_id=lsmv_codelist_stdcode_SUBSET.cdstd_fk_cl_name_rec_id
                         AND  lsmv_codelist_code_SUBSET.cdce_cl_std_code=lsmv_codelist_stdcode_SUBSET.cdstd_std_code
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP)
where target_table_name='LS_DB_CODELIST_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CODELIST_INFO'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO   
SET LS_DB_CODELIST_INFO.cdstd_user_modified = LS_DB_CODELIST_INFO_TMP.cdstd_user_modified,LS_DB_CODELIST_INFO.cdstd_user_created = LS_DB_CODELIST_INFO_TMP.cdstd_user_created,LS_DB_CODELIST_INFO.cdstd_time_stamp = LS_DB_CODELIST_INFO_TMP.cdstd_time_stamp,LS_DB_CODELIST_INFO.cdstd_std_decode = LS_DB_CODELIST_INFO_TMP.cdstd_std_decode,LS_DB_CODELIST_INFO.cdstd_std_code = LS_DB_CODELIST_INFO_TMP.cdstd_std_code,LS_DB_CODELIST_INFO.cdstd_spr_id = LS_DB_CODELIST_INFO_TMP.cdstd_spr_id,LS_DB_CODELIST_INFO.cdstd_record_id = LS_DB_CODELIST_INFO_TMP.cdstd_record_id,LS_DB_CODELIST_INFO.cdstd_multi_usage = LS_DB_CODELIST_INFO_TMP.cdstd_multi_usage,LS_DB_CODELIST_INFO.cdstd_fk_parent_std_code = LS_DB_CODELIST_INFO_TMP.cdstd_fk_parent_std_code,LS_DB_CODELIST_INFO.cdstd_fk_cl_name_rec_id = LS_DB_CODELIST_INFO_TMP.cdstd_fk_cl_name_rec_id,LS_DB_CODELIST_INFO.cdstd_date_modified = LS_DB_CODELIST_INFO_TMP.cdstd_date_modified,LS_DB_CODELIST_INFO.cdstd_date_created = LS_DB_CODELIST_INFO_TMP.cdstd_date_created,LS_DB_CODELIST_INFO.cdnm_user_modified = LS_DB_CODELIST_INFO_TMP.cdnm_user_modified,LS_DB_CODELIST_INFO.cdnm_user_created = LS_DB_CODELIST_INFO_TMP.cdnm_user_created,LS_DB_CODELIST_INFO.cdnm_use_display_order = LS_DB_CODELIST_INFO_TMP.cdnm_use_display_order,LS_DB_CODELIST_INFO.cdnm_update_allowed = LS_DB_CODELIST_INFO_TMP.cdnm_update_allowed,LS_DB_CODELIST_INFO.cdnm_time_stamp = LS_DB_CODELIST_INFO_TMP.cdnm_time_stamp,LS_DB_CODELIST_INFO.cdnm_sync_codelist = LS_DB_CODELIST_INFO_TMP.cdnm_sync_codelist,LS_DB_CODELIST_INFO.cdnm_spr_id = LS_DB_CODELIST_INFO_TMP.cdnm_spr_id,LS_DB_CODELIST_INFO.cdnm_record_id = LS_DB_CODELIST_INFO_TMP.cdnm_record_id,LS_DB_CODELIST_INFO.cdnm_max_length = LS_DB_CODELIST_INFO_TMP.cdnm_max_length,LS_DB_CODELIST_INFO.cdnm_is_before_abbr = LS_DB_CODELIST_INFO_TMP.cdnm_is_before_abbr,LS_DB_CODELIST_INFO.cdnm_initial_synch_status = LS_DB_CODELIST_INFO_TMP.cdnm_initial_synch_status,LS_DB_CODELIST_INFO.cdnm_fk_parent_codelist_id = LS_DB_CODELIST_INFO_TMP.cdnm_fk_parent_codelist_id,LS_DB_CODELIST_INFO.cdnm_e2b_r3_tag = LS_DB_CODELIST_INFO_TMP.cdnm_e2b_r3_tag,LS_DB_CODELIST_INFO.cdnm_e2b_r2_tag = LS_DB_CODELIST_INFO_TMP.cdnm_e2b_r2_tag,LS_DB_CODELIST_INFO.cdnm_date_modified = LS_DB_CODELIST_INFO_TMP.cdnm_date_modified,LS_DB_CODELIST_INFO.cdnm_date_created = LS_DB_CODELIST_INFO_TMP.cdnm_date_created,LS_DB_CODELIST_INFO.cdnm_codelist_name = LS_DB_CODELIST_INFO_TMP.cdnm_codelist_name,LS_DB_CODELIST_INFO.cdnm_codelist_id = LS_DB_CODELIST_INFO_TMP.cdnm_codelist_id,LS_DB_CODELIST_INFO.cdnm_cl_sql = LS_DB_CODELIST_INFO_TMP.cdnm_cl_sql,LS_DB_CODELIST_INFO.cdnm_arisg_codelist_id = LS_DB_CODELIST_INFO_TMP.cdnm_arisg_codelist_id,LS_DB_CODELIST_INFO.cdnm_app_id = LS_DB_CODELIST_INFO_TMP.cdnm_app_id,LS_DB_CODELIST_INFO.cdnm_add_allowed = LS_DB_CODELIST_INFO_TMP.cdnm_add_allowed,LS_DB_CODELIST_INFO.cdde_user_modified = LS_DB_CODELIST_INFO_TMP.cdde_user_modified,LS_DB_CODELIST_INFO.cdde_user_created = LS_DB_CODELIST_INFO_TMP.cdde_user_created,LS_DB_CODELIST_INFO.cdde_time_stamp = LS_DB_CODELIST_INFO_TMP.cdde_time_stamp,LS_DB_CODELIST_INFO.cdde_spr_id = LS_DB_CODELIST_INFO_TMP.cdde_spr_id,LS_DB_CODELIST_INFO.cdde_record_id = LS_DB_CODELIST_INFO_TMP.cdde_record_id,LS_DB_CODELIST_INFO.cdde_language_code = LS_DB_CODELIST_INFO_TMP.cdde_language_code,LS_DB_CODELIST_INFO.cdde_fk_cl_code_rec_id = LS_DB_CODELIST_INFO_TMP.cdde_fk_cl_code_rec_id,LS_DB_CODELIST_INFO.cdde_decode_abbr = LS_DB_CODELIST_INFO_TMP.cdde_decode_abbr,LS_DB_CODELIST_INFO.cdde_decode = LS_DB_CODELIST_INFO_TMP.cdde_decode,LS_DB_CODELIST_INFO.cdde_date_modified = LS_DB_CODELIST_INFO_TMP.cdde_date_modified,LS_DB_CODELIST_INFO.cdde_date_created = LS_DB_CODELIST_INFO_TMP.cdde_date_created,LS_DB_CODELIST_INFO.cdce_vich_code_default = LS_DB_CODELIST_INFO_TMP.cdce_vich_code_default,LS_DB_CODELIST_INFO.cdce_vich_code = LS_DB_CODELIST_INFO_TMP.cdce_vich_code,LS_DB_CODELIST_INFO.cdce_user_modified = LS_DB_CODELIST_INFO_TMP.cdce_user_modified,LS_DB_CODELIST_INFO.cdce_user_created = LS_DB_CODELIST_INFO_TMP.cdce_user_created,LS_DB_CODELIST_INFO.cdce_time_stamp = LS_DB_CODELIST_INFO_TMP.cdce_time_stamp,LS_DB_CODELIST_INFO.cdce_term_version = LS_DB_CODELIST_INFO_TMP.cdce_term_version,LS_DB_CODELIST_INFO.cdce_spr_id = LS_DB_CODELIST_INFO_TMP.cdce_spr_id,LS_DB_CODELIST_INFO.cdce_record_id = LS_DB_CODELIST_INFO_TMP.cdce_record_id,LS_DB_CODELIST_INFO.cdce_r3_code = LS_DB_CODELIST_INFO_TMP.cdce_r3_code,LS_DB_CODELIST_INFO.cdce_r2_code = LS_DB_CODELIST_INFO_TMP.cdce_r2_code,LS_DB_CODELIST_INFO.cdce_pmda_code = LS_DB_CODELIST_INFO_TMP.cdce_pmda_code,LS_DB_CODELIST_INFO.cdce_fk_cl_name_rec_id = LS_DB_CODELIST_INFO_TMP.cdce_fk_cl_name_rec_id,LS_DB_CODELIST_INFO.cdce_fda_code = LS_DB_CODELIST_INFO_TMP.cdce_fda_code,LS_DB_CODELIST_INFO.cdce_emdr_code = LS_DB_CODELIST_INFO_TMP.cdce_emdr_code,LS_DB_CODELIST_INFO.cdce_edqm_code = LS_DB_CODELIST_INFO_TMP.cdce_edqm_code,LS_DB_CODELIST_INFO.cdce_display_order = LS_DB_CODELIST_INFO_TMP.cdce_display_order,LS_DB_CODELIST_INFO.cdce_disabled_for_data_entry = LS_DB_CODELIST_INFO_TMP.cdce_disabled_for_data_entry,LS_DB_CODELIST_INFO.cdce_date_term_version = LS_DB_CODELIST_INFO_TMP.cdce_date_term_version,LS_DB_CODELIST_INFO.cdce_date_modified = LS_DB_CODELIST_INFO_TMP.cdce_date_modified,LS_DB_CODELIST_INFO.cdce_date_created = LS_DB_CODELIST_INFO_TMP.cdce_date_created,LS_DB_CODELIST_INFO.cdce_cvmp_code_default = LS_DB_CODELIST_INFO_TMP.cdce_cvmp_code_default,LS_DB_CODELIST_INFO.cdce_cvmp_code = LS_DB_CODELIST_INFO_TMP.cdce_cvmp_code,LS_DB_CODELIST_INFO.cdce_codelist_code_color = LS_DB_CODELIST_INFO_TMP.cdce_codelist_code_color,LS_DB_CODELIST_INFO.cdce_code_type = LS_DB_CODELIST_INFO_TMP.cdce_code_type,LS_DB_CODELIST_INFO.cdce_code_status = LS_DB_CODELIST_INFO_TMP.cdce_code_status,LS_DB_CODELIST_INFO.cdce_code = LS_DB_CODELIST_INFO_TMP.cdce_code,LS_DB_CODELIST_INFO.cdce_cl_std_code = LS_DB_CODELIST_INFO_TMP.cdce_cl_std_code,LS_DB_CODELIST_INFO.cdce_arisg_code_status = LS_DB_CODELIST_INFO_TMP.cdce_arisg_code_status,LS_DB_CODELIST_INFO.cdce_app_code5 = LS_DB_CODELIST_INFO_TMP.cdce_app_code5,LS_DB_CODELIST_INFO.cdce_app_code4 = LS_DB_CODELIST_INFO_TMP.cdce_app_code4,LS_DB_CODELIST_INFO.cdce_app_code3 = LS_DB_CODELIST_INFO_TMP.cdce_app_code3,LS_DB_CODELIST_INFO.cdce_app_code2 = LS_DB_CODELIST_INFO_TMP.cdce_app_code2,LS_DB_CODELIST_INFO.cdce_app_code1 = LS_DB_CODELIST_INFO_TMP.cdce_app_code1,
LS_DB_CODELIST_INFO.PROCESSING_DT = LS_DB_CODELIST_INFO_TMP.PROCESSING_DT,
LS_DB_CODELIST_INFO.user_modified  =LS_DB_CODELIST_INFO_TMP.user_modified     ,
LS_DB_CODELIST_INFO.date_modified  =LS_DB_CODELIST_INFO_TMP.date_modified     ,
LS_DB_CODELIST_INFO.expiry_date    =LS_DB_CODELIST_INFO_TMP.expiry_date       ,
LS_DB_CODELIST_INFO.created_by     =LS_DB_CODELIST_INFO_TMP.created_by        ,
LS_DB_CODELIST_INFO.created_dt     =LS_DB_CODELIST_INFO_TMP.created_dt        ,
LS_DB_CODELIST_INFO.load_ts        =LS_DB_CODELIST_INFO_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP 
WHERE 	LS_DB_CODELIST_INFO.INTEGRATION_ID = LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CODELIST_INFO_TMP.PROCESSING_DT = LS_DB_CODELIST_INFO.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CODELIST_INFO.cdstd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdde_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdce_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdnm_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CODELIST_INFO_TMP.cdstd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdde_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdce_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdnm_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CODELIST_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CODELIST_INFO.cdnm_RECORD_ID ,LS_DB_CODELIST_INFO.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP 
ON LS_DB_CODELIST_INFO.cdnm_RECORD_ID=LS_DB_CODELIST_INFO_TMP.cdnm_RECORD_ID
AND LS_DB_CODELIST_INFO.INTEGRATION_ID = LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID 
where LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID  is null AND LS_DB_CODELIST_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CODELIST_INFO.cdnm_RECORD_ID in (select cdnm_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP )
) TMP where LS_DB_CODELIST_INFO.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CODELIST_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CODELIST_INFO.cdnm_RECORD_ID ,LS_DB_CODELIST_INFO.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP 
    ON LS_DB_CODELIST_INFO.cdnm_RECORD_ID=LS_DB_CODELIST_INFO_TMP.cdnm_RECORD_ID
    AND LS_DB_CODELIST_INFO.INTEGRATION_ID = LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID 
    where LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID  is null AND LS_DB_CODELIST_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CODELIST_INFO.cdnm_RECORD_ID in (select cdnm_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,cdstd_user_modified,
cdstd_user_created,
cdstd_time_stamp,
cdstd_std_decode,
cdstd_std_code,
cdstd_spr_id,
cdstd_record_id,
cdstd_multi_usage,
cdstd_fk_parent_std_code,
cdstd_fk_cl_name_rec_id,
cdstd_date_modified,
cdstd_date_created,
cdnm_user_modified,
cdnm_user_created,
cdnm_use_display_order,
cdnm_update_allowed,
cdnm_time_stamp,
cdnm_sync_codelist,
cdnm_spr_id,
cdnm_record_id,
cdnm_max_length,
cdnm_is_before_abbr,
cdnm_initial_synch_status,
cdnm_fk_parent_codelist_id,
cdnm_e2b_r3_tag,
cdnm_e2b_r2_tag,
cdnm_date_modified,
cdnm_date_created,
cdnm_codelist_name,
cdnm_codelist_id,
cdnm_cl_sql,
cdnm_arisg_codelist_id,
cdnm_app_id,
cdnm_add_allowed,
cdde_user_modified,
cdde_user_created,
cdde_time_stamp,
cdde_spr_id,
cdde_record_id,
cdde_language_code,
cdde_fk_cl_code_rec_id,
cdde_decode_abbr,
cdde_decode,
cdde_date_modified,
cdde_date_created,
cdce_vich_code_default,
cdce_vich_code,
cdce_user_modified,
cdce_user_created,
cdce_time_stamp,
cdce_term_version,
cdce_spr_id,
cdce_record_id,
cdce_r3_code,
cdce_r2_code,
cdce_pmda_code,
cdce_fk_cl_name_rec_id,
cdce_fda_code,
cdce_emdr_code,
cdce_edqm_code,
cdce_display_order,
cdce_disabled_for_data_entry,
cdce_date_term_version,
cdce_date_modified,
cdce_date_created,
cdce_cvmp_code_default,
cdce_cvmp_code,
cdce_codelist_code_color,
cdce_code_type,
cdce_code_status,
cdce_code,
cdce_cl_std_code,
cdce_arisg_code_status,
cdce_app_code5,
cdce_app_code4,
cdce_app_code3,
cdce_app_code2,
cdce_app_code1)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,cdstd_user_modified,
cdstd_user_created,
cdstd_time_stamp,
cdstd_std_decode,
cdstd_std_code,
cdstd_spr_id,
cdstd_record_id,
cdstd_multi_usage,
cdstd_fk_parent_std_code,
cdstd_fk_cl_name_rec_id,
cdstd_date_modified,
cdstd_date_created,
cdnm_user_modified,
cdnm_user_created,
cdnm_use_display_order,
cdnm_update_allowed,
cdnm_time_stamp,
cdnm_sync_codelist,
cdnm_spr_id,
cdnm_record_id,
cdnm_max_length,
cdnm_is_before_abbr,
cdnm_initial_synch_status,
cdnm_fk_parent_codelist_id,
cdnm_e2b_r3_tag,
cdnm_e2b_r2_tag,
cdnm_date_modified,
cdnm_date_created,
cdnm_codelist_name,
cdnm_codelist_id,
cdnm_cl_sql,
cdnm_arisg_codelist_id,
cdnm_app_id,
cdnm_add_allowed,
cdde_user_modified,
cdde_user_created,
cdde_time_stamp,
cdde_spr_id,
cdde_record_id,
cdde_language_code,
cdde_fk_cl_code_rec_id,
cdde_decode_abbr,
cdde_decode,
cdde_date_modified,
cdde_date_created,
cdce_vich_code_default,
cdce_vich_code,
cdce_user_modified,
cdce_user_created,
cdce_time_stamp,
cdce_term_version,
cdce_spr_id,
cdce_record_id,
cdce_r3_code,
cdce_r2_code,
cdce_pmda_code,
cdce_fk_cl_name_rec_id,
cdce_fda_code,
cdce_emdr_code,
cdce_edqm_code,
cdce_display_order,
cdce_disabled_for_data_entry,
cdce_date_term_version,
cdce_date_modified,
cdce_date_created,
cdce_cvmp_code_default,
cdce_cvmp_code,
cdce_codelist_code_color,
cdce_code_type,
cdce_code_status,
cdce_code,
cdce_cl_std_code,
cdce_arisg_code_status,
cdce_app_code5,
cdce_app_code4,
cdce_app_code3,
cdce_app_code2,
cdce_app_code1
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP 
WHERE 	TO_DATE(LS_DB_CODELIST_INFO.PROCESSING_DT) < TO_DATE(LS_DB_CODELIST_INFO_TMP.PROCESSING_DT)
AND LS_DB_CODELIST_INFO.INTEGRATION_ID = LS_DB_CODELIST_INFO_TMP.INTEGRATION_ID
AND LS_DB_CODELIST_INFO.cdnm_RECORD_ID = LS_DB_CODELIST_INFO_TMP.cdnm_RECORD_ID
AND LS_DB_CODELIST_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_CODELIST_INFO.cdstd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdde_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdce_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_CODELIST_INFO.cdnm_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_CODELIST_INFO_TMP.cdstd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdde_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdce_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_CODELIST_INFO_TMP.cdnm_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO TGT
WHERE  ( cdce_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_code') OR cdde_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_decode') OR cdnm_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_name') OR cdstd_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_stdcode')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( cdce_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_code') OR cdde_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_decode') OR cdnm_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_name') OR cdstd_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CODELIST_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_codelist_stdcode')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CODELIST_INFO_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CODELIST_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CODELIST_INFO'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CODELIST_INFO Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CODELIST_INFO'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CODELIST_INFO not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
