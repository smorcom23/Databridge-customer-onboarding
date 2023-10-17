
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_QUALITY_CHECK()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_QUALITY_CHECK'),
	'LSRA','Case','LS_DB_QUALITY_CHECK',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_QUALITY_CHECK' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_QUALITY_CHECK'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_QUALITY_CHECK'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  As select RECORD_ID,'lsmv_quality_check_config' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_config WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_quality_check_field_conf' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_field_conf WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_quality_check_que_lan' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_que_lan WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_field_conf WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_LSMV_QUALITY_CHECK record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_field_conf WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_config WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_config WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_que_lan WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_LSMV_FIELD record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_que_lan WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_QUALITY_CHECK' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_quality_check_que_lan_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  qulitychkquelan_date_created,date_modified  qulitychkquelan_date_modified,fk_lsmv_field  qulitychkquelan_fk_lsmv_field,language  qulitychkquelan_language,question  qulitychkquelan_question,record_id  qulitychkquelan_record_id,spr_id  qulitychkquelan_spr_id,user_created  qulitychkquelan_user_created,user_modified  qulitychkquelan_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_que_lan
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_LSMV_FIELD IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_que_lan')
  ) where REC_RANK=1 )
  , lsmv_quality_check_field_conf_SUBSET AS 
(
select * from 
    (SELECT  
    active  qulitychkfield_active,date_created  qulitychkfield_date_created,date_modified  qulitychkfield_date_modified,field_id  qulitychkfield_field_id,fk_lsmv_quality_check  qulitychkfield_fk_lsmv_quality_check,language  qulitychkfield_language,question  qulitychkfield_question,record_id  qulitychkfield_record_id,sequence_id  qulitychkfield_sequence_id,spr_id  qulitychkfield_spr_id,tab_id  qulitychkfield_tab_id,user_created  qulitychkfield_user_created,user_modified  qulitychkfield_user_modified,weightage  qulitychkfield_weightage,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_field_conf
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_LSMV_QUALITY_CHECK IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_field_conf')
  ) where REC_RANK=1 )
  , lsmv_quality_check_config_SUBSET AS 
(
select * from 
    (SELECT  
    active  qulitychkque_active,date_created  qulitychkque_date_created,date_modified  qulitychkque_date_modified,form_description  qulitychkque_form_description,name  qulitychkque_name,record_id  qulitychkque_record_id,spr_id  qulitychkque_spr_id,user_created  qulitychkque_user_created,user_modified  qulitychkque_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_quality_check_config
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_config')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_quality_check_config_SUBSET.qulitychkque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_quality_check_field_conf_SUBSET.qulitychkfield_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_quality_check_config_SUBSET.qulitychkque_USER_MODIFIED USER_MODIFIED,lsmv_quality_check_config_SUBSET.qulitychkque_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_quality_check_config_SUBSET.qulitychkque_USER_CREATED CREATED_BY,lsmv_quality_check_config_SUBSET.qulitychkque_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_user_modified  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_user_created  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_spr_id  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_record_id  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_question  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_language  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_fk_lsmv_field  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_date_modified  ,lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_date_created  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_weightage  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_user_modified  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_user_created  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_tab_id  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_spr_id  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_sequence_id  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_record_id  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_question  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_language  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_fk_lsmv_quality_check  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_field_id  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_date_modified  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_date_created  ,lsmv_quality_check_field_conf_SUBSET.qulitychkfield_active  ,lsmv_quality_check_config_SUBSET.qulitychkque_user_modified  ,lsmv_quality_check_config_SUBSET.qulitychkque_user_created  ,lsmv_quality_check_config_SUBSET.qulitychkque_spr_id  ,lsmv_quality_check_config_SUBSET.qulitychkque_record_id  ,lsmv_quality_check_config_SUBSET.qulitychkque_name  ,lsmv_quality_check_config_SUBSET.qulitychkque_form_description  ,lsmv_quality_check_config_SUBSET.qulitychkque_date_modified  ,lsmv_quality_check_config_SUBSET.qulitychkque_date_created  ,lsmv_quality_check_config_SUBSET.qulitychkque_active ,CONCAT(NVL(lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_RECORD_ID,-1),'||',NVL(lsmv_quality_check_config_SUBSET.qulitychkque_RECORD_ID,-1),'||',NVL(lsmv_quality_check_field_conf_SUBSET.qulitychkfield_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_quality_check_config_SUBSET  LEFT JOIN lsmv_quality_check_field_conf_SUBSET ON lsmv_quality_check_config_SUBSET.qulitychkque_RECORD_ID=lsmv_quality_check_field_conf_SUBSET.qulitychkfield_FK_LSMV_QUALITY_CHECK
                         LEFT JOIN lsmv_quality_check_que_lan_SUBSET ON lsmv_quality_check_field_conf_SUBSET.qulitychkfield_RECORD_ID=lsmv_quality_check_que_lan_SUBSET.qulitychkquelan_FK_LSMV_FIELD
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP)
where target_table_name='LS_DB_QUALITY_CHECK'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_QUALITY_CHECK'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK   
SET LS_DB_QUALITY_CHECK.qulitychkquelan_user_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_user_modified,LS_DB_QUALITY_CHECK.qulitychkquelan_user_created = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_user_created,LS_DB_QUALITY_CHECK.qulitychkquelan_spr_id = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_spr_id,LS_DB_QUALITY_CHECK.qulitychkquelan_record_id = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_record_id,LS_DB_QUALITY_CHECK.qulitychkquelan_question = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_question,LS_DB_QUALITY_CHECK.qulitychkquelan_language = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_language,LS_DB_QUALITY_CHECK.qulitychkquelan_fk_lsmv_field = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_fk_lsmv_field,LS_DB_QUALITY_CHECK.qulitychkquelan_date_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_date_modified,LS_DB_QUALITY_CHECK.qulitychkquelan_date_created = LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_date_created,LS_DB_QUALITY_CHECK.qulitychkfield_weightage = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_weightage,LS_DB_QUALITY_CHECK.qulitychkfield_user_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_user_modified,LS_DB_QUALITY_CHECK.qulitychkfield_user_created = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_user_created,LS_DB_QUALITY_CHECK.qulitychkfield_tab_id = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_tab_id,LS_DB_QUALITY_CHECK.qulitychkfield_spr_id = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_spr_id,LS_DB_QUALITY_CHECK.qulitychkfield_sequence_id = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_sequence_id,LS_DB_QUALITY_CHECK.qulitychkfield_record_id = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_record_id,LS_DB_QUALITY_CHECK.qulitychkfield_question = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_question,LS_DB_QUALITY_CHECK.qulitychkfield_language = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_language,LS_DB_QUALITY_CHECK.qulitychkfield_fk_lsmv_quality_check = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_fk_lsmv_quality_check,LS_DB_QUALITY_CHECK.qulitychkfield_field_id = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_field_id,LS_DB_QUALITY_CHECK.qulitychkfield_date_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_date_modified,LS_DB_QUALITY_CHECK.qulitychkfield_date_created = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_date_created,LS_DB_QUALITY_CHECK.qulitychkfield_active = LS_DB_QUALITY_CHECK_TMP.qulitychkfield_active,LS_DB_QUALITY_CHECK.qulitychkque_user_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkque_user_modified,LS_DB_QUALITY_CHECK.qulitychkque_user_created = LS_DB_QUALITY_CHECK_TMP.qulitychkque_user_created,LS_DB_QUALITY_CHECK.qulitychkque_spr_id = LS_DB_QUALITY_CHECK_TMP.qulitychkque_spr_id,LS_DB_QUALITY_CHECK.qulitychkque_record_id = LS_DB_QUALITY_CHECK_TMP.qulitychkque_record_id,LS_DB_QUALITY_CHECK.qulitychkque_name = LS_DB_QUALITY_CHECK_TMP.qulitychkque_name,LS_DB_QUALITY_CHECK.qulitychkque_form_description = LS_DB_QUALITY_CHECK_TMP.qulitychkque_form_description,LS_DB_QUALITY_CHECK.qulitychkque_date_modified = LS_DB_QUALITY_CHECK_TMP.qulitychkque_date_modified,LS_DB_QUALITY_CHECK.qulitychkque_date_created = LS_DB_QUALITY_CHECK_TMP.qulitychkque_date_created,LS_DB_QUALITY_CHECK.qulitychkque_active = LS_DB_QUALITY_CHECK_TMP.qulitychkque_active,
LS_DB_QUALITY_CHECK.PROCESSING_DT = LS_DB_QUALITY_CHECK_TMP.PROCESSING_DT,
LS_DB_QUALITY_CHECK.user_modified  =LS_DB_QUALITY_CHECK_TMP.user_modified     ,
LS_DB_QUALITY_CHECK.date_modified  =LS_DB_QUALITY_CHECK_TMP.date_modified     ,
LS_DB_QUALITY_CHECK.expiry_date    =LS_DB_QUALITY_CHECK_TMP.expiry_date       ,
LS_DB_QUALITY_CHECK.created_by     =LS_DB_QUALITY_CHECK_TMP.created_by        ,
LS_DB_QUALITY_CHECK.created_dt     =LS_DB_QUALITY_CHECK_TMP.created_dt        ,
LS_DB_QUALITY_CHECK.load_ts        =LS_DB_QUALITY_CHECK_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP 
WHERE 	LS_DB_QUALITY_CHECK.INTEGRATION_ID = LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_QUALITY_CHECK_TMP.PROCESSING_DT = LS_DB_QUALITY_CHECK.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_QUALITY_CHECK.qulitychkquelan_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_QUALITY_CHECK.qulitychkque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_QUALITY_CHECK.qulitychkfield_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkfield_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_QUALITY_CHECK.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID ,LS_DB_QUALITY_CHECK.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP 
ON LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID=LS_DB_QUALITY_CHECK_TMP.qulitychkque_RECORD_ID
AND LS_DB_QUALITY_CHECK.INTEGRATION_ID = LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID 
where LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID  is null AND LS_DB_QUALITY_CHECK.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID in (select qulitychkque_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP )
) TMP where LS_DB_QUALITY_CHECK.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_QUALITY_CHECK.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID ,LS_DB_QUALITY_CHECK.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP 
    ON LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID=LS_DB_QUALITY_CHECK_TMP.qulitychkque_RECORD_ID
    AND LS_DB_QUALITY_CHECK.INTEGRATION_ID = LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID 
    where LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID  is null AND LS_DB_QUALITY_CHECK.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID in (select qulitychkque_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,qulitychkquelan_user_modified,
qulitychkquelan_user_created,
qulitychkquelan_spr_id,
qulitychkquelan_record_id,
qulitychkquelan_question,
qulitychkquelan_language,
qulitychkquelan_fk_lsmv_field,
qulitychkquelan_date_modified,
qulitychkquelan_date_created,
qulitychkfield_weightage,
qulitychkfield_user_modified,
qulitychkfield_user_created,
qulitychkfield_tab_id,
qulitychkfield_spr_id,
qulitychkfield_sequence_id,
qulitychkfield_record_id,
qulitychkfield_question,
qulitychkfield_language,
qulitychkfield_fk_lsmv_quality_check,
qulitychkfield_field_id,
qulitychkfield_date_modified,
qulitychkfield_date_created,
qulitychkfield_active,
qulitychkque_user_modified,
qulitychkque_user_created,
qulitychkque_spr_id,
qulitychkque_record_id,
qulitychkque_name,
qulitychkque_form_description,
qulitychkque_date_modified,
qulitychkque_date_created,
qulitychkque_active)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,qulitychkquelan_user_modified,
qulitychkquelan_user_created,
qulitychkquelan_spr_id,
qulitychkquelan_record_id,
qulitychkquelan_question,
qulitychkquelan_language,
qulitychkquelan_fk_lsmv_field,
qulitychkquelan_date_modified,
qulitychkquelan_date_created,
qulitychkfield_weightage,
qulitychkfield_user_modified,
qulitychkfield_user_created,
qulitychkfield_tab_id,
qulitychkfield_spr_id,
qulitychkfield_sequence_id,
qulitychkfield_record_id,
qulitychkfield_question,
qulitychkfield_language,
qulitychkfield_fk_lsmv_quality_check,
qulitychkfield_field_id,
qulitychkfield_date_modified,
qulitychkfield_date_created,
qulitychkfield_active,
qulitychkque_user_modified,
qulitychkque_user_created,
qulitychkque_spr_id,
qulitychkque_record_id,
qulitychkque_name,
qulitychkque_form_description,
qulitychkque_date_modified,
qulitychkque_date_created,
qulitychkque_active
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP 
WHERE 	TO_DATE(LS_DB_QUALITY_CHECK.PROCESSING_DT) < TO_DATE(LS_DB_QUALITY_CHECK_TMP.PROCESSING_DT)
AND LS_DB_QUALITY_CHECK.INTEGRATION_ID = LS_DB_QUALITY_CHECK_TMP.INTEGRATION_ID
AND LS_DB_QUALITY_CHECK.qulitychkque_RECORD_ID = LS_DB_QUALITY_CHECK_TMP.qulitychkque_RECORD_ID
AND LS_DB_QUALITY_CHECK.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_QUALITY_CHECK.qulitychkquelan_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_QUALITY_CHECK.qulitychkque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_QUALITY_CHECK.qulitychkfield_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkquelan_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkque_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_QUALITY_CHECK_TMP.qulitychkfield_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK TGT
WHERE  ( qulitychkque_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_config') OR qulitychkfield_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_field_conf') OR qulitychkquelan_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_que_lan')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( qulitychkque_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_config') OR qulitychkfield_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_field_conf') OR qulitychkquelan_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_QUALITY_CHECK_DELETION_TMP  WHERE TABLE_NAME='lsmv_quality_check_que_lan')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_QUALITY_CHECK_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_QUALITY_CHECK'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_QUALITY_CHECK'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_QUALITY_CHECK Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_QUALITY_CHECK'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_QUALITY_CHECK not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
