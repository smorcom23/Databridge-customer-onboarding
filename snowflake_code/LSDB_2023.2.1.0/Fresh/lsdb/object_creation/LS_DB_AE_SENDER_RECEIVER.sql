
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_AE_SENDER_RECEIVER()
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

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_AE_SENDER_RECEIVER'),
	'LSDB','Case','LS_DB_AE_SENDER_RECEIVER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_AE_SENDER_RECEIVER' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_AE_SENDER_RECEIVER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  As select RECORD_ID,'lsmv_e2b_ae_case_receiver' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_receiver WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_e2b_ae_case_sender' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_sender WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('5014')
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')),
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_sender WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_AE_SENDER_RECEIVER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_sender WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_AE_SENDER_RECEIVER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_receiver WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_AE_SENDER_RECEIVER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_receiver WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_AE_SENDER_RECEIVER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 ,
	 LSMV_COMMON_COLUMN_SUBSET as
 (   select RECORD_ID,common_parent_key,ARI_REC_ID,CASE_NO,AER_VERSION_NO,RECEIPT_ID,RECEIPT_NO,VERSION_NO 
              from     (
                                                          select LSMV_CASE_NO_SUBSET.RECORD_ID,LSMV_CASE_NO_SUBSET.common_parent_key,AER_INFO.ARI_REC_ID, AER_INFO.AER_NO CASE_NO, AER_INFO.AER_VERSION_NO, RECPT_ITM.RECORD_ID RECEIPT_ID,
                                                                                      RECPT_ITM.RECEIPT_NO RECEIPT_NO,RECPT_ITM.VERSION VERSION_NO , 
                                                                                      row_number () OVER ( PARTITION BY LSMV_CASE_NO_SUBSET.RECORD_ID,RECPT_ITM.RECORD_ID ORDER BY to_date(GREATEST(
                                                                                                                                                                             NVL(RECPT_ITM.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(AER_INFO.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
                                                                                                                                                                                            )) DESC ) REC_RANK
                                                                                      from ${stage_db_name}.${stage_schema_name}.LSMV_AER_INFO AER_INFO,${stage_db_name}.${stage_schema_name}.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_e2b_ae_case_receiver_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  e2baecaserecv_ari_rec_id,city  e2baecaserecv_city,comp_rec_id  e2baecaserecv_comp_rec_id,countrycode  e2baecaserecv_countrycode,date_created  e2baecaserecv_date_created,date_modified  e2baecaserecv_date_modified,department  e2baecaserecv_department,email_address  e2baecaserecv_email_address,entity_updated  e2baecaserecv_entity_updated,ext_clob_fld  e2baecaserecv_ext_clob_fld,familyname  e2baecaserecv_familyname,fax  e2baecaserecv_fax,fax_countrycode  e2baecaserecv_fax_countrycode,fax_extension  e2baecaserecv_fax_extension,givename  e2baecaserecv_givename,inq_rec_id  e2baecaserecv_inq_rec_id,middlename  e2baecaserecv_middlename,organization  e2baecaserecv_organization,postcode  e2baecaserecv_postcode,receiver_state  e2baecaserecv_receiver_state,receiver_type  e2baecaserecv_receiver_type,record_id  e2baecaserecv_record_id,spr_id  e2baecaserecv_spr_id,street_address  e2baecaserecv_street_address,tel  e2baecaserecv_tel,tel_countrycode  e2baecaserecv_tel_countrycode,tel_extension  e2baecaserecv_tel_extension,title  e2baecaserecv_title,user_created  e2baecaserecv_user_created,user_modified  e2baecaserecv_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_receiver
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_receiver')
  ) where REC_RANK=1 )
  , lsmv_e2b_ae_case_sender_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  e2baecasesndr_ari_rec_id,city  e2baecasesndr_city,comp_rec_id  e2baecasesndr_comp_rec_id,countrycode  e2baecasesndr_countrycode,date_created  e2baecasesndr_date_created,date_modified  e2baecasesndr_date_modified,department  e2baecasesndr_department,email_address  e2baecasesndr_email_address,entity_updated  e2baecasesndr_entity_updated,ext_clob_fld  e2baecasesndr_ext_clob_fld,familyname  e2baecasesndr_familyname,fax  e2baecasesndr_fax,fax_countrycode  e2baecasesndr_fax_countrycode,fax_extension  e2baecasesndr_fax_extension,givename  e2baecasesndr_givename,inq_rec_id  e2baecasesndr_inq_rec_id,middlename  e2baecasesndr_middlename,organization  e2baecasesndr_organization,postcode  e2baecasesndr_postcode,record_id  e2baecasesndr_record_id,sender_state  e2baecasesndr_sender_state,sender_type  e2baecasesndr_sender_type,spr_id  e2baecasesndr_spr_id,street_address  e2baecasesndr_street_address,tel  e2baecasesndr_tel,tel_countrycode  e2baecasesndr_tel_countrycode,tel_extension  e2baecasesndr_tel_extension,title  e2baecasesndr_title,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5014' AND CD=CAST(title AS VARCHAR(100)) )e2baecasesndr_title_de_ml , title_sf  e2baecasesndr_title_sf,user_created  e2baecasesndr_user_created,user_modified  e2baecasesndr_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_e2b_ae_case_sender
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_sender')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_USER_MODIFIED USER_MODIFIED,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_USER_CREATED CREATED_BY,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_user_modified  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_user_created  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_title_sf  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_title_de_ml  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_title  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_tel_extension  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_tel_countrycode  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_tel  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_street_address  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_spr_id  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_sender_type  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_sender_state  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_record_id  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_postcode  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_organization  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_middlename  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_inq_rec_id  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_givename  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_fax_extension  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_fax_countrycode  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_fax  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_familyname  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_ext_clob_fld  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_entity_updated  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_email_address  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_department  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_date_modified  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_date_created  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_countrycode  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_comp_rec_id  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_city  ,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_ari_rec_id  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_user_modified  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_user_created  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_title  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_tel_extension  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_tel_countrycode  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_tel  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_street_address  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_spr_id  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_record_id  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_receiver_type  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_receiver_state  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_postcode  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_organization  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_middlename  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_inq_rec_id  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_givename  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_fax_extension  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_fax_countrycode  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_fax  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_familyname  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_ext_clob_fld  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_entity_updated  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_email_address  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_department  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_date_modified  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_date_created  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_countrycode  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_comp_rec_id  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_city  ,lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_ari_rec_id ,CONCAT( NVL(lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_RECORD_ID,-1),'||',NVL(lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_e2b_ae_case_sender_SUBSET  FULL OUTER JOIN lsmv_e2b_ae_case_receiver_SUBSET ON lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_ARI_REC_ID=lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_ARI_REC_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_e2b_ae_case_receiver_SUBSET.e2baecaserecv_RECORD_ID,lsmv_e2b_ae_case_sender_SUBSET.e2baecasesndr_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP)
where target_table_name='LS_DB_AE_SENDER_RECEIVER'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER   
SET LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_user_modified = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_user_modified,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_user_created = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_user_created,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_title_sf = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_title_sf,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_title_de_ml = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_title_de_ml,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_title = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_title,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_tel_extension = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_tel_extension,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_tel_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_tel_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_tel = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_tel,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_street_address = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_street_address,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_spr_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_spr_id,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_sender_type = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_sender_type,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_sender_state = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_sender_state,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_record_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_record_id,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_postcode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_postcode,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_organization = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_organization,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_middlename = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_middlename,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_inq_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_inq_rec_id,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_givename = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_givename,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_fax_extension = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_fax_extension,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_fax_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_fax_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_fax = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_fax,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_familyname = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_familyname,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_ext_clob_fld = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_ext_clob_fld,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_entity_updated = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_entity_updated,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_email_address = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_email_address,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_department = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_department,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_date_modified = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_date_modified,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_date_created = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_date_created,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_comp_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_comp_rec_id,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_city = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_city,LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_ari_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_ari_rec_id,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_user_modified = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_user_modified,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_user_created = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_user_created,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_title = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_title,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_tel_extension = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_tel_extension,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_tel_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_tel_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_tel = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_tel,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_street_address = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_street_address,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_spr_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_spr_id,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_record_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_record_id,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_receiver_type = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_receiver_type,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_receiver_state = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_receiver_state,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_postcode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_postcode,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_organization = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_organization,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_middlename = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_middlename,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_inq_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_inq_rec_id,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_givename = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_givename,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_fax_extension = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_fax_extension,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_fax_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_fax_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_fax = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_fax,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_familyname = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_familyname,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_ext_clob_fld = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_ext_clob_fld,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_entity_updated = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_entity_updated,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_email_address = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_email_address,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_department = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_department,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_date_modified = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_date_modified,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_date_created = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_date_created,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_countrycode = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_countrycode,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_comp_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_comp_rec_id,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_city = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_city,LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_ari_rec_id = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_ari_rec_id,
LS_DB_AE_SENDER_RECEIVER.PROCESSING_DT = LS_DB_AE_SENDER_RECEIVER_TMP.PROCESSING_DT,
LS_DB_AE_SENDER_RECEIVER.receipt_id     =LS_DB_AE_SENDER_RECEIVER_TMP.receipt_id    ,
LS_DB_AE_SENDER_RECEIVER.case_no        =LS_DB_AE_SENDER_RECEIVER_TMP.case_no           ,
LS_DB_AE_SENDER_RECEIVER.case_version   =LS_DB_AE_SENDER_RECEIVER_TMP.case_version      ,
LS_DB_AE_SENDER_RECEIVER.version_no     =LS_DB_AE_SENDER_RECEIVER_TMP.version_no        ,
LS_DB_AE_SENDER_RECEIVER.user_modified  =LS_DB_AE_SENDER_RECEIVER_TMP.user_modified     ,
LS_DB_AE_SENDER_RECEIVER.date_modified  =LS_DB_AE_SENDER_RECEIVER_TMP.date_modified     ,
LS_DB_AE_SENDER_RECEIVER.expiry_date    =LS_DB_AE_SENDER_RECEIVER_TMP.expiry_date       ,
LS_DB_AE_SENDER_RECEIVER.created_by     =LS_DB_AE_SENDER_RECEIVER_TMP.created_by        ,
LS_DB_AE_SENDER_RECEIVER.created_dt     =LS_DB_AE_SENDER_RECEIVER_TMP.created_dt        ,
LS_DB_AE_SENDER_RECEIVER.load_ts        =LS_DB_AE_SENDER_RECEIVER_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP 
WHERE 	LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID = LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_AE_SENDER_RECEIVER_TMP.PROCESSING_DT = LS_DB_AE_SENDER_RECEIVER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_AE_SENDER_RECEIVER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID ,LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP 
ON LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID=LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_RECORD_ID
AND LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID = LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID 
where LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID  is null AND LS_DB_AE_SENDER_RECEIVER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID in (select e2baecasesndr_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP )
) TMP where LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_AE_SENDER_RECEIVER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID ,LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP 
    ON LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID=LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_RECORD_ID
    AND LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID = LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID 
    where LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID  is null AND LS_DB_AE_SENDER_RECEIVER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID in (select e2baecasesndr_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER
( receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,e2baecasesndr_user_modified,
e2baecasesndr_user_created,
e2baecasesndr_title_sf,
e2baecasesndr_title_de_ml,
e2baecasesndr_title,
e2baecasesndr_tel_extension,
e2baecasesndr_tel_countrycode,
e2baecasesndr_tel,
e2baecasesndr_street_address,
e2baecasesndr_spr_id,
e2baecasesndr_sender_type,
e2baecasesndr_sender_state,
e2baecasesndr_record_id,
e2baecasesndr_postcode,
e2baecasesndr_organization,
e2baecasesndr_middlename,
e2baecasesndr_inq_rec_id,
e2baecasesndr_givename,
e2baecasesndr_fax_extension,
e2baecasesndr_fax_countrycode,
e2baecasesndr_fax,
e2baecasesndr_familyname,
e2baecasesndr_ext_clob_fld,
e2baecasesndr_entity_updated,
e2baecasesndr_email_address,
e2baecasesndr_department,
e2baecasesndr_date_modified,
e2baecasesndr_date_created,
e2baecasesndr_countrycode,
e2baecasesndr_comp_rec_id,
e2baecasesndr_city,
e2baecasesndr_ari_rec_id,
e2baecaserecv_user_modified,
e2baecaserecv_user_created,
e2baecaserecv_title,
e2baecaserecv_tel_extension,
e2baecaserecv_tel_countrycode,
e2baecaserecv_tel,
e2baecaserecv_street_address,
e2baecaserecv_spr_id,
e2baecaserecv_record_id,
e2baecaserecv_receiver_type,
e2baecaserecv_receiver_state,
e2baecaserecv_postcode,
e2baecaserecv_organization,
e2baecaserecv_middlename,
e2baecaserecv_inq_rec_id,
e2baecaserecv_givename,
e2baecaserecv_fax_extension,
e2baecaserecv_fax_countrycode,
e2baecaserecv_fax,
e2baecaserecv_familyname,
e2baecaserecv_ext_clob_fld,
e2baecaserecv_entity_updated,
e2baecaserecv_email_address,
e2baecaserecv_department,
e2baecaserecv_date_modified,
e2baecaserecv_date_created,
e2baecaserecv_countrycode,
e2baecaserecv_comp_rec_id,
e2baecaserecv_city,
e2baecaserecv_ari_rec_id)
SELECT 
 receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,e2baecasesndr_user_modified,
e2baecasesndr_user_created,
e2baecasesndr_title_sf,
e2baecasesndr_title_de_ml,
e2baecasesndr_title,
e2baecasesndr_tel_extension,
e2baecasesndr_tel_countrycode,
e2baecasesndr_tel,
e2baecasesndr_street_address,
e2baecasesndr_spr_id,
e2baecasesndr_sender_type,
e2baecasesndr_sender_state,
e2baecasesndr_record_id,
e2baecasesndr_postcode,
e2baecasesndr_organization,
e2baecasesndr_middlename,
e2baecasesndr_inq_rec_id,
e2baecasesndr_givename,
e2baecasesndr_fax_extension,
e2baecasesndr_fax_countrycode,
e2baecasesndr_fax,
e2baecasesndr_familyname,
e2baecasesndr_ext_clob_fld,
e2baecasesndr_entity_updated,
e2baecasesndr_email_address,
e2baecasesndr_department,
e2baecasesndr_date_modified,
e2baecasesndr_date_created,
e2baecasesndr_countrycode,
e2baecasesndr_comp_rec_id,
e2baecasesndr_city,
e2baecasesndr_ari_rec_id,
e2baecaserecv_user_modified,
e2baecaserecv_user_created,
e2baecaserecv_title,
e2baecaserecv_tel_extension,
e2baecaserecv_tel_countrycode,
e2baecaserecv_tel,
e2baecaserecv_street_address,
e2baecaserecv_spr_id,
e2baecaserecv_record_id,
e2baecaserecv_receiver_type,
e2baecaserecv_receiver_state,
e2baecaserecv_postcode,
e2baecaserecv_organization,
e2baecaserecv_middlename,
e2baecaserecv_inq_rec_id,
e2baecaserecv_givename,
e2baecaserecv_fax_extension,
e2baecaserecv_fax_countrycode,
e2baecaserecv_fax,
e2baecaserecv_familyname,
e2baecaserecv_ext_clob_fld,
e2baecaserecv_entity_updated,
e2baecaserecv_email_address,
e2baecaserecv_department,
e2baecaserecv_date_modified,
e2baecaserecv_date_created,
e2baecaserecv_countrycode,
e2baecaserecv_comp_rec_id,
e2baecaserecv_city,
e2baecaserecv_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER TGT
WHERE  ( e2baecaserecv_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_receiver') OR e2baecasesndr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_sender')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP 
WHERE 	TO_DATE(LS_DB_AE_SENDER_RECEIVER.PROCESSING_DT) < TO_DATE(LS_DB_AE_SENDER_RECEIVER_TMP.PROCESSING_DT)
AND LS_DB_AE_SENDER_RECEIVER.INTEGRATION_ID = LS_DB_AE_SENDER_RECEIVER_TMP.INTEGRATION_ID
AND LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_RECORD_ID = LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_RECORD_ID
AND LS_DB_AE_SENDER_RECEIVER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_AE_SENDER_RECEIVER.e2baecaserecv_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_AE_SENDER_RECEIVER.e2baecasesndr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_AE_SENDER_RECEIVER_TMP.e2baecaserecv_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_AE_SENDER_RECEIVER_TMP.e2baecasesndr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( e2baecaserecv_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_receiver') OR e2baecasesndr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_AE_SENDER_RECEIVER_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_ae_case_sender')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_AE_SENDER_RECEIVER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_AE_SENDER_RECEIVER'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_AE_SENDER_RECEIVER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_AE_SENDER_RECEIVER'),
	'LSDB','Case','LS_DB_AE_SENDER_RECEIVER',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_AE_SENDER_RECEIVER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
