
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_LITERATURE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LITERATURE'),
	'LSDB','Case','LS_DB_LITERATURE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_LITERATURE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_LITERATURE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  As select RECORD_ID,'lsmv_literature' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_literature_author' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature_author WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1003','1015','5014','7077')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LITERATURE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LITERATURE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature_author WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LITERATURE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_lit_record_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_literature_author WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_LITERATURE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_literature_author_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  litauth_ari_rec_id,author_title  litauth_author_title,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5014' AND CD=CAST(author_title AS VARCHAR(100)) )litauth_author_title_de_ml , author_title_nf  litauth_author_title_nf,author_title_sf  litauth_author_title_sf,city  litauth_city,city_nf  litauth_city_nf,comp_rec_id  litauth_comp_rec_id,country  litauth_country,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(country AS VARCHAR(100)) )litauth_country_de_ml , country_nf  litauth_country_nf,date_created  litauth_date_created,date_modified  litauth_date_modified,department  litauth_department,department_nf  litauth_department_nf,email  litauth_email,entity_updated  litauth_entity_updated,fax  litauth_fax,fk_lit_record_id  litauth_fk_lit_record_id,given_name  litauth_given_name,given_name_nf  litauth_given_name_nf,inq_rec_id  litauth_inq_rec_id,last_name  litauth_last_name,last_name_nf  litauth_last_name_nf,middle_name  litauth_middle_name,middle_name_nf  litauth_middle_name_nf,organization  litauth_organization,organization_nf  litauth_organization_nf,phone  litauth_phone,phone_nf  litauth_phone_nf,postcode  litauth_postcode,postcode_nf  litauth_postcode_nf,qualification  litauth_qualification,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1003' AND CD=CAST(qualification AS VARCHAR(100)) )litauth_qualification_de_ml , qualification_nf  litauth_qualification_nf,record_id  litauth_record_id,spanish_state_code  litauth_spanish_state_code,spr_id  litauth_spr_id,state  litauth_state,state_nf  litauth_state_nf,street  litauth_street,street_nf  litauth_street_nf,tel_country_code  litauth_tel_country_code,tel_extension  litauth_tel_extension,user_created  litauth_user_created,user_modified  litauth_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_literature_author
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_lit_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature_author')
  ) where REC_RANK=1 )
  , lsmv_literature_SUBSET AS 
(
select * from 
    (SELECT  
    additional_lit_information  lit_additional_lit_information,ari_rec_id  lit_ari_rec_id,article_title  lit_article_title,author_name  lit_author_name,c_volume_issue_number  lit_c_volume_issue_number,comp_rec_id  lit_comp_rec_id,date_created  lit_date_created,date_modified  lit_date_modified,dig_obj_identifier  lit_dig_obj_identifier,edition  lit_edition,entity_updated  lit_entity_updated,ext_clob_fld  lit_ext_clob_fld,fk_asr_rec_id  lit_fk_asr_rec_id,included_documents  lit_included_documents,inq_rec_id  lit_inq_rec_id,issue  lit_issue,journal_title  lit_journal_title,library_no  lit_library_no,literature_doc_id  lit_literature_doc_id,literature_doc_name  lit_literature_doc_name,literature_doc_size  lit_literature_doc_size,literature_doc_user  lit_literature_doc_user,literaturereference  lit_literaturereference,literaturereference_nf  lit_literaturereference_nf,litrature_uiid  lit_litrature_uiid,other_info  lit_other_info,page_from  lit_page_from,page_to  lit_page_to,pub_date  lit_pub_date,pub_date_fmt  lit_pub_date_fmt,record_id  lit_record_id,retainliteraturereference  lit_retainliteraturereference,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(retainliteraturereference AS VARCHAR(100)) )lit_retainliteraturereference_de_ml , spr_id  lit_spr_id,user_created  lit_user_created,user_modified  lit_user_modified,version  lit_version,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_literature
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_literature_author_SUBSET.litauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_literature_SUBSET.lit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_literature_SUBSET.lit_USER_MODIFIED USER_MODIFIED,lsmv_literature_SUBSET.lit_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_literature_SUBSET.lit_USER_CREATED CREATED_BY,lsmv_literature_SUBSET.lit_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_literature_SUBSET.lit_version  ,lsmv_literature_SUBSET.lit_user_modified  ,lsmv_literature_SUBSET.lit_user_created  ,lsmv_literature_SUBSET.lit_spr_id  ,lsmv_literature_SUBSET.lit_retainliteraturereference_de_ml  ,lsmv_literature_SUBSET.lit_retainliteraturereference  ,lsmv_literature_SUBSET.lit_record_id  ,lsmv_literature_SUBSET.lit_pub_date_fmt  ,lsmv_literature_SUBSET.lit_pub_date  ,lsmv_literature_SUBSET.lit_page_to  ,lsmv_literature_SUBSET.lit_page_from  ,lsmv_literature_SUBSET.lit_other_info  ,lsmv_literature_SUBSET.lit_litrature_uiid  ,lsmv_literature_SUBSET.lit_literaturereference_nf  ,lsmv_literature_SUBSET.lit_literaturereference  ,lsmv_literature_SUBSET.lit_literature_doc_user  ,lsmv_literature_SUBSET.lit_literature_doc_size  ,lsmv_literature_SUBSET.lit_literature_doc_name  ,lsmv_literature_SUBSET.lit_literature_doc_id  ,lsmv_literature_SUBSET.lit_library_no  ,lsmv_literature_SUBSET.lit_journal_title  ,lsmv_literature_SUBSET.lit_issue  ,lsmv_literature_SUBSET.lit_inq_rec_id  ,lsmv_literature_SUBSET.lit_included_documents  ,lsmv_literature_SUBSET.lit_fk_asr_rec_id  ,lsmv_literature_SUBSET.lit_ext_clob_fld  ,lsmv_literature_SUBSET.lit_entity_updated  ,lsmv_literature_SUBSET.lit_edition  ,lsmv_literature_SUBSET.lit_dig_obj_identifier  ,lsmv_literature_SUBSET.lit_date_modified  ,lsmv_literature_SUBSET.lit_date_created  ,lsmv_literature_SUBSET.lit_comp_rec_id  ,lsmv_literature_SUBSET.lit_c_volume_issue_number  ,lsmv_literature_SUBSET.lit_author_name  ,lsmv_literature_SUBSET.lit_article_title  ,lsmv_literature_SUBSET.lit_ari_rec_id  ,lsmv_literature_SUBSET.lit_additional_lit_information  ,lsmv_literature_author_SUBSET.litauth_user_modified  ,lsmv_literature_author_SUBSET.litauth_user_created  ,lsmv_literature_author_SUBSET.litauth_tel_extension  ,lsmv_literature_author_SUBSET.litauth_tel_country_code  ,lsmv_literature_author_SUBSET.litauth_street_nf  ,lsmv_literature_author_SUBSET.litauth_street  ,lsmv_literature_author_SUBSET.litauth_state_nf  ,lsmv_literature_author_SUBSET.litauth_state  ,lsmv_literature_author_SUBSET.litauth_spr_id  ,lsmv_literature_author_SUBSET.litauth_spanish_state_code  ,lsmv_literature_author_SUBSET.litauth_record_id  ,lsmv_literature_author_SUBSET.litauth_qualification_nf  ,lsmv_literature_author_SUBSET.litauth_qualification_de_ml  ,lsmv_literature_author_SUBSET.litauth_qualification  ,lsmv_literature_author_SUBSET.litauth_postcode_nf  ,lsmv_literature_author_SUBSET.litauth_postcode  ,lsmv_literature_author_SUBSET.litauth_phone_nf  ,lsmv_literature_author_SUBSET.litauth_phone  ,lsmv_literature_author_SUBSET.litauth_organization_nf  ,lsmv_literature_author_SUBSET.litauth_organization  ,lsmv_literature_author_SUBSET.litauth_middle_name_nf  ,lsmv_literature_author_SUBSET.litauth_middle_name  ,lsmv_literature_author_SUBSET.litauth_last_name_nf  ,lsmv_literature_author_SUBSET.litauth_last_name  ,lsmv_literature_author_SUBSET.litauth_inq_rec_id  ,lsmv_literature_author_SUBSET.litauth_given_name_nf  ,lsmv_literature_author_SUBSET.litauth_given_name  ,lsmv_literature_author_SUBSET.litauth_fk_lit_record_id  ,lsmv_literature_author_SUBSET.litauth_fax  ,lsmv_literature_author_SUBSET.litauth_entity_updated  ,lsmv_literature_author_SUBSET.litauth_email  ,lsmv_literature_author_SUBSET.litauth_department_nf  ,lsmv_literature_author_SUBSET.litauth_department  ,lsmv_literature_author_SUBSET.litauth_date_modified  ,lsmv_literature_author_SUBSET.litauth_date_created  ,lsmv_literature_author_SUBSET.litauth_country_nf  ,lsmv_literature_author_SUBSET.litauth_country_de_ml  ,lsmv_literature_author_SUBSET.litauth_country  ,lsmv_literature_author_SUBSET.litauth_comp_rec_id  ,lsmv_literature_author_SUBSET.litauth_city_nf  ,lsmv_literature_author_SUBSET.litauth_city  ,lsmv_literature_author_SUBSET.litauth_author_title_sf  ,lsmv_literature_author_SUBSET.litauth_author_title_nf  ,lsmv_literature_author_SUBSET.litauth_author_title_de_ml  ,lsmv_literature_author_SUBSET.litauth_author_title  ,lsmv_literature_author_SUBSET.litauth_ari_rec_id ,CONCAT( NVL(lsmv_literature_author_SUBSET.litauth_RECORD_ID,-1),'||',NVL(lsmv_literature_SUBSET.lit_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_literature_SUBSET  LEFT JOIN lsmv_literature_author_SUBSET ON lsmv_literature_SUBSET.lit_record_id=lsmv_literature_author_SUBSET.litauth_fk_lit_record_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_literature_author_SUBSET.litauth_RECORD_ID,lsmv_literature_SUBSET.lit_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP)
where target_table_name='LS_DB_LITERATURE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE   
SET LS_DB_LITERATURE.lit_version = LS_DB_LITERATURE_TMP.lit_version,LS_DB_LITERATURE.lit_user_modified = LS_DB_LITERATURE_TMP.lit_user_modified,LS_DB_LITERATURE.lit_user_created = LS_DB_LITERATURE_TMP.lit_user_created,LS_DB_LITERATURE.lit_spr_id = LS_DB_LITERATURE_TMP.lit_spr_id,LS_DB_LITERATURE.lit_retainliteraturereference_de_ml = LS_DB_LITERATURE_TMP.lit_retainliteraturereference_de_ml,LS_DB_LITERATURE.lit_retainliteraturereference = LS_DB_LITERATURE_TMP.lit_retainliteraturereference,LS_DB_LITERATURE.lit_record_id = LS_DB_LITERATURE_TMP.lit_record_id,LS_DB_LITERATURE.lit_pub_date_fmt = LS_DB_LITERATURE_TMP.lit_pub_date_fmt,LS_DB_LITERATURE.lit_pub_date = LS_DB_LITERATURE_TMP.lit_pub_date,LS_DB_LITERATURE.lit_page_to = LS_DB_LITERATURE_TMP.lit_page_to,LS_DB_LITERATURE.lit_page_from = LS_DB_LITERATURE_TMP.lit_page_from,LS_DB_LITERATURE.lit_other_info = LS_DB_LITERATURE_TMP.lit_other_info,LS_DB_LITERATURE.lit_litrature_uiid = LS_DB_LITERATURE_TMP.lit_litrature_uiid,LS_DB_LITERATURE.lit_literaturereference_nf = LS_DB_LITERATURE_TMP.lit_literaturereference_nf,LS_DB_LITERATURE.lit_literaturereference = LS_DB_LITERATURE_TMP.lit_literaturereference,LS_DB_LITERATURE.lit_literature_doc_user = LS_DB_LITERATURE_TMP.lit_literature_doc_user,LS_DB_LITERATURE.lit_literature_doc_size = LS_DB_LITERATURE_TMP.lit_literature_doc_size,LS_DB_LITERATURE.lit_literature_doc_name = LS_DB_LITERATURE_TMP.lit_literature_doc_name,LS_DB_LITERATURE.lit_literature_doc_id = LS_DB_LITERATURE_TMP.lit_literature_doc_id,LS_DB_LITERATURE.lit_library_no = LS_DB_LITERATURE_TMP.lit_library_no,LS_DB_LITERATURE.lit_journal_title = LS_DB_LITERATURE_TMP.lit_journal_title,LS_DB_LITERATURE.lit_issue = LS_DB_LITERATURE_TMP.lit_issue,LS_DB_LITERATURE.lit_inq_rec_id = LS_DB_LITERATURE_TMP.lit_inq_rec_id,LS_DB_LITERATURE.lit_included_documents = LS_DB_LITERATURE_TMP.lit_included_documents,LS_DB_LITERATURE.lit_fk_asr_rec_id = LS_DB_LITERATURE_TMP.lit_fk_asr_rec_id,LS_DB_LITERATURE.lit_ext_clob_fld = LS_DB_LITERATURE_TMP.lit_ext_clob_fld,LS_DB_LITERATURE.lit_entity_updated = LS_DB_LITERATURE_TMP.lit_entity_updated,LS_DB_LITERATURE.lit_edition = LS_DB_LITERATURE_TMP.lit_edition,LS_DB_LITERATURE.lit_dig_obj_identifier = LS_DB_LITERATURE_TMP.lit_dig_obj_identifier,LS_DB_LITERATURE.lit_date_modified = LS_DB_LITERATURE_TMP.lit_date_modified,LS_DB_LITERATURE.lit_date_created = LS_DB_LITERATURE_TMP.lit_date_created,LS_DB_LITERATURE.lit_comp_rec_id = LS_DB_LITERATURE_TMP.lit_comp_rec_id,LS_DB_LITERATURE.lit_c_volume_issue_number = LS_DB_LITERATURE_TMP.lit_c_volume_issue_number,LS_DB_LITERATURE.lit_author_name = LS_DB_LITERATURE_TMP.lit_author_name,LS_DB_LITERATURE.lit_article_title = LS_DB_LITERATURE_TMP.lit_article_title,LS_DB_LITERATURE.lit_ari_rec_id = LS_DB_LITERATURE_TMP.lit_ari_rec_id,LS_DB_LITERATURE.lit_additional_lit_information = LS_DB_LITERATURE_TMP.lit_additional_lit_information,LS_DB_LITERATURE.litauth_user_modified = LS_DB_LITERATURE_TMP.litauth_user_modified,LS_DB_LITERATURE.litauth_user_created = LS_DB_LITERATURE_TMP.litauth_user_created,LS_DB_LITERATURE.litauth_tel_extension = LS_DB_LITERATURE_TMP.litauth_tel_extension,LS_DB_LITERATURE.litauth_tel_country_code = LS_DB_LITERATURE_TMP.litauth_tel_country_code,LS_DB_LITERATURE.litauth_street_nf = LS_DB_LITERATURE_TMP.litauth_street_nf,LS_DB_LITERATURE.litauth_street = LS_DB_LITERATURE_TMP.litauth_street,LS_DB_LITERATURE.litauth_state_nf = LS_DB_LITERATURE_TMP.litauth_state_nf,LS_DB_LITERATURE.litauth_state = LS_DB_LITERATURE_TMP.litauth_state,LS_DB_LITERATURE.litauth_spr_id = LS_DB_LITERATURE_TMP.litauth_spr_id,LS_DB_LITERATURE.litauth_spanish_state_code = LS_DB_LITERATURE_TMP.litauth_spanish_state_code,LS_DB_LITERATURE.litauth_record_id = LS_DB_LITERATURE_TMP.litauth_record_id,LS_DB_LITERATURE.litauth_qualification_nf = LS_DB_LITERATURE_TMP.litauth_qualification_nf,LS_DB_LITERATURE.litauth_qualification_de_ml = LS_DB_LITERATURE_TMP.litauth_qualification_de_ml,LS_DB_LITERATURE.litauth_qualification = LS_DB_LITERATURE_TMP.litauth_qualification,LS_DB_LITERATURE.litauth_postcode_nf = LS_DB_LITERATURE_TMP.litauth_postcode_nf,LS_DB_LITERATURE.litauth_postcode = LS_DB_LITERATURE_TMP.litauth_postcode,LS_DB_LITERATURE.litauth_phone_nf = LS_DB_LITERATURE_TMP.litauth_phone_nf,LS_DB_LITERATURE.litauth_phone = LS_DB_LITERATURE_TMP.litauth_phone,LS_DB_LITERATURE.litauth_organization_nf = LS_DB_LITERATURE_TMP.litauth_organization_nf,LS_DB_LITERATURE.litauth_organization = LS_DB_LITERATURE_TMP.litauth_organization,LS_DB_LITERATURE.litauth_middle_name_nf = LS_DB_LITERATURE_TMP.litauth_middle_name_nf,LS_DB_LITERATURE.litauth_middle_name = LS_DB_LITERATURE_TMP.litauth_middle_name,LS_DB_LITERATURE.litauth_last_name_nf = LS_DB_LITERATURE_TMP.litauth_last_name_nf,LS_DB_LITERATURE.litauth_last_name = LS_DB_LITERATURE_TMP.litauth_last_name,LS_DB_LITERATURE.litauth_inq_rec_id = LS_DB_LITERATURE_TMP.litauth_inq_rec_id,LS_DB_LITERATURE.litauth_given_name_nf = LS_DB_LITERATURE_TMP.litauth_given_name_nf,LS_DB_LITERATURE.litauth_given_name = LS_DB_LITERATURE_TMP.litauth_given_name,LS_DB_LITERATURE.litauth_fk_lit_record_id = LS_DB_LITERATURE_TMP.litauth_fk_lit_record_id,LS_DB_LITERATURE.litauth_fax = LS_DB_LITERATURE_TMP.litauth_fax,LS_DB_LITERATURE.litauth_entity_updated = LS_DB_LITERATURE_TMP.litauth_entity_updated,LS_DB_LITERATURE.litauth_email = LS_DB_LITERATURE_TMP.litauth_email,LS_DB_LITERATURE.litauth_department_nf = LS_DB_LITERATURE_TMP.litauth_department_nf,LS_DB_LITERATURE.litauth_department = LS_DB_LITERATURE_TMP.litauth_department,LS_DB_LITERATURE.litauth_date_modified = LS_DB_LITERATURE_TMP.litauth_date_modified,LS_DB_LITERATURE.litauth_date_created = LS_DB_LITERATURE_TMP.litauth_date_created,LS_DB_LITERATURE.litauth_country_nf = LS_DB_LITERATURE_TMP.litauth_country_nf,LS_DB_LITERATURE.litauth_country_de_ml = LS_DB_LITERATURE_TMP.litauth_country_de_ml,LS_DB_LITERATURE.litauth_country = LS_DB_LITERATURE_TMP.litauth_country,LS_DB_LITERATURE.litauth_comp_rec_id = LS_DB_LITERATURE_TMP.litauth_comp_rec_id,LS_DB_LITERATURE.litauth_city_nf = LS_DB_LITERATURE_TMP.litauth_city_nf,LS_DB_LITERATURE.litauth_city = LS_DB_LITERATURE_TMP.litauth_city,LS_DB_LITERATURE.litauth_author_title_sf = LS_DB_LITERATURE_TMP.litauth_author_title_sf,LS_DB_LITERATURE.litauth_author_title_nf = LS_DB_LITERATURE_TMP.litauth_author_title_nf,LS_DB_LITERATURE.litauth_author_title_de_ml = LS_DB_LITERATURE_TMP.litauth_author_title_de_ml,LS_DB_LITERATURE.litauth_author_title = LS_DB_LITERATURE_TMP.litauth_author_title,LS_DB_LITERATURE.litauth_ari_rec_id = LS_DB_LITERATURE_TMP.litauth_ari_rec_id,
LS_DB_LITERATURE.PROCESSING_DT = LS_DB_LITERATURE_TMP.PROCESSING_DT,
LS_DB_LITERATURE.receipt_id     =LS_DB_LITERATURE_TMP.receipt_id    ,
LS_DB_LITERATURE.case_no        =LS_DB_LITERATURE_TMP.case_no           ,
LS_DB_LITERATURE.case_version   =LS_DB_LITERATURE_TMP.case_version      ,
LS_DB_LITERATURE.version_no     =LS_DB_LITERATURE_TMP.version_no        ,
LS_DB_LITERATURE.user_modified  =LS_DB_LITERATURE_TMP.user_modified     ,
LS_DB_LITERATURE.date_modified  =LS_DB_LITERATURE_TMP.date_modified     ,
LS_DB_LITERATURE.expiry_date    =LS_DB_LITERATURE_TMP.expiry_date       ,
LS_DB_LITERATURE.created_by     =LS_DB_LITERATURE_TMP.created_by        ,
LS_DB_LITERATURE.created_dt     =LS_DB_LITERATURE_TMP.created_dt        ,
LS_DB_LITERATURE.load_ts        =LS_DB_LITERATURE_TMP.load_ts          
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP 
WHERE 	LS_DB_LITERATURE.INTEGRATION_ID = LS_DB_LITERATURE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_LITERATURE_TMP.PROCESSING_DT = LS_DB_LITERATURE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_LITERATURE.litauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_LITERATURE.lit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_LITERATURE_TMP.litauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_LITERATURE_TMP.lit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_LITERATURE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_LITERATURE.lit_RECORD_ID ,LS_DB_LITERATURE.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP 
ON LS_DB_LITERATURE.lit_RECORD_ID=LS_DB_LITERATURE_TMP.lit_RECORD_ID
AND LS_DB_LITERATURE.INTEGRATION_ID = LS_DB_LITERATURE_TMP.INTEGRATION_ID 
where LS_DB_LITERATURE_TMP.INTEGRATION_ID  is null AND LS_DB_LITERATURE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_LITERATURE.lit_RECORD_ID in (select lit_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP )
) TMP where LS_DB_LITERATURE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_LITERATURE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_LITERATURE.lit_RECORD_ID ,LS_DB_LITERATURE.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP 
    ON LS_DB_LITERATURE.lit_RECORD_ID=LS_DB_LITERATURE_TMP.lit_RECORD_ID
    AND LS_DB_LITERATURE.INTEGRATION_ID = LS_DB_LITERATURE_TMP.INTEGRATION_ID 
    where LS_DB_LITERATURE_TMP.INTEGRATION_ID  is null AND LS_DB_LITERATURE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_LITERATURE.lit_RECORD_ID in (select lit_RECORD_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE
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
integration_id ,lit_version,
lit_user_modified,
lit_user_created,
lit_spr_id,
lit_retainliteraturereference_de_ml,
lit_retainliteraturereference,
lit_record_id,
lit_pub_date_fmt,
lit_pub_date,
lit_page_to,
lit_page_from,
lit_other_info,
lit_litrature_uiid,
lit_literaturereference_nf,
lit_literaturereference,
lit_literature_doc_user,
lit_literature_doc_size,
lit_literature_doc_name,
lit_literature_doc_id,
lit_library_no,
lit_journal_title,
lit_issue,
lit_inq_rec_id,
lit_included_documents,
lit_fk_asr_rec_id,
lit_ext_clob_fld,
lit_entity_updated,
lit_edition,
lit_dig_obj_identifier,
lit_date_modified,
lit_date_created,
lit_comp_rec_id,
lit_c_volume_issue_number,
lit_author_name,
lit_article_title,
lit_ari_rec_id,
lit_additional_lit_information,
litauth_user_modified,
litauth_user_created,
litauth_tel_extension,
litauth_tel_country_code,
litauth_street_nf,
litauth_street,
litauth_state_nf,
litauth_state,
litauth_spr_id,
litauth_spanish_state_code,
litauth_record_id,
litauth_qualification_nf,
litauth_qualification_de_ml,
litauth_qualification,
litauth_postcode_nf,
litauth_postcode,
litauth_phone_nf,
litauth_phone,
litauth_organization_nf,
litauth_organization,
litauth_middle_name_nf,
litauth_middle_name,
litauth_last_name_nf,
litauth_last_name,
litauth_inq_rec_id,
litauth_given_name_nf,
litauth_given_name,
litauth_fk_lit_record_id,
litauth_fax,
litauth_entity_updated,
litauth_email,
litauth_department_nf,
litauth_department,
litauth_date_modified,
litauth_date_created,
litauth_country_nf,
litauth_country_de_ml,
litauth_country,
litauth_comp_rec_id,
litauth_city_nf,
litauth_city,
litauth_author_title_sf,
litauth_author_title_nf,
litauth_author_title_de_ml,
litauth_author_title,
litauth_ari_rec_id)
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
integration_id ,lit_version,
lit_user_modified,
lit_user_created,
lit_spr_id,
lit_retainliteraturereference_de_ml,
lit_retainliteraturereference,
lit_record_id,
lit_pub_date_fmt,
lit_pub_date,
lit_page_to,
lit_page_from,
lit_other_info,
lit_litrature_uiid,
lit_literaturereference_nf,
lit_literaturereference,
lit_literature_doc_user,
lit_literature_doc_size,
lit_literature_doc_name,
lit_literature_doc_id,
lit_library_no,
lit_journal_title,
lit_issue,
lit_inq_rec_id,
lit_included_documents,
lit_fk_asr_rec_id,
lit_ext_clob_fld,
lit_entity_updated,
lit_edition,
lit_dig_obj_identifier,
lit_date_modified,
lit_date_created,
lit_comp_rec_id,
lit_c_volume_issue_number,
lit_author_name,
lit_article_title,
lit_ari_rec_id,
lit_additional_lit_information,
litauth_user_modified,
litauth_user_created,
litauth_tel_extension,
litauth_tel_country_code,
litauth_street_nf,
litauth_street,
litauth_state_nf,
litauth_state,
litauth_spr_id,
litauth_spanish_state_code,
litauth_record_id,
litauth_qualification_nf,
litauth_qualification_de_ml,
litauth_qualification,
litauth_postcode_nf,
litauth_postcode,
litauth_phone_nf,
litauth_phone,
litauth_organization_nf,
litauth_organization,
litauth_middle_name_nf,
litauth_middle_name,
litauth_last_name_nf,
litauth_last_name,
litauth_inq_rec_id,
litauth_given_name_nf,
litauth_given_name,
litauth_fk_lit_record_id,
litauth_fax,
litauth_entity_updated,
litauth_email,
litauth_department_nf,
litauth_department,
litauth_date_modified,
litauth_date_created,
litauth_country_nf,
litauth_country_de_ml,
litauth_country,
litauth_comp_rec_id,
litauth_city_nf,
litauth_city,
litauth_author_title_sf,
litauth_author_title_nf,
litauth_author_title_de_ml,
litauth_author_title,
litauth_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE TGT
WHERE  ( lit_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature') OR litauth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature_author')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP 
WHERE 	TO_DATE(LS_DB_LITERATURE.PROCESSING_DT) < TO_DATE(LS_DB_LITERATURE_TMP.PROCESSING_DT)
AND LS_DB_LITERATURE.INTEGRATION_ID = LS_DB_LITERATURE_TMP.INTEGRATION_ID
AND LS_DB_LITERATURE.lit_RECORD_ID = LS_DB_LITERATURE_TMP.lit_RECORD_ID
AND LS_DB_LITERATURE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_LITERATURE.litauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_LITERATURE.lit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_LITERATURE_TMP.litauth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_LITERATURE_TMP.lit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( lit_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature') OR litauth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LITERATURE_DELETION_TMP  WHERE TABLE_NAME='lsmv_literature_author')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LITERATURE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_LITERATURE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_LITERATURE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_LITERATURE'),
	'LSDB','Case','LS_DB_LITERATURE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_LITERATURE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
