
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PREGNANCY()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PREGNANCY'),
	'LSRA','Case','LS_DB_PREGNANCY',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PREGNANCY' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PREGNANCY'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_PREGNANCY'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  As select RECORD_ID,'lsmv_neonate' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_neonate_child' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate_child WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_pregnancy' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_pregnancy_child' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_child WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_pregnancy_outcomes' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_outcomes WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_previous_preg_outcome' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_previous_preg_outcome WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1002','1002','1002','1002','1002','10080','11','11','11','15','15','347','347','347','4','4','48','48','7073','8105','8108','8119','8120','8120','820','821','951','9969','9970','9986','9987','9988','9991','9994')
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
 
select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_previous_preg_outcome WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_previous_preg_outcome WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_child WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_child WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_outcomes WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_outcomes WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate_child WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate_child WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_PREGNANCY' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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
                                                                                      from $$STG_DB_NAME.$$LSDB_RPL.LSMV_AER_INFO AER_INFO,$$STG_DB_NAME.$$LSDB_RPL.LSMV_RECEIPT_ITEM RECPT_ITM, LSMV_CASE_NO_SUBSET
                                                                                       where RECPT_ITM.RECORD_ID=AER_INFO.ARI_REC_ID
                                                                                      and RECPT_ITM.RECORD_ID = LSMV_CASE_NO_SUBSET.ARI_REC_ID
                                           ) CASE_INFO
WHERE REC_RANK=1

), lsmv_pregnancy_child_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  pregchild_ari_rec_id,child_age  pregchild_child_age,child_sex  pregchild_child_sex,child_unit  pregchild_child_unit,date_created  pregchild_date_created,date_modified  pregchild_date_modified,fk_aprg_record_id  pregchild_fk_aprg_record_id,record_id  pregchild_record_id,spr_id  pregchild_spr_id,user_created  pregchild_user_created,user_modified  pregchild_user_modified,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_child
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_child')
  ) where REC_RANK=1 )
  , lsmv_neonate_child_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  neonchild_ari_rec_id,child_age  neonchild_child_age,child_unit  neonchild_child_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='11' AND CD=CAST(child_unit AS VARCHAR(100)) )neonchild_child_unit_de_ml , date_created  neonchild_date_created,date_modified  neonchild_date_modified,fk_an_record_id  neonchild_fk_an_record_id,head_circum_birth  neonchild_head_circum_birth,head_circum_birth_unit  neonchild_head_circum_birth_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='347' AND CD=CAST(head_circum_birth_unit AS VARCHAR(100)) )neonchild_head_circum_birth_unit_de_ml , neon_birth_len  neonchild_neon_birth_len,neon_birth_len_unit  neonchild_neon_birth_len_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='347' AND CD=CAST(neon_birth_len_unit AS VARCHAR(100)) )neonchild_neon_birth_len_unit_de_ml , neon_weight  neonchild_neon_weight,neon_weight_unit  neonchild_neon_weight_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='48' AND CD=CAST(neon_weight_unit AS VARCHAR(100)) )neonchild_neon_weight_unit_de_ml , record_id  neonchild_record_id,spr_id  neonchild_spr_id,user_created  neonchild_user_created,user_modified  neonchild_user_modified,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate_child
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate_child')
  ) where REC_RANK=1 )
  , lsmv_previous_preg_outcome_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  prepregocom_ari_rec_id,comp_rec_id  prepregocom_comp_rec_id,date_created  prepregocom_date_created,date_modified  prepregocom_date_modified,entity_updated  prepregocom_entity_updated,fk_apo_record_id  prepregocom_fk_apo_record_id,inq_rec_id  prepregocom_inq_rec_id,no_of_children  prepregocom_no_of_children,number_of_abortions  prepregocom_number_of_abortions,past_pregnancy_outcome  prepregocom_past_pregnancy_outcome,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8120' AND CD=CAST(past_pregnancy_outcome AS VARCHAR(100)) )prepregocom_past_pregnancy_outcome_de_ml , past_pregnancy_outcome_details  prepregocom_past_pregnancy_outcome_details,record_id  prepregocom_record_id,spr_id  prepregocom_spr_id,user_created  prepregocom_user_created,user_modified  prepregocom_user_modified,uuid  prepregocom_uuid,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_previous_preg_outcome
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_previous_preg_outcome')
  ) where REC_RANK=1 )
  , lsmv_pregnancy_outcomes_SUBSET AS 
(
select * from 
    (SELECT  
    abortion_date  pregocom_abortion_date,abortion_date_fmt  pregocom_abortion_date_fmt,apgar_score  pregocom_apgar_score,ari_rec_id  pregocom_ari_rec_id,childweight  pregocom_childweight,childweight_unit_code  pregocom_childweight_unit_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='48' AND CD=CAST(childweight_unit_code AS VARCHAR(100)) )pregocom_childweight_unit_code_de_ml , childweight_unit_lang  pregocom_childweight_unit_lang,comp_rec_id  pregocom_comp_rec_id,csection_reason  pregocom_csection_reason,date_created  pregocom_date_created,date_modified  pregocom_date_modified,delivery_date  pregocom_delivery_date,delivery_date_fmt  pregocom_delivery_date_fmt,delivery_method  pregocom_delivery_method,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='820' AND CD=CAST(delivery_method AS VARCHAR(100)) )pregocom_delivery_method_de_ml , entity_updated  pregocom_entity_updated,fetus_clinical_condition  pregocom_fetus_clinical_condition,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='821' AND CD=CAST(fetus_clinical_condition AS VARCHAR(100)) )pregocom_fetus_clinical_condition_de_ml , fk_appo_record_id  pregocom_fk_appo_record_id,fk_aprg_record_id  pregocom_fk_aprg_record_id,gestation_age_at_outcome  pregocom_gestation_age_at_outcome,gestation_age_at_outcome_unit  pregocom_gestation_age_at_outcome_unit,gestation_age_in_weeks  pregocom_gestation_age_in_weeks,head_circum_birth  pregocom_head_circum_birth,head_circum_birth_unit  pregocom_head_circum_birth_unit,inq_rec_id  pregocom_inq_rec_id,live_birth_complications  pregocom_live_birth_complications,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8105' AND CD=CAST(live_birth_complications AS VARCHAR(100)) )pregocom_live_birth_complications_de_ml , method_of_delivery  pregocom_method_of_delivery,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='951' AND CD=CAST(method_of_delivery AS VARCHAR(100)) )pregocom_method_of_delivery_de_ml , neon_birt_leng_unit  pregocom_neon_birt_leng_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='347' AND CD=CAST(neon_birt_leng_unit AS VARCHAR(100)) )pregocom_neon_birt_leng_unit_de_ml , neonatal_birth_length  pregocom_neonatal_birth_length,no_of_foetus  pregocom_no_of_foetus,other_details  pregocom_other_details,other_outcome_dtls  pregocom_other_outcome_dtls,outcome_comments  pregocom_outcome_comments,pregnancy_clinical_status  pregocom_pregnancy_clinical_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8120' AND CD=CAST(pregnancy_clinical_status AS VARCHAR(100)) )pregocom_pregnancy_clinical_status_de_ml , pregnancy_clinical_status_oth  pregocom_pregnancy_clinical_status_oth,pregnancy_end_date  pregocom_pregnancy_end_date,pregnancy_end_date_fmt  pregocom_pregnancy_end_date_fmt,pregnancy_outcome_date  pregocom_pregnancy_outcome_date,pregnancy_outcome_date_fmt  pregocom_pregnancy_outcome_date_fmt,record_id  pregocom_record_id,spr_id  pregocom_spr_id,user_created  pregocom_user_created,user_modified  pregocom_user_modified,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy_outcomes
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_outcomes')
  ) where REC_RANK=1 )
  , lsmv_neonate_SUBSET AS 
(
select * from 
    (SELECT  
    admission_duration  neon_admission_duration,admission_duration_sf  neon_admission_duration_sf,admission_duration_unit  neon_admission_duration_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9987' AND CD=CAST(admission_duration_unit AS VARCHAR(100)) )neon_admission_duration_unit_de_ml , apgar_score  neon_apgar_score,apgar_score_10_minute  neon_apgar_score_10_minute,apgar_score_5_minute  neon_apgar_score_5_minute,ari_rec_id  neon_ari_rec_id,birth_outcome  neon_birth_outcome,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9994' AND CD=CAST(birth_outcome AS VARCHAR(100)) )neon_birth_outcome_de_ml , child_age  neon_child_age,child_identity  neon_child_identity,child_sex  neon_child_sex,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10080' AND CD=CAST(child_sex AS VARCHAR(100)) )neon_child_sex_de_ml , child_unit  neon_child_unit,congenital_anomaly  neon_congenital_anomaly,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(congenital_anomaly AS VARCHAR(100)) )neon_congenital_anomaly_de_ml , congenital_anomaly_type  neon_congenital_anomaly_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9988' AND CD=CAST(congenital_anomaly_type AS VARCHAR(100)) )neon_congenital_anomaly_type_de_ml , congenital_anomaly_type_sf  neon_congenital_anomaly_type_sf,current_pregnancy_checkbox  neon_current_pregnancy_checkbox,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9970' AND CD=CAST(current_pregnancy_checkbox AS VARCHAR(100)) )neon_current_pregnancy_checkbox_de_ml , date_created  neon_date_created,date_modified  neon_date_modified,fk_apoc_rec_id  neon_fk_apoc_rec_id,gestational_age_birth  neon_gestational_age_birth,gestational_age_birth_unit  neon_gestational_age_birth_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='11' AND CD=CAST(gestational_age_birth_unit AS VARCHAR(100)) )neon_gestational_age_birth_unit_de_ml , head_circum_birth  neon_head_circum_birth,head_circum_birth_unit  neon_head_circum_birth_unit,neon_birth_len  neon_neon_birth_len,neon_birth_len_unit  neon_neon_birth_len_unit,neon_weight  neon_neon_weight,neon_weight_unit  neon_neon_weight_unit,nicu_admission  neon_nicu_admission,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(nicu_admission AS VARCHAR(100)) )neon_nicu_admission_de_ml , no_of_foetus  neon_no_of_foetus,other_neonate_details  neon_other_neonate_details,other_outcome_details  neon_other_outcome_details,record_id  neon_record_id,resuscitated  neon_resuscitated,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(resuscitated AS VARCHAR(100)) )neon_resuscitated_de_ml , spr_id  neon_spr_id,user_created  neon_user_created,user_modified  neon_user_modified,which_pregnancy  neon_which_pregnancy,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9986' AND CD=CAST(which_pregnancy AS VARCHAR(100)) )neon_which_pregnancy_de_ml , row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_neonate
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate')
  ) where REC_RANK=1 )
  , lsmv_pregnancy_SUBSET AS 
(
select * from 
    (SELECT  
    any_relevant_info_from_partner  preg_any_relevant_info_from_partner,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(any_relevant_info_from_partner AS VARCHAR(100)) )preg_any_relevant_info_from_partner_de_ml , ari_rec_id  preg_ari_rec_id,birth_defects_history  preg_birth_defects_history,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(birth_defects_history AS VARCHAR(100)) )preg_birth_defects_history_de_ml , comments  preg_comments,comp_rec_id  preg_comp_rec_id,consent_to_contact  preg_consent_to_contact,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7073' AND CD=CAST(consent_to_contact AS VARCHAR(100)) )preg_consent_to_contact_de_ml , consent_to_preg_register  preg_consent_to_preg_register,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(consent_to_preg_register AS VARCHAR(100)) )preg_consent_to_preg_register_de_ml , contraceptive_failure  preg_contraceptive_failure,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(contraceptive_failure AS VARCHAR(100)) )preg_contraceptive_failure_de_ml , contraceptives_used  preg_contraceptives_used,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(contraceptives_used AS VARCHAR(100)) )preg_contraceptives_used_de_ml , date_created  preg_date_created,date_modified  preg_date_modified,date_of_first_consultation  preg_date_of_first_consultation,date_of_first_consultation_fmt  preg_date_of_first_consultation_fmt,delivery_method  preg_delivery_method,do_not_use_ang_comment  preg_do_not_use_ang_comment,entity_updated  preg_entity_updated,expected_due_date  preg_expected_due_date,expected_due_date_fmt  preg_expected_due_date_fmt,exposure_status  preg_exposure_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8119' AND CD=CAST(exposure_status AS VARCHAR(100)) )preg_exposure_status_de_ml , ext_clob_fld  preg_ext_clob_fld,gestatationperiod  preg_gestatationperiod,gestatationperiodunit  preg_gestatationperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='11' AND CD=CAST(gestatationperiodunit AS VARCHAR(100)) )preg_gestatationperiodunit_de_ml , gestation_week_type  preg_gestation_week_type,gravidity  preg_gravidity,inq_rec_id  preg_inq_rec_id,mother_before_pregnant  preg_mother_before_pregnant,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(mother_before_pregnant AS VARCHAR(100)) )preg_mother_before_pregnant_de_ml , mother_exp_any_medical_problem  preg_mother_exp_any_medical_problem,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(mother_exp_any_medical_problem AS VARCHAR(100)) )preg_mother_exp_any_medical_problem_de_ml , narrative_generated_comment  preg_narrative_generated_comment,no_of_abnormal_outcomes  preg_no_of_abnormal_outcomes,no_of_children  preg_no_of_children,no_of_normal_outcomes  preg_no_of_normal_outcomes,no_of_unknown_outcomes  preg_no_of_unknown_outcomes,number_of_abortions  preg_number_of_abortions,para  preg_para,parity  preg_parity,past_pregnancy_outcome  preg_past_pregnancy_outcome,past_pregnancy_outcome_details  preg_past_pregnancy_outcome_details,planned_pregnancy  preg_planned_pregnancy,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(planned_pregnancy AS VARCHAR(100)) )preg_planned_pregnancy_de_ml , pre_pregnancy_weight  preg_pre_pregnancy_weight,pre_pregnancy_weight_unit  preg_pre_pregnancy_weight_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9991' AND CD=CAST(pre_pregnancy_weight_unit AS VARCHAR(100)) )preg_pre_pregnancy_weight_unit_de_ml , pregnancy_complication_desc  preg_pregnancy_complication_desc,pregnancy_confirm_date  preg_pregnancy_confirm_date,pregnancy_confirm_date_fmt  preg_pregnancy_confirm_date_fmt,pregnancy_confirm_mode  preg_pregnancy_confirm_mode,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8108' AND CD=CAST(pregnancy_confirm_mode AS VARCHAR(100)) )preg_pregnancy_confirm_mode_de_ml , record_id  preg_record_id,spr_id  preg_spr_id,trimester_of_exposure  preg_trimester_of_exposure,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9969' AND CD=CAST(trimester_of_exposure AS VARCHAR(100)) )preg_trimester_of_exposure_de_ml , trimester_of_exposure_sf  preg_trimester_of_exposure_sf,types_of_contraceptive_used  preg_types_of_contraceptive_used,user_created  preg_user_created,user_modified  preg_user_modified,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_pregnancy
 WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_pregnancy_SUBSET.preg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_neonate_child_SUBSET.neonchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_pregnancy_outcomes_SUBSET.pregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_pregnancy_child_SUBSET.pregchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_neonate_SUBSET.neon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_previous_preg_outcome_SUBSET.prepregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_pregnancy_SUBSET.preg_USER_MODIFIED USER_MODIFIED,lsmv_pregnancy_SUBSET.preg_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_pregnancy_SUBSET.preg_USER_CREATED CREATED_BY,lsmv_pregnancy_SUBSET.preg_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_previous_preg_outcome_SUBSET.prepregocom_uuid  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_user_modified  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_user_created  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_spr_id  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_record_id  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_past_pregnancy_outcome_details  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_past_pregnancy_outcome_de_ml  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_past_pregnancy_outcome  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_number_of_abortions  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_no_of_children  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_inq_rec_id  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_fk_apo_record_id  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_entity_updated  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_date_modified  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_date_created  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_comp_rec_id  ,lsmv_previous_preg_outcome_SUBSET.prepregocom_ari_rec_id  ,lsmv_pregnancy_SUBSET.preg_user_modified  ,lsmv_pregnancy_SUBSET.preg_user_created  ,lsmv_pregnancy_SUBSET.preg_types_of_contraceptive_used  ,lsmv_pregnancy_SUBSET.preg_trimester_of_exposure_sf  ,lsmv_pregnancy_SUBSET.preg_trimester_of_exposure_de_ml  ,lsmv_pregnancy_SUBSET.preg_trimester_of_exposure  ,lsmv_pregnancy_SUBSET.preg_spr_id  ,lsmv_pregnancy_SUBSET.preg_record_id  ,lsmv_pregnancy_SUBSET.preg_pregnancy_confirm_mode_de_ml  ,lsmv_pregnancy_SUBSET.preg_pregnancy_confirm_mode  ,lsmv_pregnancy_SUBSET.preg_pregnancy_confirm_date_fmt  ,lsmv_pregnancy_SUBSET.preg_pregnancy_confirm_date  ,lsmv_pregnancy_SUBSET.preg_pregnancy_complication_desc  ,lsmv_pregnancy_SUBSET.preg_pre_pregnancy_weight_unit_de_ml  ,lsmv_pregnancy_SUBSET.preg_pre_pregnancy_weight_unit  ,lsmv_pregnancy_SUBSET.preg_pre_pregnancy_weight  ,lsmv_pregnancy_SUBSET.preg_planned_pregnancy_de_ml  ,lsmv_pregnancy_SUBSET.preg_planned_pregnancy  ,lsmv_pregnancy_SUBSET.preg_past_pregnancy_outcome_details  ,lsmv_pregnancy_SUBSET.preg_past_pregnancy_outcome  ,lsmv_pregnancy_SUBSET.preg_parity  ,lsmv_pregnancy_SUBSET.preg_para  ,lsmv_pregnancy_SUBSET.preg_number_of_abortions  ,lsmv_pregnancy_SUBSET.preg_no_of_unknown_outcomes  ,lsmv_pregnancy_SUBSET.preg_no_of_normal_outcomes  ,lsmv_pregnancy_SUBSET.preg_no_of_children  ,lsmv_pregnancy_SUBSET.preg_no_of_abnormal_outcomes  ,lsmv_pregnancy_SUBSET.preg_narrative_generated_comment  ,lsmv_pregnancy_SUBSET.preg_mother_exp_any_medical_problem_de_ml  ,lsmv_pregnancy_SUBSET.preg_mother_exp_any_medical_problem  ,lsmv_pregnancy_SUBSET.preg_mother_before_pregnant_de_ml  ,lsmv_pregnancy_SUBSET.preg_mother_before_pregnant  ,lsmv_pregnancy_SUBSET.preg_inq_rec_id  ,lsmv_pregnancy_SUBSET.preg_gravidity  ,lsmv_pregnancy_SUBSET.preg_gestation_week_type  ,lsmv_pregnancy_SUBSET.preg_gestatationperiodunit_de_ml  ,lsmv_pregnancy_SUBSET.preg_gestatationperiodunit  ,lsmv_pregnancy_SUBSET.preg_gestatationperiod  ,lsmv_pregnancy_SUBSET.preg_ext_clob_fld  ,lsmv_pregnancy_SUBSET.preg_exposure_status_de_ml  ,lsmv_pregnancy_SUBSET.preg_exposure_status  ,lsmv_pregnancy_SUBSET.preg_expected_due_date_fmt  ,lsmv_pregnancy_SUBSET.preg_expected_due_date  ,lsmv_pregnancy_SUBSET.preg_entity_updated  ,lsmv_pregnancy_SUBSET.preg_do_not_use_ang_comment  ,lsmv_pregnancy_SUBSET.preg_delivery_method  ,lsmv_pregnancy_SUBSET.preg_date_of_first_consultation_fmt  ,lsmv_pregnancy_SUBSET.preg_date_of_first_consultation  ,lsmv_pregnancy_SUBSET.preg_date_modified  ,lsmv_pregnancy_SUBSET.preg_date_created  ,lsmv_pregnancy_SUBSET.preg_contraceptives_used_de_ml  ,lsmv_pregnancy_SUBSET.preg_contraceptives_used  ,lsmv_pregnancy_SUBSET.preg_contraceptive_failure_de_ml  ,lsmv_pregnancy_SUBSET.preg_contraceptive_failure  ,lsmv_pregnancy_SUBSET.preg_consent_to_preg_register_de_ml  ,lsmv_pregnancy_SUBSET.preg_consent_to_preg_register  ,lsmv_pregnancy_SUBSET.preg_consent_to_contact_de_ml  ,lsmv_pregnancy_SUBSET.preg_consent_to_contact  ,lsmv_pregnancy_SUBSET.preg_comp_rec_id  ,lsmv_pregnancy_SUBSET.preg_comments  ,lsmv_pregnancy_SUBSET.preg_birth_defects_history_de_ml  ,lsmv_pregnancy_SUBSET.preg_birth_defects_history  ,lsmv_pregnancy_SUBSET.preg_ari_rec_id  ,lsmv_pregnancy_SUBSET.preg_any_relevant_info_from_partner_de_ml  ,lsmv_pregnancy_SUBSET.preg_any_relevant_info_from_partner  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_user_modified  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_user_created  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_spr_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_record_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_outcome_date_fmt  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_outcome_date  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_end_date_fmt  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_end_date  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_clinical_status_oth  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_clinical_status_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_pregnancy_clinical_status  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_outcome_comments  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_other_outcome_dtls  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_other_details  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_no_of_foetus  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_neonatal_birth_length  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_neon_birt_leng_unit_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_neon_birt_leng_unit  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_method_of_delivery_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_method_of_delivery  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_live_birth_complications_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_live_birth_complications  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_inq_rec_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_head_circum_birth_unit  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_head_circum_birth  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_gestation_age_in_weeks  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_gestation_age_at_outcome_unit  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_gestation_age_at_outcome  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_fk_aprg_record_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_fk_appo_record_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_fetus_clinical_condition_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_fetus_clinical_condition  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_entity_updated  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_delivery_method_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_delivery_method  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_delivery_date_fmt  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_delivery_date  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_date_modified  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_date_created  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_csection_reason  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_comp_rec_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_childweight_unit_lang  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_childweight_unit_code_de_ml  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_childweight_unit_code  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_childweight  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_ari_rec_id  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_apgar_score  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_abortion_date_fmt  ,lsmv_pregnancy_outcomes_SUBSET.pregocom_abortion_date  ,lsmv_pregnancy_child_SUBSET.pregchild_user_modified  ,lsmv_pregnancy_child_SUBSET.pregchild_user_created  ,lsmv_pregnancy_child_SUBSET.pregchild_spr_id  ,lsmv_pregnancy_child_SUBSET.pregchild_record_id  ,lsmv_pregnancy_child_SUBSET.pregchild_fk_aprg_record_id  ,lsmv_pregnancy_child_SUBSET.pregchild_date_modified  ,lsmv_pregnancy_child_SUBSET.pregchild_date_created  ,lsmv_pregnancy_child_SUBSET.pregchild_child_unit  ,lsmv_pregnancy_child_SUBSET.pregchild_child_sex  ,lsmv_pregnancy_child_SUBSET.pregchild_child_age  ,lsmv_pregnancy_child_SUBSET.pregchild_ari_rec_id  ,lsmv_neonate_SUBSET.neon_which_pregnancy_de_ml  ,lsmv_neonate_SUBSET.neon_which_pregnancy  ,lsmv_neonate_SUBSET.neon_user_modified  ,lsmv_neonate_SUBSET.neon_user_created  ,lsmv_neonate_SUBSET.neon_spr_id  ,lsmv_neonate_SUBSET.neon_resuscitated_de_ml  ,lsmv_neonate_SUBSET.neon_resuscitated  ,lsmv_neonate_SUBSET.neon_record_id  ,lsmv_neonate_SUBSET.neon_other_outcome_details  ,lsmv_neonate_SUBSET.neon_other_neonate_details  ,lsmv_neonate_SUBSET.neon_no_of_foetus  ,lsmv_neonate_SUBSET.neon_nicu_admission_de_ml  ,lsmv_neonate_SUBSET.neon_nicu_admission  ,lsmv_neonate_SUBSET.neon_neon_weight_unit  ,lsmv_neonate_SUBSET.neon_neon_weight  ,lsmv_neonate_SUBSET.neon_neon_birth_len_unit  ,lsmv_neonate_SUBSET.neon_neon_birth_len  ,lsmv_neonate_SUBSET.neon_head_circum_birth_unit  ,lsmv_neonate_SUBSET.neon_head_circum_birth  ,lsmv_neonate_SUBSET.neon_gestational_age_birth_unit_de_ml  ,lsmv_neonate_SUBSET.neon_gestational_age_birth_unit  ,lsmv_neonate_SUBSET.neon_gestational_age_birth  ,lsmv_neonate_SUBSET.neon_fk_apoc_rec_id  ,lsmv_neonate_SUBSET.neon_date_modified  ,lsmv_neonate_SUBSET.neon_date_created  ,lsmv_neonate_SUBSET.neon_current_pregnancy_checkbox_de_ml  ,lsmv_neonate_SUBSET.neon_current_pregnancy_checkbox  ,lsmv_neonate_SUBSET.neon_congenital_anomaly_type_sf  ,lsmv_neonate_SUBSET.neon_congenital_anomaly_type_de_ml  ,lsmv_neonate_SUBSET.neon_congenital_anomaly_type  ,lsmv_neonate_SUBSET.neon_congenital_anomaly_de_ml  ,lsmv_neonate_SUBSET.neon_congenital_anomaly  ,lsmv_neonate_SUBSET.neon_child_unit  ,lsmv_neonate_SUBSET.neon_child_sex_de_ml  ,lsmv_neonate_SUBSET.neon_child_sex  ,lsmv_neonate_SUBSET.neon_child_identity  ,lsmv_neonate_SUBSET.neon_child_age  ,lsmv_neonate_SUBSET.neon_birth_outcome_de_ml  ,lsmv_neonate_SUBSET.neon_birth_outcome  ,lsmv_neonate_SUBSET.neon_ari_rec_id  ,lsmv_neonate_SUBSET.neon_apgar_score_5_minute  ,lsmv_neonate_SUBSET.neon_apgar_score_10_minute  ,lsmv_neonate_SUBSET.neon_apgar_score  ,lsmv_neonate_SUBSET.neon_admission_duration_unit_de_ml  ,lsmv_neonate_SUBSET.neon_admission_duration_unit  ,lsmv_neonate_SUBSET.neon_admission_duration_sf  ,lsmv_neonate_SUBSET.neon_admission_duration  ,lsmv_neonate_child_SUBSET.neonchild_user_modified  ,lsmv_neonate_child_SUBSET.neonchild_user_created  ,lsmv_neonate_child_SUBSET.neonchild_spr_id  ,lsmv_neonate_child_SUBSET.neonchild_record_id  ,lsmv_neonate_child_SUBSET.neonchild_neon_weight_unit_de_ml  ,lsmv_neonate_child_SUBSET.neonchild_neon_weight_unit  ,lsmv_neonate_child_SUBSET.neonchild_neon_weight  ,lsmv_neonate_child_SUBSET.neonchild_neon_birth_len_unit_de_ml  ,lsmv_neonate_child_SUBSET.neonchild_neon_birth_len_unit  ,lsmv_neonate_child_SUBSET.neonchild_neon_birth_len  ,lsmv_neonate_child_SUBSET.neonchild_head_circum_birth_unit_de_ml  ,lsmv_neonate_child_SUBSET.neonchild_head_circum_birth_unit  ,lsmv_neonate_child_SUBSET.neonchild_head_circum_birth  ,lsmv_neonate_child_SUBSET.neonchild_fk_an_record_id  ,lsmv_neonate_child_SUBSET.neonchild_date_modified  ,lsmv_neonate_child_SUBSET.neonchild_date_created  ,lsmv_neonate_child_SUBSET.neonchild_child_unit_de_ml  ,lsmv_neonate_child_SUBSET.neonchild_child_unit  ,lsmv_neonate_child_SUBSET.neonchild_child_age  ,lsmv_neonate_child_SUBSET.neonchild_ari_rec_id ,CONCAT( NVL(lsmv_pregnancy_SUBSET.preg_RECORD_ID,-1),'||',NVL(lsmv_neonate_child_SUBSET.neonchild_RECORD_ID,-1),'||',NVL(lsmv_pregnancy_outcomes_SUBSET.pregocom_RECORD_ID,-1),'||',NVL(lsmv_pregnancy_child_SUBSET.pregchild_RECORD_ID,-1),'||',NVL(lsmv_neonate_SUBSET.neon_RECORD_ID,-1),'||',NVL(lsmv_previous_preg_outcome_SUBSET.prepregocom_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_pregnancy_SUBSET  LEFT JOIN lsmv_pregnancy_child_SUBSET ON lsmv_pregnancy_SUBSET.preg_ARI_REC_ID=lsmv_pregnancy_child_SUBSET.pregchild_ARI_REC_ID
                         LEFT JOIN lsmv_pregnancy_outcomes_SUBSET ON lsmv_pregnancy_SUBSET.preg_ARI_REC_ID=lsmv_pregnancy_outcomes_SUBSET.pregocom_ARI_REC_ID
                         LEFT JOIN lsmv_neonate_SUBSET ON lsmv_pregnancy_outcomes_SUBSET.pregocom_ARI_REC_ID=lsmv_neonate_SUBSET.neon_ARI_REC_ID
                         LEFT JOIN lsmv_previous_preg_outcome_SUBSET ON lsmv_pregnancy_outcomes_SUBSET.pregocom_ARI_REC_ID=lsmv_previous_preg_outcome_SUBSET.prepregocom_ARI_REC_ID
                         LEFT JOIN lsmv_neonate_child_SUBSET ON lsmv_neonate_SUBSET.neon_ARI_REC_ID=lsmv_neonate_child_SUBSET.neonchild_ARI_REC_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_pregnancy_SUBSET.preg_ARI_REC_ID,lsmv_neonate_child_SUBSET.neonchild_ARI_REC_ID,lsmv_pregnancy_outcomes_SUBSET.pregocom_ARI_REC_ID,lsmv_pregnancy_child_SUBSET.pregchild_ARI_REC_ID,lsmv_neonate_SUBSET.neon_ARI_REC_ID,lsmv_previous_preg_outcome_SUBSET.prepregocom_ARI_REC_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP)
where target_table_name='LS_DB_PREGNANCY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PREGNANCY'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY   
SET LS_DB_PREGNANCY.prepregocom_uuid = LS_DB_PREGNANCY_TMP.prepregocom_uuid,LS_DB_PREGNANCY.prepregocom_user_modified = LS_DB_PREGNANCY_TMP.prepregocom_user_modified,LS_DB_PREGNANCY.prepregocom_user_created = LS_DB_PREGNANCY_TMP.prepregocom_user_created,LS_DB_PREGNANCY.prepregocom_spr_id = LS_DB_PREGNANCY_TMP.prepregocom_spr_id,LS_DB_PREGNANCY.prepregocom_record_id = LS_DB_PREGNANCY_TMP.prepregocom_record_id,LS_DB_PREGNANCY.prepregocom_past_pregnancy_outcome_details = LS_DB_PREGNANCY_TMP.prepregocom_past_pregnancy_outcome_details,LS_DB_PREGNANCY.prepregocom_past_pregnancy_outcome_de_ml = LS_DB_PREGNANCY_TMP.prepregocom_past_pregnancy_outcome_de_ml,LS_DB_PREGNANCY.prepregocom_past_pregnancy_outcome = LS_DB_PREGNANCY_TMP.prepregocom_past_pregnancy_outcome,LS_DB_PREGNANCY.prepregocom_number_of_abortions = LS_DB_PREGNANCY_TMP.prepregocom_number_of_abortions,LS_DB_PREGNANCY.prepregocom_no_of_children = LS_DB_PREGNANCY_TMP.prepregocom_no_of_children,LS_DB_PREGNANCY.prepregocom_inq_rec_id = LS_DB_PREGNANCY_TMP.prepregocom_inq_rec_id,LS_DB_PREGNANCY.prepregocom_fk_apo_record_id = LS_DB_PREGNANCY_TMP.prepregocom_fk_apo_record_id,LS_DB_PREGNANCY.prepregocom_entity_updated = LS_DB_PREGNANCY_TMP.prepregocom_entity_updated,LS_DB_PREGNANCY.prepregocom_date_modified = LS_DB_PREGNANCY_TMP.prepregocom_date_modified,LS_DB_PREGNANCY.prepregocom_date_created = LS_DB_PREGNANCY_TMP.prepregocom_date_created,LS_DB_PREGNANCY.prepregocom_comp_rec_id = LS_DB_PREGNANCY_TMP.prepregocom_comp_rec_id,LS_DB_PREGNANCY.prepregocom_ari_rec_id = LS_DB_PREGNANCY_TMP.prepregocom_ari_rec_id,LS_DB_PREGNANCY.preg_user_modified = LS_DB_PREGNANCY_TMP.preg_user_modified,LS_DB_PREGNANCY.preg_user_created = LS_DB_PREGNANCY_TMP.preg_user_created,LS_DB_PREGNANCY.preg_types_of_contraceptive_used = LS_DB_PREGNANCY_TMP.preg_types_of_contraceptive_used,LS_DB_PREGNANCY.preg_trimester_of_exposure_sf = LS_DB_PREGNANCY_TMP.preg_trimester_of_exposure_sf,LS_DB_PREGNANCY.preg_trimester_of_exposure_de_ml = LS_DB_PREGNANCY_TMP.preg_trimester_of_exposure_de_ml,LS_DB_PREGNANCY.preg_trimester_of_exposure = LS_DB_PREGNANCY_TMP.preg_trimester_of_exposure,LS_DB_PREGNANCY.preg_spr_id = LS_DB_PREGNANCY_TMP.preg_spr_id,LS_DB_PREGNANCY.preg_record_id = LS_DB_PREGNANCY_TMP.preg_record_id,LS_DB_PREGNANCY.preg_pregnancy_confirm_mode_de_ml = LS_DB_PREGNANCY_TMP.preg_pregnancy_confirm_mode_de_ml,LS_DB_PREGNANCY.preg_pregnancy_confirm_mode = LS_DB_PREGNANCY_TMP.preg_pregnancy_confirm_mode,LS_DB_PREGNANCY.preg_pregnancy_confirm_date_fmt = LS_DB_PREGNANCY_TMP.preg_pregnancy_confirm_date_fmt,LS_DB_PREGNANCY.preg_pregnancy_confirm_date = LS_DB_PREGNANCY_TMP.preg_pregnancy_confirm_date,LS_DB_PREGNANCY.preg_pregnancy_complication_desc = LS_DB_PREGNANCY_TMP.preg_pregnancy_complication_desc,LS_DB_PREGNANCY.preg_pre_pregnancy_weight_unit_de_ml = LS_DB_PREGNANCY_TMP.preg_pre_pregnancy_weight_unit_de_ml,LS_DB_PREGNANCY.preg_pre_pregnancy_weight_unit = LS_DB_PREGNANCY_TMP.preg_pre_pregnancy_weight_unit,LS_DB_PREGNANCY.preg_pre_pregnancy_weight = LS_DB_PREGNANCY_TMP.preg_pre_pregnancy_weight,LS_DB_PREGNANCY.preg_planned_pregnancy_de_ml = LS_DB_PREGNANCY_TMP.preg_planned_pregnancy_de_ml,LS_DB_PREGNANCY.preg_planned_pregnancy = LS_DB_PREGNANCY_TMP.preg_planned_pregnancy,LS_DB_PREGNANCY.preg_past_pregnancy_outcome_details = LS_DB_PREGNANCY_TMP.preg_past_pregnancy_outcome_details,LS_DB_PREGNANCY.preg_past_pregnancy_outcome = LS_DB_PREGNANCY_TMP.preg_past_pregnancy_outcome,LS_DB_PREGNANCY.preg_parity = LS_DB_PREGNANCY_TMP.preg_parity,LS_DB_PREGNANCY.preg_para = LS_DB_PREGNANCY_TMP.preg_para,LS_DB_PREGNANCY.preg_number_of_abortions = LS_DB_PREGNANCY_TMP.preg_number_of_abortions,LS_DB_PREGNANCY.preg_no_of_unknown_outcomes = LS_DB_PREGNANCY_TMP.preg_no_of_unknown_outcomes,LS_DB_PREGNANCY.preg_no_of_normal_outcomes = LS_DB_PREGNANCY_TMP.preg_no_of_normal_outcomes,LS_DB_PREGNANCY.preg_no_of_children = LS_DB_PREGNANCY_TMP.preg_no_of_children,LS_DB_PREGNANCY.preg_no_of_abnormal_outcomes = LS_DB_PREGNANCY_TMP.preg_no_of_abnormal_outcomes,LS_DB_PREGNANCY.preg_narrative_generated_comment = LS_DB_PREGNANCY_TMP.preg_narrative_generated_comment,LS_DB_PREGNANCY.preg_mother_exp_any_medical_problem_de_ml = LS_DB_PREGNANCY_TMP.preg_mother_exp_any_medical_problem_de_ml,LS_DB_PREGNANCY.preg_mother_exp_any_medical_problem = LS_DB_PREGNANCY_TMP.preg_mother_exp_any_medical_problem,LS_DB_PREGNANCY.preg_mother_before_pregnant_de_ml = LS_DB_PREGNANCY_TMP.preg_mother_before_pregnant_de_ml,LS_DB_PREGNANCY.preg_mother_before_pregnant = LS_DB_PREGNANCY_TMP.preg_mother_before_pregnant,LS_DB_PREGNANCY.preg_inq_rec_id = LS_DB_PREGNANCY_TMP.preg_inq_rec_id,LS_DB_PREGNANCY.preg_gravidity = LS_DB_PREGNANCY_TMP.preg_gravidity,LS_DB_PREGNANCY.preg_gestation_week_type = LS_DB_PREGNANCY_TMP.preg_gestation_week_type,LS_DB_PREGNANCY.preg_gestatationperiodunit_de_ml = LS_DB_PREGNANCY_TMP.preg_gestatationperiodunit_de_ml,LS_DB_PREGNANCY.preg_gestatationperiodunit = LS_DB_PREGNANCY_TMP.preg_gestatationperiodunit,LS_DB_PREGNANCY.preg_gestatationperiod = LS_DB_PREGNANCY_TMP.preg_gestatationperiod,LS_DB_PREGNANCY.preg_ext_clob_fld = LS_DB_PREGNANCY_TMP.preg_ext_clob_fld,LS_DB_PREGNANCY.preg_exposure_status_de_ml = LS_DB_PREGNANCY_TMP.preg_exposure_status_de_ml,LS_DB_PREGNANCY.preg_exposure_status = LS_DB_PREGNANCY_TMP.preg_exposure_status,LS_DB_PREGNANCY.preg_expected_due_date_fmt = LS_DB_PREGNANCY_TMP.preg_expected_due_date_fmt,LS_DB_PREGNANCY.preg_expected_due_date = LS_DB_PREGNANCY_TMP.preg_expected_due_date,LS_DB_PREGNANCY.preg_entity_updated = LS_DB_PREGNANCY_TMP.preg_entity_updated,LS_DB_PREGNANCY.preg_do_not_use_ang_comment = LS_DB_PREGNANCY_TMP.preg_do_not_use_ang_comment,LS_DB_PREGNANCY.preg_delivery_method = LS_DB_PREGNANCY_TMP.preg_delivery_method,LS_DB_PREGNANCY.preg_date_of_first_consultation_fmt = LS_DB_PREGNANCY_TMP.preg_date_of_first_consultation_fmt,LS_DB_PREGNANCY.preg_date_of_first_consultation = LS_DB_PREGNANCY_TMP.preg_date_of_first_consultation,LS_DB_PREGNANCY.preg_date_modified = LS_DB_PREGNANCY_TMP.preg_date_modified,LS_DB_PREGNANCY.preg_date_created = LS_DB_PREGNANCY_TMP.preg_date_created,LS_DB_PREGNANCY.preg_contraceptives_used_de_ml = LS_DB_PREGNANCY_TMP.preg_contraceptives_used_de_ml,LS_DB_PREGNANCY.preg_contraceptives_used = LS_DB_PREGNANCY_TMP.preg_contraceptives_used,LS_DB_PREGNANCY.preg_contraceptive_failure_de_ml = LS_DB_PREGNANCY_TMP.preg_contraceptive_failure_de_ml,LS_DB_PREGNANCY.preg_contraceptive_failure = LS_DB_PREGNANCY_TMP.preg_contraceptive_failure,LS_DB_PREGNANCY.preg_consent_to_preg_register_de_ml = LS_DB_PREGNANCY_TMP.preg_consent_to_preg_register_de_ml,LS_DB_PREGNANCY.preg_consent_to_preg_register = LS_DB_PREGNANCY_TMP.preg_consent_to_preg_register,LS_DB_PREGNANCY.preg_consent_to_contact_de_ml = LS_DB_PREGNANCY_TMP.preg_consent_to_contact_de_ml,LS_DB_PREGNANCY.preg_consent_to_contact = LS_DB_PREGNANCY_TMP.preg_consent_to_contact,LS_DB_PREGNANCY.preg_comp_rec_id = LS_DB_PREGNANCY_TMP.preg_comp_rec_id,LS_DB_PREGNANCY.preg_comments = LS_DB_PREGNANCY_TMP.preg_comments,LS_DB_PREGNANCY.preg_birth_defects_history_de_ml = LS_DB_PREGNANCY_TMP.preg_birth_defects_history_de_ml,LS_DB_PREGNANCY.preg_birth_defects_history = LS_DB_PREGNANCY_TMP.preg_birth_defects_history,LS_DB_PREGNANCY.preg_ari_rec_id = LS_DB_PREGNANCY_TMP.preg_ari_rec_id,LS_DB_PREGNANCY.preg_any_relevant_info_from_partner_de_ml = LS_DB_PREGNANCY_TMP.preg_any_relevant_info_from_partner_de_ml,LS_DB_PREGNANCY.preg_any_relevant_info_from_partner = LS_DB_PREGNANCY_TMP.preg_any_relevant_info_from_partner,LS_DB_PREGNANCY.pregocom_user_modified = LS_DB_PREGNANCY_TMP.pregocom_user_modified,LS_DB_PREGNANCY.pregocom_user_created = LS_DB_PREGNANCY_TMP.pregocom_user_created,LS_DB_PREGNANCY.pregocom_spr_id = LS_DB_PREGNANCY_TMP.pregocom_spr_id,LS_DB_PREGNANCY.pregocom_record_id = LS_DB_PREGNANCY_TMP.pregocom_record_id,LS_DB_PREGNANCY.pregocom_pregnancy_outcome_date_fmt = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_outcome_date_fmt,LS_DB_PREGNANCY.pregocom_pregnancy_outcome_date = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_outcome_date,LS_DB_PREGNANCY.pregocom_pregnancy_end_date_fmt = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_end_date_fmt,LS_DB_PREGNANCY.pregocom_pregnancy_end_date = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_end_date,LS_DB_PREGNANCY.pregocom_pregnancy_clinical_status_oth = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_clinical_status_oth,LS_DB_PREGNANCY.pregocom_pregnancy_clinical_status_de_ml = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_clinical_status_de_ml,LS_DB_PREGNANCY.pregocom_pregnancy_clinical_status = LS_DB_PREGNANCY_TMP.pregocom_pregnancy_clinical_status,LS_DB_PREGNANCY.pregocom_outcome_comments = LS_DB_PREGNANCY_TMP.pregocom_outcome_comments,LS_DB_PREGNANCY.pregocom_other_outcome_dtls = LS_DB_PREGNANCY_TMP.pregocom_other_outcome_dtls,LS_DB_PREGNANCY.pregocom_other_details = LS_DB_PREGNANCY_TMP.pregocom_other_details,LS_DB_PREGNANCY.pregocom_no_of_foetus = LS_DB_PREGNANCY_TMP.pregocom_no_of_foetus,LS_DB_PREGNANCY.pregocom_neonatal_birth_length = LS_DB_PREGNANCY_TMP.pregocom_neonatal_birth_length,LS_DB_PREGNANCY.pregocom_neon_birt_leng_unit_de_ml = LS_DB_PREGNANCY_TMP.pregocom_neon_birt_leng_unit_de_ml,LS_DB_PREGNANCY.pregocom_neon_birt_leng_unit = LS_DB_PREGNANCY_TMP.pregocom_neon_birt_leng_unit,LS_DB_PREGNANCY.pregocom_method_of_delivery_de_ml = LS_DB_PREGNANCY_TMP.pregocom_method_of_delivery_de_ml,LS_DB_PREGNANCY.pregocom_method_of_delivery = LS_DB_PREGNANCY_TMP.pregocom_method_of_delivery,LS_DB_PREGNANCY.pregocom_live_birth_complications_de_ml = LS_DB_PREGNANCY_TMP.pregocom_live_birth_complications_de_ml,LS_DB_PREGNANCY.pregocom_live_birth_complications = LS_DB_PREGNANCY_TMP.pregocom_live_birth_complications,LS_DB_PREGNANCY.pregocom_inq_rec_id = LS_DB_PREGNANCY_TMP.pregocom_inq_rec_id,LS_DB_PREGNANCY.pregocom_head_circum_birth_unit = LS_DB_PREGNANCY_TMP.pregocom_head_circum_birth_unit,LS_DB_PREGNANCY.pregocom_head_circum_birth = LS_DB_PREGNANCY_TMP.pregocom_head_circum_birth,LS_DB_PREGNANCY.pregocom_gestation_age_in_weeks = LS_DB_PREGNANCY_TMP.pregocom_gestation_age_in_weeks,LS_DB_PREGNANCY.pregocom_gestation_age_at_outcome_unit = LS_DB_PREGNANCY_TMP.pregocom_gestation_age_at_outcome_unit,LS_DB_PREGNANCY.pregocom_gestation_age_at_outcome = LS_DB_PREGNANCY_TMP.pregocom_gestation_age_at_outcome,LS_DB_PREGNANCY.pregocom_fk_aprg_record_id = LS_DB_PREGNANCY_TMP.pregocom_fk_aprg_record_id,LS_DB_PREGNANCY.pregocom_fk_appo_record_id = LS_DB_PREGNANCY_TMP.pregocom_fk_appo_record_id,LS_DB_PREGNANCY.pregocom_fetus_clinical_condition_de_ml = LS_DB_PREGNANCY_TMP.pregocom_fetus_clinical_condition_de_ml,LS_DB_PREGNANCY.pregocom_fetus_clinical_condition = LS_DB_PREGNANCY_TMP.pregocom_fetus_clinical_condition,LS_DB_PREGNANCY.pregocom_entity_updated = LS_DB_PREGNANCY_TMP.pregocom_entity_updated,LS_DB_PREGNANCY.pregocom_delivery_method_de_ml = LS_DB_PREGNANCY_TMP.pregocom_delivery_method_de_ml,LS_DB_PREGNANCY.pregocom_delivery_method = LS_DB_PREGNANCY_TMP.pregocom_delivery_method,LS_DB_PREGNANCY.pregocom_delivery_date_fmt = LS_DB_PREGNANCY_TMP.pregocom_delivery_date_fmt,LS_DB_PREGNANCY.pregocom_delivery_date = LS_DB_PREGNANCY_TMP.pregocom_delivery_date,LS_DB_PREGNANCY.pregocom_date_modified = LS_DB_PREGNANCY_TMP.pregocom_date_modified,LS_DB_PREGNANCY.pregocom_date_created = LS_DB_PREGNANCY_TMP.pregocom_date_created,LS_DB_PREGNANCY.pregocom_csection_reason = LS_DB_PREGNANCY_TMP.pregocom_csection_reason,LS_DB_PREGNANCY.pregocom_comp_rec_id = LS_DB_PREGNANCY_TMP.pregocom_comp_rec_id,LS_DB_PREGNANCY.pregocom_childweight_unit_lang = LS_DB_PREGNANCY_TMP.pregocom_childweight_unit_lang,LS_DB_PREGNANCY.pregocom_childweight_unit_code_de_ml = LS_DB_PREGNANCY_TMP.pregocom_childweight_unit_code_de_ml,LS_DB_PREGNANCY.pregocom_childweight_unit_code = LS_DB_PREGNANCY_TMP.pregocom_childweight_unit_code,LS_DB_PREGNANCY.pregocom_childweight = LS_DB_PREGNANCY_TMP.pregocom_childweight,LS_DB_PREGNANCY.pregocom_ari_rec_id = LS_DB_PREGNANCY_TMP.pregocom_ari_rec_id,LS_DB_PREGNANCY.pregocom_apgar_score = LS_DB_PREGNANCY_TMP.pregocom_apgar_score,LS_DB_PREGNANCY.pregocom_abortion_date_fmt = LS_DB_PREGNANCY_TMP.pregocom_abortion_date_fmt,LS_DB_PREGNANCY.pregocom_abortion_date = LS_DB_PREGNANCY_TMP.pregocom_abortion_date,LS_DB_PREGNANCY.pregchild_user_modified = LS_DB_PREGNANCY_TMP.pregchild_user_modified,LS_DB_PREGNANCY.pregchild_user_created = LS_DB_PREGNANCY_TMP.pregchild_user_created,LS_DB_PREGNANCY.pregchild_spr_id = LS_DB_PREGNANCY_TMP.pregchild_spr_id,LS_DB_PREGNANCY.pregchild_record_id = LS_DB_PREGNANCY_TMP.pregchild_record_id,LS_DB_PREGNANCY.pregchild_fk_aprg_record_id = LS_DB_PREGNANCY_TMP.pregchild_fk_aprg_record_id,LS_DB_PREGNANCY.pregchild_date_modified = LS_DB_PREGNANCY_TMP.pregchild_date_modified,LS_DB_PREGNANCY.pregchild_date_created = LS_DB_PREGNANCY_TMP.pregchild_date_created,LS_DB_PREGNANCY.pregchild_child_unit = LS_DB_PREGNANCY_TMP.pregchild_child_unit,LS_DB_PREGNANCY.pregchild_child_sex = LS_DB_PREGNANCY_TMP.pregchild_child_sex,LS_DB_PREGNANCY.pregchild_child_age = LS_DB_PREGNANCY_TMP.pregchild_child_age,LS_DB_PREGNANCY.pregchild_ari_rec_id = LS_DB_PREGNANCY_TMP.pregchild_ari_rec_id,LS_DB_PREGNANCY.neon_which_pregnancy_de_ml = LS_DB_PREGNANCY_TMP.neon_which_pregnancy_de_ml,LS_DB_PREGNANCY.neon_which_pregnancy = LS_DB_PREGNANCY_TMP.neon_which_pregnancy,LS_DB_PREGNANCY.neon_user_modified = LS_DB_PREGNANCY_TMP.neon_user_modified,LS_DB_PREGNANCY.neon_user_created = LS_DB_PREGNANCY_TMP.neon_user_created,LS_DB_PREGNANCY.neon_spr_id = LS_DB_PREGNANCY_TMP.neon_spr_id,LS_DB_PREGNANCY.neon_resuscitated_de_ml = LS_DB_PREGNANCY_TMP.neon_resuscitated_de_ml,LS_DB_PREGNANCY.neon_resuscitated = LS_DB_PREGNANCY_TMP.neon_resuscitated,LS_DB_PREGNANCY.neon_record_id = LS_DB_PREGNANCY_TMP.neon_record_id,LS_DB_PREGNANCY.neon_other_outcome_details = LS_DB_PREGNANCY_TMP.neon_other_outcome_details,LS_DB_PREGNANCY.neon_other_neonate_details = LS_DB_PREGNANCY_TMP.neon_other_neonate_details,LS_DB_PREGNANCY.neon_no_of_foetus = LS_DB_PREGNANCY_TMP.neon_no_of_foetus,LS_DB_PREGNANCY.neon_nicu_admission_de_ml = LS_DB_PREGNANCY_TMP.neon_nicu_admission_de_ml,LS_DB_PREGNANCY.neon_nicu_admission = LS_DB_PREGNANCY_TMP.neon_nicu_admission,LS_DB_PREGNANCY.neon_neon_weight_unit = LS_DB_PREGNANCY_TMP.neon_neon_weight_unit,LS_DB_PREGNANCY.neon_neon_weight = LS_DB_PREGNANCY_TMP.neon_neon_weight,LS_DB_PREGNANCY.neon_neon_birth_len_unit = LS_DB_PREGNANCY_TMP.neon_neon_birth_len_unit,LS_DB_PREGNANCY.neon_neon_birth_len = LS_DB_PREGNANCY_TMP.neon_neon_birth_len,LS_DB_PREGNANCY.neon_head_circum_birth_unit = LS_DB_PREGNANCY_TMP.neon_head_circum_birth_unit,LS_DB_PREGNANCY.neon_head_circum_birth = LS_DB_PREGNANCY_TMP.neon_head_circum_birth,LS_DB_PREGNANCY.neon_gestational_age_birth_unit_de_ml = LS_DB_PREGNANCY_TMP.neon_gestational_age_birth_unit_de_ml,LS_DB_PREGNANCY.neon_gestational_age_birth_unit = LS_DB_PREGNANCY_TMP.neon_gestational_age_birth_unit,LS_DB_PREGNANCY.neon_gestational_age_birth = LS_DB_PREGNANCY_TMP.neon_gestational_age_birth,LS_DB_PREGNANCY.neon_fk_apoc_rec_id = LS_DB_PREGNANCY_TMP.neon_fk_apoc_rec_id,LS_DB_PREGNANCY.neon_date_modified = LS_DB_PREGNANCY_TMP.neon_date_modified,LS_DB_PREGNANCY.neon_date_created = LS_DB_PREGNANCY_TMP.neon_date_created,LS_DB_PREGNANCY.neon_current_pregnancy_checkbox_de_ml = LS_DB_PREGNANCY_TMP.neon_current_pregnancy_checkbox_de_ml,LS_DB_PREGNANCY.neon_current_pregnancy_checkbox = LS_DB_PREGNANCY_TMP.neon_current_pregnancy_checkbox,LS_DB_PREGNANCY.neon_congenital_anomaly_type_sf = LS_DB_PREGNANCY_TMP.neon_congenital_anomaly_type_sf,LS_DB_PREGNANCY.neon_congenital_anomaly_type_de_ml = LS_DB_PREGNANCY_TMP.neon_congenital_anomaly_type_de_ml,LS_DB_PREGNANCY.neon_congenital_anomaly_type = LS_DB_PREGNANCY_TMP.neon_congenital_anomaly_type,LS_DB_PREGNANCY.neon_congenital_anomaly_de_ml = LS_DB_PREGNANCY_TMP.neon_congenital_anomaly_de_ml,LS_DB_PREGNANCY.neon_congenital_anomaly = LS_DB_PREGNANCY_TMP.neon_congenital_anomaly,LS_DB_PREGNANCY.neon_child_unit = LS_DB_PREGNANCY_TMP.neon_child_unit,LS_DB_PREGNANCY.neon_child_sex_de_ml = LS_DB_PREGNANCY_TMP.neon_child_sex_de_ml,LS_DB_PREGNANCY.neon_child_sex = LS_DB_PREGNANCY_TMP.neon_child_sex,LS_DB_PREGNANCY.neon_child_identity = LS_DB_PREGNANCY_TMP.neon_child_identity,LS_DB_PREGNANCY.neon_child_age = LS_DB_PREGNANCY_TMP.neon_child_age,LS_DB_PREGNANCY.neon_birth_outcome_de_ml = LS_DB_PREGNANCY_TMP.neon_birth_outcome_de_ml,LS_DB_PREGNANCY.neon_birth_outcome = LS_DB_PREGNANCY_TMP.neon_birth_outcome,LS_DB_PREGNANCY.neon_ari_rec_id = LS_DB_PREGNANCY_TMP.neon_ari_rec_id,LS_DB_PREGNANCY.neon_apgar_score_5_minute = LS_DB_PREGNANCY_TMP.neon_apgar_score_5_minute,LS_DB_PREGNANCY.neon_apgar_score_10_minute = LS_DB_PREGNANCY_TMP.neon_apgar_score_10_minute,LS_DB_PREGNANCY.neon_apgar_score = LS_DB_PREGNANCY_TMP.neon_apgar_score,LS_DB_PREGNANCY.neon_admission_duration_unit_de_ml = LS_DB_PREGNANCY_TMP.neon_admission_duration_unit_de_ml,LS_DB_PREGNANCY.neon_admission_duration_unit = LS_DB_PREGNANCY_TMP.neon_admission_duration_unit,LS_DB_PREGNANCY.neon_admission_duration_sf = LS_DB_PREGNANCY_TMP.neon_admission_duration_sf,LS_DB_PREGNANCY.neon_admission_duration = LS_DB_PREGNANCY_TMP.neon_admission_duration,LS_DB_PREGNANCY.neonchild_user_modified = LS_DB_PREGNANCY_TMP.neonchild_user_modified,LS_DB_PREGNANCY.neonchild_user_created = LS_DB_PREGNANCY_TMP.neonchild_user_created,LS_DB_PREGNANCY.neonchild_spr_id = LS_DB_PREGNANCY_TMP.neonchild_spr_id,LS_DB_PREGNANCY.neonchild_record_id = LS_DB_PREGNANCY_TMP.neonchild_record_id,LS_DB_PREGNANCY.neonchild_neon_weight_unit_de_ml = LS_DB_PREGNANCY_TMP.neonchild_neon_weight_unit_de_ml,LS_DB_PREGNANCY.neonchild_neon_weight_unit = LS_DB_PREGNANCY_TMP.neonchild_neon_weight_unit,LS_DB_PREGNANCY.neonchild_neon_weight = LS_DB_PREGNANCY_TMP.neonchild_neon_weight,LS_DB_PREGNANCY.neonchild_neon_birth_len_unit_de_ml = LS_DB_PREGNANCY_TMP.neonchild_neon_birth_len_unit_de_ml,LS_DB_PREGNANCY.neonchild_neon_birth_len_unit = LS_DB_PREGNANCY_TMP.neonchild_neon_birth_len_unit,LS_DB_PREGNANCY.neonchild_neon_birth_len = LS_DB_PREGNANCY_TMP.neonchild_neon_birth_len,LS_DB_PREGNANCY.neonchild_head_circum_birth_unit_de_ml = LS_DB_PREGNANCY_TMP.neonchild_head_circum_birth_unit_de_ml,LS_DB_PREGNANCY.neonchild_head_circum_birth_unit = LS_DB_PREGNANCY_TMP.neonchild_head_circum_birth_unit,LS_DB_PREGNANCY.neonchild_head_circum_birth = LS_DB_PREGNANCY_TMP.neonchild_head_circum_birth,LS_DB_PREGNANCY.neonchild_fk_an_record_id = LS_DB_PREGNANCY_TMP.neonchild_fk_an_record_id,LS_DB_PREGNANCY.neonchild_date_modified = LS_DB_PREGNANCY_TMP.neonchild_date_modified,LS_DB_PREGNANCY.neonchild_date_created = LS_DB_PREGNANCY_TMP.neonchild_date_created,LS_DB_PREGNANCY.neonchild_child_unit_de_ml = LS_DB_PREGNANCY_TMP.neonchild_child_unit_de_ml,LS_DB_PREGNANCY.neonchild_child_unit = LS_DB_PREGNANCY_TMP.neonchild_child_unit,LS_DB_PREGNANCY.neonchild_child_age = LS_DB_PREGNANCY_TMP.neonchild_child_age,LS_DB_PREGNANCY.neonchild_ari_rec_id = LS_DB_PREGNANCY_TMP.neonchild_ari_rec_id,
LS_DB_PREGNANCY.PROCESSING_DT = LS_DB_PREGNANCY_TMP.PROCESSING_DT,
LS_DB_PREGNANCY.receipt_id     =LS_DB_PREGNANCY_TMP.receipt_id    ,
LS_DB_PREGNANCY.case_no        =LS_DB_PREGNANCY_TMP.case_no           ,
LS_DB_PREGNANCY.case_version   =LS_DB_PREGNANCY_TMP.case_version      ,
LS_DB_PREGNANCY.version_no     =LS_DB_PREGNANCY_TMP.version_no        ,
LS_DB_PREGNANCY.user_modified  =LS_DB_PREGNANCY_TMP.user_modified     ,
LS_DB_PREGNANCY.date_modified  =LS_DB_PREGNANCY_TMP.date_modified     ,
LS_DB_PREGNANCY.expiry_date    =LS_DB_PREGNANCY_TMP.expiry_date       ,
LS_DB_PREGNANCY.created_by     =LS_DB_PREGNANCY_TMP.created_by        ,
LS_DB_PREGNANCY.created_dt     =LS_DB_PREGNANCY_TMP.created_dt        ,
LS_DB_PREGNANCY.load_ts        =LS_DB_PREGNANCY_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP 
WHERE 	LS_DB_PREGNANCY.INTEGRATION_ID = LS_DB_PREGNANCY_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PREGNANCY_TMP.PROCESSING_DT = LS_DB_PREGNANCY.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PREGNANCY.preg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.neonchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.pregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.pregchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.neon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.prepregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PREGNANCY_TMP.preg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.neonchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.pregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.pregchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.neon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.prepregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PREGNANCY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PREGNANCY.preg_ARI_REC_ID ,LS_DB_PREGNANCY.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP 
ON LS_DB_PREGNANCY.preg_ARI_REC_ID=LS_DB_PREGNANCY_TMP.preg_ARI_REC_ID
AND LS_DB_PREGNANCY.INTEGRATION_ID = LS_DB_PREGNANCY_TMP.INTEGRATION_ID 
where LS_DB_PREGNANCY_TMP.INTEGRATION_ID  is null AND LS_DB_PREGNANCY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PREGNANCY.preg_ARI_REC_ID in (select preg_ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP )
) TMP where LS_DB_PREGNANCY.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PREGNANCY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PREGNANCY.preg_ARI_REC_ID ,LS_DB_PREGNANCY.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP 
    ON LS_DB_PREGNANCY.preg_ARI_REC_ID=LS_DB_PREGNANCY_TMP.preg_ARI_REC_ID
    AND LS_DB_PREGNANCY.INTEGRATION_ID = LS_DB_PREGNANCY_TMP.INTEGRATION_ID 
    where LS_DB_PREGNANCY_TMP.INTEGRATION_ID  is null AND LS_DB_PREGNANCY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PREGNANCY.preg_ARI_REC_ID in (select preg_ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY
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
integration_id ,prepregocom_uuid,
prepregocom_user_modified,
prepregocom_user_created,
prepregocom_spr_id,
prepregocom_record_id,
prepregocom_past_pregnancy_outcome_details,
prepregocom_past_pregnancy_outcome_de_ml,
prepregocom_past_pregnancy_outcome,
prepregocom_number_of_abortions,
prepregocom_no_of_children,
prepregocom_inq_rec_id,
prepregocom_fk_apo_record_id,
prepregocom_entity_updated,
prepregocom_date_modified,
prepregocom_date_created,
prepregocom_comp_rec_id,
prepregocom_ari_rec_id,
preg_user_modified,
preg_user_created,
preg_types_of_contraceptive_used,
preg_trimester_of_exposure_sf,
preg_trimester_of_exposure_de_ml,
preg_trimester_of_exposure,
preg_spr_id,
preg_record_id,
preg_pregnancy_confirm_mode_de_ml,
preg_pregnancy_confirm_mode,
preg_pregnancy_confirm_date_fmt,
preg_pregnancy_confirm_date,
preg_pregnancy_complication_desc,
preg_pre_pregnancy_weight_unit_de_ml,
preg_pre_pregnancy_weight_unit,
preg_pre_pregnancy_weight,
preg_planned_pregnancy_de_ml,
preg_planned_pregnancy,
preg_past_pregnancy_outcome_details,
preg_past_pregnancy_outcome,
preg_parity,
preg_para,
preg_number_of_abortions,
preg_no_of_unknown_outcomes,
preg_no_of_normal_outcomes,
preg_no_of_children,
preg_no_of_abnormal_outcomes,
preg_narrative_generated_comment,
preg_mother_exp_any_medical_problem_de_ml,
preg_mother_exp_any_medical_problem,
preg_mother_before_pregnant_de_ml,
preg_mother_before_pregnant,
preg_inq_rec_id,
preg_gravidity,
preg_gestation_week_type,
preg_gestatationperiodunit_de_ml,
preg_gestatationperiodunit,
preg_gestatationperiod,
preg_ext_clob_fld,
preg_exposure_status_de_ml,
preg_exposure_status,
preg_expected_due_date_fmt,
preg_expected_due_date,
preg_entity_updated,
preg_do_not_use_ang_comment,
preg_delivery_method,
preg_date_of_first_consultation_fmt,
preg_date_of_first_consultation,
preg_date_modified,
preg_date_created,
preg_contraceptives_used_de_ml,
preg_contraceptives_used,
preg_contraceptive_failure_de_ml,
preg_contraceptive_failure,
preg_consent_to_preg_register_de_ml,
preg_consent_to_preg_register,
preg_consent_to_contact_de_ml,
preg_consent_to_contact,
preg_comp_rec_id,
preg_comments,
preg_birth_defects_history_de_ml,
preg_birth_defects_history,
preg_ari_rec_id,
preg_any_relevant_info_from_partner_de_ml,
preg_any_relevant_info_from_partner,
pregocom_user_modified,
pregocom_user_created,
pregocom_spr_id,
pregocom_record_id,
pregocom_pregnancy_outcome_date_fmt,
pregocom_pregnancy_outcome_date,
pregocom_pregnancy_end_date_fmt,
pregocom_pregnancy_end_date,
pregocom_pregnancy_clinical_status_oth,
pregocom_pregnancy_clinical_status_de_ml,
pregocom_pregnancy_clinical_status,
pregocom_outcome_comments,
pregocom_other_outcome_dtls,
pregocom_other_details,
pregocom_no_of_foetus,
pregocom_neonatal_birth_length,
pregocom_neon_birt_leng_unit_de_ml,
pregocom_neon_birt_leng_unit,
pregocom_method_of_delivery_de_ml,
pregocom_method_of_delivery,
pregocom_live_birth_complications_de_ml,
pregocom_live_birth_complications,
pregocom_inq_rec_id,
pregocom_head_circum_birth_unit,
pregocom_head_circum_birth,
pregocom_gestation_age_in_weeks,
pregocom_gestation_age_at_outcome_unit,
pregocom_gestation_age_at_outcome,
pregocom_fk_aprg_record_id,
pregocom_fk_appo_record_id,
pregocom_fetus_clinical_condition_de_ml,
pregocom_fetus_clinical_condition,
pregocom_entity_updated,
pregocom_delivery_method_de_ml,
pregocom_delivery_method,
pregocom_delivery_date_fmt,
pregocom_delivery_date,
pregocom_date_modified,
pregocom_date_created,
pregocom_csection_reason,
pregocom_comp_rec_id,
pregocom_childweight_unit_lang,
pregocom_childweight_unit_code_de_ml,
pregocom_childweight_unit_code,
pregocom_childweight,
pregocom_ari_rec_id,
pregocom_apgar_score,
pregocom_abortion_date_fmt,
pregocom_abortion_date,
pregchild_user_modified,
pregchild_user_created,
pregchild_spr_id,
pregchild_record_id,
pregchild_fk_aprg_record_id,
pregchild_date_modified,
pregchild_date_created,
pregchild_child_unit,
pregchild_child_sex,
pregchild_child_age,
pregchild_ari_rec_id,
neon_which_pregnancy_de_ml,
neon_which_pregnancy,
neon_user_modified,
neon_user_created,
neon_spr_id,
neon_resuscitated_de_ml,
neon_resuscitated,
neon_record_id,
neon_other_outcome_details,
neon_other_neonate_details,
neon_no_of_foetus,
neon_nicu_admission_de_ml,
neon_nicu_admission,
neon_neon_weight_unit,
neon_neon_weight,
neon_neon_birth_len_unit,
neon_neon_birth_len,
neon_head_circum_birth_unit,
neon_head_circum_birth,
neon_gestational_age_birth_unit_de_ml,
neon_gestational_age_birth_unit,
neon_gestational_age_birth,
neon_fk_apoc_rec_id,
neon_date_modified,
neon_date_created,
neon_current_pregnancy_checkbox_de_ml,
neon_current_pregnancy_checkbox,
neon_congenital_anomaly_type_sf,
neon_congenital_anomaly_type_de_ml,
neon_congenital_anomaly_type,
neon_congenital_anomaly_de_ml,
neon_congenital_anomaly,
neon_child_unit,
neon_child_sex_de_ml,
neon_child_sex,
neon_child_identity,
neon_child_age,
neon_birth_outcome_de_ml,
neon_birth_outcome,
neon_ari_rec_id,
neon_apgar_score_5_minute,
neon_apgar_score_10_minute,
neon_apgar_score,
neon_admission_duration_unit_de_ml,
neon_admission_duration_unit,
neon_admission_duration_sf,
neon_admission_duration,
neonchild_user_modified,
neonchild_user_created,
neonchild_spr_id,
neonchild_record_id,
neonchild_neon_weight_unit_de_ml,
neonchild_neon_weight_unit,
neonchild_neon_weight,
neonchild_neon_birth_len_unit_de_ml,
neonchild_neon_birth_len_unit,
neonchild_neon_birth_len,
neonchild_head_circum_birth_unit_de_ml,
neonchild_head_circum_birth_unit,
neonchild_head_circum_birth,
neonchild_fk_an_record_id,
neonchild_date_modified,
neonchild_date_created,
neonchild_child_unit_de_ml,
neonchild_child_unit,
neonchild_child_age,
neonchild_ari_rec_id)
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
integration_id ,prepregocom_uuid,
prepregocom_user_modified,
prepregocom_user_created,
prepregocom_spr_id,
prepregocom_record_id,
prepregocom_past_pregnancy_outcome_details,
prepregocom_past_pregnancy_outcome_de_ml,
prepregocom_past_pregnancy_outcome,
prepregocom_number_of_abortions,
prepregocom_no_of_children,
prepregocom_inq_rec_id,
prepregocom_fk_apo_record_id,
prepregocom_entity_updated,
prepregocom_date_modified,
prepregocom_date_created,
prepregocom_comp_rec_id,
prepregocom_ari_rec_id,
preg_user_modified,
preg_user_created,
preg_types_of_contraceptive_used,
preg_trimester_of_exposure_sf,
preg_trimester_of_exposure_de_ml,
preg_trimester_of_exposure,
preg_spr_id,
preg_record_id,
preg_pregnancy_confirm_mode_de_ml,
preg_pregnancy_confirm_mode,
preg_pregnancy_confirm_date_fmt,
preg_pregnancy_confirm_date,
preg_pregnancy_complication_desc,
preg_pre_pregnancy_weight_unit_de_ml,
preg_pre_pregnancy_weight_unit,
preg_pre_pregnancy_weight,
preg_planned_pregnancy_de_ml,
preg_planned_pregnancy,
preg_past_pregnancy_outcome_details,
preg_past_pregnancy_outcome,
preg_parity,
preg_para,
preg_number_of_abortions,
preg_no_of_unknown_outcomes,
preg_no_of_normal_outcomes,
preg_no_of_children,
preg_no_of_abnormal_outcomes,
preg_narrative_generated_comment,
preg_mother_exp_any_medical_problem_de_ml,
preg_mother_exp_any_medical_problem,
preg_mother_before_pregnant_de_ml,
preg_mother_before_pregnant,
preg_inq_rec_id,
preg_gravidity,
preg_gestation_week_type,
preg_gestatationperiodunit_de_ml,
preg_gestatationperiodunit,
preg_gestatationperiod,
preg_ext_clob_fld,
preg_exposure_status_de_ml,
preg_exposure_status,
preg_expected_due_date_fmt,
preg_expected_due_date,
preg_entity_updated,
preg_do_not_use_ang_comment,
preg_delivery_method,
preg_date_of_first_consultation_fmt,
preg_date_of_first_consultation,
preg_date_modified,
preg_date_created,
preg_contraceptives_used_de_ml,
preg_contraceptives_used,
preg_contraceptive_failure_de_ml,
preg_contraceptive_failure,
preg_consent_to_preg_register_de_ml,
preg_consent_to_preg_register,
preg_consent_to_contact_de_ml,
preg_consent_to_contact,
preg_comp_rec_id,
preg_comments,
preg_birth_defects_history_de_ml,
preg_birth_defects_history,
preg_ari_rec_id,
preg_any_relevant_info_from_partner_de_ml,
preg_any_relevant_info_from_partner,
pregocom_user_modified,
pregocom_user_created,
pregocom_spr_id,
pregocom_record_id,
pregocom_pregnancy_outcome_date_fmt,
pregocom_pregnancy_outcome_date,
pregocom_pregnancy_end_date_fmt,
pregocom_pregnancy_end_date,
pregocom_pregnancy_clinical_status_oth,
pregocom_pregnancy_clinical_status_de_ml,
pregocom_pregnancy_clinical_status,
pregocom_outcome_comments,
pregocom_other_outcome_dtls,
pregocom_other_details,
pregocom_no_of_foetus,
pregocom_neonatal_birth_length,
pregocom_neon_birt_leng_unit_de_ml,
pregocom_neon_birt_leng_unit,
pregocom_method_of_delivery_de_ml,
pregocom_method_of_delivery,
pregocom_live_birth_complications_de_ml,
pregocom_live_birth_complications,
pregocom_inq_rec_id,
pregocom_head_circum_birth_unit,
pregocom_head_circum_birth,
pregocom_gestation_age_in_weeks,
pregocom_gestation_age_at_outcome_unit,
pregocom_gestation_age_at_outcome,
pregocom_fk_aprg_record_id,
pregocom_fk_appo_record_id,
pregocom_fetus_clinical_condition_de_ml,
pregocom_fetus_clinical_condition,
pregocom_entity_updated,
pregocom_delivery_method_de_ml,
pregocom_delivery_method,
pregocom_delivery_date_fmt,
pregocom_delivery_date,
pregocom_date_modified,
pregocom_date_created,
pregocom_csection_reason,
pregocom_comp_rec_id,
pregocom_childweight_unit_lang,
pregocom_childweight_unit_code_de_ml,
pregocom_childweight_unit_code,
pregocom_childweight,
pregocom_ari_rec_id,
pregocom_apgar_score,
pregocom_abortion_date_fmt,
pregocom_abortion_date,
pregchild_user_modified,
pregchild_user_created,
pregchild_spr_id,
pregchild_record_id,
pregchild_fk_aprg_record_id,
pregchild_date_modified,
pregchild_date_created,
pregchild_child_unit,
pregchild_child_sex,
pregchild_child_age,
pregchild_ari_rec_id,
neon_which_pregnancy_de_ml,
neon_which_pregnancy,
neon_user_modified,
neon_user_created,
neon_spr_id,
neon_resuscitated_de_ml,
neon_resuscitated,
neon_record_id,
neon_other_outcome_details,
neon_other_neonate_details,
neon_no_of_foetus,
neon_nicu_admission_de_ml,
neon_nicu_admission,
neon_neon_weight_unit,
neon_neon_weight,
neon_neon_birth_len_unit,
neon_neon_birth_len,
neon_head_circum_birth_unit,
neon_head_circum_birth,
neon_gestational_age_birth_unit_de_ml,
neon_gestational_age_birth_unit,
neon_gestational_age_birth,
neon_fk_apoc_rec_id,
neon_date_modified,
neon_date_created,
neon_current_pregnancy_checkbox_de_ml,
neon_current_pregnancy_checkbox,
neon_congenital_anomaly_type_sf,
neon_congenital_anomaly_type_de_ml,
neon_congenital_anomaly_type,
neon_congenital_anomaly_de_ml,
neon_congenital_anomaly,
neon_child_unit,
neon_child_sex_de_ml,
neon_child_sex,
neon_child_identity,
neon_child_age,
neon_birth_outcome_de_ml,
neon_birth_outcome,
neon_ari_rec_id,
neon_apgar_score_5_minute,
neon_apgar_score_10_minute,
neon_apgar_score,
neon_admission_duration_unit_de_ml,
neon_admission_duration_unit,
neon_admission_duration_sf,
neon_admission_duration,
neonchild_user_modified,
neonchild_user_created,
neonchild_spr_id,
neonchild_record_id,
neonchild_neon_weight_unit_de_ml,
neonchild_neon_weight_unit,
neonchild_neon_weight,
neonchild_neon_birth_len_unit_de_ml,
neonchild_neon_birth_len_unit,
neonchild_neon_birth_len,
neonchild_head_circum_birth_unit_de_ml,
neonchild_head_circum_birth_unit,
neonchild_head_circum_birth,
neonchild_fk_an_record_id,
neonchild_date_modified,
neonchild_date_created,
neonchild_child_unit_de_ml,
neonchild_child_unit,
neonchild_child_age,
neonchild_ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP 
WHERE 	TO_DATE(LS_DB_PREGNANCY.PROCESSING_DT) < TO_DATE(LS_DB_PREGNANCY_TMP.PROCESSING_DT)
AND LS_DB_PREGNANCY.INTEGRATION_ID = LS_DB_PREGNANCY_TMP.INTEGRATION_ID
AND LS_DB_PREGNANCY.preg_ARI_REC_ID = LS_DB_PREGNANCY_TMP.preg_ARI_REC_ID
AND LS_DB_PREGNANCY.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PREGNANCY.preg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.neonchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.pregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.pregchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.neon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PREGNANCY.prepregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PREGNANCY_TMP.preg_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.neonchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.pregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.pregchild_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.neon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PREGNANCY_TMP.prepregocom_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY TGT
WHERE  ( neon_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate') OR neonchild_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate_child') OR preg_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy') OR pregchild_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_child') OR pregocom_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_outcomes') OR prepregocom_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_previous_preg_outcome')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( neon_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate') OR neonchild_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_neonate_child') OR preg_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy') OR pregchild_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_child') OR pregocom_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_pregnancy_outcomes') OR prepregocom_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_PREGNANCY_DELETION_TMP  WHERE TABLE_NAME='lsmv_previous_preg_outcome')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PREGNANCY_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PREGNANCY'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PREGNANCY'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_PREGNANCY Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PREGNANCY'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_PREGNANCY not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
