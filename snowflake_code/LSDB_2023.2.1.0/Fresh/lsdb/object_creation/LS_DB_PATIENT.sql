
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_PATIENT()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT'),
                'LSDB','Case','LS_DB_PATIENT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_PATIENT' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_PATIENT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  As select RECORD_ID,'lsmv_death_cause' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_death_cause WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_parent' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_patient' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_patient_autopsy' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_autopsy WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_patient_death' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_death WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_patient_partner' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_partner WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1006','10064','1007','1007','10075','10078','1008','10117','1015','1016','1016','1016','1016','1016','1021','1022','1022','1026','11','15','2000','347','347','372','4','4','4','4','48','48','5014','7073','7077','7077','8101','8101','8147','9169','9941')
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

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_death_cause WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_death_cause WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_parent WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_death WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_death WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_partner WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_partner WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_autopsy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT ARI_REC_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_patient_autopsy WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_PATIENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_death_cause_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  dthcause_ari_rec_id,coding_comments  dthcause_coding_comments,coding_type  dthcause_coding_type,comp_rec_id  dthcause_comp_rec_id,date_created  dthcause_date_created,date_modified  dthcause_date_modified,deathcause_coded_flag  dthcause_deathcause_coded_flag,entity_updated  dthcause_entity_updated,ext_clob_fld  dthcause_ext_clob_fld,fk_apd_rec_id  dthcause_fk_apd_rec_id,inq_rec_id  dthcause_inq_rec_id,patdeathcause_lang  dthcause_patdeathcause_lang,patdeathrepmeddraver  dthcause_patdeathrepmeddraver,patdeathrepmeddraver_lang  dthcause_patdeathrepmeddraver_lang,patdeathreport  dthcause_patdeathreport,try_to_number(patdeathreport_code,38)  dthcause_patdeathreport_code,patdeathreport_code  dthcause_patdeathreport_code_1 ,patdeathreport_decode  dthcause_patdeathreport_decode,patdeathreport_lang  dthcause_patdeathreport_lang,try_to_number(patdeathreport_ptcode,38)  dthcause_patdeathreport_ptcode,patdeathreport_ptcode  dthcause_patdeathreport_ptcode_1 ,patdeathreportlevel  dthcause_patdeathreportlevel,record_id  dthcause_record_id,spr_id  dthcause_spr_id,user_created  dthcause_user_created,user_modified  dthcause_user_modified,uuid  dthcause_uuid,version  dthcause_version,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_death_cause
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_death_cause')
  ) where REC_RANK=1 )
  , lsmv_parent_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  par_ari_rec_id,comp_rec_id  par_comp_rec_id,consent_to_contact_parent  par_consent_to_contact_parent,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7073' AND CD=CAST(consent_to_contact_parent AS VARCHAR(100)) )par_consent_to_contact_parent_de_ml , date_created  par_date_created,date_modified  par_date_modified,duration  par_duration,duration_unit  par_duration_unit,entity_updated  par_entity_updated,ext_clob_fld  par_ext_clob_fld,inq_rec_id  par_inq_rec_id,par_ethnic_origin  par_par_ethnic_origin,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1022' AND CD=CAST(par_ethnic_origin AS VARCHAR(100)) )par_par_ethnic_origin_de_ml , parent_age_at_vaccine  par_parent_age_at_vaccine,parent_age_at_vaccine_unit  par_parent_age_at_vaccine_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(parent_age_at_vaccine_unit AS VARCHAR(100)) )par_parent_age_at_vaccine_unit_de_ml , parent_lang  par_parent_lang,try_to_number(parentage,38)  par_parentage,parentage  par_parentage_1 ,parentage_lang  par_parentage_lang,parentageunit  par_parentageunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(parentageunit AS VARCHAR(100)) )par_parentageunit_de_ml , parentdob  par_parentdob,parentdob_nf  par_parentdob_nf,parentdob_rdm  par_parentdob_rdm,parentdob_tz  par_parentdob_tz,parentdobfmt  par_parentdobfmt,parentheight  par_parentheight,parentheight_lang  par_parentheight_lang,parentheight_unit  par_parentheight_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='347' AND CD=CAST(parentheight_unit AS VARCHAR(100)) )par_parentheight_unit_de_ml , parentidentification  par_parentidentification,parentidentification_lang  par_parentidentification_lang,parentidentification_nf  par_parentidentification_nf,parentlastmensdate  par_parentlastmensdate,parentlastmensdate_nf  par_parentlastmensdate_nf,parentlastmensdate_tz  par_parentlastmensdate_tz,parentlastmensdatefmt  par_parentlastmensdatefmt,parentmedrelevanttext  par_parentmedrelevanttext,parentmedrelevanttext_lang  par_parentmedrelevanttext_lang,parentsex  par_parentsex,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1007' AND CD=CAST(parentsex AS VARCHAR(100)) )par_parentsex_de_ml , parentsex_nf  par_parentsex_nf,try_to_number(parentweight,38)  par_parentweight,parentweight  par_parentweight_1 ,parentweight_lang  par_parentweight_lang,parentweight_unit  par_parentweight_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='48' AND CD=CAST(parentweight_unit AS VARCHAR(100)) )par_parentweight_unit_de_ml , paternal_exposure  par_paternal_exposure,record_id  par_record_id,spr_id  par_spr_id,user_created  par_user_created,user_modified  par_user_modified,version  par_version,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_parent
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent')
  ) where REC_RANK=1 )
  , lsmv_patient_autopsy_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  patapsy_ari_rec_id,autopsycause_coded_flag  patapsy_autopsycause_coded_flag,coding_comments  patapsy_coding_comments,coding_type  patapsy_coding_type,comp_rec_id  patapsy_comp_rec_id,date_created  patapsy_date_created,date_modified  patapsy_date_modified,entity_updated  patapsy_entity_updated,ext_clob_fld  patapsy_ext_clob_fld,fk_apd_rec_id  patapsy_fk_apd_rec_id,inq_rec_id  patapsy_inq_rec_id,patautopsy_lang  patapsy_patautopsy_lang,patdetautopsy  patapsy_patdetautopsy,try_to_number(patdetautopsy_code,38)  patapsy_patdetautopsy_code,patdetautopsy_code  patapsy_patdetautopsy_code_1 ,patdetautopsy_decode  patapsy_patdetautopsy_decode,patdetautopsy_lang  patapsy_patdetautopsy_lang,try_to_number(patdetautopsy_ptcode,38)  patapsy_patdetautopsy_ptcode,patdetautopsy_ptcode  patapsy_patdetautopsy_ptcode_1 ,patdetautopsylevel  patapsy_patdetautopsylevel,patdetautopsymeddraver  patapsy_patdetautopsymeddraver,patdetautopsymeddraver_lang  patapsy_patdetautopsymeddraver_lang,record_id  patapsy_record_id,spr_id  patapsy_spr_id,user_created  patapsy_user_created,user_modified  patapsy_user_modified,version  patapsy_version,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_patient_autopsy
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_autopsy')
  ) where REC_RANK=1 )
  , lsmv_patient_SUBSET AS 
(
select * from 
    (SELECT  
    aborginal  pat_aborginal,address  pat_address,address1  pat_address1,address1_nf  pat_address1_nf,address2  pat_address2,address2_nf  pat_address2_nf,address_nf  pat_address_nf,age_at_time_of_vaccine  pat_age_at_time_of_vaccine,age_at_time_of_vaccine_unit  pat_age_at_time_of_vaccine_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(age_at_time_of_vaccine_unit AS VARCHAR(100)) )pat_age_at_time_of_vaccine_unit_de_ml , age_group_manual  pat_age_group_manual,allergies  pat_allergies,american_ind_or_alaskan  pat_american_ind_or_alaskan,any_relevant_info_from_partner  pat_any_relevant_info_from_partner,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(any_relevant_info_from_partner AS VARCHAR(100)) )pat_any_relevant_info_from_partner_de_ml , ari_rec_id  pat_ari_rec_id,asian  pat_asian,birth_weight  pat_birth_weight,birth_weight_unit  pat_birth_weight_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='48' AND CD=CAST(birth_weight_unit AS VARCHAR(100)) )pat_birth_weight_unit_de_ml , black_african_american  pat_black_african_american,body_mass_index  pat_body_mass_index,body_mass_index_type  pat_body_mass_index_type,body_surface_index  pat_body_surface_index,body_surface_index_type  pat_body_surface_index_type,city  pat_city,city_nf  pat_city_nf,coding_comments  pat_coding_comments,comp_rec_id  pat_comp_rec_id,concomitant_product  pat_concomitant_product,concomitant_therapies  pat_concomitant_therapies,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(concomitant_therapies AS VARCHAR(100)) )pat_concomitant_therapies_de_ml , contact_info_avail  pat_contact_info_avail,country  pat_country,country_cn  pat_country_cn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1015' AND CD=CAST(country_cn AS VARCHAR(100)) )pat_country_cn_de_ml , country_nf  pat_country_nf,county  pat_county,county_nf  pat_county_nf,data_encrypted  pat_data_encrypted,dataprivacy_present  pat_dataprivacy_present,date_created  pat_date_created,date_modified  pat_date_modified,device_problem_type  pat_device_problem_type,do_not_use_ang_mh  pat_do_not_use_ang_mh,dob_unknown_code  pat_dob_unknown_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(dob_unknown_code AS VARCHAR(100)) )pat_dob_unknown_code_de_ml , donotreportname  pat_donotreportname,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9941' AND CD=CAST(donotreportname AS VARCHAR(100)) )pat_donotreportname_de_ml , drug_type  pat_drug_type,e2b_r3_gestperiodoverride  pat_e2b_r3_gestperiodoverride,email_id  pat_email_id,email_id_nf  pat_email_id_nf,enrolment_date  pat_enrolment_date,enrolment_status  pat_enrolment_status,entity_updated  pat_entity_updated,ethenic_origin_nf  pat_ethenic_origin_nf,ethnic_origin  pat_ethnic_origin,ethnic_origin_cn  pat_ethnic_origin_cn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10075' AND CD=CAST(ethnic_origin_cn AS VARCHAR(100)) )pat_ethnic_origin_cn_de_ml , (SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1022' AND CD=CAST(ethnic_origin AS VARCHAR(100)) )pat_ethnic_origin_de_ml , event_date  pat_event_date,event_date_fmt  pat_event_date_fmt,event_date_tz  pat_event_date_tz,expectedduedate  pat_expectedduedate,expectedduedate_fmt  pat_expectedduedate_fmt,ext_clob_fld  pat_ext_clob_fld,fax_no  pat_fax_no,fk_adev_record_id  pat_fk_adev_record_id,fk_apar_rec_id  pat_fk_apar_rec_id,fk_apd_rec_id  pat_fk_apd_rec_id,fk_aprg_record_id  pat_fk_aprg_record_id,fk_as_rec_id  pat_fk_as_rec_id,fk_pat_part_rec_id  pat_fk_pat_part_rec_id,gender  pat_gender,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10117' AND CD=CAST(gender AS VARCHAR(100)) )pat_gender_de_ml , gender_oth  pat_gender_oth,try_to_number(gestatationperiod,38)  pat_gestatationperiod,gestatationperiod  pat_gestatationperiod_1 ,gestatationperiod_lang  pat_gestatationperiod_lang,gestatationperiodunit  pat_gestatationperiodunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='11' AND CD=CAST(gestatationperiodunit AS VARCHAR(100)) )pat_gestatationperiodunit_de_ml , gestation_week_type  pat_gestation_week_type,hawaiian_or_pacific_islander  pat_hawaiian_or_pacific_islander,health_damage_type  pat_health_damage_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10064' AND CD=CAST(health_damage_type AS VARCHAR(100)) )pat_health_damage_type_de_ml , illness_at_time_of_vaccine  pat_illness_at_time_of_vaccine,inform_patient  pat_inform_patient,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(inform_patient AS VARCHAR(100)) )pat_inform_patient_de_ml , inq_rec_id  pat_inq_rec_id,is_patient_pregnant  pat_is_patient_pregnant,is_pregnant_code  pat_is_pregnant_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='2000' AND CD=CAST(is_pregnant_code AS VARCHAR(100)) )pat_is_pregnant_code_de_ml , lastmenstrualdatefmt  pat_lastmenstrualdatefmt,manual_age_entry  pat_manual_age_entry,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(manual_age_entry AS VARCHAR(100)) )pat_manual_age_entry_de_ml , medical_institution_cn  pat_medical_institution_cn,middle_name  pat_middle_name,middle_name_nf  pat_middle_name_nf,mother_exp_any_medical_problem  pat_mother_exp_any_medical_problem,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(mother_exp_any_medical_problem AS VARCHAR(100)) )pat_mother_exp_any_medical_problem_de_ml , narrative_generated_mh  pat_narrative_generated_mh,narrative_map_mh  pat_narrative_map_mh,nationalities_cn  pat_nationalities_cn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10078' AND CD=CAST(nationalities_cn AS VARCHAR(100)) )pat_nationalities_cn_de_ml , ok_to_contact_dr  pat_ok_to_contact_dr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8101' AND CD=CAST(ok_to_contact_dr AS VARCHAR(100)) )pat_ok_to_contact_dr_de_ml , ok_to_share_details  pat_ok_to_share_details,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8101' AND CD=CAST(ok_to_share_details AS VARCHAR(100)) )pat_ok_to_share_details_de_ml , other_sys_int_id1  pat_other_sys_int_id1,other_sys_int_id2  pat_other_sys_int_id2,parent_child_case  pat_parent_child_case,pat_medicalhistorytext_nf  pat_pat_medicalhistorytext_nf,pat_registration_no  pat_pat_registration_no,patagegroup  pat_patagegroup,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1006' AND CD=CAST(patagegroup AS VARCHAR(100)) )pat_patagegroup_de_ml , patagegroup_nf  pat_patagegroup_nf,patdob  pat_patdob,patdob_fmt  pat_patdob_fmt,patdob_nf  pat_patdob_nf,patdob_random_no  pat_patdob_random_no,patdob_rdm  pat_patdob_rdm,patdob_tz  pat_patdob_tz,patheight  pat_patheight,patheight_lang  pat_patheight_lang,patheight_unit_code  pat_patheight_unit_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='347' AND CD=CAST(patheight_unit_code AS VARCHAR(100)) )pat_patheight_unit_code_de_ml , pathospitalrecordnumb  pat_pathospitalrecordnumb,pathospitalrecordnumb_lang  pat_pathospitalrecordnumb_lang,pathospitalrecordnumb_nf  pat_pathospitalrecordnumb_nf,patient_age_in_year  pat_patient_age_in_year,patient_first_name  pat_patient_first_name,patient_first_name_nf  pat_patient_first_name_nf,patient_id  pat_patient_id,patient_identify  pat_patient_identify,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(patient_identify AS VARCHAR(100)) )pat_patient_identify_de_ml , patient_is_reporter  pat_patient_is_reporter,patient_lang  pat_patient_lang,patient_last_name  pat_patient_last_name,patient_last_name_nf  pat_patient_last_name_nf,patient_military_status  pat_patient_military_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9169' AND CD=CAST(patient_military_status AS VARCHAR(100)) )pat_patient_military_status_de_ml , patient_name  pat_patient_name,patient_pregnant  pat_patient_pregnant,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1008' AND CD=CAST(patient_pregnant AS VARCHAR(100)) )pat_patient_pregnant_de_ml , patient_type  pat_patient_type,patientinitial  pat_patientinitial,patientinitial_lang  pat_patientinitial_lang,patientinitial_nf  pat_patientinitial_nf,patinitial_type_code  pat_patinitial_type_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='372' AND CD=CAST(patinitial_type_code AS VARCHAR(100)) )pat_patinitial_type_code_de_ml , patinvestigationnumb  pat_patinvestigationnumb,patinvestigationnumb_lang  pat_patinvestigationnumb_lang,patinvestigationnumb_nf  pat_patinvestigationnumb_nf,patlastmenstrualdate  pat_patlastmenstrualdate,patlastmenstrualdate_nf  pat_patlastmenstrualdate_nf,patlastmenstrualdate_tz  pat_patlastmenstrualdate_tz,patmedicalhistorytext  pat_patmedicalhistorytext,patmedicalhistorytext_lang  pat_patmedicalhistorytext_lang,patmedicalhistorytext_nf  pat_patmedicalhistorytext_nf,patmedicalrecordnumb  pat_patmedicalrecordnumb,patmedicalrecordnumb_lang  pat_patmedicalrecordnumb_lang,patmedicalrecordnumb_nf  pat_patmedicalrecordnumb_nf,try_to_number(patonsetage,38)  pat_patonsetage,patonsetage  pat_patonsetage_1 ,patonsetage_lang  pat_patonsetage_lang,patonsetageunit  pat_patonsetageunit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(patonsetageunit AS VARCHAR(100)) )pat_patonsetageunit_de_ml , patsex  pat_patsex,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1007' AND CD=CAST(patsex AS VARCHAR(100)) )pat_patsex_de_ml , patsex_nf  pat_patsex_nf,patspecialistrecordnumb  pat_patspecialistrecordnumb,patspecialistrecordnumb_lang  pat_patspecialistrecordnumb_lang,patspecialistrecordnumb_nf  pat_patspecialistrecordnumb_nf,try_to_number(patweight,38)  pat_patweight,patweight  pat_patweight_1 ,patweight_lang  pat_patweight_lang,patweight_unit_code  pat_patweight_unit_code,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1026' AND CD=CAST(patweight_unit_code AS VARCHAR(100)) )pat_patweight_unit_code_de_ml , phone_no  pat_phone_no,phone_no_nf  pat_phone_no_nf,phone_number_cn  pat_phone_number_cn,phone_number_cn_nf  pat_phone_number_cn_nf,pregnancy_clinical_status  pat_pregnancy_clinical_status,pregnancy_confirm_date  pat_pregnancy_confirm_date,pregnancy_confirm_how_other  pat_pregnancy_confirm_how_other,pregnancy_confirm_mode  pat_pregnancy_confirm_mode,pregnancy_history_cn  pat_pregnancy_history_cn,pregnant_at_time_of_vaccine  pat_pregnant_at_time_of_vaccine,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(pregnant_at_time_of_vaccine AS VARCHAR(100)) )pat_pregnant_at_time_of_vaccine_de_ml , pregnant_at_time_of_vaccine_nf  pat_pregnant_at_time_of_vaccine_nf,primary_disease  pat_primary_disease,raceid_nf  pat_raceid_nf,reaction_type  pat_reaction_type,record_id  pat_record_id,registration_no  pat_registration_no,resultstestsprocedures  pat_resultstestsprocedures,resultstestsprocedures_lang  pat_resultstestsprocedures_lang,siebel_patient_rowid  pat_siebel_patient_rowid,spanish_state  pat_spanish_state,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='8147' AND CD=CAST(spanish_state AS VARCHAR(100)) )pat_spanish_state_de_ml , spr_id  pat_spr_id,state  pat_state,state_nf  pat_state_nf,street  pat_street,street_nf  pat_street_nf,study_program  pat_study_program,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='4' AND CD=CAST(study_program AS VARCHAR(100)) )pat_study_program_de_ml , study_subject_father  pat_study_subject_father,study_subject_mother  pat_study_subject_mother,subject_id  pat_subject_id,subject_id_nf  pat_subject_id_nf,title  pat_title,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5014' AND CD=CAST(title AS VARCHAR(100)) )pat_title_de_ml , title_nf  pat_title_nf,title_sf  pat_title_sf,torres_strait_islander  pat_torres_strait_islander,txt_pat_risk_factor_text  pat_txt_pat_risk_factor_text,user_created  pat_user_created,user_modified  pat_user_modified,vaccine_coded_flag  pat_vaccine_coded_flag,vaccine_coding_type  pat_vaccine_coding_type,vaccine_meddra_llt_code  pat_vaccine_meddra_llt_code,vaccine_meddra_pt_code  pat_vaccine_meddra_pt_code,version  pat_version,white  pat_white,withdrawn_date  pat_withdrawn_date,zip_code  pat_zip_code,zip_code_nf  pat_zip_code_nf,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_patient
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient')
  ) where REC_RANK=1 )
  , lsmv_patient_death_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  patdth_ari_rec_id,autopsy_done_manual  patdth_autopsy_done_manual,comp_rec_id  patdth_comp_rec_id,date_created  patdth_date_created,date_modified  patdth_date_modified,entity_updated  patdth_entity_updated,ext_clob_fld  patdth_ext_clob_fld,inq_rec_id  patdth_inq_rec_id,meddra_llt_code  patdth_meddra_llt_code,meddra_llt_decode  patdth_meddra_llt_decode,meddra_pt_code  patdth_meddra_pt_code,meddra_pt_decode  patdth_meddra_pt_decode,patautopsydate  patdth_patautopsydate,patautopsydatefmt  patdth_patautopsydatefmt,patautopsyyesno  patdth_patautopsyyesno,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(patautopsyyesno AS VARCHAR(100)) )patdth_patautopsyyesno_de_ml , patautopsyyesno_nf  patdth_patautopsyyesno_nf,patdeath_lang  patdth_patdeath_lang,patdeathdate  patdth_patdeathdate,patdeathdate_nf  patdth_patdeathdate_nf,patdeathdate_tz  patdth_patdeathdate_tz,patdeathdatefmt  patdth_patdeathdatefmt,record_id  patdth_record_id,spr_id  patdth_spr_id,user_created  patdth_user_created,user_modified  patdth_user_modified,version  patdth_version,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_patient_death
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_death')
  ) where REC_RANK=1 )
  , lsmv_patient_partner_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  patptnr_ari_rec_id,biological_father_age  patptnr_biological_father_age,biological_father_age_unit  patptnr_biological_father_age_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(biological_father_age_unit AS VARCHAR(100)) )patptnr_biological_father_age_unit_de_ml , comp_rec_id  patptnr_comp_rec_id,date_created  patptnr_date_created,date_modified  patptnr_date_modified,entity_updated  patptnr_entity_updated,inq_rec_id  patptnr_inq_rec_id,partner_contact_details  patptnr_partner_contact_details,partner_dob  patptnr_partner_dob,partner_dob_fmt  patptnr_partner_dob_fmt,partner_dob_rdm  patptnr_partner_dob_rdm,partner_initial  patptnr_partner_initial,partner_name  patptnr_partner_name,record_id  patptnr_record_id,spr_id  patptnr_spr_id,user_created  patptnr_user_created,user_modified  patptnr_user_modified,version  patptnr_version,row_number() OVER ( PARTITION BY ARI_REC_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_patient_partner
WHERE ( ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
ARI_REC_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_partner')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_patient_autopsy_SUBSET.patapsy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_patient_partner_SUBSET.patptnr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_patient_death_SUBSET.patdth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_patient_SUBSET.pat_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_parent_SUBSET.par_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_death_cause_SUBSET.dthcause_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO            ,lsmv_patient_SUBSET.pat_USER_MODIFIED USER_MODIFIED,lsmv_patient_SUBSET.pat_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE     ,lsmv_patient_SUBSET.pat_USER_CREATED CREATED_BY,lsmv_patient_SUBSET.pat_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_patient_SUBSET.pat_zip_code_nf  ,lsmv_patient_SUBSET.pat_zip_code  ,lsmv_patient_SUBSET.pat_withdrawn_date  ,lsmv_patient_SUBSET.pat_white  ,lsmv_patient_SUBSET.pat_version  ,lsmv_patient_SUBSET.pat_vaccine_meddra_pt_code  ,lsmv_patient_SUBSET.pat_vaccine_meddra_llt_code  ,lsmv_patient_SUBSET.pat_vaccine_coding_type  ,lsmv_patient_SUBSET.pat_vaccine_coded_flag  ,lsmv_patient_SUBSET.pat_user_modified  ,lsmv_patient_SUBSET.pat_user_created  ,lsmv_patient_SUBSET.pat_txt_pat_risk_factor_text  ,lsmv_patient_SUBSET.pat_torres_strait_islander  ,lsmv_patient_SUBSET.pat_title_sf  ,lsmv_patient_SUBSET.pat_title_nf  ,lsmv_patient_SUBSET.pat_title_de_ml  ,lsmv_patient_SUBSET.pat_title  ,lsmv_patient_SUBSET.pat_subject_id_nf  ,lsmv_patient_SUBSET.pat_subject_id  ,lsmv_patient_SUBSET.pat_study_subject_mother  ,lsmv_patient_SUBSET.pat_study_subject_father  ,lsmv_patient_SUBSET.pat_study_program_de_ml  ,lsmv_patient_SUBSET.pat_study_program  ,lsmv_patient_SUBSET.pat_street_nf  ,lsmv_patient_SUBSET.pat_street  ,lsmv_patient_SUBSET.pat_state_nf  ,lsmv_patient_SUBSET.pat_state  ,lsmv_patient_SUBSET.pat_spr_id  ,lsmv_patient_SUBSET.pat_spanish_state_de_ml  ,lsmv_patient_SUBSET.pat_spanish_state  ,lsmv_patient_SUBSET.pat_siebel_patient_rowid  ,lsmv_patient_SUBSET.pat_resultstestsprocedures_lang  ,lsmv_patient_SUBSET.pat_resultstestsprocedures  ,lsmv_patient_SUBSET.pat_registration_no  ,lsmv_patient_SUBSET.pat_record_id  ,lsmv_patient_SUBSET.pat_reaction_type  ,lsmv_patient_SUBSET.pat_raceid_nf  ,lsmv_patient_SUBSET.pat_primary_disease  ,lsmv_patient_SUBSET.pat_pregnant_at_time_of_vaccine_nf  ,lsmv_patient_SUBSET.pat_pregnant_at_time_of_vaccine_de_ml  ,lsmv_patient_SUBSET.pat_pregnant_at_time_of_vaccine  ,lsmv_patient_SUBSET.pat_pregnancy_history_cn  ,lsmv_patient_SUBSET.pat_pregnancy_confirm_mode  ,lsmv_patient_SUBSET.pat_pregnancy_confirm_how_other  ,lsmv_patient_SUBSET.pat_pregnancy_confirm_date  ,lsmv_patient_SUBSET.pat_pregnancy_clinical_status  ,lsmv_patient_SUBSET.pat_phone_number_cn_nf  ,lsmv_patient_SUBSET.pat_phone_number_cn  ,lsmv_patient_SUBSET.pat_phone_no_nf  ,lsmv_patient_SUBSET.pat_phone_no  ,lsmv_patient_SUBSET.pat_patweight_unit_code_de_ml  ,lsmv_patient_SUBSET.pat_patweight_unit_code  ,lsmv_patient_SUBSET.pat_patweight_lang  ,lsmv_patient_SUBSET.pat_patweight ,lsmv_patient_SUBSET.pat_patweight_1  ,lsmv_patient_SUBSET.pat_patspecialistrecordnumb_nf  ,lsmv_patient_SUBSET.pat_patspecialistrecordnumb_lang  ,lsmv_patient_SUBSET.pat_patspecialistrecordnumb  ,lsmv_patient_SUBSET.pat_patsex_nf  ,lsmv_patient_SUBSET.pat_patsex_de_ml  ,lsmv_patient_SUBSET.pat_patsex  ,lsmv_patient_SUBSET.pat_patonsetageunit_de_ml  ,lsmv_patient_SUBSET.pat_patonsetageunit  ,lsmv_patient_SUBSET.pat_patonsetage_lang  ,lsmv_patient_SUBSET.pat_patonsetage ,lsmv_patient_SUBSET.pat_patonsetage_1  ,lsmv_patient_SUBSET.pat_patmedicalrecordnumb_nf  ,lsmv_patient_SUBSET.pat_patmedicalrecordnumb_lang  ,lsmv_patient_SUBSET.pat_patmedicalrecordnumb  ,lsmv_patient_SUBSET.pat_patmedicalhistorytext_nf  ,lsmv_patient_SUBSET.pat_patmedicalhistorytext_lang  ,lsmv_patient_SUBSET.pat_patmedicalhistorytext  ,lsmv_patient_SUBSET.pat_patlastmenstrualdate_tz  ,lsmv_patient_SUBSET.pat_patlastmenstrualdate_nf  ,lsmv_patient_SUBSET.pat_patlastmenstrualdate  ,lsmv_patient_SUBSET.pat_patinvestigationnumb_nf  ,lsmv_patient_SUBSET.pat_patinvestigationnumb_lang  ,lsmv_patient_SUBSET.pat_patinvestigationnumb  ,lsmv_patient_SUBSET.pat_patinitial_type_code_de_ml  ,lsmv_patient_SUBSET.pat_patinitial_type_code  ,lsmv_patient_SUBSET.pat_patientinitial_nf  ,lsmv_patient_SUBSET.pat_patientinitial_lang  ,lsmv_patient_SUBSET.pat_patientinitial  ,lsmv_patient_SUBSET.pat_patient_type  ,lsmv_patient_SUBSET.pat_patient_pregnant_de_ml  ,lsmv_patient_SUBSET.pat_patient_pregnant  ,lsmv_patient_SUBSET.pat_patient_name  ,lsmv_patient_SUBSET.pat_patient_military_status_de_ml  ,lsmv_patient_SUBSET.pat_patient_military_status  ,lsmv_patient_SUBSET.pat_patient_last_name_nf  ,lsmv_patient_SUBSET.pat_patient_last_name  ,lsmv_patient_SUBSET.pat_patient_lang  ,lsmv_patient_SUBSET.pat_patient_is_reporter  ,lsmv_patient_SUBSET.pat_patient_identify_de_ml  ,lsmv_patient_SUBSET.pat_patient_identify  ,lsmv_patient_SUBSET.pat_patient_id  ,lsmv_patient_SUBSET.pat_patient_first_name_nf  ,lsmv_patient_SUBSET.pat_patient_first_name  ,lsmv_patient_SUBSET.pat_patient_age_in_year  ,lsmv_patient_SUBSET.pat_pathospitalrecordnumb_nf  ,lsmv_patient_SUBSET.pat_pathospitalrecordnumb_lang  ,lsmv_patient_SUBSET.pat_pathospitalrecordnumb  ,lsmv_patient_SUBSET.pat_patheight_unit_code_de_ml  ,lsmv_patient_SUBSET.pat_patheight_unit_code  ,lsmv_patient_SUBSET.pat_patheight_lang  ,lsmv_patient_SUBSET.pat_patheight  ,lsmv_patient_SUBSET.pat_patdob_tz  ,lsmv_patient_SUBSET.pat_patdob_rdm  ,lsmv_patient_SUBSET.pat_patdob_random_no  ,lsmv_patient_SUBSET.pat_patdob_nf  ,lsmv_patient_SUBSET.pat_patdob_fmt  ,lsmv_patient_SUBSET.pat_patdob  ,lsmv_patient_SUBSET.pat_patagegroup_nf  ,lsmv_patient_SUBSET.pat_patagegroup_de_ml  ,lsmv_patient_SUBSET.pat_patagegroup  ,lsmv_patient_SUBSET.pat_pat_registration_no  ,lsmv_patient_SUBSET.pat_pat_medicalhistorytext_nf  ,lsmv_patient_SUBSET.pat_parent_child_case  ,lsmv_patient_SUBSET.pat_other_sys_int_id2  ,lsmv_patient_SUBSET.pat_other_sys_int_id1  ,lsmv_patient_SUBSET.pat_ok_to_share_details_de_ml  ,lsmv_patient_SUBSET.pat_ok_to_share_details  ,lsmv_patient_SUBSET.pat_ok_to_contact_dr_de_ml  ,lsmv_patient_SUBSET.pat_ok_to_contact_dr  ,lsmv_patient_SUBSET.pat_nationalities_cn_de_ml  ,lsmv_patient_SUBSET.pat_nationalities_cn  ,lsmv_patient_SUBSET.pat_narrative_map_mh  ,lsmv_patient_SUBSET.pat_narrative_generated_mh  ,lsmv_patient_SUBSET.pat_mother_exp_any_medical_problem_de_ml  ,lsmv_patient_SUBSET.pat_mother_exp_any_medical_problem  ,lsmv_patient_SUBSET.pat_middle_name_nf  ,lsmv_patient_SUBSET.pat_middle_name  ,lsmv_patient_SUBSET.pat_medical_institution_cn  ,lsmv_patient_SUBSET.pat_manual_age_entry_de_ml  ,lsmv_patient_SUBSET.pat_manual_age_entry  ,lsmv_patient_SUBSET.pat_lastmenstrualdatefmt  ,lsmv_patient_SUBSET.pat_is_pregnant_code_de_ml  ,lsmv_patient_SUBSET.pat_is_pregnant_code  ,lsmv_patient_SUBSET.pat_is_patient_pregnant  ,lsmv_patient_SUBSET.pat_inq_rec_id  ,lsmv_patient_SUBSET.pat_inform_patient_de_ml  ,lsmv_patient_SUBSET.pat_inform_patient  ,lsmv_patient_SUBSET.pat_illness_at_time_of_vaccine  ,lsmv_patient_SUBSET.pat_health_damage_type_de_ml  ,lsmv_patient_SUBSET.pat_health_damage_type  ,lsmv_patient_SUBSET.pat_hawaiian_or_pacific_islander  ,lsmv_patient_SUBSET.pat_gestation_week_type  ,lsmv_patient_SUBSET.pat_gestatationperiodunit_de_ml  ,lsmv_patient_SUBSET.pat_gestatationperiodunit  ,lsmv_patient_SUBSET.pat_gestatationperiod_lang  ,lsmv_patient_SUBSET.pat_gestatationperiod ,lsmv_patient_SUBSET.pat_gestatationperiod_1  ,lsmv_patient_SUBSET.pat_gender_oth  ,lsmv_patient_SUBSET.pat_gender_de_ml  ,lsmv_patient_SUBSET.pat_gender  ,lsmv_patient_SUBSET.pat_fk_pat_part_rec_id  ,lsmv_patient_SUBSET.pat_fk_as_rec_id  ,lsmv_patient_SUBSET.pat_fk_aprg_record_id  ,lsmv_patient_SUBSET.pat_fk_apd_rec_id  ,lsmv_patient_SUBSET.pat_fk_apar_rec_id  ,lsmv_patient_SUBSET.pat_fk_adev_record_id  ,lsmv_patient_SUBSET.pat_fax_no  ,lsmv_patient_SUBSET.pat_ext_clob_fld  ,lsmv_patient_SUBSET.pat_expectedduedate_fmt  ,lsmv_patient_SUBSET.pat_expectedduedate  ,lsmv_patient_SUBSET.pat_event_date_tz  ,lsmv_patient_SUBSET.pat_event_date_fmt  ,lsmv_patient_SUBSET.pat_event_date  ,lsmv_patient_SUBSET.pat_ethnic_origin_de_ml  ,lsmv_patient_SUBSET.pat_ethnic_origin_cn_de_ml  ,lsmv_patient_SUBSET.pat_ethnic_origin_cn  ,lsmv_patient_SUBSET.pat_ethnic_origin  ,lsmv_patient_SUBSET.pat_ethenic_origin_nf  ,lsmv_patient_SUBSET.pat_entity_updated  ,lsmv_patient_SUBSET.pat_enrolment_status  ,lsmv_patient_SUBSET.pat_enrolment_date  ,lsmv_patient_SUBSET.pat_email_id_nf  ,lsmv_patient_SUBSET.pat_email_id  ,lsmv_patient_SUBSET.pat_e2b_r3_gestperiodoverride  ,lsmv_patient_SUBSET.pat_drug_type  ,lsmv_patient_SUBSET.pat_donotreportname_de_ml  ,lsmv_patient_SUBSET.pat_donotreportname  ,lsmv_patient_SUBSET.pat_dob_unknown_code_de_ml  ,lsmv_patient_SUBSET.pat_dob_unknown_code  ,lsmv_patient_SUBSET.pat_do_not_use_ang_mh  ,lsmv_patient_SUBSET.pat_device_problem_type  ,lsmv_patient_SUBSET.pat_date_modified  ,lsmv_patient_SUBSET.pat_date_created  ,lsmv_patient_SUBSET.pat_dataprivacy_present  ,lsmv_patient_SUBSET.pat_data_encrypted  ,lsmv_patient_SUBSET.pat_county_nf  ,lsmv_patient_SUBSET.pat_county  ,lsmv_patient_SUBSET.pat_country_nf  ,lsmv_patient_SUBSET.pat_country_cn_de_ml  ,lsmv_patient_SUBSET.pat_country_cn  ,lsmv_patient_SUBSET.pat_country  ,lsmv_patient_SUBSET.pat_contact_info_avail  ,lsmv_patient_SUBSET.pat_concomitant_therapies_de_ml  ,lsmv_patient_SUBSET.pat_concomitant_therapies  ,lsmv_patient_SUBSET.pat_concomitant_product  ,lsmv_patient_SUBSET.pat_comp_rec_id  ,lsmv_patient_SUBSET.pat_coding_comments  ,lsmv_patient_SUBSET.pat_city_nf  ,lsmv_patient_SUBSET.pat_city  ,lsmv_patient_SUBSET.pat_body_surface_index_type  ,lsmv_patient_SUBSET.pat_body_surface_index  ,lsmv_patient_SUBSET.pat_body_mass_index_type  ,lsmv_patient_SUBSET.pat_body_mass_index  ,lsmv_patient_SUBSET.pat_black_african_american  ,lsmv_patient_SUBSET.pat_birth_weight_unit_de_ml  ,lsmv_patient_SUBSET.pat_birth_weight_unit  ,lsmv_patient_SUBSET.pat_birth_weight  ,lsmv_patient_SUBSET.pat_asian  ,lsmv_patient_SUBSET.pat_ari_rec_id  ,lsmv_patient_SUBSET.pat_any_relevant_info_from_partner_de_ml  ,lsmv_patient_SUBSET.pat_any_relevant_info_from_partner  ,lsmv_patient_SUBSET.pat_american_ind_or_alaskan  ,lsmv_patient_SUBSET.pat_allergies  ,lsmv_patient_SUBSET.pat_age_group_manual  ,lsmv_patient_SUBSET.pat_age_at_time_of_vaccine_unit_de_ml  ,lsmv_patient_SUBSET.pat_age_at_time_of_vaccine_unit  ,lsmv_patient_SUBSET.pat_age_at_time_of_vaccine  ,lsmv_patient_SUBSET.pat_address_nf  ,lsmv_patient_SUBSET.pat_address2_nf  ,lsmv_patient_SUBSET.pat_address2  ,lsmv_patient_SUBSET.pat_address1_nf  ,lsmv_patient_SUBSET.pat_address1  ,lsmv_patient_SUBSET.pat_address  ,lsmv_patient_SUBSET.pat_aborginal  ,lsmv_patient_partner_SUBSET.patptnr_version  ,lsmv_patient_partner_SUBSET.patptnr_user_modified  ,lsmv_patient_partner_SUBSET.patptnr_user_created  ,lsmv_patient_partner_SUBSET.patptnr_spr_id  ,lsmv_patient_partner_SUBSET.patptnr_record_id  ,lsmv_patient_partner_SUBSET.patptnr_partner_name  ,lsmv_patient_partner_SUBSET.patptnr_partner_initial  ,lsmv_patient_partner_SUBSET.patptnr_partner_dob_rdm  ,lsmv_patient_partner_SUBSET.patptnr_partner_dob_fmt  ,lsmv_patient_partner_SUBSET.patptnr_partner_dob  ,lsmv_patient_partner_SUBSET.patptnr_partner_contact_details  ,lsmv_patient_partner_SUBSET.patptnr_inq_rec_id  ,lsmv_patient_partner_SUBSET.patptnr_entity_updated  ,lsmv_patient_partner_SUBSET.patptnr_date_modified  ,lsmv_patient_partner_SUBSET.patptnr_date_created  ,lsmv_patient_partner_SUBSET.patptnr_comp_rec_id  ,lsmv_patient_partner_SUBSET.patptnr_biological_father_age_unit_de_ml  ,lsmv_patient_partner_SUBSET.patptnr_biological_father_age_unit  ,lsmv_patient_partner_SUBSET.patptnr_biological_father_age  ,lsmv_patient_partner_SUBSET.patptnr_ari_rec_id  ,lsmv_patient_death_SUBSET.patdth_version  ,lsmv_patient_death_SUBSET.patdth_user_modified  ,lsmv_patient_death_SUBSET.patdth_user_created  ,lsmv_patient_death_SUBSET.patdth_spr_id  ,lsmv_patient_death_SUBSET.patdth_record_id  ,lsmv_patient_death_SUBSET.patdth_patdeathdatefmt  ,lsmv_patient_death_SUBSET.patdth_patdeathdate_tz  ,lsmv_patient_death_SUBSET.patdth_patdeathdate_nf  ,lsmv_patient_death_SUBSET.patdth_patdeathdate  ,lsmv_patient_death_SUBSET.patdth_patdeath_lang  ,lsmv_patient_death_SUBSET.patdth_patautopsyyesno_nf  ,lsmv_patient_death_SUBSET.patdth_patautopsyyesno_de_ml  ,lsmv_patient_death_SUBSET.patdth_patautopsyyesno  ,lsmv_patient_death_SUBSET.patdth_patautopsydatefmt  ,lsmv_patient_death_SUBSET.patdth_patautopsydate  ,lsmv_patient_death_SUBSET.patdth_meddra_pt_decode  ,lsmv_patient_death_SUBSET.patdth_meddra_pt_code  ,lsmv_patient_death_SUBSET.patdth_meddra_llt_decode  ,lsmv_patient_death_SUBSET.patdth_meddra_llt_code  ,lsmv_patient_death_SUBSET.patdth_inq_rec_id  ,lsmv_patient_death_SUBSET.patdth_ext_clob_fld  ,lsmv_patient_death_SUBSET.patdth_entity_updated  ,lsmv_patient_death_SUBSET.patdth_date_modified  ,lsmv_patient_death_SUBSET.patdth_date_created  ,lsmv_patient_death_SUBSET.patdth_comp_rec_id  ,lsmv_patient_death_SUBSET.patdth_autopsy_done_manual  ,lsmv_patient_death_SUBSET.patdth_ari_rec_id  ,lsmv_patient_autopsy_SUBSET.patapsy_version  ,lsmv_patient_autopsy_SUBSET.patapsy_user_modified  ,lsmv_patient_autopsy_SUBSET.patapsy_user_created  ,lsmv_patient_autopsy_SUBSET.patapsy_spr_id  ,lsmv_patient_autopsy_SUBSET.patapsy_record_id  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsymeddraver_lang  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsymeddraver  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsylevel  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_ptcode ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_ptcode_1  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_lang  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_decode  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_code ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy_code_1  ,lsmv_patient_autopsy_SUBSET.patapsy_patdetautopsy  ,lsmv_patient_autopsy_SUBSET.patapsy_patautopsy_lang  ,lsmv_patient_autopsy_SUBSET.patapsy_inq_rec_id  ,lsmv_patient_autopsy_SUBSET.patapsy_fk_apd_rec_id  ,lsmv_patient_autopsy_SUBSET.patapsy_ext_clob_fld  ,lsmv_patient_autopsy_SUBSET.patapsy_entity_updated  ,lsmv_patient_autopsy_SUBSET.patapsy_date_modified  ,lsmv_patient_autopsy_SUBSET.patapsy_date_created  ,lsmv_patient_autopsy_SUBSET.patapsy_comp_rec_id  ,lsmv_patient_autopsy_SUBSET.patapsy_coding_type  ,lsmv_patient_autopsy_SUBSET.patapsy_coding_comments  ,lsmv_patient_autopsy_SUBSET.patapsy_autopsycause_coded_flag  ,lsmv_patient_autopsy_SUBSET.patapsy_ari_rec_id  ,lsmv_parent_SUBSET.par_version  ,lsmv_parent_SUBSET.par_user_modified  ,lsmv_parent_SUBSET.par_user_created  ,lsmv_parent_SUBSET.par_spr_id  ,lsmv_parent_SUBSET.par_record_id  ,lsmv_parent_SUBSET.par_paternal_exposure  ,lsmv_parent_SUBSET.par_parentweight_unit_de_ml  ,lsmv_parent_SUBSET.par_parentweight_unit  ,lsmv_parent_SUBSET.par_parentweight_lang  ,lsmv_parent_SUBSET.par_parentweight ,lsmv_parent_SUBSET.par_parentweight_1  ,lsmv_parent_SUBSET.par_parentsex_nf  ,lsmv_parent_SUBSET.par_parentsex_de_ml  ,lsmv_parent_SUBSET.par_parentsex  ,lsmv_parent_SUBSET.par_parentmedrelevanttext_lang  ,lsmv_parent_SUBSET.par_parentmedrelevanttext  ,lsmv_parent_SUBSET.par_parentlastmensdatefmt  ,lsmv_parent_SUBSET.par_parentlastmensdate_tz  ,lsmv_parent_SUBSET.par_parentlastmensdate_nf  ,lsmv_parent_SUBSET.par_parentlastmensdate  ,lsmv_parent_SUBSET.par_parentidentification_nf  ,lsmv_parent_SUBSET.par_parentidentification_lang  ,lsmv_parent_SUBSET.par_parentidentification  ,lsmv_parent_SUBSET.par_parentheight_unit_de_ml  ,lsmv_parent_SUBSET.par_parentheight_unit  ,lsmv_parent_SUBSET.par_parentheight_lang  ,lsmv_parent_SUBSET.par_parentheight  ,lsmv_parent_SUBSET.par_parentdobfmt  ,lsmv_parent_SUBSET.par_parentdob_tz  ,lsmv_parent_SUBSET.par_parentdob_rdm  ,lsmv_parent_SUBSET.par_parentdob_nf  ,lsmv_parent_SUBSET.par_parentdob  ,lsmv_parent_SUBSET.par_parentageunit_de_ml  ,lsmv_parent_SUBSET.par_parentageunit  ,lsmv_parent_SUBSET.par_parentage_lang  ,lsmv_parent_SUBSET.par_parentage ,lsmv_parent_SUBSET.par_parentage_1  ,lsmv_parent_SUBSET.par_parent_lang  ,lsmv_parent_SUBSET.par_parent_age_at_vaccine_unit_de_ml  ,lsmv_parent_SUBSET.par_parent_age_at_vaccine_unit  ,lsmv_parent_SUBSET.par_parent_age_at_vaccine  ,lsmv_parent_SUBSET.par_par_ethnic_origin_de_ml  ,lsmv_parent_SUBSET.par_par_ethnic_origin  ,lsmv_parent_SUBSET.par_inq_rec_id  ,lsmv_parent_SUBSET.par_ext_clob_fld  ,lsmv_parent_SUBSET.par_entity_updated  ,lsmv_parent_SUBSET.par_duration_unit  ,lsmv_parent_SUBSET.par_duration  ,lsmv_parent_SUBSET.par_date_modified  ,lsmv_parent_SUBSET.par_date_created  ,lsmv_parent_SUBSET.par_consent_to_contact_parent_de_ml  ,lsmv_parent_SUBSET.par_consent_to_contact_parent  ,lsmv_parent_SUBSET.par_comp_rec_id  ,lsmv_parent_SUBSET.par_ari_rec_id  ,lsmv_death_cause_SUBSET.dthcause_version  ,lsmv_death_cause_SUBSET.dthcause_uuid  ,lsmv_death_cause_SUBSET.dthcause_user_modified  ,lsmv_death_cause_SUBSET.dthcause_user_created  ,lsmv_death_cause_SUBSET.dthcause_spr_id  ,lsmv_death_cause_SUBSET.dthcause_record_id  ,lsmv_death_cause_SUBSET.dthcause_patdeathreportlevel  ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_ptcode ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_ptcode_1  ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_lang  ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_decode  ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_code ,lsmv_death_cause_SUBSET.dthcause_patdeathreport_code_1  ,lsmv_death_cause_SUBSET.dthcause_patdeathreport  ,lsmv_death_cause_SUBSET.dthcause_patdeathrepmeddraver_lang  ,lsmv_death_cause_SUBSET.dthcause_patdeathrepmeddraver  ,lsmv_death_cause_SUBSET.dthcause_patdeathcause_lang  ,lsmv_death_cause_SUBSET.dthcause_inq_rec_id  ,lsmv_death_cause_SUBSET.dthcause_fk_apd_rec_id  ,lsmv_death_cause_SUBSET.dthcause_ext_clob_fld  ,lsmv_death_cause_SUBSET.dthcause_entity_updated  ,lsmv_death_cause_SUBSET.dthcause_deathcause_coded_flag  ,lsmv_death_cause_SUBSET.dthcause_date_modified  ,lsmv_death_cause_SUBSET.dthcause_date_created  ,lsmv_death_cause_SUBSET.dthcause_comp_rec_id  ,lsmv_death_cause_SUBSET.dthcause_coding_type  ,lsmv_death_cause_SUBSET.dthcause_coding_comments  ,lsmv_death_cause_SUBSET.dthcause_ari_rec_id ,CONCAT( NVL(lsmv_patient_autopsy_SUBSET.patapsy_RECORD_ID,-1),'||',NVL(lsmv_patient_partner_SUBSET.patptnr_RECORD_ID,-1),'||',NVL(lsmv_patient_death_SUBSET.patdth_RECORD_ID,-1),'||',NVL(lsmv_patient_SUBSET.pat_RECORD_ID,-1),'||',NVL(lsmv_parent_SUBSET.par_RECORD_ID,-1),'||',NVL(lsmv_death_cause_SUBSET.dthcause_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_patient_SUBSET  LEFT JOIN lsmv_patient_partner_SUBSET ON lsmv_patient_SUBSET.pat_ARI_REC_ID=lsmv_patient_partner_SUBSET.patptnr_ARI_REC_ID
                         LEFT JOIN lsmv_parent_SUBSET ON lsmv_patient_SUBSET.pat_ARI_REC_ID=lsmv_parent_SUBSET.par_ARI_REC_ID
                         LEFT JOIN lsmv_patient_death_SUBSET ON lsmv_patient_SUBSET.pat_ARI_REC_ID=lsmv_patient_death_SUBSET.patdth_ARI_REC_ID
                         LEFT JOIN lsmv_patient_autopsy_SUBSET ON lsmv_patient_death_SUBSET.patdth_ARI_REC_ID=lsmv_patient_autopsy_SUBSET.patapsy_ARI_REC_ID
                         LEFT JOIN lsmv_death_cause_SUBSET ON lsmv_patient_death_SUBSET.patdth_ARI_REC_ID=lsmv_death_cause_SUBSET.dthcause_ARI_REC_ID
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_patient_autopsy_SUBSET.patapsy_ARI_REC_ID,lsmv_patient_partner_SUBSET.patptnr_ARI_REC_ID,lsmv_patient_death_SUBSET.patdth_ARI_REC_ID,lsmv_patient_SUBSET.pat_ARI_REC_ID,lsmv_parent_SUBSET.par_ARI_REC_ID,lsmv_death_cause_SUBSET.dthcause_ARI_REC_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP)
where target_table_name='LS_DB_PATIENT'

; 


        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patweight' ,
pat_patweight_1,
pat_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patweight'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (pat_patweight is null and pat_patweight_1 is not null)
and pat_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patonsetage' ,
pat_patonsetage_1,
pat_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patonsetage'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (pat_patonsetage is null and pat_patonsetage_1 is not null)
and pat_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'gestatationperiod' ,
pat_gestatationperiod_1,
pat_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on gestatationperiod'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (pat_gestatationperiod is null and pat_gestatationperiod_1 is not null)
and pat_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient_autopsy' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patdetautopsy_ptcode' ,
patapsy_patdetautopsy_ptcode_1,
patapsy_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patdetautopsy_ptcode'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (patapsy_patdetautopsy_ptcode is null and patapsy_patdetautopsy_ptcode_1 is not null)
and patapsy_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_patient_autopsy' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patdetautopsy_code' ,
patapsy_patdetautopsy_code_1,
patapsy_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patdetautopsy_code'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (patapsy_patdetautopsy_code is null and patapsy_patdetautopsy_code_1 is not null)
and patapsy_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_parent' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'parentweight' ,
par_parentweight_1,
par_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on parentweight'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (par_parentweight is null and par_parentweight_1 is not null)
and par_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_parent' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'parentage' ,
par_parentage_1,
par_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on parentage'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (par_parentage is null and par_parentage_1 is not null)
and par_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_death_cause' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patdeathreport_ptcode' ,
dthcause_patdeathreport_ptcode_1,
dthcause_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patdeathreport_ptcode'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (dthcause_patdeathreport_ptcode is null and dthcause_patdeathreport_ptcode_1 is not null)
and dthcause_ARI_REC_ID is not null 
and CASE_NO is not null;

        INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_VALIDATION_TBL
SELECT 
'Lsmv' FUNCTIONAL_AREA,
'Case' ENTITY_NAME,
'lsmv_death_cause' SRC_TABLE_NAME, 
:CURRENT_TS_VAR LOAD_TS, 
'patdeathreport_code' ,
dthcause_patdeathreport_code_1,
dthcause_RECORD_ID,
CASE_NO,
'Expected Number,Received Character value on patdeathreport_code'
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (dthcause_patdeathreport_code is null and dthcause_patdeathreport_code_1 is not null)
and dthcause_ARI_REC_ID is not null 
and CASE_NO is not null;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP
WHERE (dthcause_patdeathreport_code is null and dthcause_patdeathreport_code_1 is not null) or (dthcause_patdeathreport_ptcode is null and dthcause_patdeathreport_ptcode_1 is not null) or (par_parentage is null and par_parentage_1 is not null) or (par_parentweight is null and par_parentweight_1 is not null) or (patapsy_patdetautopsy_code is null and patapsy_patdetautopsy_code_1 is not null) or (patapsy_patdetautopsy_ptcode is null and patapsy_patdetautopsy_ptcode_1 is not null) or (pat_gestatationperiod is null and pat_gestatationperiod_1 is not null) or (pat_patonsetage is null and pat_patonsetage_1 is not null) or (pat_patweight is null and pat_patweight_1 is not null) 
and CASE_NO is not null;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT   
SET LS_DB_PATIENT.pat_zip_code_nf = LS_DB_PATIENT_TMP.pat_zip_code_nf,LS_DB_PATIENT.pat_zip_code = LS_DB_PATIENT_TMP.pat_zip_code,LS_DB_PATIENT.pat_withdrawn_date = LS_DB_PATIENT_TMP.pat_withdrawn_date,LS_DB_PATIENT.pat_white = LS_DB_PATIENT_TMP.pat_white,LS_DB_PATIENT.pat_version = LS_DB_PATIENT_TMP.pat_version,LS_DB_PATIENT.pat_vaccine_meddra_pt_code = LS_DB_PATIENT_TMP.pat_vaccine_meddra_pt_code,LS_DB_PATIENT.pat_vaccine_meddra_llt_code = LS_DB_PATIENT_TMP.pat_vaccine_meddra_llt_code,LS_DB_PATIENT.pat_vaccine_coding_type = LS_DB_PATIENT_TMP.pat_vaccine_coding_type,LS_DB_PATIENT.pat_vaccine_coded_flag = LS_DB_PATIENT_TMP.pat_vaccine_coded_flag,LS_DB_PATIENT.pat_user_modified = LS_DB_PATIENT_TMP.pat_user_modified,LS_DB_PATIENT.pat_user_created = LS_DB_PATIENT_TMP.pat_user_created,LS_DB_PATIENT.pat_txt_pat_risk_factor_text = LS_DB_PATIENT_TMP.pat_txt_pat_risk_factor_text,LS_DB_PATIENT.pat_torres_strait_islander = LS_DB_PATIENT_TMP.pat_torres_strait_islander,LS_DB_PATIENT.pat_title_sf = LS_DB_PATIENT_TMP.pat_title_sf,LS_DB_PATIENT.pat_title_nf = LS_DB_PATIENT_TMP.pat_title_nf,LS_DB_PATIENT.pat_title_de_ml = LS_DB_PATIENT_TMP.pat_title_de_ml,LS_DB_PATIENT.pat_title = LS_DB_PATIENT_TMP.pat_title,LS_DB_PATIENT.pat_subject_id_nf = LS_DB_PATIENT_TMP.pat_subject_id_nf,LS_DB_PATIENT.pat_subject_id = LS_DB_PATIENT_TMP.pat_subject_id,LS_DB_PATIENT.pat_study_subject_mother = LS_DB_PATIENT_TMP.pat_study_subject_mother,LS_DB_PATIENT.pat_study_subject_father = LS_DB_PATIENT_TMP.pat_study_subject_father,LS_DB_PATIENT.pat_study_program_de_ml = LS_DB_PATIENT_TMP.pat_study_program_de_ml,LS_DB_PATIENT.pat_study_program = LS_DB_PATIENT_TMP.pat_study_program,LS_DB_PATIENT.pat_street_nf = LS_DB_PATIENT_TMP.pat_street_nf,LS_DB_PATIENT.pat_street = LS_DB_PATIENT_TMP.pat_street,LS_DB_PATIENT.pat_state_nf = LS_DB_PATIENT_TMP.pat_state_nf,LS_DB_PATIENT.pat_state = LS_DB_PATIENT_TMP.pat_state,LS_DB_PATIENT.pat_spr_id = LS_DB_PATIENT_TMP.pat_spr_id,LS_DB_PATIENT.pat_spanish_state_de_ml = LS_DB_PATIENT_TMP.pat_spanish_state_de_ml,LS_DB_PATIENT.pat_spanish_state = LS_DB_PATIENT_TMP.pat_spanish_state,LS_DB_PATIENT.pat_siebel_patient_rowid = LS_DB_PATIENT_TMP.pat_siebel_patient_rowid,LS_DB_PATIENT.pat_resultstestsprocedures_lang = LS_DB_PATIENT_TMP.pat_resultstestsprocedures_lang,LS_DB_PATIENT.pat_resultstestsprocedures = LS_DB_PATIENT_TMP.pat_resultstestsprocedures,LS_DB_PATIENT.pat_registration_no = LS_DB_PATIENT_TMP.pat_registration_no,LS_DB_PATIENT.pat_record_id = LS_DB_PATIENT_TMP.pat_record_id,LS_DB_PATIENT.pat_reaction_type = LS_DB_PATIENT_TMP.pat_reaction_type,LS_DB_PATIENT.pat_raceid_nf = LS_DB_PATIENT_TMP.pat_raceid_nf,LS_DB_PATIENT.pat_primary_disease = LS_DB_PATIENT_TMP.pat_primary_disease,LS_DB_PATIENT.pat_pregnant_at_time_of_vaccine_nf = LS_DB_PATIENT_TMP.pat_pregnant_at_time_of_vaccine_nf,LS_DB_PATIENT.pat_pregnant_at_time_of_vaccine_de_ml = LS_DB_PATIENT_TMP.pat_pregnant_at_time_of_vaccine_de_ml,LS_DB_PATIENT.pat_pregnant_at_time_of_vaccine = LS_DB_PATIENT_TMP.pat_pregnant_at_time_of_vaccine,LS_DB_PATIENT.pat_pregnancy_history_cn = LS_DB_PATIENT_TMP.pat_pregnancy_history_cn,LS_DB_PATIENT.pat_pregnancy_confirm_mode = LS_DB_PATIENT_TMP.pat_pregnancy_confirm_mode,LS_DB_PATIENT.pat_pregnancy_confirm_how_other = LS_DB_PATIENT_TMP.pat_pregnancy_confirm_how_other,LS_DB_PATIENT.pat_pregnancy_confirm_date = LS_DB_PATIENT_TMP.pat_pregnancy_confirm_date,LS_DB_PATIENT.pat_pregnancy_clinical_status = LS_DB_PATIENT_TMP.pat_pregnancy_clinical_status,LS_DB_PATIENT.pat_phone_number_cn_nf = LS_DB_PATIENT_TMP.pat_phone_number_cn_nf,LS_DB_PATIENT.pat_phone_number_cn = LS_DB_PATIENT_TMP.pat_phone_number_cn,LS_DB_PATIENT.pat_phone_no_nf = LS_DB_PATIENT_TMP.pat_phone_no_nf,LS_DB_PATIENT.pat_phone_no = LS_DB_PATIENT_TMP.pat_phone_no,LS_DB_PATIENT.pat_patweight_unit_code_de_ml = LS_DB_PATIENT_TMP.pat_patweight_unit_code_de_ml,LS_DB_PATIENT.pat_patweight_unit_code = LS_DB_PATIENT_TMP.pat_patweight_unit_code,LS_DB_PATIENT.pat_patweight_lang = LS_DB_PATIENT_TMP.pat_patweight_lang,LS_DB_PATIENT.pat_patweight = LS_DB_PATIENT_TMP.pat_patweight,LS_DB_PATIENT.pat_patspecialistrecordnumb_nf = LS_DB_PATIENT_TMP.pat_patspecialistrecordnumb_nf,LS_DB_PATIENT.pat_patspecialistrecordnumb_lang = LS_DB_PATIENT_TMP.pat_patspecialistrecordnumb_lang,LS_DB_PATIENT.pat_patspecialistrecordnumb = LS_DB_PATIENT_TMP.pat_patspecialistrecordnumb,LS_DB_PATIENT.pat_patsex_nf = LS_DB_PATIENT_TMP.pat_patsex_nf,LS_DB_PATIENT.pat_patsex_de_ml = LS_DB_PATIENT_TMP.pat_patsex_de_ml,LS_DB_PATIENT.pat_patsex = LS_DB_PATIENT_TMP.pat_patsex,LS_DB_PATIENT.pat_patonsetageunit_de_ml = LS_DB_PATIENT_TMP.pat_patonsetageunit_de_ml,LS_DB_PATIENT.pat_patonsetageunit = LS_DB_PATIENT_TMP.pat_patonsetageunit,LS_DB_PATIENT.pat_patonsetage_lang = LS_DB_PATIENT_TMP.pat_patonsetage_lang,LS_DB_PATIENT.pat_patonsetage = LS_DB_PATIENT_TMP.pat_patonsetage,LS_DB_PATIENT.pat_patmedicalrecordnumb_nf = LS_DB_PATIENT_TMP.pat_patmedicalrecordnumb_nf,LS_DB_PATIENT.pat_patmedicalrecordnumb_lang = LS_DB_PATIENT_TMP.pat_patmedicalrecordnumb_lang,LS_DB_PATIENT.pat_patmedicalrecordnumb = LS_DB_PATIENT_TMP.pat_patmedicalrecordnumb,LS_DB_PATIENT.pat_patmedicalhistorytext_nf = LS_DB_PATIENT_TMP.pat_patmedicalhistorytext_nf,LS_DB_PATIENT.pat_patmedicalhistorytext_lang = LS_DB_PATIENT_TMP.pat_patmedicalhistorytext_lang,LS_DB_PATIENT.pat_patmedicalhistorytext = LS_DB_PATIENT_TMP.pat_patmedicalhistorytext,LS_DB_PATIENT.pat_patlastmenstrualdate_tz = LS_DB_PATIENT_TMP.pat_patlastmenstrualdate_tz,LS_DB_PATIENT.pat_patlastmenstrualdate_nf = LS_DB_PATIENT_TMP.pat_patlastmenstrualdate_nf,LS_DB_PATIENT.pat_patlastmenstrualdate = LS_DB_PATIENT_TMP.pat_patlastmenstrualdate,LS_DB_PATIENT.pat_patinvestigationnumb_nf = LS_DB_PATIENT_TMP.pat_patinvestigationnumb_nf,LS_DB_PATIENT.pat_patinvestigationnumb_lang = LS_DB_PATIENT_TMP.pat_patinvestigationnumb_lang,LS_DB_PATIENT.pat_patinvestigationnumb = LS_DB_PATIENT_TMP.pat_patinvestigationnumb,LS_DB_PATIENT.pat_patinitial_type_code_de_ml = LS_DB_PATIENT_TMP.pat_patinitial_type_code_de_ml,LS_DB_PATIENT.pat_patinitial_type_code = LS_DB_PATIENT_TMP.pat_patinitial_type_code,LS_DB_PATIENT.pat_patientinitial_nf = LS_DB_PATIENT_TMP.pat_patientinitial_nf,LS_DB_PATIENT.pat_patientinitial_lang = LS_DB_PATIENT_TMP.pat_patientinitial_lang,LS_DB_PATIENT.pat_patientinitial = LS_DB_PATIENT_TMP.pat_patientinitial,LS_DB_PATIENT.pat_patient_type = LS_DB_PATIENT_TMP.pat_patient_type,LS_DB_PATIENT.pat_patient_pregnant_de_ml = LS_DB_PATIENT_TMP.pat_patient_pregnant_de_ml,LS_DB_PATIENT.pat_patient_pregnant = LS_DB_PATIENT_TMP.pat_patient_pregnant,LS_DB_PATIENT.pat_patient_name = LS_DB_PATIENT_TMP.pat_patient_name,LS_DB_PATIENT.pat_patient_military_status_de_ml = LS_DB_PATIENT_TMP.pat_patient_military_status_de_ml,LS_DB_PATIENT.pat_patient_military_status = LS_DB_PATIENT_TMP.pat_patient_military_status,LS_DB_PATIENT.pat_patient_last_name_nf = LS_DB_PATIENT_TMP.pat_patient_last_name_nf,LS_DB_PATIENT.pat_patient_last_name = LS_DB_PATIENT_TMP.pat_patient_last_name,LS_DB_PATIENT.pat_patient_lang = LS_DB_PATIENT_TMP.pat_patient_lang,LS_DB_PATIENT.pat_patient_is_reporter = LS_DB_PATIENT_TMP.pat_patient_is_reporter,LS_DB_PATIENT.pat_patient_identify_de_ml = LS_DB_PATIENT_TMP.pat_patient_identify_de_ml,LS_DB_PATIENT.pat_patient_identify = LS_DB_PATIENT_TMP.pat_patient_identify,LS_DB_PATIENT.pat_patient_id = LS_DB_PATIENT_TMP.pat_patient_id,LS_DB_PATIENT.pat_patient_first_name_nf = LS_DB_PATIENT_TMP.pat_patient_first_name_nf,LS_DB_PATIENT.pat_patient_first_name = LS_DB_PATIENT_TMP.pat_patient_first_name,LS_DB_PATIENT.pat_patient_age_in_year = LS_DB_PATIENT_TMP.pat_patient_age_in_year,LS_DB_PATIENT.pat_pathospitalrecordnumb_nf = LS_DB_PATIENT_TMP.pat_pathospitalrecordnumb_nf,LS_DB_PATIENT.pat_pathospitalrecordnumb_lang = LS_DB_PATIENT_TMP.pat_pathospitalrecordnumb_lang,LS_DB_PATIENT.pat_pathospitalrecordnumb = LS_DB_PATIENT_TMP.pat_pathospitalrecordnumb,LS_DB_PATIENT.pat_patheight_unit_code_de_ml = LS_DB_PATIENT_TMP.pat_patheight_unit_code_de_ml,LS_DB_PATIENT.pat_patheight_unit_code = LS_DB_PATIENT_TMP.pat_patheight_unit_code,LS_DB_PATIENT.pat_patheight_lang = LS_DB_PATIENT_TMP.pat_patheight_lang,LS_DB_PATIENT.pat_patheight = LS_DB_PATIENT_TMP.pat_patheight,LS_DB_PATIENT.pat_patdob_tz = LS_DB_PATIENT_TMP.pat_patdob_tz,LS_DB_PATIENT.pat_patdob_rdm = LS_DB_PATIENT_TMP.pat_patdob_rdm,LS_DB_PATIENT.pat_patdob_random_no = LS_DB_PATIENT_TMP.pat_patdob_random_no,LS_DB_PATIENT.pat_patdob_nf = LS_DB_PATIENT_TMP.pat_patdob_nf,LS_DB_PATIENT.pat_patdob_fmt = LS_DB_PATIENT_TMP.pat_patdob_fmt,LS_DB_PATIENT.pat_patdob = LS_DB_PATIENT_TMP.pat_patdob,LS_DB_PATIENT.pat_patagegroup_nf = LS_DB_PATIENT_TMP.pat_patagegroup_nf,LS_DB_PATIENT.pat_patagegroup_de_ml = LS_DB_PATIENT_TMP.pat_patagegroup_de_ml,LS_DB_PATIENT.pat_patagegroup = LS_DB_PATIENT_TMP.pat_patagegroup,LS_DB_PATIENT.pat_pat_registration_no = LS_DB_PATIENT_TMP.pat_pat_registration_no,LS_DB_PATIENT.pat_pat_medicalhistorytext_nf = LS_DB_PATIENT_TMP.pat_pat_medicalhistorytext_nf,LS_DB_PATIENT.pat_parent_child_case = LS_DB_PATIENT_TMP.pat_parent_child_case,LS_DB_PATIENT.pat_other_sys_int_id2 = LS_DB_PATIENT_TMP.pat_other_sys_int_id2,LS_DB_PATIENT.pat_other_sys_int_id1 = LS_DB_PATIENT_TMP.pat_other_sys_int_id1,LS_DB_PATIENT.pat_ok_to_share_details_de_ml = LS_DB_PATIENT_TMP.pat_ok_to_share_details_de_ml,LS_DB_PATIENT.pat_ok_to_share_details = LS_DB_PATIENT_TMP.pat_ok_to_share_details,LS_DB_PATIENT.pat_ok_to_contact_dr_de_ml = LS_DB_PATIENT_TMP.pat_ok_to_contact_dr_de_ml,LS_DB_PATIENT.pat_ok_to_contact_dr = LS_DB_PATIENT_TMP.pat_ok_to_contact_dr,LS_DB_PATIENT.pat_nationalities_cn_de_ml = LS_DB_PATIENT_TMP.pat_nationalities_cn_de_ml,LS_DB_PATIENT.pat_nationalities_cn = LS_DB_PATIENT_TMP.pat_nationalities_cn,LS_DB_PATIENT.pat_narrative_map_mh = LS_DB_PATIENT_TMP.pat_narrative_map_mh,LS_DB_PATIENT.pat_narrative_generated_mh = LS_DB_PATIENT_TMP.pat_narrative_generated_mh,LS_DB_PATIENT.pat_mother_exp_any_medical_problem_de_ml = LS_DB_PATIENT_TMP.pat_mother_exp_any_medical_problem_de_ml,LS_DB_PATIENT.pat_mother_exp_any_medical_problem = LS_DB_PATIENT_TMP.pat_mother_exp_any_medical_problem,LS_DB_PATIENT.pat_middle_name_nf = LS_DB_PATIENT_TMP.pat_middle_name_nf,LS_DB_PATIENT.pat_middle_name = LS_DB_PATIENT_TMP.pat_middle_name,LS_DB_PATIENT.pat_medical_institution_cn = LS_DB_PATIENT_TMP.pat_medical_institution_cn,LS_DB_PATIENT.pat_manual_age_entry_de_ml = LS_DB_PATIENT_TMP.pat_manual_age_entry_de_ml,LS_DB_PATIENT.pat_manual_age_entry = LS_DB_PATIENT_TMP.pat_manual_age_entry,LS_DB_PATIENT.pat_lastmenstrualdatefmt = LS_DB_PATIENT_TMP.pat_lastmenstrualdatefmt,LS_DB_PATIENT.pat_is_pregnant_code_de_ml = LS_DB_PATIENT_TMP.pat_is_pregnant_code_de_ml,LS_DB_PATIENT.pat_is_pregnant_code = LS_DB_PATIENT_TMP.pat_is_pregnant_code,LS_DB_PATIENT.pat_is_patient_pregnant = LS_DB_PATIENT_TMP.pat_is_patient_pregnant,LS_DB_PATIENT.pat_inq_rec_id = LS_DB_PATIENT_TMP.pat_inq_rec_id,LS_DB_PATIENT.pat_inform_patient_de_ml = LS_DB_PATIENT_TMP.pat_inform_patient_de_ml,LS_DB_PATIENT.pat_inform_patient = LS_DB_PATIENT_TMP.pat_inform_patient,LS_DB_PATIENT.pat_illness_at_time_of_vaccine = LS_DB_PATIENT_TMP.pat_illness_at_time_of_vaccine,LS_DB_PATIENT.pat_health_damage_type_de_ml = LS_DB_PATIENT_TMP.pat_health_damage_type_de_ml,LS_DB_PATIENT.pat_health_damage_type = LS_DB_PATIENT_TMP.pat_health_damage_type,LS_DB_PATIENT.pat_hawaiian_or_pacific_islander = LS_DB_PATIENT_TMP.pat_hawaiian_or_pacific_islander,LS_DB_PATIENT.pat_gestation_week_type = LS_DB_PATIENT_TMP.pat_gestation_week_type,LS_DB_PATIENT.pat_gestatationperiodunit_de_ml = LS_DB_PATIENT_TMP.pat_gestatationperiodunit_de_ml,LS_DB_PATIENT.pat_gestatationperiodunit = LS_DB_PATIENT_TMP.pat_gestatationperiodunit,LS_DB_PATIENT.pat_gestatationperiod_lang = LS_DB_PATIENT_TMP.pat_gestatationperiod_lang,LS_DB_PATIENT.pat_gestatationperiod = LS_DB_PATIENT_TMP.pat_gestatationperiod,LS_DB_PATIENT.pat_gender_oth = LS_DB_PATIENT_TMP.pat_gender_oth,LS_DB_PATIENT.pat_gender_de_ml = LS_DB_PATIENT_TMP.pat_gender_de_ml,LS_DB_PATIENT.pat_gender = LS_DB_PATIENT_TMP.pat_gender,LS_DB_PATIENT.pat_fk_pat_part_rec_id = LS_DB_PATIENT_TMP.pat_fk_pat_part_rec_id,LS_DB_PATIENT.pat_fk_as_rec_id = LS_DB_PATIENT_TMP.pat_fk_as_rec_id,LS_DB_PATIENT.pat_fk_aprg_record_id = LS_DB_PATIENT_TMP.pat_fk_aprg_record_id,LS_DB_PATIENT.pat_fk_apd_rec_id = LS_DB_PATIENT_TMP.pat_fk_apd_rec_id,LS_DB_PATIENT.pat_fk_apar_rec_id = LS_DB_PATIENT_TMP.pat_fk_apar_rec_id,LS_DB_PATIENT.pat_fk_adev_record_id = LS_DB_PATIENT_TMP.pat_fk_adev_record_id,LS_DB_PATIENT.pat_fax_no = LS_DB_PATIENT_TMP.pat_fax_no,LS_DB_PATIENT.pat_ext_clob_fld = LS_DB_PATIENT_TMP.pat_ext_clob_fld,LS_DB_PATIENT.pat_expectedduedate_fmt = LS_DB_PATIENT_TMP.pat_expectedduedate_fmt,LS_DB_PATIENT.pat_expectedduedate = LS_DB_PATIENT_TMP.pat_expectedduedate,LS_DB_PATIENT.pat_event_date_tz = LS_DB_PATIENT_TMP.pat_event_date_tz,LS_DB_PATIENT.pat_event_date_fmt = LS_DB_PATIENT_TMP.pat_event_date_fmt,LS_DB_PATIENT.pat_event_date = LS_DB_PATIENT_TMP.pat_event_date,LS_DB_PATIENT.pat_ethnic_origin_de_ml = LS_DB_PATIENT_TMP.pat_ethnic_origin_de_ml,LS_DB_PATIENT.pat_ethnic_origin_cn_de_ml = LS_DB_PATIENT_TMP.pat_ethnic_origin_cn_de_ml,LS_DB_PATIENT.pat_ethnic_origin_cn = LS_DB_PATIENT_TMP.pat_ethnic_origin_cn,LS_DB_PATIENT.pat_ethnic_origin = LS_DB_PATIENT_TMP.pat_ethnic_origin,LS_DB_PATIENT.pat_ethenic_origin_nf = LS_DB_PATIENT_TMP.pat_ethenic_origin_nf,LS_DB_PATIENT.pat_entity_updated = LS_DB_PATIENT_TMP.pat_entity_updated,LS_DB_PATIENT.pat_enrolment_status = LS_DB_PATIENT_TMP.pat_enrolment_status,LS_DB_PATIENT.pat_enrolment_date = LS_DB_PATIENT_TMP.pat_enrolment_date,LS_DB_PATIENT.pat_email_id_nf = LS_DB_PATIENT_TMP.pat_email_id_nf,LS_DB_PATIENT.pat_email_id = LS_DB_PATIENT_TMP.pat_email_id,LS_DB_PATIENT.pat_e2b_r3_gestperiodoverride = LS_DB_PATIENT_TMP.pat_e2b_r3_gestperiodoverride,LS_DB_PATIENT.pat_drug_type = LS_DB_PATIENT_TMP.pat_drug_type,LS_DB_PATIENT.pat_donotreportname_de_ml = LS_DB_PATIENT_TMP.pat_donotreportname_de_ml,LS_DB_PATIENT.pat_donotreportname = LS_DB_PATIENT_TMP.pat_donotreportname,LS_DB_PATIENT.pat_dob_unknown_code_de_ml = LS_DB_PATIENT_TMP.pat_dob_unknown_code_de_ml,LS_DB_PATIENT.pat_dob_unknown_code = LS_DB_PATIENT_TMP.pat_dob_unknown_code,LS_DB_PATIENT.pat_do_not_use_ang_mh = LS_DB_PATIENT_TMP.pat_do_not_use_ang_mh,LS_DB_PATIENT.pat_device_problem_type = LS_DB_PATIENT_TMP.pat_device_problem_type,LS_DB_PATIENT.pat_date_modified = LS_DB_PATIENT_TMP.pat_date_modified,LS_DB_PATIENT.pat_date_created = LS_DB_PATIENT_TMP.pat_date_created,LS_DB_PATIENT.pat_dataprivacy_present = LS_DB_PATIENT_TMP.pat_dataprivacy_present,LS_DB_PATIENT.pat_data_encrypted = LS_DB_PATIENT_TMP.pat_data_encrypted,LS_DB_PATIENT.pat_county_nf = LS_DB_PATIENT_TMP.pat_county_nf,LS_DB_PATIENT.pat_county = LS_DB_PATIENT_TMP.pat_county,LS_DB_PATIENT.pat_country_nf = LS_DB_PATIENT_TMP.pat_country_nf,LS_DB_PATIENT.pat_country_cn_de_ml = LS_DB_PATIENT_TMP.pat_country_cn_de_ml,LS_DB_PATIENT.pat_country_cn = LS_DB_PATIENT_TMP.pat_country_cn,LS_DB_PATIENT.pat_country = LS_DB_PATIENT_TMP.pat_country,LS_DB_PATIENT.pat_contact_info_avail = LS_DB_PATIENT_TMP.pat_contact_info_avail,LS_DB_PATIENT.pat_concomitant_therapies_de_ml = LS_DB_PATIENT_TMP.pat_concomitant_therapies_de_ml,LS_DB_PATIENT.pat_concomitant_therapies = LS_DB_PATIENT_TMP.pat_concomitant_therapies,LS_DB_PATIENT.pat_concomitant_product = LS_DB_PATIENT_TMP.pat_concomitant_product,LS_DB_PATIENT.pat_comp_rec_id = LS_DB_PATIENT_TMP.pat_comp_rec_id,LS_DB_PATIENT.pat_coding_comments = LS_DB_PATIENT_TMP.pat_coding_comments,LS_DB_PATIENT.pat_city_nf = LS_DB_PATIENT_TMP.pat_city_nf,LS_DB_PATIENT.pat_city = LS_DB_PATIENT_TMP.pat_city,LS_DB_PATIENT.pat_body_surface_index_type = LS_DB_PATIENT_TMP.pat_body_surface_index_type,LS_DB_PATIENT.pat_body_surface_index = LS_DB_PATIENT_TMP.pat_body_surface_index,LS_DB_PATIENT.pat_body_mass_index_type = LS_DB_PATIENT_TMP.pat_body_mass_index_type,LS_DB_PATIENT.pat_body_mass_index = LS_DB_PATIENT_TMP.pat_body_mass_index,LS_DB_PATIENT.pat_black_african_american = LS_DB_PATIENT_TMP.pat_black_african_american,LS_DB_PATIENT.pat_birth_weight_unit_de_ml = LS_DB_PATIENT_TMP.pat_birth_weight_unit_de_ml,LS_DB_PATIENT.pat_birth_weight_unit = LS_DB_PATIENT_TMP.pat_birth_weight_unit,LS_DB_PATIENT.pat_birth_weight = LS_DB_PATIENT_TMP.pat_birth_weight,LS_DB_PATIENT.pat_asian = LS_DB_PATIENT_TMP.pat_asian,LS_DB_PATIENT.pat_ari_rec_id = LS_DB_PATIENT_TMP.pat_ari_rec_id,LS_DB_PATIENT.pat_any_relevant_info_from_partner_de_ml = LS_DB_PATIENT_TMP.pat_any_relevant_info_from_partner_de_ml,LS_DB_PATIENT.pat_any_relevant_info_from_partner = LS_DB_PATIENT_TMP.pat_any_relevant_info_from_partner,LS_DB_PATIENT.pat_american_ind_or_alaskan = LS_DB_PATIENT_TMP.pat_american_ind_or_alaskan,LS_DB_PATIENT.pat_allergies = LS_DB_PATIENT_TMP.pat_allergies,LS_DB_PATIENT.pat_age_group_manual = LS_DB_PATIENT_TMP.pat_age_group_manual,LS_DB_PATIENT.pat_age_at_time_of_vaccine_unit_de_ml = LS_DB_PATIENT_TMP.pat_age_at_time_of_vaccine_unit_de_ml,LS_DB_PATIENT.pat_age_at_time_of_vaccine_unit = LS_DB_PATIENT_TMP.pat_age_at_time_of_vaccine_unit,LS_DB_PATIENT.pat_age_at_time_of_vaccine = LS_DB_PATIENT_TMP.pat_age_at_time_of_vaccine,LS_DB_PATIENT.pat_address_nf = LS_DB_PATIENT_TMP.pat_address_nf,LS_DB_PATIENT.pat_address2_nf = LS_DB_PATIENT_TMP.pat_address2_nf,LS_DB_PATIENT.pat_address2 = LS_DB_PATIENT_TMP.pat_address2,LS_DB_PATIENT.pat_address1_nf = LS_DB_PATIENT_TMP.pat_address1_nf,LS_DB_PATIENT.pat_address1 = LS_DB_PATIENT_TMP.pat_address1,LS_DB_PATIENT.pat_address = LS_DB_PATIENT_TMP.pat_address,LS_DB_PATIENT.pat_aborginal = LS_DB_PATIENT_TMP.pat_aborginal,LS_DB_PATIENT.patptnr_version = LS_DB_PATIENT_TMP.patptnr_version,LS_DB_PATIENT.patptnr_user_modified = LS_DB_PATIENT_TMP.patptnr_user_modified,LS_DB_PATIENT.patptnr_user_created = LS_DB_PATIENT_TMP.patptnr_user_created,LS_DB_PATIENT.patptnr_spr_id = LS_DB_PATIENT_TMP.patptnr_spr_id,LS_DB_PATIENT.patptnr_record_id = LS_DB_PATIENT_TMP.patptnr_record_id,LS_DB_PATIENT.patptnr_partner_name = LS_DB_PATIENT_TMP.patptnr_partner_name,LS_DB_PATIENT.patptnr_partner_initial = LS_DB_PATIENT_TMP.patptnr_partner_initial,LS_DB_PATIENT.patptnr_partner_dob_rdm = LS_DB_PATIENT_TMP.patptnr_partner_dob_rdm,LS_DB_PATIENT.patptnr_partner_dob_fmt = LS_DB_PATIENT_TMP.patptnr_partner_dob_fmt,LS_DB_PATIENT.patptnr_partner_dob = LS_DB_PATIENT_TMP.patptnr_partner_dob,LS_DB_PATIENT.patptnr_partner_contact_details = LS_DB_PATIENT_TMP.patptnr_partner_contact_details,LS_DB_PATIENT.patptnr_inq_rec_id = LS_DB_PATIENT_TMP.patptnr_inq_rec_id,LS_DB_PATIENT.patptnr_entity_updated = LS_DB_PATIENT_TMP.patptnr_entity_updated,LS_DB_PATIENT.patptnr_date_modified = LS_DB_PATIENT_TMP.patptnr_date_modified,LS_DB_PATIENT.patptnr_date_created = LS_DB_PATIENT_TMP.patptnr_date_created,LS_DB_PATIENT.patptnr_comp_rec_id = LS_DB_PATIENT_TMP.patptnr_comp_rec_id,LS_DB_PATIENT.patptnr_biological_father_age_unit_de_ml = LS_DB_PATIENT_TMP.patptnr_biological_father_age_unit_de_ml,LS_DB_PATIENT.patptnr_biological_father_age_unit = LS_DB_PATIENT_TMP.patptnr_biological_father_age_unit,LS_DB_PATIENT.patptnr_biological_father_age = LS_DB_PATIENT_TMP.patptnr_biological_father_age,LS_DB_PATIENT.patptnr_ari_rec_id = LS_DB_PATIENT_TMP.patptnr_ari_rec_id,LS_DB_PATIENT.patdth_version = LS_DB_PATIENT_TMP.patdth_version,LS_DB_PATIENT.patdth_user_modified = LS_DB_PATIENT_TMP.patdth_user_modified,LS_DB_PATIENT.patdth_user_created = LS_DB_PATIENT_TMP.patdth_user_created,LS_DB_PATIENT.patdth_spr_id = LS_DB_PATIENT_TMP.patdth_spr_id,LS_DB_PATIENT.patdth_record_id = LS_DB_PATIENT_TMP.patdth_record_id,LS_DB_PATIENT.patdth_patdeathdatefmt = LS_DB_PATIENT_TMP.patdth_patdeathdatefmt,LS_DB_PATIENT.patdth_patdeathdate_tz = LS_DB_PATIENT_TMP.patdth_patdeathdate_tz,LS_DB_PATIENT.patdth_patdeathdate_nf = LS_DB_PATIENT_TMP.patdth_patdeathdate_nf,LS_DB_PATIENT.patdth_patdeathdate = LS_DB_PATIENT_TMP.patdth_patdeathdate,LS_DB_PATIENT.patdth_patdeath_lang = LS_DB_PATIENT_TMP.patdth_patdeath_lang,LS_DB_PATIENT.patdth_patautopsyyesno_nf = LS_DB_PATIENT_TMP.patdth_patautopsyyesno_nf,LS_DB_PATIENT.patdth_patautopsyyesno_de_ml = LS_DB_PATIENT_TMP.patdth_patautopsyyesno_de_ml,LS_DB_PATIENT.patdth_patautopsyyesno = LS_DB_PATIENT_TMP.patdth_patautopsyyesno,LS_DB_PATIENT.patdth_patautopsydatefmt = LS_DB_PATIENT_TMP.patdth_patautopsydatefmt,LS_DB_PATIENT.patdth_patautopsydate = LS_DB_PATIENT_TMP.patdth_patautopsydate,LS_DB_PATIENT.patdth_meddra_pt_decode = LS_DB_PATIENT_TMP.patdth_meddra_pt_decode,LS_DB_PATIENT.patdth_meddra_pt_code = LS_DB_PATIENT_TMP.patdth_meddra_pt_code,LS_DB_PATIENT.patdth_meddra_llt_decode = LS_DB_PATIENT_TMP.patdth_meddra_llt_decode,LS_DB_PATIENT.patdth_meddra_llt_code = LS_DB_PATIENT_TMP.patdth_meddra_llt_code,LS_DB_PATIENT.patdth_inq_rec_id = LS_DB_PATIENT_TMP.patdth_inq_rec_id,LS_DB_PATIENT.patdth_ext_clob_fld = LS_DB_PATIENT_TMP.patdth_ext_clob_fld,LS_DB_PATIENT.patdth_entity_updated = LS_DB_PATIENT_TMP.patdth_entity_updated,LS_DB_PATIENT.patdth_date_modified = LS_DB_PATIENT_TMP.patdth_date_modified,LS_DB_PATIENT.patdth_date_created = LS_DB_PATIENT_TMP.patdth_date_created,LS_DB_PATIENT.patdth_comp_rec_id = LS_DB_PATIENT_TMP.patdth_comp_rec_id,LS_DB_PATIENT.patdth_autopsy_done_manual = LS_DB_PATIENT_TMP.patdth_autopsy_done_manual,LS_DB_PATIENT.patdth_ari_rec_id = LS_DB_PATIENT_TMP.patdth_ari_rec_id,LS_DB_PATIENT.patapsy_version = LS_DB_PATIENT_TMP.patapsy_version,LS_DB_PATIENT.patapsy_user_modified = LS_DB_PATIENT_TMP.patapsy_user_modified,LS_DB_PATIENT.patapsy_user_created = LS_DB_PATIENT_TMP.patapsy_user_created,LS_DB_PATIENT.patapsy_spr_id = LS_DB_PATIENT_TMP.patapsy_spr_id,LS_DB_PATIENT.patapsy_record_id = LS_DB_PATIENT_TMP.patapsy_record_id,LS_DB_PATIENT.patapsy_patdetautopsymeddraver_lang = LS_DB_PATIENT_TMP.patapsy_patdetautopsymeddraver_lang,LS_DB_PATIENT.patapsy_patdetautopsymeddraver = LS_DB_PATIENT_TMP.patapsy_patdetautopsymeddraver,LS_DB_PATIENT.patapsy_patdetautopsylevel = LS_DB_PATIENT_TMP.patapsy_patdetautopsylevel,LS_DB_PATIENT.patapsy_patdetautopsy_ptcode = LS_DB_PATIENT_TMP.patapsy_patdetautopsy_ptcode,LS_DB_PATIENT.patapsy_patdetautopsy_lang = LS_DB_PATIENT_TMP.patapsy_patdetautopsy_lang,LS_DB_PATIENT.patapsy_patdetautopsy_decode = LS_DB_PATIENT_TMP.patapsy_patdetautopsy_decode,LS_DB_PATIENT.patapsy_patdetautopsy_code = LS_DB_PATIENT_TMP.patapsy_patdetautopsy_code,LS_DB_PATIENT.patapsy_patdetautopsy = LS_DB_PATIENT_TMP.patapsy_patdetautopsy,LS_DB_PATIENT.patapsy_patautopsy_lang = LS_DB_PATIENT_TMP.patapsy_patautopsy_lang,LS_DB_PATIENT.patapsy_inq_rec_id = LS_DB_PATIENT_TMP.patapsy_inq_rec_id,LS_DB_PATIENT.patapsy_fk_apd_rec_id = LS_DB_PATIENT_TMP.patapsy_fk_apd_rec_id,LS_DB_PATIENT.patapsy_ext_clob_fld = LS_DB_PATIENT_TMP.patapsy_ext_clob_fld,LS_DB_PATIENT.patapsy_entity_updated = LS_DB_PATIENT_TMP.patapsy_entity_updated,LS_DB_PATIENT.patapsy_date_modified = LS_DB_PATIENT_TMP.patapsy_date_modified,LS_DB_PATIENT.patapsy_date_created = LS_DB_PATIENT_TMP.patapsy_date_created,LS_DB_PATIENT.patapsy_comp_rec_id = LS_DB_PATIENT_TMP.patapsy_comp_rec_id,LS_DB_PATIENT.patapsy_coding_type = LS_DB_PATIENT_TMP.patapsy_coding_type,LS_DB_PATIENT.patapsy_coding_comments = LS_DB_PATIENT_TMP.patapsy_coding_comments,LS_DB_PATIENT.patapsy_autopsycause_coded_flag = LS_DB_PATIENT_TMP.patapsy_autopsycause_coded_flag,LS_DB_PATIENT.patapsy_ari_rec_id = LS_DB_PATIENT_TMP.patapsy_ari_rec_id,LS_DB_PATIENT.par_version = LS_DB_PATIENT_TMP.par_version,LS_DB_PATIENT.par_user_modified = LS_DB_PATIENT_TMP.par_user_modified,LS_DB_PATIENT.par_user_created = LS_DB_PATIENT_TMP.par_user_created,LS_DB_PATIENT.par_spr_id = LS_DB_PATIENT_TMP.par_spr_id,LS_DB_PATIENT.par_record_id = LS_DB_PATIENT_TMP.par_record_id,LS_DB_PATIENT.par_paternal_exposure = LS_DB_PATIENT_TMP.par_paternal_exposure,LS_DB_PATIENT.par_parentweight_unit_de_ml = LS_DB_PATIENT_TMP.par_parentweight_unit_de_ml,LS_DB_PATIENT.par_parentweight_unit = LS_DB_PATIENT_TMP.par_parentweight_unit,LS_DB_PATIENT.par_parentweight_lang = LS_DB_PATIENT_TMP.par_parentweight_lang,LS_DB_PATIENT.par_parentweight = LS_DB_PATIENT_TMP.par_parentweight,LS_DB_PATIENT.par_parentsex_nf = LS_DB_PATIENT_TMP.par_parentsex_nf,LS_DB_PATIENT.par_parentsex_de_ml = LS_DB_PATIENT_TMP.par_parentsex_de_ml,LS_DB_PATIENT.par_parentsex = LS_DB_PATIENT_TMP.par_parentsex,LS_DB_PATIENT.par_parentmedrelevanttext_lang = LS_DB_PATIENT_TMP.par_parentmedrelevanttext_lang,LS_DB_PATIENT.par_parentmedrelevanttext = LS_DB_PATIENT_TMP.par_parentmedrelevanttext,LS_DB_PATIENT.par_parentlastmensdatefmt = LS_DB_PATIENT_TMP.par_parentlastmensdatefmt,LS_DB_PATIENT.par_parentlastmensdate_tz = LS_DB_PATIENT_TMP.par_parentlastmensdate_tz,LS_DB_PATIENT.par_parentlastmensdate_nf = LS_DB_PATIENT_TMP.par_parentlastmensdate_nf,LS_DB_PATIENT.par_parentlastmensdate = LS_DB_PATIENT_TMP.par_parentlastmensdate,LS_DB_PATIENT.par_parentidentification_nf = LS_DB_PATIENT_TMP.par_parentidentification_nf,LS_DB_PATIENT.par_parentidentification_lang = LS_DB_PATIENT_TMP.par_parentidentification_lang,LS_DB_PATIENT.par_parentidentification = LS_DB_PATIENT_TMP.par_parentidentification,LS_DB_PATIENT.par_parentheight_unit_de_ml = LS_DB_PATIENT_TMP.par_parentheight_unit_de_ml,LS_DB_PATIENT.par_parentheight_unit = LS_DB_PATIENT_TMP.par_parentheight_unit,LS_DB_PATIENT.par_parentheight_lang = LS_DB_PATIENT_TMP.par_parentheight_lang,LS_DB_PATIENT.par_parentheight = LS_DB_PATIENT_TMP.par_parentheight,LS_DB_PATIENT.par_parentdobfmt = LS_DB_PATIENT_TMP.par_parentdobfmt,LS_DB_PATIENT.par_parentdob_tz = LS_DB_PATIENT_TMP.par_parentdob_tz,LS_DB_PATIENT.par_parentdob_rdm = LS_DB_PATIENT_TMP.par_parentdob_rdm,LS_DB_PATIENT.par_parentdob_nf = LS_DB_PATIENT_TMP.par_parentdob_nf,LS_DB_PATIENT.par_parentdob = LS_DB_PATIENT_TMP.par_parentdob,LS_DB_PATIENT.par_parentageunit_de_ml = LS_DB_PATIENT_TMP.par_parentageunit_de_ml,LS_DB_PATIENT.par_parentageunit = LS_DB_PATIENT_TMP.par_parentageunit,LS_DB_PATIENT.par_parentage_lang = LS_DB_PATIENT_TMP.par_parentage_lang,LS_DB_PATIENT.par_parentage = LS_DB_PATIENT_TMP.par_parentage,LS_DB_PATIENT.par_parent_lang = LS_DB_PATIENT_TMP.par_parent_lang,LS_DB_PATIENT.par_parent_age_at_vaccine_unit_de_ml = LS_DB_PATIENT_TMP.par_parent_age_at_vaccine_unit_de_ml,LS_DB_PATIENT.par_parent_age_at_vaccine_unit = LS_DB_PATIENT_TMP.par_parent_age_at_vaccine_unit,LS_DB_PATIENT.par_parent_age_at_vaccine = LS_DB_PATIENT_TMP.par_parent_age_at_vaccine,LS_DB_PATIENT.par_par_ethnic_origin_de_ml = LS_DB_PATIENT_TMP.par_par_ethnic_origin_de_ml,LS_DB_PATIENT.par_par_ethnic_origin = LS_DB_PATIENT_TMP.par_par_ethnic_origin,LS_DB_PATIENT.par_inq_rec_id = LS_DB_PATIENT_TMP.par_inq_rec_id,LS_DB_PATIENT.par_ext_clob_fld = LS_DB_PATIENT_TMP.par_ext_clob_fld,LS_DB_PATIENT.par_entity_updated = LS_DB_PATIENT_TMP.par_entity_updated,LS_DB_PATIENT.par_duration_unit = LS_DB_PATIENT_TMP.par_duration_unit,LS_DB_PATIENT.par_duration = LS_DB_PATIENT_TMP.par_duration,LS_DB_PATIENT.par_date_modified = LS_DB_PATIENT_TMP.par_date_modified,LS_DB_PATIENT.par_date_created = LS_DB_PATIENT_TMP.par_date_created,LS_DB_PATIENT.par_consent_to_contact_parent_de_ml = LS_DB_PATIENT_TMP.par_consent_to_contact_parent_de_ml,LS_DB_PATIENT.par_consent_to_contact_parent = LS_DB_PATIENT_TMP.par_consent_to_contact_parent,LS_DB_PATIENT.par_comp_rec_id = LS_DB_PATIENT_TMP.par_comp_rec_id,LS_DB_PATIENT.par_ari_rec_id = LS_DB_PATIENT_TMP.par_ari_rec_id,LS_DB_PATIENT.dthcause_version = LS_DB_PATIENT_TMP.dthcause_version,LS_DB_PATIENT.dthcause_uuid = LS_DB_PATIENT_TMP.dthcause_uuid,LS_DB_PATIENT.dthcause_user_modified = LS_DB_PATIENT_TMP.dthcause_user_modified,LS_DB_PATIENT.dthcause_user_created = LS_DB_PATIENT_TMP.dthcause_user_created,LS_DB_PATIENT.dthcause_spr_id = LS_DB_PATIENT_TMP.dthcause_spr_id,LS_DB_PATIENT.dthcause_record_id = LS_DB_PATIENT_TMP.dthcause_record_id,LS_DB_PATIENT.dthcause_patdeathreportlevel = LS_DB_PATIENT_TMP.dthcause_patdeathreportlevel,LS_DB_PATIENT.dthcause_patdeathreport_ptcode = LS_DB_PATIENT_TMP.dthcause_patdeathreport_ptcode,LS_DB_PATIENT.dthcause_patdeathreport_lang = LS_DB_PATIENT_TMP.dthcause_patdeathreport_lang,LS_DB_PATIENT.dthcause_patdeathreport_decode = LS_DB_PATIENT_TMP.dthcause_patdeathreport_decode,LS_DB_PATIENT.dthcause_patdeathreport_code = LS_DB_PATIENT_TMP.dthcause_patdeathreport_code,LS_DB_PATIENT.dthcause_patdeathreport = LS_DB_PATIENT_TMP.dthcause_patdeathreport,LS_DB_PATIENT.dthcause_patdeathrepmeddraver_lang = LS_DB_PATIENT_TMP.dthcause_patdeathrepmeddraver_lang,LS_DB_PATIENT.dthcause_patdeathrepmeddraver = LS_DB_PATIENT_TMP.dthcause_patdeathrepmeddraver,LS_DB_PATIENT.dthcause_patdeathcause_lang = LS_DB_PATIENT_TMP.dthcause_patdeathcause_lang,LS_DB_PATIENT.dthcause_inq_rec_id = LS_DB_PATIENT_TMP.dthcause_inq_rec_id,LS_DB_PATIENT.dthcause_fk_apd_rec_id = LS_DB_PATIENT_TMP.dthcause_fk_apd_rec_id,LS_DB_PATIENT.dthcause_ext_clob_fld = LS_DB_PATIENT_TMP.dthcause_ext_clob_fld,LS_DB_PATIENT.dthcause_entity_updated = LS_DB_PATIENT_TMP.dthcause_entity_updated,LS_DB_PATIENT.dthcause_deathcause_coded_flag = LS_DB_PATIENT_TMP.dthcause_deathcause_coded_flag,LS_DB_PATIENT.dthcause_date_modified = LS_DB_PATIENT_TMP.dthcause_date_modified,LS_DB_PATIENT.dthcause_date_created = LS_DB_PATIENT_TMP.dthcause_date_created,LS_DB_PATIENT.dthcause_comp_rec_id = LS_DB_PATIENT_TMP.dthcause_comp_rec_id,LS_DB_PATIENT.dthcause_coding_type = LS_DB_PATIENT_TMP.dthcause_coding_type,LS_DB_PATIENT.dthcause_coding_comments = LS_DB_PATIENT_TMP.dthcause_coding_comments,LS_DB_PATIENT.dthcause_ari_rec_id = LS_DB_PATIENT_TMP.dthcause_ari_rec_id,
LS_DB_PATIENT.PROCESSING_DT = LS_DB_PATIENT_TMP.PROCESSING_DT,
LS_DB_PATIENT.receipt_id     =LS_DB_PATIENT_TMP.receipt_id    ,
LS_DB_PATIENT.case_no        =LS_DB_PATIENT_TMP.case_no           ,
LS_DB_PATIENT.case_version   =LS_DB_PATIENT_TMP.case_version      ,
LS_DB_PATIENT.version_no     =LS_DB_PATIENT_TMP.version_no        ,
LS_DB_PATIENT.user_modified  =LS_DB_PATIENT_TMP.user_modified     ,
LS_DB_PATIENT.date_modified  =LS_DB_PATIENT_TMP.date_modified     ,
LS_DB_PATIENT.expiry_date    =LS_DB_PATIENT_TMP.expiry_date       ,
LS_DB_PATIENT.created_by     =LS_DB_PATIENT_TMP.created_by        ,
LS_DB_PATIENT.created_dt     =LS_DB_PATIENT_TMP.created_dt        ,
LS_DB_PATIENT.load_ts        =LS_DB_PATIENT_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP 
WHERE LS_DB_PATIENT.INTEGRATION_ID = LS_DB_PATIENT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_PATIENT_TMP.PROCESSING_DT = LS_DB_PATIENT.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_PATIENT.patapsy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.patptnr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.patdth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.pat_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.par_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.dthcause_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PATIENT_TMP.patapsy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.patptnr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.patdth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.pat_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.par_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.dthcause_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_PATIENT.pat_ARI_REC_ID ,LS_DB_PATIENT.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP 
ON LS_DB_PATIENT.pat_ARI_REC_ID=LS_DB_PATIENT_TMP.pat_ARI_REC_ID
AND LS_DB_PATIENT.INTEGRATION_ID = LS_DB_PATIENT_TMP.INTEGRATION_ID 
where LS_DB_PATIENT_TMP.INTEGRATION_ID  is null AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_PATIENT.pat_ARI_REC_ID in (select pat_ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP )
) TMP where LS_DB_PATIENT.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_PATIENT.pat_ARI_REC_ID ,LS_DB_PATIENT.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP 
    ON LS_DB_PATIENT.pat_ARI_REC_ID=LS_DB_PATIENT_TMP.pat_ARI_REC_ID
    AND LS_DB_PATIENT.INTEGRATION_ID = LS_DB_PATIENT_TMP.INTEGRATION_ID 
    where LS_DB_PATIENT_TMP.INTEGRATION_ID  is null AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_PATIENT.pat_ARI_REC_ID in (select pat_ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT
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
integration_id ,pat_zip_code_nf,
pat_zip_code,
pat_withdrawn_date,
pat_white,
pat_version,
pat_vaccine_meddra_pt_code,
pat_vaccine_meddra_llt_code,
pat_vaccine_coding_type,
pat_vaccine_coded_flag,
pat_user_modified,
pat_user_created,
pat_txt_pat_risk_factor_text,
pat_torres_strait_islander,
pat_title_sf,
pat_title_nf,
pat_title_de_ml,
pat_title,
pat_subject_id_nf,
pat_subject_id,
pat_study_subject_mother,
pat_study_subject_father,
pat_study_program_de_ml,
pat_study_program,
pat_street_nf,
pat_street,
pat_state_nf,
pat_state,
pat_spr_id,
pat_spanish_state_de_ml,
pat_spanish_state,
pat_siebel_patient_rowid,
pat_resultstestsprocedures_lang,
pat_resultstestsprocedures,
pat_registration_no,
pat_record_id,
pat_reaction_type,
pat_raceid_nf,
pat_primary_disease,
pat_pregnant_at_time_of_vaccine_nf,
pat_pregnant_at_time_of_vaccine_de_ml,
pat_pregnant_at_time_of_vaccine,
pat_pregnancy_history_cn,
pat_pregnancy_confirm_mode,
pat_pregnancy_confirm_how_other,
pat_pregnancy_confirm_date,
pat_pregnancy_clinical_status,
pat_phone_number_cn_nf,
pat_phone_number_cn,
pat_phone_no_nf,
pat_phone_no,
pat_patweight_unit_code_de_ml,
pat_patweight_unit_code,
pat_patweight_lang,
pat_patweight,
pat_patspecialistrecordnumb_nf,
pat_patspecialistrecordnumb_lang,
pat_patspecialistrecordnumb,
pat_patsex_nf,
pat_patsex_de_ml,
pat_patsex,
pat_patonsetageunit_de_ml,
pat_patonsetageunit,
pat_patonsetage_lang,
pat_patonsetage,
pat_patmedicalrecordnumb_nf,
pat_patmedicalrecordnumb_lang,
pat_patmedicalrecordnumb,
pat_patmedicalhistorytext_nf,
pat_patmedicalhistorytext_lang,
pat_patmedicalhistorytext,
pat_patlastmenstrualdate_tz,
pat_patlastmenstrualdate_nf,
pat_patlastmenstrualdate,
pat_patinvestigationnumb_nf,
pat_patinvestigationnumb_lang,
pat_patinvestigationnumb,
pat_patinitial_type_code_de_ml,
pat_patinitial_type_code,
pat_patientinitial_nf,
pat_patientinitial_lang,
pat_patientinitial,
pat_patient_type,
pat_patient_pregnant_de_ml,
pat_patient_pregnant,
pat_patient_name,
pat_patient_military_status_de_ml,
pat_patient_military_status,
pat_patient_last_name_nf,
pat_patient_last_name,
pat_patient_lang,
pat_patient_is_reporter,
pat_patient_identify_de_ml,
pat_patient_identify,
pat_patient_id,
pat_patient_first_name_nf,
pat_patient_first_name,
pat_patient_age_in_year,
pat_pathospitalrecordnumb_nf,
pat_pathospitalrecordnumb_lang,
pat_pathospitalrecordnumb,
pat_patheight_unit_code_de_ml,
pat_patheight_unit_code,
pat_patheight_lang,
pat_patheight,
pat_patdob_tz,
pat_patdob_rdm,
pat_patdob_random_no,
pat_patdob_nf,
pat_patdob_fmt,
pat_patdob,
pat_patagegroup_nf,
pat_patagegroup_de_ml,
pat_patagegroup,
pat_pat_registration_no,
pat_pat_medicalhistorytext_nf,
pat_parent_child_case,
pat_other_sys_int_id2,
pat_other_sys_int_id1,
pat_ok_to_share_details_de_ml,
pat_ok_to_share_details,
pat_ok_to_contact_dr_de_ml,
pat_ok_to_contact_dr,
pat_nationalities_cn_de_ml,
pat_nationalities_cn,
pat_narrative_map_mh,
pat_narrative_generated_mh,
pat_mother_exp_any_medical_problem_de_ml,
pat_mother_exp_any_medical_problem,
pat_middle_name_nf,
pat_middle_name,
pat_medical_institution_cn,
pat_manual_age_entry_de_ml,
pat_manual_age_entry,
pat_lastmenstrualdatefmt,
pat_is_pregnant_code_de_ml,
pat_is_pregnant_code,
pat_is_patient_pregnant,
pat_inq_rec_id,
pat_inform_patient_de_ml,
pat_inform_patient,
pat_illness_at_time_of_vaccine,
pat_health_damage_type_de_ml,
pat_health_damage_type,
pat_hawaiian_or_pacific_islander,
pat_gestation_week_type,
pat_gestatationperiodunit_de_ml,
pat_gestatationperiodunit,
pat_gestatationperiod_lang,
pat_gestatationperiod,
pat_gender_oth,
pat_gender_de_ml,
pat_gender,
pat_fk_pat_part_rec_id,
pat_fk_as_rec_id,
pat_fk_aprg_record_id,
pat_fk_apd_rec_id,
pat_fk_apar_rec_id,
pat_fk_adev_record_id,
pat_fax_no,
pat_ext_clob_fld,
pat_expectedduedate_fmt,
pat_expectedduedate,
pat_event_date_tz,
pat_event_date_fmt,
pat_event_date,
pat_ethnic_origin_de_ml,
pat_ethnic_origin_cn_de_ml,
pat_ethnic_origin_cn,
pat_ethnic_origin,
pat_ethenic_origin_nf,
pat_entity_updated,
pat_enrolment_status,
pat_enrolment_date,
pat_email_id_nf,
pat_email_id,
pat_e2b_r3_gestperiodoverride,
pat_drug_type,
pat_donotreportname_de_ml,
pat_donotreportname,
pat_dob_unknown_code_de_ml,
pat_dob_unknown_code,
pat_do_not_use_ang_mh,
pat_device_problem_type,
pat_date_modified,
pat_date_created,
pat_dataprivacy_present,
pat_data_encrypted,
pat_county_nf,
pat_county,
pat_country_nf,
pat_country_cn_de_ml,
pat_country_cn,
pat_country,
pat_contact_info_avail,
pat_concomitant_therapies_de_ml,
pat_concomitant_therapies,
pat_concomitant_product,
pat_comp_rec_id,
pat_coding_comments,
pat_city_nf,
pat_city,
pat_body_surface_index_type,
pat_body_surface_index,
pat_body_mass_index_type,
pat_body_mass_index,
pat_black_african_american,
pat_birth_weight_unit_de_ml,
pat_birth_weight_unit,
pat_birth_weight,
pat_asian,
pat_ari_rec_id,
pat_any_relevant_info_from_partner_de_ml,
pat_any_relevant_info_from_partner,
pat_american_ind_or_alaskan,
pat_allergies,
pat_age_group_manual,
pat_age_at_time_of_vaccine_unit_de_ml,
pat_age_at_time_of_vaccine_unit,
pat_age_at_time_of_vaccine,
pat_address_nf,
pat_address2_nf,
pat_address2,
pat_address1_nf,
pat_address1,
pat_address,
pat_aborginal,
patptnr_version,
patptnr_user_modified,
patptnr_user_created,
patptnr_spr_id,
patptnr_record_id,
patptnr_partner_name,
patptnr_partner_initial,
patptnr_partner_dob_rdm,
patptnr_partner_dob_fmt,
patptnr_partner_dob,
patptnr_partner_contact_details,
patptnr_inq_rec_id,
patptnr_entity_updated,
patptnr_date_modified,
patptnr_date_created,
patptnr_comp_rec_id,
patptnr_biological_father_age_unit_de_ml,
patptnr_biological_father_age_unit,
patptnr_biological_father_age,
patptnr_ari_rec_id,
patdth_version,
patdth_user_modified,
patdth_user_created,
patdth_spr_id,
patdth_record_id,
patdth_patdeathdatefmt,
patdth_patdeathdate_tz,
patdth_patdeathdate_nf,
patdth_patdeathdate,
patdth_patdeath_lang,
patdth_patautopsyyesno_nf,
patdth_patautopsyyesno_de_ml,
patdth_patautopsyyesno,
patdth_patautopsydatefmt,
patdth_patautopsydate,
patdth_meddra_pt_decode,
patdth_meddra_pt_code,
patdth_meddra_llt_decode,
patdth_meddra_llt_code,
patdth_inq_rec_id,
patdth_ext_clob_fld,
patdth_entity_updated,
patdth_date_modified,
patdth_date_created,
patdth_comp_rec_id,
patdth_autopsy_done_manual,
patdth_ari_rec_id,
patapsy_version,
patapsy_user_modified,
patapsy_user_created,
patapsy_spr_id,
patapsy_record_id,
patapsy_patdetautopsymeddraver_lang,
patapsy_patdetautopsymeddraver,
patapsy_patdetautopsylevel,
patapsy_patdetautopsy_ptcode,
patapsy_patdetautopsy_lang,
patapsy_patdetautopsy_decode,
patapsy_patdetautopsy_code,
patapsy_patdetautopsy,
patapsy_patautopsy_lang,
patapsy_inq_rec_id,
patapsy_fk_apd_rec_id,
patapsy_ext_clob_fld,
patapsy_entity_updated,
patapsy_date_modified,
patapsy_date_created,
patapsy_comp_rec_id,
patapsy_coding_type,
patapsy_coding_comments,
patapsy_autopsycause_coded_flag,
patapsy_ari_rec_id,
par_version,
par_user_modified,
par_user_created,
par_spr_id,
par_record_id,
par_paternal_exposure,
par_parentweight_unit_de_ml,
par_parentweight_unit,
par_parentweight_lang,
par_parentweight,
par_parentsex_nf,
par_parentsex_de_ml,
par_parentsex,
par_parentmedrelevanttext_lang,
par_parentmedrelevanttext,
par_parentlastmensdatefmt,
par_parentlastmensdate_tz,
par_parentlastmensdate_nf,
par_parentlastmensdate,
par_parentidentification_nf,
par_parentidentification_lang,
par_parentidentification,
par_parentheight_unit_de_ml,
par_parentheight_unit,
par_parentheight_lang,
par_parentheight,
par_parentdobfmt,
par_parentdob_tz,
par_parentdob_rdm,
par_parentdob_nf,
par_parentdob,
par_parentageunit_de_ml,
par_parentageunit,
par_parentage_lang,
par_parentage,
par_parent_lang,
par_parent_age_at_vaccine_unit_de_ml,
par_parent_age_at_vaccine_unit,
par_parent_age_at_vaccine,
par_par_ethnic_origin_de_ml,
par_par_ethnic_origin,
par_inq_rec_id,
par_ext_clob_fld,
par_entity_updated,
par_duration_unit,
par_duration,
par_date_modified,
par_date_created,
par_consent_to_contact_parent_de_ml,
par_consent_to_contact_parent,
par_comp_rec_id,
par_ari_rec_id,
dthcause_version,
dthcause_uuid,
dthcause_user_modified,
dthcause_user_created,
dthcause_spr_id,
dthcause_record_id,
dthcause_patdeathreportlevel,
dthcause_patdeathreport_ptcode,
dthcause_patdeathreport_lang,
dthcause_patdeathreport_decode,
dthcause_patdeathreport_code,
dthcause_patdeathreport,
dthcause_patdeathrepmeddraver_lang,
dthcause_patdeathrepmeddraver,
dthcause_patdeathcause_lang,
dthcause_inq_rec_id,
dthcause_fk_apd_rec_id,
dthcause_ext_clob_fld,
dthcause_entity_updated,
dthcause_deathcause_coded_flag,
dthcause_date_modified,
dthcause_date_created,
dthcause_comp_rec_id,
dthcause_coding_type,
dthcause_coding_comments,
dthcause_ari_rec_id)
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
integration_id ,pat_zip_code_nf,
pat_zip_code,
pat_withdrawn_date,
pat_white,
pat_version,
pat_vaccine_meddra_pt_code,
pat_vaccine_meddra_llt_code,
pat_vaccine_coding_type,
pat_vaccine_coded_flag,
pat_user_modified,
pat_user_created,
pat_txt_pat_risk_factor_text,
pat_torres_strait_islander,
pat_title_sf,
pat_title_nf,
pat_title_de_ml,
pat_title,
pat_subject_id_nf,
pat_subject_id,
pat_study_subject_mother,
pat_study_subject_father,
pat_study_program_de_ml,
pat_study_program,
pat_street_nf,
pat_street,
pat_state_nf,
pat_state,
pat_spr_id,
pat_spanish_state_de_ml,
pat_spanish_state,
pat_siebel_patient_rowid,
pat_resultstestsprocedures_lang,
pat_resultstestsprocedures,
pat_registration_no,
pat_record_id,
pat_reaction_type,
pat_raceid_nf,
pat_primary_disease,
pat_pregnant_at_time_of_vaccine_nf,
pat_pregnant_at_time_of_vaccine_de_ml,
pat_pregnant_at_time_of_vaccine,
pat_pregnancy_history_cn,
pat_pregnancy_confirm_mode,
pat_pregnancy_confirm_how_other,
pat_pregnancy_confirm_date,
pat_pregnancy_clinical_status,
pat_phone_number_cn_nf,
pat_phone_number_cn,
pat_phone_no_nf,
pat_phone_no,
pat_patweight_unit_code_de_ml,
pat_patweight_unit_code,
pat_patweight_lang,
pat_patweight,
pat_patspecialistrecordnumb_nf,
pat_patspecialistrecordnumb_lang,
pat_patspecialistrecordnumb,
pat_patsex_nf,
pat_patsex_de_ml,
pat_patsex,
pat_patonsetageunit_de_ml,
pat_patonsetageunit,
pat_patonsetage_lang,
pat_patonsetage,
pat_patmedicalrecordnumb_nf,
pat_patmedicalrecordnumb_lang,
pat_patmedicalrecordnumb,
pat_patmedicalhistorytext_nf,
pat_patmedicalhistorytext_lang,
pat_patmedicalhistorytext,
pat_patlastmenstrualdate_tz,
pat_patlastmenstrualdate_nf,
pat_patlastmenstrualdate,
pat_patinvestigationnumb_nf,
pat_patinvestigationnumb_lang,
pat_patinvestigationnumb,
pat_patinitial_type_code_de_ml,
pat_patinitial_type_code,
pat_patientinitial_nf,
pat_patientinitial_lang,
pat_patientinitial,
pat_patient_type,
pat_patient_pregnant_de_ml,
pat_patient_pregnant,
pat_patient_name,
pat_patient_military_status_de_ml,
pat_patient_military_status,
pat_patient_last_name_nf,
pat_patient_last_name,
pat_patient_lang,
pat_patient_is_reporter,
pat_patient_identify_de_ml,
pat_patient_identify,
pat_patient_id,
pat_patient_first_name_nf,
pat_patient_first_name,
pat_patient_age_in_year,
pat_pathospitalrecordnumb_nf,
pat_pathospitalrecordnumb_lang,
pat_pathospitalrecordnumb,
pat_patheight_unit_code_de_ml,
pat_patheight_unit_code,
pat_patheight_lang,
pat_patheight,
pat_patdob_tz,
pat_patdob_rdm,
pat_patdob_random_no,
pat_patdob_nf,
pat_patdob_fmt,
pat_patdob,
pat_patagegroup_nf,
pat_patagegroup_de_ml,
pat_patagegroup,
pat_pat_registration_no,
pat_pat_medicalhistorytext_nf,
pat_parent_child_case,
pat_other_sys_int_id2,
pat_other_sys_int_id1,
pat_ok_to_share_details_de_ml,
pat_ok_to_share_details,
pat_ok_to_contact_dr_de_ml,
pat_ok_to_contact_dr,
pat_nationalities_cn_de_ml,
pat_nationalities_cn,
pat_narrative_map_mh,
pat_narrative_generated_mh,
pat_mother_exp_any_medical_problem_de_ml,
pat_mother_exp_any_medical_problem,
pat_middle_name_nf,
pat_middle_name,
pat_medical_institution_cn,
pat_manual_age_entry_de_ml,
pat_manual_age_entry,
pat_lastmenstrualdatefmt,
pat_is_pregnant_code_de_ml,
pat_is_pregnant_code,
pat_is_patient_pregnant,
pat_inq_rec_id,
pat_inform_patient_de_ml,
pat_inform_patient,
pat_illness_at_time_of_vaccine,
pat_health_damage_type_de_ml,
pat_health_damage_type,
pat_hawaiian_or_pacific_islander,
pat_gestation_week_type,
pat_gestatationperiodunit_de_ml,
pat_gestatationperiodunit,
pat_gestatationperiod_lang,
pat_gestatationperiod,
pat_gender_oth,
pat_gender_de_ml,
pat_gender,
pat_fk_pat_part_rec_id,
pat_fk_as_rec_id,
pat_fk_aprg_record_id,
pat_fk_apd_rec_id,
pat_fk_apar_rec_id,
pat_fk_adev_record_id,
pat_fax_no,
pat_ext_clob_fld,
pat_expectedduedate_fmt,
pat_expectedduedate,
pat_event_date_tz,
pat_event_date_fmt,
pat_event_date,
pat_ethnic_origin_de_ml,
pat_ethnic_origin_cn_de_ml,
pat_ethnic_origin_cn,
pat_ethnic_origin,
pat_ethenic_origin_nf,
pat_entity_updated,
pat_enrolment_status,
pat_enrolment_date,
pat_email_id_nf,
pat_email_id,
pat_e2b_r3_gestperiodoverride,
pat_drug_type,
pat_donotreportname_de_ml,
pat_donotreportname,
pat_dob_unknown_code_de_ml,
pat_dob_unknown_code,
pat_do_not_use_ang_mh,
pat_device_problem_type,
pat_date_modified,
pat_date_created,
pat_dataprivacy_present,
pat_data_encrypted,
pat_county_nf,
pat_county,
pat_country_nf,
pat_country_cn_de_ml,
pat_country_cn,
pat_country,
pat_contact_info_avail,
pat_concomitant_therapies_de_ml,
pat_concomitant_therapies,
pat_concomitant_product,
pat_comp_rec_id,
pat_coding_comments,
pat_city_nf,
pat_city,
pat_body_surface_index_type,
pat_body_surface_index,
pat_body_mass_index_type,
pat_body_mass_index,
pat_black_african_american,
pat_birth_weight_unit_de_ml,
pat_birth_weight_unit,
pat_birth_weight,
pat_asian,
pat_ari_rec_id,
pat_any_relevant_info_from_partner_de_ml,
pat_any_relevant_info_from_partner,
pat_american_ind_or_alaskan,
pat_allergies,
pat_age_group_manual,
pat_age_at_time_of_vaccine_unit_de_ml,
pat_age_at_time_of_vaccine_unit,
pat_age_at_time_of_vaccine,
pat_address_nf,
pat_address2_nf,
pat_address2,
pat_address1_nf,
pat_address1,
pat_address,
pat_aborginal,
patptnr_version,
patptnr_user_modified,
patptnr_user_created,
patptnr_spr_id,
patptnr_record_id,
patptnr_partner_name,
patptnr_partner_initial,
patptnr_partner_dob_rdm,
patptnr_partner_dob_fmt,
patptnr_partner_dob,
patptnr_partner_contact_details,
patptnr_inq_rec_id,
patptnr_entity_updated,
patptnr_date_modified,
patptnr_date_created,
patptnr_comp_rec_id,
patptnr_biological_father_age_unit_de_ml,
patptnr_biological_father_age_unit,
patptnr_biological_father_age,
patptnr_ari_rec_id,
patdth_version,
patdth_user_modified,
patdth_user_created,
patdth_spr_id,
patdth_record_id,
patdth_patdeathdatefmt,
patdth_patdeathdate_tz,
patdth_patdeathdate_nf,
patdth_patdeathdate,
patdth_patdeath_lang,
patdth_patautopsyyesno_nf,
patdth_patautopsyyesno_de_ml,
patdth_patautopsyyesno,
patdth_patautopsydatefmt,
patdth_patautopsydate,
patdth_meddra_pt_decode,
patdth_meddra_pt_code,
patdth_meddra_llt_decode,
patdth_meddra_llt_code,
patdth_inq_rec_id,
patdth_ext_clob_fld,
patdth_entity_updated,
patdth_date_modified,
patdth_date_created,
patdth_comp_rec_id,
patdth_autopsy_done_manual,
patdth_ari_rec_id,
patapsy_version,
patapsy_user_modified,
patapsy_user_created,
patapsy_spr_id,
patapsy_record_id,
patapsy_patdetautopsymeddraver_lang,
patapsy_patdetautopsymeddraver,
patapsy_patdetautopsylevel,
patapsy_patdetautopsy_ptcode,
patapsy_patdetautopsy_lang,
patapsy_patdetautopsy_decode,
patapsy_patdetautopsy_code,
patapsy_patdetautopsy,
patapsy_patautopsy_lang,
patapsy_inq_rec_id,
patapsy_fk_apd_rec_id,
patapsy_ext_clob_fld,
patapsy_entity_updated,
patapsy_date_modified,
patapsy_date_created,
patapsy_comp_rec_id,
patapsy_coding_type,
patapsy_coding_comments,
patapsy_autopsycause_coded_flag,
patapsy_ari_rec_id,
par_version,
par_user_modified,
par_user_created,
par_spr_id,
par_record_id,
par_paternal_exposure,
par_parentweight_unit_de_ml,
par_parentweight_unit,
par_parentweight_lang,
par_parentweight,
par_parentsex_nf,
par_parentsex_de_ml,
par_parentsex,
par_parentmedrelevanttext_lang,
par_parentmedrelevanttext,
par_parentlastmensdatefmt,
par_parentlastmensdate_tz,
par_parentlastmensdate_nf,
par_parentlastmensdate,
par_parentidentification_nf,
par_parentidentification_lang,
par_parentidentification,
par_parentheight_unit_de_ml,
par_parentheight_unit,
par_parentheight_lang,
par_parentheight,
par_parentdobfmt,
par_parentdob_tz,
par_parentdob_rdm,
par_parentdob_nf,
par_parentdob,
par_parentageunit_de_ml,
par_parentageunit,
par_parentage_lang,
par_parentage,
par_parent_lang,
par_parent_age_at_vaccine_unit_de_ml,
par_parent_age_at_vaccine_unit,
par_parent_age_at_vaccine,
par_par_ethnic_origin_de_ml,
par_par_ethnic_origin,
par_inq_rec_id,
par_ext_clob_fld,
par_entity_updated,
par_duration_unit,
par_duration,
par_date_modified,
par_date_created,
par_consent_to_contact_parent_de_ml,
par_consent_to_contact_parent,
par_comp_rec_id,
par_ari_rec_id,
dthcause_version,
dthcause_uuid,
dthcause_user_modified,
dthcause_user_created,
dthcause_spr_id,
dthcause_record_id,
dthcause_patdeathreportlevel,
dthcause_patdeathreport_ptcode,
dthcause_patdeathreport_lang,
dthcause_patdeathreport_decode,
dthcause_patdeathreport_code,
dthcause_patdeathreport,
dthcause_patdeathrepmeddraver_lang,
dthcause_patdeathrepmeddraver,
dthcause_patdeathcause_lang,
dthcause_inq_rec_id,
dthcause_fk_apd_rec_id,
dthcause_ext_clob_fld,
dthcause_entity_updated,
dthcause_deathcause_coded_flag,
dthcause_date_modified,
dthcause_date_created,
dthcause_comp_rec_id,
dthcause_coding_type,
dthcause_coding_comments,
dthcause_ari_rec_id
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP 
WHERE TO_DATE(LS_DB_PATIENT.PROCESSING_DT) < TO_DATE(LS_DB_PATIENT_TMP.PROCESSING_DT)
AND LS_DB_PATIENT.INTEGRATION_ID = LS_DB_PATIENT_TMP.INTEGRATION_ID
AND LS_DB_PATIENT.pat_ARI_REC_ID = LS_DB_PATIENT_TMP.pat_ARI_REC_ID
AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_PATIENT.patapsy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.patptnr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.patdth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.pat_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.par_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_PATIENT.dthcause_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_PATIENT_TMP.patapsy_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.patptnr_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.patdth_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.pat_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.par_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_PATIENT_TMP.dthcause_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;

DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT TGT
WHERE  ( dthcause_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_death_cause') OR par_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent') OR pat_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient') OR patapsy_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_autopsy') OR patdth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_death') OR patptnr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_partner')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( dthcause_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_death_cause') OR par_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_parent') OR pat_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient') OR patapsy_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_autopsy') OR patdth_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_death') OR patptnr_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_PATIENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_patient_partner')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PATIENT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PATIENT'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_PATIENT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT'),
                'LSDB','Case','LS_DB_PATIENT',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_PATIENT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
