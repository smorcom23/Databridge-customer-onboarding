
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_DEVICE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE'),
                'LSDB','Case','LS_DB_DEVICE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_DEVICE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_DEVICE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  As select RECORD_ID,'lsmv_device' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_device WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_device_manufacturer' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_manufacturer WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10','10008','1002','1002','1002','1002','1002','10047','10048','10057','10058','1008','1008','10118','1013','1016','1021','1021','1021','1021','15','15','2011','2015','2031','5015','7077','7077','823','823','823','823','824','827','829','9606','9606','9749','9752','9862','9863','9864','9918','9925','9978','9979')
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

select DISTINCT record_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT record_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_manufacturer WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
                AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT fk_agx_device_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_device_manufacturer WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_DEVICE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_device_manufacturer_SUBSET AS 
(
select * from 
    (SELECT  
    add_manfact_narrative_info  dvcmnf_add_manfact_narrative_info,additional_manfact_narrative  dvcmnf_additional_manfact_narrative,ari_rec_id  dvcmnf_ari_rec_id,city  dvcmnf_city,component_code  dvcmnf_component_code,conclusions  dvcmnf_conclusions,corrected_data  dvcmnf_corrected_data,corrected_data_info  dvcmnf_corrected_data_info,country  dvcmnf_country,date_created  dvcmnf_date_created,date_modified  dvcmnf_date_modified,death  dvcmnf_death,device_code  dvcmnf_device_code,device_evaluated_by_manf  dvcmnf_device_evaluated_by_manf,device_manuf_date  dvcmnf_device_manuf_date,eval_no_code  dvcmnf_eval_no_code,eval_summary_attached  dvcmnf_eval_summary_attached,event_summarized  dvcmnf_event_summarized,fk_agx_device_rec_id  dvcmnf_fk_agx_device_rec_id,followup_type  dvcmnf_followup_type,health_impact_code  dvcmnf_health_impact_code,inq_rec_id  dvcmnf_inq_rec_id,is_additional_manufacturer  dvcmnf_is_additional_manufacturer,is_corrected_data  dvcmnf_is_corrected_data,labeled_single_use  dvcmnf_labeled_single_use,mal_function  dvcmnf_mal_function,manufact_method  dvcmnf_manufact_method,manufacturer_address  dvcmnf_manufacturer_address,manufacturer_as_coded  dvcmnf_manufacturer_as_coded,manufacturer_as_reported  dvcmnf_manufacturer_as_reported,manufacturer_rec_id  dvcmnf_manufacturer_rec_id,manufacturer_record_id  dvcmnf_manufacturer_record_id,patient_code  dvcmnf_patient_code,record_id  dvcmnf_record_id,remedial_action_type  dvcmnf_remedial_action_type,remedial_other_action_type  dvcmnf_remedial_other_action_type,removal_report_number  dvcmnf_removal_report_number,reportable_event_type  dvcmnf_reportable_event_type,results  dvcmnf_results,serious_injury  dvcmnf_serious_injury,spr_id  dvcmnf_spr_id,state  dvcmnf_state,summary_report  dvcmnf_summary_report,usage_of_device  dvcmnf_usage_of_device,user_created  dvcmnf_user_created,user_modified  dvcmnf_user_modified,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_device_manufacturer
WHERE ( record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_agx_device_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_manufacturer')
  ) where REC_RANK=1 )
  , lsmv_device_SUBSET AS 
(
select * from 
    (SELECT  
    aer_number  dvc_aer_number,approval_no  dvc_approval_no,approx_age_device  dvc_approx_age_device,ari_rec_id  dvc_ari_rec_id,associated_with_distributor  dvc_associated_with_distributor,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='2015' AND CD=CAST(associated_with_distributor AS VARCHAR(100)) )dvc_associated_with_distributor_de_ml , authorised_represent_recid  dvc_authorised_represent_recid,authorised_representative  dvc_authorised_representative,awar_event_date  dvc_awar_event_date,basic_udi_di  dvc_basic_udi_di,biological_device  dvc_biological_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10058' AND CD=CAST(biological_device AS VARCHAR(100)) )dvc_biological_device_de_ml , brand_name  dvc_brand_name,brand_name_jp  dvc_brand_name_jp,brand_name_nf  dvc_brand_name_nf,c_other_operator  dvc_c_other_operator,catalogue_number  dvc_catalogue_number,charecterization  dvc_charecterization,class_of_device  dvc_class_of_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10057' AND CD=CAST(class_of_device AS VARCHAR(100)) )dvc_class_of_device_de_ml , common_device_name  dvc_common_device_name,common_device_name_nf  dvc_common_device_name_nf,comp_rec_id  dvc_comp_rec_id,component_code  dvc_component_code,concomitant_medical_product  dvc_concomitant_medical_product,concomitant_therapy  dvc_concomitant_therapy,conditional_time_limited_authorization  dvc_conditional_time_limited_authorization,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10118' AND CD=CAST(conditional_time_limited_authorization AS VARCHAR(100)) )dvc_conditional_time_limited_authorization_de_ml , contact_person  dvc_contact_person,correction_remove_reporting_no  dvc_correction_remove_reporting_no,cuname_for_comp_auth  dvc_cuname_for_comp_auth,current_dev_location  dvc_current_dev_location,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9978' AND CD=CAST(current_dev_location AS VARCHAR(100)) )dvc_current_dev_location_de_ml , current_location_of_device  dvc_current_location_of_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='824' AND CD=CAST(current_location_of_device AS VARCHAR(100)) )dvc_current_location_of_device_de_ml , date_created  dvc_date_created,date_expected_next_report  dvc_date_expected_next_report,date_expected_next_report_fmt  dvc_date_expected_next_report_fmt,date_explanted_fmt  dvc_date_explanted_fmt,date_explanted_to  dvc_date_explanted_to,date_implanted_fmt  dvc_date_implanted_fmt,date_implanted_to  dvc_date_implanted_to,date_modified  dvc_date_modified,date_of_this_report  dvc_date_of_this_report,date_returned_to_manufactu  dvc_date_returned_to_manufactu,date_returned_to_manufactu_fmt  dvc_date_returned_to_manufactu_fmt,device_age  dvc_device_age,device_age_unit  dvc_device_age_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1016' AND CD=CAST(device_age_unit AS VARCHAR(100)) )dvc_device_age_unit_de_ml , device_availablefor_evaluation  dvc_device_availablefor_evaluation,device_brandname_codedflag  dvc_device_brandname_codedflag,device_cmnt_termid_version  dvc_device_cmnt_termid_version,device_code  dvc_device_code,device_component_name  dvc_device_component_name,device_component_termid  dvc_device_component_termid,device_detail_info  dvc_device_detail_info,device_evaluateby_manufacturer  dvc_device_evaluateby_manufacturer,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9862' AND CD=CAST(device_evaluateby_manufacturer AS VARCHAR(100)) )dvc_device_evaluateby_manufacturer_de_ml , device_evaluation  dvc_device_evaluation,device_manufacture_date  dvc_device_manufacture_date,device_manufacture_date_fmt  dvc_device_manufacture_date_fmt,device_manufacture_site  dvc_device_manufacture_site,device_reprocessed_and_reused  dvc_device_reprocessed_and_reused,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1008' AND CD=CAST(device_reprocessed_and_reused AS VARCHAR(100)) )dvc_device_reprocessed_and_reused_de_ml , device_reprocessor_unit  dvc_device_reprocessor_unit,device_type  dvc_device_type,device_used_for  dvc_device_used_for,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='823' AND CD=CAST(device_used_for AS VARCHAR(100)) )dvc_device_used_for_de_ml , duration_of_implantation  dvc_duration_of_implantation,duration_of_implantation_unit  dvc_duration_of_implantation_unit,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10' AND CD=CAST(duration_of_implantation_unit AS VARCHAR(100)) )dvc_duration_of_implantation_unit_de_ml , entity_updated  dvc_entity_updated,eudamed_fsca_ref_num  dvc_eudamed_fsca_ref_num,eudamed_ref_num  dvc_eudamed_ref_num,evaluation_term  dvc_evaluation_term,evaluation_type  dvc_evaluation_type,event_change_status  dvc_event_change_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(event_change_status AS VARCHAR(100)) )dvc_event_change_status_de_ml , event_occur_location  dvc_event_occur_location,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9864' AND CD=CAST(event_occur_location AS VARCHAR(100)) )dvc_event_occur_location_de_ml , event_occur_status  dvc_event_occur_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='15' AND CD=CAST(event_occur_status AS VARCHAR(100)) )dvc_event_occur_status_de_ml , expiration_date  dvc_expiration_date,expiration_date_fmt  dvc_expiration_date_fmt,explanation_form_missing_code_a  dvc_explanation_form_missing_code_a,explanation_form_missing_code_b  dvc_explanation_form_missing_code_b,explanation_form_missing_code_c  dvc_explanation_form_missing_code_c,explanation_form_missing_code_d  dvc_explanation_form_missing_code_d,explanation_form_missing_code_g  dvc_explanation_form_missing_code_g,explant_facility  dvc_explant_facility,explanted_date  dvc_explanted_date,explanted_date_fmt  dvc_explanted_date_fmt,fda_reg_num  dvc_fda_reg_num,final_nonreportable  dvc_final_nonreportable,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='823' AND CD=CAST(final_nonreportable AS VARCHAR(100)) )dvc_final_nonreportable_de_ml , final_reportable  dvc_final_reportable,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='823' AND CD=CAST(final_reportable AS VARCHAR(100)) )dvc_final_reportable_de_ml , firmware_version  dvc_firmware_version,fk_ad_rec_id  dvc_fk_ad_rec_id,follow_up_add_info  dvc_follow_up_add_info,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(follow_up_add_info AS VARCHAR(100)) )dvc_follow_up_add_info_de_ml , follow_up_correction  dvc_follow_up_correction,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(follow_up_correction AS VARCHAR(100)) )dvc_follow_up_correction_de_ml , follow_up_device_evaluation  dvc_follow_up_device_evaluation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(follow_up_device_evaluation AS VARCHAR(100)) )dvc_follow_up_device_evaluation_de_ml , follow_up_response_to_fda  dvc_follow_up_response_to_fda,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1021' AND CD=CAST(follow_up_response_to_fda AS VARCHAR(100)) )dvc_follow_up_response_to_fda_de_ml , follow_up_type  dvc_follow_up_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9752' AND CD=CAST(follow_up_type AS VARCHAR(100)) )dvc_follow_up_type_de_ml , general_name  dvc_general_name,general_name_code  dvc_general_name_code,general_name_sf  dvc_general_name_sf,generic  dvc_generic,health_impact_code  dvc_health_impact_code,if_not_returned  dvc_if_not_returned,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10047' AND CD=CAST(if_not_returned AS VARCHAR(100)) )dvc_if_not_returned_de_ml , if_not_returned_sf  dvc_if_not_returned_sf,if_remedial_action_initiated  dvc_if_remedial_action_initiated,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9749' AND CD=CAST(if_remedial_action_initiated AS VARCHAR(100)) )dvc_if_remedial_action_initiated_de_ml , implant_facility  dvc_implant_facility,implanted_date  dvc_implanted_date,implanted_date_fmt  dvc_implanted_date_fmt,inq_rec_id  dvc_inq_rec_id,interventionrequired  dvc_interventionrequired,is_prod_combination  dvc_is_prod_combination,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(is_prod_combination AS VARCHAR(100)) )dvc_is_prod_combination_de_ml , is_route_caused_confirmed  dvc_is_route_caused_confirmed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(is_route_caused_confirmed AS VARCHAR(100)) )dvc_is_route_caused_confirmed_de_ml , labelled_for_single_use  dvc_labelled_for_single_use,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9606' AND CD=CAST(labelled_for_single_use AS VARCHAR(100)) )dvc_labelled_for_single_use_de_ml , lot_number  dvc_lot_number,mal_function  dvc_mal_function,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9606' AND CD=CAST(mal_function AS VARCHAR(100)) )dvc_mal_function_de_ml , manuf_name_addres  dvc_manuf_name_addres,manufacture_fsca_ref_num  dvc_manufacture_fsca_ref_num,manufacturer  dvc_manufacturer,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='2031' AND CD=CAST(manufacturer AS VARCHAR(100)) )dvc_manufacturer_de_ml , manufacturer_name  dvc_manufacturer_name,manufacturers_city  dvc_manufacturers_city,manufacturers_state  dvc_manufacturers_state,mfr_number  dvc_mfr_number,model_number  dvc_model_number,nca_fsca_ref_num  dvc_nca_fsca_ref_num,nca_local_ref_num  dvc_nca_local_ref_num,needle_type  dvc_needle_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10008' AND CD=CAST(needle_type AS VARCHAR(100)) )dvc_needle_type_de_ml , no_patient_involved  dvc_no_patient_involved,nomenclature_code  dvc_nomenclature_code,nomenclature_decode  dvc_nomenclature_decode,nomenclature_sys_other_text  dvc_nomenclature_sys_other_text,nomenclature_system  dvc_nomenclature_system,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9925' AND CD=CAST(nomenclature_system AS VARCHAR(100)) )dvc_nomenclature_system_de_ml , nomenclature_text  dvc_nomenclature_text,operator_of_device  dvc_operator_of_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9918' AND CD=CAST(operator_of_device AS VARCHAR(100)) )dvc_operator_of_device_de_ml , other_device_loc  dvc_other_device_loc,other_event_occur_location  dvc_other_event_occur_location,other_identifing_info  dvc_other_identifing_info,other_number  dvc_other_number,other_operator_dev_text  dvc_other_operator_dev_text,other_please_specify  dvc_other_please_specify,other_usage_of_device  dvc_other_usage_of_device,patient_code  dvc_patient_code,phone_number  dvc_phone_number,pmcf_pmpf_eudamed_id  dvc_pmcf_pmpf_eudamed_id,primary_device_flag  dvc_primary_device_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(primary_device_flag AS VARCHAR(100)) )dvc_primary_device_flag_de_ml , priority_summary_id  dvc_priority_summary_id,pro_code  dvc_pro_code,procedure_description  dvc_procedure_description,procedure_or_surgery_name  dvc_procedure_or_surgery_name,procedure_surgery_date  dvc_procedure_surgery_date,procedure_surgery_date_fmt  dvc_procedure_surgery_date_fmt,prod_artg_num  dvc_prod_artg_num,prod_device_cenum  dvc_prod_device_cenum,prod_return_date  dvc_prod_return_date,prod_return_date_fmt  dvc_prod_return_date_fmt,product_available_for_eva  dvc_product_available_for_eva,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10048' AND CD=CAST(product_available_for_eva AS VARCHAR(100)) )dvc_product_available_for_eva_de_ml , product_charecterisation  dvc_product_charecterisation,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1013' AND CD=CAST(product_charecterisation AS VARCHAR(100)) )dvc_product_charecterisation_de_ml , product_desc  dvc_product_desc,product_flag  dvc_product_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='5015' AND CD=CAST(product_flag AS VARCHAR(100)) )dvc_product_flag_de_ml , product_record_id  dvc_product_record_id,product_type  dvc_product_type,rational_review  dvc_rational_review,reason_evaluation_not_provided  dvc_reason_evaluation_not_provided,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9863' AND CD=CAST(reason_evaluation_not_provided AS VARCHAR(100)) )dvc_reason_evaluation_not_provided_de_ml , record_id  dvc_record_id,record_id_of_comp_auth  dvc_record_id_of_comp_auth,record_id_please_specify  dvc_record_id_please_specify,relevent_accessories_device  dvc_relevent_accessories_device,relevent_associated_device  dvc_relevent_associated_device,remedial_action_inspection  dvc_remedial_action_inspection,remedial_action_modify_adjust  dvc_remedial_action_modify_adjust,remedial_action_notify  dvc_remedial_action_notify,remedial_action_other  dvc_remedial_action_other,remedial_action_pat_monitor  dvc_remedial_action_pat_monitor,remedial_action_recall  dvc_remedial_action_recall,remedial_action_relabel  dvc_remedial_action_relabel,remedial_action_repair  dvc_remedial_action_repair,remedial_action_replace  dvc_remedial_action_replace,remedial_other  dvc_remedial_other,report_sent_fda_date  dvc_report_sent_fda_date,report_sent_manuf_date  dvc_report_sent_manuf_date,report_sent_to_fda  dvc_report_sent_to_fda,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(report_sent_to_fda AS VARCHAR(100)) )dvc_report_sent_to_fda_de_ml , report_sent_to_manuf  dvc_report_sent_to_manuf,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(report_sent_to_manuf AS VARCHAR(100)) )dvc_report_sent_to_manuf_de_ml , reportable_event_type  dvc_reportable_event_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='2011' AND CD=CAST(reportable_event_type AS VARCHAR(100)) )dvc_reportable_event_type_de_ml , reported_trade_name  dvc_reported_trade_name,reprocessor_contact_address  dvc_reprocessor_contact_address,result_of_assessment  dvc_result_of_assessment,risk_assessment_reviewed  dvc_risk_assessment_reviewed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(risk_assessment_reviewed AS VARCHAR(100)) )dvc_risk_assessment_reviewed_de_ml , risk_assment_been_reviewed  dvc_risk_assment_been_reviewed,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(risk_assment_been_reviewed AS VARCHAR(100)) )dvc_risk_assment_been_reviewed_de_ml , serial_number  dvc_serial_number,software_version  dvc_software_version,software_version_number  dvc_software_version_number,someone_operating_device  dvc_someone_operating_device,spr_id  dvc_spr_id,submitter_of_report  dvc_submitter_of_report,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9979' AND CD=CAST(submitter_of_report AS VARCHAR(100)) )dvc_submitter_of_report_de_ml , terminology_code  dvc_terminology_code,third_party_service  dvc_third_party_service,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1008' AND CD=CAST(third_party_service AS VARCHAR(100)) )dvc_third_party_service_de_ml , type_of_report  dvc_type_of_report,type_of_report_followup  dvc_type_of_report_followup,udi_number  dvc_udi_number,udi_prod_identifier  dvc_udi_prod_identifier,uf_importer  dvc_uf_importer,uf_importer_rec_id  dvc_uf_importer_rec_id,uf_or_dist_report_number  dvc_uf_or_dist_report_number,uf_or_imp_name_address  dvc_uf_or_imp_name_address,uf_or_importer_report_no  dvc_uf_or_importer_report_no,units_use_udi  dvc_units_use_udi,usage_of_device  dvc_usage_of_device,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='823' AND CD=CAST(usage_of_device AS VARCHAR(100)) )dvc_usage_of_device_de_ml , user_created  dvc_user_created,user_facility_or_distributor  dvc_user_facility_or_distributor,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='827' AND CD=CAST(user_facility_or_distributor AS VARCHAR(100)) )dvc_user_facility_or_distributor_de_ml , user_facility_or_importer  dvc_user_facility_or_importer,user_modified  dvc_user_modified,when_did_device_problem_occur  dvc_when_did_device_problem_occur,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='829' AND CD=CAST(when_did_device_problem_occur AS VARCHAR(100)) )dvc_when_did_device_problem_occur_de_ml , row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_device
WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_device_manufacturer_SUBSET.dvcmnf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_device_SUBSET.dvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO            ,lsmv_device_SUBSET.dvc_USER_MODIFIED USER_MODIFIED,lsmv_device_SUBSET.dvc_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE     ,lsmv_device_SUBSET.dvc_USER_CREATED CREATED_BY,lsmv_device_SUBSET.dvc_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_device_SUBSET.dvc_when_did_device_problem_occur_de_ml  ,lsmv_device_SUBSET.dvc_when_did_device_problem_occur  ,lsmv_device_SUBSET.dvc_user_modified  ,lsmv_device_SUBSET.dvc_user_facility_or_importer  ,lsmv_device_SUBSET.dvc_user_facility_or_distributor_de_ml  ,lsmv_device_SUBSET.dvc_user_facility_or_distributor  ,lsmv_device_SUBSET.dvc_user_created  ,lsmv_device_SUBSET.dvc_usage_of_device_de_ml  ,lsmv_device_SUBSET.dvc_usage_of_device  ,lsmv_device_SUBSET.dvc_units_use_udi  ,lsmv_device_SUBSET.dvc_uf_or_importer_report_no  ,lsmv_device_SUBSET.dvc_uf_or_imp_name_address  ,lsmv_device_SUBSET.dvc_uf_or_dist_report_number  ,lsmv_device_SUBSET.dvc_uf_importer_rec_id  ,lsmv_device_SUBSET.dvc_uf_importer  ,lsmv_device_SUBSET.dvc_udi_prod_identifier  ,lsmv_device_SUBSET.dvc_udi_number  ,lsmv_device_SUBSET.dvc_type_of_report_followup  ,lsmv_device_SUBSET.dvc_type_of_report  ,lsmv_device_SUBSET.dvc_third_party_service_de_ml  ,lsmv_device_SUBSET.dvc_third_party_service  ,lsmv_device_SUBSET.dvc_terminology_code  ,lsmv_device_SUBSET.dvc_submitter_of_report_de_ml  ,lsmv_device_SUBSET.dvc_submitter_of_report  ,lsmv_device_SUBSET.dvc_spr_id  ,lsmv_device_SUBSET.dvc_someone_operating_device  ,lsmv_device_SUBSET.dvc_software_version_number  ,lsmv_device_SUBSET.dvc_software_version  ,lsmv_device_SUBSET.dvc_serial_number  ,lsmv_device_SUBSET.dvc_risk_assment_been_reviewed_de_ml  ,lsmv_device_SUBSET.dvc_risk_assment_been_reviewed  ,lsmv_device_SUBSET.dvc_risk_assessment_reviewed_de_ml  ,lsmv_device_SUBSET.dvc_risk_assessment_reviewed  ,lsmv_device_SUBSET.dvc_result_of_assessment  ,lsmv_device_SUBSET.dvc_reprocessor_contact_address  ,lsmv_device_SUBSET.dvc_reported_trade_name  ,lsmv_device_SUBSET.dvc_reportable_event_type_de_ml  ,lsmv_device_SUBSET.dvc_reportable_event_type  ,lsmv_device_SUBSET.dvc_report_sent_to_manuf_de_ml  ,lsmv_device_SUBSET.dvc_report_sent_to_manuf  ,lsmv_device_SUBSET.dvc_report_sent_to_fda_de_ml  ,lsmv_device_SUBSET.dvc_report_sent_to_fda  ,lsmv_device_SUBSET.dvc_report_sent_manuf_date  ,lsmv_device_SUBSET.dvc_report_sent_fda_date  ,lsmv_device_SUBSET.dvc_remedial_other  ,lsmv_device_SUBSET.dvc_remedial_action_replace  ,lsmv_device_SUBSET.dvc_remedial_action_repair  ,lsmv_device_SUBSET.dvc_remedial_action_relabel  ,lsmv_device_SUBSET.dvc_remedial_action_recall  ,lsmv_device_SUBSET.dvc_remedial_action_pat_monitor  ,lsmv_device_SUBSET.dvc_remedial_action_other  ,lsmv_device_SUBSET.dvc_remedial_action_notify  ,lsmv_device_SUBSET.dvc_remedial_action_modify_adjust  ,lsmv_device_SUBSET.dvc_remedial_action_inspection  ,lsmv_device_SUBSET.dvc_relevent_associated_device  ,lsmv_device_SUBSET.dvc_relevent_accessories_device  ,lsmv_device_SUBSET.dvc_record_id_please_specify  ,lsmv_device_SUBSET.dvc_record_id_of_comp_auth  ,lsmv_device_SUBSET.dvc_record_id  ,lsmv_device_SUBSET.dvc_reason_evaluation_not_provided_de_ml  ,lsmv_device_SUBSET.dvc_reason_evaluation_not_provided  ,lsmv_device_SUBSET.dvc_rational_review  ,lsmv_device_SUBSET.dvc_product_type  ,lsmv_device_SUBSET.dvc_product_record_id  ,lsmv_device_SUBSET.dvc_product_flag_de_ml  ,lsmv_device_SUBSET.dvc_product_flag  ,lsmv_device_SUBSET.dvc_product_desc  ,lsmv_device_SUBSET.dvc_product_charecterisation_de_ml  ,lsmv_device_SUBSET.dvc_product_charecterisation  ,lsmv_device_SUBSET.dvc_product_available_for_eva_de_ml  ,lsmv_device_SUBSET.dvc_product_available_for_eva  ,lsmv_device_SUBSET.dvc_prod_return_date_fmt  ,lsmv_device_SUBSET.dvc_prod_return_date  ,lsmv_device_SUBSET.dvc_prod_device_cenum  ,lsmv_device_SUBSET.dvc_prod_artg_num  ,lsmv_device_SUBSET.dvc_procedure_surgery_date_fmt  ,lsmv_device_SUBSET.dvc_procedure_surgery_date  ,lsmv_device_SUBSET.dvc_procedure_or_surgery_name  ,lsmv_device_SUBSET.dvc_procedure_description  ,lsmv_device_SUBSET.dvc_pro_code  ,lsmv_device_SUBSET.dvc_priority_summary_id  ,lsmv_device_SUBSET.dvc_primary_device_flag_de_ml  ,lsmv_device_SUBSET.dvc_primary_device_flag  ,lsmv_device_SUBSET.dvc_pmcf_pmpf_eudamed_id  ,lsmv_device_SUBSET.dvc_phone_number  ,lsmv_device_SUBSET.dvc_patient_code  ,lsmv_device_SUBSET.dvc_other_usage_of_device  ,lsmv_device_SUBSET.dvc_other_please_specify  ,lsmv_device_SUBSET.dvc_other_operator_dev_text  ,lsmv_device_SUBSET.dvc_other_number  ,lsmv_device_SUBSET.dvc_other_identifing_info  ,lsmv_device_SUBSET.dvc_other_event_occur_location  ,lsmv_device_SUBSET.dvc_other_device_loc  ,lsmv_device_SUBSET.dvc_operator_of_device_de_ml  ,lsmv_device_SUBSET.dvc_operator_of_device  ,lsmv_device_SUBSET.dvc_nomenclature_text  ,lsmv_device_SUBSET.dvc_nomenclature_system_de_ml  ,lsmv_device_SUBSET.dvc_nomenclature_system  ,lsmv_device_SUBSET.dvc_nomenclature_sys_other_text  ,lsmv_device_SUBSET.dvc_nomenclature_decode  ,lsmv_device_SUBSET.dvc_nomenclature_code  ,lsmv_device_SUBSET.dvc_no_patient_involved  ,lsmv_device_SUBSET.dvc_needle_type_de_ml  ,lsmv_device_SUBSET.dvc_needle_type  ,lsmv_device_SUBSET.dvc_nca_local_ref_num  ,lsmv_device_SUBSET.dvc_nca_fsca_ref_num  ,lsmv_device_SUBSET.dvc_model_number  ,lsmv_device_SUBSET.dvc_mfr_number  ,lsmv_device_SUBSET.dvc_manufacturers_state  ,lsmv_device_SUBSET.dvc_manufacturers_city  ,lsmv_device_SUBSET.dvc_manufacturer_name  ,lsmv_device_SUBSET.dvc_manufacturer_de_ml  ,lsmv_device_SUBSET.dvc_manufacturer  ,lsmv_device_SUBSET.dvc_manufacture_fsca_ref_num  ,lsmv_device_SUBSET.dvc_manuf_name_addres  ,lsmv_device_SUBSET.dvc_mal_function_de_ml  ,lsmv_device_SUBSET.dvc_mal_function  ,lsmv_device_SUBSET.dvc_lot_number  ,lsmv_device_SUBSET.dvc_labelled_for_single_use_de_ml  ,lsmv_device_SUBSET.dvc_labelled_for_single_use  ,lsmv_device_SUBSET.dvc_is_route_caused_confirmed_de_ml  ,lsmv_device_SUBSET.dvc_is_route_caused_confirmed  ,lsmv_device_SUBSET.dvc_is_prod_combination_de_ml  ,lsmv_device_SUBSET.dvc_is_prod_combination  ,lsmv_device_SUBSET.dvc_interventionrequired  ,lsmv_device_SUBSET.dvc_inq_rec_id  ,lsmv_device_SUBSET.dvc_implanted_date_fmt  ,lsmv_device_SUBSET.dvc_implanted_date  ,lsmv_device_SUBSET.dvc_implant_facility  ,lsmv_device_SUBSET.dvc_if_remedial_action_initiated_de_ml  ,lsmv_device_SUBSET.dvc_if_remedial_action_initiated  ,lsmv_device_SUBSET.dvc_if_not_returned_sf  ,lsmv_device_SUBSET.dvc_if_not_returned_de_ml  ,lsmv_device_SUBSET.dvc_if_not_returned  ,lsmv_device_SUBSET.dvc_health_impact_code  ,lsmv_device_SUBSET.dvc_generic  ,lsmv_device_SUBSET.dvc_general_name_sf  ,lsmv_device_SUBSET.dvc_general_name_code  ,lsmv_device_SUBSET.dvc_general_name  ,lsmv_device_SUBSET.dvc_follow_up_type_de_ml  ,lsmv_device_SUBSET.dvc_follow_up_type  ,lsmv_device_SUBSET.dvc_follow_up_response_to_fda_de_ml  ,lsmv_device_SUBSET.dvc_follow_up_response_to_fda  ,lsmv_device_SUBSET.dvc_follow_up_device_evaluation_de_ml  ,lsmv_device_SUBSET.dvc_follow_up_device_evaluation  ,lsmv_device_SUBSET.dvc_follow_up_correction_de_ml  ,lsmv_device_SUBSET.dvc_follow_up_correction  ,lsmv_device_SUBSET.dvc_follow_up_add_info_de_ml  ,lsmv_device_SUBSET.dvc_follow_up_add_info  ,lsmv_device_SUBSET.dvc_fk_ad_rec_id  ,lsmv_device_SUBSET.dvc_firmware_version  ,lsmv_device_SUBSET.dvc_final_reportable_de_ml  ,lsmv_device_SUBSET.dvc_final_reportable  ,lsmv_device_SUBSET.dvc_final_nonreportable_de_ml  ,lsmv_device_SUBSET.dvc_final_nonreportable  ,lsmv_device_SUBSET.dvc_fda_reg_num  ,lsmv_device_SUBSET.dvc_explanted_date_fmt  ,lsmv_device_SUBSET.dvc_explanted_date  ,lsmv_device_SUBSET.dvc_explant_facility  ,lsmv_device_SUBSET.dvc_explanation_form_missing_code_g  ,lsmv_device_SUBSET.dvc_explanation_form_missing_code_d  ,lsmv_device_SUBSET.dvc_explanation_form_missing_code_c  ,lsmv_device_SUBSET.dvc_explanation_form_missing_code_b  ,lsmv_device_SUBSET.dvc_explanation_form_missing_code_a  ,lsmv_device_SUBSET.dvc_expiration_date_fmt  ,lsmv_device_SUBSET.dvc_expiration_date  ,lsmv_device_SUBSET.dvc_event_occur_status_de_ml  ,lsmv_device_SUBSET.dvc_event_occur_status  ,lsmv_device_SUBSET.dvc_event_occur_location_de_ml  ,lsmv_device_SUBSET.dvc_event_occur_location  ,lsmv_device_SUBSET.dvc_event_change_status_de_ml  ,lsmv_device_SUBSET.dvc_event_change_status  ,lsmv_device_SUBSET.dvc_evaluation_type  ,lsmv_device_SUBSET.dvc_evaluation_term  ,lsmv_device_SUBSET.dvc_eudamed_ref_num  ,lsmv_device_SUBSET.dvc_eudamed_fsca_ref_num  ,lsmv_device_SUBSET.dvc_entity_updated  ,lsmv_device_SUBSET.dvc_duration_of_implantation_unit_de_ml  ,lsmv_device_SUBSET.dvc_duration_of_implantation_unit  ,lsmv_device_SUBSET.dvc_duration_of_implantation  ,lsmv_device_SUBSET.dvc_device_used_for_de_ml  ,lsmv_device_SUBSET.dvc_device_used_for  ,lsmv_device_SUBSET.dvc_device_type  ,lsmv_device_SUBSET.dvc_device_reprocessor_unit  ,lsmv_device_SUBSET.dvc_device_reprocessed_and_reused_de_ml  ,lsmv_device_SUBSET.dvc_device_reprocessed_and_reused  ,lsmv_device_SUBSET.dvc_device_manufacture_site  ,lsmv_device_SUBSET.dvc_device_manufacture_date_fmt  ,lsmv_device_SUBSET.dvc_device_manufacture_date  ,lsmv_device_SUBSET.dvc_device_evaluation  ,lsmv_device_SUBSET.dvc_device_evaluateby_manufacturer_de_ml  ,lsmv_device_SUBSET.dvc_device_evaluateby_manufacturer  ,lsmv_device_SUBSET.dvc_device_detail_info  ,lsmv_device_SUBSET.dvc_device_component_termid  ,lsmv_device_SUBSET.dvc_device_component_name  ,lsmv_device_SUBSET.dvc_device_code  ,lsmv_device_SUBSET.dvc_device_cmnt_termid_version  ,lsmv_device_SUBSET.dvc_device_brandname_codedflag  ,lsmv_device_SUBSET.dvc_device_availablefor_evaluation  ,lsmv_device_SUBSET.dvc_device_age_unit_de_ml  ,lsmv_device_SUBSET.dvc_device_age_unit  ,lsmv_device_SUBSET.dvc_device_age  ,lsmv_device_SUBSET.dvc_date_returned_to_manufactu_fmt  ,lsmv_device_SUBSET.dvc_date_returned_to_manufactu  ,lsmv_device_SUBSET.dvc_date_of_this_report  ,lsmv_device_SUBSET.dvc_date_modified  ,lsmv_device_SUBSET.dvc_date_implanted_to  ,lsmv_device_SUBSET.dvc_date_implanted_fmt  ,lsmv_device_SUBSET.dvc_date_explanted_to  ,lsmv_device_SUBSET.dvc_date_explanted_fmt  ,lsmv_device_SUBSET.dvc_date_expected_next_report_fmt  ,lsmv_device_SUBSET.dvc_date_expected_next_report  ,lsmv_device_SUBSET.dvc_date_created  ,lsmv_device_SUBSET.dvc_current_location_of_device_de_ml  ,lsmv_device_SUBSET.dvc_current_location_of_device  ,lsmv_device_SUBSET.dvc_current_dev_location_de_ml  ,lsmv_device_SUBSET.dvc_current_dev_location  ,lsmv_device_SUBSET.dvc_cuname_for_comp_auth  ,lsmv_device_SUBSET.dvc_correction_remove_reporting_no  ,lsmv_device_SUBSET.dvc_contact_person  ,lsmv_device_SUBSET.dvc_conditional_time_limited_authorization_de_ml  ,lsmv_device_SUBSET.dvc_conditional_time_limited_authorization  ,lsmv_device_SUBSET.dvc_concomitant_therapy  ,lsmv_device_SUBSET.dvc_concomitant_medical_product  ,lsmv_device_SUBSET.dvc_component_code  ,lsmv_device_SUBSET.dvc_comp_rec_id  ,lsmv_device_SUBSET.dvc_common_device_name_nf  ,lsmv_device_SUBSET.dvc_common_device_name  ,lsmv_device_SUBSET.dvc_class_of_device_de_ml  ,lsmv_device_SUBSET.dvc_class_of_device  ,lsmv_device_SUBSET.dvc_charecterization  ,lsmv_device_SUBSET.dvc_catalogue_number  ,lsmv_device_SUBSET.dvc_c_other_operator  ,lsmv_device_SUBSET.dvc_brand_name_nf  ,lsmv_device_SUBSET.dvc_brand_name_jp  ,lsmv_device_SUBSET.dvc_brand_name  ,lsmv_device_SUBSET.dvc_biological_device_de_ml  ,lsmv_device_SUBSET.dvc_biological_device  ,lsmv_device_SUBSET.dvc_basic_udi_di  ,lsmv_device_SUBSET.dvc_awar_event_date  ,lsmv_device_SUBSET.dvc_authorised_representative  ,lsmv_device_SUBSET.dvc_authorised_represent_recid  ,lsmv_device_SUBSET.dvc_associated_with_distributor_de_ml  ,lsmv_device_SUBSET.dvc_associated_with_distributor  ,lsmv_device_SUBSET.dvc_ari_rec_id  ,lsmv_device_SUBSET.dvc_approx_age_device  ,lsmv_device_SUBSET.dvc_approval_no  ,lsmv_device_SUBSET.dvc_aer_number  ,lsmv_device_manufacturer_SUBSET.dvcmnf_user_modified  ,lsmv_device_manufacturer_SUBSET.dvcmnf_user_created  ,lsmv_device_manufacturer_SUBSET.dvcmnf_usage_of_device  ,lsmv_device_manufacturer_SUBSET.dvcmnf_summary_report  ,lsmv_device_manufacturer_SUBSET.dvcmnf_state  ,lsmv_device_manufacturer_SUBSET.dvcmnf_spr_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_serious_injury  ,lsmv_device_manufacturer_SUBSET.dvcmnf_results  ,lsmv_device_manufacturer_SUBSET.dvcmnf_reportable_event_type  ,lsmv_device_manufacturer_SUBSET.dvcmnf_removal_report_number  ,lsmv_device_manufacturer_SUBSET.dvcmnf_remedial_other_action_type  ,lsmv_device_manufacturer_SUBSET.dvcmnf_remedial_action_type  ,lsmv_device_manufacturer_SUBSET.dvcmnf_record_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_patient_code  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufacturer_record_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufacturer_rec_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufacturer_as_reported  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufacturer_as_coded  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufacturer_address  ,lsmv_device_manufacturer_SUBSET.dvcmnf_manufact_method  ,lsmv_device_manufacturer_SUBSET.dvcmnf_mal_function  ,lsmv_device_manufacturer_SUBSET.dvcmnf_labeled_single_use  ,lsmv_device_manufacturer_SUBSET.dvcmnf_is_corrected_data  ,lsmv_device_manufacturer_SUBSET.dvcmnf_is_additional_manufacturer  ,lsmv_device_manufacturer_SUBSET.dvcmnf_inq_rec_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_health_impact_code  ,lsmv_device_manufacturer_SUBSET.dvcmnf_followup_type  ,lsmv_device_manufacturer_SUBSET.dvcmnf_fk_agx_device_rec_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_event_summarized  ,lsmv_device_manufacturer_SUBSET.dvcmnf_eval_summary_attached  ,lsmv_device_manufacturer_SUBSET.dvcmnf_eval_no_code  ,lsmv_device_manufacturer_SUBSET.dvcmnf_device_manuf_date  ,lsmv_device_manufacturer_SUBSET.dvcmnf_device_evaluated_by_manf  ,lsmv_device_manufacturer_SUBSET.dvcmnf_device_code  ,lsmv_device_manufacturer_SUBSET.dvcmnf_death  ,lsmv_device_manufacturer_SUBSET.dvcmnf_date_modified  ,lsmv_device_manufacturer_SUBSET.dvcmnf_date_created  ,lsmv_device_manufacturer_SUBSET.dvcmnf_country  ,lsmv_device_manufacturer_SUBSET.dvcmnf_corrected_data_info  ,lsmv_device_manufacturer_SUBSET.dvcmnf_corrected_data  ,lsmv_device_manufacturer_SUBSET.dvcmnf_conclusions  ,lsmv_device_manufacturer_SUBSET.dvcmnf_component_code  ,lsmv_device_manufacturer_SUBSET.dvcmnf_city  ,lsmv_device_manufacturer_SUBSET.dvcmnf_ari_rec_id  ,lsmv_device_manufacturer_SUBSET.dvcmnf_additional_manfact_narrative  ,lsmv_device_manufacturer_SUBSET.dvcmnf_add_manfact_narrative_info ,CONCAT( NVL(lsmv_device_manufacturer_SUBSET.dvcmnf_RECORD_ID,-1),'||',NVL(lsmv_device_SUBSET.dvc_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_device_SUBSET  LEFT JOIN lsmv_device_manufacturer_SUBSET ON lsmv_device_SUBSET.dvc_record_id=lsmv_device_manufacturer_SUBSET.dvcmnf_fk_agx_device_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_device_manufacturer_SUBSET.dvcmnf_record_id,lsmv_device_SUBSET.dvc_record_id)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP)
where target_table_name='LS_DB_DEVICE'

; 







UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE   
SET LS_DB_DEVICE.dvc_when_did_device_problem_occur_de_ml = LS_DB_DEVICE_TMP.dvc_when_did_device_problem_occur_de_ml,LS_DB_DEVICE.dvc_when_did_device_problem_occur = LS_DB_DEVICE_TMP.dvc_when_did_device_problem_occur,LS_DB_DEVICE.dvc_user_modified = LS_DB_DEVICE_TMP.dvc_user_modified,LS_DB_DEVICE.dvc_user_facility_or_importer = LS_DB_DEVICE_TMP.dvc_user_facility_or_importer,LS_DB_DEVICE.dvc_user_facility_or_distributor_de_ml = LS_DB_DEVICE_TMP.dvc_user_facility_or_distributor_de_ml,LS_DB_DEVICE.dvc_user_facility_or_distributor = LS_DB_DEVICE_TMP.dvc_user_facility_or_distributor,LS_DB_DEVICE.dvc_user_created = LS_DB_DEVICE_TMP.dvc_user_created,LS_DB_DEVICE.dvc_usage_of_device_de_ml = LS_DB_DEVICE_TMP.dvc_usage_of_device_de_ml,LS_DB_DEVICE.dvc_usage_of_device = LS_DB_DEVICE_TMP.dvc_usage_of_device,LS_DB_DEVICE.dvc_units_use_udi = LS_DB_DEVICE_TMP.dvc_units_use_udi,LS_DB_DEVICE.dvc_uf_or_importer_report_no = LS_DB_DEVICE_TMP.dvc_uf_or_importer_report_no,LS_DB_DEVICE.dvc_uf_or_imp_name_address = LS_DB_DEVICE_TMP.dvc_uf_or_imp_name_address,LS_DB_DEVICE.dvc_uf_or_dist_report_number = LS_DB_DEVICE_TMP.dvc_uf_or_dist_report_number,LS_DB_DEVICE.dvc_uf_importer_rec_id = LS_DB_DEVICE_TMP.dvc_uf_importer_rec_id,LS_DB_DEVICE.dvc_uf_importer = LS_DB_DEVICE_TMP.dvc_uf_importer,LS_DB_DEVICE.dvc_udi_prod_identifier = LS_DB_DEVICE_TMP.dvc_udi_prod_identifier,LS_DB_DEVICE.dvc_udi_number = LS_DB_DEVICE_TMP.dvc_udi_number,LS_DB_DEVICE.dvc_type_of_report_followup = LS_DB_DEVICE_TMP.dvc_type_of_report_followup,LS_DB_DEVICE.dvc_type_of_report = LS_DB_DEVICE_TMP.dvc_type_of_report,LS_DB_DEVICE.dvc_third_party_service_de_ml = LS_DB_DEVICE_TMP.dvc_third_party_service_de_ml,LS_DB_DEVICE.dvc_third_party_service = LS_DB_DEVICE_TMP.dvc_third_party_service,LS_DB_DEVICE.dvc_terminology_code = LS_DB_DEVICE_TMP.dvc_terminology_code,LS_DB_DEVICE.dvc_submitter_of_report_de_ml = LS_DB_DEVICE_TMP.dvc_submitter_of_report_de_ml,LS_DB_DEVICE.dvc_submitter_of_report = LS_DB_DEVICE_TMP.dvc_submitter_of_report,LS_DB_DEVICE.dvc_spr_id = LS_DB_DEVICE_TMP.dvc_spr_id,LS_DB_DEVICE.dvc_someone_operating_device = LS_DB_DEVICE_TMP.dvc_someone_operating_device,LS_DB_DEVICE.dvc_software_version_number = LS_DB_DEVICE_TMP.dvc_software_version_number,LS_DB_DEVICE.dvc_software_version = LS_DB_DEVICE_TMP.dvc_software_version,LS_DB_DEVICE.dvc_serial_number = LS_DB_DEVICE_TMP.dvc_serial_number,LS_DB_DEVICE.dvc_risk_assment_been_reviewed_de_ml = LS_DB_DEVICE_TMP.dvc_risk_assment_been_reviewed_de_ml,LS_DB_DEVICE.dvc_risk_assment_been_reviewed = LS_DB_DEVICE_TMP.dvc_risk_assment_been_reviewed,LS_DB_DEVICE.dvc_risk_assessment_reviewed_de_ml = LS_DB_DEVICE_TMP.dvc_risk_assessment_reviewed_de_ml,LS_DB_DEVICE.dvc_risk_assessment_reviewed = LS_DB_DEVICE_TMP.dvc_risk_assessment_reviewed,LS_DB_DEVICE.dvc_result_of_assessment = LS_DB_DEVICE_TMP.dvc_result_of_assessment,LS_DB_DEVICE.dvc_reprocessor_contact_address = LS_DB_DEVICE_TMP.dvc_reprocessor_contact_address,LS_DB_DEVICE.dvc_reported_trade_name = LS_DB_DEVICE_TMP.dvc_reported_trade_name,LS_DB_DEVICE.dvc_reportable_event_type_de_ml = LS_DB_DEVICE_TMP.dvc_reportable_event_type_de_ml,LS_DB_DEVICE.dvc_reportable_event_type = LS_DB_DEVICE_TMP.dvc_reportable_event_type,LS_DB_DEVICE.dvc_report_sent_to_manuf_de_ml = LS_DB_DEVICE_TMP.dvc_report_sent_to_manuf_de_ml,LS_DB_DEVICE.dvc_report_sent_to_manuf = LS_DB_DEVICE_TMP.dvc_report_sent_to_manuf,LS_DB_DEVICE.dvc_report_sent_to_fda_de_ml = LS_DB_DEVICE_TMP.dvc_report_sent_to_fda_de_ml,LS_DB_DEVICE.dvc_report_sent_to_fda = LS_DB_DEVICE_TMP.dvc_report_sent_to_fda,LS_DB_DEVICE.dvc_report_sent_manuf_date = LS_DB_DEVICE_TMP.dvc_report_sent_manuf_date,LS_DB_DEVICE.dvc_report_sent_fda_date = LS_DB_DEVICE_TMP.dvc_report_sent_fda_date,LS_DB_DEVICE.dvc_remedial_other = LS_DB_DEVICE_TMP.dvc_remedial_other,LS_DB_DEVICE.dvc_remedial_action_replace = LS_DB_DEVICE_TMP.dvc_remedial_action_replace,LS_DB_DEVICE.dvc_remedial_action_repair = LS_DB_DEVICE_TMP.dvc_remedial_action_repair,LS_DB_DEVICE.dvc_remedial_action_relabel = LS_DB_DEVICE_TMP.dvc_remedial_action_relabel,LS_DB_DEVICE.dvc_remedial_action_recall = LS_DB_DEVICE_TMP.dvc_remedial_action_recall,LS_DB_DEVICE.dvc_remedial_action_pat_monitor = LS_DB_DEVICE_TMP.dvc_remedial_action_pat_monitor,LS_DB_DEVICE.dvc_remedial_action_other = LS_DB_DEVICE_TMP.dvc_remedial_action_other,LS_DB_DEVICE.dvc_remedial_action_notify = LS_DB_DEVICE_TMP.dvc_remedial_action_notify,LS_DB_DEVICE.dvc_remedial_action_modify_adjust = LS_DB_DEVICE_TMP.dvc_remedial_action_modify_adjust,LS_DB_DEVICE.dvc_remedial_action_inspection = LS_DB_DEVICE_TMP.dvc_remedial_action_inspection,LS_DB_DEVICE.dvc_relevent_associated_device = LS_DB_DEVICE_TMP.dvc_relevent_associated_device,LS_DB_DEVICE.dvc_relevent_accessories_device = LS_DB_DEVICE_TMP.dvc_relevent_accessories_device,LS_DB_DEVICE.dvc_record_id_please_specify = LS_DB_DEVICE_TMP.dvc_record_id_please_specify,LS_DB_DEVICE.dvc_record_id_of_comp_auth = LS_DB_DEVICE_TMP.dvc_record_id_of_comp_auth,LS_DB_DEVICE.dvc_record_id = LS_DB_DEVICE_TMP.dvc_record_id,LS_DB_DEVICE.dvc_reason_evaluation_not_provided_de_ml = LS_DB_DEVICE_TMP.dvc_reason_evaluation_not_provided_de_ml,LS_DB_DEVICE.dvc_reason_evaluation_not_provided = LS_DB_DEVICE_TMP.dvc_reason_evaluation_not_provided,LS_DB_DEVICE.dvc_rational_review = LS_DB_DEVICE_TMP.dvc_rational_review,LS_DB_DEVICE.dvc_product_type = LS_DB_DEVICE_TMP.dvc_product_type,LS_DB_DEVICE.dvc_product_record_id = LS_DB_DEVICE_TMP.dvc_product_record_id,LS_DB_DEVICE.dvc_product_flag_de_ml = LS_DB_DEVICE_TMP.dvc_product_flag_de_ml,LS_DB_DEVICE.dvc_product_flag = LS_DB_DEVICE_TMP.dvc_product_flag,LS_DB_DEVICE.dvc_product_desc = LS_DB_DEVICE_TMP.dvc_product_desc,LS_DB_DEVICE.dvc_product_charecterisation_de_ml = LS_DB_DEVICE_TMP.dvc_product_charecterisation_de_ml,LS_DB_DEVICE.dvc_product_charecterisation = LS_DB_DEVICE_TMP.dvc_product_charecterisation,LS_DB_DEVICE.dvc_product_available_for_eva_de_ml = LS_DB_DEVICE_TMP.dvc_product_available_for_eva_de_ml,LS_DB_DEVICE.dvc_product_available_for_eva = LS_DB_DEVICE_TMP.dvc_product_available_for_eva,LS_DB_DEVICE.dvc_prod_return_date_fmt = LS_DB_DEVICE_TMP.dvc_prod_return_date_fmt,LS_DB_DEVICE.dvc_prod_return_date = LS_DB_DEVICE_TMP.dvc_prod_return_date,LS_DB_DEVICE.dvc_prod_device_cenum = LS_DB_DEVICE_TMP.dvc_prod_device_cenum,LS_DB_DEVICE.dvc_prod_artg_num = LS_DB_DEVICE_TMP.dvc_prod_artg_num,LS_DB_DEVICE.dvc_procedure_surgery_date_fmt = LS_DB_DEVICE_TMP.dvc_procedure_surgery_date_fmt,LS_DB_DEVICE.dvc_procedure_surgery_date = LS_DB_DEVICE_TMP.dvc_procedure_surgery_date,LS_DB_DEVICE.dvc_procedure_or_surgery_name = LS_DB_DEVICE_TMP.dvc_procedure_or_surgery_name,LS_DB_DEVICE.dvc_procedure_description = LS_DB_DEVICE_TMP.dvc_procedure_description,LS_DB_DEVICE.dvc_pro_code = LS_DB_DEVICE_TMP.dvc_pro_code,LS_DB_DEVICE.dvc_priority_summary_id = LS_DB_DEVICE_TMP.dvc_priority_summary_id,LS_DB_DEVICE.dvc_primary_device_flag_de_ml = LS_DB_DEVICE_TMP.dvc_primary_device_flag_de_ml,LS_DB_DEVICE.dvc_primary_device_flag = LS_DB_DEVICE_TMP.dvc_primary_device_flag,LS_DB_DEVICE.dvc_pmcf_pmpf_eudamed_id = LS_DB_DEVICE_TMP.dvc_pmcf_pmpf_eudamed_id,LS_DB_DEVICE.dvc_phone_number = LS_DB_DEVICE_TMP.dvc_phone_number,LS_DB_DEVICE.dvc_patient_code = LS_DB_DEVICE_TMP.dvc_patient_code,LS_DB_DEVICE.dvc_other_usage_of_device = LS_DB_DEVICE_TMP.dvc_other_usage_of_device,LS_DB_DEVICE.dvc_other_please_specify = LS_DB_DEVICE_TMP.dvc_other_please_specify,LS_DB_DEVICE.dvc_other_operator_dev_text = LS_DB_DEVICE_TMP.dvc_other_operator_dev_text,LS_DB_DEVICE.dvc_other_number = LS_DB_DEVICE_TMP.dvc_other_number,LS_DB_DEVICE.dvc_other_identifing_info = LS_DB_DEVICE_TMP.dvc_other_identifing_info,LS_DB_DEVICE.dvc_other_event_occur_location = LS_DB_DEVICE_TMP.dvc_other_event_occur_location,LS_DB_DEVICE.dvc_other_device_loc = LS_DB_DEVICE_TMP.dvc_other_device_loc,LS_DB_DEVICE.dvc_operator_of_device_de_ml = LS_DB_DEVICE_TMP.dvc_operator_of_device_de_ml,LS_DB_DEVICE.dvc_operator_of_device = LS_DB_DEVICE_TMP.dvc_operator_of_device,LS_DB_DEVICE.dvc_nomenclature_text = LS_DB_DEVICE_TMP.dvc_nomenclature_text,LS_DB_DEVICE.dvc_nomenclature_system_de_ml = LS_DB_DEVICE_TMP.dvc_nomenclature_system_de_ml,LS_DB_DEVICE.dvc_nomenclature_system = LS_DB_DEVICE_TMP.dvc_nomenclature_system,LS_DB_DEVICE.dvc_nomenclature_sys_other_text = LS_DB_DEVICE_TMP.dvc_nomenclature_sys_other_text,LS_DB_DEVICE.dvc_nomenclature_decode = LS_DB_DEVICE_TMP.dvc_nomenclature_decode,LS_DB_DEVICE.dvc_nomenclature_code = LS_DB_DEVICE_TMP.dvc_nomenclature_code,LS_DB_DEVICE.dvc_no_patient_involved = LS_DB_DEVICE_TMP.dvc_no_patient_involved,LS_DB_DEVICE.dvc_needle_type_de_ml = LS_DB_DEVICE_TMP.dvc_needle_type_de_ml,LS_DB_DEVICE.dvc_needle_type = LS_DB_DEVICE_TMP.dvc_needle_type,LS_DB_DEVICE.dvc_nca_local_ref_num = LS_DB_DEVICE_TMP.dvc_nca_local_ref_num,LS_DB_DEVICE.dvc_nca_fsca_ref_num = LS_DB_DEVICE_TMP.dvc_nca_fsca_ref_num,LS_DB_DEVICE.dvc_model_number = LS_DB_DEVICE_TMP.dvc_model_number,LS_DB_DEVICE.dvc_mfr_number = LS_DB_DEVICE_TMP.dvc_mfr_number,LS_DB_DEVICE.dvc_manufacturers_state = LS_DB_DEVICE_TMP.dvc_manufacturers_state,LS_DB_DEVICE.dvc_manufacturers_city = LS_DB_DEVICE_TMP.dvc_manufacturers_city,LS_DB_DEVICE.dvc_manufacturer_name = LS_DB_DEVICE_TMP.dvc_manufacturer_name,LS_DB_DEVICE.dvc_manufacturer_de_ml = LS_DB_DEVICE_TMP.dvc_manufacturer_de_ml,LS_DB_DEVICE.dvc_manufacturer = LS_DB_DEVICE_TMP.dvc_manufacturer,LS_DB_DEVICE.dvc_manufacture_fsca_ref_num = LS_DB_DEVICE_TMP.dvc_manufacture_fsca_ref_num,LS_DB_DEVICE.dvc_manuf_name_addres = LS_DB_DEVICE_TMP.dvc_manuf_name_addres,LS_DB_DEVICE.dvc_mal_function_de_ml = LS_DB_DEVICE_TMP.dvc_mal_function_de_ml,LS_DB_DEVICE.dvc_mal_function = LS_DB_DEVICE_TMP.dvc_mal_function,LS_DB_DEVICE.dvc_lot_number = LS_DB_DEVICE_TMP.dvc_lot_number,LS_DB_DEVICE.dvc_labelled_for_single_use_de_ml = LS_DB_DEVICE_TMP.dvc_labelled_for_single_use_de_ml,LS_DB_DEVICE.dvc_labelled_for_single_use = LS_DB_DEVICE_TMP.dvc_labelled_for_single_use,LS_DB_DEVICE.dvc_is_route_caused_confirmed_de_ml = LS_DB_DEVICE_TMP.dvc_is_route_caused_confirmed_de_ml,LS_DB_DEVICE.dvc_is_route_caused_confirmed = LS_DB_DEVICE_TMP.dvc_is_route_caused_confirmed,LS_DB_DEVICE.dvc_is_prod_combination_de_ml = LS_DB_DEVICE_TMP.dvc_is_prod_combination_de_ml,LS_DB_DEVICE.dvc_is_prod_combination = LS_DB_DEVICE_TMP.dvc_is_prod_combination,LS_DB_DEVICE.dvc_interventionrequired = LS_DB_DEVICE_TMP.dvc_interventionrequired,LS_DB_DEVICE.dvc_inq_rec_id = LS_DB_DEVICE_TMP.dvc_inq_rec_id,LS_DB_DEVICE.dvc_implanted_date_fmt = LS_DB_DEVICE_TMP.dvc_implanted_date_fmt,LS_DB_DEVICE.dvc_implanted_date = LS_DB_DEVICE_TMP.dvc_implanted_date,LS_DB_DEVICE.dvc_implant_facility = LS_DB_DEVICE_TMP.dvc_implant_facility,LS_DB_DEVICE.dvc_if_remedial_action_initiated_de_ml = LS_DB_DEVICE_TMP.dvc_if_remedial_action_initiated_de_ml,LS_DB_DEVICE.dvc_if_remedial_action_initiated = LS_DB_DEVICE_TMP.dvc_if_remedial_action_initiated,LS_DB_DEVICE.dvc_if_not_returned_sf = LS_DB_DEVICE_TMP.dvc_if_not_returned_sf,LS_DB_DEVICE.dvc_if_not_returned_de_ml = LS_DB_DEVICE_TMP.dvc_if_not_returned_de_ml,LS_DB_DEVICE.dvc_if_not_returned = LS_DB_DEVICE_TMP.dvc_if_not_returned,LS_DB_DEVICE.dvc_health_impact_code = LS_DB_DEVICE_TMP.dvc_health_impact_code,LS_DB_DEVICE.dvc_generic = LS_DB_DEVICE_TMP.dvc_generic,LS_DB_DEVICE.dvc_general_name_sf = LS_DB_DEVICE_TMP.dvc_general_name_sf,LS_DB_DEVICE.dvc_general_name_code = LS_DB_DEVICE_TMP.dvc_general_name_code,LS_DB_DEVICE.dvc_general_name = LS_DB_DEVICE_TMP.dvc_general_name,LS_DB_DEVICE.dvc_follow_up_type_de_ml = LS_DB_DEVICE_TMP.dvc_follow_up_type_de_ml,LS_DB_DEVICE.dvc_follow_up_type = LS_DB_DEVICE_TMP.dvc_follow_up_type,LS_DB_DEVICE.dvc_follow_up_response_to_fda_de_ml = LS_DB_DEVICE_TMP.dvc_follow_up_response_to_fda_de_ml,LS_DB_DEVICE.dvc_follow_up_response_to_fda = LS_DB_DEVICE_TMP.dvc_follow_up_response_to_fda,LS_DB_DEVICE.dvc_follow_up_device_evaluation_de_ml = LS_DB_DEVICE_TMP.dvc_follow_up_device_evaluation_de_ml,LS_DB_DEVICE.dvc_follow_up_device_evaluation = LS_DB_DEVICE_TMP.dvc_follow_up_device_evaluation,LS_DB_DEVICE.dvc_follow_up_correction_de_ml = LS_DB_DEVICE_TMP.dvc_follow_up_correction_de_ml,LS_DB_DEVICE.dvc_follow_up_correction = LS_DB_DEVICE_TMP.dvc_follow_up_correction,LS_DB_DEVICE.dvc_follow_up_add_info_de_ml = LS_DB_DEVICE_TMP.dvc_follow_up_add_info_de_ml,LS_DB_DEVICE.dvc_follow_up_add_info = LS_DB_DEVICE_TMP.dvc_follow_up_add_info,LS_DB_DEVICE.dvc_fk_ad_rec_id = LS_DB_DEVICE_TMP.dvc_fk_ad_rec_id,LS_DB_DEVICE.dvc_firmware_version = LS_DB_DEVICE_TMP.dvc_firmware_version,LS_DB_DEVICE.dvc_final_reportable_de_ml = LS_DB_DEVICE_TMP.dvc_final_reportable_de_ml,LS_DB_DEVICE.dvc_final_reportable = LS_DB_DEVICE_TMP.dvc_final_reportable,LS_DB_DEVICE.dvc_final_nonreportable_de_ml = LS_DB_DEVICE_TMP.dvc_final_nonreportable_de_ml,LS_DB_DEVICE.dvc_final_nonreportable = LS_DB_DEVICE_TMP.dvc_final_nonreportable,LS_DB_DEVICE.dvc_fda_reg_num = LS_DB_DEVICE_TMP.dvc_fda_reg_num,LS_DB_DEVICE.dvc_explanted_date_fmt = LS_DB_DEVICE_TMP.dvc_explanted_date_fmt,LS_DB_DEVICE.dvc_explanted_date = LS_DB_DEVICE_TMP.dvc_explanted_date,LS_DB_DEVICE.dvc_explant_facility = LS_DB_DEVICE_TMP.dvc_explant_facility,LS_DB_DEVICE.dvc_explanation_form_missing_code_g = LS_DB_DEVICE_TMP.dvc_explanation_form_missing_code_g,LS_DB_DEVICE.dvc_explanation_form_missing_code_d = LS_DB_DEVICE_TMP.dvc_explanation_form_missing_code_d,LS_DB_DEVICE.dvc_explanation_form_missing_code_c = LS_DB_DEVICE_TMP.dvc_explanation_form_missing_code_c,LS_DB_DEVICE.dvc_explanation_form_missing_code_b = LS_DB_DEVICE_TMP.dvc_explanation_form_missing_code_b,LS_DB_DEVICE.dvc_explanation_form_missing_code_a = LS_DB_DEVICE_TMP.dvc_explanation_form_missing_code_a,LS_DB_DEVICE.dvc_expiration_date_fmt = LS_DB_DEVICE_TMP.dvc_expiration_date_fmt,LS_DB_DEVICE.dvc_expiration_date = LS_DB_DEVICE_TMP.dvc_expiration_date,LS_DB_DEVICE.dvc_event_occur_status_de_ml = LS_DB_DEVICE_TMP.dvc_event_occur_status_de_ml,LS_DB_DEVICE.dvc_event_occur_status = LS_DB_DEVICE_TMP.dvc_event_occur_status,LS_DB_DEVICE.dvc_event_occur_location_de_ml = LS_DB_DEVICE_TMP.dvc_event_occur_location_de_ml,LS_DB_DEVICE.dvc_event_occur_location = LS_DB_DEVICE_TMP.dvc_event_occur_location,LS_DB_DEVICE.dvc_event_change_status_de_ml = LS_DB_DEVICE_TMP.dvc_event_change_status_de_ml,LS_DB_DEVICE.dvc_event_change_status = LS_DB_DEVICE_TMP.dvc_event_change_status,LS_DB_DEVICE.dvc_evaluation_type = LS_DB_DEVICE_TMP.dvc_evaluation_type,LS_DB_DEVICE.dvc_evaluation_term = LS_DB_DEVICE_TMP.dvc_evaluation_term,LS_DB_DEVICE.dvc_eudamed_ref_num = LS_DB_DEVICE_TMP.dvc_eudamed_ref_num,LS_DB_DEVICE.dvc_eudamed_fsca_ref_num = LS_DB_DEVICE_TMP.dvc_eudamed_fsca_ref_num,LS_DB_DEVICE.dvc_entity_updated = LS_DB_DEVICE_TMP.dvc_entity_updated,LS_DB_DEVICE.dvc_duration_of_implantation_unit_de_ml = LS_DB_DEVICE_TMP.dvc_duration_of_implantation_unit_de_ml,LS_DB_DEVICE.dvc_duration_of_implantation_unit = LS_DB_DEVICE_TMP.dvc_duration_of_implantation_unit,LS_DB_DEVICE.dvc_duration_of_implantation = LS_DB_DEVICE_TMP.dvc_duration_of_implantation,LS_DB_DEVICE.dvc_device_used_for_de_ml = LS_DB_DEVICE_TMP.dvc_device_used_for_de_ml,LS_DB_DEVICE.dvc_device_used_for = LS_DB_DEVICE_TMP.dvc_device_used_for,LS_DB_DEVICE.dvc_device_type = LS_DB_DEVICE_TMP.dvc_device_type,LS_DB_DEVICE.dvc_device_reprocessor_unit = LS_DB_DEVICE_TMP.dvc_device_reprocessor_unit,LS_DB_DEVICE.dvc_device_reprocessed_and_reused_de_ml = LS_DB_DEVICE_TMP.dvc_device_reprocessed_and_reused_de_ml,LS_DB_DEVICE.dvc_device_reprocessed_and_reused = LS_DB_DEVICE_TMP.dvc_device_reprocessed_and_reused,LS_DB_DEVICE.dvc_device_manufacture_site = LS_DB_DEVICE_TMP.dvc_device_manufacture_site,LS_DB_DEVICE.dvc_device_manufacture_date_fmt = LS_DB_DEVICE_TMP.dvc_device_manufacture_date_fmt,LS_DB_DEVICE.dvc_device_manufacture_date = LS_DB_DEVICE_TMP.dvc_device_manufacture_date,LS_DB_DEVICE.dvc_device_evaluation = LS_DB_DEVICE_TMP.dvc_device_evaluation,LS_DB_DEVICE.dvc_device_evaluateby_manufacturer_de_ml = LS_DB_DEVICE_TMP.dvc_device_evaluateby_manufacturer_de_ml,LS_DB_DEVICE.dvc_device_evaluateby_manufacturer = LS_DB_DEVICE_TMP.dvc_device_evaluateby_manufacturer,LS_DB_DEVICE.dvc_device_detail_info = LS_DB_DEVICE_TMP.dvc_device_detail_info,LS_DB_DEVICE.dvc_device_component_termid = LS_DB_DEVICE_TMP.dvc_device_component_termid,LS_DB_DEVICE.dvc_device_component_name = LS_DB_DEVICE_TMP.dvc_device_component_name,LS_DB_DEVICE.dvc_device_code = LS_DB_DEVICE_TMP.dvc_device_code,LS_DB_DEVICE.dvc_device_cmnt_termid_version = LS_DB_DEVICE_TMP.dvc_device_cmnt_termid_version,LS_DB_DEVICE.dvc_device_brandname_codedflag = LS_DB_DEVICE_TMP.dvc_device_brandname_codedflag,LS_DB_DEVICE.dvc_device_availablefor_evaluation = LS_DB_DEVICE_TMP.dvc_device_availablefor_evaluation,LS_DB_DEVICE.dvc_device_age_unit_de_ml = LS_DB_DEVICE_TMP.dvc_device_age_unit_de_ml,LS_DB_DEVICE.dvc_device_age_unit = LS_DB_DEVICE_TMP.dvc_device_age_unit,LS_DB_DEVICE.dvc_device_age = LS_DB_DEVICE_TMP.dvc_device_age,LS_DB_DEVICE.dvc_date_returned_to_manufactu_fmt = LS_DB_DEVICE_TMP.dvc_date_returned_to_manufactu_fmt,LS_DB_DEVICE.dvc_date_returned_to_manufactu = LS_DB_DEVICE_TMP.dvc_date_returned_to_manufactu,LS_DB_DEVICE.dvc_date_of_this_report = LS_DB_DEVICE_TMP.dvc_date_of_this_report,LS_DB_DEVICE.dvc_date_modified = LS_DB_DEVICE_TMP.dvc_date_modified,LS_DB_DEVICE.dvc_date_implanted_to = LS_DB_DEVICE_TMP.dvc_date_implanted_to,LS_DB_DEVICE.dvc_date_implanted_fmt = LS_DB_DEVICE_TMP.dvc_date_implanted_fmt,LS_DB_DEVICE.dvc_date_explanted_to = LS_DB_DEVICE_TMP.dvc_date_explanted_to,LS_DB_DEVICE.dvc_date_explanted_fmt = LS_DB_DEVICE_TMP.dvc_date_explanted_fmt,LS_DB_DEVICE.dvc_date_expected_next_report_fmt = LS_DB_DEVICE_TMP.dvc_date_expected_next_report_fmt,LS_DB_DEVICE.dvc_date_expected_next_report = LS_DB_DEVICE_TMP.dvc_date_expected_next_report,LS_DB_DEVICE.dvc_date_created = LS_DB_DEVICE_TMP.dvc_date_created,LS_DB_DEVICE.dvc_current_location_of_device_de_ml = LS_DB_DEVICE_TMP.dvc_current_location_of_device_de_ml,LS_DB_DEVICE.dvc_current_location_of_device = LS_DB_DEVICE_TMP.dvc_current_location_of_device,LS_DB_DEVICE.dvc_current_dev_location_de_ml = LS_DB_DEVICE_TMP.dvc_current_dev_location_de_ml,LS_DB_DEVICE.dvc_current_dev_location = LS_DB_DEVICE_TMP.dvc_current_dev_location,LS_DB_DEVICE.dvc_cuname_for_comp_auth = LS_DB_DEVICE_TMP.dvc_cuname_for_comp_auth,LS_DB_DEVICE.dvc_correction_remove_reporting_no = LS_DB_DEVICE_TMP.dvc_correction_remove_reporting_no,LS_DB_DEVICE.dvc_contact_person = LS_DB_DEVICE_TMP.dvc_contact_person,LS_DB_DEVICE.dvc_conditional_time_limited_authorization_de_ml = LS_DB_DEVICE_TMP.dvc_conditional_time_limited_authorization_de_ml,LS_DB_DEVICE.dvc_conditional_time_limited_authorization = LS_DB_DEVICE_TMP.dvc_conditional_time_limited_authorization,LS_DB_DEVICE.dvc_concomitant_therapy = LS_DB_DEVICE_TMP.dvc_concomitant_therapy,LS_DB_DEVICE.dvc_concomitant_medical_product = LS_DB_DEVICE_TMP.dvc_concomitant_medical_product,LS_DB_DEVICE.dvc_component_code = LS_DB_DEVICE_TMP.dvc_component_code,LS_DB_DEVICE.dvc_comp_rec_id = LS_DB_DEVICE_TMP.dvc_comp_rec_id,LS_DB_DEVICE.dvc_common_device_name_nf = LS_DB_DEVICE_TMP.dvc_common_device_name_nf,LS_DB_DEVICE.dvc_common_device_name = LS_DB_DEVICE_TMP.dvc_common_device_name,LS_DB_DEVICE.dvc_class_of_device_de_ml = LS_DB_DEVICE_TMP.dvc_class_of_device_de_ml,LS_DB_DEVICE.dvc_class_of_device = LS_DB_DEVICE_TMP.dvc_class_of_device,LS_DB_DEVICE.dvc_charecterization = LS_DB_DEVICE_TMP.dvc_charecterization,LS_DB_DEVICE.dvc_catalogue_number = LS_DB_DEVICE_TMP.dvc_catalogue_number,LS_DB_DEVICE.dvc_c_other_operator = LS_DB_DEVICE_TMP.dvc_c_other_operator,LS_DB_DEVICE.dvc_brand_name_nf = LS_DB_DEVICE_TMP.dvc_brand_name_nf,LS_DB_DEVICE.dvc_brand_name_jp = LS_DB_DEVICE_TMP.dvc_brand_name_jp,LS_DB_DEVICE.dvc_brand_name = LS_DB_DEVICE_TMP.dvc_brand_name,LS_DB_DEVICE.dvc_biological_device_de_ml = LS_DB_DEVICE_TMP.dvc_biological_device_de_ml,LS_DB_DEVICE.dvc_biological_device = LS_DB_DEVICE_TMP.dvc_biological_device,LS_DB_DEVICE.dvc_basic_udi_di = LS_DB_DEVICE_TMP.dvc_basic_udi_di,LS_DB_DEVICE.dvc_awar_event_date = LS_DB_DEVICE_TMP.dvc_awar_event_date,LS_DB_DEVICE.dvc_authorised_representative = LS_DB_DEVICE_TMP.dvc_authorised_representative,LS_DB_DEVICE.dvc_authorised_represent_recid = LS_DB_DEVICE_TMP.dvc_authorised_represent_recid,LS_DB_DEVICE.dvc_associated_with_distributor_de_ml = LS_DB_DEVICE_TMP.dvc_associated_with_distributor_de_ml,LS_DB_DEVICE.dvc_associated_with_distributor = LS_DB_DEVICE_TMP.dvc_associated_with_distributor,LS_DB_DEVICE.dvc_ari_rec_id = LS_DB_DEVICE_TMP.dvc_ari_rec_id,LS_DB_DEVICE.dvc_approx_age_device = LS_DB_DEVICE_TMP.dvc_approx_age_device,LS_DB_DEVICE.dvc_approval_no = LS_DB_DEVICE_TMP.dvc_approval_no,LS_DB_DEVICE.dvc_aer_number = LS_DB_DEVICE_TMP.dvc_aer_number,LS_DB_DEVICE.dvcmnf_user_modified = LS_DB_DEVICE_TMP.dvcmnf_user_modified,LS_DB_DEVICE.dvcmnf_user_created = LS_DB_DEVICE_TMP.dvcmnf_user_created,LS_DB_DEVICE.dvcmnf_usage_of_device = LS_DB_DEVICE_TMP.dvcmnf_usage_of_device,LS_DB_DEVICE.dvcmnf_summary_report = LS_DB_DEVICE_TMP.dvcmnf_summary_report,LS_DB_DEVICE.dvcmnf_state = LS_DB_DEVICE_TMP.dvcmnf_state,LS_DB_DEVICE.dvcmnf_spr_id = LS_DB_DEVICE_TMP.dvcmnf_spr_id,LS_DB_DEVICE.dvcmnf_serious_injury = LS_DB_DEVICE_TMP.dvcmnf_serious_injury,LS_DB_DEVICE.dvcmnf_results = LS_DB_DEVICE_TMP.dvcmnf_results,LS_DB_DEVICE.dvcmnf_reportable_event_type = LS_DB_DEVICE_TMP.dvcmnf_reportable_event_type,LS_DB_DEVICE.dvcmnf_removal_report_number = LS_DB_DEVICE_TMP.dvcmnf_removal_report_number,LS_DB_DEVICE.dvcmnf_remedial_other_action_type = LS_DB_DEVICE_TMP.dvcmnf_remedial_other_action_type,LS_DB_DEVICE.dvcmnf_remedial_action_type = LS_DB_DEVICE_TMP.dvcmnf_remedial_action_type,LS_DB_DEVICE.dvcmnf_record_id = LS_DB_DEVICE_TMP.dvcmnf_record_id,LS_DB_DEVICE.dvcmnf_patient_code = LS_DB_DEVICE_TMP.dvcmnf_patient_code,LS_DB_DEVICE.dvcmnf_manufacturer_record_id = LS_DB_DEVICE_TMP.dvcmnf_manufacturer_record_id,LS_DB_DEVICE.dvcmnf_manufacturer_rec_id = LS_DB_DEVICE_TMP.dvcmnf_manufacturer_rec_id,LS_DB_DEVICE.dvcmnf_manufacturer_as_reported = LS_DB_DEVICE_TMP.dvcmnf_manufacturer_as_reported,LS_DB_DEVICE.dvcmnf_manufacturer_as_coded = LS_DB_DEVICE_TMP.dvcmnf_manufacturer_as_coded,LS_DB_DEVICE.dvcmnf_manufacturer_address = LS_DB_DEVICE_TMP.dvcmnf_manufacturer_address,LS_DB_DEVICE.dvcmnf_manufact_method = LS_DB_DEVICE_TMP.dvcmnf_manufact_method,LS_DB_DEVICE.dvcmnf_mal_function = LS_DB_DEVICE_TMP.dvcmnf_mal_function,LS_DB_DEVICE.dvcmnf_labeled_single_use = LS_DB_DEVICE_TMP.dvcmnf_labeled_single_use,LS_DB_DEVICE.dvcmnf_is_corrected_data = LS_DB_DEVICE_TMP.dvcmnf_is_corrected_data,LS_DB_DEVICE.dvcmnf_is_additional_manufacturer = LS_DB_DEVICE_TMP.dvcmnf_is_additional_manufacturer,LS_DB_DEVICE.dvcmnf_inq_rec_id = LS_DB_DEVICE_TMP.dvcmnf_inq_rec_id,LS_DB_DEVICE.dvcmnf_health_impact_code = LS_DB_DEVICE_TMP.dvcmnf_health_impact_code,LS_DB_DEVICE.dvcmnf_followup_type = LS_DB_DEVICE_TMP.dvcmnf_followup_type,LS_DB_DEVICE.dvcmnf_fk_agx_device_rec_id = LS_DB_DEVICE_TMP.dvcmnf_fk_agx_device_rec_id,LS_DB_DEVICE.dvcmnf_event_summarized = LS_DB_DEVICE_TMP.dvcmnf_event_summarized,LS_DB_DEVICE.dvcmnf_eval_summary_attached = LS_DB_DEVICE_TMP.dvcmnf_eval_summary_attached,LS_DB_DEVICE.dvcmnf_eval_no_code = LS_DB_DEVICE_TMP.dvcmnf_eval_no_code,LS_DB_DEVICE.dvcmnf_device_manuf_date = LS_DB_DEVICE_TMP.dvcmnf_device_manuf_date,LS_DB_DEVICE.dvcmnf_device_evaluated_by_manf = LS_DB_DEVICE_TMP.dvcmnf_device_evaluated_by_manf,LS_DB_DEVICE.dvcmnf_device_code = LS_DB_DEVICE_TMP.dvcmnf_device_code,LS_DB_DEVICE.dvcmnf_death = LS_DB_DEVICE_TMP.dvcmnf_death,LS_DB_DEVICE.dvcmnf_date_modified = LS_DB_DEVICE_TMP.dvcmnf_date_modified,LS_DB_DEVICE.dvcmnf_date_created = LS_DB_DEVICE_TMP.dvcmnf_date_created,LS_DB_DEVICE.dvcmnf_country = LS_DB_DEVICE_TMP.dvcmnf_country,LS_DB_DEVICE.dvcmnf_corrected_data_info = LS_DB_DEVICE_TMP.dvcmnf_corrected_data_info,LS_DB_DEVICE.dvcmnf_corrected_data = LS_DB_DEVICE_TMP.dvcmnf_corrected_data,LS_DB_DEVICE.dvcmnf_conclusions = LS_DB_DEVICE_TMP.dvcmnf_conclusions,LS_DB_DEVICE.dvcmnf_component_code = LS_DB_DEVICE_TMP.dvcmnf_component_code,LS_DB_DEVICE.dvcmnf_city = LS_DB_DEVICE_TMP.dvcmnf_city,LS_DB_DEVICE.dvcmnf_ari_rec_id = LS_DB_DEVICE_TMP.dvcmnf_ari_rec_id,LS_DB_DEVICE.dvcmnf_additional_manfact_narrative = LS_DB_DEVICE_TMP.dvcmnf_additional_manfact_narrative,LS_DB_DEVICE.dvcmnf_add_manfact_narrative_info = LS_DB_DEVICE_TMP.dvcmnf_add_manfact_narrative_info,
LS_DB_DEVICE.PROCESSING_DT = LS_DB_DEVICE_TMP.PROCESSING_DT,
LS_DB_DEVICE.receipt_id     =LS_DB_DEVICE_TMP.receipt_id    ,
LS_DB_DEVICE.case_no        =LS_DB_DEVICE_TMP.case_no           ,
LS_DB_DEVICE.case_version   =LS_DB_DEVICE_TMP.case_version      ,
LS_DB_DEVICE.version_no     =LS_DB_DEVICE_TMP.version_no        ,
LS_DB_DEVICE.user_modified  =LS_DB_DEVICE_TMP.user_modified     ,
LS_DB_DEVICE.date_modified  =LS_DB_DEVICE_TMP.date_modified     ,
LS_DB_DEVICE.expiry_date    =LS_DB_DEVICE_TMP.expiry_date       ,
LS_DB_DEVICE.created_by     =LS_DB_DEVICE_TMP.created_by        ,
LS_DB_DEVICE.created_dt     =LS_DB_DEVICE_TMP.created_dt        ,
LS_DB_DEVICE.load_ts        =LS_DB_DEVICE_TMP.load_ts          
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP 
WHERE LS_DB_DEVICE.INTEGRATION_ID = LS_DB_DEVICE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_DEVICE_TMP.PROCESSING_DT = LS_DB_DEVICE.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_DEVICE.dvcmnf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DEVICE.dvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DEVICE_TMP.dvcmnf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DEVICE_TMP.dvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_DEVICE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_DEVICE.dvc_record_id ,LS_DB_DEVICE.INTEGRATION_ID
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP 
ON LS_DB_DEVICE.dvc_record_id=LS_DB_DEVICE_TMP.dvc_record_id
AND LS_DB_DEVICE.INTEGRATION_ID = LS_DB_DEVICE_TMP.INTEGRATION_ID 
where LS_DB_DEVICE_TMP.INTEGRATION_ID  is null AND LS_DB_DEVICE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_DEVICE.dvc_record_id in (select dvc_record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP )
) TMP where LS_DB_DEVICE.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_DEVICE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_DEVICE.dvc_record_id ,LS_DB_DEVICE.INTEGRATION_ID
    FROM               ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP 
    ON LS_DB_DEVICE.dvc_record_id=LS_DB_DEVICE_TMP.dvc_record_id
    AND LS_DB_DEVICE.INTEGRATION_ID = LS_DB_DEVICE_TMP.INTEGRATION_ID 
    where LS_DB_DEVICE_TMP.INTEGRATION_ID  is null AND LS_DB_DEVICE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_DEVICE.dvc_record_id in (select dvc_record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
)
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;






INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE
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
integration_id ,dvc_when_did_device_problem_occur_de_ml,
dvc_when_did_device_problem_occur,
dvc_user_modified,
dvc_user_facility_or_importer,
dvc_user_facility_or_distributor_de_ml,
dvc_user_facility_or_distributor,
dvc_user_created,
dvc_usage_of_device_de_ml,
dvc_usage_of_device,
dvc_units_use_udi,
dvc_uf_or_importer_report_no,
dvc_uf_or_imp_name_address,
dvc_uf_or_dist_report_number,
dvc_uf_importer_rec_id,
dvc_uf_importer,
dvc_udi_prod_identifier,
dvc_udi_number,
dvc_type_of_report_followup,
dvc_type_of_report,
dvc_third_party_service_de_ml,
dvc_third_party_service,
dvc_terminology_code,
dvc_submitter_of_report_de_ml,
dvc_submitter_of_report,
dvc_spr_id,
dvc_someone_operating_device,
dvc_software_version_number,
dvc_software_version,
dvc_serial_number,
dvc_risk_assment_been_reviewed_de_ml,
dvc_risk_assment_been_reviewed,
dvc_risk_assessment_reviewed_de_ml,
dvc_risk_assessment_reviewed,
dvc_result_of_assessment,
dvc_reprocessor_contact_address,
dvc_reported_trade_name,
dvc_reportable_event_type_de_ml,
dvc_reportable_event_type,
dvc_report_sent_to_manuf_de_ml,
dvc_report_sent_to_manuf,
dvc_report_sent_to_fda_de_ml,
dvc_report_sent_to_fda,
dvc_report_sent_manuf_date,
dvc_report_sent_fda_date,
dvc_remedial_other,
dvc_remedial_action_replace,
dvc_remedial_action_repair,
dvc_remedial_action_relabel,
dvc_remedial_action_recall,
dvc_remedial_action_pat_monitor,
dvc_remedial_action_other,
dvc_remedial_action_notify,
dvc_remedial_action_modify_adjust,
dvc_remedial_action_inspection,
dvc_relevent_associated_device,
dvc_relevent_accessories_device,
dvc_record_id_please_specify,
dvc_record_id_of_comp_auth,
dvc_record_id,
dvc_reason_evaluation_not_provided_de_ml,
dvc_reason_evaluation_not_provided,
dvc_rational_review,
dvc_product_type,
dvc_product_record_id,
dvc_product_flag_de_ml,
dvc_product_flag,
dvc_product_desc,
dvc_product_charecterisation_de_ml,
dvc_product_charecterisation,
dvc_product_available_for_eva_de_ml,
dvc_product_available_for_eva,
dvc_prod_return_date_fmt,
dvc_prod_return_date,
dvc_prod_device_cenum,
dvc_prod_artg_num,
dvc_procedure_surgery_date_fmt,
dvc_procedure_surgery_date,
dvc_procedure_or_surgery_name,
dvc_procedure_description,
dvc_pro_code,
dvc_priority_summary_id,
dvc_primary_device_flag_de_ml,
dvc_primary_device_flag,
dvc_pmcf_pmpf_eudamed_id,
dvc_phone_number,
dvc_patient_code,
dvc_other_usage_of_device,
dvc_other_please_specify,
dvc_other_operator_dev_text,
dvc_other_number,
dvc_other_identifing_info,
dvc_other_event_occur_location,
dvc_other_device_loc,
dvc_operator_of_device_de_ml,
dvc_operator_of_device,
dvc_nomenclature_text,
dvc_nomenclature_system_de_ml,
dvc_nomenclature_system,
dvc_nomenclature_sys_other_text,
dvc_nomenclature_decode,
dvc_nomenclature_code,
dvc_no_patient_involved,
dvc_needle_type_de_ml,
dvc_needle_type,
dvc_nca_local_ref_num,
dvc_nca_fsca_ref_num,
dvc_model_number,
dvc_mfr_number,
dvc_manufacturers_state,
dvc_manufacturers_city,
dvc_manufacturer_name,
dvc_manufacturer_de_ml,
dvc_manufacturer,
dvc_manufacture_fsca_ref_num,
dvc_manuf_name_addres,
dvc_mal_function_de_ml,
dvc_mal_function,
dvc_lot_number,
dvc_labelled_for_single_use_de_ml,
dvc_labelled_for_single_use,
dvc_is_route_caused_confirmed_de_ml,
dvc_is_route_caused_confirmed,
dvc_is_prod_combination_de_ml,
dvc_is_prod_combination,
dvc_interventionrequired,
dvc_inq_rec_id,
dvc_implanted_date_fmt,
dvc_implanted_date,
dvc_implant_facility,
dvc_if_remedial_action_initiated_de_ml,
dvc_if_remedial_action_initiated,
dvc_if_not_returned_sf,
dvc_if_not_returned_de_ml,
dvc_if_not_returned,
dvc_health_impact_code,
dvc_generic,
dvc_general_name_sf,
dvc_general_name_code,
dvc_general_name,
dvc_follow_up_type_de_ml,
dvc_follow_up_type,
dvc_follow_up_response_to_fda_de_ml,
dvc_follow_up_response_to_fda,
dvc_follow_up_device_evaluation_de_ml,
dvc_follow_up_device_evaluation,
dvc_follow_up_correction_de_ml,
dvc_follow_up_correction,
dvc_follow_up_add_info_de_ml,
dvc_follow_up_add_info,
dvc_fk_ad_rec_id,
dvc_firmware_version,
dvc_final_reportable_de_ml,
dvc_final_reportable,
dvc_final_nonreportable_de_ml,
dvc_final_nonreportable,
dvc_fda_reg_num,
dvc_explanted_date_fmt,
dvc_explanted_date,
dvc_explant_facility,
dvc_explanation_form_missing_code_g,
dvc_explanation_form_missing_code_d,
dvc_explanation_form_missing_code_c,
dvc_explanation_form_missing_code_b,
dvc_explanation_form_missing_code_a,
dvc_expiration_date_fmt,
dvc_expiration_date,
dvc_event_occur_status_de_ml,
dvc_event_occur_status,
dvc_event_occur_location_de_ml,
dvc_event_occur_location,
dvc_event_change_status_de_ml,
dvc_event_change_status,
dvc_evaluation_type,
dvc_evaluation_term,
dvc_eudamed_ref_num,
dvc_eudamed_fsca_ref_num,
dvc_entity_updated,
dvc_duration_of_implantation_unit_de_ml,
dvc_duration_of_implantation_unit,
dvc_duration_of_implantation,
dvc_device_used_for_de_ml,
dvc_device_used_for,
dvc_device_type,
dvc_device_reprocessor_unit,
dvc_device_reprocessed_and_reused_de_ml,
dvc_device_reprocessed_and_reused,
dvc_device_manufacture_site,
dvc_device_manufacture_date_fmt,
dvc_device_manufacture_date,
dvc_device_evaluation,
dvc_device_evaluateby_manufacturer_de_ml,
dvc_device_evaluateby_manufacturer,
dvc_device_detail_info,
dvc_device_component_termid,
dvc_device_component_name,
dvc_device_code,
dvc_device_cmnt_termid_version,
dvc_device_brandname_codedflag,
dvc_device_availablefor_evaluation,
dvc_device_age_unit_de_ml,
dvc_device_age_unit,
dvc_device_age,
dvc_date_returned_to_manufactu_fmt,
dvc_date_returned_to_manufactu,
dvc_date_of_this_report,
dvc_date_modified,
dvc_date_implanted_to,
dvc_date_implanted_fmt,
dvc_date_explanted_to,
dvc_date_explanted_fmt,
dvc_date_expected_next_report_fmt,
dvc_date_expected_next_report,
dvc_date_created,
dvc_current_location_of_device_de_ml,
dvc_current_location_of_device,
dvc_current_dev_location_de_ml,
dvc_current_dev_location,
dvc_cuname_for_comp_auth,
dvc_correction_remove_reporting_no,
dvc_contact_person,
dvc_conditional_time_limited_authorization_de_ml,
dvc_conditional_time_limited_authorization,
dvc_concomitant_therapy,
dvc_concomitant_medical_product,
dvc_component_code,
dvc_comp_rec_id,
dvc_common_device_name_nf,
dvc_common_device_name,
dvc_class_of_device_de_ml,
dvc_class_of_device,
dvc_charecterization,
dvc_catalogue_number,
dvc_c_other_operator,
dvc_brand_name_nf,
dvc_brand_name_jp,
dvc_brand_name,
dvc_biological_device_de_ml,
dvc_biological_device,
dvc_basic_udi_di,
dvc_awar_event_date,
dvc_authorised_representative,
dvc_authorised_represent_recid,
dvc_associated_with_distributor_de_ml,
dvc_associated_with_distributor,
dvc_ari_rec_id,
dvc_approx_age_device,
dvc_approval_no,
dvc_aer_number,
dvcmnf_user_modified,
dvcmnf_user_created,
dvcmnf_usage_of_device,
dvcmnf_summary_report,
dvcmnf_state,
dvcmnf_spr_id,
dvcmnf_serious_injury,
dvcmnf_results,
dvcmnf_reportable_event_type,
dvcmnf_removal_report_number,
dvcmnf_remedial_other_action_type,
dvcmnf_remedial_action_type,
dvcmnf_record_id,
dvcmnf_patient_code,
dvcmnf_manufacturer_record_id,
dvcmnf_manufacturer_rec_id,
dvcmnf_manufacturer_as_reported,
dvcmnf_manufacturer_as_coded,
dvcmnf_manufacturer_address,
dvcmnf_manufact_method,
dvcmnf_mal_function,
dvcmnf_labeled_single_use,
dvcmnf_is_corrected_data,
dvcmnf_is_additional_manufacturer,
dvcmnf_inq_rec_id,
dvcmnf_health_impact_code,
dvcmnf_followup_type,
dvcmnf_fk_agx_device_rec_id,
dvcmnf_event_summarized,
dvcmnf_eval_summary_attached,
dvcmnf_eval_no_code,
dvcmnf_device_manuf_date,
dvcmnf_device_evaluated_by_manf,
dvcmnf_device_code,
dvcmnf_death,
dvcmnf_date_modified,
dvcmnf_date_created,
dvcmnf_country,
dvcmnf_corrected_data_info,
dvcmnf_corrected_data,
dvcmnf_conclusions,
dvcmnf_component_code,
dvcmnf_city,
dvcmnf_ari_rec_id,
dvcmnf_additional_manfact_narrative,
dvcmnf_add_manfact_narrative_info)
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
integration_id ,dvc_when_did_device_problem_occur_de_ml,
dvc_when_did_device_problem_occur,
dvc_user_modified,
dvc_user_facility_or_importer,
dvc_user_facility_or_distributor_de_ml,
dvc_user_facility_or_distributor,
dvc_user_created,
dvc_usage_of_device_de_ml,
dvc_usage_of_device,
dvc_units_use_udi,
dvc_uf_or_importer_report_no,
dvc_uf_or_imp_name_address,
dvc_uf_or_dist_report_number,
dvc_uf_importer_rec_id,
dvc_uf_importer,
dvc_udi_prod_identifier,
dvc_udi_number,
dvc_type_of_report_followup,
dvc_type_of_report,
dvc_third_party_service_de_ml,
dvc_third_party_service,
dvc_terminology_code,
dvc_submitter_of_report_de_ml,
dvc_submitter_of_report,
dvc_spr_id,
dvc_someone_operating_device,
dvc_software_version_number,
dvc_software_version,
dvc_serial_number,
dvc_risk_assment_been_reviewed_de_ml,
dvc_risk_assment_been_reviewed,
dvc_risk_assessment_reviewed_de_ml,
dvc_risk_assessment_reviewed,
dvc_result_of_assessment,
dvc_reprocessor_contact_address,
dvc_reported_trade_name,
dvc_reportable_event_type_de_ml,
dvc_reportable_event_type,
dvc_report_sent_to_manuf_de_ml,
dvc_report_sent_to_manuf,
dvc_report_sent_to_fda_de_ml,
dvc_report_sent_to_fda,
dvc_report_sent_manuf_date,
dvc_report_sent_fda_date,
dvc_remedial_other,
dvc_remedial_action_replace,
dvc_remedial_action_repair,
dvc_remedial_action_relabel,
dvc_remedial_action_recall,
dvc_remedial_action_pat_monitor,
dvc_remedial_action_other,
dvc_remedial_action_notify,
dvc_remedial_action_modify_adjust,
dvc_remedial_action_inspection,
dvc_relevent_associated_device,
dvc_relevent_accessories_device,
dvc_record_id_please_specify,
dvc_record_id_of_comp_auth,
dvc_record_id,
dvc_reason_evaluation_not_provided_de_ml,
dvc_reason_evaluation_not_provided,
dvc_rational_review,
dvc_product_type,
dvc_product_record_id,
dvc_product_flag_de_ml,
dvc_product_flag,
dvc_product_desc,
dvc_product_charecterisation_de_ml,
dvc_product_charecterisation,
dvc_product_available_for_eva_de_ml,
dvc_product_available_for_eva,
dvc_prod_return_date_fmt,
dvc_prod_return_date,
dvc_prod_device_cenum,
dvc_prod_artg_num,
dvc_procedure_surgery_date_fmt,
dvc_procedure_surgery_date,
dvc_procedure_or_surgery_name,
dvc_procedure_description,
dvc_pro_code,
dvc_priority_summary_id,
dvc_primary_device_flag_de_ml,
dvc_primary_device_flag,
dvc_pmcf_pmpf_eudamed_id,
dvc_phone_number,
dvc_patient_code,
dvc_other_usage_of_device,
dvc_other_please_specify,
dvc_other_operator_dev_text,
dvc_other_number,
dvc_other_identifing_info,
dvc_other_event_occur_location,
dvc_other_device_loc,
dvc_operator_of_device_de_ml,
dvc_operator_of_device,
dvc_nomenclature_text,
dvc_nomenclature_system_de_ml,
dvc_nomenclature_system,
dvc_nomenclature_sys_other_text,
dvc_nomenclature_decode,
dvc_nomenclature_code,
dvc_no_patient_involved,
dvc_needle_type_de_ml,
dvc_needle_type,
dvc_nca_local_ref_num,
dvc_nca_fsca_ref_num,
dvc_model_number,
dvc_mfr_number,
dvc_manufacturers_state,
dvc_manufacturers_city,
dvc_manufacturer_name,
dvc_manufacturer_de_ml,
dvc_manufacturer,
dvc_manufacture_fsca_ref_num,
dvc_manuf_name_addres,
dvc_mal_function_de_ml,
dvc_mal_function,
dvc_lot_number,
dvc_labelled_for_single_use_de_ml,
dvc_labelled_for_single_use,
dvc_is_route_caused_confirmed_de_ml,
dvc_is_route_caused_confirmed,
dvc_is_prod_combination_de_ml,
dvc_is_prod_combination,
dvc_interventionrequired,
dvc_inq_rec_id,
dvc_implanted_date_fmt,
dvc_implanted_date,
dvc_implant_facility,
dvc_if_remedial_action_initiated_de_ml,
dvc_if_remedial_action_initiated,
dvc_if_not_returned_sf,
dvc_if_not_returned_de_ml,
dvc_if_not_returned,
dvc_health_impact_code,
dvc_generic,
dvc_general_name_sf,
dvc_general_name_code,
dvc_general_name,
dvc_follow_up_type_de_ml,
dvc_follow_up_type,
dvc_follow_up_response_to_fda_de_ml,
dvc_follow_up_response_to_fda,
dvc_follow_up_device_evaluation_de_ml,
dvc_follow_up_device_evaluation,
dvc_follow_up_correction_de_ml,
dvc_follow_up_correction,
dvc_follow_up_add_info_de_ml,
dvc_follow_up_add_info,
dvc_fk_ad_rec_id,
dvc_firmware_version,
dvc_final_reportable_de_ml,
dvc_final_reportable,
dvc_final_nonreportable_de_ml,
dvc_final_nonreportable,
dvc_fda_reg_num,
dvc_explanted_date_fmt,
dvc_explanted_date,
dvc_explant_facility,
dvc_explanation_form_missing_code_g,
dvc_explanation_form_missing_code_d,
dvc_explanation_form_missing_code_c,
dvc_explanation_form_missing_code_b,
dvc_explanation_form_missing_code_a,
dvc_expiration_date_fmt,
dvc_expiration_date,
dvc_event_occur_status_de_ml,
dvc_event_occur_status,
dvc_event_occur_location_de_ml,
dvc_event_occur_location,
dvc_event_change_status_de_ml,
dvc_event_change_status,
dvc_evaluation_type,
dvc_evaluation_term,
dvc_eudamed_ref_num,
dvc_eudamed_fsca_ref_num,
dvc_entity_updated,
dvc_duration_of_implantation_unit_de_ml,
dvc_duration_of_implantation_unit,
dvc_duration_of_implantation,
dvc_device_used_for_de_ml,
dvc_device_used_for,
dvc_device_type,
dvc_device_reprocessor_unit,
dvc_device_reprocessed_and_reused_de_ml,
dvc_device_reprocessed_and_reused,
dvc_device_manufacture_site,
dvc_device_manufacture_date_fmt,
dvc_device_manufacture_date,
dvc_device_evaluation,
dvc_device_evaluateby_manufacturer_de_ml,
dvc_device_evaluateby_manufacturer,
dvc_device_detail_info,
dvc_device_component_termid,
dvc_device_component_name,
dvc_device_code,
dvc_device_cmnt_termid_version,
dvc_device_brandname_codedflag,
dvc_device_availablefor_evaluation,
dvc_device_age_unit_de_ml,
dvc_device_age_unit,
dvc_device_age,
dvc_date_returned_to_manufactu_fmt,
dvc_date_returned_to_manufactu,
dvc_date_of_this_report,
dvc_date_modified,
dvc_date_implanted_to,
dvc_date_implanted_fmt,
dvc_date_explanted_to,
dvc_date_explanted_fmt,
dvc_date_expected_next_report_fmt,
dvc_date_expected_next_report,
dvc_date_created,
dvc_current_location_of_device_de_ml,
dvc_current_location_of_device,
dvc_current_dev_location_de_ml,
dvc_current_dev_location,
dvc_cuname_for_comp_auth,
dvc_correction_remove_reporting_no,
dvc_contact_person,
dvc_conditional_time_limited_authorization_de_ml,
dvc_conditional_time_limited_authorization,
dvc_concomitant_therapy,
dvc_concomitant_medical_product,
dvc_component_code,
dvc_comp_rec_id,
dvc_common_device_name_nf,
dvc_common_device_name,
dvc_class_of_device_de_ml,
dvc_class_of_device,
dvc_charecterization,
dvc_catalogue_number,
dvc_c_other_operator,
dvc_brand_name_nf,
dvc_brand_name_jp,
dvc_brand_name,
dvc_biological_device_de_ml,
dvc_biological_device,
dvc_basic_udi_di,
dvc_awar_event_date,
dvc_authorised_representative,
dvc_authorised_represent_recid,
dvc_associated_with_distributor_de_ml,
dvc_associated_with_distributor,
dvc_ari_rec_id,
dvc_approx_age_device,
dvc_approval_no,
dvc_aer_number,
dvcmnf_user_modified,
dvcmnf_user_created,
dvcmnf_usage_of_device,
dvcmnf_summary_report,
dvcmnf_state,
dvcmnf_spr_id,
dvcmnf_serious_injury,
dvcmnf_results,
dvcmnf_reportable_event_type,
dvcmnf_removal_report_number,
dvcmnf_remedial_other_action_type,
dvcmnf_remedial_action_type,
dvcmnf_record_id,
dvcmnf_patient_code,
dvcmnf_manufacturer_record_id,
dvcmnf_manufacturer_rec_id,
dvcmnf_manufacturer_as_reported,
dvcmnf_manufacturer_as_coded,
dvcmnf_manufacturer_address,
dvcmnf_manufact_method,
dvcmnf_mal_function,
dvcmnf_labeled_single_use,
dvcmnf_is_corrected_data,
dvcmnf_is_additional_manufacturer,
dvcmnf_inq_rec_id,
dvcmnf_health_impact_code,
dvcmnf_followup_type,
dvcmnf_fk_agx_device_rec_id,
dvcmnf_event_summarized,
dvcmnf_eval_summary_attached,
dvcmnf_eval_no_code,
dvcmnf_device_manuf_date,
dvcmnf_device_evaluated_by_manf,
dvcmnf_device_code,
dvcmnf_death,
dvcmnf_date_modified,
dvcmnf_date_created,
dvcmnf_country,
dvcmnf_corrected_data_info,
dvcmnf_corrected_data,
dvcmnf_conclusions,
dvcmnf_component_code,
dvcmnf_city,
dvcmnf_ari_rec_id,
dvcmnf_additional_manfact_narrative,
dvcmnf_add_manfact_narrative_info
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
                                                                                                                                                                                                                                FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
                                                                                                                                                                                                                                WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE TGT
WHERE  ( dvc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device') OR dvcmnf_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_manufacturer')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM    ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP 
WHERE TO_DATE(LS_DB_DEVICE.PROCESSING_DT) < TO_DATE(LS_DB_DEVICE_TMP.PROCESSING_DT)
AND LS_DB_DEVICE.INTEGRATION_ID = LS_DB_DEVICE_TMP.INTEGRATION_ID
AND LS_DB_DEVICE.dvc_record_id = LS_DB_DEVICE_TMP.dvc_record_id
AND LS_DB_DEVICE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_DEVICE.dvcmnf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_DEVICE.dvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_DEVICE_TMP.dvcmnf_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_DEVICE_TMP.dvc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')   
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE TGT
SET         TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE  ( dvc_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device') OR dvcmnf_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_DEVICE_DELETION_TMP  WHERE TABLE_NAME='lsmv_device_manufacturer')
)
AND       TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DEVICE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_DEVICE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_DEVICE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_DEVICE'),
                'LSDB','Case','LS_DB_DEVICE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;


  RETURN 'LS_DB_DEVICE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
