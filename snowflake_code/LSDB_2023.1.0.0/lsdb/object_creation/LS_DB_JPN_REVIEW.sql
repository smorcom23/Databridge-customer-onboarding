
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_JPN_REVIEW()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_REVIEW'),
	'LSRA','Case','LS_DB_JPN_REVIEW',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_JPN_REVIEW' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_JPN_REVIEW'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_JPN_REVIEW'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  As select RECORD_ID,'lsmv_jpn_pv_review' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_pv_review WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_jpn_review_contract' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_contract WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_jpn_review_dlst_test' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_dlst_test WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_jpn_review_doc_type' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_doc_type WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('10021','10022','10023','10024','10025','10029','10030','10031','10032','10033','10034','7077','7077','7077','7077','7077','7077','7077','7077')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_contract WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_ljpr_co_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_contract WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_doc_type WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_ljpr_doc_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_doc_type WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_pv_review WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_pv_review WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_dlst_test WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_ljpr_rec_id record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_dlst_test WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_JPN_REVIEW' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_jpn_review_contract_SUBSET AS 
(
select * from 
    (SELECT  
    ae_preg_sheet_rcd  jprevcon_ae_preg_sheet_rcd,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10034' AND CD=CAST(ae_preg_sheet_rcd AS VARCHAR(100)) )jprevcon_ae_preg_sheet_rcd_de_ml , aggrement_form_type  jprevcon_aggrement_form_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10030' AND CD=CAST(aggrement_form_type AS VARCHAR(100)) )jprevcon_aggrement_form_type_de_ml , ari_rec_id  jprevcon_ari_rec_id,contract  jprevcon_contract,contract_execution  jprevcon_contract_execution,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10031' AND CD=CAST(contract_execution AS VARCHAR(100)) )jprevcon_contract_execution_de_ml , contract_number  jprevcon_contract_number,date_created  jprevcon_date_created,date_modified  jprevcon_date_modified,fk_ljpr_co_rec_id  jprevcon_fk_ljpr_co_rec_id,gen_aff_c_sub_date  jprevcon_gen_aff_c_sub_date,invoice_type  jprevcon_invoice_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10033' AND CD=CAST(invoice_type AS VARCHAR(100)) )jprevcon_invoice_type_de_ml , mou_execution  jprevcon_mou_execution,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10032' AND CD=CAST(mou_execution AS VARCHAR(100)) )jprevcon_mou_execution_de_ml , record_id  jprevcon_record_id,remarks  jprevcon_remarks,spr_id  jprevcon_spr_id,status  jprevcon_status,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10029' AND CD=CAST(status AS VARCHAR(100)) )jprevcon_status_de_ml , user_created  jprevcon_user_created,user_modified  jprevcon_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_contract
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_ljpr_co_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_contract')
  ) where REC_RANK=1 )
  , lsmv_jpn_review_dlst_test_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  jprevdlst_ari_rec_id,bulk_powder_provision  jprevdlst_bulk_powder_provision,date_created  jprevdlst_date_created,date_modified  jprevdlst_date_modified,drug  jprevdlst_drug,drug_shipping_date  jprevdlst_drug_shipping_date,fk_ljpr_rec_id  jprevdlst_fk_ljpr_rec_id,pv_rec_date_of_prod_rec  jprevdlst_pv_rec_date_of_prod_rec,record_id  jprevdlst_record_id,registered  jprevdlst_registered,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(registered AS VARCHAR(100)) )jprevdlst_registered_de_ml , remarks  jprevdlst_remarks,spr_id  jprevdlst_spr_id,study_consent_date  jprevdlst_study_consent_date,test_result_received  jprevdlst_test_result_received,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(test_result_received AS VARCHAR(100)) )jprevdlst_test_result_received_de_ml , user_created  jprevdlst_user_created,user_modified  jprevdlst_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_dlst_test
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_ljpr_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_dlst_test')
  ) where REC_RANK=1 )
  , lsmv_jpn_pv_review_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  jppvrew_ari_rec_id,assessment_in_the_follow_up_report  jppvrew_assessment_in_the_follow_up_report,box_no_info  jppvrew_box_no_info,case_due_date  jppvrew_case_due_date,case_due_manual  jppvrew_case_due_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(case_due_manual AS VARCHAR(100)) )jppvrew_case_due_manual_de_ml , data_correction  jppvrew_data_correction,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(data_correction AS VARCHAR(100)) )jppvrew_data_correction_de_ml , date_created  jppvrew_date_created,date_modified  jppvrew_date_modified,drug  jppvrew_drug,ljpr_safety_manager  jppvrew_ljpr_safety_manager,med_inst_rep_ship_date  jppvrew_med_inst_rep_ship_date,merge_case  jppvrew_merge_case,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(merge_case AS VARCHAR(100)) )jppvrew_merge_case_de_ml , necessity_of_gsd  jppvrew_necessity_of_gsd,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10021' AND CD=CAST(necessity_of_gsd AS VARCHAR(100)) )jppvrew_necessity_of_gsd_de_ml , original_loc  jppvrew_original_loc,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10025' AND CD=CAST(original_loc AS VARCHAR(100)) )jppvrew_original_loc_de_ml , other_reason  jppvrew_other_reason,pmda_submisiion_due_date  jppvrew_pmda_submisiion_due_date,pmda_submisiion_due_manual  jppvrew_pmda_submisiion_due_manual,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(pmda_submisiion_due_manual AS VARCHAR(100)) )jppvrew_pmda_submisiion_due_manual_de_ml , presence  jppvrew_presence,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10024' AND CD=CAST(presence AS VARCHAR(100)) )jppvrew_presence_de_ml , quatation_consent_date  jppvrew_quatation_consent_date,reason_for_exl_gsd  jppvrew_reason_for_exl_gsd,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10022' AND CD=CAST(reason_for_exl_gsd AS VARCHAR(100)) )jppvrew_reason_for_exl_gsd_de_ml , record_id  jppvrew_record_id,registered  jppvrew_registered,remarks  jppvrew_remarks,safety_manager_approval_date  jppvrew_safety_manager_approval_date,spr_id  jppvrew_spr_id,study_consent_date  jppvrew_study_consent_date,sup_org_store  jppvrew_sup_org_store,test_result_received  jppvrew_test_result_received,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(test_result_received AS VARCHAR(100)) )jppvrew_test_result_received_de_ml , user_created  jppvrew_user_created,user_modified  jppvrew_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_pv_review
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_pv_review')
  ) where REC_RANK=1 )
  , lsmv_jpn_review_doc_type_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  jprevdoc_ari_rec_id,date_created  jprevdoc_date_created,date_modified  jprevdoc_date_modified,doc_type  jprevdoc_doc_type,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10023' AND CD=CAST(doc_type AS VARCHAR(100)) )jprevdoc_doc_type_de_ml , fk_ljpr_doc_rec_id  jprevdoc_fk_ljpr_doc_rec_id,original_rcv_date  jprevdoc_original_rcv_date,record_id  jprevdoc_record_id,registered  jprevdoc_registered,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(registered AS VARCHAR(100)) )jprevdoc_registered_de_ml , remarks  jprevdoc_remarks,spr_id  jprevdoc_spr_id,user_created  jprevdoc_user_created,user_modified  jprevdoc_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_jpn_review_doc_type
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_ljpr_doc_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_doc_type')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_jpn_pv_review_SUBSET.jppvrew_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_jpn_review_doc_type_SUBSET.jprevdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_jpn_review_contract_SUBSET.jprevcon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO	,lsmv_jpn_pv_review_SUBSET.jppvrew_USER_MODIFIED USER_MODIFIED,lsmv_jpn_pv_review_SUBSET.jppvrew_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_jpn_pv_review_SUBSET.jppvrew_USER_CREATED CREATED_BY,lsmv_jpn_pv_review_SUBSET.jppvrew_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_user_modified  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_user_created  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_spr_id  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_remarks  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_registered_de_ml  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_registered  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_record_id  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_original_rcv_date  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_fk_ljpr_doc_rec_id  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_doc_type_de_ml  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_doc_type  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_date_modified  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_date_created  ,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_ari_rec_id  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_user_modified  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_user_created  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_test_result_received_de_ml  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_test_result_received  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_study_consent_date  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_spr_id  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_remarks  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_registered_de_ml  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_registered  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_record_id  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_pv_rec_date_of_prod_rec  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_fk_ljpr_rec_id  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_drug_shipping_date  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_drug  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_date_modified  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_date_created  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_bulk_powder_provision  ,lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_ari_rec_id  ,lsmv_jpn_review_contract_SUBSET.jprevcon_user_modified  ,lsmv_jpn_review_contract_SUBSET.jprevcon_user_created  ,lsmv_jpn_review_contract_SUBSET.jprevcon_status_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_status  ,lsmv_jpn_review_contract_SUBSET.jprevcon_spr_id  ,lsmv_jpn_review_contract_SUBSET.jprevcon_remarks  ,lsmv_jpn_review_contract_SUBSET.jprevcon_record_id  ,lsmv_jpn_review_contract_SUBSET.jprevcon_mou_execution_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_mou_execution  ,lsmv_jpn_review_contract_SUBSET.jprevcon_invoice_type_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_invoice_type  ,lsmv_jpn_review_contract_SUBSET.jprevcon_gen_aff_c_sub_date  ,lsmv_jpn_review_contract_SUBSET.jprevcon_fk_ljpr_co_rec_id  ,lsmv_jpn_review_contract_SUBSET.jprevcon_date_modified  ,lsmv_jpn_review_contract_SUBSET.jprevcon_date_created  ,lsmv_jpn_review_contract_SUBSET.jprevcon_contract_number  ,lsmv_jpn_review_contract_SUBSET.jprevcon_contract_execution_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_contract_execution  ,lsmv_jpn_review_contract_SUBSET.jprevcon_contract  ,lsmv_jpn_review_contract_SUBSET.jprevcon_ari_rec_id  ,lsmv_jpn_review_contract_SUBSET.jprevcon_aggrement_form_type_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_aggrement_form_type  ,lsmv_jpn_review_contract_SUBSET.jprevcon_ae_preg_sheet_rcd_de_ml  ,lsmv_jpn_review_contract_SUBSET.jprevcon_ae_preg_sheet_rcd  ,lsmv_jpn_pv_review_SUBSET.jppvrew_user_modified  ,lsmv_jpn_pv_review_SUBSET.jppvrew_user_created  ,lsmv_jpn_pv_review_SUBSET.jppvrew_test_result_received_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_test_result_received  ,lsmv_jpn_pv_review_SUBSET.jppvrew_sup_org_store  ,lsmv_jpn_pv_review_SUBSET.jppvrew_study_consent_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_spr_id  ,lsmv_jpn_pv_review_SUBSET.jppvrew_safety_manager_approval_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_remarks  ,lsmv_jpn_pv_review_SUBSET.jppvrew_registered  ,lsmv_jpn_pv_review_SUBSET.jppvrew_record_id  ,lsmv_jpn_pv_review_SUBSET.jppvrew_reason_for_exl_gsd_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_reason_for_exl_gsd  ,lsmv_jpn_pv_review_SUBSET.jppvrew_quatation_consent_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_presence_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_presence  ,lsmv_jpn_pv_review_SUBSET.jppvrew_pmda_submisiion_due_manual_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_pmda_submisiion_due_manual  ,lsmv_jpn_pv_review_SUBSET.jppvrew_pmda_submisiion_due_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_other_reason  ,lsmv_jpn_pv_review_SUBSET.jppvrew_original_loc_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_original_loc  ,lsmv_jpn_pv_review_SUBSET.jppvrew_necessity_of_gsd_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_necessity_of_gsd  ,lsmv_jpn_pv_review_SUBSET.jppvrew_merge_case_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_merge_case  ,lsmv_jpn_pv_review_SUBSET.jppvrew_med_inst_rep_ship_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_ljpr_safety_manager  ,lsmv_jpn_pv_review_SUBSET.jppvrew_drug  ,lsmv_jpn_pv_review_SUBSET.jppvrew_date_modified  ,lsmv_jpn_pv_review_SUBSET.jppvrew_date_created  ,lsmv_jpn_pv_review_SUBSET.jppvrew_data_correction_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_data_correction  ,lsmv_jpn_pv_review_SUBSET.jppvrew_case_due_manual_de_ml  ,lsmv_jpn_pv_review_SUBSET.jppvrew_case_due_manual  ,lsmv_jpn_pv_review_SUBSET.jppvrew_case_due_date  ,lsmv_jpn_pv_review_SUBSET.jppvrew_box_no_info  ,lsmv_jpn_pv_review_SUBSET.jppvrew_assessment_in_the_follow_up_report  ,lsmv_jpn_pv_review_SUBSET.jppvrew_ari_rec_id ,CONCAT( NVL(lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_RECORD_ID,-1),'||',NVL(lsmv_jpn_pv_review_SUBSET.jppvrew_RECORD_ID,-1),'||',NVL(lsmv_jpn_review_doc_type_SUBSET.jprevdoc_RECORD_ID,-1),'||',NVL(lsmv_jpn_review_contract_SUBSET.jprevcon_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_jpn_pv_review_SUBSET  LEFT JOIN lsmv_jpn_review_contract_SUBSET ON lsmv_jpn_pv_review_SUBSET.jppvrew_record_id=lsmv_jpn_review_contract_SUBSET.jprevcon_fk_ljpr_co_rec_id
                         LEFT JOIN lsmv_jpn_review_dlst_test_SUBSET ON lsmv_jpn_pv_review_SUBSET.jppvrew_record_id=lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_fk_ljpr_rec_id
                         LEFT JOIN lsmv_jpn_review_doc_type_SUBSET ON lsmv_jpn_pv_review_SUBSET.jppvrew_record_id=lsmv_jpn_review_doc_type_SUBSET.jprevdoc_fk_ljpr_doc_rec_id
                         LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON  COALESCE(lsmv_jpn_review_dlst_test_SUBSET.jprevdlst_RECORD_ID,lsmv_jpn_pv_review_SUBSET.jppvrew_RECORD_ID,lsmv_jpn_review_doc_type_SUBSET.jprevdoc_RECORD_ID,lsmv_jpn_review_contract_SUBSET.jprevcon_RECORD_ID)  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP)
where target_table_name='LS_DB_JPN_REVIEW'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_REVIEW'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW   
SET LS_DB_JPN_REVIEW.jprevdoc_user_modified = LS_DB_JPN_REVIEW_TMP.jprevdoc_user_modified,LS_DB_JPN_REVIEW.jprevdoc_user_created = LS_DB_JPN_REVIEW_TMP.jprevdoc_user_created,LS_DB_JPN_REVIEW.jprevdoc_spr_id = LS_DB_JPN_REVIEW_TMP.jprevdoc_spr_id,LS_DB_JPN_REVIEW.jprevdoc_remarks = LS_DB_JPN_REVIEW_TMP.jprevdoc_remarks,LS_DB_JPN_REVIEW.jprevdoc_registered_de_ml = LS_DB_JPN_REVIEW_TMP.jprevdoc_registered_de_ml,LS_DB_JPN_REVIEW.jprevdoc_registered = LS_DB_JPN_REVIEW_TMP.jprevdoc_registered,LS_DB_JPN_REVIEW.jprevdoc_record_id = LS_DB_JPN_REVIEW_TMP.jprevdoc_record_id,LS_DB_JPN_REVIEW.jprevdoc_original_rcv_date = LS_DB_JPN_REVIEW_TMP.jprevdoc_original_rcv_date,LS_DB_JPN_REVIEW.jprevdoc_fk_ljpr_doc_rec_id = LS_DB_JPN_REVIEW_TMP.jprevdoc_fk_ljpr_doc_rec_id,LS_DB_JPN_REVIEW.jprevdoc_doc_type_de_ml = LS_DB_JPN_REVIEW_TMP.jprevdoc_doc_type_de_ml,LS_DB_JPN_REVIEW.jprevdoc_doc_type = LS_DB_JPN_REVIEW_TMP.jprevdoc_doc_type,LS_DB_JPN_REVIEW.jprevdoc_date_modified = LS_DB_JPN_REVIEW_TMP.jprevdoc_date_modified,LS_DB_JPN_REVIEW.jprevdoc_date_created = LS_DB_JPN_REVIEW_TMP.jprevdoc_date_created,LS_DB_JPN_REVIEW.jprevdoc_ari_rec_id = LS_DB_JPN_REVIEW_TMP.jprevdoc_ari_rec_id,LS_DB_JPN_REVIEW.jprevdlst_user_modified = LS_DB_JPN_REVIEW_TMP.jprevdlst_user_modified,LS_DB_JPN_REVIEW.jprevdlst_user_created = LS_DB_JPN_REVIEW_TMP.jprevdlst_user_created,LS_DB_JPN_REVIEW.jprevdlst_test_result_received_de_ml = LS_DB_JPN_REVIEW_TMP.jprevdlst_test_result_received_de_ml,LS_DB_JPN_REVIEW.jprevdlst_test_result_received = LS_DB_JPN_REVIEW_TMP.jprevdlst_test_result_received,LS_DB_JPN_REVIEW.jprevdlst_study_consent_date = LS_DB_JPN_REVIEW_TMP.jprevdlst_study_consent_date,LS_DB_JPN_REVIEW.jprevdlst_spr_id = LS_DB_JPN_REVIEW_TMP.jprevdlst_spr_id,LS_DB_JPN_REVIEW.jprevdlst_remarks = LS_DB_JPN_REVIEW_TMP.jprevdlst_remarks,LS_DB_JPN_REVIEW.jprevdlst_registered_de_ml = LS_DB_JPN_REVIEW_TMP.jprevdlst_registered_de_ml,LS_DB_JPN_REVIEW.jprevdlst_registered = LS_DB_JPN_REVIEW_TMP.jprevdlst_registered,LS_DB_JPN_REVIEW.jprevdlst_record_id = LS_DB_JPN_REVIEW_TMP.jprevdlst_record_id,LS_DB_JPN_REVIEW.jprevdlst_pv_rec_date_of_prod_rec = LS_DB_JPN_REVIEW_TMP.jprevdlst_pv_rec_date_of_prod_rec,LS_DB_JPN_REVIEW.jprevdlst_fk_ljpr_rec_id = LS_DB_JPN_REVIEW_TMP.jprevdlst_fk_ljpr_rec_id,LS_DB_JPN_REVIEW.jprevdlst_drug_shipping_date = LS_DB_JPN_REVIEW_TMP.jprevdlst_drug_shipping_date,LS_DB_JPN_REVIEW.jprevdlst_drug = LS_DB_JPN_REVIEW_TMP.jprevdlst_drug,LS_DB_JPN_REVIEW.jprevdlst_date_modified = LS_DB_JPN_REVIEW_TMP.jprevdlst_date_modified,LS_DB_JPN_REVIEW.jprevdlst_date_created = LS_DB_JPN_REVIEW_TMP.jprevdlst_date_created,LS_DB_JPN_REVIEW.jprevdlst_bulk_powder_provision = LS_DB_JPN_REVIEW_TMP.jprevdlst_bulk_powder_provision,LS_DB_JPN_REVIEW.jprevdlst_ari_rec_id = LS_DB_JPN_REVIEW_TMP.jprevdlst_ari_rec_id,LS_DB_JPN_REVIEW.jprevcon_user_modified = LS_DB_JPN_REVIEW_TMP.jprevcon_user_modified,LS_DB_JPN_REVIEW.jprevcon_user_created = LS_DB_JPN_REVIEW_TMP.jprevcon_user_created,LS_DB_JPN_REVIEW.jprevcon_status_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_status_de_ml,LS_DB_JPN_REVIEW.jprevcon_status = LS_DB_JPN_REVIEW_TMP.jprevcon_status,LS_DB_JPN_REVIEW.jprevcon_spr_id = LS_DB_JPN_REVIEW_TMP.jprevcon_spr_id,LS_DB_JPN_REVIEW.jprevcon_remarks = LS_DB_JPN_REVIEW_TMP.jprevcon_remarks,LS_DB_JPN_REVIEW.jprevcon_record_id = LS_DB_JPN_REVIEW_TMP.jprevcon_record_id,LS_DB_JPN_REVIEW.jprevcon_mou_execution_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_mou_execution_de_ml,LS_DB_JPN_REVIEW.jprevcon_mou_execution = LS_DB_JPN_REVIEW_TMP.jprevcon_mou_execution,LS_DB_JPN_REVIEW.jprevcon_invoice_type_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_invoice_type_de_ml,LS_DB_JPN_REVIEW.jprevcon_invoice_type = LS_DB_JPN_REVIEW_TMP.jprevcon_invoice_type,LS_DB_JPN_REVIEW.jprevcon_gen_aff_c_sub_date = LS_DB_JPN_REVIEW_TMP.jprevcon_gen_aff_c_sub_date,LS_DB_JPN_REVIEW.jprevcon_fk_ljpr_co_rec_id = LS_DB_JPN_REVIEW_TMP.jprevcon_fk_ljpr_co_rec_id,LS_DB_JPN_REVIEW.jprevcon_date_modified = LS_DB_JPN_REVIEW_TMP.jprevcon_date_modified,LS_DB_JPN_REVIEW.jprevcon_date_created = LS_DB_JPN_REVIEW_TMP.jprevcon_date_created,LS_DB_JPN_REVIEW.jprevcon_contract_number = LS_DB_JPN_REVIEW_TMP.jprevcon_contract_number,LS_DB_JPN_REVIEW.jprevcon_contract_execution_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_contract_execution_de_ml,LS_DB_JPN_REVIEW.jprevcon_contract_execution = LS_DB_JPN_REVIEW_TMP.jprevcon_contract_execution,LS_DB_JPN_REVIEW.jprevcon_contract = LS_DB_JPN_REVIEW_TMP.jprevcon_contract,LS_DB_JPN_REVIEW.jprevcon_ari_rec_id = LS_DB_JPN_REVIEW_TMP.jprevcon_ari_rec_id,LS_DB_JPN_REVIEW.jprevcon_aggrement_form_type_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_aggrement_form_type_de_ml,LS_DB_JPN_REVIEW.jprevcon_aggrement_form_type = LS_DB_JPN_REVIEW_TMP.jprevcon_aggrement_form_type,LS_DB_JPN_REVIEW.jprevcon_ae_preg_sheet_rcd_de_ml = LS_DB_JPN_REVIEW_TMP.jprevcon_ae_preg_sheet_rcd_de_ml,LS_DB_JPN_REVIEW.jprevcon_ae_preg_sheet_rcd = LS_DB_JPN_REVIEW_TMP.jprevcon_ae_preg_sheet_rcd,LS_DB_JPN_REVIEW.jppvrew_user_modified = LS_DB_JPN_REVIEW_TMP.jppvrew_user_modified,LS_DB_JPN_REVIEW.jppvrew_user_created = LS_DB_JPN_REVIEW_TMP.jppvrew_user_created,LS_DB_JPN_REVIEW.jppvrew_test_result_received_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_test_result_received_de_ml,LS_DB_JPN_REVIEW.jppvrew_test_result_received = LS_DB_JPN_REVIEW_TMP.jppvrew_test_result_received,LS_DB_JPN_REVIEW.jppvrew_sup_org_store = LS_DB_JPN_REVIEW_TMP.jppvrew_sup_org_store,LS_DB_JPN_REVIEW.jppvrew_study_consent_date = LS_DB_JPN_REVIEW_TMP.jppvrew_study_consent_date,LS_DB_JPN_REVIEW.jppvrew_spr_id = LS_DB_JPN_REVIEW_TMP.jppvrew_spr_id,LS_DB_JPN_REVIEW.jppvrew_safety_manager_approval_date = LS_DB_JPN_REVIEW_TMP.jppvrew_safety_manager_approval_date,LS_DB_JPN_REVIEW.jppvrew_remarks = LS_DB_JPN_REVIEW_TMP.jppvrew_remarks,LS_DB_JPN_REVIEW.jppvrew_registered = LS_DB_JPN_REVIEW_TMP.jppvrew_registered,LS_DB_JPN_REVIEW.jppvrew_record_id = LS_DB_JPN_REVIEW_TMP.jppvrew_record_id,LS_DB_JPN_REVIEW.jppvrew_reason_for_exl_gsd_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_reason_for_exl_gsd_de_ml,LS_DB_JPN_REVIEW.jppvrew_reason_for_exl_gsd = LS_DB_JPN_REVIEW_TMP.jppvrew_reason_for_exl_gsd,LS_DB_JPN_REVIEW.jppvrew_quatation_consent_date = LS_DB_JPN_REVIEW_TMP.jppvrew_quatation_consent_date,LS_DB_JPN_REVIEW.jppvrew_presence_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_presence_de_ml,LS_DB_JPN_REVIEW.jppvrew_presence = LS_DB_JPN_REVIEW_TMP.jppvrew_presence,LS_DB_JPN_REVIEW.jppvrew_pmda_submisiion_due_manual_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_pmda_submisiion_due_manual_de_ml,LS_DB_JPN_REVIEW.jppvrew_pmda_submisiion_due_manual = LS_DB_JPN_REVIEW_TMP.jppvrew_pmda_submisiion_due_manual,LS_DB_JPN_REVIEW.jppvrew_pmda_submisiion_due_date = LS_DB_JPN_REVIEW_TMP.jppvrew_pmda_submisiion_due_date,LS_DB_JPN_REVIEW.jppvrew_other_reason = LS_DB_JPN_REVIEW_TMP.jppvrew_other_reason,LS_DB_JPN_REVIEW.jppvrew_original_loc_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_original_loc_de_ml,LS_DB_JPN_REVIEW.jppvrew_original_loc = LS_DB_JPN_REVIEW_TMP.jppvrew_original_loc,LS_DB_JPN_REVIEW.jppvrew_necessity_of_gsd_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_necessity_of_gsd_de_ml,LS_DB_JPN_REVIEW.jppvrew_necessity_of_gsd = LS_DB_JPN_REVIEW_TMP.jppvrew_necessity_of_gsd,LS_DB_JPN_REVIEW.jppvrew_merge_case_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_merge_case_de_ml,LS_DB_JPN_REVIEW.jppvrew_merge_case = LS_DB_JPN_REVIEW_TMP.jppvrew_merge_case,LS_DB_JPN_REVIEW.jppvrew_med_inst_rep_ship_date = LS_DB_JPN_REVIEW_TMP.jppvrew_med_inst_rep_ship_date,LS_DB_JPN_REVIEW.jppvrew_ljpr_safety_manager = LS_DB_JPN_REVIEW_TMP.jppvrew_ljpr_safety_manager,LS_DB_JPN_REVIEW.jppvrew_drug = LS_DB_JPN_REVIEW_TMP.jppvrew_drug,LS_DB_JPN_REVIEW.jppvrew_date_modified = LS_DB_JPN_REVIEW_TMP.jppvrew_date_modified,LS_DB_JPN_REVIEW.jppvrew_date_created = LS_DB_JPN_REVIEW_TMP.jppvrew_date_created,LS_DB_JPN_REVIEW.jppvrew_data_correction_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_data_correction_de_ml,LS_DB_JPN_REVIEW.jppvrew_data_correction = LS_DB_JPN_REVIEW_TMP.jppvrew_data_correction,LS_DB_JPN_REVIEW.jppvrew_case_due_manual_de_ml = LS_DB_JPN_REVIEW_TMP.jppvrew_case_due_manual_de_ml,LS_DB_JPN_REVIEW.jppvrew_case_due_manual = LS_DB_JPN_REVIEW_TMP.jppvrew_case_due_manual,LS_DB_JPN_REVIEW.jppvrew_case_due_date = LS_DB_JPN_REVIEW_TMP.jppvrew_case_due_date,LS_DB_JPN_REVIEW.jppvrew_box_no_info = LS_DB_JPN_REVIEW_TMP.jppvrew_box_no_info,LS_DB_JPN_REVIEW.jppvrew_assessment_in_the_follow_up_report = LS_DB_JPN_REVIEW_TMP.jppvrew_assessment_in_the_follow_up_report,LS_DB_JPN_REVIEW.jppvrew_ari_rec_id = LS_DB_JPN_REVIEW_TMP.jppvrew_ari_rec_id,
LS_DB_JPN_REVIEW.PROCESSING_DT = LS_DB_JPN_REVIEW_TMP.PROCESSING_DT,
LS_DB_JPN_REVIEW.receipt_id     =LS_DB_JPN_REVIEW_TMP.receipt_id    ,
LS_DB_JPN_REVIEW.case_no        =LS_DB_JPN_REVIEW_TMP.case_no           ,
LS_DB_JPN_REVIEW.case_version   =LS_DB_JPN_REVIEW_TMP.case_version      ,
LS_DB_JPN_REVIEW.version_no     =LS_DB_JPN_REVIEW_TMP.version_no        ,
LS_DB_JPN_REVIEW.user_modified  =LS_DB_JPN_REVIEW_TMP.user_modified     ,
LS_DB_JPN_REVIEW.date_modified  =LS_DB_JPN_REVIEW_TMP.date_modified     ,
LS_DB_JPN_REVIEW.expiry_date    =LS_DB_JPN_REVIEW_TMP.expiry_date       ,
LS_DB_JPN_REVIEW.created_by     =LS_DB_JPN_REVIEW_TMP.created_by        ,
LS_DB_JPN_REVIEW.created_dt     =LS_DB_JPN_REVIEW_TMP.created_dt        ,
LS_DB_JPN_REVIEW.load_ts        =LS_DB_JPN_REVIEW_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP 
WHERE 	LS_DB_JPN_REVIEW.INTEGRATION_ID = LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_JPN_REVIEW_TMP.PROCESSING_DT = LS_DB_JPN_REVIEW.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_JPN_REVIEW.jprevdlst_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jppvrew_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jprevdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jprevcon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_JPN_REVIEW_TMP.jprevdlst_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jppvrew_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jprevdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jprevcon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_JPN_REVIEW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_JPN_REVIEW.jppvrew_RECORD_ID ,LS_DB_JPN_REVIEW.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP 
ON LS_DB_JPN_REVIEW.jppvrew_RECORD_ID=LS_DB_JPN_REVIEW_TMP.jppvrew_RECORD_ID
AND LS_DB_JPN_REVIEW.INTEGRATION_ID = LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID 
where LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID  is null AND LS_DB_JPN_REVIEW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_JPN_REVIEW.jppvrew_RECORD_ID in (select jppvrew_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP )
) TMP where LS_DB_JPN_REVIEW.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_JPN_REVIEW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_JPN_REVIEW.jppvrew_RECORD_ID ,LS_DB_JPN_REVIEW.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP 
    ON LS_DB_JPN_REVIEW.jppvrew_RECORD_ID=LS_DB_JPN_REVIEW_TMP.jppvrew_RECORD_ID
    AND LS_DB_JPN_REVIEW.INTEGRATION_ID = LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID 
    where LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID  is null AND LS_DB_JPN_REVIEW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_JPN_REVIEW.jppvrew_RECORD_ID in (select jppvrew_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW
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
integration_id ,jprevdoc_user_modified,
jprevdoc_user_created,
jprevdoc_spr_id,
jprevdoc_remarks,
jprevdoc_registered_de_ml,
jprevdoc_registered,
jprevdoc_record_id,
jprevdoc_original_rcv_date,
jprevdoc_fk_ljpr_doc_rec_id,
jprevdoc_doc_type_de_ml,
jprevdoc_doc_type,
jprevdoc_date_modified,
jprevdoc_date_created,
jprevdoc_ari_rec_id,
jprevdlst_user_modified,
jprevdlst_user_created,
jprevdlst_test_result_received_de_ml,
jprevdlst_test_result_received,
jprevdlst_study_consent_date,
jprevdlst_spr_id,
jprevdlst_remarks,
jprevdlst_registered_de_ml,
jprevdlst_registered,
jprevdlst_record_id,
jprevdlst_pv_rec_date_of_prod_rec,
jprevdlst_fk_ljpr_rec_id,
jprevdlst_drug_shipping_date,
jprevdlst_drug,
jprevdlst_date_modified,
jprevdlst_date_created,
jprevdlst_bulk_powder_provision,
jprevdlst_ari_rec_id,
jprevcon_user_modified,
jprevcon_user_created,
jprevcon_status_de_ml,
jprevcon_status,
jprevcon_spr_id,
jprevcon_remarks,
jprevcon_record_id,
jprevcon_mou_execution_de_ml,
jprevcon_mou_execution,
jprevcon_invoice_type_de_ml,
jprevcon_invoice_type,
jprevcon_gen_aff_c_sub_date,
jprevcon_fk_ljpr_co_rec_id,
jprevcon_date_modified,
jprevcon_date_created,
jprevcon_contract_number,
jprevcon_contract_execution_de_ml,
jprevcon_contract_execution,
jprevcon_contract,
jprevcon_ari_rec_id,
jprevcon_aggrement_form_type_de_ml,
jprevcon_aggrement_form_type,
jprevcon_ae_preg_sheet_rcd_de_ml,
jprevcon_ae_preg_sheet_rcd,
jppvrew_user_modified,
jppvrew_user_created,
jppvrew_test_result_received_de_ml,
jppvrew_test_result_received,
jppvrew_sup_org_store,
jppvrew_study_consent_date,
jppvrew_spr_id,
jppvrew_safety_manager_approval_date,
jppvrew_remarks,
jppvrew_registered,
jppvrew_record_id,
jppvrew_reason_for_exl_gsd_de_ml,
jppvrew_reason_for_exl_gsd,
jppvrew_quatation_consent_date,
jppvrew_presence_de_ml,
jppvrew_presence,
jppvrew_pmda_submisiion_due_manual_de_ml,
jppvrew_pmda_submisiion_due_manual,
jppvrew_pmda_submisiion_due_date,
jppvrew_other_reason,
jppvrew_original_loc_de_ml,
jppvrew_original_loc,
jppvrew_necessity_of_gsd_de_ml,
jppvrew_necessity_of_gsd,
jppvrew_merge_case_de_ml,
jppvrew_merge_case,
jppvrew_med_inst_rep_ship_date,
jppvrew_ljpr_safety_manager,
jppvrew_drug,
jppvrew_date_modified,
jppvrew_date_created,
jppvrew_data_correction_de_ml,
jppvrew_data_correction,
jppvrew_case_due_manual_de_ml,
jppvrew_case_due_manual,
jppvrew_case_due_date,
jppvrew_box_no_info,
jppvrew_assessment_in_the_follow_up_report,
jppvrew_ari_rec_id)
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
integration_id ,jprevdoc_user_modified,
jprevdoc_user_created,
jprevdoc_spr_id,
jprevdoc_remarks,
jprevdoc_registered_de_ml,
jprevdoc_registered,
jprevdoc_record_id,
jprevdoc_original_rcv_date,
jprevdoc_fk_ljpr_doc_rec_id,
jprevdoc_doc_type_de_ml,
jprevdoc_doc_type,
jprevdoc_date_modified,
jprevdoc_date_created,
jprevdoc_ari_rec_id,
jprevdlst_user_modified,
jprevdlst_user_created,
jprevdlst_test_result_received_de_ml,
jprevdlst_test_result_received,
jprevdlst_study_consent_date,
jprevdlst_spr_id,
jprevdlst_remarks,
jprevdlst_registered_de_ml,
jprevdlst_registered,
jprevdlst_record_id,
jprevdlst_pv_rec_date_of_prod_rec,
jprevdlst_fk_ljpr_rec_id,
jprevdlst_drug_shipping_date,
jprevdlst_drug,
jprevdlst_date_modified,
jprevdlst_date_created,
jprevdlst_bulk_powder_provision,
jprevdlst_ari_rec_id,
jprevcon_user_modified,
jprevcon_user_created,
jprevcon_status_de_ml,
jprevcon_status,
jprevcon_spr_id,
jprevcon_remarks,
jprevcon_record_id,
jprevcon_mou_execution_de_ml,
jprevcon_mou_execution,
jprevcon_invoice_type_de_ml,
jprevcon_invoice_type,
jprevcon_gen_aff_c_sub_date,
jprevcon_fk_ljpr_co_rec_id,
jprevcon_date_modified,
jprevcon_date_created,
jprevcon_contract_number,
jprevcon_contract_execution_de_ml,
jprevcon_contract_execution,
jprevcon_contract,
jprevcon_ari_rec_id,
jprevcon_aggrement_form_type_de_ml,
jprevcon_aggrement_form_type,
jprevcon_ae_preg_sheet_rcd_de_ml,
jprevcon_ae_preg_sheet_rcd,
jppvrew_user_modified,
jppvrew_user_created,
jppvrew_test_result_received_de_ml,
jppvrew_test_result_received,
jppvrew_sup_org_store,
jppvrew_study_consent_date,
jppvrew_spr_id,
jppvrew_safety_manager_approval_date,
jppvrew_remarks,
jppvrew_registered,
jppvrew_record_id,
jppvrew_reason_for_exl_gsd_de_ml,
jppvrew_reason_for_exl_gsd,
jppvrew_quatation_consent_date,
jppvrew_presence_de_ml,
jppvrew_presence,
jppvrew_pmda_submisiion_due_manual_de_ml,
jppvrew_pmda_submisiion_due_manual,
jppvrew_pmda_submisiion_due_date,
jppvrew_other_reason,
jppvrew_original_loc_de_ml,
jppvrew_original_loc,
jppvrew_necessity_of_gsd_de_ml,
jppvrew_necessity_of_gsd,
jppvrew_merge_case_de_ml,
jppvrew_merge_case,
jppvrew_med_inst_rep_ship_date,
jppvrew_ljpr_safety_manager,
jppvrew_drug,
jppvrew_date_modified,
jppvrew_date_created,
jppvrew_data_correction_de_ml,
jppvrew_data_correction,
jppvrew_case_due_manual_de_ml,
jppvrew_case_due_manual,
jppvrew_case_due_date,
jppvrew_box_no_info,
jppvrew_assessment_in_the_follow_up_report,
jppvrew_ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP 
WHERE 	TO_DATE(LS_DB_JPN_REVIEW.PROCESSING_DT) < TO_DATE(LS_DB_JPN_REVIEW_TMP.PROCESSING_DT)
AND LS_DB_JPN_REVIEW.INTEGRATION_ID = LS_DB_JPN_REVIEW_TMP.INTEGRATION_ID
AND LS_DB_JPN_REVIEW.jppvrew_RECORD_ID = LS_DB_JPN_REVIEW_TMP.jppvrew_RECORD_ID
AND LS_DB_JPN_REVIEW.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_JPN_REVIEW.jprevdlst_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jppvrew_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jprevdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_JPN_REVIEW.jprevcon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_JPN_REVIEW_TMP.jprevdlst_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jppvrew_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jprevdoc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_JPN_REVIEW_TMP.jprevcon_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW TGT
WHERE  ( jppvrew_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_pv_review') OR jprevcon_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_contract') OR jprevdlst_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_dlst_test') OR jprevdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_doc_type')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( jppvrew_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_pv_review') OR jprevcon_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_contract') OR jprevdlst_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_dlst_test') OR jprevdoc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_JPN_REVIEW_DELETION_TMP  WHERE TABLE_NAME='lsmv_jpn_review_doc_type')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_JPN_REVIEW_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_JPN_REVIEW'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_JPN_REVIEW'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_JPN_REVIEW Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_JPN_REVIEW'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_JPN_REVIEW not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
