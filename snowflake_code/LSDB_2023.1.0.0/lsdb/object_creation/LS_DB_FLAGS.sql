
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_FLAGS()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FLAGS'),
	'LSRA','Case','LS_DB_FLAGS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_FLAGS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_FLAGS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_FLAGS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_FLAGS_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_FLAGS_DELETION_TMP  As select RECORD_ID,'lsmv_flags' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_flags WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('7077','7077','7077','7077','7077','7077')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_flags WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FLAGS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FLAGS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_flags WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FLAGS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_FLAGS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_flags_SUBSET AS 
(
select * from 
    (SELECT  
    all_product_sub_track  all_product_sub_track,ari_rec_id  ari_rec_id,comp_rec_id  comp_rec_id,company_narrative_flag  company_narrative_flag,correspondence_delivery_status  correspondence_delivery_status,correspondence_flag  correspondence_flag,correspondence_seq  correspondence_seq,data_percentage  data_percentage,date_created  date_created,date_modified  date_modified,dispossed_through_e2b  dispossed_through_e2b,distribution_status  distribution_status,document_checkout_flag  document_checkout_flag,document_flag  document_flag,event_outcome_flag  event_outcome_flag,extraction_confidence  extraction_confidence,extraction_invoc_id  extraction_invoc_id,extraction_result  extraction_result,foia_status  foia_status,foia_submitted_date  foia_submitted_date,foia_submitted_flag  foia_submitted_flag,followup_notify_flag  followup_notify_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(followup_notify_flag AS VARCHAR(100)) )followup_notify_flag_de_ml , im_rec_id  im_rec_id,inq_rec_id  inq_rec_id,is_candidate_for_touchless  is_candidate_for_touchless,link_ae_flag  link_ae_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(link_ae_flag AS VARCHAR(100)) )link_ae_flag_de_ml , lrn  lrn,lrn_version  lrn_version,medwatch_status  medwatch_status,narr_case_json  narr_case_json,nlp_case_compare_flag  nlp_case_compare_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(nlp_case_compare_flag AS VARCHAR(100)) )nlp_case_compare_flag_de_ml , nlp_conf_breakup  nlp_conf_breakup,nlp_confidence  nlp_confidence,nlp_invoc_ids  nlp_invoc_ids,ocr_applied  ocr_applied,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(ocr_applied AS VARCHAR(100)) )ocr_applied_de_ml , ocr_extraction_report  ocr_extraction_report,ocr_partial_extraction  ocr_partial_extraction,onset_date_flag  onset_date_flag,patient_age_flag  patient_age_flag,patient_sex_flag  patient_sex_flag,question_followup  question_followup,question_followup_available  question_followup_available,rcpt_no  rcpt_no,read_unread_correspondence  read_unread_correspondence,record_id  record_id,redact_status  redact_status,reported_term_flag  reported_term_flag,req_completed_date  req_completed_date,req_created_date  req_created_date,requested_by  requested_by,safety_departments_flag  safety_departments_flag,safety_tag_flag  safety_tag_flag,spr_id  spr_id,translate_req_flag  translate_req_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(translate_req_flag AS VARCHAR(100)) )translate_req_flag_de_ml , turn_off_touchless_flag  turn_off_touchless_flag,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='7077' AND CD=CAST(turn_off_touchless_flag AS VARCHAR(100)) )turn_off_touchless_flag_de_ml , user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_flags
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_FLAGS_DELETION_TMP  WHERE TABLE_NAME='lsmv_flags')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_flags_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_flags_SUBSET.USER_CREATED CREATED_BY,lsmv_flags_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_flags_SUBSET.user_modified  ,lsmv_flags_SUBSET.user_created  ,lsmv_flags_SUBSET.turn_off_touchless_flag_de_ml  ,lsmv_flags_SUBSET.turn_off_touchless_flag  ,lsmv_flags_SUBSET.translate_req_flag_de_ml  ,lsmv_flags_SUBSET.translate_req_flag  ,lsmv_flags_SUBSET.spr_id  ,lsmv_flags_SUBSET.safety_tag_flag  ,lsmv_flags_SUBSET.safety_departments_flag  ,lsmv_flags_SUBSET.requested_by  ,lsmv_flags_SUBSET.req_created_date  ,lsmv_flags_SUBSET.req_completed_date  ,lsmv_flags_SUBSET.reported_term_flag  ,lsmv_flags_SUBSET.redact_status  ,lsmv_flags_SUBSET.record_id  ,lsmv_flags_SUBSET.read_unread_correspondence  ,lsmv_flags_SUBSET.rcpt_no  ,lsmv_flags_SUBSET.question_followup_available  ,lsmv_flags_SUBSET.question_followup  ,lsmv_flags_SUBSET.patient_sex_flag  ,lsmv_flags_SUBSET.patient_age_flag  ,lsmv_flags_SUBSET.onset_date_flag  ,lsmv_flags_SUBSET.ocr_partial_extraction  ,lsmv_flags_SUBSET.ocr_extraction_report  ,lsmv_flags_SUBSET.ocr_applied_de_ml  ,lsmv_flags_SUBSET.ocr_applied  ,lsmv_flags_SUBSET.nlp_invoc_ids  ,lsmv_flags_SUBSET.nlp_confidence  ,lsmv_flags_SUBSET.nlp_conf_breakup  ,lsmv_flags_SUBSET.nlp_case_compare_flag_de_ml  ,lsmv_flags_SUBSET.nlp_case_compare_flag  ,lsmv_flags_SUBSET.narr_case_json  ,lsmv_flags_SUBSET.medwatch_status  ,lsmv_flags_SUBSET.lrn_version  ,lsmv_flags_SUBSET.lrn  ,lsmv_flags_SUBSET.link_ae_flag_de_ml  ,lsmv_flags_SUBSET.link_ae_flag  ,lsmv_flags_SUBSET.is_candidate_for_touchless  ,lsmv_flags_SUBSET.inq_rec_id  ,lsmv_flags_SUBSET.im_rec_id  ,lsmv_flags_SUBSET.followup_notify_flag_de_ml  ,lsmv_flags_SUBSET.followup_notify_flag  ,lsmv_flags_SUBSET.foia_submitted_flag  ,lsmv_flags_SUBSET.foia_submitted_date  ,lsmv_flags_SUBSET.foia_status  ,lsmv_flags_SUBSET.extraction_result  ,lsmv_flags_SUBSET.extraction_invoc_id  ,lsmv_flags_SUBSET.extraction_confidence  ,lsmv_flags_SUBSET.event_outcome_flag  ,lsmv_flags_SUBSET.document_flag  ,lsmv_flags_SUBSET.document_checkout_flag  ,lsmv_flags_SUBSET.distribution_status  ,lsmv_flags_SUBSET.dispossed_through_e2b  ,lsmv_flags_SUBSET.date_modified  ,lsmv_flags_SUBSET.date_created  ,lsmv_flags_SUBSET.data_percentage  ,lsmv_flags_SUBSET.correspondence_seq  ,lsmv_flags_SUBSET.correspondence_flag  ,lsmv_flags_SUBSET.correspondence_delivery_status  ,lsmv_flags_SUBSET.company_narrative_flag  ,lsmv_flags_SUBSET.comp_rec_id  ,lsmv_flags_SUBSET.ari_rec_id  ,lsmv_flags_SUBSET.all_product_sub_track ,CONCAT(NVL(lsmv_flags_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_flags_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_flags_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP)
where target_table_name='LS_DB_FLAGS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FLAGS'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS   
SET LS_DB_FLAGS.user_modified = LS_DB_FLAGS_TMP.user_modified,LS_DB_FLAGS.user_created = LS_DB_FLAGS_TMP.user_created,LS_DB_FLAGS.turn_off_touchless_flag_de_ml = LS_DB_FLAGS_TMP.turn_off_touchless_flag_de_ml,LS_DB_FLAGS.turn_off_touchless_flag = LS_DB_FLAGS_TMP.turn_off_touchless_flag,LS_DB_FLAGS.translate_req_flag_de_ml = LS_DB_FLAGS_TMP.translate_req_flag_de_ml,LS_DB_FLAGS.translate_req_flag = LS_DB_FLAGS_TMP.translate_req_flag,LS_DB_FLAGS.spr_id = LS_DB_FLAGS_TMP.spr_id,LS_DB_FLAGS.safety_tag_flag = LS_DB_FLAGS_TMP.safety_tag_flag,LS_DB_FLAGS.safety_departments_flag = LS_DB_FLAGS_TMP.safety_departments_flag,LS_DB_FLAGS.requested_by = LS_DB_FLAGS_TMP.requested_by,LS_DB_FLAGS.req_created_date = LS_DB_FLAGS_TMP.req_created_date,LS_DB_FLAGS.req_completed_date = LS_DB_FLAGS_TMP.req_completed_date,LS_DB_FLAGS.reported_term_flag = LS_DB_FLAGS_TMP.reported_term_flag,LS_DB_FLAGS.redact_status = LS_DB_FLAGS_TMP.redact_status,LS_DB_FLAGS.record_id = LS_DB_FLAGS_TMP.record_id,LS_DB_FLAGS.read_unread_correspondence = LS_DB_FLAGS_TMP.read_unread_correspondence,LS_DB_FLAGS.rcpt_no = LS_DB_FLAGS_TMP.rcpt_no,LS_DB_FLAGS.question_followup_available = LS_DB_FLAGS_TMP.question_followup_available,LS_DB_FLAGS.question_followup = LS_DB_FLAGS_TMP.question_followup,LS_DB_FLAGS.patient_sex_flag = LS_DB_FLAGS_TMP.patient_sex_flag,LS_DB_FLAGS.patient_age_flag = LS_DB_FLAGS_TMP.patient_age_flag,LS_DB_FLAGS.onset_date_flag = LS_DB_FLAGS_TMP.onset_date_flag,LS_DB_FLAGS.ocr_partial_extraction = LS_DB_FLAGS_TMP.ocr_partial_extraction,LS_DB_FLAGS.ocr_extraction_report = LS_DB_FLAGS_TMP.ocr_extraction_report,LS_DB_FLAGS.ocr_applied_de_ml = LS_DB_FLAGS_TMP.ocr_applied_de_ml,LS_DB_FLAGS.ocr_applied = LS_DB_FLAGS_TMP.ocr_applied,LS_DB_FLAGS.nlp_invoc_ids = LS_DB_FLAGS_TMP.nlp_invoc_ids,LS_DB_FLAGS.nlp_confidence = LS_DB_FLAGS_TMP.nlp_confidence,LS_DB_FLAGS.nlp_conf_breakup = LS_DB_FLAGS_TMP.nlp_conf_breakup,LS_DB_FLAGS.nlp_case_compare_flag_de_ml = LS_DB_FLAGS_TMP.nlp_case_compare_flag_de_ml,LS_DB_FLAGS.nlp_case_compare_flag = LS_DB_FLAGS_TMP.nlp_case_compare_flag,LS_DB_FLAGS.narr_case_json = LS_DB_FLAGS_TMP.narr_case_json,LS_DB_FLAGS.medwatch_status = LS_DB_FLAGS_TMP.medwatch_status,LS_DB_FLAGS.lrn_version = LS_DB_FLAGS_TMP.lrn_version,LS_DB_FLAGS.lrn = LS_DB_FLAGS_TMP.lrn,LS_DB_FLAGS.link_ae_flag_de_ml = LS_DB_FLAGS_TMP.link_ae_flag_de_ml,LS_DB_FLAGS.link_ae_flag = LS_DB_FLAGS_TMP.link_ae_flag,LS_DB_FLAGS.is_candidate_for_touchless = LS_DB_FLAGS_TMP.is_candidate_for_touchless,LS_DB_FLAGS.inq_rec_id = LS_DB_FLAGS_TMP.inq_rec_id,LS_DB_FLAGS.im_rec_id = LS_DB_FLAGS_TMP.im_rec_id,LS_DB_FLAGS.followup_notify_flag_de_ml = LS_DB_FLAGS_TMP.followup_notify_flag_de_ml,LS_DB_FLAGS.followup_notify_flag = LS_DB_FLAGS_TMP.followup_notify_flag,LS_DB_FLAGS.foia_submitted_flag = LS_DB_FLAGS_TMP.foia_submitted_flag,LS_DB_FLAGS.foia_submitted_date = LS_DB_FLAGS_TMP.foia_submitted_date,LS_DB_FLAGS.foia_status = LS_DB_FLAGS_TMP.foia_status,LS_DB_FLAGS.extraction_result = LS_DB_FLAGS_TMP.extraction_result,LS_DB_FLAGS.extraction_invoc_id = LS_DB_FLAGS_TMP.extraction_invoc_id,LS_DB_FLAGS.extraction_confidence = LS_DB_FLAGS_TMP.extraction_confidence,LS_DB_FLAGS.event_outcome_flag = LS_DB_FLAGS_TMP.event_outcome_flag,LS_DB_FLAGS.document_flag = LS_DB_FLAGS_TMP.document_flag,LS_DB_FLAGS.document_checkout_flag = LS_DB_FLAGS_TMP.document_checkout_flag,LS_DB_FLAGS.distribution_status = LS_DB_FLAGS_TMP.distribution_status,LS_DB_FLAGS.dispossed_through_e2b = LS_DB_FLAGS_TMP.dispossed_through_e2b,LS_DB_FLAGS.date_modified = LS_DB_FLAGS_TMP.date_modified,LS_DB_FLAGS.date_created = LS_DB_FLAGS_TMP.date_created,LS_DB_FLAGS.data_percentage = LS_DB_FLAGS_TMP.data_percentage,LS_DB_FLAGS.correspondence_seq = LS_DB_FLAGS_TMP.correspondence_seq,LS_DB_FLAGS.correspondence_flag = LS_DB_FLAGS_TMP.correspondence_flag,LS_DB_FLAGS.correspondence_delivery_status = LS_DB_FLAGS_TMP.correspondence_delivery_status,LS_DB_FLAGS.company_narrative_flag = LS_DB_FLAGS_TMP.company_narrative_flag,LS_DB_FLAGS.comp_rec_id = LS_DB_FLAGS_TMP.comp_rec_id,LS_DB_FLAGS.ari_rec_id = LS_DB_FLAGS_TMP.ari_rec_id,LS_DB_FLAGS.all_product_sub_track = LS_DB_FLAGS_TMP.all_product_sub_track,
LS_DB_FLAGS.PROCESSING_DT = LS_DB_FLAGS_TMP.PROCESSING_DT ,
LS_DB_FLAGS.receipt_id     =LS_DB_FLAGS_TMP.receipt_id        ,
LS_DB_FLAGS.case_no        =LS_DB_FLAGS_TMP.case_no           ,
LS_DB_FLAGS.case_version   =LS_DB_FLAGS_TMP.case_version      ,
LS_DB_FLAGS.version_no     =LS_DB_FLAGS_TMP.version_no        ,
LS_DB_FLAGS.expiry_date    =LS_DB_FLAGS_TMP.expiry_date       ,
LS_DB_FLAGS.load_ts        =LS_DB_FLAGS_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP 
WHERE 	LS_DB_FLAGS.INTEGRATION_ID = LS_DB_FLAGS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_FLAGS_TMP.PROCESSING_DT = LS_DB_FLAGS.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
turn_off_touchless_flag_de_ml,
turn_off_touchless_flag,
translate_req_flag_de_ml,
translate_req_flag,
spr_id,
safety_tag_flag,
safety_departments_flag,
requested_by,
req_created_date,
req_completed_date,
reported_term_flag,
redact_status,
record_id,
read_unread_correspondence,
rcpt_no,
question_followup_available,
question_followup,
patient_sex_flag,
patient_age_flag,
onset_date_flag,
ocr_partial_extraction,
ocr_extraction_report,
ocr_applied_de_ml,
ocr_applied,
nlp_invoc_ids,
nlp_confidence,
nlp_conf_breakup,
nlp_case_compare_flag_de_ml,
nlp_case_compare_flag,
narr_case_json,
medwatch_status,
lrn_version,
lrn,
link_ae_flag_de_ml,
link_ae_flag,
is_candidate_for_touchless,
inq_rec_id,
im_rec_id,
followup_notify_flag_de_ml,
followup_notify_flag,
foia_submitted_flag,
foia_submitted_date,
foia_status,
extraction_result,
extraction_invoc_id,
extraction_confidence,
event_outcome_flag,
document_flag,
document_checkout_flag,
distribution_status,
dispossed_through_e2b,
date_modified,
date_created,
data_percentage,
correspondence_seq,
correspondence_flag,
correspondence_delivery_status,
company_narrative_flag,
comp_rec_id,
ari_rec_id,
all_product_sub_track)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_created,
turn_off_touchless_flag_de_ml,
turn_off_touchless_flag,
translate_req_flag_de_ml,
translate_req_flag,
spr_id,
safety_tag_flag,
safety_departments_flag,
requested_by,
req_created_date,
req_completed_date,
reported_term_flag,
redact_status,
record_id,
read_unread_correspondence,
rcpt_no,
question_followup_available,
question_followup,
patient_sex_flag,
patient_age_flag,
onset_date_flag,
ocr_partial_extraction,
ocr_extraction_report,
ocr_applied_de_ml,
ocr_applied,
nlp_invoc_ids,
nlp_confidence,
nlp_conf_breakup,
nlp_case_compare_flag_de_ml,
nlp_case_compare_flag,
narr_case_json,
medwatch_status,
lrn_version,
lrn,
link_ae_flag_de_ml,
link_ae_flag,
is_candidate_for_touchless,
inq_rec_id,
im_rec_id,
followup_notify_flag_de_ml,
followup_notify_flag,
foia_submitted_flag,
foia_submitted_date,
foia_status,
extraction_result,
extraction_invoc_id,
extraction_confidence,
event_outcome_flag,
document_flag,
document_checkout_flag,
distribution_status,
dispossed_through_e2b,
date_modified,
date_created,
data_percentage,
correspondence_seq,
correspondence_flag,
correspondence_delivery_status,
company_narrative_flag,
comp_rec_id,
ari_rec_id,
all_product_sub_track
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP 
WHERE 	TO_DATE(LS_DB_FLAGS.PROCESSING_DT) < TO_DATE(LS_DB_FLAGS_TMP.PROCESSING_DT)
AND LS_DB_FLAGS.INTEGRATION_ID = LS_DB_FLAGS_TMP.INTEGRATION_ID
AND LS_DB_FLAGS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_FLAGS_DELETION_TMP  WHERE TABLE_NAME='lsmv_flags')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_FLAGS_DELETION_TMP  WHERE TABLE_NAME='lsmv_flags')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_FLAGS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_FLAGS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_FLAGS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_FLAGS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_FLAGS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_FLAGS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
