
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_E2B_SUBMISSIONS()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_SUBMISSIONS'),
	'LSRA','Case','LS_DB_E2B_SUBMISSIONS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_E2B_SUBMISSIONS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_E2B_SUBMISSIONS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_E2B_SUBMISSIONS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_E2B_SUBMISSIONS_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_E2B_SUBMISSIONS_DELETION_TMP  As select RECORD_ID,'lsmv_e2b_submissions' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_e2b_submissions WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_e2b_submissions WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_E2B_SUBMISSIONS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_E2B_SUBMISSIONS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  FK_ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_e2b_submissions WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_E2B_SUBMISSIONS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_E2B_SUBMISSIONS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_e2b_submissions_SUBSET AS 
(
select * from 
    (SELECT  
    ack_doc_id  ack_doc_id,ack_encoding_format  ack_encoding_format,ack_icsr_message_number  ack_icsr_message_number,ack_import_enabled  ack_import_enabled,ack_message_date  ack_message_date,ack_xml_file_name  ack_xml_file_name,aer_info_rec_id  aer_info_rec_id,batch_id  batch_id,case_xml_export_status  case_xml_export_status,comments  comments,date_created  date_created,date_modified  date_modified,default_receiver  default_receiver,default_sender  default_sender,docs_ack_attach_file_name  docs_ack_attach_file_name,docs_ack_code  docs_ack_code,docs_ack_fda_rcv_date  docs_ack_fda_rcv_date,docs_ack_msg  docs_ack_msg,docs_ack_subject  docs_ack_subject,docs_xml_ack_data  docs_xml_ack_data,dtd_type  dtd_type,encoding_format  encoding_format,export_status  export_status,export_xml_file_name  export_xml_file_name,fk_ari_rec_id  fk_ari_rec_id,fk_case_export_queue  fk_case_export_queue,fk_inq_rec_id  fk_inq_rec_id,fk_ldd_rec_id  fk_ldd_rec_id,from_alert_rule  from_alert_rule,ichicsr_messagenumb  ichicsr_messagenumb,icsr_dtd_codelist_code  icsr_dtd_codelist_code,im_rec_id  im_rec_id,is_exception_on_export  is_exception_on_export,is_icsr_xml_doc_stored  is_icsr_xml_doc_stored,local_message_number  local_message_number,manual_abort_reason  manual_abort_reason,mdn_date  mdn_date,mdn_message  mdn_message,mdn_status  mdn_status,mdn_update_attempts  mdn_update_attempts,message_date  message_date,on_enter  on_enter,parsing_error_message  parsing_error_message,receiver  receiver,receiver_id  receiver_id,receiver_rec_id  receiver_rec_id,record_id  record_id,sender  sender,sender_id  sender_id,sender_rec_id  sender_rec_id,spr_id  spr_id,submission_date  submission_date,submission_rec_id  submission_rec_id,submit_by_mail  submit_by_mail,submitted_by  submitted_by,submitted_xml_data  submitted_xml_data,transmission_ack_code  transmission_ack_code,user_created  user_created,user_modified  user_modified,xml_ack_data  xml_ack_data,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_e2b_submissions
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_E2B_SUBMISSIONS_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_submissions')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_e2b_submissions_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_e2b_submissions_SUBSET.USER_CREATED CREATED_BY,lsmv_e2b_submissions_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_e2b_submissions_SUBSET.xml_ack_data  ,lsmv_e2b_submissions_SUBSET.user_modified  ,lsmv_e2b_submissions_SUBSET.user_created  ,lsmv_e2b_submissions_SUBSET.transmission_ack_code  ,lsmv_e2b_submissions_SUBSET.submitted_xml_data  ,lsmv_e2b_submissions_SUBSET.submitted_by  ,lsmv_e2b_submissions_SUBSET.submit_by_mail  ,lsmv_e2b_submissions_SUBSET.submission_rec_id  ,lsmv_e2b_submissions_SUBSET.submission_date  ,lsmv_e2b_submissions_SUBSET.spr_id  ,lsmv_e2b_submissions_SUBSET.sender_rec_id  ,lsmv_e2b_submissions_SUBSET.sender_id  ,lsmv_e2b_submissions_SUBSET.sender  ,lsmv_e2b_submissions_SUBSET.record_id  ,lsmv_e2b_submissions_SUBSET.receiver_rec_id  ,lsmv_e2b_submissions_SUBSET.receiver_id  ,lsmv_e2b_submissions_SUBSET.receiver  ,lsmv_e2b_submissions_SUBSET.parsing_error_message  ,lsmv_e2b_submissions_SUBSET.on_enter  ,lsmv_e2b_submissions_SUBSET.message_date  ,lsmv_e2b_submissions_SUBSET.mdn_update_attempts  ,lsmv_e2b_submissions_SUBSET.mdn_status  ,lsmv_e2b_submissions_SUBSET.mdn_message  ,lsmv_e2b_submissions_SUBSET.mdn_date  ,lsmv_e2b_submissions_SUBSET.manual_abort_reason  ,lsmv_e2b_submissions_SUBSET.local_message_number  ,lsmv_e2b_submissions_SUBSET.is_icsr_xml_doc_stored  ,lsmv_e2b_submissions_SUBSET.is_exception_on_export  ,lsmv_e2b_submissions_SUBSET.im_rec_id  ,lsmv_e2b_submissions_SUBSET.icsr_dtd_codelist_code  ,lsmv_e2b_submissions_SUBSET.ichicsr_messagenumb  ,lsmv_e2b_submissions_SUBSET.from_alert_rule  ,lsmv_e2b_submissions_SUBSET.fk_ldd_rec_id  ,lsmv_e2b_submissions_SUBSET.fk_inq_rec_id  ,lsmv_e2b_submissions_SUBSET.fk_case_export_queue  ,lsmv_e2b_submissions_SUBSET.fk_ari_rec_id  ,lsmv_e2b_submissions_SUBSET.export_xml_file_name  ,lsmv_e2b_submissions_SUBSET.export_status  ,lsmv_e2b_submissions_SUBSET.encoding_format  ,lsmv_e2b_submissions_SUBSET.dtd_type  ,lsmv_e2b_submissions_SUBSET.docs_xml_ack_data  ,lsmv_e2b_submissions_SUBSET.docs_ack_subject  ,lsmv_e2b_submissions_SUBSET.docs_ack_msg  ,lsmv_e2b_submissions_SUBSET.docs_ack_fda_rcv_date  ,lsmv_e2b_submissions_SUBSET.docs_ack_code  ,lsmv_e2b_submissions_SUBSET.docs_ack_attach_file_name  ,lsmv_e2b_submissions_SUBSET.default_sender  ,lsmv_e2b_submissions_SUBSET.default_receiver  ,lsmv_e2b_submissions_SUBSET.date_modified  ,lsmv_e2b_submissions_SUBSET.date_created  ,lsmv_e2b_submissions_SUBSET.comments  ,lsmv_e2b_submissions_SUBSET.case_xml_export_status  ,lsmv_e2b_submissions_SUBSET.batch_id  ,lsmv_e2b_submissions_SUBSET.aer_info_rec_id  ,lsmv_e2b_submissions_SUBSET.ack_xml_file_name  ,lsmv_e2b_submissions_SUBSET.ack_message_date  ,lsmv_e2b_submissions_SUBSET.ack_import_enabled  ,lsmv_e2b_submissions_SUBSET.ack_icsr_message_number  ,lsmv_e2b_submissions_SUBSET.ack_encoding_format  ,lsmv_e2b_submissions_SUBSET.ack_doc_id ,CONCAT(NVL(lsmv_e2b_submissions_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_e2b_submissions_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_e2b_submissions_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP)
where target_table_name='LS_DB_E2B_SUBMISSIONS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_SUBMISSIONS'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS   
SET LS_DB_E2B_SUBMISSIONS.xml_ack_data = LS_DB_E2B_SUBMISSIONS_TMP.xml_ack_data,LS_DB_E2B_SUBMISSIONS.user_modified = LS_DB_E2B_SUBMISSIONS_TMP.user_modified,LS_DB_E2B_SUBMISSIONS.user_created = LS_DB_E2B_SUBMISSIONS_TMP.user_created,LS_DB_E2B_SUBMISSIONS.transmission_ack_code = LS_DB_E2B_SUBMISSIONS_TMP.transmission_ack_code,LS_DB_E2B_SUBMISSIONS.submitted_xml_data = LS_DB_E2B_SUBMISSIONS_TMP.submitted_xml_data,LS_DB_E2B_SUBMISSIONS.submitted_by = LS_DB_E2B_SUBMISSIONS_TMP.submitted_by,LS_DB_E2B_SUBMISSIONS.submit_by_mail = LS_DB_E2B_SUBMISSIONS_TMP.submit_by_mail,LS_DB_E2B_SUBMISSIONS.submission_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.submission_rec_id,LS_DB_E2B_SUBMISSIONS.submission_date = LS_DB_E2B_SUBMISSIONS_TMP.submission_date,LS_DB_E2B_SUBMISSIONS.spr_id = LS_DB_E2B_SUBMISSIONS_TMP.spr_id,LS_DB_E2B_SUBMISSIONS.sender_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.sender_rec_id,LS_DB_E2B_SUBMISSIONS.sender_id = LS_DB_E2B_SUBMISSIONS_TMP.sender_id,LS_DB_E2B_SUBMISSIONS.sender = LS_DB_E2B_SUBMISSIONS_TMP.sender,LS_DB_E2B_SUBMISSIONS.record_id = LS_DB_E2B_SUBMISSIONS_TMP.record_id,LS_DB_E2B_SUBMISSIONS.receiver_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.receiver_rec_id,LS_DB_E2B_SUBMISSIONS.receiver_id = LS_DB_E2B_SUBMISSIONS_TMP.receiver_id,LS_DB_E2B_SUBMISSIONS.receiver = LS_DB_E2B_SUBMISSIONS_TMP.receiver,LS_DB_E2B_SUBMISSIONS.parsing_error_message = LS_DB_E2B_SUBMISSIONS_TMP.parsing_error_message,LS_DB_E2B_SUBMISSIONS.on_enter = LS_DB_E2B_SUBMISSIONS_TMP.on_enter,LS_DB_E2B_SUBMISSIONS.message_date = LS_DB_E2B_SUBMISSIONS_TMP.message_date,LS_DB_E2B_SUBMISSIONS.mdn_update_attempts = LS_DB_E2B_SUBMISSIONS_TMP.mdn_update_attempts,LS_DB_E2B_SUBMISSIONS.mdn_status = LS_DB_E2B_SUBMISSIONS_TMP.mdn_status,LS_DB_E2B_SUBMISSIONS.mdn_message = LS_DB_E2B_SUBMISSIONS_TMP.mdn_message,LS_DB_E2B_SUBMISSIONS.mdn_date = LS_DB_E2B_SUBMISSIONS_TMP.mdn_date,LS_DB_E2B_SUBMISSIONS.manual_abort_reason = LS_DB_E2B_SUBMISSIONS_TMP.manual_abort_reason,LS_DB_E2B_SUBMISSIONS.local_message_number = LS_DB_E2B_SUBMISSIONS_TMP.local_message_number,LS_DB_E2B_SUBMISSIONS.is_icsr_xml_doc_stored = LS_DB_E2B_SUBMISSIONS_TMP.is_icsr_xml_doc_stored,LS_DB_E2B_SUBMISSIONS.is_exception_on_export = LS_DB_E2B_SUBMISSIONS_TMP.is_exception_on_export,LS_DB_E2B_SUBMISSIONS.im_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.im_rec_id,LS_DB_E2B_SUBMISSIONS.icsr_dtd_codelist_code = LS_DB_E2B_SUBMISSIONS_TMP.icsr_dtd_codelist_code,LS_DB_E2B_SUBMISSIONS.ichicsr_messagenumb = LS_DB_E2B_SUBMISSIONS_TMP.ichicsr_messagenumb,LS_DB_E2B_SUBMISSIONS.from_alert_rule = LS_DB_E2B_SUBMISSIONS_TMP.from_alert_rule,LS_DB_E2B_SUBMISSIONS.fk_ldd_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.fk_ldd_rec_id,LS_DB_E2B_SUBMISSIONS.fk_inq_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.fk_inq_rec_id,LS_DB_E2B_SUBMISSIONS.fk_case_export_queue = LS_DB_E2B_SUBMISSIONS_TMP.fk_case_export_queue,LS_DB_E2B_SUBMISSIONS.fk_ari_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.fk_ari_rec_id,LS_DB_E2B_SUBMISSIONS.export_xml_file_name = LS_DB_E2B_SUBMISSIONS_TMP.export_xml_file_name,LS_DB_E2B_SUBMISSIONS.export_status = LS_DB_E2B_SUBMISSIONS_TMP.export_status,LS_DB_E2B_SUBMISSIONS.encoding_format = LS_DB_E2B_SUBMISSIONS_TMP.encoding_format,LS_DB_E2B_SUBMISSIONS.dtd_type = LS_DB_E2B_SUBMISSIONS_TMP.dtd_type,LS_DB_E2B_SUBMISSIONS.docs_xml_ack_data = LS_DB_E2B_SUBMISSIONS_TMP.docs_xml_ack_data,LS_DB_E2B_SUBMISSIONS.docs_ack_subject = LS_DB_E2B_SUBMISSIONS_TMP.docs_ack_subject,LS_DB_E2B_SUBMISSIONS.docs_ack_msg = LS_DB_E2B_SUBMISSIONS_TMP.docs_ack_msg,LS_DB_E2B_SUBMISSIONS.docs_ack_fda_rcv_date = LS_DB_E2B_SUBMISSIONS_TMP.docs_ack_fda_rcv_date,LS_DB_E2B_SUBMISSIONS.docs_ack_code = LS_DB_E2B_SUBMISSIONS_TMP.docs_ack_code,LS_DB_E2B_SUBMISSIONS.docs_ack_attach_file_name = LS_DB_E2B_SUBMISSIONS_TMP.docs_ack_attach_file_name,LS_DB_E2B_SUBMISSIONS.default_sender = LS_DB_E2B_SUBMISSIONS_TMP.default_sender,LS_DB_E2B_SUBMISSIONS.default_receiver = LS_DB_E2B_SUBMISSIONS_TMP.default_receiver,LS_DB_E2B_SUBMISSIONS.date_modified = LS_DB_E2B_SUBMISSIONS_TMP.date_modified,LS_DB_E2B_SUBMISSIONS.date_created = LS_DB_E2B_SUBMISSIONS_TMP.date_created,LS_DB_E2B_SUBMISSIONS.comments = LS_DB_E2B_SUBMISSIONS_TMP.comments,LS_DB_E2B_SUBMISSIONS.case_xml_export_status = LS_DB_E2B_SUBMISSIONS_TMP.case_xml_export_status,LS_DB_E2B_SUBMISSIONS.batch_id = LS_DB_E2B_SUBMISSIONS_TMP.batch_id,LS_DB_E2B_SUBMISSIONS.aer_info_rec_id = LS_DB_E2B_SUBMISSIONS_TMP.aer_info_rec_id,LS_DB_E2B_SUBMISSIONS.ack_xml_file_name = LS_DB_E2B_SUBMISSIONS_TMP.ack_xml_file_name,LS_DB_E2B_SUBMISSIONS.ack_message_date = LS_DB_E2B_SUBMISSIONS_TMP.ack_message_date,LS_DB_E2B_SUBMISSIONS.ack_import_enabled = LS_DB_E2B_SUBMISSIONS_TMP.ack_import_enabled,LS_DB_E2B_SUBMISSIONS.ack_icsr_message_number = LS_DB_E2B_SUBMISSIONS_TMP.ack_icsr_message_number,LS_DB_E2B_SUBMISSIONS.ack_encoding_format = LS_DB_E2B_SUBMISSIONS_TMP.ack_encoding_format,LS_DB_E2B_SUBMISSIONS.ack_doc_id = LS_DB_E2B_SUBMISSIONS_TMP.ack_doc_id,
LS_DB_E2B_SUBMISSIONS.PROCESSING_DT = LS_DB_E2B_SUBMISSIONS_TMP.PROCESSING_DT ,
LS_DB_E2B_SUBMISSIONS.receipt_id     =LS_DB_E2B_SUBMISSIONS_TMP.receipt_id        ,
LS_DB_E2B_SUBMISSIONS.case_no        =LS_DB_E2B_SUBMISSIONS_TMP.case_no           ,
LS_DB_E2B_SUBMISSIONS.case_version   =LS_DB_E2B_SUBMISSIONS_TMP.case_version      ,
LS_DB_E2B_SUBMISSIONS.version_no     =LS_DB_E2B_SUBMISSIONS_TMP.version_no        ,
LS_DB_E2B_SUBMISSIONS.expiry_date    =LS_DB_E2B_SUBMISSIONS_TMP.expiry_date       ,
LS_DB_E2B_SUBMISSIONS.load_ts        =LS_DB_E2B_SUBMISSIONS_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP 
WHERE 	LS_DB_E2B_SUBMISSIONS.INTEGRATION_ID = LS_DB_E2B_SUBMISSIONS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_E2B_SUBMISSIONS_TMP.PROCESSING_DT = LS_DB_E2B_SUBMISSIONS.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,xml_ack_data,
user_modified,
user_created,
transmission_ack_code,
submitted_xml_data,
submitted_by,
submit_by_mail,
submission_rec_id,
submission_date,
spr_id,
sender_rec_id,
sender_id,
sender,
record_id,
receiver_rec_id,
receiver_id,
receiver,
parsing_error_message,
on_enter,
message_date,
mdn_update_attempts,
mdn_status,
mdn_message,
mdn_date,
manual_abort_reason,
local_message_number,
is_icsr_xml_doc_stored,
is_exception_on_export,
im_rec_id,
icsr_dtd_codelist_code,
ichicsr_messagenumb,
from_alert_rule,
fk_ldd_rec_id,
fk_inq_rec_id,
fk_case_export_queue,
fk_ari_rec_id,
export_xml_file_name,
export_status,
encoding_format,
dtd_type,
docs_xml_ack_data,
docs_ack_subject,
docs_ack_msg,
docs_ack_fda_rcv_date,
docs_ack_code,
docs_ack_attach_file_name,
default_sender,
default_receiver,
date_modified,
date_created,
comments,
case_xml_export_status,
batch_id,
aer_info_rec_id,
ack_xml_file_name,
ack_message_date,
ack_import_enabled,
ack_icsr_message_number,
ack_encoding_format,
ack_doc_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,xml_ack_data,
user_modified,
user_created,
transmission_ack_code,
submitted_xml_data,
submitted_by,
submit_by_mail,
submission_rec_id,
submission_date,
spr_id,
sender_rec_id,
sender_id,
sender,
record_id,
receiver_rec_id,
receiver_id,
receiver,
parsing_error_message,
on_enter,
message_date,
mdn_update_attempts,
mdn_status,
mdn_message,
mdn_date,
manual_abort_reason,
local_message_number,
is_icsr_xml_doc_stored,
is_exception_on_export,
im_rec_id,
icsr_dtd_codelist_code,
ichicsr_messagenumb,
from_alert_rule,
fk_ldd_rec_id,
fk_inq_rec_id,
fk_case_export_queue,
fk_ari_rec_id,
export_xml_file_name,
export_status,
encoding_format,
dtd_type,
docs_xml_ack_data,
docs_ack_subject,
docs_ack_msg,
docs_ack_fda_rcv_date,
docs_ack_code,
docs_ack_attach_file_name,
default_sender,
default_receiver,
date_modified,
date_created,
comments,
case_xml_export_status,
batch_id,
aer_info_rec_id,
ack_xml_file_name,
ack_message_date,
ack_import_enabled,
ack_icsr_message_number,
ack_encoding_format,
ack_doc_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS TGT)
                                                                                ; 
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_E2B_SUBMISSIONS_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_submissions')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP 
WHERE 	TO_DATE(LS_DB_E2B_SUBMISSIONS.PROCESSING_DT) < TO_DATE(LS_DB_E2B_SUBMISSIONS_TMP.PROCESSING_DT)
AND LS_DB_E2B_SUBMISSIONS.INTEGRATION_ID = LS_DB_E2B_SUBMISSIONS_TMP.INTEGRATION_ID
AND LS_DB_E2B_SUBMISSIONS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_E2B_SUBMISSIONS_DELETION_TMP  WHERE TABLE_NAME='lsmv_e2b_submissions')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_E2B_SUBMISSIONS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_E2B_SUBMISSIONS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_E2B_SUBMISSIONS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_E2B_SUBMISSIONS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_E2B_SUBMISSIONS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_E2B_SUBMISSIONS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
