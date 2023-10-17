
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_REQUESTERS()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REQUESTERS'),
	'LSRA','Case','LS_DB_REQUESTERS',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_REQUESTERS' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_REQUESTERS'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_REQUESTERS'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REQUESTERS_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REQUESTERS_DELETION_TMP  As select RECORD_ID,'lsmv_requesters' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_requesters WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_requesters WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REQUESTERS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REQUESTERS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_requesters WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REQUESTERS' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_REQUESTERS' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_requesters_SUBSET AS 
(
select * from 
    (SELECT  
    affiliation  affiliation,ari_rec_id  ari_rec_id,comp_rec_id  comp_rec_id,contact_type  contact_type,cover_letter_name  cover_letter_name,cover_letter_record_id  cover_letter_record_id,cover_letter_response  cover_letter_response,date_created  date_created,date_modified  date_modified,default_contact_address  default_contact_address,fk_airr_rec_id  fk_airr_rec_id,inq_rec_id  inq_rec_id,is_reporter  is_reporter,modified_response  modified_response,modified_template_email_body  modified_template_email_body,modified_word_cover_data  modified_word_cover_data,modified_word_template_data  modified_word_template_data,preferred_address  preferred_address,preferred_address_comm_mode  preferred_address_comm_mode,record_id  record_id,requester_rec_id  requester_rec_id,response_status  response_status,spr_id  spr_id,template_name  template_name,template_record_id  template_record_id,user_created  user_created,user_id  user_id,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_requesters
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REQUESTERS_DELETION_TMP  WHERE TABLE_NAME='lsmv_requesters')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_requesters_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_requesters_SUBSET.USER_CREATED CREATED_BY,lsmv_requesters_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_requesters_SUBSET.user_modified  ,lsmv_requesters_SUBSET.user_id  ,lsmv_requesters_SUBSET.user_created  ,lsmv_requesters_SUBSET.template_record_id  ,lsmv_requesters_SUBSET.template_name  ,lsmv_requesters_SUBSET.spr_id  ,lsmv_requesters_SUBSET.response_status  ,lsmv_requesters_SUBSET.requester_rec_id  ,lsmv_requesters_SUBSET.record_id  ,lsmv_requesters_SUBSET.preferred_address_comm_mode  ,lsmv_requesters_SUBSET.preferred_address  ,lsmv_requesters_SUBSET.modified_word_template_data  ,lsmv_requesters_SUBSET.modified_word_cover_data  ,lsmv_requesters_SUBSET.modified_template_email_body  ,lsmv_requesters_SUBSET.modified_response  ,lsmv_requesters_SUBSET.is_reporter  ,lsmv_requesters_SUBSET.inq_rec_id  ,lsmv_requesters_SUBSET.fk_airr_rec_id  ,lsmv_requesters_SUBSET.default_contact_address  ,lsmv_requesters_SUBSET.date_modified  ,lsmv_requesters_SUBSET.date_created  ,lsmv_requesters_SUBSET.cover_letter_response  ,lsmv_requesters_SUBSET.cover_letter_record_id  ,lsmv_requesters_SUBSET.cover_letter_name  ,lsmv_requesters_SUBSET.contact_type  ,lsmv_requesters_SUBSET.comp_rec_id  ,lsmv_requesters_SUBSET.ari_rec_id  ,lsmv_requesters_SUBSET.affiliation ,CONCAT(NVL(lsmv_requesters_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_requesters_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_requesters_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP)
where target_table_name='LS_DB_REQUESTERS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REQUESTERS'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS   
SET LS_DB_REQUESTERS.user_modified = LS_DB_REQUESTERS_TMP.user_modified,LS_DB_REQUESTERS.user_id = LS_DB_REQUESTERS_TMP.user_id,LS_DB_REQUESTERS.user_created = LS_DB_REQUESTERS_TMP.user_created,LS_DB_REQUESTERS.template_record_id = LS_DB_REQUESTERS_TMP.template_record_id,LS_DB_REQUESTERS.template_name = LS_DB_REQUESTERS_TMP.template_name,LS_DB_REQUESTERS.spr_id = LS_DB_REQUESTERS_TMP.spr_id,LS_DB_REQUESTERS.response_status = LS_DB_REQUESTERS_TMP.response_status,LS_DB_REQUESTERS.requester_rec_id = LS_DB_REQUESTERS_TMP.requester_rec_id,LS_DB_REQUESTERS.record_id = LS_DB_REQUESTERS_TMP.record_id,LS_DB_REQUESTERS.preferred_address_comm_mode = LS_DB_REQUESTERS_TMP.preferred_address_comm_mode,LS_DB_REQUESTERS.preferred_address = LS_DB_REQUESTERS_TMP.preferred_address,LS_DB_REQUESTERS.modified_word_template_data = LS_DB_REQUESTERS_TMP.modified_word_template_data,LS_DB_REQUESTERS.modified_word_cover_data = LS_DB_REQUESTERS_TMP.modified_word_cover_data,LS_DB_REQUESTERS.modified_template_email_body = LS_DB_REQUESTERS_TMP.modified_template_email_body,LS_DB_REQUESTERS.modified_response = LS_DB_REQUESTERS_TMP.modified_response,LS_DB_REQUESTERS.is_reporter = LS_DB_REQUESTERS_TMP.is_reporter,LS_DB_REQUESTERS.inq_rec_id = LS_DB_REQUESTERS_TMP.inq_rec_id,LS_DB_REQUESTERS.fk_airr_rec_id = LS_DB_REQUESTERS_TMP.fk_airr_rec_id,LS_DB_REQUESTERS.default_contact_address = LS_DB_REQUESTERS_TMP.default_contact_address,LS_DB_REQUESTERS.date_modified = LS_DB_REQUESTERS_TMP.date_modified,LS_DB_REQUESTERS.date_created = LS_DB_REQUESTERS_TMP.date_created,LS_DB_REQUESTERS.cover_letter_response = LS_DB_REQUESTERS_TMP.cover_letter_response,LS_DB_REQUESTERS.cover_letter_record_id = LS_DB_REQUESTERS_TMP.cover_letter_record_id,LS_DB_REQUESTERS.cover_letter_name = LS_DB_REQUESTERS_TMP.cover_letter_name,LS_DB_REQUESTERS.contact_type = LS_DB_REQUESTERS_TMP.contact_type,LS_DB_REQUESTERS.comp_rec_id = LS_DB_REQUESTERS_TMP.comp_rec_id,LS_DB_REQUESTERS.ari_rec_id = LS_DB_REQUESTERS_TMP.ari_rec_id,LS_DB_REQUESTERS.affiliation = LS_DB_REQUESTERS_TMP.affiliation,
LS_DB_REQUESTERS.PROCESSING_DT = LS_DB_REQUESTERS_TMP.PROCESSING_DT ,
LS_DB_REQUESTERS.receipt_id     =LS_DB_REQUESTERS_TMP.receipt_id        ,
LS_DB_REQUESTERS.case_no        =LS_DB_REQUESTERS_TMP.case_no           ,
LS_DB_REQUESTERS.case_version   =LS_DB_REQUESTERS_TMP.case_version      ,
LS_DB_REQUESTERS.version_no     =LS_DB_REQUESTERS_TMP.version_no        ,
LS_DB_REQUESTERS.expiry_date    =LS_DB_REQUESTERS_TMP.expiry_date       ,
LS_DB_REQUESTERS.load_ts        =LS_DB_REQUESTERS_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP 
WHERE 	LS_DB_REQUESTERS.INTEGRATION_ID = LS_DB_REQUESTERS_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_REQUESTERS_TMP.PROCESSING_DT = LS_DB_REQUESTERS.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_id,
user_created,
template_record_id,
template_name,
spr_id,
response_status,
requester_rec_id,
record_id,
preferred_address_comm_mode,
preferred_address,
modified_word_template_data,
modified_word_cover_data,
modified_template_email_body,
modified_response,
is_reporter,
inq_rec_id,
fk_airr_rec_id,
default_contact_address,
date_modified,
date_created,
cover_letter_response,
cover_letter_record_id,
cover_letter_name,
contact_type,
comp_rec_id,
ari_rec_id,
affiliation)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,user_modified,
user_id,
user_created,
template_record_id,
template_name,
spr_id,
response_status,
requester_rec_id,
record_id,
preferred_address_comm_mode,
preferred_address,
modified_word_template_data,
modified_word_cover_data,
modified_template_email_body,
modified_response,
is_reporter,
inq_rec_id,
fk_airr_rec_id,
default_contact_address,
date_modified,
date_created,
cover_letter_response,
cover_letter_record_id,
cover_letter_name,
contact_type,
comp_rec_id,
ari_rec_id,
affiliation
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS TGT)
                                                                                ; 
COMMIT;



DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REQUESTERS_DELETION_TMP  WHERE TABLE_NAME='lsmv_requesters')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');

UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP 
WHERE 	TO_DATE(LS_DB_REQUESTERS.PROCESSING_DT) < TO_DATE(LS_DB_REQUESTERS_TMP.PROCESSING_DT)
AND LS_DB_REQUESTERS.INTEGRATION_ID = LS_DB_REQUESTERS_TMP.INTEGRATION_ID
AND LS_DB_REQUESTERS.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_REQUESTERS_DELETION_TMP  WHERE TABLE_NAME='lsmv_requesters')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_REQUESTERS_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REQUESTERS'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REQUESTERS'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_REQUESTERS Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_REQUESTERS'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_REQUESTERS not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
