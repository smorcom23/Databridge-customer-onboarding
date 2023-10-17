
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_SUBMISSION_LOCAL_APPROVAL()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'),
	'LSRA','Case','LS_DB_SUBMISSION_LOCAL_APPROVAL',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_SUBMISSION_LOCAL_APPROVAL' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_SUBMISSION_LOCAL_APPROVAL'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_SUBMISSION_LOCAL_APPROVAL'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SUBMISSION_LOCAL_APPROVAL_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SUBMISSION_LOCAL_APPROVAL_DELETION_TMP  As select RECORD_ID,'lsmv_st_local_approval' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_st_local_approval WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_st_local_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_LOCAL_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_LOCAL_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_st_local_approval WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_LOCAL_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_SUBMISSION_LOCAL_APPROVAL' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_st_local_approval_SUBSET AS 
(
select * from 
    (SELECT  
    approval_no  approval_no,approval_submission_type  approval_submission_type,approval_type  approval_type,ari_rec_id  ari_rec_id,arisg_approval_type  arisg_approval_type,cm_approval_record  cm_approval_record,cm_approval_record_id  cm_approval_record_id,country_code  country_code,date_created  date_created,date_modified  date_modified,fk_lsm_rec_id  fk_lsm_rec_id,jpn_drug_code  jpn_drug_code,made_by  made_by,marketing_auth_holder  marketing_auth_holder,marketing_auth_holder_name  marketing_auth_holder_name,product_type  product_type,record_id  record_id,record_owner_unit  record_owner_unit,selected_for_regeneration  selected_for_regeneration,seq_approval_no  seq_approval_no,seq_product  seq_product,spr_id  spr_id,submission_state  submission_state,to_be_synchronize  to_be_synchronize,trade_name  trade_name,trade_name_rec_id  trade_name_rec_id,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_st_local_approval
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SUBMISSION_LOCAL_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_local_approval')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_st_local_approval_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_st_local_approval_SUBSET.USER_CREATED CREATED_BY,lsmv_st_local_approval_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_st_local_approval_SUBSET.user_modified  ,lsmv_st_local_approval_SUBSET.user_created  ,lsmv_st_local_approval_SUBSET.trade_name_rec_id  ,lsmv_st_local_approval_SUBSET.trade_name  ,lsmv_st_local_approval_SUBSET.to_be_synchronize  ,lsmv_st_local_approval_SUBSET.submission_state  ,lsmv_st_local_approval_SUBSET.spr_id  ,lsmv_st_local_approval_SUBSET.seq_product  ,lsmv_st_local_approval_SUBSET.seq_approval_no  ,lsmv_st_local_approval_SUBSET.selected_for_regeneration  ,lsmv_st_local_approval_SUBSET.record_owner_unit  ,lsmv_st_local_approval_SUBSET.record_id  ,lsmv_st_local_approval_SUBSET.product_type  ,lsmv_st_local_approval_SUBSET.marketing_auth_holder_name  ,lsmv_st_local_approval_SUBSET.marketing_auth_holder  ,lsmv_st_local_approval_SUBSET.made_by  ,lsmv_st_local_approval_SUBSET.jpn_drug_code  ,lsmv_st_local_approval_SUBSET.fk_lsm_rec_id  ,lsmv_st_local_approval_SUBSET.date_modified  ,lsmv_st_local_approval_SUBSET.date_created  ,lsmv_st_local_approval_SUBSET.country_code  ,lsmv_st_local_approval_SUBSET.cm_approval_record_id  ,lsmv_st_local_approval_SUBSET.cm_approval_record  ,lsmv_st_local_approval_SUBSET.arisg_approval_type  ,lsmv_st_local_approval_SUBSET.ari_rec_id  ,lsmv_st_local_approval_SUBSET.approval_type  ,lsmv_st_local_approval_SUBSET.approval_submission_type  ,lsmv_st_local_approval_SUBSET.approval_no ,CONCAT(NVL(lsmv_st_local_approval_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_st_local_approval_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_st_local_approval_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP)
where target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL   
SET LS_DB_SUBMISSION_LOCAL_APPROVAL.user_modified = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.user_modified,LS_DB_SUBMISSION_LOCAL_APPROVAL.user_created = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.user_created,LS_DB_SUBMISSION_LOCAL_APPROVAL.trade_name_rec_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.trade_name_rec_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.trade_name = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.trade_name,LS_DB_SUBMISSION_LOCAL_APPROVAL.to_be_synchronize = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.to_be_synchronize,LS_DB_SUBMISSION_LOCAL_APPROVAL.submission_state = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.submission_state,LS_DB_SUBMISSION_LOCAL_APPROVAL.spr_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.spr_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.seq_product = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.seq_product,LS_DB_SUBMISSION_LOCAL_APPROVAL.seq_approval_no = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.seq_approval_no,LS_DB_SUBMISSION_LOCAL_APPROVAL.selected_for_regeneration = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.selected_for_regeneration,LS_DB_SUBMISSION_LOCAL_APPROVAL.record_owner_unit = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.record_owner_unit,LS_DB_SUBMISSION_LOCAL_APPROVAL.record_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.record_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.product_type = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.product_type,LS_DB_SUBMISSION_LOCAL_APPROVAL.marketing_auth_holder_name = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.marketing_auth_holder_name,LS_DB_SUBMISSION_LOCAL_APPROVAL.marketing_auth_holder = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.marketing_auth_holder,LS_DB_SUBMISSION_LOCAL_APPROVAL.made_by = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.made_by,LS_DB_SUBMISSION_LOCAL_APPROVAL.jpn_drug_code = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.jpn_drug_code,LS_DB_SUBMISSION_LOCAL_APPROVAL.fk_lsm_rec_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.fk_lsm_rec_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.date_modified = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.date_modified,LS_DB_SUBMISSION_LOCAL_APPROVAL.date_created = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.date_created,LS_DB_SUBMISSION_LOCAL_APPROVAL.country_code = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.country_code,LS_DB_SUBMISSION_LOCAL_APPROVAL.cm_approval_record_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.cm_approval_record_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.cm_approval_record = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.cm_approval_record,LS_DB_SUBMISSION_LOCAL_APPROVAL.arisg_approval_type = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.arisg_approval_type,LS_DB_SUBMISSION_LOCAL_APPROVAL.ari_rec_id = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.ari_rec_id,LS_DB_SUBMISSION_LOCAL_APPROVAL.approval_type = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.approval_type,LS_DB_SUBMISSION_LOCAL_APPROVAL.approval_submission_type = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.approval_submission_type,LS_DB_SUBMISSION_LOCAL_APPROVAL.approval_no = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.approval_no,
LS_DB_SUBMISSION_LOCAL_APPROVAL.PROCESSING_DT = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.PROCESSING_DT ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.receipt_id     =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.receipt_id        ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.case_no        =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.case_no           ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.case_version   =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.case_version      ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.version_no     =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.version_no        ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.expiry_date    =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.expiry_date       ,
LS_DB_SUBMISSION_LOCAL_APPROVAL.load_ts        =LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP 
WHERE 	LS_DB_SUBMISSION_LOCAL_APPROVAL.INTEGRATION_ID = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.PROCESSING_DT = LS_DB_SUBMISSION_LOCAL_APPROVAL.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL
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
trade_name_rec_id,
trade_name,
to_be_synchronize,
submission_state,
spr_id,
seq_product,
seq_approval_no,
selected_for_regeneration,
record_owner_unit,
record_id,
product_type,
marketing_auth_holder_name,
marketing_auth_holder,
made_by,
jpn_drug_code,
fk_lsm_rec_id,
date_modified,
date_created,
country_code,
cm_approval_record_id,
cm_approval_record,
arisg_approval_type,
ari_rec_id,
approval_type,
approval_submission_type,
approval_no)
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
trade_name_rec_id,
trade_name,
to_be_synchronize,
submission_state,
spr_id,
seq_product,
seq_approval_no,
selected_for_regeneration,
record_owner_unit,
record_id,
product_type,
marketing_auth_holder_name,
marketing_auth_holder,
made_by,
jpn_drug_code,
fk_lsm_rec_id,
date_modified,
date_created,
country_code,
cm_approval_record_id,
cm_approval_record,
arisg_approval_type,
ari_rec_id,
approval_type,
approval_submission_type,
approval_no
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP 
WHERE 	TO_DATE(LS_DB_SUBMISSION_LOCAL_APPROVAL.PROCESSING_DT) < TO_DATE(LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.PROCESSING_DT)
AND LS_DB_SUBMISSION_LOCAL_APPROVAL.INTEGRATION_ID = LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP.INTEGRATION_ID
AND LS_DB_SUBMISSION_LOCAL_APPROVAL.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SUBMISSION_LOCAL_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_local_approval')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_SUBMISSION_LOCAL_APPROVAL_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_local_approval')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_SUBMISSION_LOCAL_APPROVAL_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_SUBMISSION_LOCAL_APPROVAL Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_SUBMISSION_LOCAL_APPROVAL'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_SUBMISSION_LOCAL_APPROVAL not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
