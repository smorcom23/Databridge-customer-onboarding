
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_ACCOUNTS_INFO()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_INFO'),
	'LSRA','Case','LS_DB_ACCOUNTS_INFO',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_ACCOUNTS_INFO' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_ACCOUNTS_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_ACCOUNTS_INFO'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  As select RECORD_ID,'lsmv_account_accountgroup' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_accountgroup WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_account_company_unit' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_company_unit WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_account_prod_distribution' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_prod_distribution WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_account_product' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_product WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_accounts' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_accounts_contacts' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_contacts WHERE CDC_OPERATION_TYPE IN ('D') UNION ALL select RECORD_ID,'lsmv_accounts_group' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_group WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP  AS WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_product WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_ACCOUNT_PRODUCT_ID record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_product WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_accountgroup WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_account_rec_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_accountgroup WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_contacts WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_account_record_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_contacts WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_prod_distribution WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_ACCOUNT_ID record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_prod_distribution WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT fk_account_record_id record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_group WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT RECORD_ID record_id, 0 common_parent_key   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT FK_ACCOUNT_RECORD_ID record_id, 0 common_parent_key  FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_account_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_ACCOUNTS_INFO' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
)
 , lsmv_account_product_SUBSET AS 
(
select * from 
    (SELECT  
    approval_no  accprd_approval_no,country  accprd_country,date_created  accprd_date_created,date_modified  accprd_date_modified,fk_account_product_id  accprd_fk_account_product_id,fk_product_record_id  accprd_fk_product_record_id,local_tradename  accprd_local_tradename,product_class_code  accprd_product_class_code,product_status  accprd_product_status,product_type  accprd_product_type,record_id  accprd_record_id,report_type  accprd_report_type,spr_id  accprd_spr_id,tradename_recid  accprd_tradename_recid,user_created  accprd_user_created,user_modified  accprd_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_product
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_ACCOUNT_PRODUCT_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_product')
  ) where REC_RANK=1 )
  , lsmv_accounts_group_SUBSET AS 
(
select * from 
    (SELECT  
    accounts_group_name  acgrp_accounts_group_name,date_created  acgrp_date_created,date_modified  acgrp_date_modified,description  acgrp_description,fk_account_record_id  acgrp_fk_account_record_id,is_active  acgrp_is_active,reason_for_deactive  acgrp_reason_for_deactive,record_id  acgrp_record_id,spr_id  acgrp_spr_id,user_created  acgrp_user_created,user_modified  acgrp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_group
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_account_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_group')
  ) where REC_RANK=1 )
  , lsmv_accounts_contacts_SUBSET AS 
(
select * from 
    (SELECT  
    contact_distribute  acccont_contact_distribute,contact_record_id  acccont_contact_record_id,date_created  acccont_date_created,date_modified  acccont_date_modified,fk_account_record_id  acccont_fk_account_record_id,fk_partner_record_id  acccont_fk_partner_record_id,interchange_id  acccont_interchange_id,is_e2b_contact  acccont_is_e2b_contact,is_primary_contact  acccont_is_primary_contact,language  acccont_language,parent_name  acccont_parent_name,record_id  acccont_record_id,spr_id  acccont_spr_id,user_created  acccont_user_created,user_modified  acccont_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts_contacts
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_account_record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_contacts')
  ) where REC_RANK=1 )
  , lsmv_account_accountgroup_SUBSET AS 
(
select * from 
    (SELECT  
    date_created  accgrp_date_created,date_modified  accgrp_date_modified,fk_account_rec_id  accgrp_fk_account_rec_id,fk_accountgroup_rec_id  accgrp_fk_accountgroup_rec_id,record_id  accgrp_record_id,spr_id  accgrp_spr_id,user_created  accgrp_user_created,user_modified  accgrp_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_accountgroup
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
fk_account_rec_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_accountgroup')
  ) where REC_RANK=1 )
  , lsmv_account_company_unit_SUBSET AS 
(
select * from 
    (SELECT  
    company_unit_record_id  acccmpunit_company_unit_record_id,date_created  acccmpunit_date_created,date_modified  acccmpunit_date_modified,fk_account_record_id  acccmpunit_fk_account_record_id,record_id  acccmpunit_record_id,spr_id  acccmpunit_spr_id,user_created  acccmpunit_user_created,user_modified  acccmpunit_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_company_unit
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_ACCOUNT_RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_company_unit')
  ) where REC_RANK=1 )
  , lsmv_accounts_SUBSET AS 
(
select * from 
    (SELECT  
    acc_ins_from_schedler  acc_acc_ins_from_schedler,account_active  acc_account_active,account_group_name  acc_account_group_name,account_group_rec_id  acc_account_group_rec_id,account_id  acc_account_id,account_manager  acc_account_manager,account_name  acc_account_name,account_type  acc_account_type,ack_comments_length  acc_ack_comments_length,address  acc_address,all_company_unit  acc_all_company_unit,all_product_assign  acc_all_product_assign,all_product_class_assign  acc_all_product_class_assign,all_product_distr_assign  acc_all_product_distr_assign,assign_to  acc_assign_to,assigned_to  acc_assigned_to,casuality_methods  acc_casuality_methods,casuality_results  acc_casuality_results,city  acc_city,company_status  acc_company_status,compression  acc_compression,correspondence_flag  acc_correspondence_flag,correspondence_seq  acc_correspondence_seq,country  acc_country,cover_letter_tmpl_id  acc_cover_letter_tmpl_id,create_date  acc_create_date,crm_last_modified_date  acc_crm_last_modified_date,crm_record_id  acc_crm_record_id,crm_source  acc_crm_source,crm_source_flag  acc_crm_source_flag,date_created  acc_date_created,date_modified  acc_date_modified,description  acc_description,distribution_setting  acc_distribution_setting,domain  acc_domain,duns_no  acc_duns_no,dup_acc  acc_dup_acc,e2b_encoding_format  acc_e2b_encoding_format,e2b_meddra  acc_e2b_meddra,email  acc_email,email_exclusions  acc_email_exclusions,eudamed_number  acc_eudamed_number,exclusion_account  acc_exclusion_account,exclusion_partner  acc_exclusion_partner,fax  acc_fax,fax_area_code  acc_fax_area_code,fax_country_code  acc_fax_country_code,fax_domain  acc_fax_domain,fax_inbound_directory  acc_fax_inbound_directory,fei_no  acc_fei_no,firm_address  acc_firm_address,firm_city  acc_firm_city,firm_country  acc_firm_country,firm_function  acc_firm_function,firm_name  acc_firm_name,firm_postalcode  acc_firm_postalcode,firm_state  acc_firm_state,fk_aec_rec_id  acc_fk_aec_rec_id,hc_assigned_company_id  acc_hc_assigned_company_id,hc_assigned_estd_lic_no  acc_hc_assigned_estd_lic_no,health_authority  acc_health_authority,incoming_source_lang  acc_incoming_source_lang,industry  acc_industry,ird_lrd  acc_ird_lrd,is_distribution_contact  acc_is_distribution_contact,is_e2b_account  acc_is_e2b_account,keyword  acc_keyword,language  acc_language,mask_exclusion_id  acc_mask_exclusion_id,mask_id  acc_mask_id,migration_flag  acc_migration_flag,notes_flag  acc_notes_flag,parent_fei_no  acc_parent_fei_no,parent_firm_address  acc_parent_firm_address,parent_firm_city  acc_parent_firm_city,parent_firm_country  acc_parent_firm_country,parent_firm_name  acc_parent_firm_name,parent_firm_postalcode  acc_parent_firm_postalcode,parent_firm_state  acc_parent_firm_state,phone_area_code  acc_phone_area_code,phone_country_code  acc_phone_country_code,phone_no  acc_phone_no,po_box  acc_po_box,postal_code  acc_postal_code,product_class_type  acc_product_class_type,qc_sampling_count  acc_qc_sampling_count,qc_sampling_running_count  acc_qc_sampling_running_count,read_unread_correspondence  acc_read_unread_correspondence,reason_for_deactive  acc_reason_for_deactive,recalculate_safety_report_id  acc_recalculate_safety_report_id,receiver_id  acc_receiver_id,reconciliation  acc_reconciliation,record_id  acc_record_id,region  acc_region,region_code  acc_region_code,report_type  acc_report_type,reporting_medium  acc_reporting_medium,sender_organization_type  acc_sender_organization_type,single_reg_num  acc_single_reg_num,spr_id  acc_spr_id,state  acc_state,status_date  acc_status_date,task_flag  acc_task_flag,time_zone  acc_time_zone,user_created  acc_user_created,user_modified  acc_user_modified,vet_report_type  acc_vet_report_type,website  acc_website,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_accounts
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts')
  ) where REC_RANK=1 )
  , lsmv_account_prod_distribution_SUBSET AS 
(
select * from 
    (SELECT  
    approval_no  accprddist_approval_no,country  accprddist_country,date_created  accprddist_date_created,date_modified  accprddist_date_modified,fk_account_id  accprddist_fk_account_id,fk_product_record_id  accprddist_fk_product_record_id,local_tradename  accprddist_local_tradename,product_class_code  accprddist_product_class_code,product_status  accprddist_product_status,product_type  accprddist_product_type,record_id  accprddist_record_id,report_type  accprddist_report_type,reporting_medium  accprddist_reporting_medium,spr_id  accprddist_spr_id,tradename_recid  accprddist_tradename_recid,user_created  accprddist_user_created,user_modified  accprddist_user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_account_prod_distribution
 WHERE ( RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) OR
FK_ACCOUNT_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET))
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_prod_distribution')
  ) where REC_RANK=1 )
    SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_account_company_unit_SUBSET.acccmpunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_accounts_group_SUBSET.acgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_account_prod_distribution_SUBSET.accprddist_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_accounts_contacts_SUBSET.acccont_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_account_accountgroup_SUBSET.accgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_accounts_SUBSET.acc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),NVL(lsmv_account_product_SUBSET.accprd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,lsmv_accounts_SUBSET.acc_USER_MODIFIED USER_MODIFIED,lsmv_accounts_SUBSET.acc_DATE_MODIFIED DATE_MODIFIED,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_accounts_SUBSET.acc_USER_CREATED CREATED_BY,lsmv_accounts_SUBSET.acc_DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_accounts_SUBSET.acc_website  ,lsmv_accounts_SUBSET.acc_vet_report_type  ,lsmv_accounts_SUBSET.acc_user_modified  ,lsmv_accounts_SUBSET.acc_user_created  ,lsmv_accounts_SUBSET.acc_time_zone  ,lsmv_accounts_SUBSET.acc_task_flag  ,lsmv_accounts_SUBSET.acc_status_date  ,lsmv_accounts_SUBSET.acc_state  ,lsmv_accounts_SUBSET.acc_spr_id  ,lsmv_accounts_SUBSET.acc_single_reg_num  ,lsmv_accounts_SUBSET.acc_sender_organization_type  ,lsmv_accounts_SUBSET.acc_reporting_medium  ,lsmv_accounts_SUBSET.acc_report_type  ,lsmv_accounts_SUBSET.acc_region_code  ,lsmv_accounts_SUBSET.acc_region  ,lsmv_accounts_SUBSET.acc_record_id  ,lsmv_accounts_SUBSET.acc_reconciliation  ,lsmv_accounts_SUBSET.acc_receiver_id  ,lsmv_accounts_SUBSET.acc_recalculate_safety_report_id  ,lsmv_accounts_SUBSET.acc_reason_for_deactive  ,lsmv_accounts_SUBSET.acc_read_unread_correspondence  ,lsmv_accounts_SUBSET.acc_qc_sampling_running_count  ,lsmv_accounts_SUBSET.acc_qc_sampling_count  ,lsmv_accounts_SUBSET.acc_product_class_type  ,lsmv_accounts_SUBSET.acc_postal_code  ,lsmv_accounts_SUBSET.acc_po_box  ,lsmv_accounts_SUBSET.acc_phone_no  ,lsmv_accounts_SUBSET.acc_phone_country_code  ,lsmv_accounts_SUBSET.acc_phone_area_code  ,lsmv_accounts_SUBSET.acc_parent_firm_state  ,lsmv_accounts_SUBSET.acc_parent_firm_postalcode  ,lsmv_accounts_SUBSET.acc_parent_firm_name  ,lsmv_accounts_SUBSET.acc_parent_firm_country  ,lsmv_accounts_SUBSET.acc_parent_firm_city  ,lsmv_accounts_SUBSET.acc_parent_firm_address  ,lsmv_accounts_SUBSET.acc_parent_fei_no  ,lsmv_accounts_SUBSET.acc_notes_flag  ,lsmv_accounts_SUBSET.acc_migration_flag  ,lsmv_accounts_SUBSET.acc_mask_id  ,lsmv_accounts_SUBSET.acc_mask_exclusion_id  ,lsmv_accounts_SUBSET.acc_language  ,lsmv_accounts_SUBSET.acc_keyword  ,lsmv_accounts_SUBSET.acc_is_e2b_account  ,lsmv_accounts_SUBSET.acc_is_distribution_contact  ,lsmv_accounts_SUBSET.acc_ird_lrd  ,lsmv_accounts_SUBSET.acc_industry  ,lsmv_accounts_SUBSET.acc_incoming_source_lang  ,lsmv_accounts_SUBSET.acc_health_authority  ,lsmv_accounts_SUBSET.acc_hc_assigned_estd_lic_no  ,lsmv_accounts_SUBSET.acc_hc_assigned_company_id  ,lsmv_accounts_SUBSET.acc_fk_aec_rec_id  ,lsmv_accounts_SUBSET.acc_firm_state  ,lsmv_accounts_SUBSET.acc_firm_postalcode  ,lsmv_accounts_SUBSET.acc_firm_name  ,lsmv_accounts_SUBSET.acc_firm_function  ,lsmv_accounts_SUBSET.acc_firm_country  ,lsmv_accounts_SUBSET.acc_firm_city  ,lsmv_accounts_SUBSET.acc_firm_address  ,lsmv_accounts_SUBSET.acc_fei_no  ,lsmv_accounts_SUBSET.acc_fax_inbound_directory  ,lsmv_accounts_SUBSET.acc_fax_domain  ,lsmv_accounts_SUBSET.acc_fax_country_code  ,lsmv_accounts_SUBSET.acc_fax_area_code  ,lsmv_accounts_SUBSET.acc_fax  ,lsmv_accounts_SUBSET.acc_exclusion_partner  ,lsmv_accounts_SUBSET.acc_exclusion_account  ,lsmv_accounts_SUBSET.acc_eudamed_number  ,lsmv_accounts_SUBSET.acc_email_exclusions  ,lsmv_accounts_SUBSET.acc_email  ,lsmv_accounts_SUBSET.acc_e2b_meddra  ,lsmv_accounts_SUBSET.acc_e2b_encoding_format  ,lsmv_accounts_SUBSET.acc_dup_acc  ,lsmv_accounts_SUBSET.acc_duns_no  ,lsmv_accounts_SUBSET.acc_domain  ,lsmv_accounts_SUBSET.acc_distribution_setting  ,lsmv_accounts_SUBSET.acc_description  ,lsmv_accounts_SUBSET.acc_date_modified  ,lsmv_accounts_SUBSET.acc_date_created  ,lsmv_accounts_SUBSET.acc_crm_source_flag  ,lsmv_accounts_SUBSET.acc_crm_source  ,lsmv_accounts_SUBSET.acc_crm_record_id  ,lsmv_accounts_SUBSET.acc_crm_last_modified_date  ,lsmv_accounts_SUBSET.acc_create_date  ,lsmv_accounts_SUBSET.acc_cover_letter_tmpl_id  ,lsmv_accounts_SUBSET.acc_country  ,lsmv_accounts_SUBSET.acc_correspondence_seq  ,lsmv_accounts_SUBSET.acc_correspondence_flag  ,lsmv_accounts_SUBSET.acc_compression  ,lsmv_accounts_SUBSET.acc_company_status  ,lsmv_accounts_SUBSET.acc_city  ,lsmv_accounts_SUBSET.acc_casuality_results  ,lsmv_accounts_SUBSET.acc_casuality_methods  ,lsmv_accounts_SUBSET.acc_assigned_to  ,lsmv_accounts_SUBSET.acc_assign_to  ,lsmv_accounts_SUBSET.acc_all_product_distr_assign  ,lsmv_accounts_SUBSET.acc_all_product_class_assign  ,lsmv_accounts_SUBSET.acc_all_product_assign  ,lsmv_accounts_SUBSET.acc_all_company_unit  ,lsmv_accounts_SUBSET.acc_address  ,lsmv_accounts_SUBSET.acc_ack_comments_length  ,lsmv_accounts_SUBSET.acc_account_type  ,lsmv_accounts_SUBSET.acc_account_name  ,lsmv_accounts_SUBSET.acc_account_manager  ,lsmv_accounts_SUBSET.acc_account_id  ,lsmv_accounts_SUBSET.acc_account_group_rec_id  ,lsmv_accounts_SUBSET.acc_account_group_name  ,lsmv_accounts_SUBSET.acc_account_active  ,lsmv_accounts_SUBSET.acc_acc_ins_from_schedler  ,lsmv_accounts_group_SUBSET.acgrp_user_modified  ,lsmv_accounts_group_SUBSET.acgrp_user_created  ,lsmv_accounts_group_SUBSET.acgrp_spr_id  ,lsmv_accounts_group_SUBSET.acgrp_record_id  ,lsmv_accounts_group_SUBSET.acgrp_reason_for_deactive  ,lsmv_accounts_group_SUBSET.acgrp_is_active  ,lsmv_accounts_group_SUBSET.acgrp_fk_account_record_id  ,lsmv_accounts_group_SUBSET.acgrp_description  ,lsmv_accounts_group_SUBSET.acgrp_date_modified  ,lsmv_accounts_group_SUBSET.acgrp_date_created  ,lsmv_accounts_group_SUBSET.acgrp_accounts_group_name  ,lsmv_accounts_contacts_SUBSET.acccont_user_modified  ,lsmv_accounts_contacts_SUBSET.acccont_user_created  ,lsmv_accounts_contacts_SUBSET.acccont_spr_id  ,lsmv_accounts_contacts_SUBSET.acccont_record_id  ,lsmv_accounts_contacts_SUBSET.acccont_parent_name  ,lsmv_accounts_contacts_SUBSET.acccont_language  ,lsmv_accounts_contacts_SUBSET.acccont_is_primary_contact  ,lsmv_accounts_contacts_SUBSET.acccont_is_e2b_contact  ,lsmv_accounts_contacts_SUBSET.acccont_interchange_id  ,lsmv_accounts_contacts_SUBSET.acccont_fk_partner_record_id  ,lsmv_accounts_contacts_SUBSET.acccont_fk_account_record_id  ,lsmv_accounts_contacts_SUBSET.acccont_date_modified  ,lsmv_accounts_contacts_SUBSET.acccont_date_created  ,lsmv_accounts_contacts_SUBSET.acccont_contact_record_id  ,lsmv_accounts_contacts_SUBSET.acccont_contact_distribute  ,lsmv_account_product_SUBSET.accprd_user_modified  ,lsmv_account_product_SUBSET.accprd_user_created  ,lsmv_account_product_SUBSET.accprd_tradename_recid  ,lsmv_account_product_SUBSET.accprd_spr_id  ,lsmv_account_product_SUBSET.accprd_report_type  ,lsmv_account_product_SUBSET.accprd_record_id  ,lsmv_account_product_SUBSET.accprd_product_type  ,lsmv_account_product_SUBSET.accprd_product_status  ,lsmv_account_product_SUBSET.accprd_product_class_code  ,lsmv_account_product_SUBSET.accprd_local_tradename  ,lsmv_account_product_SUBSET.accprd_fk_product_record_id  ,lsmv_account_product_SUBSET.accprd_fk_account_product_id  ,lsmv_account_product_SUBSET.accprd_date_modified  ,lsmv_account_product_SUBSET.accprd_date_created  ,lsmv_account_product_SUBSET.accprd_country  ,lsmv_account_product_SUBSET.accprd_approval_no  ,lsmv_account_prod_distribution_SUBSET.accprddist_user_modified  ,lsmv_account_prod_distribution_SUBSET.accprddist_user_created  ,lsmv_account_prod_distribution_SUBSET.accprddist_tradename_recid  ,lsmv_account_prod_distribution_SUBSET.accprddist_spr_id  ,lsmv_account_prod_distribution_SUBSET.accprddist_reporting_medium  ,lsmv_account_prod_distribution_SUBSET.accprddist_report_type  ,lsmv_account_prod_distribution_SUBSET.accprddist_record_id  ,lsmv_account_prod_distribution_SUBSET.accprddist_product_type  ,lsmv_account_prod_distribution_SUBSET.accprddist_product_status  ,lsmv_account_prod_distribution_SUBSET.accprddist_product_class_code  ,lsmv_account_prod_distribution_SUBSET.accprddist_local_tradename  ,lsmv_account_prod_distribution_SUBSET.accprddist_fk_product_record_id  ,lsmv_account_prod_distribution_SUBSET.accprddist_fk_account_id  ,lsmv_account_prod_distribution_SUBSET.accprddist_date_modified  ,lsmv_account_prod_distribution_SUBSET.accprddist_date_created  ,lsmv_account_prod_distribution_SUBSET.accprddist_country  ,lsmv_account_prod_distribution_SUBSET.accprddist_approval_no  ,lsmv_account_company_unit_SUBSET.acccmpunit_user_modified  ,lsmv_account_company_unit_SUBSET.acccmpunit_user_created  ,lsmv_account_company_unit_SUBSET.acccmpunit_spr_id  ,lsmv_account_company_unit_SUBSET.acccmpunit_record_id  ,lsmv_account_company_unit_SUBSET.acccmpunit_fk_account_record_id  ,lsmv_account_company_unit_SUBSET.acccmpunit_date_modified  ,lsmv_account_company_unit_SUBSET.acccmpunit_date_created  ,lsmv_account_company_unit_SUBSET.acccmpunit_company_unit_record_id  ,lsmv_account_accountgroup_SUBSET.accgrp_user_modified  ,lsmv_account_accountgroup_SUBSET.accgrp_user_created  ,lsmv_account_accountgroup_SUBSET.accgrp_spr_id  ,lsmv_account_accountgroup_SUBSET.accgrp_record_id  ,lsmv_account_accountgroup_SUBSET.accgrp_fk_accountgroup_rec_id  ,lsmv_account_accountgroup_SUBSET.accgrp_fk_account_rec_id  ,lsmv_account_accountgroup_SUBSET.accgrp_date_modified  ,lsmv_account_accountgroup_SUBSET.accgrp_date_created ,CONCAT(NVL(lsmv_account_company_unit_SUBSET.acccmpunit_RECORD_ID,-1),'||',NVL(lsmv_accounts_group_SUBSET.acgrp_RECORD_ID,-1),'||',NVL(lsmv_account_prod_distribution_SUBSET.accprddist_RECORD_ID,-1),'||',NVL(lsmv_accounts_contacts_SUBSET.acccont_RECORD_ID,-1),'||',NVL(lsmv_account_accountgroup_SUBSET.accgrp_RECORD_ID,-1),'||',NVL(lsmv_accounts_SUBSET.acc_RECORD_ID,-1),'||',NVL(lsmv_account_product_SUBSET.accprd_RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_accounts_SUBSET  LEFT JOIN lsmv_accounts_group_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_accounts_group_SUBSET.acgrp_fk_account_record_id
                         LEFT JOIN lsmv_account_accountgroup_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_account_accountgroup_SUBSET.accgrp_fk_account_rec_id
                         LEFT JOIN lsmv_accounts_contacts_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_accounts_contacts_SUBSET.acccont_fk_account_record_id
                         LEFT JOIN lsmv_account_company_unit_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_account_company_unit_SUBSET.acccmpunit_FK_ACCOUNT_RECORD_ID
                         LEFT JOIN lsmv_account_prod_distribution_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_account_prod_distribution_SUBSET.accprddist_FK_ACCOUNT_ID
                         LEFT JOIN lsmv_account_product_SUBSET ON lsmv_accounts_SUBSET.acc_record_id=lsmv_account_product_SUBSET.accprd_FK_ACCOUNT_PRODUCT_ID
                         WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP)
where target_table_name='LS_DB_ACCOUNTS_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_INFO'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO   
SET LS_DB_ACCOUNTS_INFO.acc_website = LS_DB_ACCOUNTS_INFO_TMP.acc_website,LS_DB_ACCOUNTS_INFO.acc_vet_report_type = LS_DB_ACCOUNTS_INFO_TMP.acc_vet_report_type,LS_DB_ACCOUNTS_INFO.acc_user_modified = LS_DB_ACCOUNTS_INFO_TMP.acc_user_modified,LS_DB_ACCOUNTS_INFO.acc_user_created = LS_DB_ACCOUNTS_INFO_TMP.acc_user_created,LS_DB_ACCOUNTS_INFO.acc_time_zone = LS_DB_ACCOUNTS_INFO_TMP.acc_time_zone,LS_DB_ACCOUNTS_INFO.acc_task_flag = LS_DB_ACCOUNTS_INFO_TMP.acc_task_flag,LS_DB_ACCOUNTS_INFO.acc_status_date = LS_DB_ACCOUNTS_INFO_TMP.acc_status_date,LS_DB_ACCOUNTS_INFO.acc_state = LS_DB_ACCOUNTS_INFO_TMP.acc_state,LS_DB_ACCOUNTS_INFO.acc_spr_id = LS_DB_ACCOUNTS_INFO_TMP.acc_spr_id,LS_DB_ACCOUNTS_INFO.acc_single_reg_num = LS_DB_ACCOUNTS_INFO_TMP.acc_single_reg_num,LS_DB_ACCOUNTS_INFO.acc_sender_organization_type = LS_DB_ACCOUNTS_INFO_TMP.acc_sender_organization_type,LS_DB_ACCOUNTS_INFO.acc_reporting_medium = LS_DB_ACCOUNTS_INFO_TMP.acc_reporting_medium,LS_DB_ACCOUNTS_INFO.acc_report_type = LS_DB_ACCOUNTS_INFO_TMP.acc_report_type,LS_DB_ACCOUNTS_INFO.acc_region_code = LS_DB_ACCOUNTS_INFO_TMP.acc_region_code,LS_DB_ACCOUNTS_INFO.acc_region = LS_DB_ACCOUNTS_INFO_TMP.acc_region,LS_DB_ACCOUNTS_INFO.acc_record_id = LS_DB_ACCOUNTS_INFO_TMP.acc_record_id,LS_DB_ACCOUNTS_INFO.acc_reconciliation = LS_DB_ACCOUNTS_INFO_TMP.acc_reconciliation,LS_DB_ACCOUNTS_INFO.acc_receiver_id = LS_DB_ACCOUNTS_INFO_TMP.acc_receiver_id,LS_DB_ACCOUNTS_INFO.acc_recalculate_safety_report_id = LS_DB_ACCOUNTS_INFO_TMP.acc_recalculate_safety_report_id,LS_DB_ACCOUNTS_INFO.acc_reason_for_deactive = LS_DB_ACCOUNTS_INFO_TMP.acc_reason_for_deactive,LS_DB_ACCOUNTS_INFO.acc_read_unread_correspondence = LS_DB_ACCOUNTS_INFO_TMP.acc_read_unread_correspondence,LS_DB_ACCOUNTS_INFO.acc_qc_sampling_running_count = LS_DB_ACCOUNTS_INFO_TMP.acc_qc_sampling_running_count,LS_DB_ACCOUNTS_INFO.acc_qc_sampling_count = LS_DB_ACCOUNTS_INFO_TMP.acc_qc_sampling_count,LS_DB_ACCOUNTS_INFO.acc_product_class_type = LS_DB_ACCOUNTS_INFO_TMP.acc_product_class_type,LS_DB_ACCOUNTS_INFO.acc_postal_code = LS_DB_ACCOUNTS_INFO_TMP.acc_postal_code,LS_DB_ACCOUNTS_INFO.acc_po_box = LS_DB_ACCOUNTS_INFO_TMP.acc_po_box,LS_DB_ACCOUNTS_INFO.acc_phone_no = LS_DB_ACCOUNTS_INFO_TMP.acc_phone_no,LS_DB_ACCOUNTS_INFO.acc_phone_country_code = LS_DB_ACCOUNTS_INFO_TMP.acc_phone_country_code,LS_DB_ACCOUNTS_INFO.acc_phone_area_code = LS_DB_ACCOUNTS_INFO_TMP.acc_phone_area_code,LS_DB_ACCOUNTS_INFO.acc_parent_firm_state = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_state,LS_DB_ACCOUNTS_INFO.acc_parent_firm_postalcode = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_postalcode,LS_DB_ACCOUNTS_INFO.acc_parent_firm_name = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_name,LS_DB_ACCOUNTS_INFO.acc_parent_firm_country = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_country,LS_DB_ACCOUNTS_INFO.acc_parent_firm_city = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_city,LS_DB_ACCOUNTS_INFO.acc_parent_firm_address = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_firm_address,LS_DB_ACCOUNTS_INFO.acc_parent_fei_no = LS_DB_ACCOUNTS_INFO_TMP.acc_parent_fei_no,LS_DB_ACCOUNTS_INFO.acc_notes_flag = LS_DB_ACCOUNTS_INFO_TMP.acc_notes_flag,LS_DB_ACCOUNTS_INFO.acc_migration_flag = LS_DB_ACCOUNTS_INFO_TMP.acc_migration_flag,LS_DB_ACCOUNTS_INFO.acc_mask_id = LS_DB_ACCOUNTS_INFO_TMP.acc_mask_id,LS_DB_ACCOUNTS_INFO.acc_mask_exclusion_id = LS_DB_ACCOUNTS_INFO_TMP.acc_mask_exclusion_id,LS_DB_ACCOUNTS_INFO.acc_language = LS_DB_ACCOUNTS_INFO_TMP.acc_language,LS_DB_ACCOUNTS_INFO.acc_keyword = LS_DB_ACCOUNTS_INFO_TMP.acc_keyword,LS_DB_ACCOUNTS_INFO.acc_is_e2b_account = LS_DB_ACCOUNTS_INFO_TMP.acc_is_e2b_account,LS_DB_ACCOUNTS_INFO.acc_is_distribution_contact = LS_DB_ACCOUNTS_INFO_TMP.acc_is_distribution_contact,LS_DB_ACCOUNTS_INFO.acc_ird_lrd = LS_DB_ACCOUNTS_INFO_TMP.acc_ird_lrd,LS_DB_ACCOUNTS_INFO.acc_industry = LS_DB_ACCOUNTS_INFO_TMP.acc_industry,LS_DB_ACCOUNTS_INFO.acc_incoming_source_lang = LS_DB_ACCOUNTS_INFO_TMP.acc_incoming_source_lang,LS_DB_ACCOUNTS_INFO.acc_health_authority = LS_DB_ACCOUNTS_INFO_TMP.acc_health_authority,LS_DB_ACCOUNTS_INFO.acc_hc_assigned_estd_lic_no = LS_DB_ACCOUNTS_INFO_TMP.acc_hc_assigned_estd_lic_no,LS_DB_ACCOUNTS_INFO.acc_hc_assigned_company_id = LS_DB_ACCOUNTS_INFO_TMP.acc_hc_assigned_company_id,LS_DB_ACCOUNTS_INFO.acc_fk_aec_rec_id = LS_DB_ACCOUNTS_INFO_TMP.acc_fk_aec_rec_id,LS_DB_ACCOUNTS_INFO.acc_firm_state = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_state,LS_DB_ACCOUNTS_INFO.acc_firm_postalcode = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_postalcode,LS_DB_ACCOUNTS_INFO.acc_firm_name = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_name,LS_DB_ACCOUNTS_INFO.acc_firm_function = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_function,LS_DB_ACCOUNTS_INFO.acc_firm_country = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_country,LS_DB_ACCOUNTS_INFO.acc_firm_city = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_city,LS_DB_ACCOUNTS_INFO.acc_firm_address = LS_DB_ACCOUNTS_INFO_TMP.acc_firm_address,LS_DB_ACCOUNTS_INFO.acc_fei_no = LS_DB_ACCOUNTS_INFO_TMP.acc_fei_no,LS_DB_ACCOUNTS_INFO.acc_fax_inbound_directory = LS_DB_ACCOUNTS_INFO_TMP.acc_fax_inbound_directory,LS_DB_ACCOUNTS_INFO.acc_fax_domain = LS_DB_ACCOUNTS_INFO_TMP.acc_fax_domain,LS_DB_ACCOUNTS_INFO.acc_fax_country_code = LS_DB_ACCOUNTS_INFO_TMP.acc_fax_country_code,LS_DB_ACCOUNTS_INFO.acc_fax_area_code = LS_DB_ACCOUNTS_INFO_TMP.acc_fax_area_code,LS_DB_ACCOUNTS_INFO.acc_fax = LS_DB_ACCOUNTS_INFO_TMP.acc_fax,LS_DB_ACCOUNTS_INFO.acc_exclusion_partner = LS_DB_ACCOUNTS_INFO_TMP.acc_exclusion_partner,LS_DB_ACCOUNTS_INFO.acc_exclusion_account = LS_DB_ACCOUNTS_INFO_TMP.acc_exclusion_account,LS_DB_ACCOUNTS_INFO.acc_eudamed_number = LS_DB_ACCOUNTS_INFO_TMP.acc_eudamed_number,LS_DB_ACCOUNTS_INFO.acc_email_exclusions = LS_DB_ACCOUNTS_INFO_TMP.acc_email_exclusions,LS_DB_ACCOUNTS_INFO.acc_email = LS_DB_ACCOUNTS_INFO_TMP.acc_email,LS_DB_ACCOUNTS_INFO.acc_e2b_meddra = LS_DB_ACCOUNTS_INFO_TMP.acc_e2b_meddra,LS_DB_ACCOUNTS_INFO.acc_e2b_encoding_format = LS_DB_ACCOUNTS_INFO_TMP.acc_e2b_encoding_format,LS_DB_ACCOUNTS_INFO.acc_dup_acc = LS_DB_ACCOUNTS_INFO_TMP.acc_dup_acc,LS_DB_ACCOUNTS_INFO.acc_duns_no = LS_DB_ACCOUNTS_INFO_TMP.acc_duns_no,LS_DB_ACCOUNTS_INFO.acc_domain = LS_DB_ACCOUNTS_INFO_TMP.acc_domain,LS_DB_ACCOUNTS_INFO.acc_distribution_setting = LS_DB_ACCOUNTS_INFO_TMP.acc_distribution_setting,LS_DB_ACCOUNTS_INFO.acc_description = LS_DB_ACCOUNTS_INFO_TMP.acc_description,LS_DB_ACCOUNTS_INFO.acc_date_modified = LS_DB_ACCOUNTS_INFO_TMP.acc_date_modified,LS_DB_ACCOUNTS_INFO.acc_date_created = LS_DB_ACCOUNTS_INFO_TMP.acc_date_created,LS_DB_ACCOUNTS_INFO.acc_crm_source_flag = LS_DB_ACCOUNTS_INFO_TMP.acc_crm_source_flag,LS_DB_ACCOUNTS_INFO.acc_crm_source = LS_DB_ACCOUNTS_INFO_TMP.acc_crm_source,LS_DB_ACCOUNTS_INFO.acc_crm_record_id = LS_DB_ACCOUNTS_INFO_TMP.acc_crm_record_id,LS_DB_ACCOUNTS_INFO.acc_crm_last_modified_date = LS_DB_ACCOUNTS_INFO_TMP.acc_crm_last_modified_date,LS_DB_ACCOUNTS_INFO.acc_create_date = LS_DB_ACCOUNTS_INFO_TMP.acc_create_date,LS_DB_ACCOUNTS_INFO.acc_cover_letter_tmpl_id = LS_DB_ACCOUNTS_INFO_TMP.acc_cover_letter_tmpl_id,LS_DB_ACCOUNTS_INFO.acc_country = LS_DB_ACCOUNTS_INFO_TMP.acc_country,LS_DB_ACCOUNTS_INFO.acc_correspondence_seq = LS_DB_ACCOUNTS_INFO_TMP.acc_correspondence_seq,LS_DB_ACCOUNTS_INFO.acc_correspondence_flag = LS_DB_ACCOUNTS_INFO_TMP.acc_correspondence_flag,LS_DB_ACCOUNTS_INFO.acc_compression = LS_DB_ACCOUNTS_INFO_TMP.acc_compression,LS_DB_ACCOUNTS_INFO.acc_company_status = LS_DB_ACCOUNTS_INFO_TMP.acc_company_status,LS_DB_ACCOUNTS_INFO.acc_city = LS_DB_ACCOUNTS_INFO_TMP.acc_city,LS_DB_ACCOUNTS_INFO.acc_casuality_results = LS_DB_ACCOUNTS_INFO_TMP.acc_casuality_results,LS_DB_ACCOUNTS_INFO.acc_casuality_methods = LS_DB_ACCOUNTS_INFO_TMP.acc_casuality_methods,LS_DB_ACCOUNTS_INFO.acc_assigned_to = LS_DB_ACCOUNTS_INFO_TMP.acc_assigned_to,LS_DB_ACCOUNTS_INFO.acc_assign_to = LS_DB_ACCOUNTS_INFO_TMP.acc_assign_to,LS_DB_ACCOUNTS_INFO.acc_all_product_distr_assign = LS_DB_ACCOUNTS_INFO_TMP.acc_all_product_distr_assign,LS_DB_ACCOUNTS_INFO.acc_all_product_class_assign = LS_DB_ACCOUNTS_INFO_TMP.acc_all_product_class_assign,LS_DB_ACCOUNTS_INFO.acc_all_product_assign = LS_DB_ACCOUNTS_INFO_TMP.acc_all_product_assign,LS_DB_ACCOUNTS_INFO.acc_all_company_unit = LS_DB_ACCOUNTS_INFO_TMP.acc_all_company_unit,LS_DB_ACCOUNTS_INFO.acc_address = LS_DB_ACCOUNTS_INFO_TMP.acc_address,LS_DB_ACCOUNTS_INFO.acc_ack_comments_length = LS_DB_ACCOUNTS_INFO_TMP.acc_ack_comments_length,LS_DB_ACCOUNTS_INFO.acc_account_type = LS_DB_ACCOUNTS_INFO_TMP.acc_account_type,LS_DB_ACCOUNTS_INFO.acc_account_name = LS_DB_ACCOUNTS_INFO_TMP.acc_account_name,LS_DB_ACCOUNTS_INFO.acc_account_manager = LS_DB_ACCOUNTS_INFO_TMP.acc_account_manager,LS_DB_ACCOUNTS_INFO.acc_account_id = LS_DB_ACCOUNTS_INFO_TMP.acc_account_id,LS_DB_ACCOUNTS_INFO.acc_account_group_rec_id = LS_DB_ACCOUNTS_INFO_TMP.acc_account_group_rec_id,LS_DB_ACCOUNTS_INFO.acc_account_group_name = LS_DB_ACCOUNTS_INFO_TMP.acc_account_group_name,LS_DB_ACCOUNTS_INFO.acc_account_active = LS_DB_ACCOUNTS_INFO_TMP.acc_account_active,LS_DB_ACCOUNTS_INFO.acc_acc_ins_from_schedler = LS_DB_ACCOUNTS_INFO_TMP.acc_acc_ins_from_schedler,LS_DB_ACCOUNTS_INFO.acgrp_user_modified = LS_DB_ACCOUNTS_INFO_TMP.acgrp_user_modified,LS_DB_ACCOUNTS_INFO.acgrp_user_created = LS_DB_ACCOUNTS_INFO_TMP.acgrp_user_created,LS_DB_ACCOUNTS_INFO.acgrp_spr_id = LS_DB_ACCOUNTS_INFO_TMP.acgrp_spr_id,LS_DB_ACCOUNTS_INFO.acgrp_record_id = LS_DB_ACCOUNTS_INFO_TMP.acgrp_record_id,LS_DB_ACCOUNTS_INFO.acgrp_reason_for_deactive = LS_DB_ACCOUNTS_INFO_TMP.acgrp_reason_for_deactive,LS_DB_ACCOUNTS_INFO.acgrp_is_active = LS_DB_ACCOUNTS_INFO_TMP.acgrp_is_active,LS_DB_ACCOUNTS_INFO.acgrp_fk_account_record_id = LS_DB_ACCOUNTS_INFO_TMP.acgrp_fk_account_record_id,LS_DB_ACCOUNTS_INFO.acgrp_description = LS_DB_ACCOUNTS_INFO_TMP.acgrp_description,LS_DB_ACCOUNTS_INFO.acgrp_date_modified = LS_DB_ACCOUNTS_INFO_TMP.acgrp_date_modified,LS_DB_ACCOUNTS_INFO.acgrp_date_created = LS_DB_ACCOUNTS_INFO_TMP.acgrp_date_created,LS_DB_ACCOUNTS_INFO.acgrp_accounts_group_name = LS_DB_ACCOUNTS_INFO_TMP.acgrp_accounts_group_name,LS_DB_ACCOUNTS_INFO.acccont_user_modified = LS_DB_ACCOUNTS_INFO_TMP.acccont_user_modified,LS_DB_ACCOUNTS_INFO.acccont_user_created = LS_DB_ACCOUNTS_INFO_TMP.acccont_user_created,LS_DB_ACCOUNTS_INFO.acccont_spr_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_spr_id,LS_DB_ACCOUNTS_INFO.acccont_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_record_id,LS_DB_ACCOUNTS_INFO.acccont_parent_name = LS_DB_ACCOUNTS_INFO_TMP.acccont_parent_name,LS_DB_ACCOUNTS_INFO.acccont_language = LS_DB_ACCOUNTS_INFO_TMP.acccont_language,LS_DB_ACCOUNTS_INFO.acccont_is_primary_contact = LS_DB_ACCOUNTS_INFO_TMP.acccont_is_primary_contact,LS_DB_ACCOUNTS_INFO.acccont_is_e2b_contact = LS_DB_ACCOUNTS_INFO_TMP.acccont_is_e2b_contact,LS_DB_ACCOUNTS_INFO.acccont_interchange_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_interchange_id,LS_DB_ACCOUNTS_INFO.acccont_fk_partner_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_fk_partner_record_id,LS_DB_ACCOUNTS_INFO.acccont_fk_account_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_fk_account_record_id,LS_DB_ACCOUNTS_INFO.acccont_date_modified = LS_DB_ACCOUNTS_INFO_TMP.acccont_date_modified,LS_DB_ACCOUNTS_INFO.acccont_date_created = LS_DB_ACCOUNTS_INFO_TMP.acccont_date_created,LS_DB_ACCOUNTS_INFO.acccont_contact_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccont_contact_record_id,LS_DB_ACCOUNTS_INFO.acccont_contact_distribute = LS_DB_ACCOUNTS_INFO_TMP.acccont_contact_distribute,LS_DB_ACCOUNTS_INFO.accprd_user_modified = LS_DB_ACCOUNTS_INFO_TMP.accprd_user_modified,LS_DB_ACCOUNTS_INFO.accprd_user_created = LS_DB_ACCOUNTS_INFO_TMP.accprd_user_created,LS_DB_ACCOUNTS_INFO.accprd_tradename_recid = LS_DB_ACCOUNTS_INFO_TMP.accprd_tradename_recid,LS_DB_ACCOUNTS_INFO.accprd_spr_id = LS_DB_ACCOUNTS_INFO_TMP.accprd_spr_id,LS_DB_ACCOUNTS_INFO.accprd_report_type = LS_DB_ACCOUNTS_INFO_TMP.accprd_report_type,LS_DB_ACCOUNTS_INFO.accprd_record_id = LS_DB_ACCOUNTS_INFO_TMP.accprd_record_id,LS_DB_ACCOUNTS_INFO.accprd_product_type = LS_DB_ACCOUNTS_INFO_TMP.accprd_product_type,LS_DB_ACCOUNTS_INFO.accprd_product_status = LS_DB_ACCOUNTS_INFO_TMP.accprd_product_status,LS_DB_ACCOUNTS_INFO.accprd_product_class_code = LS_DB_ACCOUNTS_INFO_TMP.accprd_product_class_code,LS_DB_ACCOUNTS_INFO.accprd_local_tradename = LS_DB_ACCOUNTS_INFO_TMP.accprd_local_tradename,LS_DB_ACCOUNTS_INFO.accprd_fk_product_record_id = LS_DB_ACCOUNTS_INFO_TMP.accprd_fk_product_record_id,LS_DB_ACCOUNTS_INFO.accprd_fk_account_product_id = LS_DB_ACCOUNTS_INFO_TMP.accprd_fk_account_product_id,LS_DB_ACCOUNTS_INFO.accprd_date_modified = LS_DB_ACCOUNTS_INFO_TMP.accprd_date_modified,LS_DB_ACCOUNTS_INFO.accprd_date_created = LS_DB_ACCOUNTS_INFO_TMP.accprd_date_created,LS_DB_ACCOUNTS_INFO.accprd_country = LS_DB_ACCOUNTS_INFO_TMP.accprd_country,LS_DB_ACCOUNTS_INFO.accprd_approval_no = LS_DB_ACCOUNTS_INFO_TMP.accprd_approval_no,LS_DB_ACCOUNTS_INFO.accprddist_user_modified = LS_DB_ACCOUNTS_INFO_TMP.accprddist_user_modified,LS_DB_ACCOUNTS_INFO.accprddist_user_created = LS_DB_ACCOUNTS_INFO_TMP.accprddist_user_created,LS_DB_ACCOUNTS_INFO.accprddist_tradename_recid = LS_DB_ACCOUNTS_INFO_TMP.accprddist_tradename_recid,LS_DB_ACCOUNTS_INFO.accprddist_spr_id = LS_DB_ACCOUNTS_INFO_TMP.accprddist_spr_id,LS_DB_ACCOUNTS_INFO.accprddist_reporting_medium = LS_DB_ACCOUNTS_INFO_TMP.accprddist_reporting_medium,LS_DB_ACCOUNTS_INFO.accprddist_report_type = LS_DB_ACCOUNTS_INFO_TMP.accprddist_report_type,LS_DB_ACCOUNTS_INFO.accprddist_record_id = LS_DB_ACCOUNTS_INFO_TMP.accprddist_record_id,LS_DB_ACCOUNTS_INFO.accprddist_product_type = LS_DB_ACCOUNTS_INFO_TMP.accprddist_product_type,LS_DB_ACCOUNTS_INFO.accprddist_product_status = LS_DB_ACCOUNTS_INFO_TMP.accprddist_product_status,LS_DB_ACCOUNTS_INFO.accprddist_product_class_code = LS_DB_ACCOUNTS_INFO_TMP.accprddist_product_class_code,LS_DB_ACCOUNTS_INFO.accprddist_local_tradename = LS_DB_ACCOUNTS_INFO_TMP.accprddist_local_tradename,LS_DB_ACCOUNTS_INFO.accprddist_fk_product_record_id = LS_DB_ACCOUNTS_INFO_TMP.accprddist_fk_product_record_id,LS_DB_ACCOUNTS_INFO.accprddist_fk_account_id = LS_DB_ACCOUNTS_INFO_TMP.accprddist_fk_account_id,LS_DB_ACCOUNTS_INFO.accprddist_date_modified = LS_DB_ACCOUNTS_INFO_TMP.accprddist_date_modified,LS_DB_ACCOUNTS_INFO.accprddist_date_created = LS_DB_ACCOUNTS_INFO_TMP.accprddist_date_created,LS_DB_ACCOUNTS_INFO.accprddist_country = LS_DB_ACCOUNTS_INFO_TMP.accprddist_country,LS_DB_ACCOUNTS_INFO.accprddist_approval_no = LS_DB_ACCOUNTS_INFO_TMP.accprddist_approval_no,LS_DB_ACCOUNTS_INFO.acccmpunit_user_modified = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_user_modified,LS_DB_ACCOUNTS_INFO.acccmpunit_user_created = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_user_created,LS_DB_ACCOUNTS_INFO.acccmpunit_spr_id = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_spr_id,LS_DB_ACCOUNTS_INFO.acccmpunit_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_record_id,LS_DB_ACCOUNTS_INFO.acccmpunit_fk_account_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_fk_account_record_id,LS_DB_ACCOUNTS_INFO.acccmpunit_date_modified = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_date_modified,LS_DB_ACCOUNTS_INFO.acccmpunit_date_created = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_date_created,LS_DB_ACCOUNTS_INFO.acccmpunit_company_unit_record_id = LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_company_unit_record_id,LS_DB_ACCOUNTS_INFO.accgrp_user_modified = LS_DB_ACCOUNTS_INFO_TMP.accgrp_user_modified,LS_DB_ACCOUNTS_INFO.accgrp_user_created = LS_DB_ACCOUNTS_INFO_TMP.accgrp_user_created,LS_DB_ACCOUNTS_INFO.accgrp_spr_id = LS_DB_ACCOUNTS_INFO_TMP.accgrp_spr_id,LS_DB_ACCOUNTS_INFO.accgrp_record_id = LS_DB_ACCOUNTS_INFO_TMP.accgrp_record_id,LS_DB_ACCOUNTS_INFO.accgrp_fk_accountgroup_rec_id = LS_DB_ACCOUNTS_INFO_TMP.accgrp_fk_accountgroup_rec_id,LS_DB_ACCOUNTS_INFO.accgrp_fk_account_rec_id = LS_DB_ACCOUNTS_INFO_TMP.accgrp_fk_account_rec_id,LS_DB_ACCOUNTS_INFO.accgrp_date_modified = LS_DB_ACCOUNTS_INFO_TMP.accgrp_date_modified,LS_DB_ACCOUNTS_INFO.accgrp_date_created = LS_DB_ACCOUNTS_INFO_TMP.accgrp_date_created,
LS_DB_ACCOUNTS_INFO.PROCESSING_DT = LS_DB_ACCOUNTS_INFO_TMP.PROCESSING_DT,
LS_DB_ACCOUNTS_INFO.user_modified  =LS_DB_ACCOUNTS_INFO_TMP.user_modified     ,
LS_DB_ACCOUNTS_INFO.date_modified  =LS_DB_ACCOUNTS_INFO_TMP.date_modified     ,
LS_DB_ACCOUNTS_INFO.expiry_date    =LS_DB_ACCOUNTS_INFO_TMP.expiry_date       ,
LS_DB_ACCOUNTS_INFO.created_by     =LS_DB_ACCOUNTS_INFO_TMP.created_by        ,
LS_DB_ACCOUNTS_INFO.created_dt     =LS_DB_ACCOUNTS_INFO_TMP.created_dt        ,
LS_DB_ACCOUNTS_INFO.load_ts        =LS_DB_ACCOUNTS_INFO_TMP.load_ts          
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP 
WHERE 	LS_DB_ACCOUNTS_INFO.INTEGRATION_ID = LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_ACCOUNTS_INFO_TMP.PROCESSING_DT = LS_DB_ACCOUNTS_INFO.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_ACCOUNTS_INFO.acccmpunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accprddist_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acccont_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accprd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accprddist_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acccont_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accprd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_ACCOUNTS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_ACCOUNTS_INFO.acc_RECORD_ID ,LS_DB_ACCOUNTS_INFO.INTEGRATION_ID
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP 
ON LS_DB_ACCOUNTS_INFO.acc_RECORD_ID=LS_DB_ACCOUNTS_INFO_TMP.acc_RECORD_ID
AND LS_DB_ACCOUNTS_INFO.INTEGRATION_ID = LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID 
where LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID  is null AND LS_DB_ACCOUNTS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_ACCOUNTS_INFO.acc_RECORD_ID in (select acc_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP )
) TMP where LS_DB_ACCOUNTS_INFO.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_ACCOUNTS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_ACCOUNTS_INFO.acc_RECORD_ID ,LS_DB_ACCOUNTS_INFO.INTEGRATION_ID
    FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO left join $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP 
    ON LS_DB_ACCOUNTS_INFO.acc_RECORD_ID=LS_DB_ACCOUNTS_INFO_TMP.acc_RECORD_ID
    AND LS_DB_ACCOUNTS_INFO.INTEGRATION_ID = LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID 
    where LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID  is null AND LS_DB_ACCOUNTS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_ACCOUNTS_INFO.acc_RECORD_ID in (select acc_RECORD_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     



INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO
( 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,acc_website,
acc_vet_report_type,
acc_user_modified,
acc_user_created,
acc_time_zone,
acc_task_flag,
acc_status_date,
acc_state,
acc_spr_id,
acc_single_reg_num,
acc_sender_organization_type,
acc_reporting_medium,
acc_report_type,
acc_region_code,
acc_region,
acc_record_id,
acc_reconciliation,
acc_receiver_id,
acc_recalculate_safety_report_id,
acc_reason_for_deactive,
acc_read_unread_correspondence,
acc_qc_sampling_running_count,
acc_qc_sampling_count,
acc_product_class_type,
acc_postal_code,
acc_po_box,
acc_phone_no,
acc_phone_country_code,
acc_phone_area_code,
acc_parent_firm_state,
acc_parent_firm_postalcode,
acc_parent_firm_name,
acc_parent_firm_country,
acc_parent_firm_city,
acc_parent_firm_address,
acc_parent_fei_no,
acc_notes_flag,
acc_migration_flag,
acc_mask_id,
acc_mask_exclusion_id,
acc_language,
acc_keyword,
acc_is_e2b_account,
acc_is_distribution_contact,
acc_ird_lrd,
acc_industry,
acc_incoming_source_lang,
acc_health_authority,
acc_hc_assigned_estd_lic_no,
acc_hc_assigned_company_id,
acc_fk_aec_rec_id,
acc_firm_state,
acc_firm_postalcode,
acc_firm_name,
acc_firm_function,
acc_firm_country,
acc_firm_city,
acc_firm_address,
acc_fei_no,
acc_fax_inbound_directory,
acc_fax_domain,
acc_fax_country_code,
acc_fax_area_code,
acc_fax,
acc_exclusion_partner,
acc_exclusion_account,
acc_eudamed_number,
acc_email_exclusions,
acc_email,
acc_e2b_meddra,
acc_e2b_encoding_format,
acc_dup_acc,
acc_duns_no,
acc_domain,
acc_distribution_setting,
acc_description,
acc_date_modified,
acc_date_created,
acc_crm_source_flag,
acc_crm_source,
acc_crm_record_id,
acc_crm_last_modified_date,
acc_create_date,
acc_cover_letter_tmpl_id,
acc_country,
acc_correspondence_seq,
acc_correspondence_flag,
acc_compression,
acc_company_status,
acc_city,
acc_casuality_results,
acc_casuality_methods,
acc_assigned_to,
acc_assign_to,
acc_all_product_distr_assign,
acc_all_product_class_assign,
acc_all_product_assign,
acc_all_company_unit,
acc_address,
acc_ack_comments_length,
acc_account_type,
acc_account_name,
acc_account_manager,
acc_account_id,
acc_account_group_rec_id,
acc_account_group_name,
acc_account_active,
acc_acc_ins_from_schedler,
acgrp_user_modified,
acgrp_user_created,
acgrp_spr_id,
acgrp_record_id,
acgrp_reason_for_deactive,
acgrp_is_active,
acgrp_fk_account_record_id,
acgrp_description,
acgrp_date_modified,
acgrp_date_created,
acgrp_accounts_group_name,
acccont_user_modified,
acccont_user_created,
acccont_spr_id,
acccont_record_id,
acccont_parent_name,
acccont_language,
acccont_is_primary_contact,
acccont_is_e2b_contact,
acccont_interchange_id,
acccont_fk_partner_record_id,
acccont_fk_account_record_id,
acccont_date_modified,
acccont_date_created,
acccont_contact_record_id,
acccont_contact_distribute,
accprd_user_modified,
accprd_user_created,
accprd_tradename_recid,
accprd_spr_id,
accprd_report_type,
accprd_record_id,
accprd_product_type,
accprd_product_status,
accprd_product_class_code,
accprd_local_tradename,
accprd_fk_product_record_id,
accprd_fk_account_product_id,
accprd_date_modified,
accprd_date_created,
accprd_country,
accprd_approval_no,
accprddist_user_modified,
accprddist_user_created,
accprddist_tradename_recid,
accprddist_spr_id,
accprddist_reporting_medium,
accprddist_report_type,
accprddist_record_id,
accprddist_product_type,
accprddist_product_status,
accprddist_product_class_code,
accprddist_local_tradename,
accprddist_fk_product_record_id,
accprddist_fk_account_id,
accprddist_date_modified,
accprddist_date_created,
accprddist_country,
accprddist_approval_no,
acccmpunit_user_modified,
acccmpunit_user_created,
acccmpunit_spr_id,
acccmpunit_record_id,
acccmpunit_fk_account_record_id,
acccmpunit_date_modified,
acccmpunit_date_created,
acccmpunit_company_unit_record_id,
accgrp_user_modified,
accgrp_user_created,
accgrp_spr_id,
accgrp_record_id,
accgrp_fk_accountgroup_rec_id,
accgrp_fk_account_rec_id,
accgrp_date_modified,
accgrp_date_created)
SELECT 
 
processing_dt ,
user_modified ,
date_modified ,
expiry_date   ,
created_by    ,
created_dt    ,
load_ts       ,
integration_id ,acc_website,
acc_vet_report_type,
acc_user_modified,
acc_user_created,
acc_time_zone,
acc_task_flag,
acc_status_date,
acc_state,
acc_spr_id,
acc_single_reg_num,
acc_sender_organization_type,
acc_reporting_medium,
acc_report_type,
acc_region_code,
acc_region,
acc_record_id,
acc_reconciliation,
acc_receiver_id,
acc_recalculate_safety_report_id,
acc_reason_for_deactive,
acc_read_unread_correspondence,
acc_qc_sampling_running_count,
acc_qc_sampling_count,
acc_product_class_type,
acc_postal_code,
acc_po_box,
acc_phone_no,
acc_phone_country_code,
acc_phone_area_code,
acc_parent_firm_state,
acc_parent_firm_postalcode,
acc_parent_firm_name,
acc_parent_firm_country,
acc_parent_firm_city,
acc_parent_firm_address,
acc_parent_fei_no,
acc_notes_flag,
acc_migration_flag,
acc_mask_id,
acc_mask_exclusion_id,
acc_language,
acc_keyword,
acc_is_e2b_account,
acc_is_distribution_contact,
acc_ird_lrd,
acc_industry,
acc_incoming_source_lang,
acc_health_authority,
acc_hc_assigned_estd_lic_no,
acc_hc_assigned_company_id,
acc_fk_aec_rec_id,
acc_firm_state,
acc_firm_postalcode,
acc_firm_name,
acc_firm_function,
acc_firm_country,
acc_firm_city,
acc_firm_address,
acc_fei_no,
acc_fax_inbound_directory,
acc_fax_domain,
acc_fax_country_code,
acc_fax_area_code,
acc_fax,
acc_exclusion_partner,
acc_exclusion_account,
acc_eudamed_number,
acc_email_exclusions,
acc_email,
acc_e2b_meddra,
acc_e2b_encoding_format,
acc_dup_acc,
acc_duns_no,
acc_domain,
acc_distribution_setting,
acc_description,
acc_date_modified,
acc_date_created,
acc_crm_source_flag,
acc_crm_source,
acc_crm_record_id,
acc_crm_last_modified_date,
acc_create_date,
acc_cover_letter_tmpl_id,
acc_country,
acc_correspondence_seq,
acc_correspondence_flag,
acc_compression,
acc_company_status,
acc_city,
acc_casuality_results,
acc_casuality_methods,
acc_assigned_to,
acc_assign_to,
acc_all_product_distr_assign,
acc_all_product_class_assign,
acc_all_product_assign,
acc_all_company_unit,
acc_address,
acc_ack_comments_length,
acc_account_type,
acc_account_name,
acc_account_manager,
acc_account_id,
acc_account_group_rec_id,
acc_account_group_name,
acc_account_active,
acc_acc_ins_from_schedler,
acgrp_user_modified,
acgrp_user_created,
acgrp_spr_id,
acgrp_record_id,
acgrp_reason_for_deactive,
acgrp_is_active,
acgrp_fk_account_record_id,
acgrp_description,
acgrp_date_modified,
acgrp_date_created,
acgrp_accounts_group_name,
acccont_user_modified,
acccont_user_created,
acccont_spr_id,
acccont_record_id,
acccont_parent_name,
acccont_language,
acccont_is_primary_contact,
acccont_is_e2b_contact,
acccont_interchange_id,
acccont_fk_partner_record_id,
acccont_fk_account_record_id,
acccont_date_modified,
acccont_date_created,
acccont_contact_record_id,
acccont_contact_distribute,
accprd_user_modified,
accprd_user_created,
accprd_tradename_recid,
accprd_spr_id,
accprd_report_type,
accprd_record_id,
accprd_product_type,
accprd_product_status,
accprd_product_class_code,
accprd_local_tradename,
accprd_fk_product_record_id,
accprd_fk_account_product_id,
accprd_date_modified,
accprd_date_created,
accprd_country,
accprd_approval_no,
accprddist_user_modified,
accprddist_user_created,
accprddist_tradename_recid,
accprddist_spr_id,
accprddist_reporting_medium,
accprddist_report_type,
accprddist_record_id,
accprddist_product_type,
accprddist_product_status,
accprddist_product_class_code,
accprddist_local_tradename,
accprddist_fk_product_record_id,
accprddist_fk_account_id,
accprddist_date_modified,
accprddist_date_created,
accprddist_country,
accprddist_approval_no,
acccmpunit_user_modified,
acccmpunit_user_created,
acccmpunit_spr_id,
acccmpunit_record_id,
acccmpunit_fk_account_record_id,
acccmpunit_date_modified,
acccmpunit_date_created,
acccmpunit_company_unit_record_id,
accgrp_user_modified,
accgrp_user_created,
accgrp_spr_id,
accgrp_record_id,
accgrp_fk_accountgroup_rec_id,
accgrp_fk_account_rec_id,
accgrp_date_modified,
accgrp_date_created
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP TMP where not exists (select 1 from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP 
WHERE 	TO_DATE(LS_DB_ACCOUNTS_INFO.PROCESSING_DT) < TO_DATE(LS_DB_ACCOUNTS_INFO_TMP.PROCESSING_DT)
AND LS_DB_ACCOUNTS_INFO.INTEGRATION_ID = LS_DB_ACCOUNTS_INFO_TMP.INTEGRATION_ID
AND LS_DB_ACCOUNTS_INFO.acc_RECORD_ID = LS_DB_ACCOUNTS_INFO_TMP.acc_RECORD_ID
AND LS_DB_ACCOUNTS_INFO.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND MD5(NVL(LS_DB_ACCOUNTS_INFO.acccmpunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accprddist_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acccont_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.acc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ||'-'||NVL(LS_DB_ACCOUNTS_INFO.accprd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')) ) <> MD5(NVL(LS_DB_ACCOUNTS_INFO_TMP.acccmpunit_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accprddist_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acccont_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accgrp_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.acc_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))||'-'||NVL(LS_DB_ACCOUNTS_INFO_TMP.accprd_DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;

DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO TGT
WHERE  ( accgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_accountgroup') OR acccmpunit_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_company_unit') OR accprddist_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_prod_distribution') OR accprd_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_product') OR acc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts') OR acccont_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_contacts') OR acgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_group')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( accgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_accountgroup') OR acccmpunit_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_company_unit') OR accprddist_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_prod_distribution') OR accprd_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_account_product') OR acc_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts') OR acccont_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_contacts') OR acgrp_record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_ACCOUNTS_INFO_DELETION_TMP  WHERE TABLE_NAME='lsmv_accounts_group')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ACCOUNTS_INFO_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_ACCOUNTS_INFO'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_ACCOUNTS_INFO'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_ACCOUNTS_INFO Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_ACCOUNTS_INFO'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_ACCOUNTS_INFO not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
