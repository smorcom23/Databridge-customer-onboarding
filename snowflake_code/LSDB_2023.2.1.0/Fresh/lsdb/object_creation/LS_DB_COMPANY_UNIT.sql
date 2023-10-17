
-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_COMPANY_UNIT()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_COMPANY_UNIT'),
	'LSDB','Case','LS_DB_COMPANY_UNIT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_COMPANY_UNIT' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_COMPANY_UNIT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_COMPANY_UNIT_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_COMPANY_UNIT_DELETION_TMP  As select RECORD_ID,'lsmv_company_unit' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_company_unit WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN (NULL)
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
WHERE MEDDRA_VERSION in (select meddra_version from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_VERSION where EXPIRY_DATE='9999-12-31')) ,
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT record_id record_id, 0 common_parent_key   FROM ${stage_db_name}.${stage_schema_name}.lsmv_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_COMPANY_UNIT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key  FROM ${stage_db_name}.${stage_schema_name}.lsmv_company_unit WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_COMPANY_UNIT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
)
 , lsmv_company_unit_SUBSET AS 
(
select * from 
    (SELECT  
    ack_encoding  ack_encoding,addr  addr,addr_1  addr_1,allow_batch_export  allow_batch_export,authority  authority,canadian_licence_no  canadian_licence_no,company_doctor  company_doctor,company_unit_flag  company_unit_flag,contact_email_id  contact_email_id,contact_fax_no  contact_fax_no,contact_phone_no  contact_phone_no,country  country,country_code  country_code,database_id  database_id,date_created  date_created,date_modified  date_modified,dept  dept,dgfps_no  dgfps_no,doctor_role  doctor_role,doctype_ack  doctype_ack,doctype_icsr  doctype_icsr,e2b_aer_owner_unit  e2b_aer_owner_unit,e2b_approved_status  e2b_approved_status,e2b_authority  e2b_authority,e2b_file_format  e2b_file_format,e2b_meddra_coding  e2b_meddra_coding,e2b_sender_receiver  e2b_sender_receiver,e2b_test_name_coding  e2b_test_name_coding,e2b_user_id_archive  e2b_user_id_archive,e2b_validation  e2b_validation,external_app_updated_date  external_app_updated_date,fda_registration_number  fda_registration_number,icsr_encoding  icsr_encoding,interchange_id  interchange_id,is_arisg_unit  is_arisg_unit,is_international_version  is_international_version,location  location,manual_update  manual_update,manual_update_of_ip  manual_update_of_ip,message_transmission_date  message_transmission_date,postal_code  postal_code,record_id  record_id,record_type  record_type,retired  retired,spr_id  spr_id,state  state,timezone  timezone,type_company_unit  type_company_unit,unit_id  unit_id,unit_name  unit_name,unit_type_flag  unit_type_flag,upd_auth_respond_date  upd_auth_respond_date,update_date_informed  update_date_informed,user_created  user_created,user_modified  user_modified,version  version,working_hour_from  working_hour_from,working_hour_to  working_hour_to,row_number() OVER ( PARTITION BY record_id,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_company_unit
 WHERE  record_id IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_company_unit')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_company_unit_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE,lsmv_company_unit_SUBSET.USER_CREATED CREATED_BY,lsmv_company_unit_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_company_unit_SUBSET.working_hour_to  ,lsmv_company_unit_SUBSET.working_hour_from  ,lsmv_company_unit_SUBSET.version  ,lsmv_company_unit_SUBSET.user_modified  ,lsmv_company_unit_SUBSET.user_created  ,lsmv_company_unit_SUBSET.update_date_informed  ,lsmv_company_unit_SUBSET.upd_auth_respond_date  ,lsmv_company_unit_SUBSET.unit_type_flag  ,lsmv_company_unit_SUBSET.unit_name  ,lsmv_company_unit_SUBSET.unit_id  ,lsmv_company_unit_SUBSET.type_company_unit  ,lsmv_company_unit_SUBSET.timezone  ,lsmv_company_unit_SUBSET.state  ,lsmv_company_unit_SUBSET.spr_id  ,lsmv_company_unit_SUBSET.retired  ,lsmv_company_unit_SUBSET.record_type  ,lsmv_company_unit_SUBSET.record_id  ,lsmv_company_unit_SUBSET.postal_code  ,lsmv_company_unit_SUBSET.message_transmission_date  ,lsmv_company_unit_SUBSET.manual_update_of_ip  ,lsmv_company_unit_SUBSET.manual_update  ,lsmv_company_unit_SUBSET.location  ,lsmv_company_unit_SUBSET.is_international_version  ,lsmv_company_unit_SUBSET.is_arisg_unit  ,lsmv_company_unit_SUBSET.interchange_id  ,lsmv_company_unit_SUBSET.icsr_encoding  ,lsmv_company_unit_SUBSET.fda_registration_number  ,lsmv_company_unit_SUBSET.external_app_updated_date  ,lsmv_company_unit_SUBSET.e2b_validation  ,lsmv_company_unit_SUBSET.e2b_user_id_archive  ,lsmv_company_unit_SUBSET.e2b_test_name_coding  ,lsmv_company_unit_SUBSET.e2b_sender_receiver  ,lsmv_company_unit_SUBSET.e2b_meddra_coding  ,lsmv_company_unit_SUBSET.e2b_file_format  ,lsmv_company_unit_SUBSET.e2b_authority  ,lsmv_company_unit_SUBSET.e2b_approved_status  ,lsmv_company_unit_SUBSET.e2b_aer_owner_unit  ,lsmv_company_unit_SUBSET.doctype_icsr  ,lsmv_company_unit_SUBSET.doctype_ack  ,lsmv_company_unit_SUBSET.doctor_role  ,lsmv_company_unit_SUBSET.dgfps_no  ,lsmv_company_unit_SUBSET.dept  ,lsmv_company_unit_SUBSET.date_modified  ,lsmv_company_unit_SUBSET.date_created  ,lsmv_company_unit_SUBSET.database_id  ,lsmv_company_unit_SUBSET.country_code  ,lsmv_company_unit_SUBSET.country  ,lsmv_company_unit_SUBSET.contact_phone_no  ,lsmv_company_unit_SUBSET.contact_fax_no  ,lsmv_company_unit_SUBSET.contact_email_id  ,lsmv_company_unit_SUBSET.company_unit_flag  ,lsmv_company_unit_SUBSET.company_doctor  ,lsmv_company_unit_SUBSET.canadian_licence_no  ,lsmv_company_unit_SUBSET.authority  ,lsmv_company_unit_SUBSET.allow_batch_export  ,lsmv_company_unit_SUBSET.addr_1  ,lsmv_company_unit_SUBSET.addr  ,lsmv_company_unit_SUBSET.ack_encoding ,CONCAT( NVL(lsmv_company_unit_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_company_unit_SUBSET  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP)
where target_table_name='LS_DB_COMPANY_UNIT'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT   
SET LS_DB_COMPANY_UNIT.working_hour_to = LS_DB_COMPANY_UNIT_TMP.working_hour_to,LS_DB_COMPANY_UNIT.working_hour_from = LS_DB_COMPANY_UNIT_TMP.working_hour_from,LS_DB_COMPANY_UNIT.version = LS_DB_COMPANY_UNIT_TMP.version,LS_DB_COMPANY_UNIT.user_modified = LS_DB_COMPANY_UNIT_TMP.user_modified,LS_DB_COMPANY_UNIT.user_created = LS_DB_COMPANY_UNIT_TMP.user_created,LS_DB_COMPANY_UNIT.update_date_informed = LS_DB_COMPANY_UNIT_TMP.update_date_informed,LS_DB_COMPANY_UNIT.upd_auth_respond_date = LS_DB_COMPANY_UNIT_TMP.upd_auth_respond_date,LS_DB_COMPANY_UNIT.unit_type_flag = LS_DB_COMPANY_UNIT_TMP.unit_type_flag,LS_DB_COMPANY_UNIT.unit_name = LS_DB_COMPANY_UNIT_TMP.unit_name,LS_DB_COMPANY_UNIT.unit_id = LS_DB_COMPANY_UNIT_TMP.unit_id,LS_DB_COMPANY_UNIT.type_company_unit = LS_DB_COMPANY_UNIT_TMP.type_company_unit,LS_DB_COMPANY_UNIT.timezone = LS_DB_COMPANY_UNIT_TMP.timezone,LS_DB_COMPANY_UNIT.state = LS_DB_COMPANY_UNIT_TMP.state,LS_DB_COMPANY_UNIT.spr_id = LS_DB_COMPANY_UNIT_TMP.spr_id,LS_DB_COMPANY_UNIT.retired = LS_DB_COMPANY_UNIT_TMP.retired,LS_DB_COMPANY_UNIT.record_type = LS_DB_COMPANY_UNIT_TMP.record_type,LS_DB_COMPANY_UNIT.record_id = LS_DB_COMPANY_UNIT_TMP.record_id,LS_DB_COMPANY_UNIT.postal_code = LS_DB_COMPANY_UNIT_TMP.postal_code,LS_DB_COMPANY_UNIT.message_transmission_date = LS_DB_COMPANY_UNIT_TMP.message_transmission_date,LS_DB_COMPANY_UNIT.manual_update_of_ip = LS_DB_COMPANY_UNIT_TMP.manual_update_of_ip,LS_DB_COMPANY_UNIT.manual_update = LS_DB_COMPANY_UNIT_TMP.manual_update,LS_DB_COMPANY_UNIT.location = LS_DB_COMPANY_UNIT_TMP.location,LS_DB_COMPANY_UNIT.is_international_version = LS_DB_COMPANY_UNIT_TMP.is_international_version,LS_DB_COMPANY_UNIT.is_arisg_unit = LS_DB_COMPANY_UNIT_TMP.is_arisg_unit,LS_DB_COMPANY_UNIT.interchange_id = LS_DB_COMPANY_UNIT_TMP.interchange_id,LS_DB_COMPANY_UNIT.icsr_encoding = LS_DB_COMPANY_UNIT_TMP.icsr_encoding,LS_DB_COMPANY_UNIT.fda_registration_number = LS_DB_COMPANY_UNIT_TMP.fda_registration_number,LS_DB_COMPANY_UNIT.external_app_updated_date = LS_DB_COMPANY_UNIT_TMP.external_app_updated_date,LS_DB_COMPANY_UNIT.e2b_validation = LS_DB_COMPANY_UNIT_TMP.e2b_validation,LS_DB_COMPANY_UNIT.e2b_user_id_archive = LS_DB_COMPANY_UNIT_TMP.e2b_user_id_archive,LS_DB_COMPANY_UNIT.e2b_test_name_coding = LS_DB_COMPANY_UNIT_TMP.e2b_test_name_coding,LS_DB_COMPANY_UNIT.e2b_sender_receiver = LS_DB_COMPANY_UNIT_TMP.e2b_sender_receiver,LS_DB_COMPANY_UNIT.e2b_meddra_coding = LS_DB_COMPANY_UNIT_TMP.e2b_meddra_coding,LS_DB_COMPANY_UNIT.e2b_file_format = LS_DB_COMPANY_UNIT_TMP.e2b_file_format,LS_DB_COMPANY_UNIT.e2b_authority = LS_DB_COMPANY_UNIT_TMP.e2b_authority,LS_DB_COMPANY_UNIT.e2b_approved_status = LS_DB_COMPANY_UNIT_TMP.e2b_approved_status,LS_DB_COMPANY_UNIT.e2b_aer_owner_unit = LS_DB_COMPANY_UNIT_TMP.e2b_aer_owner_unit,LS_DB_COMPANY_UNIT.doctype_icsr = LS_DB_COMPANY_UNIT_TMP.doctype_icsr,LS_DB_COMPANY_UNIT.doctype_ack = LS_DB_COMPANY_UNIT_TMP.doctype_ack,LS_DB_COMPANY_UNIT.doctor_role = LS_DB_COMPANY_UNIT_TMP.doctor_role,LS_DB_COMPANY_UNIT.dgfps_no = LS_DB_COMPANY_UNIT_TMP.dgfps_no,LS_DB_COMPANY_UNIT.dept = LS_DB_COMPANY_UNIT_TMP.dept,LS_DB_COMPANY_UNIT.date_modified = LS_DB_COMPANY_UNIT_TMP.date_modified,LS_DB_COMPANY_UNIT.date_created = LS_DB_COMPANY_UNIT_TMP.date_created,LS_DB_COMPANY_UNIT.database_id = LS_DB_COMPANY_UNIT_TMP.database_id,LS_DB_COMPANY_UNIT.country_code = LS_DB_COMPANY_UNIT_TMP.country_code,LS_DB_COMPANY_UNIT.country = LS_DB_COMPANY_UNIT_TMP.country,LS_DB_COMPANY_UNIT.contact_phone_no = LS_DB_COMPANY_UNIT_TMP.contact_phone_no,LS_DB_COMPANY_UNIT.contact_fax_no = LS_DB_COMPANY_UNIT_TMP.contact_fax_no,LS_DB_COMPANY_UNIT.contact_email_id = LS_DB_COMPANY_UNIT_TMP.contact_email_id,LS_DB_COMPANY_UNIT.company_unit_flag = LS_DB_COMPANY_UNIT_TMP.company_unit_flag,LS_DB_COMPANY_UNIT.company_doctor = LS_DB_COMPANY_UNIT_TMP.company_doctor,LS_DB_COMPANY_UNIT.canadian_licence_no = LS_DB_COMPANY_UNIT_TMP.canadian_licence_no,LS_DB_COMPANY_UNIT.authority = LS_DB_COMPANY_UNIT_TMP.authority,LS_DB_COMPANY_UNIT.allow_batch_export = LS_DB_COMPANY_UNIT_TMP.allow_batch_export,LS_DB_COMPANY_UNIT.addr_1 = LS_DB_COMPANY_UNIT_TMP.addr_1,LS_DB_COMPANY_UNIT.addr = LS_DB_COMPANY_UNIT_TMP.addr,LS_DB_COMPANY_UNIT.ack_encoding = LS_DB_COMPANY_UNIT_TMP.ack_encoding,
LS_DB_COMPANY_UNIT.PROCESSING_DT = LS_DB_COMPANY_UNIT_TMP.PROCESSING_DT ,
LS_DB_COMPANY_UNIT.expiry_date    =LS_DB_COMPANY_UNIT_TMP.expiry_date       ,
LS_DB_COMPANY_UNIT.created_by     =LS_DB_COMPANY_UNIT_TMP.created_by        ,
LS_DB_COMPANY_UNIT.created_dt     =LS_DB_COMPANY_UNIT_TMP.created_dt        ,
LS_DB_COMPANY_UNIT.load_ts        =LS_DB_COMPANY_UNIT_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP 
WHERE 	LS_DB_COMPANY_UNIT.INTEGRATION_ID = LS_DB_COMPANY_UNIT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_COMPANY_UNIT_TMP.PROCESSING_DT = LS_DB_COMPANY_UNIT.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT
(
processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,working_hour_to,
working_hour_from,
version,
user_modified,
user_created,
update_date_informed,
upd_auth_respond_date,
unit_type_flag,
unit_name,
unit_id,
type_company_unit,
timezone,
state,
spr_id,
retired,
record_type,
record_id,
postal_code,
message_transmission_date,
manual_update_of_ip,
manual_update,
location,
is_international_version,
is_arisg_unit,
interchange_id,
icsr_encoding,
fda_registration_number,
external_app_updated_date,
e2b_validation,
e2b_user_id_archive,
e2b_test_name_coding,
e2b_sender_receiver,
e2b_meddra_coding,
e2b_file_format,
e2b_authority,
e2b_approved_status,
e2b_aer_owner_unit,
doctype_icsr,
doctype_ack,
doctor_role,
dgfps_no,
dept,
date_modified,
date_created,
database_id,
country_code,
country,
contact_phone_no,
contact_fax_no,
contact_email_id,
company_unit_flag,
company_doctor,
canadian_licence_no,
authority,
allow_batch_export,
addr_1,
addr,
ack_encoding)
SELECT 

processing_dt ,
expiry_date   ,
load_ts       ,
integration_id ,working_hour_to,
working_hour_from,
version,
user_modified,
user_created,
update_date_informed,
upd_auth_respond_date,
unit_type_flag,
unit_name,
unit_id,
type_company_unit,
timezone,
state,
spr_id,
retired,
record_type,
record_id,
postal_code,
message_transmission_date,
manual_update_of_ip,
manual_update,
location,
is_international_version,
is_arisg_unit,
interchange_id,
icsr_encoding,
fda_registration_number,
external_app_updated_date,
e2b_validation,
e2b_user_id_archive,
e2b_test_name_coding,
e2b_sender_receiver,
e2b_meddra_coding,
e2b_file_format,
e2b_authority,
e2b_approved_status,
e2b_aer_owner_unit,
doctype_icsr,
doctype_ack,
doctor_role,
dgfps_no,
dept,
date_modified,
date_created,
database_id,
country_code,
country,
contact_phone_no,
contact_fax_no,
contact_email_id,
company_unit_flag,
company_doctor,
canadian_licence_no,
authority,
allow_batch_export,
addr_1,
addr,
ack_encoding
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP 
WHERE 	TO_DATE(LS_DB_COMPANY_UNIT.PROCESSING_DT) < TO_DATE(LS_DB_COMPANY_UNIT_TMP.PROCESSING_DT)
AND LS_DB_COMPANY_UNIT.INTEGRATION_ID = LS_DB_COMPANY_UNIT_TMP.INTEGRATION_ID
AND LS_DB_COMPANY_UNIT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_company_unit')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_COMPANY_UNIT_DELETION_TMP  WHERE TABLE_NAME='lsmv_company_unit')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_COMPANY_UNIT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_COMPANY_UNIT'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_COMPANY_UNIT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_COMPANY_UNIT'),
	'LSDB','Case','LS_DB_COMPANY_UNIT',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_COMPANY_UNIT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
