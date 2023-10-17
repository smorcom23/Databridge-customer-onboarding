
-- -- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};
CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_REACT_VACCINE()
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
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACT_VACCINE'),
	'LSDB','Case','LS_DB_REACT_VACCINE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null; 

DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP; 
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP  As 
select 'LS_DB_REACT_VACCINE' TARGET_TABLE_NAME,
nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_REACT_VACCINE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD')) PARAM_VALUE,
'CDC_EXTRACT_TS_LB' PARAM_NAME; 




DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACT_VACCINE_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACT_VACCINE_DELETION_TMP  As select RECORD_ID,'lsmv_react_vaccine' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_react_vaccine WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','1002','1002','1002','1002')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_react_vaccine WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_REACT_VACCINE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= :CURRENT_TS_VAR
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_react_vaccine WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG_TMP WHERE TARGET_TABLE_NAME ='LS_DB_REACT_VACCINE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
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

), lsmv_react_vaccine_SUBSET AS 
(
select * from 
    (SELECT  
    ae_outcome  ae_outcome,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(ae_outcome AS VARCHAR(100)) )ae_outcome_de_ml , ae_outcome_nf  ae_outcome_nf,ari_rec_id  ari_rec_id,city  city,city_nf  city_nf,date_created  date_created,date_modified  date_modified,department  department,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(department AS VARCHAR(100)) )department_de_ml , department_nf  department_nf,email_id  email_id,first_name  first_name,healthcare_professional  healthcare_professional,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(healthcare_professional AS VARCHAR(100)) )healthcare_professional_de_ml , healthcare_professional_nf  healthcare_professional_nf,hospital_name  hospital_name,hospital_name_nf  hospital_name_nf,hospitalization_required  hospitalization_required,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(hospitalization_required AS VARCHAR(100)) )hospitalization_required_de_ml , hospitalization_required_nf  hospitalization_required_nf,last_name  last_name,middle_name  middle_name,no_of_days_hospitalized  no_of_days_hospitalized,no_of_days_hospitalized_nf  no_of_days_hospitalized_nf,record_id  record_id,result_prolong_hospital  result_prolong_hospital,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(result_prolong_hospital AS VARCHAR(100)) )result_prolong_hospital_de_ml , result_prolong_hospital_nf  result_prolong_hospital_nf,spr_id  spr_id,state  state,state_nf  state_nf,telephone  telephone,title  title,user_created  user_created,user_modified  user_modified,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
${stage_db_name}.${stage_schema_name}.lsmv_react_vaccine
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACT_VACCINE_DELETION_TMP  WHERE TABLE_NAME='lsmv_react_vaccine')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_react_vaccine_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_react_vaccine_SUBSET.USER_CREATED CREATED_BY,lsmv_react_vaccine_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_react_vaccine_SUBSET.user_modified  ,lsmv_react_vaccine_SUBSET.user_created  ,lsmv_react_vaccine_SUBSET.title  ,lsmv_react_vaccine_SUBSET.telephone  ,lsmv_react_vaccine_SUBSET.state_nf  ,lsmv_react_vaccine_SUBSET.state  ,lsmv_react_vaccine_SUBSET.spr_id  ,lsmv_react_vaccine_SUBSET.result_prolong_hospital_nf  ,lsmv_react_vaccine_SUBSET.result_prolong_hospital_de_ml  ,lsmv_react_vaccine_SUBSET.result_prolong_hospital  ,lsmv_react_vaccine_SUBSET.record_id  ,lsmv_react_vaccine_SUBSET.no_of_days_hospitalized_nf  ,lsmv_react_vaccine_SUBSET.no_of_days_hospitalized  ,lsmv_react_vaccine_SUBSET.middle_name  ,lsmv_react_vaccine_SUBSET.last_name  ,lsmv_react_vaccine_SUBSET.hospitalization_required_nf  ,lsmv_react_vaccine_SUBSET.hospitalization_required_de_ml  ,lsmv_react_vaccine_SUBSET.hospitalization_required  ,lsmv_react_vaccine_SUBSET.hospital_name_nf  ,lsmv_react_vaccine_SUBSET.hospital_name  ,lsmv_react_vaccine_SUBSET.healthcare_professional_nf  ,lsmv_react_vaccine_SUBSET.healthcare_professional_de_ml  ,lsmv_react_vaccine_SUBSET.healthcare_professional  ,lsmv_react_vaccine_SUBSET.first_name  ,lsmv_react_vaccine_SUBSET.email_id  ,lsmv_react_vaccine_SUBSET.department_nf  ,lsmv_react_vaccine_SUBSET.department_de_ml  ,lsmv_react_vaccine_SUBSET.department  ,lsmv_react_vaccine_SUBSET.date_modified  ,lsmv_react_vaccine_SUBSET.date_created  ,lsmv_react_vaccine_SUBSET.city_nf  ,lsmv_react_vaccine_SUBSET.city  ,lsmv_react_vaccine_SUBSET.ari_rec_id  ,lsmv_react_vaccine_SUBSET.ae_outcome_nf  ,lsmv_react_vaccine_SUBSET.ae_outcome_de_ml  ,lsmv_react_vaccine_SUBSET.ae_outcome ,CONCAT(NVL(lsmv_react_vaccine_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_react_vaccine_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_react_vaccine_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP)
where target_table_name='LS_DB_REACT_VACCINE'

; 






UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE   
SET LS_DB_REACT_VACCINE.user_modified = LS_DB_REACT_VACCINE_TMP.user_modified,LS_DB_REACT_VACCINE.user_created = LS_DB_REACT_VACCINE_TMP.user_created,LS_DB_REACT_VACCINE.title = LS_DB_REACT_VACCINE_TMP.title,LS_DB_REACT_VACCINE.telephone = LS_DB_REACT_VACCINE_TMP.telephone,LS_DB_REACT_VACCINE.state_nf = LS_DB_REACT_VACCINE_TMP.state_nf,LS_DB_REACT_VACCINE.state = LS_DB_REACT_VACCINE_TMP.state,LS_DB_REACT_VACCINE.spr_id = LS_DB_REACT_VACCINE_TMP.spr_id,LS_DB_REACT_VACCINE.result_prolong_hospital_nf = LS_DB_REACT_VACCINE_TMP.result_prolong_hospital_nf,LS_DB_REACT_VACCINE.result_prolong_hospital_de_ml = LS_DB_REACT_VACCINE_TMP.result_prolong_hospital_de_ml,LS_DB_REACT_VACCINE.result_prolong_hospital = LS_DB_REACT_VACCINE_TMP.result_prolong_hospital,LS_DB_REACT_VACCINE.record_id = LS_DB_REACT_VACCINE_TMP.record_id,LS_DB_REACT_VACCINE.no_of_days_hospitalized_nf = LS_DB_REACT_VACCINE_TMP.no_of_days_hospitalized_nf,LS_DB_REACT_VACCINE.no_of_days_hospitalized = LS_DB_REACT_VACCINE_TMP.no_of_days_hospitalized,LS_DB_REACT_VACCINE.middle_name = LS_DB_REACT_VACCINE_TMP.middle_name,LS_DB_REACT_VACCINE.last_name = LS_DB_REACT_VACCINE_TMP.last_name,LS_DB_REACT_VACCINE.hospitalization_required_nf = LS_DB_REACT_VACCINE_TMP.hospitalization_required_nf,LS_DB_REACT_VACCINE.hospitalization_required_de_ml = LS_DB_REACT_VACCINE_TMP.hospitalization_required_de_ml,LS_DB_REACT_VACCINE.hospitalization_required = LS_DB_REACT_VACCINE_TMP.hospitalization_required,LS_DB_REACT_VACCINE.hospital_name_nf = LS_DB_REACT_VACCINE_TMP.hospital_name_nf,LS_DB_REACT_VACCINE.hospital_name = LS_DB_REACT_VACCINE_TMP.hospital_name,LS_DB_REACT_VACCINE.healthcare_professional_nf = LS_DB_REACT_VACCINE_TMP.healthcare_professional_nf,LS_DB_REACT_VACCINE.healthcare_professional_de_ml = LS_DB_REACT_VACCINE_TMP.healthcare_professional_de_ml,LS_DB_REACT_VACCINE.healthcare_professional = LS_DB_REACT_VACCINE_TMP.healthcare_professional,LS_DB_REACT_VACCINE.first_name = LS_DB_REACT_VACCINE_TMP.first_name,LS_DB_REACT_VACCINE.email_id = LS_DB_REACT_VACCINE_TMP.email_id,LS_DB_REACT_VACCINE.department_nf = LS_DB_REACT_VACCINE_TMP.department_nf,LS_DB_REACT_VACCINE.department_de_ml = LS_DB_REACT_VACCINE_TMP.department_de_ml,LS_DB_REACT_VACCINE.department = LS_DB_REACT_VACCINE_TMP.department,LS_DB_REACT_VACCINE.date_modified = LS_DB_REACT_VACCINE_TMP.date_modified,LS_DB_REACT_VACCINE.date_created = LS_DB_REACT_VACCINE_TMP.date_created,LS_DB_REACT_VACCINE.city_nf = LS_DB_REACT_VACCINE_TMP.city_nf,LS_DB_REACT_VACCINE.city = LS_DB_REACT_VACCINE_TMP.city,LS_DB_REACT_VACCINE.ari_rec_id = LS_DB_REACT_VACCINE_TMP.ari_rec_id,LS_DB_REACT_VACCINE.ae_outcome_nf = LS_DB_REACT_VACCINE_TMP.ae_outcome_nf,LS_DB_REACT_VACCINE.ae_outcome_de_ml = LS_DB_REACT_VACCINE_TMP.ae_outcome_de_ml,LS_DB_REACT_VACCINE.ae_outcome = LS_DB_REACT_VACCINE_TMP.ae_outcome,
LS_DB_REACT_VACCINE.PROCESSING_DT = LS_DB_REACT_VACCINE_TMP.PROCESSING_DT ,
LS_DB_REACT_VACCINE.receipt_id     =LS_DB_REACT_VACCINE_TMP.receipt_id        ,
LS_DB_REACT_VACCINE.case_no        =LS_DB_REACT_VACCINE_TMP.case_no           ,
LS_DB_REACT_VACCINE.case_version   =LS_DB_REACT_VACCINE_TMP.case_version      ,
LS_DB_REACT_VACCINE.version_no     =LS_DB_REACT_VACCINE_TMP.version_no        ,
LS_DB_REACT_VACCINE.expiry_date    =LS_DB_REACT_VACCINE_TMP.expiry_date       ,
LS_DB_REACT_VACCINE.load_ts        =LS_DB_REACT_VACCINE_TMP.load_ts         
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP 
WHERE 	LS_DB_REACT_VACCINE.INTEGRATION_ID = LS_DB_REACT_VACCINE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_REACT_VACCINE_TMP.PROCESSING_DT = LS_DB_REACT_VACCINE.PROCESSING_DT,1=1);


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE
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
title,
telephone,
state_nf,
state,
spr_id,
result_prolong_hospital_nf,
result_prolong_hospital_de_ml,
result_prolong_hospital,
record_id,
no_of_days_hospitalized_nf,
no_of_days_hospitalized,
middle_name,
last_name,
hospitalization_required_nf,
hospitalization_required_de_ml,
hospitalization_required,
hospital_name_nf,
hospital_name,
healthcare_professional_nf,
healthcare_professional_de_ml,
healthcare_professional,
first_name,
email_id,
department_nf,
department_de_ml,
department,
date_modified,
date_created,
city_nf,
city,
ari_rec_id,
ae_outcome_nf,
ae_outcome_de_ml,
ae_outcome)
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
title,
telephone,
state_nf,
state,
spr_id,
result_prolong_hospital_nf,
result_prolong_hospital_de_ml,
result_prolong_hospital,
record_id,
no_of_days_hospitalized_nf,
no_of_days_hospitalized,
middle_name,
last_name,
hospitalization_required_nf,
hospitalization_required_de_ml,
hospitalization_required,
hospital_name_nf,
hospital_name,
healthcare_professional_nf,
healthcare_professional_de_ml,
healthcare_professional,
first_name,
email_id,
department_nf,
department_de_ml,
department,
date_modified,
date_created,
city_nf,
city,
ari_rec_id,
ae_outcome_nf,
ae_outcome_de_ml,
ae_outcome
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE TGT)
                                                                                ; 
COMMIT;



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP 
WHERE 	TO_DATE(LS_DB_REACT_VACCINE.PROCESSING_DT) < TO_DATE(LS_DB_REACT_VACCINE_TMP.PROCESSING_DT)
AND LS_DB_REACT_VACCINE.INTEGRATION_ID = LS_DB_REACT_VACCINE_TMP.INTEGRATION_ID
AND LS_DB_REACT_VACCINE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACT_VACCINE_DELETION_TMP  WHERE TABLE_NAME='lsmv_react_vaccine')
)
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_REACT_VACCINE_DELETION_TMP  WHERE TABLE_NAME='lsmv_react_vaccine')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_REACT_VACCINE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_REACT_VACCINE'
;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
SELECT * FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL_TMP;


  RETURN 'LS_DB_REACT_VACCINE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;

INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_REACT_VACCINE'),
	'LSDB','Case','LS_DB_REACT_VACCINE',null,:CURRENT_TS_VAR,null,null,null,null,:line,'Error',null;
    
    
  RETURN 'LS_DB_REACT_VACCINE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
