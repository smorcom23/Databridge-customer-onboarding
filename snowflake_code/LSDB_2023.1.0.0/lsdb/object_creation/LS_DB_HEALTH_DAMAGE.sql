
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_HEALTH_DAMAGE()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HEALTH_DAMAGE'),
	'LSRA','Case','LS_DB_HEALTH_DAMAGE',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_HEALTH_DAMAGE' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_HEALTH_DAMAGE'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_HEALTH_DAMAGE'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_HEALTH_DAMAGE_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_HEALTH_DAMAGE_DELETION_TMP  As select RECORD_ID,'lsmv_health_damage' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_health_damage WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','10063','10068','10120')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_health_damage WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HEALTH_DAMAGE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HEALTH_DAMAGE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_health_damage WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HEALTH_DAMAGE' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_HEALTH_DAMAGE' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_health_damage_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,associated_device_problem  associated_device_problem,date_created  date_created,date_modified  date_modified,event_received_date_jpnd  event_received_date_jpnd,event_received_date_jpnd_manual  event_received_date_jpnd_manual,fk_patient_rec_id  fk_patient_rec_id,health_damage_code  health_damage_code,health_damage_name  health_damage_name,record_id  record_id,regenerative_onset_date  regenerative_onset_date,regenerative_onset_date_fmt  regenerative_onset_date_fmt,regenerative_onset_date_sf  regenerative_onset_date_sf,regenerative_outcome_date  regenerative_outcome_date,regenerative_outcome_date_fmt  regenerative_outcome_date_fmt,regenerative_outcome_date_sf  regenerative_outcome_date_sf,regenerative_outcome_detail  regenerative_outcome_detail,regenerative_seriousness_criteria  regenerative_seriousness_criteria,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10120' AND CD=CAST(regenerative_seriousness_criteria AS VARCHAR(100)) )regenerative_seriousness_criteria_de_ml , seriousness_healath_damage  seriousness_healath_damage,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(seriousness_healath_damage AS VARCHAR(100)) )seriousness_healath_damage_de_ml , seriousness_healath_outcome  seriousness_healath_outcome,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10068' AND CD=CAST(seriousness_healath_outcome AS VARCHAR(100)) )seriousness_healath_outcome_de_ml , spr_id  spr_id,suspect_risk  suspect_risk,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10063' AND CD=CAST(suspect_risk AS VARCHAR(100)) )suspect_risk_de_ml , user_created  user_created,user_modified  user_modified,uuid  uuid,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_health_damage
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_HEALTH_DAMAGE_DELETION_TMP  WHERE TABLE_NAME='lsmv_health_damage')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_health_damage_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_health_damage_SUBSET.USER_CREATED CREATED_BY,lsmv_health_damage_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_health_damage_SUBSET.uuid  ,lsmv_health_damage_SUBSET.user_modified  ,lsmv_health_damage_SUBSET.user_created  ,lsmv_health_damage_SUBSET.suspect_risk_de_ml  ,lsmv_health_damage_SUBSET.suspect_risk  ,lsmv_health_damage_SUBSET.spr_id  ,lsmv_health_damage_SUBSET.seriousness_healath_outcome_de_ml  ,lsmv_health_damage_SUBSET.seriousness_healath_outcome  ,lsmv_health_damage_SUBSET.seriousness_healath_damage_de_ml  ,lsmv_health_damage_SUBSET.seriousness_healath_damage  ,lsmv_health_damage_SUBSET.regenerative_seriousness_criteria_de_ml  ,lsmv_health_damage_SUBSET.regenerative_seriousness_criteria  ,lsmv_health_damage_SUBSET.regenerative_outcome_detail  ,lsmv_health_damage_SUBSET.regenerative_outcome_date_sf  ,lsmv_health_damage_SUBSET.regenerative_outcome_date_fmt  ,lsmv_health_damage_SUBSET.regenerative_outcome_date  ,lsmv_health_damage_SUBSET.regenerative_onset_date_sf  ,lsmv_health_damage_SUBSET.regenerative_onset_date_fmt  ,lsmv_health_damage_SUBSET.regenerative_onset_date  ,lsmv_health_damage_SUBSET.record_id  ,lsmv_health_damage_SUBSET.health_damage_name  ,lsmv_health_damage_SUBSET.health_damage_code  ,lsmv_health_damage_SUBSET.fk_patient_rec_id  ,lsmv_health_damage_SUBSET.event_received_date_jpnd_manual  ,lsmv_health_damage_SUBSET.event_received_date_jpnd  ,lsmv_health_damage_SUBSET.date_modified  ,lsmv_health_damage_SUBSET.date_created  ,lsmv_health_damage_SUBSET.associated_device_problem  ,lsmv_health_damage_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_health_damage_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_health_damage_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_health_damage_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP)
where target_table_name='LS_DB_HEALTH_DAMAGE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HEALTH_DAMAGE'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE   
SET LS_DB_HEALTH_DAMAGE.uuid = LS_DB_HEALTH_DAMAGE_TMP.uuid,LS_DB_HEALTH_DAMAGE.user_modified = LS_DB_HEALTH_DAMAGE_TMP.user_modified,LS_DB_HEALTH_DAMAGE.user_created = LS_DB_HEALTH_DAMAGE_TMP.user_created,LS_DB_HEALTH_DAMAGE.suspect_risk_de_ml = LS_DB_HEALTH_DAMAGE_TMP.suspect_risk_de_ml,LS_DB_HEALTH_DAMAGE.suspect_risk = LS_DB_HEALTH_DAMAGE_TMP.suspect_risk,LS_DB_HEALTH_DAMAGE.spr_id = LS_DB_HEALTH_DAMAGE_TMP.spr_id,LS_DB_HEALTH_DAMAGE.seriousness_healath_outcome_de_ml = LS_DB_HEALTH_DAMAGE_TMP.seriousness_healath_outcome_de_ml,LS_DB_HEALTH_DAMAGE.seriousness_healath_outcome = LS_DB_HEALTH_DAMAGE_TMP.seriousness_healath_outcome,LS_DB_HEALTH_DAMAGE.seriousness_healath_damage_de_ml = LS_DB_HEALTH_DAMAGE_TMP.seriousness_healath_damage_de_ml,LS_DB_HEALTH_DAMAGE.seriousness_healath_damage = LS_DB_HEALTH_DAMAGE_TMP.seriousness_healath_damage,LS_DB_HEALTH_DAMAGE.regenerative_seriousness_criteria_de_ml = LS_DB_HEALTH_DAMAGE_TMP.regenerative_seriousness_criteria_de_ml,LS_DB_HEALTH_DAMAGE.regenerative_seriousness_criteria = LS_DB_HEALTH_DAMAGE_TMP.regenerative_seriousness_criteria,LS_DB_HEALTH_DAMAGE.regenerative_outcome_detail = LS_DB_HEALTH_DAMAGE_TMP.regenerative_outcome_detail,LS_DB_HEALTH_DAMAGE.regenerative_outcome_date_sf = LS_DB_HEALTH_DAMAGE_TMP.regenerative_outcome_date_sf,LS_DB_HEALTH_DAMAGE.regenerative_outcome_date_fmt = LS_DB_HEALTH_DAMAGE_TMP.regenerative_outcome_date_fmt,LS_DB_HEALTH_DAMAGE.regenerative_outcome_date = LS_DB_HEALTH_DAMAGE_TMP.regenerative_outcome_date,LS_DB_HEALTH_DAMAGE.regenerative_onset_date_sf = LS_DB_HEALTH_DAMAGE_TMP.regenerative_onset_date_sf,LS_DB_HEALTH_DAMAGE.regenerative_onset_date_fmt = LS_DB_HEALTH_DAMAGE_TMP.regenerative_onset_date_fmt,LS_DB_HEALTH_DAMAGE.regenerative_onset_date = LS_DB_HEALTH_DAMAGE_TMP.regenerative_onset_date,LS_DB_HEALTH_DAMAGE.record_id = LS_DB_HEALTH_DAMAGE_TMP.record_id,LS_DB_HEALTH_DAMAGE.health_damage_name = LS_DB_HEALTH_DAMAGE_TMP.health_damage_name,LS_DB_HEALTH_DAMAGE.health_damage_code = LS_DB_HEALTH_DAMAGE_TMP.health_damage_code,LS_DB_HEALTH_DAMAGE.fk_patient_rec_id = LS_DB_HEALTH_DAMAGE_TMP.fk_patient_rec_id,LS_DB_HEALTH_DAMAGE.event_received_date_jpnd_manual = LS_DB_HEALTH_DAMAGE_TMP.event_received_date_jpnd_manual,LS_DB_HEALTH_DAMAGE.event_received_date_jpnd = LS_DB_HEALTH_DAMAGE_TMP.event_received_date_jpnd,LS_DB_HEALTH_DAMAGE.date_modified = LS_DB_HEALTH_DAMAGE_TMP.date_modified,LS_DB_HEALTH_DAMAGE.date_created = LS_DB_HEALTH_DAMAGE_TMP.date_created,LS_DB_HEALTH_DAMAGE.associated_device_problem = LS_DB_HEALTH_DAMAGE_TMP.associated_device_problem,LS_DB_HEALTH_DAMAGE.ari_rec_id = LS_DB_HEALTH_DAMAGE_TMP.ari_rec_id,
LS_DB_HEALTH_DAMAGE.PROCESSING_DT = LS_DB_HEALTH_DAMAGE_TMP.PROCESSING_DT ,
LS_DB_HEALTH_DAMAGE.receipt_id     =LS_DB_HEALTH_DAMAGE_TMP.receipt_id        ,
LS_DB_HEALTH_DAMAGE.case_no        =LS_DB_HEALTH_DAMAGE_TMP.case_no           ,
LS_DB_HEALTH_DAMAGE.case_version   =LS_DB_HEALTH_DAMAGE_TMP.case_version      ,
LS_DB_HEALTH_DAMAGE.version_no     =LS_DB_HEALTH_DAMAGE_TMP.version_no        ,
LS_DB_HEALTH_DAMAGE.expiry_date    =LS_DB_HEALTH_DAMAGE_TMP.expiry_date       ,
LS_DB_HEALTH_DAMAGE.load_ts        =LS_DB_HEALTH_DAMAGE_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP 
WHERE 	LS_DB_HEALTH_DAMAGE.INTEGRATION_ID = LS_DB_HEALTH_DAMAGE_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_HEALTH_DAMAGE_TMP.PROCESSING_DT = LS_DB_HEALTH_DAMAGE.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uuid,
user_modified,
user_created,
suspect_risk_de_ml,
suspect_risk,
spr_id,
seriousness_healath_outcome_de_ml,
seriousness_healath_outcome,
seriousness_healath_damage_de_ml,
seriousness_healath_damage,
regenerative_seriousness_criteria_de_ml,
regenerative_seriousness_criteria,
regenerative_outcome_detail,
regenerative_outcome_date_sf,
regenerative_outcome_date_fmt,
regenerative_outcome_date,
regenerative_onset_date_sf,
regenerative_onset_date_fmt,
regenerative_onset_date,
record_id,
health_damage_name,
health_damage_code,
fk_patient_rec_id,
event_received_date_jpnd_manual,
event_received_date_jpnd,
date_modified,
date_created,
associated_device_problem,
ari_rec_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,uuid,
user_modified,
user_created,
suspect_risk_de_ml,
suspect_risk,
spr_id,
seriousness_healath_outcome_de_ml,
seriousness_healath_outcome,
seriousness_healath_damage_de_ml,
seriousness_healath_damage,
regenerative_seriousness_criteria_de_ml,
regenerative_seriousness_criteria,
regenerative_outcome_detail,
regenerative_outcome_date_sf,
regenerative_outcome_date_fmt,
regenerative_outcome_date,
regenerative_onset_date_sf,
regenerative_onset_date_fmt,
regenerative_onset_date,
record_id,
health_damage_name,
health_damage_code,
fk_patient_rec_id,
event_received_date_jpnd_manual,
event_received_date_jpnd,
date_modified,
date_created,
associated_device_problem,
ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP 
WHERE 	TO_DATE(LS_DB_HEALTH_DAMAGE.PROCESSING_DT) < TO_DATE(LS_DB_HEALTH_DAMAGE_TMP.PROCESSING_DT)
AND LS_DB_HEALTH_DAMAGE.INTEGRATION_ID = LS_DB_HEALTH_DAMAGE_TMP.INTEGRATION_ID
AND LS_DB_HEALTH_DAMAGE.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_HEALTH_DAMAGE_DELETION_TMP  WHERE TABLE_NAME='lsmv_health_damage')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_HEALTH_DAMAGE_DELETION_TMP  WHERE TABLE_NAME='lsmv_health_damage')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_HEALTH_DAMAGE_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_HEALTH_DAMAGE'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_HEALTH_DAMAGE'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_HEALTH_DAMAGE Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_HEALTH_DAMAGE'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_HEALTH_DAMAGE not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
