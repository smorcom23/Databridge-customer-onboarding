
/*
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT ADD COLUMN DER_AUTOPSY_DATE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT ADD COLUMN DER_DEATH_DATE TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT ADD COLUMN DER_DATE_OF_BIRTH TEXT;
ALTER TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT ADD COLUMN DER_PATIENT_AGE_IN_YEARS TEXT;

select distinct DER_AUTOPSY_DATE,DER_DEATH_DATE,DER_DATE_OF_BIRTH,DER_PATIENT_AGE_IN_YEARS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT

*/

-- delete from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_DER'
-- CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT_DER()
-- -- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT_DER()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();





insert into $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+1 ROW_WID,'LS_DB_PATIENT_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select (select max( ROW_WID) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)+2,'LS_DB_PATIENT_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG)
;

INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_DER'),
	'LSRA','Case','LS_DB_PATIENT_DER',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL 
						WHERE TARGET_TABLE_NAME='LS_DB_PATIENT_DER' 
						AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= (select max(load_ts) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT)
WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_CASE_QFC;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_CASE_QFC AS
select distinct pat_ari_rec_id as ari_rec_id
FROM 
	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT 
	
	where load_ts > (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB')  and

load_ts <=  (SELECT PARAM_VALUE FROM 
$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME = 'LS_DB_PATIENT_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB') AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;	


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT   
SET LS_DB_PATIENT.DER_AUTOPSY_DATE=LS_DB_PATIENT_TMP.DER_AUTOPSY_DATE  , 
LS_DB_PATIENT.DER_DEATH_DATE=LS_DB_PATIENT_TMP.DER_DEATH_DATE,
LS_DB_PATIENT.DER_DATE_OF_BIRTH=LS_DB_PATIENT_TMP.DER_DATE_OF_BIRTH
FROM (SELECT 	distinct pat_ARI_REC_ID,
		case  when  patdth_PATAUTOPSYDATEFMT is null  then to_char(patdth_PATAUTOPSYDATE,'DD-MON-YYYY')
	  when  patdth_PATAUTOPSYDATEFMT in (0,102)  then to_char(patdth_PATAUTOPSYDATE,'DD-MON-YYYY')
	  when  patdth_PATAUTOPSYDATEFMT in (1,602)  then to_char(patdth_PATAUTOPSYDATE,'YYYY')
	  when  patdth_PATAUTOPSYDATEFMT in (2,610)  then to_char(patdth_PATAUTOPSYDATE,'MON-YYYY')
	  when  patdth_PATAUTOPSYDATEFMT in (3,4,5,6,7,8,9,204,203)  then to_char(patdth_PATAUTOPSYDATE,'DD-MON-YYYY')
	end DER_AUTOPSY_DATE,
	case when  patdth_PATDEATHDATE_NF is not null then UPPER(patdth_PATDEATHDATE_NF)
	 when  patdth_PATDEATHDATEFMT is null  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY'))
	  when  patdth_PATDEATHDATEFMT in (0,7,8,9,102)  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY'))
	  when  patdth_PATDEATHDATEFMT in (1,602)  then to_char(patdth_PATDEATHDATE,'YYYY')
	  when  patdth_PATDEATHDATEFMT in (2,610)  then UPPER(to_char(patdth_PATDEATHDATE,'MON-YYYY'))
	  when  patdth_PATDEATHDATEFMT in (3,204)  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY'))
	  when  patdth_PATDEATHDATEFMT in (4,204)  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY hh24'))
	  when  patdth_PATDEATHDATEFMT in (5,204)  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY hh24:mi'))
	  when  patdth_PATDEATHDATEFMT in (6,204,203)  then UPPER(to_char(patdth_PATDEATHDATE,'DD-MON-YYYY hh24:mi:ss'))
	end DER_DEATH_DATE,
	case  when  pat_PATDOB_NF is not null then UPPER(pat_PATDOB_NF)
	  when  pat_PATDOB_FMT is null  then UPPER(to_char(pat_PATDOB,'DD-MON-YYYY'))
	  when  pat_PATDOB_FMT in (0,102)  then UPPER(to_char(pat_PATDOB,'DD-MON-YYYY'))
	  when  pat_PATDOB_FMT in (1,602)  then UPPER(to_char(pat_PATDOB,'YYYY'))
	  when  pat_PATDOB_FMT in (2,610)  then UPPER(to_char(pat_PATDOB,'MON-YYYY'))
	  when  pat_PATDOB_FMT in (3,4,5,6,7,8,9,204,203)  then UPPER(to_char(pat_PATDOB,'DD-MON-YYYY'))
	end DER_DATE_OF_BIRTH
	
FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT where pat_ARI_REC_ID in (select ari_rec_id from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_CASE_QFC)
AND EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
) LS_DB_PATIENT_TMP
    WHERE LS_DB_PATIENT.pat_ARI_REC_ID = LS_DB_PATIENT_TMP.pat_ARI_REC_ID	
	AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT   
SET LS_DB_PATIENT.DER_PATIENT_AGE_IN_YEARS=LS_DB_PATIENT_TMP.DER_PATIENT_AGE_IN_YEARS 
FROM (
select PAT_ARI_REC_ID,
CASE 
WHEN  AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '801' THEN FLOOR(ROUND(AGE,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '802' THEN FLOOR(ROUND(AGE/12,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '803' THEN FLOOR(ROUND(AGE/52,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '804' THEN FLOOR(ROUND(AGE/365,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '805' THEN FLOOR(ROUND(AGE/8760,3))
 WHEN AGE_UNIT IS NOT NULL AND AGE IS NOT NULL AND AGE_UNIT = '800' THEN FLOOR(ROUND(AGE*10,3))-5
 WHEN REACTSTARTDATE IS NOT NULL AND DOB IS NOT NULL
THEN (CASE WHEN FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) < 0 
		   THEN FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) 
		   ELSE FLOOR(ROUND((datediff(DAY,DOB,REACTSTARTDATE)/365.25),3)) END)
 ELSE NULL
  END AS DER_PATIENT_AGE_IN_YEARS
FROM

(
select distinct  pat_ARI_REC_ID,REACTSTARTDATE,
case when LENGTH(A.PAT_PATONSETAGE) > 0 
     then CAST(A.PAT_PATONSETAGE AS DOUBLE PRECISION)
     else CAST(null AS DOUBLE PRECISION) end AS AGE,
                A.PAT_PATONSETAGEUNIT AS AGE_UNIT,
				A.PAT_PATDOB AS DOB

FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT A JOIN

(
select ARI_REC_ID,MIN(REACTSTARTDATE) as REACTSTARTDATE 
FROM  (
select ARI_REC_ID,REACTSTARTDATE,REACTMEDDRAPT_CODE
,row_number() over (partition by ARI_REC_ID order by CDC_OPERATION_TIME desc) rank
 from $$STG_DB_NAME.$$LSDB_RPL.LSMV_REACTION where ARI_REC_ID in 
(select  ARI_REC_ID from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT_CASE_QFC)
 and CDC_OPERATION_TYPE IN ('I','U')
 and coalesce(REACTMEDDRAPT_CODE,'-1')  not in ('My nose is bleeding','My back hurts too','EVENT02','EVENT01'))
  where rank=1
group by ARI_REC_ID) B on A.pat_ari_rec_id=B.ARI_REC_ID
and A.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
)) LS_DB_PATIENT_TMP
    WHERE LS_DB_PATIENT.pat_ARI_REC_ID = LS_DB_PATIENT_TMP.pat_ARI_REC_ID	
	AND LS_DB_PATIENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
LOAD_TS=(select max(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_PATIENT),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_PATIENT_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_PATIENT_DER'
and LOAD_STATUS = 'In Progress') ;	


 RETURN 'LS_DB_PATIENT_DER Update completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line,LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_PATIENT_DER'
and LOAD_STATUS = 'In Progress'
;



END;
$$
;	
/*


 CREATE or replace TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_PATIENT
  WAREHOUSE = EXTRASMALL
  Schedule = '15 minute'
AS
EXECUTE IMMEDIATE $$
DECLARE
    id int;
BEGIN
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT();
  CALL $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_PATIENT_DER();
END;
$$
;

ALTER TASK $$TGT_DB_NAME.$$LSDB_TRANSFM.TSK_LS_DB_PATIENT RESUME; 
 				*/