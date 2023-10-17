
/*

drop table if Exists  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER ;
CREATE TABLE if not Exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER (
ari_rec_id number (38,0),
st_msg_record_id number (38,0),
processing_dt DATE, 
EXPIRY_DATE DATE , 
date_modified TIMESTAMP_NTZ,
load_ts TIMESTAMP_NTZ,  
DER_DISTRIBUTED_ON_DAY TEXT(10),
integration_id TEXT(400)

) CHANGE_TRACKING = TRUE;


call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER();
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_CASE_ST_MSG_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_ST_MSG_DER';


-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};

*/

CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_ST_MSG_DER()
RETURNS VARCHAR NOT NULL

LANGUAGE SQL
AS
$$

DECLARE

CURRENT_TS_VAR TIMESTAMP;

BEGIN 

CURRENT_TS_VAR := CURRENT_TIMESTAMP();

insert into ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG (ROW_WID,TARGET_TABLE_NAME,PARAM_NAME)

select ROW_WID,TARGET_TABLE_NAME,PARAM_NAME from 
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_ST_MSG_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_ST_MSG_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_ST_MSG_DER'),
	'LSRA','Case','LS_DB_CASE_ST_MSG_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_ST_MSG_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_ST_MSG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_ST_MSG_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP  As 
select RECORD_ID,'lsmv_safety_report' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_safety_report WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_st_message' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_drug WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID    FROM ${stage_db_name}.${stage_schema_name}.lsmv_safety_report 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_ST_MSG_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_ST_MSG_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_ST_MSG_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_ST_MSG_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
),lsmv_st_message_SUBSET as
(
select  ARI_REC_ID,st_msg_record_id,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,record_id st_msg_record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_st_message WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP where table_name='lsmv_st_message')
) where
REC_RANK=1
group by 1,2
),LSMV_SAFETY_REPORT_SUBSET as
(
select  ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP where table_name='LSMV_SAFETY_REPORT')
) where
REC_RANK=1
group by 1
)



 SELECT lsmv_st_message_SUBSET.ARI_REC_ID,
 lsmv_st_message_SUBSET.st_msg_record_id,
 max(to_date(GREATEST(NVL(lsmv_st_message_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_safety_report_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(lsmv_st_message_SUBSET.ARI_REC_ID,-1),'||',NVL(lsmv_st_message_SUBSET.st_msg_record_id,-1)) INTEGRATION_ID
,max(GREATEST(NVL(lsmv_safety_report_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)) as DATE_MODIFIED
FROM lsmv_st_message_SUBSET left join lsmv_safety_report_SUBSET 
on lsmv_st_message_SUBSET.ari_rec_id=lsmv_safety_report_SUBSET.ari_rec_id
group by lsmv_st_message_SUBSET.ARI_REC_ID,
 lsmv_st_message_SUBSET.st_msg_record_id
;


  
  

/*


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID,BLINDED_PRODUCT_REC_ID,record_id
				from 
				(
				SELECT record_id,ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP)
				and record_id not in (select record_id from LS_DB_CASE_ST_MSG_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
				) LSMV_DRUG
                 where LSMV_DRUG.rank=1 
				AND LSMV_DRUG.CDC_OPERATION_TYPE IN ('I','U')  
;      

 drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp ;
create temp table  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.CODELIST_SUBSET_tmp AS 
 
 select CODE,DECODE,SPR_ID,CODELIST_ID,EMDR_CODE from 
    (
    	SELECT distinct LSMV_CODELIST_CODE.CODE ,LSMV_CODELIST_DECODE.DECODE ,SPR_ID,LSMV_CODELIST_NAME.CODELIST_ID,EMDR_CODE
    	,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE,SPR_ID 
    					order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank
						FROM    
                                 (
                                    SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_NAME WHERE codelist_id IN ('9947','9948','9949','10007')
                                 ) LSMV_CODELIST_NAME JOIN
                                 (
                                    SELECT RECORD_ID,      CODE,   FK_CL_NAME_REC_ID,CDC_OPERATION_TIME,EMDR_CODE,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                     FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_CODE
                                 ) LSMV_CODELIST_CODE  ON LSMV_CODELIST_NAME.RECORD_ID=LSMV_CODELIST_CODE.FK_CL_NAME_REC_ID
                                 AND LSMV_CODELIST_NAME.RANK=1 AND LSMV_CODELIST_CODE.RANK=1
                                 JOIN 
                                 (
                                    SELECT RECORD_ID,LANGUAGE_CODE, DECODE, FK_CL_CODE_REC_ID  ,CDC_OPERATION_TIME,
                                   Coalesce(SPR_ID,'-9999') SPR_ID,
									ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                    FROM ${stage_db_name}.${stage_schema_name}.LSMV_CODELIST_DECODE where LANGUAGE_CODE='en'
                                 ) LSMV_CODELIST_DECODE ON LSMV_CODELIST_CODE.RECORD_ID = LSMV_CODELIST_DECODE.FK_CL_CODE_REC_ID
                                 AND LSMV_CODELIST_DECODE.RANK=1) where rank=1 
;					

*/
	

 ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_tmp ADD COLUMN DER_DISTRIBUTED_ON_DAY TEXT(10);	
 
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_tmp   
SET LS_DB_CASE_ST_MSG_DER_tmp.DER_DISTRIBUTED_ON_DAY=LS_DB_CASE_ST_MSG_DER_FINAL.DER_DISTRIBUTED_ON_DAY   
FROM (
SELECT D.ARI_REC_ID ,
D.record_id,

CASE 
WHEN A.RECEIPT_DATE IS NULL OR D.APP_DISTRIBUTED_DATE IS NULL 
THEN CAST(NULL AS INTEGER) 
WHEN A.RECEIPT_DATE_FMT IN ('102','204','611','203')
THEN DATEDIFF ( DAY, A.RECEIPT_DATE, D.APP_DISTRIBUTED_DATE )
ELSE CAST(NULL AS INTEGER) 
END AS DER_DISTRIBUTED_ON_DAY
FROM 
              (select ARI_REC_ID,APP_DISTRIBUTED_DATE,record_id
				FROM 
				(select ARI_REC_ID,APP_DISTRIBUTED_DATE,record_id,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_ST_MESSAGE where 
				ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP)
				)
				WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
				) D
            INNER JOIN (select ARI_REC_ID,RECEIPT_DATE,RECEIPT_DATE_FMT
				FROM 
				(select ARI_REC_ID,RECEIPT_DATE,RECEIPT_DATE_FMT,record_id,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_SAFETY_REPORT where 
				ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP)
				)
				WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')
			) A
ON D.ARI_REC_ID = A.ARI_REC_ID
) LS_DB_CASE_ST_MSG_DER_FINAL
    WHERE LS_DB_CASE_ST_MSG_DER_tmp.ARI_REC_ID = LS_DB_CASE_ST_MSG_DER_FINAL.ARI_REC_ID	
	AND LS_DB_CASE_ST_MSG_DER_tmp.st_msg_record_id= LS_DB_CASE_ST_MSG_DER_FINAL.record_id	
	AND LS_DB_CASE_ST_MSG_DER_tmp.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM');



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP)
where target_table_name='LS_DB_CASE_ST_MSG_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                  where target_table_name='LS_DB_CASE_ST_MSG_DER'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER   
SET LS_DB_CASE_ST_MSG_DER.ARI_REC_ID = LS_DB_CASE_ST_MSG_DER_TMP.ARI_REC_ID,
LS_DB_CASE_ST_MSG_DER.st_msg_record_id = LS_DB_CASE_ST_MSG_DER_TMP.st_msg_record_id,
LS_DB_CASE_ST_MSG_DER.PROCESSING_DT = LS_DB_CASE_ST_MSG_DER_TMP.PROCESSING_DT,
LS_DB_CASE_ST_MSG_DER.expiry_date    =LS_DB_CASE_ST_MSG_DER_TMP.expiry_date,
LS_DB_CASE_ST_MSG_DER.date_modified    =LS_DB_CASE_ST_MSG_DER_TMP.date_modified,
LS_DB_CASE_ST_MSG_DER.load_ts    =LS_DB_CASE_ST_MSG_DER_TMP.load_ts,
LS_DB_CASE_ST_MSG_DER.DER_DISTRIBUTED_ON_DAY     =LS_DB_CASE_ST_MSG_DER_TMP.DER_DISTRIBUTED_ON_DAY
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP 
WHERE 	LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID = LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_ST_MSG_DER_TMP.PROCESSING_DT = LS_DB_CASE_ST_MSG_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_ST_MSG_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_ST_MSG_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_ST_MSG_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_ST_MSG_DER.ari_rec_id ,LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP 
ON LS_DB_CASE_ST_MSG_DER.ari_rec_id=LS_DB_CASE_ST_MSG_DER_TMP.ari_rec_id
AND LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID = LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_ST_MSG_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_ST_MSG_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP )
) TMP where LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_ST_MSG_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_ST_MSG_DER.ari_rec_id ,LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP 
    ON LS_DB_CASE_ST_MSG_DER.ari_rec_id=LS_DB_CASE_ST_MSG_DER_TMP.ari_rec_id
    AND LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID = LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_ST_MSG_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_ST_MSG_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER
( ARI_REC_ID    ,
st_msg_record_id       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_DISTRIBUTED_ON_DAY
)
SELECT 
  ARI_REC_ID    ,
st_msg_record_id       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_DISTRIBUTED_ON_DAY                              
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER TGT
WHERE  ( st_msg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_message') )
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_ST_MSG_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_ST_MSG_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_ST_MSG_DER.INTEGRATION_ID = LS_DB_CASE_ST_MSG_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_ST_MSG_DER.st_msg_record_id = LS_DB_CASE_ST_MSG_DER_TMP.st_msg_record_id
AND LS_DB_CASE_ST_MSG_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
 AND MD5(NVL(LS_DB_CASE_ST_MSG_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_ST_MSG_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( st_msg_record_id  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_st_message')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_ST_MSG_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_ST_MSG_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_ST_MSG_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_ST_MSG_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_ST_MSG_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_ST_MSG_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
