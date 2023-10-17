 
/*

drop table if Exists  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER ;
CREATE TABLE if not Exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER (
ari_rec_id number (38,0),
SEQ_WF_TRACKER number (38,0),
processing_dt DATE, 
EXPIRY_DATE DATE , 
date_modified TIMESTAMP_NTZ,
load_ts TIMESTAMP_NTZ,  
integration_id TEXT(400)

) CHANGE_TRACKING = TRUE;


call ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER();
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG  where TARGET_TABLE_NAME='LS_DB_CASE_WF_TRACKER_DER';
-- delete from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL  where TARGET_TABLE_NAME='LS_DB_CASE_WF_TRACKER_DER';


-- USE SCHEMA ${tenant_transfm_db_name}.${tenant_transfm_schema_name};

*/

CREATE OR REPLACE PROCEDURE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.PRC_LS_DB_CASE_WF_TRACKER_DER()
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
(select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+1,1) ROW_WID,'LS_DB_CASE_WF_TRACKER_DER' AS TARGET_TABLE_NAME,'CDC_EXTRACT_TS_LB' PARAM_NAME
 union all select coalesce((select max( ROW_WID) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)+2,1),'LS_DB_CASE_WF_TRACKER_DER','CDC_EXTRACT_TS_UB'
) where TARGET_TABLE_NAME not in (select TARGET_TABLE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG)
;



INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL(ROW_WID,FUNCTIONAL_AREA,ENTITY_NAME,TARGET_TABLE_NAME,LOAD_TS,LOAD_START_TS,LOAD_END_TS,
REC_READ_CNT,REC_PROCESSED_CNT,ERROR_REC_CNT,ERROR_DETAILS,LOAD_STATUS,CHANGED_REC_SET
)
SELECT (select nvl(max(row_wid)+1,1) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_WF_TRACKER_DER'),
	'LSRA','Case','LS_DB_CASE_WF_TRACKER_DER',null,CURRENT_TIMESTAMP,null,null,null,null,null,'In Progress',null;


UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASE_WF_TRACKER_DER' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG
SET PARAM_VALUE= CURRENT_TIMESTAMP
WHERE TARGET_TABLE_NAME = 'LS_DB_CASE_WF_TRACKER_DER'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP  As 
select RECORD_ID,'lsmv_wf_tracker' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker WHERE CDC_OPERATION_TYPE IN ('D') 
UNION ALL select RECORD_ID,'lsmv_message' AS TABLE_NAME FROM ${stage_db_name}.${stage_schema_name}.lsmv_message WHERE CDC_OPERATION_TYPE IN ('D') 
;


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP;
CREATE TEMPORARY TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP  AS WITH 
LSMV_CASE_NO_SUBSET as
 (
 
select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID    FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM ${stage_db_name}.${stage_schema_name}.lsmv_message 
   WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT a.record_id, 0 common_parent_key,  b.ARI_REC_ID ARI_REC_ID  from (select record_id,im_rec_id from ${stage_db_name}.${stage_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO
							WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
							AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASE_WF_TRACKER_DER' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
                          ) a left join (select DISTINCT fk_aim_record_id, ARI_REC_ID    FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker) b
						  on a.im_rec_id=b.fk_aim_record_id

),LSMV_MESSAGE_SUBSET as
(
select  msg_record_id,FK_ARI_REC_ID as ARI_REC_ID,max(date_modified) date_modified  
FROM
(
select record_id msg_record_id,FK_ARI_REC_ID,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_message WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP where table_name='lsmv_message')
) where
REC_RANK=1
group by 1,2
),LSMV_WF_TRACKER_SUBSET as
(
select  ARI_REC_ID,Record_id,fk_aim_record_id,max(date_modified) date_modified  
FROM
(
select ARI_REC_ID,Record_id,fk_aim_record_id,date_modified,row_number() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker WHERE 
    ARI_REC_ID in (select ARI_REC_ID from LSMV_CASE_NO_SUBSET)
    -- AND RECORD_ID not in (select record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP where table_name='lsmv_wf_tracker')
) where
REC_RANK=1
group by 1,2,3
)



 SELECT lsmv_message_SUBSET.ARI_REC_ID,
 lsmv_wf_tracker_SUBSET.record_id as SEQ_WF_TRACKER,
 max(to_date(GREATEST(NVL(lsmv_message_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_wf_tracker_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)))
PROCESSING_DT  
,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	
,CURRENT_TIMESTAMP as load_ts  
,CONCAT(NVL(lsmv_message_SUBSET.ARI_REC_ID,-1),'||',NVL(lsmv_wf_tracker_SUBSET.record_id,-1)) INTEGRATION_ID
,max(GREATEST(NVL(lsmv_message_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')),
NVL(lsmv_wf_tracker_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
)) as DATE_MODIFIED
FROM lsmv_wf_tracker_SUBSET  left join  lsmv_message_SUBSET
on lsmv_wf_tracker_SUBSET.fk_aim_record_id=lsmv_message_SUBSET.msg_record_id
group by lsmv_message_SUBSET.ARI_REC_ID,
 lsmv_wf_tracker_SUBSET.record_id
;


  
  

/*


DROP TABLE IF EXISTS ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_UNBLINDED;
CREATE TEMPORARY TABLE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_UNBLINDED AS
select LSMV_DRUG.ARI_REC_ID,BLINDED_PRODUCT_REC_ID,record_id
				from 
				(
				SELECT record_id,ARI_REC_ID,
					BLINDED_PRODUCT_REC_ID,CDC_OPERATION_TYPE,
					row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_DRUG where ARI_REC_ID in (select  ARI_REC_ID from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP)
				and record_id not in (select record_id from LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_drug')
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
                                   
drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET;                                   
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET as 
select * 
			FROM (select ARI_REC_ID,RECORD_ID,due_date,exit_time,fk_aim_record_id,wf_activity_name,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.lsmv_wf_tracker where 
					ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP)
					
				)
			WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')	;

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET;
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET as 
select * 
			FROM (select RECORD_ID,IM_REC_ID,LATE_BY_DAYS,due_date,EXIT_DATE,wf_activity_name,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
				FROM ${stage_db_name}.${stage_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO where 
					im_rec_id in (select fk_aim_record_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET)
					
				)
			WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')	;

drop table if exists ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET;
create table ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET as 
select * 
			FROM (select FK_ARI_REC_ID,RECORD_ID,CDC_OPERATION_TYPE
					, row_number() over (partition by RECORD_ID order by CDC_OPERATION_TIME desc) rank
					FROM ${stage_db_name}.${stage_schema_name}.LSMV_MESSAGE where 
					FK_ARI_REC_ID in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP)
					
				)
			WHERE RANK=1 and CDC_OPERATION_TYPE IN ('I','U')	;
	


      

 ALTER TABLE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_tmp ADD COLUMN DER_FIRST_LATE_FLAG TEXT(10);	



update
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_tmp
set
DER_FIRST_LATE_FLAG= 'Yes'
FROM (
SELECT ARI_REC_ID,WF_TRACK_REC_ID,LATE_BY_DAYS,DER_LATE_BY_DAYS,OUTPUT as DER_FIRST_LATE_FLAG from 
(
SELECT 
       COALESCE(SEQ_WF_ACTIVITY,WF_TRACK_REC_ID) AS WF_TRACK_REC_ID,
       LATENESS.LATE_BY_DAYS,
       WORKFLOW.DER_LATE_BY_DAYS,
       COALESCE(cast(LATENESS.LATE_BY_DAYS as varchar),WORKFLOW.DER_LATE_BY_DAYS) as OUTPUT,
       COALESCE(LATENESS.ARI_REC_ID,WORKFLOW.ARI_REC_ID) as ARI_REC_ID
FROM (select ARI_REC_ID,WF_TRACK_REC_ID,DER_LATE_BY_DAYS
		FROM 
		(
		select 
		LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID,
		lsmv_wf_tracker_SUBSET.RECORD_ID AS  WF_TRACK_REC_ID,
		CASE
			WHEN datediff(day, due_date,exit_time ) > 0 THEN CAST(datediff(day,due_date,exit_time) AS VARCHAR)
			WHEN datediff(day, due_date,exit_time ) <= 0 THEN '-'
			ELSE NULL END AS DER_LATE_BY_DAYS
		,DENSE_RANK() OVER (PARTITION BY LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID ORDER BY lsmv_wf_tracker_SUBSET.RECORD_ID) SEQ_WORKFLOW	
			
		FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET
			LEFT JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON lsmv_wf_tracker_SUBSET.fk_aim_record_id=LSMV_MESSAGE_SUBSET.record_id	
			where datediff(day, due_date,exit_time ) is not null
			) WHERE SEQ_WORKFLOW = 1
	) WORKFLOW
  LEFT OUTER JOIN (select ARI_REC_ID,SEQ_WF_ACTIVITY,LATE_BY_DAYS
FROM 
(
	
SELECT 
DISTINCT LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID
,LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.LATE_BY_DAYS
,wft.record_id SEQ_WF_ACTIVITY
,DENSE_RANK() OVER (PARTITION BY LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID ORDER BY wft.record_id) SEQ_LATENESS
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.IM_REC_ID=LSMV_MESSAGE_SUBSET.RECORD_ID
LEFT OUTER JOIN LSMV_WF_TRACKER_SUBSET wfT ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.im_rec_id = wft.fk_aim_record_id
and LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.wf_activity_name = wft.wf_activity_name
WHERE
LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID IS NOT NULL 
) WHERE SEQ_LATENESS = 1 and LATE_BY_DAYS is not null
	
)
LATENESS ON
WORKFLOW.ARI_REC_ID=LATENESS.ARI_REC_ID and WF_TRACK_REC_ID=SEQ_WF_ACTIVITY
) where OUTPUT<>'-'
) T
WHERE  LS_DB_CASE_WF_TRACKER_DER_TMP.ARI_REC_ID=T.ARI_REC_ID
and   LS_DB_CASE_WF_TRACKER_DER_TMP.seq_wf_tracker=T.WF_TRACK_REC_ID; 



update
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_tmp
set
DER_FIRST_LATE_FLAG= 'Yes'
FROM (
SELECT ARI_REC_ID,WF_TRACK_REC_ID,LATE_BY_DAYS,DER_LATE_BY_DAYS,OUTPUT as DER_FIRST_LATE_FLAG from 
(
SELECT 
       COALESCE(SEQ_WF_ACTIVITY,WF_TRACK_REC_ID) AS WF_TRACK_REC_ID,
       LATENESS.LATE_BY_DAYS,
       WORKFLOW.DER_LATE_BY_DAYS,
       COALESCE(cast(LATENESS.LATE_BY_DAYS as varchar),WORKFLOW.DER_LATE_BY_DAYS) as OUTPUT,
       COALESCE(LATENESS.ARI_REC_ID,WORKFLOW.ARI_REC_ID) as ARI_REC_ID
FROM (select ARI_REC_ID,WF_TRACK_REC_ID,DER_LATE_BY_DAYS
		FROM 
		(
		select 
		LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID,
		lsmv_wf_tracker_SUBSET.RECORD_ID AS  WF_TRACK_REC_ID,
		CASE
			WHEN datediff(day, due_date,exit_time ) > 0 THEN CAST(datediff(day,due_date,exit_time) AS VARCHAR)
			WHEN datediff(day, due_date,exit_time ) <= 0 THEN '-'
			ELSE NULL END AS DER_LATE_BY_DAYS
		,DENSE_RANK() OVER (PARTITION BY LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID ORDER BY lsmv_wf_tracker_SUBSET.RECORD_ID) SEQ_WORKFLOW	
			
		FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET
			LEFT JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON lsmv_wf_tracker_SUBSET.fk_aim_record_id=LSMV_MESSAGE_SUBSET.record_id	
			where datediff(day, due_date,exit_time ) is  null
			) WHERE SEQ_WORKFLOW = 1
	) WORKFLOW  LEFT OUTER JOIN (select ARI_REC_ID,SEQ_WF_ACTIVITY,LATE_BY_DAYS
FROM 
(
	
SELECT 
DISTINCT LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID
,LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.LATE_BY_DAYS
,wft.record_id SEQ_WF_ACTIVITY
,DENSE_RANK() OVER (PARTITION BY LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID ORDER BY wft.record_id) SEQ_LATENESS
FROM
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.IM_REC_ID=LSMV_MESSAGE_SUBSET.RECORD_ID
LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_WF_TRACKER_SUBSET wfT ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.im_rec_id = wft.fk_aim_record_id
and LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.wf_activity_name = wft.wf_activity_name
WHERE
LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID IS NOT NULL 
) WHERE SEQ_LATENESS = 1 and LATE_BY_DAYS is not null
	
) LATENESS ON WORKFLOW.ARI_REC_ID=LATENESS.ARI_REC_ID and WF_TRACK_REC_ID=SEQ_WF_ACTIVITY
) where OUTPUT<>'-'
) T
WHERE  LS_DB_CASE_WF_TRACKER_DER_TMP.ARI_REC_ID=T.ARI_REC_ID
and   LS_DB_CASE_WF_TRACKER_DER_TMP.seq_wf_tracker=T.WF_TRACK_REC_ID
and  (T.LATE_BY_DAYS IS NOT NULL and T.DER_LATE_BY_DAYS IS NULL)
; 




update
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_tmp
set
DER_FIRST_LATE_FLAG= NULL
FROM (
SELECT ARI_REC_ID,WF_TRACK_REC_ID,LATE_BY_DAYS,DER_LATE_BY_DAYS,OUTPUT as DER_FIRST_LATE_FLAG from 
(
SELECT 
       COALESCE(SEQ_WF_ACTIVITY,WF_TRACK_REC_ID) AS WF_TRACK_REC_ID,
       LATENESS.LATE_BY_DAYS,
       WORKFLOW.DER_LATE_BY_DAYS,
       cast(LATENESS.LATE_BY_DAYS as varchar) as OUTPUT,
       COALESCE(LATENESS.ARI_REC_ID,WORKFLOW.ARI_REC_ID) as ARI_REC_ID,LATENESS.EXIT_DATE,LATENESS.DUE_DATE
FROM (select ARI_REC_ID,WF_TRACK_REC_ID,DER_LATE_BY_DAYS
		FROM 
			(
			select 
				LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID,
				lsmv_wf_tracker_SUBSET.RECORD_ID AS  WF_TRACK_REC_ID,
				CASE
					WHEN datediff(day, due_date,exit_time ) > 0 THEN CAST(datediff(day,due_date,exit_time) AS VARCHAR)
					WHEN datediff(day, due_date,exit_time ) <= 0 THEN '-'
					ELSE NULL END AS DER_LATE_BY_DAYS
				,DENSE_RANK() OVER (PARTITION BY LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID ORDER BY lsmv_wf_tracker_SUBSET.RECORD_ID) SEQ_WORKFLOW	
					
				FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET
					LEFT JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON lsmv_wf_tracker_SUBSET.fk_aim_record_id=LSMV_MESSAGE_SUBSET.record_id	
					where datediff(day, due_date,exit_time ) is not null
			) WHERE SEQ_WORKFLOW = 1
	
	) WORKFLOW
  INNER JOIN (
SELECT SEQ_WF_ACTIVITY,LATE_BY_DAYS,ARI_REC_ID,EXIT_DATE,DUE_DATE
FROM
(
SELECT SEQ_WF_ACTIVITY,C.ARI_REC_ID,
DENSE_RANK() OVER (PARTITION BY C.ARI_REC_ID ORDER BY SEQ_WF_ACTIVITY) SEQ_LATENESS,
C.LATE_BY_DAYS,
C.EXIT_DATE,
C.DUE_DATE
FROM  
(select * from
(
SELECT 
DISTINCT LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID AS ARI_REC_ID
,wft.record_id SEQ_WF_ACTIVITY
,LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.LATE_BY_DAYS
,LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.EXIT_DATE
,LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.DUE_DATE
,count(LATE_BY_DAYS) over (partition by LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID order by wft.record_id) as CNT_LATE_BY_DAYS
from 
${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET  INNER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LSMV_MESSAGE_SUBSET ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.IM_REC_ID=LSMV_MESSAGE_SUBSET.RECORD_ID
LEFT OUTER JOIN ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.lsmv_wf_tracker_SUBSET wfT ON
LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.im_rec_id = wft.fk_aim_record_id
and LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.wf_activity_name = wft.wf_activity_name
WHERE
LSMV_MESSAGE_SUBSET.FK_ARI_REC_ID IS NOT NULL 
and (LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.EXIT_DATE IS NOT NULL and LSMV_LATE_CASE_ACTIVITY_INFO_SUBSET.DUE_DATE IS NOT NULL) and LATE_BY_DAYS IS NULL
) where CNT_LATE_BY_DAYS=0
)C
) WHERE SEQ_LATENESS=1
)
LATENESS ON
WORKFLOW.ARI_REC_ID=LATENESS.ARI_REC_ID
where (LATENESS.LATE_BY_DAYS IS NULL and LATENESS.EXIT_DATE IS NOT NULL) and LATE_BY_DAYS IS NULL
)
) 
T
WHERE LS_DB_CASE_WF_TRACKER_DER_TMP.ARI_REC_ID=T.ARI_REC_ID
and   LS_DB_CASE_WF_TRACKER_DER_TMP.seq_wf_tracker=T.WF_TRACK_REC_ID;



UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP)
where target_table_name='LS_DB_CASE_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL 
                  where target_table_name='LS_DB_CASE_WF_TRACKER_DER'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER   
SET LS_DB_CASE_WF_TRACKER_DER.ARI_REC_ID = LS_DB_CASE_WF_TRACKER_DER_TMP.ARI_REC_ID,
LS_DB_CASE_WF_TRACKER_DER.SEQ_WF_TRACKER = LS_DB_CASE_WF_TRACKER_DER_TMP.SEQ_WF_TRACKER,
LS_DB_CASE_WF_TRACKER_DER.PROCESSING_DT = LS_DB_CASE_WF_TRACKER_DER_TMP.PROCESSING_DT,
LS_DB_CASE_WF_TRACKER_DER.expiry_date    =LS_DB_CASE_WF_TRACKER_DER_TMP.expiry_date,
LS_DB_CASE_WF_TRACKER_DER.date_modified    =LS_DB_CASE_WF_TRACKER_DER_TMP.date_modified,
LS_DB_CASE_WF_TRACKER_DER.load_ts    =LS_DB_CASE_WF_TRACKER_DER_TMP.load_ts,
LS_DB_CASE_WF_TRACKER_DER.DER_FIRST_LATE_FLAG     =LS_DB_CASE_WF_TRACKER_DER_TMP.DER_FIRST_LATE_FLAG
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP 
WHERE 	LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID = LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASE_WF_TRACKER_DER_TMP.PROCESSING_DT = LS_DB_CASE_WF_TRACKER_DER.PROCESSING_DT,1=1)
           AND MD5(NVL(LS_DB_CASE_WF_TRACKER_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_WF_TRACKER_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
           and LS_DB_CASE_WF_TRACKER_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
           ;
           
           
UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER 
SET EXPIRY_DATE=CURRENT_DATE()
from (
select LS_DB_CASE_WF_TRACKER_DER.ari_rec_id ,LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP 
ON LS_DB_CASE_WF_TRACKER_DER.ari_rec_id=LS_DB_CASE_WF_TRACKER_DER_TMP.ari_rec_id
AND LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID = LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID 
where LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_WF_TRACKER_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
and LS_DB_CASE_WF_TRACKER_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP )
) TMP where LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID=TMP.INTEGRATION_ID
AND LS_DB_CASE_WF_TRACKER_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;



DELETE FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER TGT 
WHERE EXISTS  (SELECT 1 FROM 
  (
    select LS_DB_CASE_WF_TRACKER_DER.ari_rec_id ,LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID
    FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER left join ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP 
    ON LS_DB_CASE_WF_TRACKER_DER.ari_rec_id=LS_DB_CASE_WF_TRACKER_DER_TMP.ari_rec_id
    AND LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID = LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID 
    where LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID  is null AND LS_DB_CASE_WF_TRACKER_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
    and  LS_DB_CASE_WF_TRACKER_DER.ari_rec_id in (select ari_rec_id from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP )
) TMP where TGT.INTEGRATION_ID=TMP.INTEGRATION_ID
 )
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
;

   
     


INSERT INTO ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER
( ARI_REC_ID    ,
SEQ_WF_TRACKER       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_FIRST_LATE_FLAG
)
SELECT 
  ARI_REC_ID    ,
SEQ_WF_TRACKER       ,
processing_dt ,
expiry_date   ,
load_ts,  
date_modified,
INTEGRATION_ID,
DER_FIRST_LATE_FLAG                              
FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP TMP where not exists (select 1 from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER TGT
where TMP.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END = TGT.INTEGRATION_ID||'-'||CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') THEN TGT.PROCESSING_DT ELSE '9999-12-31' END
AND TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
);


DELETE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER TGT
WHERE  ( SEQ_WF_TRACKER  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_tracker') )
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');



UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP 
WHERE 	TO_DATE(LS_DB_CASE_WF_TRACKER_DER.PROCESSING_DT) < TO_DATE(LS_DB_CASE_WF_TRACKER_DER_TMP.PROCESSING_DT)
AND LS_DB_CASE_WF_TRACKER_DER.INTEGRATION_ID = LS_DB_CASE_WF_TRACKER_DER_TMP.INTEGRATION_ID
AND LS_DB_CASE_WF_TRACKER_DER.SEQ_WF_TRACKER = LS_DB_CASE_WF_TRACKER_DER_TMP.SEQ_WF_TRACKER
AND LS_DB_CASE_WF_TRACKER_DER.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
 AND MD5(NVL(LS_DB_CASE_WF_TRACKER_DER.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))) <> MD5(NVL(LS_DB_CASE_WF_TRACKER_DER_TMP.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM')))
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


UPDATE  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( seq_wf_tracker  in (SELECT RECORD_ID FROM  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_DELETION_TMP  WHERE TABLE_NAME='lsmv_wf_tracker')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER where LOAD_TS= (select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP limit 1)),
LOAD_TS=(select LOAD_TS from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CASE_WF_TRACKER_DER_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASE_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASE_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASE_WF_TRACKER_DER Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASE_WF_TRACKER_DER'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASE_WF_TRACKER_DER not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
