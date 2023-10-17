
-- USE SCHEMA $$TGT_DB_NAME.$$LSDB_TRANSFM;
CREATE OR REPLACE PROCEDURE $$TGT_DB_NAME.$$LSDB_TRANSFM.PRC_LS_DB_CASUALTY_ASSESSMENT()
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
SELECT (select nvl(max(row_wid)+1,1) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASUALTY_ASSESSMENT'),
	'LSRA','Case','LS_DB_CASUALTY_ASSESSMENT',null,:CURRENT_TS_VAR,null,null,null,null,null,'In Progress',null;


UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= nvl((SELECT LOAD_START_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL WHERE TARGET_TABLE_NAME='LS_DB_CASUALTY_ASSESSMENT' AND LOAD_STATUS = 'Completed' ORDER BY ROW_WID DESC limit 1),to_timestamp('1900-01-01','YYYY-MM-DD'))
WHERE TARGET_TABLE_NAME = 'LS_DB_CASUALTY_ASSESSMENT'
AND PARAM_NAME='CDC_EXTRACT_TS_LB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG
SET PARAM_VALUE= :CURRENT_TS_VAR
WHERE TARGET_TABLE_NAME = 'LS_DB_CASUALTY_ASSESSMENT'
AND PARAM_NAME='CDC_EXTRACT_TS_UB'
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
;





DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASUALTY_ASSESSMENT_DELETION_TMP;
CREATE TEMPORARY TABLE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASUALTY_ASSESSMENT_DELETION_TMP  As select RECORD_ID,'lsmv_casualty_assessment' AS TABLE_NAME FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_casualty_assessment WHERE CDC_OPERATION_TYPE IN ('D') ;
DROP TABLE IF EXISTS $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP;
CREATE TEMPORARY TABLE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP  AS  WITH CD_WITH AS (select CD_ID,CD,DE,LN from 
(
SELECT distinct LSMV_CODELIST_NAME.CODELIST_ID CD_ID,LSMV_CODELIST_CODE.CODE CD,LSMV_CODELIST_DECODE.DECODE DE,LSMV_CODELIST_DECODE.LANGUAGE_CODE LN 
,row_number() over(partition by LSMV_CODELIST_NAME.CODELIST_ID,LSMV_CODELIST_CODE.CODE,LSMV_CODELIST_DECODE.LANGUAGE_CODE 
                   order by LSMV_CODELIST_NAME.CDC_OPERATION_TIME  DESC,LSMV_CODELIST_CODE.CDC_OPERATION_TIME DESC,LSMV_CODELIST_DECODE.CDC_OPERATION_TIME DESC) rank


                                                                                      FROM
                                                                                      (
                                                                                                     SELECT RECORD_ID,CODELIST_ID,CDC_OPERATION_TIME,ROW_NUMBER() OVER ( PARTITION BY RECORD_ID ORDER BY CDC_OPERATION_TIME DESC) RANK
                                                                                                     FROM $$STG_DB_NAME.$$LSDB_RPL.LSMV_CODELIST_NAME WHERE CODELIST_ID IN ('1002','10076','10077','9055','9062','9641','9930','9931','9943')
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
 
select DISTINCT RECORD_ID record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_casualty_assessment WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASUALTY_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASUALTY_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
UNION 

select DISTINCT 0 record_id, 0 common_parent_key,  ARI_REC_ID ARI_REC_ID   FROM $$STG_DB_NAME.$$LSDB_RPL.lsmv_casualty_assessment WHERE CDC_OPERATION_TIME >(SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASUALTY_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_LB')
	AND CDC_OPERATION_TIME<= (SELECT PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LS_DB_CASUALTY_ASSESSMENT' AND PARAM_NAME='CDC_EXTRACT_TS_UB')
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

), lsmv_casualty_assessment_SUBSET AS 
(
select * from 
    (SELECT  
    ari_rec_id  ari_rec_id,assess_method_kr  assess_method_kr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9930' AND CD=CAST(assess_method_kr AS VARCHAR(100)) )assess_method_kr_de_ml , assess_relship  assess_relship,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='1002' AND CD=CAST(assess_relship AS VARCHAR(100)) )assess_relship_de_ml , auto_populated_causality  auto_populated_causality,causality_source  causality_source,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9055' AND CD=CAST(causality_source AS VARCHAR(100)) )causality_source_de_ml , causalitysource_sf  causalitysource_sf,date_created  date_created,date_modified  date_modified,drugresult  drugresult,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9062' AND CD=CAST(drugresult AS VARCHAR(100)) )drugresult_de_ml , fk_react_reltdns  fk_react_reltdns,is_added_from_krjp  is_added_from_krjp,is_company_causality  is_company_causality,is_reporter_causality  is_reporter_causality,krctresult_kr  krctresult_kr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9943' AND CD=CAST(krctresult_kr AS VARCHAR(100)) )krctresult_kr_de_ml , mah_init_rep_or_eval_res_cn  mah_init_rep_or_eval_res_cn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10077' AND CD=CAST(mah_init_rep_or_eval_res_cn AS VARCHAR(100)) )mah_init_rep_or_eval_res_cn_de_ml , mah_init_rep_or_eval_src_cn  mah_init_rep_or_eval_src_cn,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='10076' AND CD=CAST(mah_init_rep_or_eval_src_cn AS VARCHAR(100)) )mah_init_rep_or_eval_src_cn_de_ml , methods  methods,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9641' AND CD=CAST(methods AS VARCHAR(100)) )methods_de_ml , methods_sf  methods_sf,not_reportable_ct  not_reportable_ct,not_reportable_pm  not_reportable_pm,record_id  record_id,result_sf  result_sf,spr_id  spr_id,user_created  user_created,user_modified  user_modified,whoumcresult_kr  whoumcresult_kr,(SELECT OBJECT_AGG(CAST(LN AS VARCHAR(100)), CAST(DE AS VARIANT)) FROM CD_WITH WHERE CD_ID ='9931' AND CD=CAST(whoumcresult_kr AS VARCHAR(100)) )whoumcresult_kr_de_ml , whoumcresult_kr_nf  whoumcresult_kr_nf,row_number() OVER ( PARTITION BY RECORD_ID,RECORD_ID ORDER BY CDC_OPERATION_TIME DESC ) REC_RANK
FROM 
$$STG_DB_NAME.$$LSDB_RPL.lsmv_casualty_assessment
 WHERE  RECORD_ID IN (SELECT record_id FROM LSMV_CASE_NO_SUBSET) 
 AND RECORD_ID not in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASUALTY_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_casualty_assessment')
  ) where REC_RANK=1 )
   SELECT DISTINCT  to_date(GREATEST(
NVL(lsmv_casualty_assessment_SUBSET.DATE_MODIFIED ,TO_DATE('1900-01-01','YYYY-DD-MM'))
))
PROCESSING_DT 
,LSMV_COMMON_COLUMN_SUBSET.RECEIPT_ID ,LSMV_COMMON_COLUMN_SUBSET.CASE_NO ,LSMV_COMMON_COLUMN_SUBSET.AER_VERSION_NO  CASE_VERSION ,LSMV_COMMON_COLUMN_SUBSET.VERSION_NO,TO_DATE('9999-31-12','YYYY-DD-MM') EXPIRY_DATE	,lsmv_casualty_assessment_SUBSET.USER_CREATED CREATED_BY,lsmv_casualty_assessment_SUBSET.DATE_CREATED CREATED_DT,:CURRENT_TS_VAR LOAD_TS,lsmv_casualty_assessment_SUBSET.whoumcresult_kr_nf  ,lsmv_casualty_assessment_SUBSET.whoumcresult_kr_de_ml  ,lsmv_casualty_assessment_SUBSET.whoumcresult_kr  ,lsmv_casualty_assessment_SUBSET.user_modified  ,lsmv_casualty_assessment_SUBSET.user_created  ,lsmv_casualty_assessment_SUBSET.spr_id  ,lsmv_casualty_assessment_SUBSET.result_sf  ,lsmv_casualty_assessment_SUBSET.record_id  ,lsmv_casualty_assessment_SUBSET.not_reportable_pm  ,lsmv_casualty_assessment_SUBSET.not_reportable_ct  ,lsmv_casualty_assessment_SUBSET.methods_sf  ,lsmv_casualty_assessment_SUBSET.methods_de_ml  ,lsmv_casualty_assessment_SUBSET.methods  ,lsmv_casualty_assessment_SUBSET.mah_init_rep_or_eval_src_cn_de_ml  ,lsmv_casualty_assessment_SUBSET.mah_init_rep_or_eval_src_cn  ,lsmv_casualty_assessment_SUBSET.mah_init_rep_or_eval_res_cn_de_ml  ,lsmv_casualty_assessment_SUBSET.mah_init_rep_or_eval_res_cn  ,lsmv_casualty_assessment_SUBSET.krctresult_kr_de_ml  ,lsmv_casualty_assessment_SUBSET.krctresult_kr  ,lsmv_casualty_assessment_SUBSET.is_reporter_causality  ,lsmv_casualty_assessment_SUBSET.is_company_causality  ,lsmv_casualty_assessment_SUBSET.is_added_from_krjp  ,lsmv_casualty_assessment_SUBSET.fk_react_reltdns  ,lsmv_casualty_assessment_SUBSET.drugresult_de_ml  ,lsmv_casualty_assessment_SUBSET.drugresult  ,lsmv_casualty_assessment_SUBSET.date_modified  ,lsmv_casualty_assessment_SUBSET.date_created  ,lsmv_casualty_assessment_SUBSET.causalitysource_sf  ,lsmv_casualty_assessment_SUBSET.causality_source_de_ml  ,lsmv_casualty_assessment_SUBSET.causality_source  ,lsmv_casualty_assessment_SUBSET.auto_populated_causality  ,lsmv_casualty_assessment_SUBSET.assess_relship_de_ml  ,lsmv_casualty_assessment_SUBSET.assess_relship  ,lsmv_casualty_assessment_SUBSET.assess_method_kr_de_ml  ,lsmv_casualty_assessment_SUBSET.assess_method_kr  ,lsmv_casualty_assessment_SUBSET.ari_rec_id ,CONCAT(NVL(lsmv_casualty_assessment_SUBSET.RECORD_ID,-1)) INTEGRATION_ID FROM lsmv_casualty_assessment_SUBSET  LEFT JOIN LSMV_COMMON_COLUMN_SUBSET ON lsmv_casualty_assessment_SUBSET.RECORD_ID  =  LSMV_COMMON_COLUMN_SUBSET.record_id  WHERE 1=1  
;



UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET REC_READ_CNT = (select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP)
where target_table_name='LS_DB_CASUALTY_ASSESSMENT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASUALTY_ASSESSMENT'
					and LOAD_STATUS = 'In Progress') 
; 

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT   
SET LS_DB_CASUALTY_ASSESSMENT.whoumcresult_kr_nf = LS_DB_CASUALTY_ASSESSMENT_TMP.whoumcresult_kr_nf,LS_DB_CASUALTY_ASSESSMENT.whoumcresult_kr_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.whoumcresult_kr_de_ml,LS_DB_CASUALTY_ASSESSMENT.whoumcresult_kr = LS_DB_CASUALTY_ASSESSMENT_TMP.whoumcresult_kr,LS_DB_CASUALTY_ASSESSMENT.user_modified = LS_DB_CASUALTY_ASSESSMENT_TMP.user_modified,LS_DB_CASUALTY_ASSESSMENT.user_created = LS_DB_CASUALTY_ASSESSMENT_TMP.user_created,LS_DB_CASUALTY_ASSESSMENT.spr_id = LS_DB_CASUALTY_ASSESSMENT_TMP.spr_id,LS_DB_CASUALTY_ASSESSMENT.result_sf = LS_DB_CASUALTY_ASSESSMENT_TMP.result_sf,LS_DB_CASUALTY_ASSESSMENT.record_id = LS_DB_CASUALTY_ASSESSMENT_TMP.record_id,LS_DB_CASUALTY_ASSESSMENT.not_reportable_pm = LS_DB_CASUALTY_ASSESSMENT_TMP.not_reportable_pm,LS_DB_CASUALTY_ASSESSMENT.not_reportable_ct = LS_DB_CASUALTY_ASSESSMENT_TMP.not_reportable_ct,LS_DB_CASUALTY_ASSESSMENT.methods_sf = LS_DB_CASUALTY_ASSESSMENT_TMP.methods_sf,LS_DB_CASUALTY_ASSESSMENT.methods_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.methods_de_ml,LS_DB_CASUALTY_ASSESSMENT.methods = LS_DB_CASUALTY_ASSESSMENT_TMP.methods,LS_DB_CASUALTY_ASSESSMENT.mah_init_rep_or_eval_src_cn_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.mah_init_rep_or_eval_src_cn_de_ml,LS_DB_CASUALTY_ASSESSMENT.mah_init_rep_or_eval_src_cn = LS_DB_CASUALTY_ASSESSMENT_TMP.mah_init_rep_or_eval_src_cn,LS_DB_CASUALTY_ASSESSMENT.mah_init_rep_or_eval_res_cn_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.mah_init_rep_or_eval_res_cn_de_ml,LS_DB_CASUALTY_ASSESSMENT.mah_init_rep_or_eval_res_cn = LS_DB_CASUALTY_ASSESSMENT_TMP.mah_init_rep_or_eval_res_cn,LS_DB_CASUALTY_ASSESSMENT.krctresult_kr_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.krctresult_kr_de_ml,LS_DB_CASUALTY_ASSESSMENT.krctresult_kr = LS_DB_CASUALTY_ASSESSMENT_TMP.krctresult_kr,LS_DB_CASUALTY_ASSESSMENT.is_reporter_causality = LS_DB_CASUALTY_ASSESSMENT_TMP.is_reporter_causality,LS_DB_CASUALTY_ASSESSMENT.is_company_causality = LS_DB_CASUALTY_ASSESSMENT_TMP.is_company_causality,LS_DB_CASUALTY_ASSESSMENT.is_added_from_krjp = LS_DB_CASUALTY_ASSESSMENT_TMP.is_added_from_krjp,LS_DB_CASUALTY_ASSESSMENT.fk_react_reltdns = LS_DB_CASUALTY_ASSESSMENT_TMP.fk_react_reltdns,LS_DB_CASUALTY_ASSESSMENT.drugresult_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.drugresult_de_ml,LS_DB_CASUALTY_ASSESSMENT.drugresult = LS_DB_CASUALTY_ASSESSMENT_TMP.drugresult,LS_DB_CASUALTY_ASSESSMENT.date_modified = LS_DB_CASUALTY_ASSESSMENT_TMP.date_modified,LS_DB_CASUALTY_ASSESSMENT.date_created = LS_DB_CASUALTY_ASSESSMENT_TMP.date_created,LS_DB_CASUALTY_ASSESSMENT.causalitysource_sf = LS_DB_CASUALTY_ASSESSMENT_TMP.causalitysource_sf,LS_DB_CASUALTY_ASSESSMENT.causality_source_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.causality_source_de_ml,LS_DB_CASUALTY_ASSESSMENT.causality_source = LS_DB_CASUALTY_ASSESSMENT_TMP.causality_source,LS_DB_CASUALTY_ASSESSMENT.auto_populated_causality = LS_DB_CASUALTY_ASSESSMENT_TMP.auto_populated_causality,LS_DB_CASUALTY_ASSESSMENT.assess_relship_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.assess_relship_de_ml,LS_DB_CASUALTY_ASSESSMENT.assess_relship = LS_DB_CASUALTY_ASSESSMENT_TMP.assess_relship,LS_DB_CASUALTY_ASSESSMENT.assess_method_kr_de_ml = LS_DB_CASUALTY_ASSESSMENT_TMP.assess_method_kr_de_ml,LS_DB_CASUALTY_ASSESSMENT.assess_method_kr = LS_DB_CASUALTY_ASSESSMENT_TMP.assess_method_kr,LS_DB_CASUALTY_ASSESSMENT.ari_rec_id = LS_DB_CASUALTY_ASSESSMENT_TMP.ari_rec_id,
LS_DB_CASUALTY_ASSESSMENT.PROCESSING_DT = LS_DB_CASUALTY_ASSESSMENT_TMP.PROCESSING_DT ,
LS_DB_CASUALTY_ASSESSMENT.receipt_id     =LS_DB_CASUALTY_ASSESSMENT_TMP.receipt_id        ,
LS_DB_CASUALTY_ASSESSMENT.case_no        =LS_DB_CASUALTY_ASSESSMENT_TMP.case_no           ,
LS_DB_CASUALTY_ASSESSMENT.case_version   =LS_DB_CASUALTY_ASSESSMENT_TMP.case_version      ,
LS_DB_CASUALTY_ASSESSMENT.version_no     =LS_DB_CASUALTY_ASSESSMENT_TMP.version_no        ,
LS_DB_CASUALTY_ASSESSMENT.expiry_date    =LS_DB_CASUALTY_ASSESSMENT_TMP.expiry_date       ,
LS_DB_CASUALTY_ASSESSMENT.load_ts        =LS_DB_CASUALTY_ASSESSMENT_TMP.load_ts         
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP 
WHERE 	LS_DB_CASUALTY_ASSESSMENT.INTEGRATION_ID = LS_DB_CASUALTY_ASSESSMENT_TMP.INTEGRATION_ID
AND DECODE('YES',(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME='HISTORY_CONTROL'),
           LS_DB_CASUALTY_ASSESSMENT_TMP.PROCESSING_DT = LS_DB_CASUALTY_ASSESSMENT.PROCESSING_DT,1=1);


INSERT INTO $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT
(
receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,whoumcresult_kr_nf,
whoumcresult_kr_de_ml,
whoumcresult_kr,
user_modified,
user_created,
spr_id,
result_sf,
record_id,
not_reportable_pm,
not_reportable_ct,
methods_sf,
methods_de_ml,
methods,
mah_init_rep_or_eval_src_cn_de_ml,
mah_init_rep_or_eval_src_cn,
mah_init_rep_or_eval_res_cn_de_ml,
mah_init_rep_or_eval_res_cn,
krctresult_kr_de_ml,
krctresult_kr,
is_reporter_causality,
is_company_causality,
is_added_from_krjp,
fk_react_reltdns,
drugresult_de_ml,
drugresult,
date_modified,
date_created,
causalitysource_sf,
causality_source_de_ml,
causality_source,
auto_populated_causality,
assess_relship_de_ml,
assess_relship,
assess_method_kr_de_ml,
assess_method_kr,
ari_rec_id)
SELECT 

receipt_id    ,
case_no       ,
case_version  ,
processing_dt ,
version_no    ,
expiry_date   ,
load_ts       ,
integration_id ,whoumcresult_kr_nf,
whoumcresult_kr_de_ml,
whoumcresult_kr,
user_modified,
user_created,
spr_id,
result_sf,
record_id,
not_reportable_pm,
not_reportable_ct,
methods_sf,
methods_de_ml,
methods,
mah_init_rep_or_eval_src_cn_de_ml,
mah_init_rep_or_eval_src_cn,
mah_init_rep_or_eval_res_cn_de_ml,
mah_init_rep_or_eval_res_cn,
krctresult_kr_de_ml,
krctresult_kr,
is_reporter_causality,
is_company_causality,
is_added_from_krjp,
fk_react_reltdns,
drugresult_de_ml,
drugresult,
date_modified,
date_created,
causalitysource_sf,
causality_source_de_ml,
causality_source,
auto_populated_causality,
assess_relship_de_ml,
assess_relship,
assess_method_kr_de_ml,
assess_method_kr,
ari_rec_id
FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP TMP WHERE CONCAT(TMP.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE 
														FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG 
														WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
                                                        THEN TMP.PROCESSING_DT ELSE '9999-12-31' END ) 
									NOT IN (SELECT CONCAT(TGT.INTEGRATION_ID,'||',CASE WHEN 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY') 
																				THEN TGT.PROCESSING_DT ELSE '9999-12-31' END) FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT TGT)
                                                                                ; 
COMMIT;



UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT 
SET EXPIRY_DATE=CURRENT_DATE()-1
FROM 	$$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP 
WHERE 	TO_DATE(LS_DB_CASUALTY_ASSESSMENT.PROCESSING_DT) < TO_DATE(LS_DB_CASUALTY_ASSESSMENT_TMP.PROCESSING_DT)
AND LS_DB_CASUALTY_ASSESSMENT.INTEGRATION_ID = LS_DB_CASUALTY_ASSESSMENT_TMP.INTEGRATION_ID
AND LS_DB_CASUALTY_ASSESSMENT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY')	
;


DELETE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT TGT
WHERE  ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASUALTY_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_casualty_assessment')
)
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'NO'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' AND PARAM_NAME='KEEP_HISTORY');


UPDATE  $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT TGT
SET 	TGT.EXPIRY_DATE=CURRENT_DATE()
WHERE 	 ( record_id  in (SELECT RECORD_ID FROM  $$TGT_DB_NAME.$$LSDB_TRANSFM.LSMV_CASUALTY_ASSESSMENT_DELETION_TMP  WHERE TABLE_NAME='lsmv_casualty_assessment')
)
AND 	TGT.EXPIRY_DATE = TO_DATE('9999-31-12','YYYY-DD-MM')
--AND 'I' = (SELECT PARAM_NAME FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='LOAD_CONTROL')
AND 'YES'=(SELECT CHAR_PARAM_VALUE FROM $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_ETL_CONFIG WHERE TARGET_TABLE_NAME ='HISTORY_CONTROL' 
           AND PARAM_NAME='KEEP_HISTORY');

UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL
SET LOAD_END_TS = current_timestamp,
REC_PROCESSED_CNT=(select count(LOAD_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT where LOAD_TS= (select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP limit 1)),
LOAD_TS=(select LOAD_TS from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_CASUALTY_ASSESSMENT_TMP limit 1),
LOAD_STATUS='Completed'
where target_table_name='LS_DB_CASUALTY_ASSESSMENT'
and LOAD_STATUS = 'In Progress'
and LOAD_START_TS=(select max(LOAD_START_TS) from $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL where target_table_name='LS_DB_CASUALTY_ASSESSMENT'
and LOAD_STATUS = 'In Progress') ;
           
  RETURN 'LS_DB_CASUALTY_ASSESSMENT Load completed';

EXCEPTION
  WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
UPDATE $$TGT_DB_NAME.$$LSDB_TRANSFM.LS_DB_DATA_PROCESSING_DTL_TBL set ERROR_DETAILS=:line, LOAD_STATUS = 'Error' WHERE target_table_name='LS_DB_CASUALTY_ASSESSMENT'
and LOAD_STATUS = 'In Progress'
;

  RETURN 'LS_DB_CASUALTY_ASSESSMENT not loaded due to etl error. please check LS_DB_DATA_PROCESSING_DTL_TBL for more details';
END;
$$
;



           
