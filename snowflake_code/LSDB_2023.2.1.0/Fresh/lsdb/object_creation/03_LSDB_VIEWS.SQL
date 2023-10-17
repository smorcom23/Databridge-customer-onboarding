CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL_ATC_CODES AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_ATC_CODES;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL_DRUG_ATC_RELATION AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_DRUG_ATC_RELATION;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL_DRUG_INGREDIENT AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_DRUG_INGREDIENT;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL_MFR AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_MFR;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_DRL_SUBSTANCE AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_DRL_SUBSTANCE;

-- CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SDG_LIST AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_SDG_LIST;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_CC_SMQ_CMQ_LIST;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_HLGT_PREF_TERM AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLGT_PREF_TERM;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MEDDRA_ICD AS select 
A.BK_MEDDRA_ICD_WID
,A.PROCESSING_DT
,A.EXPIRY_DATE
,A.CREATED_BY
,A.CREATED_DT
,A.LOAD_TS
,A.INTEGRATION_ID
,A.MDHI_RECORD_ID
,A.LOWLE_RECORD_ID
,A.ICD_CODE
,A.LLT_CODE
,A.LLT_NAME
,A.PT_CODE
,A.HLT_CODE
,A.HLGT_CODE
,A.SOC_CODE
,A.PT_NAME
,A.HLT_NAME
,A.HLGT_NAME
,A.SOC_NAME
,A.MEDDRA_VERSION
,A.LLT_NAME_OTHER
,A.IS_MEDDRA_FLAG
,A.IS_ICD_FLAG
,A.LANGUAGE_CODE
,A.INTERNATIONAL_SOC_ORDER
,A.SOC_ORDER
,A.PRIMARY_SOC_NAME
,A.INTERNATIONAL_PRIMARY_ORDER
,A.HLGT_NAME_OTHER
,A.HLT_NAME_OTHER
,A.PT_NAME_OTHER
,A.SOC_NAME_OTHER
,A.PT_SOC_CODE
,A.PRIMARY_SOC_FG
,B.SK_LANGUAGE_WID as LANGUAGE_WID
FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MEDDRA_ICD A Join (select SK_LANGUAGE_WID,
	CODE,LANGUAGE_NAME from ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANGUAGE ) B on A.language_code=b.code;



CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_HLT_PREF_TERM AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_HLT_PREF_TERM;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LOW_LEVEL_TERM AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_LOW_LEVEL_TERM;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_MD_HIERARCHY AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_MD_HIERARCHY;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PREF_TERM AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_PREF_TERM;

CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SOC_TERM AS SELECT * FROM ${cntrl_transfm_db_name}.${cntrl_transfm_schema_name}.LS_DB_SOC_TERM;


CREATE OR REPLACE VIEW ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO_C  AS SELECT * FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_CC_SMQ_CMQ_TERMS_INFO;


CREATE OR REPLACE VIEW ${stage_db_name}.${stage_schema_name}.LSMV_CC_SMQ_CMQ_LIST_C AS SELECT * FROM 
${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_CC_SMQ_CMQ_LIST;


 
 CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_V_PARTNER AS
    SELECT
        NAME AS NAME
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_PARTNER
    UNION
    SELECT
        ACC_ACCOUNT_NAME AS NAME
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_ACCOUNTS_INFO
    UNION
    SELECT
        RECPT_ACCOUNT_NAME AS NAME
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_SAFETY_MASTER;



CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_v_initial_unit AS
    SELECT
        '02'       AS e2b_sender_receiver,
        partner_id unit_code,
        name       AS name,
        country,
        type,
        type_de_ml type_de_ml,
        record_id,
        '001'      language_code,
        authority  AS authority,
        sender_id,
        '0'        is_authority_flag,
        '1'        is_company_unit_flag
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_partner
    UNION
    SELECT
        '01'                   AS e2b_sender_receiver,
        acc_account_id         unit_code,
        acc_account_name       AS name,
        acc_country            country,
        acc_account_type       AS type,
        acc_account_type_de_ml type_de_ml,
        acc_record_id          record_id,
        '001'                  language_code,
        acc_health_authority   health_authority,
        NULL                   AS sender_id,
        '0'                    is_authority_flag,
        '1'                    is_company_unit_flag
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_accounts_info
    UNION
    SELECT
        NULL        e2b_sender_receiver,
        cdce_code   unit_code,
        CDNM_CODELIST_NAME name,
        NULL        country,
        NULL        type,
        NULL        type_de_ml,
        NULL        record_id,
        CASE
            WHEN cdde_language_code = 'en' THEN
                '001'
            WHEN cdde_language_code = 'de' THEN
                '002'
            WHEN cdde_language_code = 'es' THEN
                '003'
            WHEN cdde_language_code = 'fr' THEN
                '005'
            WHEN cdde_language_code = 'it' THEN
                '007'
            WHEN cdde_language_code = 'ja' THEN
                '008'
            WHEN cdde_language_code = 'zh' THEN
                '009'
            WHEN cdde_language_code = 'po' THEN
                '010'
        END         language_code,
        CDDE_DECODE        health_authority,
        NULL        AS sender_id,
        '1'         is_authority_flag,
        '0'         is_company_unit_flag
    FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_codelist_info
    WHERE
        cdnm_codelist_id = 9638;

		
		
		
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_SOC_TERM 		AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_SOC_TERM;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_DRL_MFR 		AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_DRL_MFR;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_DRL_SUBSTANCE	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_DRL_SUBSTANCE;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_HLGT_HLT_COMP 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_HLGT_HLT_COMP;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_HLGT_PREF_TERM 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_HLGT_PREF_TERM;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_HLT_PREF_COMP 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_HLT_PREF_COMP;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_HLT_PREF_TERM 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_HLT_PREF_TERM;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_LOW_LEVEL_TERM 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_LOW_LEVEL_TERM;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_MD_HIERARCHY 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_MD_HIERARCHY;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_PREF_TERM 		AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_PREF_TERM;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_DRL_DRUG_INGREDIENT 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_DRL_DRUG_INGREDIENT;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_DRL_DRUG_ATC_RELATION	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_DRL_DRUG_ATC_RELATION;
CREATE or REPLACE VIEW   ${stage_db_name}.${stage_schema_name}.LSMV_DRL_ATC_CODES 	AS SELECT * FROM    ${stage_cntrl_db_name}.${stage_cntrl_schema_name}.LSMV_DRL_ATC_CODES;
		


CREATE OR REPLACE  MATERIALIZED VIEW  ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.MAT_LS_DB_LANG_REDUCTED
    AS
	SELECT 
	LS_DB_LANG_REDUCTED.FK_ASR_REC_ID,
	LS_DB_LANG_REDUCTED.ENTITY_RECORD_ID,
		CASE
			WHEN LANGUAGE_CODE='127' THEN '001'
			WHEN LANGUAGE_CODE='110' THEN '002'
			WHEN LANGUAGE_CODE='404' THEN '003'
			WHEN LANGUAGE_CODE='142' THEN '005'
			WHEN LANGUAGE_CODE='205' THEN '007'
			WHEN LANGUAGE_CODE='208' THEN '008'
			WHEN LANGUAGE_CODE='81' THEN '009'
			WHEN LANGUAGE_CODE='351' THEN '010'
		ELSE LANGUAGE_CODE
		END LANGUAGE_CODE,
	CASE WHEN FIELD_ID='112643' AND CONTEXT_NAME= 'TEST_COMMENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END TEST_COMMENT,
CASE WHEN FIELD_ID='956119' AND CONTEXT_NAME= 'PATIENT_RESULT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PATIENT_RESULT,
CASE WHEN FIELD_ID='956199' AND CONTEXT_NAME= 'THERPHY_END_DATE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END THERPHY_END_DATE,
CASE WHEN FIELD_ID='956148' AND CONTEXT_NAME= 'ADDITIONAL_MANUFACTURE_NARRATIVE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END ADDITIONAL_MANUFACTURE_NARRATIVE,
CASE WHEN FIELD_ID='956184' AND CONTEXT_NAME= 'E2BCAUSALITY_SOURCESF' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END E2BCAUSALITY_SOURCESF,
CASE WHEN FIELD_ID='956144' AND CONTEXT_NAME= 'NARRATIVE_REMARKS_3' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_REMARKS_3,
CASE WHEN FIELD_ID='956146' AND CONTEXT_NAME= 'NARRATIVE_REMARKS_4' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_REMARKS_4,
CASE WHEN FIELD_ID='106940' AND CONTEXT_NAME= 'ILLNESS_AT_TIME_OF_VACCINATION' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END ILLNESS_AT_TIME_OF_VACCINATION,
CASE WHEN FIELD_ID='956212' AND CONTEXT_NAME= 'LITERATURE_REFERENCE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END LITERATURE_REFERENCE,
CASE WHEN FIELD_ID='956106' AND CONTEXT_NAME= 'NARRATIVE_EVENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT,
CASE WHEN FIELD_ID='956196' AND CONTEXT_NAME= 'THERPHY_START_DATE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END THERPHY_START_DATE,
CASE WHEN FIELD_ID='114652' AND CONTEXT_NAME= 'EVALUATION_SUMMARY' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END EVALUATION_SUMMARY,
CASE WHEN FIELD_ID='956223' AND CONTEXT_NAME= 'PREGNANCY_OUTCOME_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PREGNANCY_OUTCOME_TEXT,
CASE WHEN FIELD_ID='956162' AND CONTEXT_NAME= 'INITIAL_MANUFACTURE_ANALYSIS' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END INITIAL_MANUFACTURE_ANALYSIS,
CASE WHEN FIELD_ID='956112' AND CONTEXT_NAME= 'NARRATIVE_EVENT_PHARMA_COMMENTS' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT_PHARMA_COMMENTS,
CASE WHEN FIELD_ID='123004' AND CONTEXT_NAME= 'MULTI_INDICATION_TERM_PRODUCT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_INDICATION_TERM_PRODUCT,
CASE WHEN FIELD_ID='956164' AND CONTEXT_NAME= 'RATIONAL_NOT_REPORTING' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END RATIONAL_NOT_REPORTING,
CASE WHEN FIELD_ID='956179' AND CONTEXT_NAME= 'JOURNAL_TITLE_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END JOURNAL_TITLE_TEXT,
CASE WHEN FIELD_ID='956450' AND CONTEXT_NAME= 'MULTI_CORRECTIVE_ACTIONS' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_CORRECTIVE_ACTIONS,
CASE WHEN FIELD_ID='956150' AND CONTEXT_NAME= 'NARRATIVE_EVENT_ADDITIONAL_COMMENTS' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT_ADDITIONAL_COMMENTS,
CASE WHEN FIELD_ID='95612' AND CONTEXT_NAME= 'DRUG_ADD_INFO' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END DRUG_ADD_INFO,
CASE WHEN FIELD_ID='956177' AND CONTEXT_NAME= 'ARTICLE_TITLE_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END ARTICLE_TITLE_TEXT,
CASE WHEN FIELD_ID='956446' AND CONTEXT_NAME= 'MULTI_IDENTIFIED_ACTION_NARRATIVE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_IDENTIFIED_ACTION_NARRATIVE,
CASE WHEN FIELD_ID='956142' AND CONTEXT_NAME= 'NARRATIVE_REMARKS_2' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_REMARKS_2,
CASE WHEN FIELD_ID='951102' AND CONTEXT_NAME= 'ADD_DOC_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END ADD_DOC_TEXT,
CASE WHEN FIELD_ID='956225' AND CONTEXT_NAME= 'PREGNANCY_HISTORY_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PREGNANCY_HISTORY_TEXT,
CASE WHEN FIELD_ID='108110' AND CONTEXT_NAME= 'MULTI_INDICATION_TERM_PATIENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_INDICATION_TERM_PATIENT,
CASE WHEN FIELD_ID='956192' AND CONTEXT_NAME= 'PROJECTED_TIMING' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PROJECTED_TIMING,
CASE WHEN FIELD_ID='106798' AND CONTEXT_NAME= 'PATIENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PATIENT,
CASE WHEN FIELD_ID='956208' AND CONTEXT_NAME= 'CONGENITAL_ANOMALY' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END CONGENITAL_ANOMALY,
CASE WHEN FIELD_ID='108114' AND CONTEXT_NAME= 'MULTI_REACTION_TERM_PATIENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_REACTION_TERM_PATIENT,
CASE WHEN FIELD_ID='956229' AND CONTEXT_NAME= 'PATIENT_REPCAUSE_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PATIENT_REPCAUSE_TEXT,
CASE WHEN FIELD_ID='956217' AND CONTEXT_NAME= 'SOURCE_DESCRIPTION_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END SOURCE_DESCRIPTION_TEXT,
CASE WHEN FIELD_ID='125001' AND CONTEXT_NAME= 'STUDY_NAME' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END STUDY_NAME,
CASE WHEN FIELD_ID='956181' AND CONTEXT_NAME= 'OTHER_INFO_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END OTHER_INFO_TEXT,
CASE WHEN FIELD_ID='956194' AND CONTEXT_NAME= 'TREATMENT_DESC' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END TREATMENT_DESC,
CASE WHEN FIELD_ID='956231' AND CONTEXT_NAME= 'PREGNANCY_COMMENT_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PREGNANCY_COMMENT_TEXT,
CASE WHEN FIELD_ID='956188' AND CONTEXT_NAME= 'E2BCAUSALITY_RESULTSF' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END E2BCAUSALITY_RESULTSF,
CASE WHEN FIELD_ID='122608' AND CONTEXT_NAME= 'DRUG_DOSAGE_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END DRUG_DOSAGE_TEXT,
CASE WHEN FIELD_ID='956204' AND CONTEXT_NAME= 'RATIONAL_REVIEW' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END RATIONAL_REVIEW,
CASE WHEN FIELD_ID='956215' AND CONTEXT_NAME= 'GENERAL_DUPLICATE_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END GENERAL_DUPLICATE_TEXT,
CASE WHEN FIELD_ID='956140' AND CONTEXT_NAME= 'NARRATIVE_REMARKS_1' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_REMARKS_1,
CASE WHEN FIELD_ID='112606' AND CONTEXT_NAME= 'RESULT_UNSTRUCTURED_DATA' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END RESULT_UNSTRUCTURED_DATA,
CASE WHEN FIELD_ID='121114' AND CONTEXT_NAME= 'MULTI_REACTION_TERM_PARENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_REACTION_TERM_PARENT,
CASE WHEN FIELD_ID='956206' AND CONTEXT_NAME= 'DISEASE_TERM' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END DISEASE_TERM,
CASE WHEN FIELD_ID='956444' AND CONTEXT_NAME= 'MULTI_CORRECTIVE_PREVENTION_ACTION_NARRATIVE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_CORRECTIVE_PREVENTION_ACTION_NARRATIVE,
CASE WHEN FIELD_ID='956448' AND CONTEXT_NAME= 'MULTI_MANUFACTUREE_NARRATIVE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_MANUFACTUREE_NARRATIVE,
CASE WHEN FIELD_ID='956186' AND CONTEXT_NAME= 'E2BCAUSALITY_METHODSF' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END E2BCAUSALITY_METHODSF,
CASE WHEN FIELD_ID='956200' AND CONTEXT_NAME= 'CORRECTIVE_PREVT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END CORRECTIVE_PREVT,
CASE WHEN FIELD_ID='956104' AND CONTEXT_NAME= 'PARENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PARENT,
CASE WHEN FIELD_ID='956202' AND CONTEXT_NAME= 'MANUFACTURER_NARRATIVE' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MANUFACTURER_NARRATIVE,
CASE WHEN FIELD_ID='956110' AND CONTEXT_NAME= 'NARRATIVE_EVENT_COMPANY_REMARKS' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT_COMPANY_REMARKS,
CASE WHEN FIELD_ID='121110' AND CONTEXT_NAME= 'MULTI_INDICATION_TERM_PARENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_INDICATION_TERM_PARENT,
CASE WHEN FIELD_ID='956219' AND CONTEXT_NAME= 'REPORTER_OCCUPATION_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END REPORTER_OCCUPATION_TEXT,
CASE WHEN FIELD_ID='956123' AND CONTEXT_NAME= 'MEDICAL_COMMENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MEDICAL_COMMENT,
CASE WHEN FIELD_ID='956114' AND CONTEXT_NAME= 'NARRATIVE_EVENT_SUMMARY_DESCRIPTION' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT_SUMMARY_DESCRIPTION,
CASE WHEN FIELD_ID='120102' AND CONTEXT_NAME= 'MULTI_REPORTED_DISEASE_TERM_PARENT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_REPORTED_DISEASE_TERM_PARENT,
CASE WHEN FIELD_ID='956210' AND CONTEXT_NAME= 'TEST_NAME' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END TEST_NAME,
CASE WHEN FIELD_ID='668003' AND CONTEXT_NAME= 'MULTI_SENDER_DIAGNOSIS_TERM' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_SENDER_DIAGNOSIS_TERM,
CASE WHEN FIELD_ID='125937' AND CONTEXT_NAME= 'MULTI_UNBLINDED_INFORMATION' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END MULTI_UNBLINDED_INFORMATION,
CASE WHEN FIELD_ID='956227' AND CONTEXT_NAME= 'PATIENT_AUTOPSY_TEXT' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END PATIENT_AUTOPSY_TEXT,
CASE WHEN FIELD_ID='956108' AND CONTEXT_NAME= 'NARRATIVE_EVENT_REACTION_DESCRIPTION' THEN COALESCE(VALUE,VALUE_NF) ELSE NULL END NARRATIVE_EVENT_REACTION_DESCRIPTION
	FROM
        ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.LS_DB_LANG_REDUCTED 
    WHERE
        LS_DB_LANG_REDUCTED.FK_ASR_REC_ID IS NOT NULL;	
		
		
CREATE OR REPLACE VIEW ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_icd_smq_hier AS
    ( SELECT
        a.bk_meddra_icd_wid,
        b.sk_cmq_smq_hier_term_scope_wid,
        a.meddra_version,
        b.smq_term_scope,
        b.language_code,
        a.language_wid,
        b.is_deleted,
        b.process_date,
        b.expiry_date,
        b.load_ts,
        b.integration_id
    FROM
        (
            SELECT
                *
            FROM
                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_icd
            WHERE
                is_meddra_flag = 1
        ) a,
        (
            SELECT
                *
            FROM
                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_cmq_smq_hier_term_scope
            WHERE
                term_level = 4
        ) b
    WHERE
            a.pt_code = b.term_code
        AND a.meddra_version = b.meddra_version
    UNION
    SELECT
        a.bk_meddra_icd_wid,
        b.sk_cmq_smq_hier_term_scope_wid,
        a.meddra_version,
        b.smq_term_scope,
        b.language_code,
        a.language_wid,
        b.is_deleted,
        b.process_date,
        b.expiry_date,
        b.load_ts,
        b.integration_id
    FROM
        (
            SELECT
                *
            FROM
                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_meddra_icd
            WHERE
                is_meddra_flag = 1
        ) a,
        (
            SELECT
                *
            FROM
                ${tenant_transfm_db_name}.${tenant_transfm_schema_name}.ls_db_cmq_smq_hier_term_scope
            WHERE
                term_level = 5
        ) b
    WHERE
            a.llt_code = b.term_code
        AND a.meddra_version = b.meddra_version
    );
		